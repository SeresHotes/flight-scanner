//! Рейсы джобы колонками (FlightCols): все плечи одной таблицей, поля стыковки уже
//! числами. Из неё строят свои структуры перебор A* (search) и наборы городов
//! (overview). Полный рейс лежит JSON-колонкой `seg` (только поля сегмента) и
//! разбирается лениво — для рейсов из результата.
//!
//! Хранение — Parquet-файл на джобу (`plan_flights/<job>.parquet` рядом с БД) в той же
//! схеме, что писал Python-планировщик (файлы прежних джоб читаются как есть).

use std::collections::HashMap;
use std::fs::File;
use std::sync::{Arc, Mutex, OnceLock};

use rustc_hash::FxHashMap;

use arrow::array::{Array, ArrayRef, BooleanArray, DictionaryArray, Float64Array, Int16Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::Int32Type;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector};
use parquet::file::metadata::PageIndexPolicy;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::dates::{naive_seconds, parse_naive};
use crate::ticket::Ticket;

pub const STR_COLS: [&str; 6] = ["orig_city", "orig_airport", "dest", "dest_airport", "dep_iso", "arr_iso"];
/// Колонки кодов: читаются словарём прямо в номера (без String на строку).
const CODE_COLS: [&str; 4] = ["orig_city", "orig_airport", "dest", "dest_airport"];
pub const NUM_COLS: [&str; 12] =
    ["leg", "price", "transfers", "duration", "hidden", "bag_incl", "pts_min", "layover", "dep_ts", "arr_ts", "dep_ord", "arr_ord"];

/// Нет кода (пустая строка) в числовых колонках кодов.
pub const NO_CODE: u32 = u32::MAX;

pub struct FlightCols {
    pub n: usize,
    /// Словарь кодов городов/аэропортов (все четыре колонки кодов) и числовые колонки:
    /// перебор и наборы сравнивают и хэшируют числа, а не строки.
    pub codes: Vec<String>,
    pub code_ix: FxHashMap<String, u32>,
    pub orig_city_id: Vec<u32>,
    pub orig_airport_id: Vec<u32>,
    pub dest_id: Vec<u32>,
    pub dest_airport_id: Vec<u32>,
    pub leg: Vec<i16>,
    /// Вылет/прилёт строками (ISO) — нужны только для перезаписи файла: у рейсов из
    /// файла дочитываются лениво (dep_iso/arr_iso).
    iso: OnceLock<(Vec<Option<String>>, Vec<Option<String>>)>,
    pub price: Vec<f64>,
    pub transfers: Vec<i64>,
    pub duration: Vec<i64>,
    pub hidden: Vec<bool>,
    pub bag_incl: Vec<bool>,
    pub pts_min: Vec<Option<i64>>,
    pub layover: Vec<Option<i64>>,
    pub dep_ts: Vec<f64>,
    pub arr_ts: Vec<f64>,
    pub dep_ord: Vec<i64>,
    pub arr_ord: Vec<i64>,
    /// Источники рейсов (из сбора) или JSON `seg` (из файла), разбираемый лениво.
    raw: Mutex<Raw>,
    parsed: Mutex<HashMap<usize, Arc<Ticket>>>,
}

/// Откуда полный рейс строки джобы: строка склада (номер строки блока, у hidden-city —
/// и выход на пересадке; билет разбирается только для результата) или сам билет (серии
/// коллектора/GraphQL, тесты).
#[derive(Clone, Debug)]
pub enum FlightSrc {
    Store(crate::lakestore::StoreRow),
    Owned(Arc<Ticket>),
}


/// Словарь кодов джобы (города, аэропорты). Коды склада заносятся первыми в том же
/// порядке — номер кода склада и есть номер в джобе (строки склада копируются без
/// перевода); остальные (серии коллектора, хабы) дописываются. Пустой код — NO_CODE.
pub struct JobCodes {
    inner: Mutex<(Vec<String>, FxHashMap<String, u32>)>,
    /// Сколько кодов склада занесено при создании; номер склада ≥ base — дописан позже.
    base: usize,
    /// Номер пустого кода склада (он в джобе — NO_CODE).
    store_empty: Option<u32>,
    /// Словарь начат с кодов склада (номера колонок склада годятся как есть).
    pub store_based: bool,
}

impl JobCodes {
    pub fn new(store_names: &[String]) -> JobCodes {
        let names: Vec<String> = store_names.to_vec();
        let ix: FxHashMap<String, u32> = names.iter().enumerate().filter(|(_, n)| !n.is_empty()).map(|(i, n)| (n.clone(), i as u32)).collect();
        let store_empty = names.iter().position(|n| n.is_empty()).map(|i| i as u32);
        JobCodes { base: names.len(), store_based: !names.is_empty(), inner: Mutex::new((names, ix)), store_empty }
    }

    /// Номер кода (пустой — NO_CODE); новые дописываются.
    pub fn id(&self, code: &str) -> u32 {
        if code.is_empty() {
            return NO_CODE;
        }
        let mut g = self.inner.lock().unwrap();
        let (codes, ix) = &mut *g;
        intern(codes, ix, code)
    }

    /// Номер без занесения (None — такого кода нет).
    pub fn lookup(&self, code: &str) -> Option<u32> {
        self.inner.lock().unwrap().1.get(code).copied()
    }

    /// Номер кода склада в джобе: тот же, кроме пустого и дописанных в склад после
    /// создания словаря (`name` — их имя).
    pub fn from_store(&self, id: u32, name: &dyn Fn(u32) -> String) -> u32 {
        if Some(id) == self.store_empty {
            return NO_CODE;
        }
        if (id as usize) < self.base {
            return id;
        }
        self.id(&name(id))
    }

    pub fn name(&self, id: u32) -> String {
        if id == NO_CODE {
            return String::new();
        }
        self.inner.lock().unwrap().0.get(id as usize).cloned().unwrap_or_default()
    }

    fn snapshot(&self) -> (Vec<String>, FxHashMap<String, u32>) {
        let g = self.inner.lock().unwrap();
        (g.0.clone(), g.1.clone())
    }
}

/// Пересадка рейса для hidden-city: аэропорт (номер кода джобы), прилёт в него (секунды
/// наивного времени), длительность до него, мин. стыковка до него, хэш ключа виртуального
/// рейса «выходим здесь».
pub struct Tp {
    pub ap: u32,
    pub arr: Option<i64>,
    pub dur: i64,
    pub pmin: Option<i64>,
    pub key: u64,
}

/// Рейс сбора — компактная строка: всё для стыковки числами (коды — номера `JobCodes`)
/// плюс источник полного рейса. Строка склада копируется в неё без разбора билета;
/// билет серии коллектора — переводится (`from_ticket`).
#[derive(Clone, Debug)]
pub struct Fl {
    pub leg: i16,
    pub orig_city: u32,
    pub orig_airport: u32,
    pub dest: u32,
    pub dest_airport: u32,
    pub price: f64,
    pub transfers: i64,
    pub duration: i64,
    pub hidden: bool,
    pub bag_incl: bool,
    pub pts_min: Option<i64>,
    pub layover: Option<i64>,
    pub dep_ts: f64,
    pub arr_ts: f64,
    pub dep_ord: i64,
    pub arr_ord: i64,
    /// Хэш ключа рейса (`Ticket::flight_key`) — дедуп.
    pub key: u64,
    /// День вылета серии (`lakestore::day_num`) и город серии (запроса X→ANY).
    pub day: i32,
    pub series: u32,
    pub src: FlightSrc,
}

/// Секунды «наивного» времени → (ts, номер дня) колонок джобы; None → (NaN, −1).
pub fn ts_ord(secs: Option<i64>) -> (f64, i64) {
    match secs {
        Some(s) => (s as f64, s.div_euclid(86_400) + 719_163),
        None => (f64::NAN, -1),
    }
}

impl Fl {
    /// Из полного билета (серии коллектора, тесты): те же поля, что даёт строка склада.
    pub fn from_ticket(f: Ticket, codes: &JobCodes) -> Fl {
        let dep = f.departure_at.clone().filter(|s| !s.is_empty());
        let arr = if dep.is_some() { f.arrival() } else { None };
        let secs = |iso: Option<&str>| iso.and_then(parse_naive).map(|dt| naive_seconds(dt) as i64);
        let (dep_ts, dep_ord) = ts_ord(secs(dep.as_deref()));
        let (arr_ts, arr_ord) = ts_ord(secs(arr.as_deref()));
        let day = f.search_date.as_deref().filter(|s| s.len() >= 10).or(f.departure_at.as_deref().filter(|s| s.len() >= 10)).and_then(|s| chrono::NaiveDate::parse_from_str(&s[..10], "%Y-%m-%d").ok()).map(crate::lakestore::day_num).unwrap_or(i32::MIN);
        let series = codes.id(&f.search_origin.as_deref().filter(|s| !s.is_empty()).or(f.origin_city()).unwrap_or("").to_uppercase());
        let src = match &f.src {
            Some(s) => FlightSrc::Store(s.clone()),
            None => FlightSrc::Owned(Arc::new(Ticket::default())),
        };
        let mut fl = Fl {
            leg: 0,
            orig_city: codes.id(&f.origin_city().map(|s| s.to_uppercase()).unwrap_or_default()),
            orig_airport: codes.id(&upper(&f.origin_airport)),
            dest: codes.id(&f.dest_city().map(|s| s.to_uppercase()).unwrap_or_default()),
            dest_airport: codes.id(&upper(&f.destination_airport)),
            price: f.price(),
            transfers: f.transfers,
            duration: f.duration.unwrap_or(0),
            hidden: f.hidden_city.is_some(),
            bag_incl: f.baggage.as_ref().map(|b| b.included).unwrap_or(false),
            pts_min: f.min_transfer_minutes(),
            layover: f.layover_minutes,
            dep_ts,
            arr_ts,
            dep_ord,
            arr_ord,
            key: crate::lakestore::key_hash(&f.flight_key()),
            day,
            series,
            src,
        };
        if let FlightSrc::Owned(_) = fl.src {
            fl.src = FlightSrc::Owned(Arc::new(f));
        }
        fl
    }

    /// Полный билет, если рейс не из склада (серии коллектора).
    pub fn owned(&self) -> Option<&Arc<Ticket>> {
        match &self.src {
            FlightSrc::Owned(t) => Some(t),
            FlightSrc::Store(_) => None,
        }
    }
}

/// Рейсы джобы по плечам до колонок: сбор кладёт плечо сразу, как собрал (плечо «любой →
/// любой» — по дням). Плечи — в любом порядке, колонки в `finish` — по порядку плеч.
pub struct FlightColsBuilder {
    codes: Arc<JobCodes>,
    legs: Vec<Vec<Fl>>,
}

impl FlightColsBuilder {
    pub fn new(legs: usize, codes: Arc<JobCodes>) -> FlightColsBuilder {
        FlightColsBuilder { codes, legs: (0..legs).map(|_| Vec::new()).collect() }
    }

    pub fn codes(&self) -> &Arc<JobCodes> {
        &self.codes
    }

    pub fn push_leg(&mut self, leg: usize, flights: Vec<Fl>) {
        if self.legs.len() <= leg {
            self.legs.resize_with(leg + 1, Vec::new);
        }
        let out = &mut self.legs[leg];
        out.reserve(flights.len());
        for mut f in flights {
            f.leg = leg as i16;
            out.push(f);
        }
    }

    pub fn len(&self) -> usize {
        self.legs.iter().map(|l| l.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn leg_len(&self, leg: usize) -> usize {
        self.legs.get(leg).map(|l| l.len()).unwrap_or(0)
    }

    /// Города прилёта рейсов плеча (UPPER, без повторов, по порядку появления).
    pub fn dest_cities(&self, leg: usize) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        self.legs.get(leg).map(|l| l.as_slice()).unwrap_or(&[]).iter().filter(|r| r.dest != NO_CODE && seen.insert(r.dest)).map(|r| self.codes.name(r.dest)).collect()
    }

    /// Коды вылета рейсов плеча — город и аэропорт (UPPER, без повторов).
    pub fn origin_codes(&self, leg: usize) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for r in self.legs.get(leg).map(|l| l.as_slice()).unwrap_or(&[]) {
            for id in [r.orig_city, r.orig_airport] {
                if id != NO_CODE && seen.insert(id) {
                    out.push(self.codes.name(id));
                }
            }
        }
        out
    }

    pub fn finish(self) -> FlightCols {
        let n = self.len();
        let mut c = FlightCols::with_capacity(n);
        let (codes, ix) = self.codes.snapshot();
        c.codes = codes;
        c.code_ix = ix;
        let mut srcs = Vec::with_capacity(n);
        for leg in self.legs {
            for r in leg {
                c.leg.push(r.leg);
                c.orig_city_id.push(r.orig_city);
                c.orig_airport_id.push(r.orig_airport);
                c.dest_id.push(r.dest);
                c.dest_airport_id.push(r.dest_airport);
                c.price.push(r.price);
                c.transfers.push(r.transfers);
                c.duration.push(r.duration);
                c.hidden.push(r.hidden);
                c.bag_incl.push(r.bag_incl);
                c.pts_min.push(r.pts_min);
                c.layover.push(r.layover);
                c.dep_ts.push(r.dep_ts);
                c.arr_ts.push(r.arr_ts);
                c.dep_ord.push(r.dep_ord);
                c.arr_ord.push(r.arr_ord);
                srcs.push(r.src);
            }
        }
        c.n = c.leg.len();
        c.raw = Mutex::new(Raw::Srcs(srcs));
        c
    }
}

enum Raw {
    Srcs(Vec<FlightSrc>),
    /// Путь Parquet: нужные строки колонки seg дочитываются выборкой (prefetch / flight).
    Lazy(String),
}

fn upper(v: &Option<String>) -> String {
    v.as_deref().map(|s| s.to_uppercase()).unwrap_or_default()
}

/// Номер кода по словарю (пустой код — NO_CODE); новые коды дописываются.
fn intern(codes: &mut Vec<String>, ix: &mut FxHashMap<String, u32>, code: &str) -> u32 {
    if code.is_empty() {
        return NO_CODE;
    }
    if let Some(&id) = ix.get(code) {
        return id;
    }
    let id = codes.len() as u32;
    codes.push(code.to_string());
    ix.insert(code.to_string(), id);
    id
}

impl FlightCols {
    pub fn len(&self) -> usize {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// Из собранных рейсов по плечам (билеты из склада — номерами строк).
    pub fn from_collected(collected: &[Vec<Arc<Ticket>>]) -> FlightCols {
        let codes = Arc::new(JobCodes::new(&[]));
        let mut b = FlightColsBuilder::new(collected.len(), codes.clone());
        for (leg, flights) in collected.iter().enumerate() {
            b.push_leg(leg, flights.iter().map(|f| Fl::from_ticket((**f).clone(), &codes)).collect());
        }
        b.finish()
    }

    /// Билеты не из склада (серии коллектора/GraphQL), без виртуальных — для котировок.
    pub fn owned_tickets(&self) -> Vec<Arc<Ticket>> {
        match &*self.raw.lock().unwrap() {
            Raw::Srcs(v) => v.iter().filter_map(|s| match s {
                FlightSrc::Owned(t) if t.hidden_city.is_none() => Some(t.clone()),
                _ => None,
            }).collect(),
            Raw::Lazy(_) => Vec::new(),
        }
    }

    /// Номер кода (None — такого кода в рейсах нет).
    pub fn code_id(&self, code: &str) -> Option<u32> {
        self.code_ix.get(code).copied()
    }

    fn with_capacity(n: usize) -> FlightCols {
        FlightCols {
            n: 0,
            codes: Vec::new(),
            code_ix: FxHashMap::default(),
            orig_city_id: Vec::with_capacity(n),
            orig_airport_id: Vec::with_capacity(n),
            dest_id: Vec::with_capacity(n),
            dest_airport_id: Vec::with_capacity(n),
            leg: Vec::with_capacity(n),
            iso: OnceLock::new(),
            price: Vec::with_capacity(n),
            transfers: Vec::with_capacity(n),
            duration: Vec::with_capacity(n),
            hidden: Vec::with_capacity(n),
            bag_incl: Vec::with_capacity(n),
            pts_min: Vec::with_capacity(n),
            layover: Vec::with_capacity(n),
            dep_ts: Vec::with_capacity(n),
            arr_ts: Vec::with_capacity(n),
            dep_ord: Vec::with_capacity(n),
            arr_ord: Vec::with_capacity(n),
            raw: Mutex::new(Raw::Srcs(Vec::new())),
            parsed: Mutex::new(HashMap::new()),
        }
    }

    /// Код по номеру ('' — NO_CODE).
    pub fn code(&self, id: u32) -> &str {
        if id == NO_CODE { "" } else { &self.codes[id as usize] }
    }

    pub fn orig_city(&self, r: usize) -> &str {
        self.code(self.orig_city_id[r])
    }

    pub fn orig_airport(&self, r: usize) -> &str {
        self.code(self.orig_airport_id[r])
    }

    pub fn dest(&self, r: usize) -> &str {
        self.code(self.dest_id[r])
    }

    pub fn dest_airport(&self, r: usize) -> &str {
        self.code(self.dest_airport_id[r])
    }

    fn iso(&self) -> &(Vec<Option<String>>, Vec<Option<String>>) {
        self.iso.get_or_init(|| {
            let path = match &*self.raw.lock().unwrap() {
                Raw::Lazy(p) => Some(p.clone()),
                Raw::Srcs(_) => None,
            };
            match path {
                Some(p) => read_iso(&p).map_err(|e| eprintln!("[flightcols] {p}: dep_iso/arr_iso не прочитаны: {e}")).unwrap_or_else(|_| (vec![None; self.n], vec![None; self.n])),
                // рейсы сбора: строки — из полных рейсов (разбор всех строк — только по запросу)
                None => {
                    let (_, dep, arr) = self.seg_all();
                    (dep, arr)
                }
            }
        })
    }

    /// Вылет строками (ISO, как в рейсе).
    pub fn dep_iso(&self) -> &[Option<String>] {
        &self.iso().0
    }

    /// Прилёт строками (ISO без зоны, segments.arrival).
    pub fn arr_iso(&self) -> &[Option<String>] {
        &self.iso().1
    }

    /// Полный рейс строки i — для сегментов результата. Из файла — разобранный
    /// заранее prefetch, иначе дочитывается одна строка.
    pub fn flight(&self, i: usize) -> Arc<Ticket> {
        {
            let raw = self.raw.lock().unwrap();
            if let Raw::Srcs(v) = &*raw {
                if let FlightSrc::Owned(t) = &v[i] {
                    return t.clone();
                }
            }
        }
        if let Some(got) = self.parsed.lock().unwrap().get(&i) {
            return got.clone();
        }
        self.prefetch(&[i]);
        self.parsed.lock().unwrap().get(&i).cloned().unwrap_or_default()
    }

    /// Разобрать рейсы строк rows одним проходом по файлу: выборка строк Parquet
    /// (RowSelection) — читается и копируется только нужное, а при индексе страниц
    /// ненужные страницы колонки seg не распаковываются вовсе.
    pub fn prefetch(&self, rows: &[usize]) {
        let mut need: Vec<usize> = {
            let parsed = self.parsed.lock().unwrap();
            rows.iter().copied().filter(|r| *r < self.n && !parsed.contains_key(r)).collect()
        };
        need.sort_unstable();
        need.dedup();
        if need.is_empty() {
            return;
        }
        let got = self.materialize(&need);
        let mut parsed = self.parsed.lock().unwrap();
        for (r, t) in need.into_iter().zip(got) {
            parsed.insert(r, t);
        }
    }

    /// Полные рейсы строк (по возрастанию) без кэша: билеты как есть, строки склада —
    /// разжатием их блоков, из файла — выборкой колонки seg.
    fn materialize(&self, rows: &[usize]) -> Vec<Arc<Ticket>> {
        let raw = self.raw.lock().unwrap();
        match &*raw {
            Raw::Srcs(srcs) => {
                let store: Vec<(usize, &crate::lakestore::StoreRow)> = rows.iter().enumerate().filter_map(|(k, &r)| match &srcs[r] {
                    FlightSrc::Store(s) => Some((k, s)),
                    FlightSrc::Owned(_) => None,
                }).collect();
                let refs: Vec<&crate::lakestore::StoreRow> = store.iter().map(|(_, s)| *s).collect();
                let mut decoded = crate::lakestore::materialize(&refs).unwrap_or_else(|e| {
                    eprintln!("[flightcols] строки склада не разобраны: {e}");
                    vec![Ticket::default(); refs.len()]
                }).into_iter();
                let mut out: Vec<Arc<Ticket>> = Vec::with_capacity(rows.len());
                for &r in rows {
                    out.push(match &srcs[r] {
                        FlightSrc::Owned(t) => t.clone(),
                        FlightSrc::Store(_) => Arc::new(decoded.next().unwrap_or_default()),
                    });
                }
                out
            }
            Raw::Lazy(path) => {
                let path = path.clone();
                drop(raw);
                let jsons = read_seg_rows(&path, rows).unwrap_or_else(|e| {
                    eprintln!("[flightcols] {path}: колонка seg не прочитана: {e}");
                    vec![None; rows.len()]
                });
                jsons.into_iter().map(|json| Arc::new(json.as_deref().and_then(|s| serde_json::from_str::<Ticket>(s).ok()).unwrap_or_default())).collect()
            }
        }
    }

    /// Полные рейсы плеча (по порядку строк).
    pub fn leg_flights(&self, leg: usize) -> Vec<Arc<Ticket>> {
        let rows = self.rows(leg);
        self.prefetch(&rows);
        rows.into_iter().map(|r| self.flight(r)).collect()
    }

    pub fn rows(&self, leg: usize) -> Vec<usize> {
        (0..self.n).filter(|&r| self.leg[r] as usize == leg).collect()
    }

    pub fn origin_codes(&self, r: usize) -> (&str, &str) {
        (self.orig_city(r), self.orig_airport(r))
    }

    /// Город или аэропорт прилёта строки r в allow (None — любой).
    pub fn dest_in(&self, r: usize, allow: Option<&std::collections::HashSet<String>>) -> bool {
        match allow {
            None => true,
            Some(a) => a.contains(self.dest(r)) || a.contains(self.dest_airport(r)),
        }
    }

    /// Строки плеча по коду вылета — и городу, и аэропорту.
    pub fn by_origin(&self, rows: &[usize]) -> HashMap<String, Vec<usize>> {
        let mut out: HashMap<String, Vec<usize>> = HashMap::new();
        for &r in rows {
            let (c, a) = (self.orig_city(r), self.orig_airport(r));
            if !c.is_empty() {
                out.entry(c.to_string()).or_default().push(r);
            }
            if !a.is_empty() && a != c {
                out.entry(a.to_string()).or_default().push(r);
            }
        }
        out
    }

    /// Строки плеча по номеру кода вылета — и городу, и аэропорту.
    pub fn by_origin_ids(&self, rows: &[usize]) -> FxHashMap<u32, Vec<usize>> {
        let mut out: FxHashMap<u32, Vec<usize>> = FxHashMap::default();
        for &r in rows {
            let (c, a) = (self.orig_city_id[r], self.orig_airport_id[r]);
            if c != NO_CODE {
                out.entry(c).or_default().push(r);
            }
            if a != NO_CODE && a != c {
                out.entry(a).or_default().push(r);
            }
        }
        out
    }

    // ------------------------------ хранение ---------------------------------

    fn schema() -> Arc<Schema> {
        let mut fields: Vec<Field> = STR_COLS.iter().map(|c| Field::new(*c, DataType::Utf8, true)).collect();
        fields.extend([
            Field::new("leg", DataType::Int16, true),
            Field::new("price", DataType::Float64, true),
            Field::new("transfers", DataType::Int32, true),
            Field::new("duration", DataType::Int64, true),
            Field::new("hidden", DataType::Boolean, true),
            Field::new("bag_incl", DataType::Boolean, true),
            Field::new("pts_min", DataType::Float64, true),
            Field::new("layover", DataType::Float64, true),
            Field::new("dep_ts", DataType::Float64, true),
            Field::new("arr_ts", DataType::Float64, true),
            Field::new("dep_ord", DataType::Int64, true),
            Field::new("arr_ord", DataType::Int64, true),
            Field::new("seg", DataType::Utf8, true),
        ]);
        Arc::new(Schema::new(fields))
    }

    /// JSON сегмента (колонка seg) всех строк. Строки склада разбираются в порядке их
    /// блоков кусками по ~SEG_CHUNK строк — каждый блок разжимается один раз.
    #[allow(clippy::type_complexity)]
    fn seg_all(&self) -> (Vec<Option<String>>, Vec<Option<String>>, Vec<Option<String>>) {
        const SEG_CHUNK: usize = 32_768;
        let mut order: Vec<usize> = (0..self.n).collect();
        {
            let raw = self.raw.lock().unwrap();
            if let Raw::Srcs(srcs) = &*raw {
                let key = |r: usize| match &srcs[r] {
                    FlightSrc::Store(s) => (Arc::as_ptr(&s.block) as usize, s.row as usize),
                    FlightSrc::Owned(_) => (0, r),
                };
                order.sort_by_key(|&r| key(r));
            }
        }
        let mut out: Vec<Option<String>> = vec![None; self.n];
        let (mut dep, mut arr): (Vec<Option<String>>, Vec<Option<String>>) = (vec![None; self.n], vec![None; self.n]);
        for chunk in order.chunks(SEG_CHUNK) {
            let mut rows = chunk.to_vec();
            rows.sort_unstable();
            for (r, t) in rows.iter().zip(self.materialize(&rows)) {
                out[*r] = serde_json::to_string(&t.segment_source()).ok();
                // вылет/прилёт строками — как в рейсе (прилёт — arrival(), если есть вылет)
                dep[*r] = t.departure_at.clone().filter(|s| !s.is_empty());
                arr[*r] = if dep[*r].is_some() { t.arrival() } else { None };
            }
        }
        (out, dep, arr)
    }

    /// Строки [from, to) батчем Parquet (seg — готовые JSON этих строк).
    fn to_batch(&self, from: usize, to: usize, seg: &[Option<String>], dep: &[Option<String>], arr: &[Option<String>]) -> Result<RecordBatch, String> {
        let r = from..to;
        let codes = |ids: &[u32]| -> ArrayRef { Arc::new(StringArray::from(ids.iter().map(|&id| Some(self.code(id))).collect::<Vec<_>>())) };
        let opt = |v: &[Option<i64>]| -> ArrayRef { Arc::new(Float64Array::from(v.iter().map(|v| v.map(|x| x as f64).unwrap_or(f64::NAN)).collect::<Vec<_>>())) };
        let cols: Vec<ArrayRef> = vec![
            codes(&self.orig_city_id[r.clone()]),
            codes(&self.orig_airport_id[r.clone()]),
            codes(&self.dest_id[r.clone()]),
            codes(&self.dest_airport_id[r.clone()]),
            Arc::new(StringArray::from(dep.iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
            Arc::new(StringArray::from(arr.iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
            Arc::new(Int16Array::from(self.leg[r.clone()].to_vec())),
            Arc::new(Float64Array::from(self.price[r.clone()].to_vec())),
            Arc::new(Int32Array::from(self.transfers[r.clone()].iter().map(|t| *t as i32).collect::<Vec<_>>())),
            Arc::new(Int64Array::from(self.duration[r.clone()].to_vec())),
            Arc::new(BooleanArray::from(self.hidden[r.clone()].to_vec())),
            Arc::new(BooleanArray::from(self.bag_incl[r.clone()].to_vec())),
            opt(&self.pts_min[r.clone()]),
            opt(&self.layover[r.clone()]),
            Arc::new(Float64Array::from(self.dep_ts[r.clone()].to_vec())),
            Arc::new(Float64Array::from(self.arr_ts[r.clone()].to_vec())),
            Arc::new(Int64Array::from(self.dep_ord[r.clone()].to_vec())),
            Arc::new(Int64Array::from(self.arr_ord[r].to_vec())),
            Arc::new(StringArray::from(seg.iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
        ];
        RecordBatch::try_new(Self::schema(), cols).map_err(|e| e.to_string())
    }

    /// Рейсы джобы в Parquet кусками по WRITE_CHUNK строк; JSON сегментов (seg) — заранее,
    /// в порядке блоков склада.
    pub fn write(&self, path: &str) -> Result<(), String> {
        const WRITE_CHUNK: usize = 32_768;
        let file = File::create(path).map_err(|e| e.to_string())?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_column_compression("seg".into(), Compression::LZ4_RAW)
            .build();
        let mut writer = ArrowWriter::try_new(file, Self::schema(), Some(props)).map_err(|e| e.to_string())?;
        let (seg, dep, arr) = self.seg_all();
        let mut from = 0;
        while from < self.n || from == 0 {
            let to = (from + WRITE_CHUNK).min(self.n);
            writer.write(&self.to_batch(from, to, &seg[from..to], &dep[from..to], &arr[from..to])?).map_err(|e| e.to_string())?;
            if to >= self.n {
                break;
            }
            from = to;
        }
        writer.close().map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Колонки стыковки из Parquet: коды — словарём прямо в номера, числа как есть;
    /// `seg` — выборкой строк (prefetch/flight), даты-строки — лениво (dep_iso/arr_iso).
    pub fn read(path: &str) -> Result<FlightCols, String> {
        let file = File::open(path).map_err(|e| e.to_string())?;
        let plain = ParquetRecordBatchReaderBuilder::try_new(file.try_clone().map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
        // Коды просим словарём (в файле они и так словарные): уникальных значений —
        // сотни–тысячи на 135 тыс. строк, номера строк — по индексам словаря.
        let dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let hinted = Schema::new(
            plain
                .schema()
                .fields()
                .iter()
                .map(|f| if CODE_COLS.contains(&f.name().as_str()) { Field::new(f.name(), dict.clone(), true) } else { f.as_ref().clone() })
                .collect::<Vec<_>>(),
        );
        let builder = match ParquetRecordBatchReaderBuilder::try_new_with_options(file, ArrowReaderOptions::new().with_schema(Arc::new(hinted))) {
            Ok(b) => b,
            Err(_) => plain, // схема не подошла — коды строками, номера по значениям
        };
        let schema = builder.schema().clone();
        let wanted: Vec<usize> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| CODE_COLS.contains(&f.name().as_str()) || NUM_COLS.contains(&f.name().as_str()))
            .map(|(i, _)| i)
            .collect();
        let mask = parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), wanted);
        let reader = builder.with_projection(mask).with_batch_size(1 << 16).build().map_err(|e| e.to_string())?;
        let mut batches = Vec::new();
        for b in reader {
            batches.push(b.map_err(|e| e.to_string())?);
        }
        let n: usize = batches.iter().map(|b| b.num_rows()).sum();
        let mut c = FlightCols::with_capacity(n);
        for b in &batches {
            let f = |name: &str| -> Result<Vec<Option<f64>>, String> { f64_col(b, name) };
            let bl = |name: &str| -> Result<Vec<bool>, String> { bool_col(b, name) };
            for (name, out) in [("orig_city", 0), ("orig_airport", 1), ("dest", 2), ("dest_airport", 3)] {
                let ids = code_col(b, name, &mut c.codes, &mut c.code_ix)?;
                match out {
                    0 => c.orig_city_id.extend(ids),
                    1 => c.orig_airport_id.extend(ids),
                    2 => c.dest_id.extend(ids),
                    _ => c.dest_airport_id.extend(ids),
                }
            }
            c.leg.extend(i64_vals(b, "leg", 0)?.into_iter().map(|v| v as i16));
            c.price.extend(f64_vals(b, "price", 0.0)?);
            c.transfers.extend(i64_vals(b, "transfers", 0)?);
            c.duration.extend(i64_vals(b, "duration", 0)?);
            c.hidden.extend(bl("hidden")?);
            c.bag_incl.extend(bl("bag_incl")?);
            c.pts_min.extend(f("pts_min")?.into_iter().map(|v| v.filter(|x| !x.is_nan()).map(|x| x as i64)));
            c.layover.extend(f("layover")?.into_iter().map(|v| v.filter(|x| !x.is_nan()).map(|x| x as i64)));
            c.dep_ts.extend(f64_vals(b, "dep_ts", f64::NAN)?);
            c.arr_ts.extend(f64_vals(b, "arr_ts", f64::NAN)?);
            c.dep_ord.extend(i64_vals(b, "dep_ord", -1)?);
            c.arr_ord.extend(i64_vals(b, "arr_ord", -1)?);
        }
        c.n = n;
        c.raw = Mutex::new(Raw::Lazy(path.to_string()));
        Ok(c)
    }
}

/// Номера кодов колонки name: словарная колонка — по уникальным значениям словаря,
/// строковая — по значениям строк (без String на строку); пусто/null — NO_CODE.
fn code_col(b: &RecordBatch, name: &str, codes: &mut Vec<String>, ix: &mut FxHashMap<String, u32>) -> Result<Vec<u32>, String> {
    let Some(col) = b.column_by_name(name) else { return Ok(vec![NO_CODE; b.num_rows()]) };
    if let Some(d) = col.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
        let values = cast(d.values(), &DataType::Utf8).map_err(|e| e.to_string())?;
        let values = values.as_any().downcast_ref::<StringArray>().ok_or("словарь не строки")?;
        let map: Vec<u32> = (0..values.len()).map(|k| if values.is_null(k) { NO_CODE } else { intern(codes, ix, values.value(k)) }).collect();
        let keys = d.keys();
        return Ok((0..keys.len()).map(|r| if keys.is_null(r) { NO_CODE } else { map[keys.value(r) as usize] }).collect());
    }
    let arr = cast(col, &DataType::Utf8).map_err(|e| e.to_string())?;
    let arr = arr.as_any().downcast_ref::<StringArray>().ok_or("не строка")?;
    Ok((0..arr.len()).map(|r| if arr.is_null(r) { NO_CODE } else { intern(codes, ix, arr.value(r)) }).collect())
}

/// dep_iso / arr_iso из файла (для перезаписи).
fn read_iso(path: &str) -> Result<(Vec<Option<String>>, Vec<Option<String>>), String> {
    let file = File::open(path).map_err(|e| e.to_string())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| e.to_string())?;
    let wanted: Vec<usize> = builder.schema().fields().iter().enumerate().filter(|(_, f)| f.name() == "dep_iso" || f.name() == "arr_iso").map(|(i, _)| i).collect();
    let mask = parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), wanted);
    let reader = builder.with_projection(mask).with_batch_size(1 << 16).build().map_err(|e| e.to_string())?;
    let (mut dep, mut arr) = (Vec::new(), Vec::new());
    for b in reader {
        let b = b.map_err(|e| e.to_string())?;
        dep.extend(str_col(&b, "dep_iso")?);
        arr.extend(str_col(&b, "arr_iso")?);
    }
    Ok((dep, arr))
}


/// JSON колонки seg (или raw у файлов прежнего формата) для строк rows (по возрастанию).
fn read_seg_rows(path: &str, rows: &[usize]) -> Result<Vec<Option<String>>, String> {
    let file = File::open(path).map_err(|e| e.to_string())?;
    let options = ArrowReaderOptions::new().with_offset_index_policy(PageIndexPolicy::Optional);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).map_err(|e| e.to_string())?;
    let schema = builder.schema().clone();
    let name = if schema.fields().iter().any(|f| f.name() == "seg") { "seg" } else { "raw" };
    let idx = schema.fields().iter().position(|f| f.name() == name).ok_or("нет колонки seg/raw")?;
    let mask = parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), [idx]);
    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut pos = 0usize;
    for &r in rows {
        if r > pos {
            selectors.push(RowSelector::skip(r - pos));
        }
        selectors.push(RowSelector::select(1));
        pos = r + 1;
    }
    let reader = builder
        .with_projection(mask)
        .with_row_selection(RowSelection::from(selectors))
        .with_batch_size(1 << 16)
        .build()
        .map_err(|e| e.to_string())?;
    let mut out: Vec<Option<String>> = Vec::with_capacity(rows.len());
    for b in reader {
        let b = b.map_err(|e| e.to_string())?;
        out.extend(str_col(&b, name)?);
    }
    if out.len() != rows.len() {
        return Err(format!("выборка seg: {} строк вместо {}", out.len(), rows.len()));
    }
    Ok(out)
}

fn column<'a>(b: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, String> {
    b.column_by_name(name).ok_or_else(|| format!("нет колонки {name}"))
}

pub fn str_col(b: &RecordBatch, name: &str) -> Result<Vec<Option<String>>, String> {
    let Some(col) = b.column_by_name(name) else { return Ok(vec![None; b.num_rows()]) };
    let arr = cast(col, &DataType::Utf8).map_err(|e| e.to_string())?;
    let arr = arr.as_any().downcast_ref::<StringArray>().ok_or("не строка")?;
    Ok((0..arr.len()).map(|i| if arr.is_null(i) { None } else { Some(arr.value(i).to_string()) }).collect())
}

pub fn f64_col(b: &RecordBatch, name: &str) -> Result<Vec<Option<f64>>, String> {
    let Some(col) = b.column_by_name(name) else { return Ok(vec![None; b.num_rows()]) };
    let arr = cast(col, &DataType::Float64).map_err(|e| e.to_string())?;
    let arr = arr.as_any().downcast_ref::<Float64Array>().ok_or("не число")?;
    Ok((0..arr.len()).map(|i| if arr.is_null(i) { None } else { Some(arr.value(i)) }).collect())
}

/// Колонка чисел без Option: null → default; без null — копия буфера целиком.
fn f64_vals(b: &RecordBatch, name: &str, default: f64) -> Result<Vec<f64>, String> {
    let Some(col) = b.column_by_name(name) else { return Ok(vec![default; b.num_rows()]) };
    let arr = cast(col, &DataType::Float64).map_err(|e| e.to_string())?;
    let arr = arr.as_any().downcast_ref::<Float64Array>().ok_or("не число")?;
    if arr.null_count() == 0 {
        return Ok(arr.values().to_vec());
    }
    Ok((0..arr.len()).map(|i| if arr.is_null(i) { default } else { arr.value(i) }).collect())
}

fn i64_vals(b: &RecordBatch, name: &str, default: i64) -> Result<Vec<i64>, String> {
    let Some(col) = b.column_by_name(name) else { return Ok(vec![default; b.num_rows()]) };
    let arr = cast(col, &DataType::Int64).map_err(|e| e.to_string())?;
    let arr = arr.as_any().downcast_ref::<Int64Array>().ok_or("не целое")?;
    if arr.null_count() == 0 {
        return Ok(arr.values().to_vec());
    }
    Ok((0..arr.len()).map(|i| if arr.is_null(i) { default } else { arr.value(i) }).collect())
}

pub fn i64_col(b: &RecordBatch, name: &str) -> Result<Vec<Option<i64>>, String> {
    let Some(col) = b.column_by_name(name) else { return Ok(vec![None; b.num_rows()]) };
    let arr = cast(col, &DataType::Int64).map_err(|e| e.to_string())?;
    let arr = arr.as_any().downcast_ref::<Int64Array>().ok_or("не целое")?;
    Ok((0..arr.len()).map(|i| if arr.is_null(i) { None } else { Some(arr.value(i)) }).collect())
}

pub fn bool_col(b: &RecordBatch, name: &str) -> Result<Vec<bool>, String> {
    let col = column(b, name)?;
    let arr = cast(col, &DataType::Boolean).map_err(|e| e.to_string())?;
    let arr = arr.as_any().downcast_ref::<BooleanArray>().ok_or("не bool")?;
    Ok((0..arr.len()).map(|i| !arr.is_null(i) && arr.value(i)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(o: &str, d: &str, dep: &str, price: f64) -> Arc<Ticket> {
        Arc::new(Ticket {
            origin: Some(o.into()),
            destination: Some(d.into()),
            origin_airport: Some(o.into()),
            destination_airport: Some(d.into()),
            departure_at: Some(dep.into()),
            duration: Some(300),
            price: Some(price),
            transfers: 1,
            transfer_points: Some(vec![crate::ticket::TransferPoint { code: Some("X".into()), minutes: Some(80), ..Default::default() }]),
            ..Default::default()
        })
    }

    #[test]
    fn builds_and_roundtrips_parquet() {
        let collected = vec![vec![t("MOW", "IST", "2026-11-01T10:00:00+03:00", 100.0)], vec![t("IST", "SEL", "2026-11-03T08:00:00+03:00", 200.0)]];
        let cols = FlightCols::from_collected(&collected);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols.arr_iso()[0].as_deref(), Some("2026-11-01T15:00:00"));
        assert_eq!(cols.pts_min[0], Some(80));
        assert_eq!(cols.rows(1), vec![1]);
        assert_eq!(cols.by_origin(&[0, 1]).get("MOW"), Some(&vec![0]));
        let dir = std::env::temp_dir().join(format!("fc-{}.parquet", std::process::id()));
        let path = dir.to_string_lossy().to_string();
        cols.write(&path).unwrap();
        let back = FlightCols::read(&path).unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back.dep_ord, cols.dep_ord);
        assert_eq!(back.dest(1), "SEL");
        assert_eq!(back.dest_id[1], back.code_id("SEL").unwrap());
        assert_eq!(back.arr_iso()[0].as_deref(), Some("2026-11-01T15:00:00")); // лениво из файла
        assert_eq!(back.pts_min[0], Some(80));
        back.prefetch(&[1, 0, 1]); // выборка строк: дубли и обратный порядок
        assert_eq!(back.flight(0).price(), 100.0);
        assert_eq!(back.flight(1).price(), 200.0);
        assert_eq!(back.flight(1).transfer_points.as_ref().unwrap()[0].minutes, Some(80));
        std::fs::remove_file(&path).ok();
    }
}
