//! Динамика цен по направлению (страница `/dynamics`): история из озера коллектора.
//!
//! Каждая повторная выборка серии — новый файл озера (`tickets/fetched=<день>/origin=X/…`),
//! поэтому «сколько стоил билет, если смотреть в разные дни» — это все файлы X→ANY (и X→Y)
//! без ценовых коридоров, чьё окно дней вылета задевает запрошенные дни. Файл = снимок
//! (момент загрузки из имени), рейс = маршрут (цепочка аэропортов, вылет, номера рейсов).
//!
//! Склад в памяти (`lakestore`) держит только последний снимок дня, поэтому история
//! читается из озера напрямую и ни на что больше не влияет. Из файла читаются только
//! нужные колонки и row group'ы (в файле окна row group на день): футер — запросом хвоста,
//! колонки — Range-запросами (ссылка и пересадки — самые тяжёлые колонки — не качаются).
//! Файлы неизменяемы, поэтому выжимка файла под направление кэшируется.
//!
//! Запрос идёт в фоне: ручка сразу отвечает `{pending, done, total}`, фронт опрашивает её
//! теми же параметрами, пока не придёт результат (кэш ответа — 10 минут).
//! Фильтры (время вылета/прилёта, пересадки, багаж, авиакомпании) — на фронте: ответ
//! компактный (рейсы + тройки «рейс × снимок → цена»).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use chrono::{NaiveDate, Utc};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::reader::{ChunkReader, Length};
use serde::Serialize;
use serde_json::{json, Value};

use crate::lakestore::{parse_file_key, read_lake_parquet, FileMeta, LakeCols};
use crate::lakesync::{lake_from_env, Lake};
use crate::segments::{booking_link, city_info};

/// Сколько дней вылета подряд можно смотреть разом (календарь на месяц).
pub const MAX_DAYS: i64 = 31;
/// Глубина истории по дню загрузки, дней.
pub const DEFAULT_HISTORY_DAYS: i64 = 60;
pub const MAX_HISTORY_DAYS: i64 = 180;
/// Потолок файлов на запрос (страховка от слишком широкого окна).
pub const MAX_FILES: usize = 6000;
const WORKERS: usize = 24;
const CACHE_TTL: Duration = Duration::from_secs(10 * 60);
const CACHE_SIZE: usize = 12;
/// Выжимки файлов в кэше: суммарно строк (≈ 250 Б на строку).
const FILE_CACHE_ROWS: usize = 600_000;
/// Хвост файла за один запрос: футер с метаданными обычно меньше.
const TAIL_BYTES: u64 = 64 * 1024;
/// Соседние диапазоны колонок с зазором меньше этого качаются одним запросом.
const RANGE_GAP: u64 = 16 * 1024;
/// Колонки схемы озера, нужные динамике (ссылка, пересадки, служебные — нет).
const COLUMNS: [&str; 16] = [
    "search_date", "origin", "destination", "origin_airport", "destination_airport", "departure_at", "arrival_at", "duration", "transfers", "airline", "flight_number", "price", "chain_json", "legs_json", "baggage_known", "baggage_included",
];

fn lake() -> Option<Arc<dyn Lake>> {
    static LAKE: OnceLock<Option<Arc<dyn Lake>>> = OnceLock::new();
    LAKE.get_or_init(lake_from_env).clone()
}

#[derive(Debug, Clone)]
pub struct DynamicsQuery {
    /// Код города или аэропорта вылета, как выбрал пользователь.
    pub origin: String,
    /// Город запроса в озере (для аэропорта — его город).
    pub origin_city: String,
    pub destination: String,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub history_days: i64,
}

impl DynamicsQuery {
    fn cache_key(&self) -> String {
        format!("{}|{}|{}|{}|{}|{}", self.origin, self.origin_city, self.destination, self.from, self.to, self.history_days)
    }
}

/// Рейс (маршрут) в ответе.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Flight {
    pub day: String,
    pub departure_at: String,
    pub arrival_at: Option<String>,
    pub duration: Option<i64>,
    pub transfers: i64,
    pub airline: Option<String>,
    /// Номера рейсов по плечам: «SU 1860», «A4 123».
    pub flights: Vec<String>,
    /// Аэропорты по цепочке: вылет, пересадки, прилёт.
    pub chain: Vec<String>,
    /// Аэропорты пересадок (середина цепочки).
    pub via: Vec<String>,
    pub origin_airport: Option<String>,
    pub destination_airport: Option<String>,
    /// Поиск на Aviasales на день вылета.
    pub link: Option<String>,
}

/// Снимок = файл озера (момент загрузки) и какие из запрошенных дней он покрыл.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Snapshot {
    pub at: String,
    pub days: Vec<String>,
}

/// Что дал один файл под направление (все дни файла — фильтр дней при сборке ответа).
pub struct FileRows {
    observed: i64,
    days: Vec<NaiveDate>,
    /// (день вылета, ключ рейса, рейс, цена, багаж: 1 включён, 0 нет, -1 неизвестно).
    rows: Vec<(NaiveDate, String, Arc<Flight>, f64, i8)>,
}

fn up(s: Option<&str>) -> String {
    s.unwrap_or("").to_uppercase()
}

fn leg_label(carrier: Option<&str>, number: Option<&str>) -> String {
    let c = carrier.unwrap_or("").trim();
    let n = number.unwrap_or("").trim();
    if n.is_empty() {
        return c.to_string();
    }
    if c.is_empty() || n.starts_with(c) { n.to_string() } else { format!("{c} {n}") }
}

/// Строки файла под направление (дни — все, что есть в файле).
fn rows_of(origin: &str, origin_city: &str, destination: &str, meta: &FileMeta, cols: &LakeCols) -> FileRows {
    let mut rows = Vec::new();
    let origin_is_city = origin == origin_city;
    for r in 0..cols.n {
        let Some(day) = cols.dep_day(r) else { continue };
        let t = cols.ticket(r);
        let (Some(price), Some(dep)) = (t.price, t.departure_at.clone().filter(|s| !s.is_empty())) else { continue };
        let dest_ok = up(t.destination.as_deref()) == destination || up(t.destination_airport.as_deref()) == destination;
        let origin_ok = (origin_is_city && up(t.origin.as_deref()) == origin) || up(t.origin_airport.as_deref()) == origin;
        if !dest_ok || !origin_ok {
            continue;
        }
        let flights: Vec<String> = if t.legs.is_empty() {
            vec![leg_label(t.airline.as_deref(), t.flight_number.as_deref())]
        } else {
            t.legs.iter().map(|l| leg_label(l.carrier.as_deref(), l.flight_number.as_deref())).collect()
        };
        let chain: Vec<String> = if t.chain.is_empty() {
            [t.origin_airport.clone(), t.destination_airport.clone()].into_iter().flatten().collect()
        } else {
            t.chain.clone()
        };
        let via: Vec<String> = if chain.len() > 2 { chain[1..chain.len() - 1].to_vec() } else { Vec::new() };
        let key = format!("{}|{}|{}", chain.join(">"), dep, flights.join(","));
        let bag = if cols.known[r] { i8::from(cols.included[r]) } else { -1 };
        let link = Some(booking_link(t.origin.as_deref().unwrap_or(origin_city), t.destination.as_deref().unwrap_or(destination), &dep));
        let flight = Flight {
            day: day.to_string(),
            departure_at: dep,
            arrival_at: t.arrival(),
            duration: t.duration,
            transfers: t.transfers,
            airline: t.airline.clone(),
            flights,
            chain,
            via,
            origin_airport: t.origin_airport.clone(),
            destination_airport: t.destination_airport.clone(),
            link,
        };
        rows.push((day, key, Arc::new(flight), price, bag));
    }
    FileRows { observed: meta.observed.timestamp(), days: meta.days(), rows }
}

// ---------------------------------------------------------------- чтение по частям

/// Файл, от которого скачаны только некоторые диапазоны байт.
struct Sparse {
    len: u64,
    parts: Vec<(u64, Bytes)>,
}

impl Sparse {
    /// Добавляет диапазон и склеивает соседние/перекрывающиеся.
    fn add(&mut self, start: u64, data: Bytes) {
        self.parts.push((start, data));
        self.parts.sort_by_key(|p| p.0);
        let mut out: Vec<(u64, Bytes)> = Vec::new();
        for (s, b) in self.parts.drain(..) {
            if let Some((ls, lb)) = out.last_mut() {
                let lend = *ls + lb.len() as u64;
                if s <= lend {
                    let end = s + b.len() as u64;
                    if end > lend {
                        let mut v = Vec::with_capacity((end - *ls) as usize);
                        v.extend_from_slice(lb);
                        v.extend_from_slice(&b[(lend - s) as usize..]);
                        *lb = Bytes::from(v);
                    }
                    continue;
                }
            }
            out.push((s, b));
        }
        self.parts = out;
    }

    fn covers(&self, start: u64, len: u64) -> bool {
        self.parts.iter().any(|(s, b)| *s <= start && start + len <= s + b.len() as u64)
    }
}

impl Length for Sparse {
    fn len(&self) -> u64 {
        self.len
    }
}

impl ChunkReader for Sparse {
    type T = std::io::Cursor<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let (s, b) = self.parts.iter().find(|(s, b)| *s <= start && start < s + b.len() as u64).ok_or_else(|| ParquetError::General(format!("байты {start} не скачаны")))?;
        Ok(std::io::Cursor::new(b.slice((start - s) as usize..)))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let (s, b) = self.parts.iter().find(|(s, b)| *s <= start && start + length as u64 <= s + b.len() as u64).ok_or_else(|| ParquetError::General(format!("байты {start}+{length} не скачаны")))?;
        let off = (start - s) as usize;
        Ok(b.slice(off..off + length))
    }
}

/// Можно ли пропустить row group по статистике дня вылета.
fn row_group_outside(meta: &ParquetMetaData, rg: usize, day_col: Option<usize>, from: NaiveDate, to: NaiveDate) -> bool {
    let Some(c) = day_col else { return false };
    let Some(st) = meta.row_group(rg).column(c).statistics() else { return false };
    let day = |b: Option<&[u8]>| b.and_then(|b| std::str::from_utf8(b).ok()).and_then(|s| NaiveDate::parse_from_str(s.get(..10)?, "%Y-%m-%d").ok());
    match (day(st.min_bytes_opt()), day(st.max_bytes_opt())) {
        (Some(lo), Some(hi)) => hi < from || lo > to,
        _ => false,
    }
}

/// Нужные колонки нужных row group'ов файла: хвост → метаданные → Range по колонкам.
/// Любая неожиданность — файл целиком (`read_lake_parquet`).
pub fn read_projected(lake: &dyn Lake, key: &str, from: NaiveDate, to: NaiveDate) -> Result<LakeCols, String> {
    let (tail, len) = lake.get_tail(key, TAIL_BYTES)?;
    if tail.len() as u64 >= len {
        return read_lake_parquet(tail);
    }
    let mut sp = Sparse { len, parts: vec![(len - tail.len() as u64, tail)] };
    let meta = match ParquetMetaDataReader::new().parse_and_finish(&sp) {
        Ok(m) => m,
        Err(_) => return lake.get(key).and_then(read_lake_parquet),
    };
    let schema = meta.file_metadata().schema_descr();
    let leaves: Vec<usize> = (0..schema.num_columns()).filter(|&i| COLUMNS.contains(&schema.column(i).name())).collect();
    let day_col = (0..schema.num_columns()).find(|&i| schema.column(i).name() == "search_date");
    let groups: Vec<usize> = (0..meta.num_row_groups()).filter(|&rg| !row_group_outside(&meta, rg, day_col, from, to)).collect();
    if groups.is_empty() {
        return Ok(LakeCols::empty());
    }
    let mut ranges: Vec<(u64, u64)> = groups.iter().flat_map(|&rg| leaves.iter().map(move |&c| (rg, c))).map(|(rg, c)| meta.row_group(rg).column(c).byte_range()).collect();
    ranges.sort();
    let mut merged: Vec<(u64, u64)> = Vec::new();
    for (s, l) in ranges {
        match merged.last_mut() {
            Some((ms, ml)) if s <= *ms + *ml + RANGE_GAP => *ml = (*ml).max(s + l - *ms),
            _ => merged.push((s, l)),
        }
    }
    for (s, l) in merged {
        if !sp.covers(s, l) {
            let data = lake.get_range(key, s, l)?;
            sp.add(s, data);
        }
    }
    let arrow_meta = ArrowReaderMetadata::try_new(Arc::new(meta), ArrowReaderOptions::default()).map_err(|e| format!("parquet: {e}"))?;
    let mask = ProjectionMask::leaves(arrow_meta.metadata().file_metadata().schema_descr(), leaves);
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(sp, arrow_meta).with_projection(mask).with_row_groups(groups).with_batch_size(8192).build().map_err(|e| format!("parquet: {e}"))?;
    let mut out: Option<LakeCols> = None;
    for batch in reader {
        let cols = LakeCols::from_batch(&batch.map_err(|e| format!("parquet: {e}"))?)?;
        match &mut out {
            Some(o) => o.append(cols),
            None => out = Some(cols),
        }
    }
    Ok(out.unwrap_or_else(LakeCols::empty))
}

// ---------------------------------------------------------------- кэш выжимок файлов

#[derive(Default)]
struct FileCache {
    map: HashMap<String, Arc<FileRows>>,
    order: VecDeque<String>,
    rows: usize,
}

fn file_cache() -> &'static Mutex<FileCache> {
    static C: OnceLock<Mutex<FileCache>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(FileCache::default()))
}

fn file_cache_key(q: &DynamicsQuery, key: &str) -> String {
    format!("{}|{}|{}|{}|{}", q.origin, q.destination, q.from, q.to, key)
}

fn cache_get(k: &str) -> Option<Arc<FileRows>> {
    file_cache().lock().unwrap().map.get(k).cloned()
}

fn cache_put(k: String, v: Arc<FileRows>) {
    let mut c = file_cache().lock().unwrap();
    if c.map.contains_key(&k) {
        return;
    }
    c.rows += v.rows.len() + 1;
    c.map.insert(k.clone(), v);
    c.order.push_back(k);
    while c.rows > FILE_CACHE_ROWS {
        let Some(old) = c.order.pop_front() else { break };
        if let Some(v) = c.map.remove(&old) {
            c.rows -= v.rows.len() + 1;
        }
    }
}

// ---------------------------------------------------------------- сбор ответа

/// Файлы озера под запрос: X→ANY и X→Y без параметров, окно задевает [from, to].
pub fn pick_files(q: &DynamicsQuery, keys: &[String]) -> Vec<(String, FileMeta)> {
    let mut out: Vec<(String, FileMeta)> = keys
        .iter()
        .filter_map(|k| parse_file_key(k).map(|m| (k.clone(), m)))
        .filter(|(_, m)| {
            m.origin.as_deref() == Some(q.origin_city.as_str())
                && m.params_key.is_empty()
                && m.destination.as_deref().map(|d| d == q.destination).unwrap_or(true)
                && m.day <= q.to
                && m.day_to >= q.from
        })
        .collect();
    out.sort_by_key(|(_, m)| m.observed);
    out
}

/// Ключи файлов города вылета за последние `history_days` дней загрузки.
fn list_keys(lake: &dyn Lake, q: &DynamicsQuery) -> Result<Vec<String>, String> {
    let today = Utc::now().date_naive();
    // загрузка не раньше чем за history_days до сегодня и не позже последнего дня вылета
    let last = today.min(q.to);
    let first = (today - chrono::Duration::days(q.history_days)).min(last);
    let days: Vec<NaiveDate> = first.iter_days().take_while(|d| *d <= last).collect();
    let next = AtomicUsize::new(0);
    let out = Mutex::new((Vec::new(), None::<String>));
    std::thread::scope(|s| {
        for _ in 0..WORKERS.min(days.len().max(1)) {
            s.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                let Some(d) = days.get(i) else { break };
                let res = lake.list(&format!("tickets/fetched={d}/origin={}/", q.origin_city));
                let mut o = out.lock().unwrap();
                match res {
                    Ok(keys) => o.0.extend(keys),
                    Err(e) => o.1 = Some(e),
                }
            });
        }
    });
    let (keys, err) = out.into_inner().unwrap();
    match err {
        Some(e) if keys.is_empty() => Err(e),
        _ => Ok(keys),
    }
}

/// Собирает ответ из выжимок файлов (снимки по времени, рейсы по вылету).
fn assemble(q: &DynamicsQuery, files: &[Arc<FileRows>], failed: usize) -> Value {
    let in_range = |d: &NaiveDate| *d >= q.from && *d <= q.to;
    // снимки: один на момент загрузки (два файла одной секунды склеиваются)
    let mut snap_days: BTreeMap<i64, Vec<NaiveDate>> = BTreeMap::new();
    for f in files {
        let e = snap_days.entry(f.observed).or_default();
        e.extend(f.days.iter().copied().filter(in_range));
        e.sort();
        e.dedup();
    }
    snap_days.retain(|_, d| !d.is_empty());
    let snap_ix: HashMap<i64, usize> = snap_days.keys().enumerate().map(|(i, t)| (*t, i)).collect();
    let snapshots: Vec<Snapshot> = snap_days
        .iter()
        .map(|(t, days)| Snapshot {
            at: chrono::DateTime::from_timestamp(*t, 0).unwrap_or_default().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            days: days.iter().map(|d| d.to_string()).collect(),
        })
        .collect();
    // рейсы: описание — из самого свежего снимка, цена — минимум на (рейс, снимок, багаж)
    let mut flights: HashMap<&str, (i64, &Arc<Flight>)> = HashMap::new();
    let mut prices: HashMap<(&str, usize, i8), f64> = HashMap::new();
    for f in files {
        let Some(&si) = snap_ix.get(&f.observed) else { continue };
        for (day, key, flight, price, bag) in &f.rows {
            if !in_range(day) {
                continue;
            }
            let p = prices.entry((key.as_str(), si, *bag)).or_insert(f64::INFINITY);
            *p = p.min(*price);
            let e = flights.entry(key.as_str()).or_insert((f.observed, flight));
            if e.0 <= f.observed {
                *e = (f.observed, flight);
            }
        }
    }
    let mut order: Vec<(&str, &Arc<Flight>)> = flights.into_iter().map(|(k, (_, f))| (k, f)).collect();
    order.sort_by(|a, b| a.1.departure_at.cmp(&b.1.departure_at).then_with(|| a.1.transfers.cmp(&b.1.transfers)).then_with(|| a.0.cmp(b.0)));
    let flight_ix: HashMap<&str, usize> = order.iter().enumerate().map(|(i, (k, _))| (*k, i)).collect();
    let mut obs: Vec<(usize, usize, f64, i8)> = prices.iter().map(|((k, s, b), p)| (flight_ix[k], *s, *p, *b)).collect();
    obs.sort_by(|a, b| (a.0, a.1, a.3).cmp(&(b.0, b.1, b.3)));
    let (oc, dc) = (city_info(&q.origin), city_info(&q.destination));
    json!({
        "origin": q.origin,
        "origin_city": q.origin_city,
        "destination": q.destination,
        "origin_name": oc.city,
        "destination_name": dc.city,
        "from": q.from.to_string(),
        "to": q.to.to_string(),
        "history_days": q.history_days,
        "files": snapshots.len(),
        "files_failed": failed,
        "snapshots": snapshots,
        "flights": order.into_iter().map(|(_, f)| f.as_ref()).collect::<Vec<_>>(),
        "obs": obs.into_iter().map(|(f, s, p, b)| json!([f, s, p, b])).collect::<Vec<_>>(),
    })
}

/// Ход фонового запроса.
#[derive(Default)]
pub struct Progress {
    pub stage: String,
    pub done: AtomicUsize,
    pub total: AtomicUsize,
    pub cached: AtomicUsize,
}

/// История цен направления из озера.
pub fn run(lake: &dyn Lake, q: &DynamicsQuery, progress: &Progress) -> Result<Value, String> {
    let keys = list_keys(lake, q)?;
    let files = pick_files(q, &keys);
    if files.len() > MAX_FILES {
        return Err(format!("Слишком много снимков ({}): сузьте дни вылета или глубину истории.", files.len()));
    }
    progress.total.store(files.len(), Ordering::Relaxed);
    let next = AtomicUsize::new(0);
    let results: Mutex<(Vec<Arc<FileRows>>, usize)> = Mutex::new((Vec::new(), 0));
    std::thread::scope(|s| {
        for _ in 0..WORKERS.min(files.len().max(1)) {
            s.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                let Some((key, meta)) = files.get(i) else { break };
                let ck = file_cache_key(q, key);
                let res = match cache_get(&ck) {
                    Some(v) => {
                        progress.cached.fetch_add(1, Ordering::Relaxed);
                        Ok(v)
                    }
                    None => read_projected(lake, key, q.from, q.to).map(|cols| {
                        let v = Arc::new(rows_of(&q.origin, &q.origin_city, &q.destination, meta, &cols));
                        cache_put(ck, v.clone());
                        v
                    }),
                };
                progress.done.fetch_add(1, Ordering::Relaxed);
                let mut r = results.lock().unwrap();
                match res {
                    Ok(rows) => r.0.push(rows),
                    Err(e) => {
                        eprintln!("[dynamics] {key}: {e}");
                        r.1 += 1;
                    }
                }
            });
        }
    });
    let (rows, failed) = results.into_inner().unwrap();
    Ok(assemble(q, &rows, failed))
}

// ---------------------------------------------------------------- ручка: фон + кэш

type Cache = Mutex<Vec<(String, Instant, Arc<Value>)>>;

fn cache() -> &'static Cache {
    static CACHE: OnceLock<Cache> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(Vec::new()))
}

fn running() -> &'static Mutex<HashMap<String, Arc<Progress>>> {
    static R: OnceLock<Mutex<HashMap<String, Arc<Progress>>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cached(key: &str) -> Option<Arc<Value>> {
    let mut c = cache().lock().unwrap();
    c.retain(|(_, t, _)| t.elapsed() < CACHE_TTL);
    c.iter().find(|(k, _, _)| k == key).map(|(_, _, v)| v.clone())
}

fn pending(p: &Progress) -> Value {
    json!({"pending": true, "done": p.done.load(Ordering::Relaxed), "total": p.total.load(Ordering::Relaxed), "cached": p.cached.load(Ordering::Relaxed)})
}

/// Ответ ручки `/api/dynamics`: результат (из кэша на 10 минут), `{pending, done, total}`
/// пока считается в фоне, или `{error}` — понятный текст.
pub fn handle(q: DynamicsQuery) -> Value {
    let key = q.cache_key();
    if let Some(v) = cached(&key) {
        return (*v).clone();
    }
    let Some(lake) = lake() else {
        return json!({"error": "Озеро билетов не подключено (нет S3_* / LAKE_LOCAL_ROOT) — истории цен нет."});
    };
    let mut run_map = running().lock().unwrap();
    if let Some(p) = run_map.get(&key) {
        return pending(p);
    }
    // результат мог появиться, пока ждали замок
    if let Some(v) = cached(&key) {
        return (*v).clone();
    }
    let progress = Arc::new(Progress::default());
    run_map.insert(key.clone(), progress.clone());
    drop(run_map);
    let p2 = progress.clone();
    std::thread::Builder::new()
        .name("dynamics".into())
        .spawn(move || {
            let t0 = Instant::now();
            let v = match run(lake.as_ref(), &q, &p2) {
                Ok(mut v) => {
                    v["seconds"] = json!((t0.elapsed().as_secs_f64() * 10.0).round() / 10.0);
                    println!("[dynamics] {}→{} {}..{}: снимков {}, файлов {} (из кэша {}), рейсов {} — {:.1} с", q.origin, q.destination, q.from, q.to, v["files"], p2.total.load(Ordering::Relaxed), p2.cached.load(Ordering::Relaxed), v["flights"].as_array().map(|a| a.len()).unwrap_or(0), t0.elapsed().as_secs_f64());
                    v
                }
                Err(e) => json!({"error": e}),
            };
            {
                let mut c = cache().lock().unwrap();
                // ошибку держим недолго: следующая попытка через минуту
                let at = if v.get("error").is_some() { Instant::now() - CACHE_TTL + Duration::from_secs(60) } else { Instant::now() };
                c.push((key.clone(), at, Arc::new(v)));
                while c.len() > CACHE_SIZE {
                    c.remove(0);
                }
            }
            running().lock().unwrap().remove(&key);
        })
        .expect("dynamics thread");
    pending(&progress)
}

/// Синхронно — для тестов и скриптов (ждёт фон).
pub fn handle_wait(q: DynamicsQuery) -> Value {
    loop {
        let v = handle(q.clone());
        if v.get("pending").is_none() {
            return v;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lakesync::LocalLake;
    use crate::lakestore::tests::ticket;
    use parquet::arrow::ArrowWriter;

    fn write_file(root: &std::path::Path, key: &str, tickets: &[crate::ticket::Ticket]) {
        let ipc = crate::collector::testing::lake_ipc(tickets);
        let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(ipc), None).unwrap();
        let schema = reader.schema();
        let path = root.join(key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut w = ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
        for b in reader {
            w.write(&b.unwrap()).unwrap();
        }
        w.close().unwrap();
    }

    fn q(origin: &str, city: &str, dest: &str, day: NaiveDate) -> DynamicsQuery {
        DynamicsQuery { origin: origin.into(), origin_city: city.into(), destination: dest.into(), from: day, to: day, history_days: 30 }
    }

    #[test]
    fn history_of_one_flight_across_snapshots() {
        let root = std::env::temp_dir().join(format!("dyn-{}", uuid::Uuid::new_v4()));
        let today = Utc::now().date_naive();
        let day = today + chrono::Duration::days(20);
        let ds = day.to_string();
        let f1 = today - chrono::Duration::days(5);
        let f2 = today - chrono::Duration::days(2);
        let mut late = ticket("MOW", "EVN", &ds, 15000.0);
        late.departure_at = Some(format!("{ds}T18:00:00+03:00"));
        late.legs[0].departure_at = late.departure_at.clone();
        let mut bag = ticket("MOW", "EVN", &ds, 12500.0);
        bag.baggage = Some(crate::ticket::Baggage { known: true, included: true, pieces: Some(1), kg: Some(23) });
        // снимок 1: рейс 10:00 за 10 000 и 11 000 (два агента), вечерний, чужое направление
        write_file(&root, &format!("tickets/fetched={f1}/origin=MOW/MOW-ANY__{ds}..{}__08-00-00Z.parquet", day + chrono::Duration::days(1)), &[ticket("MOW", "EVN", &ds, 10000.0), ticket("MOW", "EVN", &ds, 11000.0), late, ticket("MOW", "IST", &ds, 5000.0)]);
        // снимок 2: подорожал, плюс тариф с багажом; вечернего уже нет
        write_file(&root, &format!("tickets/fetched={f2}/origin=MOW/MOW-ANY__{ds}__09-30-00Z.parquet"), &[ticket("MOW", "EVN", &ds, 12000.0), bag]);
        // мимо: коридор цен, другой город, другой день
        write_file(&root, &format!("tickets/fetched={f2}/origin=MOW/MOW-ANY__{ds}__min=1,max=99999__10-00-00Z.parquet"), &[ticket("MOW", "EVN", &ds, 1.0)]);
        write_file(&root, &format!("tickets/fetched={f2}/origin=LED/LED-ANY__{ds}__10-00-00Z.parquet"), &[ticket("LED", "EVN", &ds, 1.0)]);
        let other = (day + chrono::Duration::days(3)).to_string();
        write_file(&root, &format!("tickets/fetched={f2}/origin=MOW/MOW-ANY__{other}__10-00-00Z.parquet"), &[ticket("MOW", "EVN", &other, 1.0)]);

        let lake = LocalLake::new(&root.to_string_lossy());
        let v = run(&lake, &q("MOW", "MOW", "EVN", day), &Progress::default()).unwrap();
        assert_eq!(v["files"], 2, "{v}");
        let snaps = v["snapshots"].as_array().unwrap();
        assert_eq!(snaps[0]["at"], format!("{f1}T08:00:00Z"));
        assert_eq!(snaps[0]["days"], json!([ds]));
        let flights = v["flights"].as_array().unwrap();
        assert_eq!(flights.len(), 2);
        assert_eq!(flights[0]["flights"], json!(["SU 100"]));
        assert_eq!(flights[0]["chain"], json!(["MOWA", "ISTA", "EVNA"]));
        assert!(flights[1]["departure_at"].as_str().unwrap().contains("T18:00"));
        // рейс 10:00: снимок 0 — минимум 10 000 без багажа; снимок 1 — 12 000 и 12 500 с багажом
        assert_eq!(v["obs"], json!([[0, 0, 10000.0, 0], [0, 1, 12000.0, 0], [0, 1, 12500.0, 1], [1, 0, 15000.0, 0]]));

        // аэропорт вылета: город берётся из запроса, совпадение — по аэропорту
        let v = run(&lake, &q("MOWA", "MOW", "EVN", day), &Progress::default()).unwrap();
        assert_eq!(v["flights"].as_array().unwrap().len(), 2);
        let v = run(&lake, &q("SVO", "MOW", "EVN", day), &Progress::default()).unwrap();
        assert_eq!(v["flights"].as_array().unwrap().len(), 0);
        assert_eq!(v["files"], 2);
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Озеро, считающее скачанные байты.
    struct Counting(LocalLake, AtomicUsize);

    impl Lake for Counting {
        fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
            self.0.list(prefix)
        }
        fn get(&self, key: &str) -> Result<Bytes, String> {
            let b = self.0.get(key)?;
            self.1.fetch_add(b.len(), Ordering::Relaxed);
            Ok(b)
        }
        fn describe(&self) -> String {
            "counting".into()
        }
        fn get_tail(&self, key: &str, n: u64) -> Result<(Bytes, u64), String> {
            let r = self.0.get_tail(key, n)?;
            self.1.fetch_add(r.0.len(), Ordering::Relaxed);
            Ok(r)
        }
        fn get_range(&self, key: &str, start: u64, len: u64) -> Result<Bytes, String> {
            let b = self.0.get_range(key, start, len)?;
            self.1.fetch_add(b.len(), Ordering::Relaxed);
            Ok(b)
        }
    }

    #[test]
    fn projected_read_matches_full_read() {
        let root = std::env::temp_dir().join(format!("dyn-big-{}", uuid::Uuid::new_v4()));
        let key = "tickets/fetched=2026-10-01/origin=MOW/MOW-ANY__2026-11-01..2026-11-03__08-00-00Z.parquet";
        // окно на три дня, row group на день (как пишет коллектор), длинные ссылки
        let path = root.join(key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut w: Option<ArrowWriter<std::fs::File>> = None;
        for day in ["2026-11-01", "2026-11-02", "2026-11-03"] {
            let ts: Vec<_> = (0..1500)
                .map(|i| {
                    let mut t = ticket("MOW", if i % 3 == 0 { "EVN" } else { "IST" }, day, 5000.0 + i as f64);
                    t.link = Some(format!("/search/MOW{day}?t={}", uuid::Uuid::new_v4().simple().to_string().repeat(6)));
                    t
                })
                .collect();
            let ipc = crate::collector::testing::lake_ipc(&ts);
            let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(ipc), None).unwrap();
            let schema = reader.schema();
            let wr = w.get_or_insert_with(|| ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap());
            for b in reader {
                wr.write(&b.unwrap()).unwrap();
            }
            wr.flush().unwrap();
        }
        w.unwrap().close().unwrap();
        let size = std::fs::metadata(&path).unwrap().len() as usize;
        let lake = Counting(LocalLake::new(&root.to_string_lossy()), AtomicUsize::new(0));
        let d = |s: &str| NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
        let got = read_projected(&lake, key, d("2026-11-02"), d("2026-11-02")).unwrap();
        let fetched = lake.1.load(Ordering::Relaxed);
        assert!(fetched * 3 < size, "скачано {fetched} из {size}");
        let full = read_lake_parquet(lake.0.get(key).unwrap()).unwrap();
        let want: Vec<usize> = (0..full.n).filter(|&r| full.dep_day(r) == Some(d("2026-11-02"))).collect();
        assert_eq!(got.n, want.len());
        for (i, &r) in want.iter().enumerate() {
            let (a, b) = (got.ticket(i), full.ticket(r));
            assert_eq!((a.destination, a.price, a.legs, a.chain, a.departure_at), (b.destination, b.price, b.legs, b.chain, b.departure_at));
            assert_eq!((got.known[i], got.included[i]), (full.known[r], full.included[r]));
        }
        // дни вне окна — ни одной row group
        assert_eq!(read_projected(&lake, key, d("2026-11-05"), d("2026-11-06")).unwrap().n, 0);
        // и через всю ручку: рейсы EVN на 2 ноября
        let q = DynamicsQuery { origin: "MOW".into(), origin_city: "MOW".into(), destination: "EVN".into(), from: d("2026-11-02"), to: d("2026-11-02"), history_days: 180 };
        let rows = rows_of(&q.origin, &q.origin_city, &q.destination, &parse_file_key(key).unwrap(), &got);
        assert_eq!(rows.rows.len(), 500);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn picks_only_plain_series_of_origin() {
        let day = NaiveDate::from_ymd_opt(2026, 10, 30).unwrap();
        let keys: Vec<String> = [
            "tickets/fetched=2026-10-01/origin=MOW/MOW-ANY__2026-10-29..2026-10-31__08-00-00Z.parquet",
            "tickets/fetched=2026-10-01/origin=MOW/MOW-EVN__2026-10-30__08-00-01Z.parquet",
            "tickets/fetched=2026-10-01/origin=MOW/MOW-IST__2026-10-30__08-00-02Z.parquet",
            "tickets/fetched=2026-10-01/origin=MOW/MOW-ANY__2026-10-31__08-00-03Z.parquet",
            "tickets/fetched=2026-10-01/origin=MOW/MOW-ANY__2026-10-30__direct=1__08-00-04Z.parquet",
            "tickets/fetched=2026-10-01/origin=ANY/ANY-EVN__2026-10-30__08-00-05Z.parquet",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let got: Vec<String> = pick_files(&q("MOW", "MOW", "EVN", day), &keys).into_iter().map(|(k, _)| k).collect();
        assert_eq!(got, keys[..2].to_vec());
    }
}
