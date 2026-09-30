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

use crate::dates::{naive_seconds, ordinal, parse_naive};
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
    /// Рейсы целиком (из сбора) или JSON `seg` (из файла), разбираемый лениво.
    raw: Mutex<Raw>,
    parsed: Mutex<HashMap<usize, Arc<Ticket>>>,
}

enum Raw {
    Tickets(Vec<Arc<Ticket>>),
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

    /// Из собранных рейсов по плечам.
    pub fn from_collected(collected: &[Vec<Arc<Ticket>>]) -> FlightCols {
        let n: usize = collected.iter().map(|v| v.len()).sum();
        let mut c = FlightCols::with_capacity(n);
        let mut raw = Vec::with_capacity(n);
        let (mut dep_iso, mut arr_iso) = (Vec::with_capacity(n), Vec::with_capacity(n));
        for (leg, flights) in collected.iter().enumerate() {
            for f in flights {
                let (dep, arr) = c.push(leg as i16, f);
                dep_iso.push(dep);
                arr_iso.push(arr);
                raw.push(f.clone());
            }
        }
        c.n = c.leg.len();
        c.raw = Mutex::new(Raw::Tickets(raw));
        let _ = c.iso.set((dep_iso, arr_iso));
        c
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
            raw: Mutex::new(Raw::Tickets(Vec::new())),
            parsed: Mutex::new(HashMap::new()),
        }
    }

    /// Строка рейса f; возвращает (вылет, прилёт) строками — для колонок ISO.
    fn push(&mut self, leg: i16, f: &Ticket) -> (Option<String>, Option<String>) {
        let dep = f.departure_at.clone().filter(|s| !s.is_empty());
        let arr = if dep.is_some() { f.arrival() } else { None };
        let dep_dt = dep.as_deref().and_then(parse_naive);
        let arr_dt = arr.as_deref().and_then(parse_naive);
        self.leg.push(leg);
        let (codes, ix) = (&mut self.codes, &mut self.code_ix);
        self.orig_city_id.push(intern(codes, ix, &f.origin_city().map(|s| s.to_uppercase()).unwrap_or_default()));
        self.orig_airport_id.push(intern(codes, ix, &upper(&f.origin_airport)));
        self.dest_id.push(intern(codes, ix, &f.dest_city().map(|s| s.to_uppercase()).unwrap_or_default()));
        self.dest_airport_id.push(intern(codes, ix, &upper(&f.destination_airport)));
        self.price.push(f.price());
        self.transfers.push(f.transfers);
        self.duration.push(f.duration.unwrap_or(0));
        self.hidden.push(f.hidden_city.is_some());
        self.bag_incl.push(f.baggage.as_ref().map(|b| b.included).unwrap_or(false));
        self.pts_min.push(f.min_transfer_minutes());
        self.layover.push(f.layover_minutes);
        self.dep_ts.push(dep_dt.map(naive_seconds).unwrap_or(f64::NAN));
        self.arr_ts.push(arr_dt.map(naive_seconds).unwrap_or(f64::NAN));
        self.dep_ord.push(dep_dt.map(|d| ordinal(d.date())).unwrap_or(-1));
        self.arr_ord.push(arr_dt.map(|d| ordinal(d.date())).unwrap_or(-1));
        (dep, arr)
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
                Raw::Tickets(_) => None,
            };
            path.and_then(|p| read_iso(&p).map_err(|e| eprintln!("[flightcols] {p}: dep_iso/arr_iso не прочитаны: {e}")).ok())
                .unwrap_or_else(|| (vec![None; self.n], vec![None; self.n]))
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
            if let Raw::Tickets(v) = &*raw {
                return v[i].clone();
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
        let path = match &*self.raw.lock().unwrap() {
            Raw::Lazy(p) => p.clone(),
            Raw::Tickets(_) => return,
        };
        let mut need: Vec<usize> = {
            let parsed = self.parsed.lock().unwrap();
            rows.iter().copied().filter(|r| *r < self.n && !parsed.contains_key(r)).collect()
        };
        need.sort_unstable();
        need.dedup();
        if need.is_empty() {
            return;
        }
        let jsons = read_seg_rows(&path, &need).unwrap_or_else(|e| {
            eprintln!("[flightcols] {path}: колонка seg не прочитана: {e}");
            vec![None; need.len()]
        });
        let mut parsed = self.parsed.lock().unwrap();
        for (r, json) in need.into_iter().zip(jsons) {
            let ticket: Ticket = json.as_deref().and_then(|s| serde_json::from_str(s).ok()).unwrap_or_default();
            parsed.insert(r, Arc::new(ticket));
        }
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

    fn to_batch(&self) -> Result<RecordBatch, String> {
        self.prefetch(&(0..self.n).collect::<Vec<_>>());
        let seg: Vec<Option<String>> = (0..self.n)
            .map(|i| serde_json::to_string(&self.flight(i).segment_source()).ok())
            .collect();
        let codes = |ids: &Vec<u32>| -> ArrayRef { Arc::new(StringArray::from(ids.iter().map(|&id| Some(self.code(id))).collect::<Vec<_>>())) };
        let cols: Vec<ArrayRef> = vec![
            codes(&self.orig_city_id),
            codes(&self.orig_airport_id),
            codes(&self.dest_id),
            codes(&self.dest_airport_id),
            Arc::new(StringArray::from(self.dep_iso().iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
            Arc::new(StringArray::from(self.arr_iso().iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
            Arc::new(Int16Array::from(self.leg.clone())),
            Arc::new(Float64Array::from(self.price.clone())),
            Arc::new(Int32Array::from(self.transfers.iter().map(|t| *t as i32).collect::<Vec<_>>())),
            Arc::new(Int64Array::from(self.duration.clone())),
            Arc::new(BooleanArray::from(self.hidden.clone())),
            Arc::new(BooleanArray::from(self.bag_incl.clone())),
            Arc::new(Float64Array::from(self.pts_min.iter().map(|v| v.map(|x| x as f64).unwrap_or(f64::NAN)).collect::<Vec<_>>())),
            Arc::new(Float64Array::from(self.layover.iter().map(|v| v.map(|x| x as f64).unwrap_or(f64::NAN)).collect::<Vec<_>>())),
            Arc::new(Float64Array::from(self.dep_ts.clone())),
            Arc::new(Float64Array::from(self.arr_ts.clone())),
            Arc::new(Int64Array::from(self.dep_ord.clone())),
            Arc::new(Int64Array::from(self.arr_ord.clone())),
            Arc::new(StringArray::from(seg.iter().map(|s| s.as_deref()).collect::<Vec<_>>())),
        ];
        RecordBatch::try_new(Self::schema(), cols).map_err(|e| e.to_string())
    }

    pub fn write(&self, path: &str) -> Result<(), String> {
        let batch = self.to_batch()?;
        let file = File::create(path).map_err(|e| e.to_string())?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_column_compression("seg".into(), Compression::LZ4_RAW)
            .build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).map_err(|e| e.to_string())?;
        writer.write(&batch).map_err(|e| e.to_string())?;
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
