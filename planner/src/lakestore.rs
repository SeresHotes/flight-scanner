//! Склад билетов в памяти планировщика (docs/TICKETS.md): текущее состояние серий X→ANY
//! озера коллектора — блок на серию «город вылета × день». Блок держит горячие колонки
//! числами (город вылета и прилёта номерами словаря, цена) и холодную часть — все поля
//! билета в схеме озера, по колонкам, сжатые zstd; разжимается только блок, попавший в
//! выборку, и строки разбираются только выбранные. Новая серия (по моменту загрузки из
//! имени файла) атомарно заменяет блок дня; дни раньше вчера выбрасываются.
//!
//! Наполнение — `lakesync` (файлы озера напрямую из S3 и снапшот на диске).

use std::collections::{BTreeMap, HashMap};
use std::io::{BufReader, BufWriter, Read, Write};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use regex::Regex;
use rustc_hash::FxHashMap;
use serde_json::{json, Value};

use crate::collect::CollectError;
use crate::flightcols::{bool_col, f64_col, i64_col, str_col};
use crate::ticket::{Baggage, Leg, Ticket, TransferPoint};
use crate::flightcols::{ts_ord, FlightSrc, Fl, JobCodes, Tp};
use crate::tickets::{TicketStore, STORE_MAX_ROWS};

// ---------------------------------------------------------------- ключи файлов озера

/// Что известно о серии по имени файла (`collector/lake.series_file_key`).
#[derive(Debug, Clone, PartialEq)]
pub struct FileMeta {
    /// Город запроса (None — ANY).
    pub origin: Option<String>,
    pub destination: Option<String>,
    pub day: NaiveDate,
    pub day_to: NaiveDate,
    pub params_key: String,
    pub observed: DateTime<Utc>,
}

impl FileMeta {
    /// Дни вылета серии (окно включительно).
    pub fn days(&self) -> Vec<NaiveDate> {
        self.day.iter_days().take_while(|d| *d <= self.day_to).collect()
    }

    /// Серия склада: X→ANY без параметров (покрытие всех городов краулером). Файлы ANY→Y,
    /// A→B и с ценовыми коридорами (запросы приложения) в склад не идут.
    pub fn store_origin(&self) -> Option<&str> {
        self.origin.as_deref().filter(|_| self.destination.is_none() && self.params_key.is_empty())
    }
}

fn re_new() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^tickets/fetched=(?P<date>\d{4}-\d{2}-\d{2})/origin=[^/_-]+/(?P<origin>[^/_-]+)-(?P<dest>[^/_-]+)__(?P<day>\d{4}-\d{2}-\d{2})(?:\.\.(?P<day_to>\d{4}-\d{2}-\d{2}))?(?:__(?P<params>[^_]+))?__(?P<time>\d{2}-\d{2}-\d{2})Z\.parquet$").unwrap()
    })
}

fn re_legacy() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^tickets/date=(?P<day>\d{4}-\d{2}-\d{2})/origin=[^/_-]+/(?P<origin>[^/_-]+)-(?P<dest>[^/_-]+)(?:__(?P<params>[^_]+))?__(?P<stamp>\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})Z\.parquet$").unwrap()
    })
}

fn any_none(code: &str) -> Option<String> {
    if code == "ANY" { None } else { Some(code.to_string()) }
}

fn ymd(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}

/// Разбор ключа файла озера: `tickets/fetched=<день загрузки>/origin=<город>/<A>-<B>__<день>[..<день>][__<параметры>]__<HH-MM-SS>Z.parquet`
/// и прежняя раскладка `tickets/date=<день>/origin=X/<A>-<B>[__<параметры>]__<день>T<HH-MM-SS>Z.parquet`
/// (окно — параметром `to=`). None — файл не по схеме.
pub fn parse_file_key(key: &str) -> Option<FileMeta> {
    let params_of = |p: Option<&str>| -> Vec<String> { p.map(|p| p.split(',').filter(|s| !s.is_empty()).map(String::from).collect()).unwrap_or_default() };
    if let Some(m) = re_new().captures(key) {
        let day = ymd(&m["day"])?;
        let day_to = m.name("day_to").and_then(|s| ymd(s.as_str())).unwrap_or(day);
        let observed = chrono::NaiveDateTime::parse_from_str(&format!("{}T{}", &m["date"], &m["time"]), "%Y-%m-%dT%H-%M-%S").ok()?.and_utc();
        let params = params_of(m.name("params").map(|p| p.as_str()));
        return Some(FileMeta { origin: any_none(&m["origin"]), destination: any_none(&m["dest"]), day, day_to, params_key: params.join("&"), observed });
    }
    let m = re_legacy().captures(key)?;
    let day = ymd(&m["day"])?;
    let observed = chrono::NaiveDateTime::parse_from_str(&m["stamp"], "%Y-%m-%dT%H-%M-%S").ok()?.and_utc();
    let params = params_of(m.name("params").map(|p| p.as_str()));
    let day_to = params.iter().find_map(|p| p.strip_prefix("to=")).and_then(ymd).unwrap_or(day);
    let params: Vec<String> = params.into_iter().filter(|p| !p.starts_with("to=")).collect();
    Some(FileMeta { origin: any_none(&m["origin"]), destination: any_none(&m["dest"]), day, day_to, params_key: params.join("&"), observed })
}

// ---------------------------------------------------------------- колонки схемы озера

/// Строковые поля билета в схеме озера (`collector/lake.SCHEMA`), которые нужны `Ticket`.
const STR_FIELDS: [&str; 18] = [
    "search_origin", "search_destination", "search_date", "origin", "destination", "origin_airport", "destination_airport", "departure_at", "arrival_at", "airline", "flight_number", "currency", "link", "baggage_code", "source", "chain_json", "legs_json", "transfer_points_json",
];
const INT_FIELDS: [&str; 5] = ["duration", "duration_to", "transfers", "baggage_pieces", "baggage_kg"];
const S_SEARCH_DATE: usize = 2;
const S_ORIGIN: usize = 3;
const S_DEST: usize = 4;
const S_ORIGIN_AP: usize = 5;
const S_DEST_AP: usize = 6;
const S_DEPARTURE: usize = 7;

/// Билеты в схеме озера по колонкам (ровно то, что разбирает `tickets_from_batch`).
#[derive(Debug, Default, Clone, PartialEq)]
pub struct LakeCols {
    pub n: usize,
    pub strs: Vec<Vec<Option<String>>>,
    pub ints: Vec<Vec<Option<i64>>>,
    pub price: Vec<Option<f64>>,
    pub known: Vec<bool>,
    pub included: Vec<bool>,
}

impl LakeCols {
    pub fn from_batch(b: &RecordBatch) -> Result<LakeCols, String> {
        let n = b.num_rows();
        let strs = STR_FIELDS.iter().map(|f| str_col(b, f)).collect::<Result<Vec<_>, _>>()?;
        let ints = INT_FIELDS.iter().map(|f| i64_col(b, f)).collect::<Result<Vec<_>, _>>()?;
        let bl = |name: &str| -> Result<Vec<bool>, String> { if b.column_by_name(name).is_some() { bool_col(b, name) } else { Ok(vec![false; n]) } };
        Ok(LakeCols { n, strs, ints, price: f64_col(b, "price")?, known: bl("baggage_known")?, included: bl("baggage_included")? })
    }

    /// Билеты → колонки тем же путём, что файлы озера (IPC в схеме озера); тесты и стенды.
    pub fn from_tickets(tickets: &[Ticket]) -> LakeCols {
        let ipc = crate::collector::testing::lake_ipc(tickets);
        let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(ipc), None).expect("ipc");
        let mut out = LakeCols::empty();
        for b in reader {
            out.append(LakeCols::from_batch(&b.expect("ipc")).expect("lake cols"));
        }
        out
    }

    pub fn empty() -> LakeCols {
        LakeCols { n: 0, strs: vec![Vec::new(); STR_FIELDS.len()], ints: vec![Vec::new(); INT_FIELDS.len()], ..Default::default() }
    }

    pub fn append(&mut self, other: LakeCols) {
        self.n += other.n;
        for (a, b) in self.strs.iter_mut().zip(other.strs) {
            a.extend(b);
        }
        for (a, b) in self.ints.iter_mut().zip(other.ints) {
            a.extend(b);
        }
        self.price.extend(other.price);
        self.known.extend(other.known);
        self.included.extend(other.included);
    }

    fn s(&self, field: usize, r: usize) -> Option<&str> {
        self.strs[field][r].as_deref()
    }

    /// День вылета серии: search_date, иначе дата из departure_at.
    pub fn dep_day(&self, r: usize) -> Option<NaiveDate> {
        let s = self.s(S_SEARCH_DATE, r).filter(|s| s.len() >= 10).or(self.s(S_DEPARTURE, r).filter(|s| s.len() >= 10))?;
        ymd(&s[..10])
    }

    /// Строка r — нормализованный билет (тот же словарь, что `core.series_arrow`).
    pub fn ticket(&self, r: usize) -> Ticket {
        let s = |f: usize| self.strs[f][r].clone();
        let i = |f: usize| self.ints[f][r];
        let chain: Vec<String> = self.s(15, r).and_then(|j| serde_json::from_str(j).ok()).unwrap_or_default();
        let legs: Vec<Leg> = self.s(16, r).and_then(|j| serde_json::from_str(j).ok()).unwrap_or_default();
        let transfer_points: Vec<TransferPoint> = self.s(17, r).and_then(|j| serde_json::from_str(j).ok()).unwrap_or_default();
        Ticket {
            origin: s(3),
            destination: s(4),
            origin_airport: s(5),
            destination_airport: s(6),
            departure_at: s(7),
            arrival_at: s(8),
            duration: i(0),
            duration_to: i(1),
            transfers: i(2).unwrap_or(0),
            airline: s(9),
            flight_number: s(10),
            price: self.price[r],
            currency: s(11),
            link: s(12),
            chain,
            legs,
            transfer_points: Some(transfer_points),
            baggage: Some(Baggage { known: self.known[r], included: self.included[r], pieces: i(3), kg: i(4) }),
            baggage_code: s(13),
            source: s(14),
            search_origin: s(0),
            search_destination: s(1),
            search_date: s(2),
            hidden_city: None,
            layover_minutes: None,
            src: None,
        }
    }
}

/// Все строки Parquet-файла озера.
pub fn read_lake_parquet(data: bytes::Bytes) -> Result<LakeCols, String> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(data).map_err(|e| format!("parquet: {e}"))?.with_batch_size(8192).build().map_err(|e| format!("parquet: {e}"))?;
    let mut out = LakeCols::empty();
    for batch in reader {
        let batch = batch.map_err(|e| format!("parquet: {e}"))?;
        out.append(LakeCols::from_batch(&batch)?);
    }
    Ok(out)
}

// ---------------------------------------------------------------- кодек холодной части

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn varint(&mut self) -> Result<u64, String> {
        let mut v = 0u64;
        let mut shift = 0;
        loop {
            let b = *self.buf.get(self.pos).ok_or("блок склада обрезан")?;
            self.pos += 1;
            v |= ((b & 0x7f) as u64) << shift;
            if b < 0x80 {
                return Ok(v);
            }
            shift += 7;
            if shift > 63 {
                return Err("блок склада: битый varint".into());
            }
        }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self.pos.checked_add(n).filter(|&e| e <= self.buf.len()).ok_or("блок склада обрезан")?;
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }
}

/// Строки `rows` колонками: по каждому полю сначала длины (0 — нет значения, иначе len+1),
/// затем байты подряд — так zstd видит одинаковые значения колонки рядом.
fn encode_cold(c: &LakeCols, rows: &[usize]) -> Vec<u8> {
    let mut out = Vec::with_capacity(rows.len() * 600);
    for col in &c.strs {
        for &r in rows {
            put_varint(&mut out, col[r].as_ref().map(|s| s.len() as u64 + 1).unwrap_or(0));
        }
        for &r in rows {
            if let Some(s) = &col[r] {
                out.extend_from_slice(s.as_bytes());
            }
        }
    }
    for col in &c.ints {
        for &r in rows {
            // 0 — нет значения, иначе zigzag + 1
            put_varint(&mut out, col[r].map(|v| (((v << 1) ^ (v >> 63)) as u64) + 1).unwrap_or(0));
        }
    }
    for &r in rows {
        match c.price[r] {
            Some(p) => {
                out.push(1);
                out.extend_from_slice(&p.to_le_bytes());
            }
            None => out.push(0),
        }
    }
    for &r in rows {
        out.push(c.known[r] as u8 | (c.included[r] as u8) << 1);
    }
    out
}

/// Разбор холодной части блока из n строк — только строки `sel` (по возрастанию), в
/// порядке `sel`.
fn decode_cold(buf: &[u8], n: usize, sel: &[usize]) -> Result<LakeCols, String> {
    let mut cur = Cursor { buf, pos: 0 };
    let mut out = LakeCols::empty();
    out.n = sel.len();
    let mut lens = vec![0u64; n];
    for f in 0..STR_FIELDS.len() {
        for l in lens.iter_mut() {
            *l = cur.varint()?;
        }
        let mut k = 0;
        let col = &mut out.strs[f];
        for (r, &l) in lens.iter().enumerate() {
            let bytes = if l > 0 { cur.take(l as usize - 1)? } else { &[][..] };
            if k < sel.len() && sel[k] == r {
                col.push(if l > 0 { Some(std::str::from_utf8(bytes).map(str::to_owned).unwrap_or_else(|_| String::from_utf8_lossy(bytes).into_owned())) } else { None });
                k += 1;
            }
        }
    }
    for f in 0..INT_FIELDS.len() {
        let mut k = 0;
        for r in 0..n {
            let v = cur.varint()?;
            if k < sel.len() && sel[k] == r {
                out.ints[f].push(if v == 0 { None } else { let z = v - 1; Some(((z >> 1) as i64) ^ -((z & 1) as i64)) });
                k += 1;
            }
        }
    }
    let mut k = 0;
    for r in 0..n {
        let tag = cur.take(1)?[0];
        let p = if tag == 1 { Some(f64::from_le_bytes(cur.take(8)?.try_into().unwrap())) } else { None };
        if k < sel.len() && sel[k] == r {
            out.price.push(p);
            k += 1;
        }
    }
    let flags = cur.take(n)?;
    for &r in sel {
        out.known.push(flags[r] & 1 != 0);
        out.included.push(flags[r] & 2 != 0);
    }
    Ok(out)
}

// ---------------------------------------------------------------- склад

/// Серия склада «город вылета × день».
#[derive(Default)]
pub struct Block {
    /// Момент загрузки серии (unix-секунды, из имени файла озера).
    pub fetched: i64,
    /// Горячие колонки — всё, что нужно сбору джобы и стыковке, числами (сбор копирует
    /// их, не разбирая билет): город вылета и прилёта, аэропорты — номера словаря кодов;
    /// цена; вылет и прилёт — секунды «наивного» местного времени от `EPOCH` (NONE — нет);
    /// длительность, пересадки, багаж, мин. стыковка; хэш ключа рейса (дедуп).
    pub origin: Vec<u32>,
    pub dest: Vec<u32>,
    pub price: Vec<f64>,
    pub orig_ap: Vec<u32>,
    pub dest_ap: Vec<u32>,
    pub dep: Vec<i32>,
    pub arr: Vec<i32>,
    pub dur: Vec<i32>,
    pub transfers: Vec<u8>,
    /// бит 0 — багаж включён.
    pub flags: Vec<u8>,
    pub pts_min: Vec<i32>,
    pub key: Vec<u64>,
    /// Пересадки строки r (для hidden-city) — tp_*[tp_off[r]..tp_off[r + 1]]: аэропорт,
    /// прилёт в него, длительность до него (с поясами), мин. стыковка до него, хэш ключа
    /// виртуального рейса «выходим здесь».
    pub tp_off: Vec<u32>,
    pub tp_ap: Vec<u32>,
    pub tp_arr: Vec<i32>,
    pub tp_dur: Vec<i32>,
    pub tp_pmin: Vec<i32>,
    pub tp_key: Vec<u64>,
    /// Холодная часть: все поля билета (`encode_cold`), zstd; raw_len — её размер до сжатия.
    pub cold: Box<[u8]>,
    pub raw_len: u32,
}

/// Нет значения в i32-колонках блока.
pub const NONE_I32: i32 = i32::MIN;
/// Начало отсчёта секунд горячих колонок: 2020-01-01T00:00:00 (наивное время).
pub const EPOCH: i64 = 1_577_836_800;

/// Пересадки строки склада для hidden-city — готовыми колонками блока.
pub fn store_tps(b: &Block, r: usize, codes: &JobCodes) -> Vec<Tp> {
    let (a, z) = (b.tp_off[r] as usize, b.tp_off[r + 1] as usize);
    let store = global();
    let name = |id: u32| store.as_ref().map(|s| s.code_name(id)).unwrap_or_default();
    (a..z)
        .map(|k| Tp {
            ap: codes.from_store(b.tp_ap[k], &name),
            arr: (b.tp_arr[k] != NONE_I32).then(|| b.tp_arr[k] as i64 + EPOCH),
            dur: b.tp_dur[k] as i64,
            pmin: (b.tp_pmin[k] != NONE_I32).then(|| b.tp_pmin[k] as i64),
            key: b.tp_key[k],
        })
        .collect()
}

/// Пересадки полного билета (серии коллектора) — те же поля, что у строки склада.
pub fn ticket_tps(t: &Ticket, codes: &JobCodes) -> Vec<Tp> {
    hot_row(t)
        .tps
        .into_iter()
        .map(|(ap, arr, dur, pmin, key)| Tp { ap: codes.id(&ap), arr: (arr != NONE_I32).then(|| arr as i64 + EPOCH), dur: dur as i64, pmin: (pmin != NONE_I32).then(|| pmin as i64), key })
        .collect()
}

/// Хэш ключа рейса (`Ticket::flight_key`) — дедуп рейсов в сборе.
pub fn key_hash(key: &str) -> u64 {
    use std::hash::Hasher;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    h.write(key.as_bytes());
    h.finish()
}

fn rel_secs(iso: Option<&str>) -> i32 {
    iso.and_then(crate::dates::parse_naive).map(|dt| (crate::dates::naive_seconds(dt) as i64 - EPOCH) as i32).unwrap_or(NONE_I32)
}

fn opt_i32(v: Option<i64>) -> i32 {
    v.map(|x| x.clamp(i32::MIN as i64 + 1, i32::MAX as i64) as i32).unwrap_or(NONE_I32)
}

/// Горячие поля строки из билета (то же, что строка колонок джобы `flightcols::row_of`).
struct HotRow {
    dep: i32,
    arr: i32,
    dur: i32,
    transfers: u8,
    flags: u8,
    pts_min: i32,
    key: u64,
    /// (аэропорт, прилёт, длительность, мин. стыковка, ключ) по пересадкам.
    tps: Vec<(String, i32, i32, i32, u64)>,
}

fn hot_row(t: &Ticket) -> HotRow {
    let dep_iso = t.departure_at.clone().filter(|s| !s.is_empty());
    let dep = rel_secs(dep_iso.as_deref());
    let arr = if dep_iso.is_some() { rel_secs(t.arrival().as_deref()) } else { NONE_I32 };
    let mut tps = Vec::new();
    let points = t.transfer_points.as_deref().unwrap_or(&[]);
    // как hidden_city_flights: виртуальные рейсы — только при сегментах на каждую пересадку
    if !points.is_empty() && !t.legs.is_empty() && t.legs.len() >= points.len() + 1 {
        for (k, tp) in points.iter().enumerate() {
            let v = t.virtual_flight(k, "");
            let v_arr = if dep_iso.is_some() { rel_secs(v.arrival().as_deref()) } else { NONE_I32 };
            tps.push((tp.code.clone().unwrap_or_default(), v_arr, v.duration.unwrap_or(0) as i32, opt_i32(v.min_transfer_minutes()), key_hash(&v.flight_key())));
        }
    }
    HotRow {
        dep,
        arr,
        dur: t.duration.unwrap_or(0) as i32,
        transfers: t.transfers.clamp(0, 255) as u8,
        flags: t.baggage.as_ref().map(|b| b.included).unwrap_or(false) as u8,
        pts_min: opt_i32(t.min_transfer_minutes()),
        key: key_hash(&t.flight_key()),
        tps,
    }
}

impl Block {
    pub fn len(&self) -> usize {
        self.dest.len()
    }

    pub fn is_empty(&self) -> bool {
        self.dest.is_empty()
    }

    fn bytes(&self) -> usize {
        self.len() * (4 * 4 + 8 + 4 * 4 + 2 + 8 + 4) + self.tp_ap.len() * (4 * 4 + 8) + self.cold.len() + 64
    }

    fn rows(&self, sel: &[usize]) -> Result<LakeCols, String> {
        if sel.is_empty() {
            return Ok(LakeCols::empty());
        }
        let raw = zstd::bulk::decompress(&self.cold, self.raw_len as usize).map_err(|e| format!("блок склада: {e}"))?;
        decode_cold(&raw, self.len(), sel)
    }
}

/// Ссылка на строку склада: блок (Arc держит его, даже если серию уже заменили) и номер
/// строки; `hub` — виртуальный рейс hidden-city «выходим на k-й пересадке в городе».
/// В сравнении билетов не участвует.
#[derive(Clone)]
pub struct StoreRow {
    pub block: Arc<Block>,
    pub row: u32,
    pub hub: Option<(u8, Box<str>)>,
}

impl StoreRow {
    pub fn with_hub(&self, k: usize, city: &str) -> StoreRow {
        StoreRow { block: self.block.clone(), row: self.row, hub: Some((k as u8, city.into())) }
    }

    /// Блоки равны по указателю: строки одного блока разбираются одним разжатием.
    pub fn same_block(&self, other: &StoreRow) -> bool {
        Arc::ptr_eq(&self.block, &other.block)
    }
}

impl std::fmt::Debug for StoreRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StoreRow({:p}#{}{:?})", Arc::as_ptr(&self.block), self.row, self.hub)
    }
}

impl PartialEq for StoreRow {
    fn eq(&self, _other: &StoreRow) -> bool {
        true
    }
}

/// Билеты строк склада: строки группируются по блоку (одно разжатие на блок), порядок —
/// как в `rows`; виртуальные (hub) — `virtual_flight` от билета строки.
pub fn materialize(rows: &[&StoreRow]) -> Result<Vec<Ticket>, String> {
    let mut out: Vec<Option<Ticket>> = vec![None; rows.len()];
    let mut order: Vec<usize> = (0..rows.len()).collect();
    order.sort_by_key(|&i| (Arc::as_ptr(&rows[i].block) as usize, rows[i].row));
    let mut k = 0;
    while k < order.len() {
        let first = rows[order[k]];
        let mut end = k;
        while end < order.len() && rows[order[end]].same_block(first) {
            end += 1;
        }
        let mut sel: Vec<usize> = order[k..end].iter().map(|&i| rows[i].row as usize).collect();
        sel.dedup();
        let cols = first.block.rows(&sel)?;
        for &i in &order[k..end] {
            let r = rows[i];
            let pos = sel.binary_search(&(r.row as usize)).map_err(|_| "строка склада вне блока".to_string())?;
            let mut t = cols.ticket(pos);
            t.src = Some(StoreRow { block: r.block.clone(), row: r.row, hub: None });
            if let Some((hk, city)) = &r.hub {
                let pts = t.transfer_points.as_ref().map(|p| p.len()).unwrap_or(0);
                if (*hk as usize) < pts && t.legs.len() > *hk as usize {
                    t = t.virtual_flight(*hk as usize, city);
                }
            }
            out[i] = Some(t);
        }
        k = end;
    }
    Ok(out.into_iter().map(|t| t.unwrap_or_default()).collect())
}

/// Словарь кодов городов: горячие колонки хранят номера.
#[derive(Default)]
struct Codes {
    names: Vec<String>,
    ix: FxHashMap<String, u32>,
}

impl Codes {
    fn id(&mut self, code: &str) -> u32 {
        if let Some(&id) = self.ix.get(code) {
            return id;
        }
        let id = self.names.len() as u32;
        self.names.push(code.to_string());
        self.ix.insert(code.to_string(), id);
        id
    }
}

/// День как число (дни от н. э.) — ключ серий.
pub fn day_num(d: NaiveDate) -> i32 {
    d.num_days_from_ce()
}

fn num_day(n: i32) -> NaiveDate {
    NaiveDate::from_num_days_from_ce_opt(n).unwrap_or_default()
}

#[derive(Debug, Default, Clone, PartialEq, serde::Serialize)]
pub struct ApplyStats {
    pub days: usize,
    pub rows: usize,
    pub skipped_days: usize,
}

/// Ход загрузки склада — для /api/health.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct SyncStatus {
    pub state: String,
    pub files_total: usize,
    pub files_done: usize,
    pub files_failed: usize,
    pub last_error: Option<String>,
    pub last_sync_at: Option<String>,
    pub last_sync_seconds: Option<f64>,
    pub snapshot_at: Option<String>,
    pub loaded_from_snapshot: bool,
    /// От старта процесса до готовности склада, с.
    pub ready_seconds: Option<f64>,
    /// Чтение снапшота на старте и последняя запись: секунды, МБ на диске.
    pub snapshot_load_seconds: Option<f64>,
    pub snapshot_write_seconds: Option<f64>,
    pub snapshot_mb: Option<u64>,
    /// Последний полный проход по озеру: ключей, нужно, применено, строк, МБ скачано, секунд.
    pub last_full: Option<Value>,
}

#[derive(Default)]
pub struct LakeStore {
    codes: RwLock<Codes>,
    /// (день, город вылета) → серия.
    series: RwLock<BTreeMap<(i32, u32), Arc<Block>>>,
    ready: Mutex<bool>,
    ready_cv: Condvar,
    pub status: Mutex<SyncStatus>,
    /// Изменения после последнего снапшота.
    pub dirty: std::sync::atomic::AtomicBool,
    /// Аэропорт → город из концов билетов (PKX → BJS): hidden-city и остановки по городу.
    airports: RwLock<HashMap<String, String>>,
    airports_arc: Mutex<Option<Arc<HashMap<String, String>>>>,
}

/// Сколько ждать первой загрузки склада в запросе джобы.
const READY_WAIT: Duration = Duration::from_secs(30 * 60);

impl LakeStore {
    pub fn new() -> LakeStore {
        LakeStore::default()
    }

    pub fn set_ready(&self) {
        *self.ready.lock().unwrap() = true;
        self.ready_cv.notify_all();
    }

    pub fn is_ready(&self) -> bool {
        *self.ready.lock().unwrap()
    }

    /// Ждёт первой загрузки (снапшот или озеро целиком).
    fn wait_ready(&self) -> Result<(), CollectError> {
        let g = self.ready.lock().unwrap();
        let (g, _) = self.ready_cv.wait_timeout_while(g, READY_WAIT, |r| !*r).unwrap();
        if *g { Ok(()) } else { Err(CollectError::Failed("склад билетов ещё загружается — попробуйте через пару минут".into())) }
    }

    fn code_id(&self, code: &str) -> Option<u32> {
        self.codes.read().unwrap().ix.get(code).copied()
    }

    /// Момент загрузки серии (unix-секунды) или None.
    pub fn fetched(&self, origin: &str, day: NaiveDate) -> Option<i64> {
        let id = self.code_id(origin)?;
        self.series.read().unwrap().get(&(day_num(day), id)).map(|b| b.fetched)
    }

    /// Нужен ли файл складу: хоть один его день не раньше `cutoff` без серии или с серией
    /// старше файла. Применённый файл даёт false — журнал файлов не нужен.
    pub fn needs(&self, meta: &FileMeta, cutoff: NaiveDate) -> bool {
        let Some(origin) = meta.store_origin() else { return false };
        let observed = meta.observed.timestamp();
        let id = self.code_id(origin);
        let series = self.series.read().unwrap();
        meta.days().into_iter().filter(|d| *d >= cutoff).any(|d| match id.and_then(|id| series.get(&(day_num(d), id))) {
            Some(b) => b.fetched < observed,
            None => true,
        })
    }

    /// Файл озера → серии склада: по каждому дню окна (и дню любой строки) — если в складе
    /// серия не новее, блок дня заменяется; пустые дни окна тоже серии (0 билетов — день
    /// покрыт). Дни раньше `cutoff` пропускаются. Повтор того же файла ничего не меняет.
    pub fn apply(&self, meta: &FileMeta, cols: &LakeCols, cutoff: NaiveDate) -> Result<ApplyStats, String> {
        let Some(origin) = meta.store_origin() else { return Ok(ApplyStats::default()) };
        let fetched = meta.observed.timestamp();
        let mut by_day: BTreeMap<NaiveDate, Vec<usize>> = meta.days().into_iter().map(|d| (d, Vec::new())).collect();
        for r in 0..cols.n {
            if let Some(d) = cols.dep_day(r) {
                by_day.entry(d).or_default().push(r);
            }
        }
        let mut stats = ApplyStats::default();
        self.learn_airports(cols);
        let origin_id = self.codes.write().unwrap().id(origin);
        let mut blocks: Vec<((i32, u32), Arc<Block>)> = Vec::new();
        for (day, rows) in by_day {
            if day < cutoff {
                continue;
            }
            let key = (day_num(day), origin_id);
            if self.series.read().unwrap().get(&key).map(|b| b.fetched > fetched).unwrap_or(false) {
                stats.skipped_days += 1;
                continue;
            }
            let tickets: Vec<Ticket> = rows.iter().map(|&r| cols.ticket(r)).collect();
            let hot: Vec<HotRow> = tickets.iter().map(hot_row).collect();
            let mut b = Block { fetched, price: rows.iter().map(|&r| cols.price[r].unwrap_or(f64::INFINITY)).collect(), ..Block::default() };
            {
                let mut codes = self.codes.write().unwrap();
                let up = |s: Option<&str>| s.map(|s| s.to_uppercase()).unwrap_or_default();
                b.tp_off.push(0);
                for (k, &r) in rows.iter().enumerate() {
                    b.origin.push(codes.id(&up(cols.s(S_ORIGIN, r))));
                    b.dest.push(codes.id(&up(cols.s(S_DEST, r))));
                    b.orig_ap.push(codes.id(&up(cols.s(S_ORIGIN_AP, r))));
                    b.dest_ap.push(codes.id(&up(cols.s(S_DEST_AP, r))));
                    let h = &hot[k];
                    b.dep.push(h.dep);
                    b.arr.push(h.arr);
                    b.dur.push(h.dur);
                    b.transfers.push(h.transfers);
                    b.flags.push(h.flags);
                    b.pts_min.push(h.pts_min);
                    b.key.push(h.key);
                    for (ap, arr, dur, pmin, key) in &h.tps {
                        b.tp_ap.push(codes.id(ap));
                        b.tp_arr.push(*arr);
                        b.tp_dur.push(*dur);
                        b.tp_pmin.push(*pmin);
                        b.tp_key.push(*key);
                    }
                    b.tp_off.push(b.tp_ap.len() as u32);
                }
            }
            drop(tickets);
            let raw = encode_cold(cols, &rows);
            b.raw_len = raw.len() as u32;
            b.cold = zstd::bulk::compress(&raw, 3).map_err(|e| format!("zstd: {e}"))?.into_boxed_slice();
            stats.rows += rows.len();
            stats.days += 1;
            blocks.push((key, Arc::new(b)));
        }
        if !blocks.is_empty() {
            let mut series = self.series.write().unwrap();
            for (key, b) in blocks {
                // между проверкой и вставкой мог прийти более свежий файл того же дня
                if series.get(&key).map(|cur| cur.fetched > b.fetched).unwrap_or(false) {
                    stats.days -= 1;
                    stats.rows -= b.len();
                    stats.skipped_days += 1;
                    continue;
                }
                series.insert(key, b);
            }
            self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(stats)
    }

    fn learn_airports(&self, cols: &LakeCols) {
        let mut new: Vec<(String, String)> = Vec::new();
        {
            let known = self.airports.read().unwrap();
            for r in 0..cols.n {
                for (a, c) in [(5, 3), (6, 4)] {
                    if let (Some(a), Some(c)) = (cols.s(a, r).filter(|s| !s.is_empty()), cols.s(c, r).filter(|s| !s.is_empty())) {
                        if a != c && !known.contains_key(&a.to_uppercase()) {
                            new.push((a.to_uppercase(), c.to_uppercase()));
                        }
                    }
                }
            }
        }
        if !new.is_empty() {
            let mut known = self.airports.write().unwrap();
            for (a, c) in new {
                known.entry(a).or_insert(c);
            }
        }
    }

    /// Карта аэропорт → город из билетов склада.
    pub fn airport_city(&self) -> HashMap<String, String> {
        self.airports.read().unwrap().clone()
    }

    /// То же, общей копией: пересобирается, только когда карта выросла.
    pub fn airport_city_arc(&self) -> Arc<HashMap<String, String>> {
        let len = self.airports.read().unwrap().len();
        let mut cached = self.airports_arc.lock().unwrap();
        if cached.as_ref().map(|m| m.len() != len).unwrap_or(true) {
            *cached = Some(Arc::new(self.airport_city()));
        }
        cached.clone().unwrap()
    }

    /// Прошедшие дни вылета — вон (строго раньше `before`). Сколько серий удалено.
    pub fn retention(&self, before: NaiveDate) -> usize {
        let mut series = self.series.write().unwrap();
        let keep = series.split_off(&(day_num(before), 0));
        let dropped = series.len();
        *series = keep;
        if dropped > 0 {
            self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        dropped
    }

    /// Билеты с днём вылета в [from, to]: города вылета / прилёта (верхний регистр),
    /// пересадка через код. Порядок — день, город вылета, цена (как у прежнего склада).
    /// Больше `limit` строк — ошибка «слишком широкий запрос» (до разбора строк).
    pub fn select(&self, origins: &[String], dests: &[String], via: &[String], from: NaiveDate, to: NaiveDate, limit: usize) -> Result<Vec<Ticket>, String> {
        if origins.is_empty() && dests.is_empty() && via.is_empty() {
            return Err("нужен хотя бы один из origin/destination/via".into());
        }
        let (o_ids, d_ids): (Vec<u32>, Vec<u32>) = {
            let codes = self.codes.read().unwrap();
            let ids = |v: &[String]| v.iter().filter_map(|c| codes.ix.get(&c.to_uppercase()).copied()).collect::<Vec<u32>>();
            (ids(origins), ids(dests))
        };
        if (!origins.is_empty() && o_ids.is_empty()) || (!dests.is_empty() && d_ids.is_empty()) {
            return Ok(Vec::new());
        }
        let (picked, order) = self.select_rows(&o_ids, &d_ids, from, to, limit)?;
        let via_up: Vec<String> = via.iter().map(|v| v.to_uppercase()).collect();
        let mut decoded: Vec<Vec<Option<Ticket>>> = Vec::with_capacity(picked.len());
        for (_, b, sel) in &picked {
            let sel: Vec<usize> = sel.iter().map(|&r| r as usize).collect();
            let cols = b.rows(&sel)?;
            decoded.push((0..cols.n).map(|r| {
                let mut t = cols.ticket(r);
                t.src = Some(StoreRow { block: b.clone(), row: sel[r] as u32, hub: None });
                if !via_up.is_empty() {
                    let pts = t.transfer_points.as_deref().unwrap_or(&[]);
                    if !pts.iter().any(|p| [&p.code, &p.to].iter().any(|c| c.as_deref().map(|c| via_up.contains(&c.to_uppercase())).unwrap_or(false))) {
                        return None;
                    }
                }
                Some(t)
            }).collect());
        }
        Ok(order.into_iter().filter_map(|(_, _, _, k, pos)| decoded[k as usize][pos as usize].take()).collect())
    }

    /// Горячий проход выборки: блоки и строки под фильтры городов (номера кодов склада),
    /// без разбора; порядок — день, город вылета (по имени), цена, как ORDER BY прежнего
    /// склада. Больше `limit` строк — ошибка до любого разбора.
    #[allow(clippy::type_complexity)]
    fn select_rows(&self, o_ids: &[u32], d_ids: &[u32], from: NaiveDate, to: NaiveDate, limit: usize) -> Result<(Vec<(i32, Arc<Block>, Vec<u32>)>, Vec<(i32, u32, f64, u32, u32)>), String> {
        let mut picked: Vec<(i32, Arc<Block>, Vec<u32>)> = Vec::new();
        let mut total = 0usize;
        {
            let series = self.series.read().unwrap();
            let range = series.range((day_num(from), 0)..=(day_num(to), u32::MAX));
            for ((day, _), b) in range {
                let sel: Vec<u32> = (0..b.len()).filter(|&r| (o_ids.is_empty() || o_ids.contains(&b.origin[r])) && (d_ids.is_empty() || d_ids.contains(&b.dest[r]))).map(|r| r as u32).collect();
                if sel.is_empty() {
                    continue;
                }
                total += sel.len();
                if total >= limit {
                    return Err(format!("больше {limit} строк"));
                }
                picked.push((*day, b.clone(), sel));
            }
        }
        let rank: FxHashMap<u32, u32> = {
            let codes = self.codes.read().unwrap();
            let mut ids: Vec<u32> = picked.iter().flat_map(|(_, b, sel)| sel.iter().map(move |&r| b.origin[r as usize])).collect();
            ids.sort_unstable();
            ids.dedup();
            ids.sort_by(|a, b| codes.names[*a as usize].cmp(&codes.names[*b as usize]));
            ids.into_iter().enumerate().map(|(k, id)| (id, k as u32)).collect()
        };
        let mut order: Vec<(i32, u32, f64, u32, u32)> = Vec::with_capacity(total);
        for (k, (day, b, sel)) in picked.iter().enumerate() {
            for (pos, &r) in sel.iter().enumerate() {
                order.push((*day, rank[&b.origin[r as usize]], b.price[r as usize], k as u32, pos as u32));
            }
        }
        order.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.total_cmp(&b.2)));
        Ok((picked, order))
    }

    /// Номера кодов склада по списку кодов (неизвестные пропускаются).
    fn ids_of(&self, list: &[String]) -> Vec<u32> {
        let codes = self.codes.read().unwrap();
        list.iter().filter_map(|c| codes.ix.get(&c.to_uppercase()).copied()).collect()
    }

    pub fn code_names(&self) -> Vec<String> {
        self.codes.read().unwrap().names.clone()
    }

    fn code_name(&self, id: u32) -> String {
        self.codes.read().unwrap().names.get(id as usize).cloned().unwrap_or_default()
    }

    /// Рейсы сбора строками склада — колонки копируются, билеты не разбираются; порядок —
    /// как у `select`.
    pub fn select_flights(&self, origins: &[String], dests: &[String], from: NaiveDate, to: NaiveDate, limit: usize, codes: &JobCodes) -> Result<Vec<Fl>, String> {
        let (o_ids, d_ids) = (self.ids_of(origins), self.ids_of(dests));
        if (!origins.is_empty() && o_ids.is_empty()) || (!dests.is_empty() && d_ids.is_empty()) {
            return Ok(Vec::new());
        }
        let (picked, order) = self.select_rows(&o_ids, &d_ids, from, to, limit)?;
        let name = |id: u32| self.code_name(id);
        let c = |id: u32| codes.from_store(id, &name);
        let mut out = Vec::with_capacity(order.len());
        for (day, _, _, k, pos) in order {
            let (_, b, sel) = &picked[k as usize];
            let r = sel[pos as usize] as usize;
            let secs = |v: i32| (v != NONE_I32).then(|| v as i64 + EPOCH);
            let (dep_ts, dep_ord) = ts_ord(secs(b.dep[r]));
            let (arr_ts, arr_ord) = ts_ord(secs(b.arr[r]));
            out.push(Fl {
                leg: 0,
                orig_city: c(b.origin[r]),
                orig_airport: c(b.orig_ap[r]),
                dest: c(b.dest[r]),
                dest_airport: c(b.dest_ap[r]),
                price: if b.price[r].is_finite() { b.price[r] } else { 0.0 },
                transfers: b.transfers[r] as i64,
                duration: b.dur[r] as i64,
                hidden: false,
                bag_incl: b.flags[r] & 1 != 0,
                pts_min: (b.pts_min[r] != NONE_I32).then(|| b.pts_min[r] as i64),
                layover: None,
                dep_ts,
                arr_ts,
                dep_ord,
                arr_ord,
                key: b.key[r],
                day,
                series: c(b.origin[r]),
                src: FlightSrc::Store(StoreRow { block: b.clone(), row: r as u32, hub: None }),
            });
        }
        Ok(out)
    }

    /// Серии за дни [from, to]: (город, день) → билетов; пустой список городов — все.
    pub fn series_in(&self, origins: &[String], from: NaiveDate, to: NaiveDate) -> HashMap<(String, String), i64> {
        let codes = self.codes.read().unwrap();
        let series = self.series.read().unwrap();
        let want: Vec<u32> = origins.iter().filter_map(|c| codes.ix.get(&c.to_uppercase()).copied()).collect();
        series
            .range((day_num(from), 0)..=(day_num(to), u32::MAX))
            .filter(|((_, o), _)| origins.is_empty() || want.contains(o))
            .map(|((d, o), b)| ((codes.names[*o as usize].clone(), num_day(*d).to_string()), b.len() as i64))
            .collect()
    }

    /// По дням [from, to]: сколько серий (городов).
    pub fn days_in(&self, from: NaiveDate, to: NaiveDate) -> HashMap<String, i64> {
        let mut out: HashMap<String, i64> = HashMap::new();
        for ((d, _), _) in self.series.read().unwrap().range((day_num(from), 0)..=(day_num(to), u32::MAX)) {
            *out.entry(num_day(*d).to_string()).or_default() += 1;
        }
        out
    }

    pub fn health(&self) -> Value {
        // порядок блокировок везде один: словарь кодов, затем серии
        let codes = self.codes.read().unwrap().names.len();
        let (mut tickets, mut bytes, mut fetched_max) = (0usize, 0usize, 0i64);
        let series = self.series.read().unwrap();
        for b in series.values() {
            tickets += b.len();
            bytes += b.bytes();
            fetched_max = fetched_max.max(b.fetched);
        }
        let cities: std::collections::HashSet<u32> = series.keys().map(|(_, o)| *o).collect();
        json!({
            "status": if self.is_ready() { "ok" } else { "loading" },
            "series": series.len(),
            "tickets": tickets,
            "cities": cities.len(),
            "day_min": series.keys().next().map(|(d, _)| num_day(*d).to_string()),
            "day_max": series.keys().next_back().map(|(d, _)| num_day(*d).to_string()),
            "last_fetched_at": DateTime::<Utc>::from_timestamp(fetched_max, 0).filter(|_| fetched_max > 0).map(|d| d.to_rfc3339()),
            "memory_mb": bytes / (1 << 20),
            "codes": codes,
            "sync": serde_json::to_value(self.status.lock().unwrap().clone()).unwrap_or(Value::Null),
        })
    }

    // ------------------------------------------------------------ снапшот

    /// Снапшот склада на диск (через временный файл): словарь кодов и блоки как есть.
    /// Под блокировкой — только копия списка блоков (Arc), запись идёт без неё.
    pub fn save_snapshot(&self, path: &str) -> std::io::Result<usize> {
        self.dirty.store(false, std::sync::atomic::Ordering::Relaxed);
        let names = self.codes.read().unwrap().names.clone();
        let blocks: Vec<((i32, u32), Arc<Block>)> = self.series.read().unwrap().iter().map(|(k, b)| (*k, b.clone())).collect();
        let tmp = format!("{path}.tmp");
        {
            let mut w = BufWriter::with_capacity(1 << 20, std::fs::File::create(&tmp)?);
            w.write_all(SNAP_MAGIC)?;
            w.write_all(&(names.len() as u32).to_le_bytes())?;
            for n in &names {
                w.write_all(&(n.len() as u32).to_le_bytes())?;
                w.write_all(n.as_bytes())?;
            }
            w.write_all(&(blocks.len() as u64).to_le_bytes())?;
            for ((day, origin), b) in &blocks {
                w.write_all(&day.to_le_bytes())?;
                w.write_all(&origin.to_le_bytes())?;
                w.write_all(&b.fetched.to_le_bytes())?;
                b.write_cols(&mut w)?;
            }
            let airports = self.airport_city();
            w.write_all(&(airports.len() as u32).to_le_bytes())?;
            for (a, c) in &airports {
                for s in [a, c] {
                    w.write_all(&(s.len() as u32).to_le_bytes())?;
                    w.write_all(s.as_bytes())?;
                }
            }
            w.flush()?;
            w.get_ref().sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        Ok(blocks.len())
    }

    /// Снапшот с диска в пустой склад. Серии раньше `cutoff` пропускаются.
    pub fn load_snapshot(&self, path: &str, cutoff: NaiveDate) -> std::io::Result<usize> {
        let mut r = BufReader::with_capacity(1 << 20, std::fs::File::open(path)?);
        let bad = |m: &str| std::io::Error::new(std::io::ErrorKind::InvalidData, m.to_string());
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)?;
        if &magic != SNAP_MAGIC {
            return Err(bad("не снапшот склада (другая версия формата)"));
        }
        fn u32_(r: &mut impl Read) -> std::io::Result<u32> {
            let mut b = [0u8; 4];
            r.read_exact(&mut b)?;
            Ok(u32::from_le_bytes(b))
        }
        fn u64_(r: &mut impl Read) -> std::io::Result<u64> {
            let mut b = [0u8; 8];
            r.read_exact(&mut b)?;
            Ok(u64::from_le_bytes(b))
        }
        let mut codes = Codes::default();
        for _ in 0..u32_(&mut r)? {
            let mut s = vec![0u8; u32_(&mut r)? as usize];
            r.read_exact(&mut s)?;
            codes.id(&String::from_utf8(s).map_err(|_| bad("код не UTF-8"))?);
        }
        let mut series = BTreeMap::new();
        let cut = day_num(cutoff);
        for _ in 0..u64_(&mut r)? {
            let day = u32_(&mut r)? as i32;
            let origin = u32_(&mut r)?;
            let fetched = u64_(&mut r)? as i64;
            let mut b = Block::read_cols(&mut r)?;
            b.fetched = fetched;
            let ncodes = codes.names.len();
            if [&b.origin, &b.dest, &b.orig_ap, &b.dest_ap, &b.tp_ap].iter().any(|v| v.iter().any(|&c| c as usize >= ncodes)) || origin as usize >= ncodes {
                return Err(bad("номер кода вне словаря"));
            }
            if day >= cut {
                series.insert((day, origin), Arc::new(b));
            }
        }
        let mut airports = HashMap::new();
        for _ in 0..u32_(&mut r)? {
            let mut pair = [String::new(), String::new()];
            for p in pair.iter_mut() {
                let mut s = vec![0u8; u32_(&mut r)? as usize];
                r.read_exact(&mut s)?;
                *p = String::from_utf8(s).map_err(|_| bad("код не UTF-8"))?;
            }
            let [a, c] = pair;
            airports.insert(a, c);
        }
        let loaded = series.len();
        *self.airports.write().unwrap() = airports;
        *self.codes.write().unwrap() = codes;
        *self.series.write().unwrap() = series;
        Ok(loaded)
    }
}

const SNAP_MAGIC: &[u8; 8] = b"FSTORE03";

/// Колонки блока в снапшоте: длина + значения little-endian.
trait Le: Sized + Copy {
    const N: usize;
    fn put(self, out: &mut Vec<u8>);
    fn get(b: &[u8]) -> Self;
}
macro_rules! le {
    ($($t:ty),*) => {$(
        impl Le for $t {
            const N: usize = std::mem::size_of::<$t>();
            fn put(self, out: &mut Vec<u8>) {
                out.extend_from_slice(&self.to_le_bytes());
            }
            fn get(b: &[u8]) -> Self {
                <$t>::from_le_bytes(b.try_into().unwrap())
            }
        }
    )*};
}
le!(u8, u32, i32, u64, f64);

fn write_col<T: Le>(w: &mut impl Write, v: &[T]) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(8 + v.len() * T::N);
    (v.len() as u64).put(&mut buf);
    for &x in v {
        x.put(&mut buf);
    }
    w.write_all(&buf)
}

fn read_col<T: Le>(r: &mut impl Read) -> std::io::Result<Vec<T>> {
    let mut n = [0u8; 8];
    r.read_exact(&mut n)?;
    let n = u64::from_le_bytes(n) as usize;
    if n > 1 << 30 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "колонка снапшота слишком длинная"));
    }
    let mut buf = vec![0u8; n * T::N];
    r.read_exact(&mut buf)?;
    Ok(buf.chunks_exact(T::N).map(T::get).collect())
}

impl Block {
    fn write_cols(&self, w: &mut impl Write) -> std::io::Result<()> {
        write_col(w, &self.origin)?;
        write_col(w, &self.dest)?;
        write_col(w, &self.price)?;
        write_col(w, &self.orig_ap)?;
        write_col(w, &self.dest_ap)?;
        write_col(w, &self.dep)?;
        write_col(w, &self.arr)?;
        write_col(w, &self.dur)?;
        write_col(w, &self.transfers)?;
        write_col(w, &self.flags)?;
        write_col(w, &self.pts_min)?;
        write_col(w, &self.key)?;
        write_col(w, &self.tp_off)?;
        write_col(w, &self.tp_ap)?;
        write_col(w, &self.tp_arr)?;
        write_col(w, &self.tp_dur)?;
        write_col(w, &self.tp_pmin)?;
        write_col(w, &self.tp_key)?;
        write_col(w, &[self.raw_len])?;
        write_col(w, &self.cold)
    }

    fn read_cols(r: &mut impl Read) -> std::io::Result<Block> {
        let mut b = Block {
            origin: read_col(r)?,
            dest: read_col(r)?,
            price: read_col(r)?,
            orig_ap: read_col(r)?,
            dest_ap: read_col(r)?,
            dep: read_col(r)?,
            arr: read_col(r)?,
            dur: read_col(r)?,
            transfers: read_col(r)?,
            flags: read_col(r)?,
            pts_min: read_col(r)?,
            key: read_col(r)?,
            tp_off: read_col(r)?,
            tp_ap: read_col(r)?,
            tp_arr: read_col(r)?,
            tp_dur: read_col(r)?,
            tp_pmin: read_col(r)?,
            tp_key: read_col(r)?,
            ..Block::default()
        };
        b.raw_len = read_col::<u32>(r)?.first().copied().unwrap_or(0);
        b.cold = read_col::<u8>(r)?.into_boxed_slice();
        let n = b.origin.len();
        let t = b.tp_ap.len();
        let ok = [b.dest.len(), b.price.len(), b.orig_ap.len(), b.dest_ap.len(), b.dep.len(), b.arr.len(), b.dur.len(), b.transfers.len(), b.flags.len(), b.pts_min.len(), b.key.len()].iter().all(|&l| l == n)
            && b.tp_off.len() == n + 1
            && [b.tp_arr.len(), b.tp_dur.len(), b.tp_pmin.len(), b.tp_key.len()].iter().all(|&l| l == t)
            && b.tp_off.last().map(|&x| x as usize == t).unwrap_or(false);
        if !ok {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "блок снапшота: длины колонок не сходятся"));
        }
        Ok(b)
    }
}

/// Склад процесса (задаётся на старте, если есть озеро).
static GLOBAL: OnceLock<Arc<LakeStore>> = OnceLock::new();

pub fn set_global(store: Arc<LakeStore>) {
    let _ = GLOBAL.set(store);
}

pub fn global() -> Option<Arc<LakeStore>> {
    GLOBAL.get().cloned()
}

fn parse_day(s: &str) -> Result<NaiveDate, CollectError> {
    ymd(s).ok_or_else(|| CollectError::Failed(format!("склад билетов: плохая дата {s}")))
}

/// Карта аэропорт → город: со складом — из памяти склада (строится из концов билетов при
/// загрузке, лежит в снапшоте); без склада (локально, без озера) — пустая, её дополняет сбор.
pub fn airport_city_map(db_path: &str) -> Arc<HashMap<String, String>> {
    match global() {
        Some(store) if store.is_ready() => store.airport_city_arc(),
        // склад ещё грузится с нуля (минуты после рестарта без снапшота) — прежняя карта
        _ => crate::hot::airport_city_map_cached(db_path),
    }
}

/// Склад как источник рейсов джобы.
pub struct SharedStore(pub Arc<LakeStore>);

impl TicketStore for SharedStore {
    fn tickets(&self, origins: &[String], dests: &[String], via: &[String], from: &str, to: &str) -> Result<Vec<Ticket>, CollectError> {
        self.0.wait_ready()?;
        self.0.select(origins, dests, via, parse_day(from)?, parse_day(to)?, STORE_MAX_ROWS).map_err(|e| {
            if e.starts_with("больше") {
                let side = if !origins.is_empty() { format!("из {} городов", origins.len()) } else { format!("в {} городов", dests.len()) };
                CollectError::Failed(format!("Слишком широкий запрос: плечо {side} за {from}..{to} даёт больше {STORE_MAX_ROWS} рейсов. Сузьте окна дат или задайте города вместо «любых»."))
            } else {
                CollectError::Failed(format!("склад билетов: {e}"))
            }
        })
    }

    fn coverage(&self, origins: &[String], from: &str, to: &str) -> Result<HashMap<(String, String), i64>, CollectError> {
        self.0.wait_ready()?;
        Ok(self.0.series_in(origins, parse_day(from)?, parse_day(to)?))
    }

    fn coverage_days(&self, from: &str, to: &str) -> Result<HashMap<String, i64>, CollectError> {
        self.0.wait_ready()?;
        Ok(self.0.days_in(parse_day(from)?, parse_day(to)?))
    }

    fn flights(&self, origins: &[String], dests: &[String], from: &str, to: &str, codes: &JobCodes) -> Result<Vec<Fl>, CollectError> {
        self.0.wait_ready()?;
        self.0.select_flights(origins, dests, parse_day(from)?, parse_day(to)?, STORE_MAX_ROWS, codes).map_err(|e| {
            if e.starts_with("больше") {
                let side = if !origins.is_empty() { format!("из {} городов", origins.len()) } else { format!("в {} городов", dests.len()) };
                CollectError::Failed(format!("Слишком широкий запрос: плечо {side} за {from}..{to} даёт больше {STORE_MAX_ROWS} рейсов. Сузьте окна дат или задайте города вместо «любых»."))
            } else {
                CollectError::Failed(format!("склад билетов: {e}"))
            }
        })
    }

    fn code_names(&self) -> Vec<String> {
        self.0.code_names()
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::collector::testing::lake_ipc;

    pub fn ticket(origin: &str, dest: &str, day: &str, price: f64) -> Ticket {
        Ticket {
            origin: Some(origin.into()),
            destination: Some(dest.into()),
            origin_airport: Some(format!("{origin}A")),
            destination_airport: Some(format!("{dest}A")),
            departure_at: Some(format!("{day}T10:00:00+03:00")),
            arrival_at: Some(format!("{day}T14:00:00+03:00")),
            duration: Some(240),
            duration_to: Some(240),
            transfers: 1,
            airline: Some("SU".into()),
            flight_number: Some("100".into()),
            price: Some(price),
            currency: Some("rub".into()),
            link: Some(format!("/search/{origin}{dest}?t=x{price}")),
            chain: vec![format!("{origin}A"), "ISTA".into(), format!("{dest}A")],
            legs: vec![Leg { origin: Some(format!("{origin}A")), destination: Some("ISTA".into()), departure_at: Some(format!("{day}T10:00:00+03:00")), arrival_at: Some(format!("{day}T12:00:00+03:00")), flight_number: Some("100".into()), carrier: Some("SU".into()) }],
            transfer_points: Some(vec![TransferPoint { code: Some("IST".into()), to: Some("IST".into()), minutes: Some(60), ..Default::default() }]),
            baggage: Some(Baggage { known: true, included: false, pieces: Some(1), kg: None }),
            baggage_code: Some("1".into()),
            source: Some("graphql".into()),
            search_origin: Some(origin.into()),
            search_destination: None,
            search_date: Some(day.into()),
            hidden_city: None,
            layover_minutes: None,
            src: None,
        }
    }

    /// Билеты → LakeCols через тот же путь, что файлы озера (IPC в схеме озера).
    pub fn cols(tickets: &[Ticket]) -> LakeCols {
        LakeCols::from_tickets(tickets)
    }

    fn meta(key: &str) -> FileMeta {
        parse_file_key(key).unwrap()
    }

    fn d(s: &str) -> NaiveDate {
        ymd(s).unwrap()
    }

    #[test]
    fn parses_new_and_legacy_keys() {
        let m = meta("tickets/fetched=2026-09-30/origin=MOW/MOW-ANY__2026-10-01..2026-10-05__12-34-56Z.parquet");
        assert_eq!(m.store_origin(), Some("MOW"));
        assert_eq!(m.days().len(), 5);
        assert_eq!(m.observed.to_rfc3339(), "2026-09-30T12:34:56+00:00");
        let m = meta("tickets/fetched=2026-09-30/origin=ANY/ANY-SEL__2026-10-01__direct=1,min=100__00-00-01Z.parquet");
        assert_eq!(m.origin, None);
        assert_eq!(m.params_key, "direct=1&min=100");
        assert_eq!(m.store_origin(), None);
        let m = meta("tickets/date=2026-10-01/origin=LED/LED-ANY__to=2026-10-03__2026-09-29T10-00-00Z.parquet");
        assert_eq!(m.store_origin(), Some("LED"));
        assert_eq!(m.days().len(), 3);
        assert!(parse_file_key("coverage/latest.parquet").is_none());
    }

    #[test]
    fn codec_roundtrip_matches_lake_reader() {
        let mut ts = vec![ticket("MOW", "IST", "2026-10-10", 9000.0), ticket("MOW", "SEL", "2026-10-10", 31000.0)];
        ts[1].link = None;
        ts[1].duration = Some(-5);
        ts[1].price = None;
        ts[1].baggage = Some(Baggage { known: false, included: true, pieces: None, kg: Some(23) });
        let c = cols(&ts);
        let enc = encode_cold(&c, &[0, 1]);
        let all = decode_cold(&enc, 2, &[0, 1]).unwrap();
        assert_eq!(all, c);
        let one = decode_cold(&enc, 2, &[1]).unwrap();
        assert_eq!(one.ticket(0), c.ticket(1));
        // тот же билет, что даёт разбор ответа коллектора
        let via_ipc = crate::collector::tickets_from_ipc(&lake_ipc(&ts)).unwrap();
        assert_eq!(c.ticket(0), via_ipc[0]);
        assert_eq!(c.ticket(1), via_ipc[1]);
    }

    #[test]
    fn apply_replaces_older_and_keeps_newer() {
        let s = LakeStore::new();
        let cutoff = d("2026-10-01");
        let old = meta("tickets/fetched=2026-10-01/origin=MOW/MOW-ANY__2026-10-10..2026-10-11__08-00-00Z.parquet");
        let new = meta("tickets/fetched=2026-10-02/origin=MOW/MOW-ANY__2026-10-10__08-00-00Z.parquet");
        let st = s.apply(&old, &cols(&[ticket("MOW", "IST", "2026-10-10", 100.0), ticket("MOW", "SEL", "2026-10-10", 200.0)]), cutoff).unwrap();
        assert_eq!(st, ApplyStats { days: 2, rows: 2, skipped_days: 0 }); // 11-е — пустая серия
        assert!(!s.needs(&old, cutoff));
        assert!(s.needs(&new, cutoff));
        s.apply(&new, &cols(&[ticket("MOW", "AYT", "2026-10-10", 50.0)]), cutoff).unwrap();
        // старый файл повторно — день 10 уже свежее, день 11 тот же
        let st = s.apply(&old, &cols(&[ticket("MOW", "IST", "2026-10-10", 100.0)]), cutoff).unwrap();
        assert_eq!(st.skipped_days, 1);
        let got = s.select(&["MOW".into()], &[], &[], d("2026-10-10"), d("2026-10-11"), 1000).unwrap();
        assert_eq!(got.iter().map(|t| t.destination.clone().unwrap()).collect::<Vec<_>>(), vec!["AYT"]);
        let cov = s.series_in(&[], d("2026-10-01"), d("2026-10-31"));
        assert_eq!(cov.get(&("MOW".into(), "2026-10-11".into())), Some(&0));
        assert_eq!(s.days_in(d("2026-10-10"), d("2026-10-11")).len(), 2);
        // ANY→Y, ANY-файлы склад не берёт, лимит, ретеншн
        s.apply(&meta("tickets/fetched=2026-10-02/origin=LED/LED-ANY__2026-10-10__09-00-00Z.parquet"), &cols(&[ticket("LED", "AYT", "2026-10-10", 70.0), ticket("LED", "IST", "2026-10-10", 80.0)]), cutoff).unwrap();
        let ayt = s.select(&[], &["AYT".into()], &[], d("2026-10-10"), d("2026-10-10"), 1000).unwrap();
        assert_eq!(ayt.iter().map(|t| t.origin.clone().unwrap()).collect::<Vec<_>>(), vec!["LED", "MOW"]);
        assert!(s.select(&[], &["AYT".into()], &[], d("2026-10-10"), d("2026-10-10"), 2).is_err());
        assert_eq!(s.select(&[], &[], &["IST".into()], d("2026-10-10"), d("2026-10-10"), 1000).unwrap().len(), 3);
        let any = meta("tickets/fetched=2026-10-02/origin=ANY/ANY-AYT__2026-10-10__09-00-00Z.parquet");
        assert!(!s.needs(&any, cutoff));
        assert_eq!(s.apply(&any, &cols(&[ticket("KZN", "AYT", "2026-10-10", 1.0)]), cutoff).unwrap(), ApplyStats::default());
        assert_eq!(s.retention(d("2026-10-11")), 2);
        assert!(s.select(&[], &["AYT".into()], &[], d("2026-10-10"), d("2026-10-10"), 1000).unwrap().is_empty());
    }

    #[test]
    fn job_flights_by_store_rows() {
        use crate::flightcols::FlightCols;
        let s = LakeStore::new();
        let cutoff = d("2026-10-01");
        s.apply(&meta("tickets/fetched=2026-10-02/origin=MOW/MOW-ANY__2026-10-10__08-00-00Z.parquet"), &cols(&[ticket("MOW", "SEL", "2026-10-10", 300.0), ticket("MOW", "AYT", "2026-10-10", 100.0)]), cutoff).unwrap();
        let got = s.select(&["MOW".into()], &[], &[], d("2026-10-10"), d("2026-10-10"), 100).unwrap();
        assert!(got.iter().all(|t| t.src.is_some()));
        let mut v = got[1].virtual_flight(0, "IST");
        assert_eq!(v.src.as_ref().unwrap().hub.as_ref().map(|(k, c)| (*k, c.to_string())), Some((0, "IST".to_string())));
        v.price = Some(300.0);
        let legs = vec![got.iter().cloned().map(Arc::new).collect::<Vec<_>>(), vec![Arc::new(got[1].virtual_flight(0, "IST"))]];
        let table = FlightCols::from_collected(&legs);
        assert_eq!(table.owned_tickets().len(), 0, "из склада — только номера строк");
        assert_eq!(*table.flight(0), got[0]);
        assert_eq!(*table.flight(1), got[1]);
        let virt = table.flight(2);
        assert_eq!(virt.destination.as_deref(), Some("IST"));
        assert!(virt.hidden_city.is_some());
        assert_eq!(*virt, got[1].virtual_flight(0, "IST"));
        // Parquet: seg тех же рейсов
        let dir = std::env::temp_dir().join(format!("lakestore-job-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("job.parquet").to_string_lossy().to_string();
        table.write(&path).unwrap();
        let back = FlightCols::read(&path).unwrap();
        assert_eq!(back.len(), 3);
        assert_eq!(back.flight(2).destination.as_deref(), Some("IST"));
        assert_eq!(back.flight(0).link, got[0].segment_source().link);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn snapshot_roundtrip() {
        let s = LakeStore::new();
        let cutoff = d("2026-10-01");
        s.apply(&meta("tickets/fetched=2026-10-02/origin=MOW/MOW-ANY__2026-10-10..2026-10-12__08-00-00Z.parquet"), &cols(&[ticket("MOW", "IST", "2026-10-10", 100.0), ticket("MOW", "SEL", "2026-10-12", 200.0)]), cutoff).unwrap();
        let dir = std::env::temp_dir().join(format!("lakestore-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.snap").to_string_lossy().to_string();
        assert_eq!(s.save_snapshot(&path).unwrap(), 3);
        let t = LakeStore::new();
        assert_eq!(t.load_snapshot(&path, d("2026-10-11")).unwrap(), 2);
        let a = s.select(&["MOW".into()], &[], &[], d("2026-10-11"), d("2026-10-12"), 100).unwrap();
        let b = t.select(&["MOW".into()], &[], &[], d("2026-10-11"), d("2026-10-12"), 100).unwrap();
        assert_eq!(a, b);
        assert_eq!(b.len(), 1);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod bench {
    use super::tests::{cols, ticket};
    use super::*;

    /// Синтетический замер: `cargo test --release --lib bench_store -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn bench_store() {
        let cities: Vec<String> = (0..600).map(|i| format!("C{:02}", i % 100).chars().chain(std::iter::once((b'A' + (i / 100) as u8) as char)).collect()).collect();
        let mut rng = 12345u64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let s = LakeStore::new();
        let cutoff = ymd("2026-10-01").unwrap();
        let (mut raw_bytes, mut rows) = (0usize, 0usize);
        let t0 = std::time::Instant::now();
        let mut decode = Duration::ZERO;
        for o in 0..150 {
            for day in 0..20 {
                let d = (cutoff + chrono::Duration::days(day)).to_string();
                let n = 20 + (next() % 200) as usize;
                let ts: Vec<Ticket> = (0..n)
                    .map(|_| {
                        let dest = &cities[(next() % 600) as usize];
                        let mut t = ticket(&cities[o], dest, &d, (3000 + next() % 90000) as f64);
                        let hubs = next() % 3;
                        t.legs = (0..=hubs).map(|k| Leg { origin: Some(format!("H{k}{}", next() % 50)), destination: Some(format!("H{}{}", k + 1, next() % 50)), departure_at: Some(format!("{d}T{:02}:{:02}:00+03:00", next() % 24, next() % 60)), arrival_at: Some(format!("{d}T{:02}:{:02}:00+03:00", next() % 24, next() % 60)), flight_number: Some(format!("{}", next() % 9000)), carrier: Some(["SU", "TK", "PC", "EK", "QR"][(next() % 5) as usize].into()) }).collect();
                        t.link = Some(format!("/search/{}{}{}1?t={}{:016x}{:016x}_{}", cities[o], &d[8..10], dest, t.legs[0].carrier.clone().unwrap(), next(), next(), next() % 100000));
                        t
                    })
                    .collect();
                let c = cols(&ts);
                raw_bytes += encode_cold(&c, &(0..c.n).collect::<Vec<_>>()).len();
                rows += n;
                let key = format!("tickets/fetched=2026-10-01/origin={}/{}-ANY__{d}__08-00-00Z.parquet", cities[o], cities[o]);
                let td = std::time::Instant::now();
                s.apply(&parse_file_key(&key).unwrap(), &c, cutoff).unwrap();
                decode += td.elapsed();
            }
        }
        let h = s.health();
        println!("строк {rows}, сырая холодная часть {:.0} Б/строку, в складе {} МБ = {:.0} Б/строку; apply {:.2} мкс/строку (всего {:.1} с)", raw_bytes as f64 / rows as f64, h["memory_mb"], (h["memory_mb"].as_u64().unwrap() << 20) as f64 / rows as f64, decode.as_secs_f64() * 1e6 / rows as f64, t0.elapsed().as_secs_f64());
        let t = std::time::Instant::now();
        let got = s.select(&[], &[cities[7].clone()], &[], cutoff, cutoff + chrono::Duration::days(6), 1_000_000).unwrap();
        println!("ANY→Y за неделю: {} билетов за {:.1} мс", got.len(), t.elapsed().as_secs_f64() * 1e3);
        let t = std::time::Instant::now();
        let got = s.select(&cities[..10], &[], &[], cutoff, cutoff + chrono::Duration::days(6), 1_000_000).unwrap();
        println!("10 городов → ANY за неделю: {} билетов за {:.1} мс", got.len(), t.elapsed().as_secs_f64() * 1e3);
    }
}

/// Сбор из колонок склада против сбора из полных билетов: одни и те же билеты, одни и те
/// же рейсы джобы (все поля стыковки) и маршруты.
#[cfg(test)]
mod equivalence {
    use super::tests::cols;
    use super::*;
    use crate::collect::{collect_plan, store_view, NoProgress, SeriesFetcher, SeriesResult};
    use crate::flightcols::FlightCols;
    use crate::planquery::PlanQuery;
    use crate::search::{build_ctx, search_cheapest};
    use crate::stops::parse_stops;
    use crate::ticket::{Baggage, Leg, TransferPoint};

    /// Тот же склад, но рейсы — через полные билеты (путь по умолчанию трейта).
    struct ViaTickets(SharedStore);
    impl TicketStore for ViaTickets {
        fn tickets(&self, o: &[String], d: &[String], v: &[String], from: &str, to: &str) -> Result<Vec<Ticket>, CollectError> {
            self.0.tickets(o, d, v, from, to)
        }
        fn coverage(&self, o: &[String], from: &str, to: &str) -> Result<HashMap<(String, String), i64>, CollectError> {
            self.0.coverage(o, from, to)
        }
        fn coverage_days(&self, from: &str, to: &str) -> Result<HashMap<String, i64>, CollectError> {
            self.0.coverage_days(from, to)
        }
    }

    struct NoSeries;
    impl SeriesFetcher for NoSeries {
        fn fetch(&self, _o: Option<&str>, _d: Option<&str>, _day: &str, _p: i64, _cb: &dyn Fn(i64, i64)) -> Result<SeriesResult, CollectError> {
            Ok(SeriesResult { tickets: Vec::new(), pages: 1, exhausted: true, error: false, cached: true })
        }
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n.max(1)
        }
    }

    const CITIES: [&str; 8] = ["MOW", "IST", "SEL", "TAS", "DXB", "BKK", "ALA", "TBS"];

    fn random_ticket(rng: &mut Rng, o: usize, day: &str) -> Ticket {
        let mut d = rng.below(CITIES.len() as u64) as usize;
        if d == o {
            d = (d + 1) % CITIES.len();
        }
        let ap = |c: &str, k: u64| if k == 0 { c.to_string() } else { format!("{}{}", &c[..2], k) };
        let hubs: Vec<usize> = (0..rng.below(3)).map(|_| rng.below(CITIES.len() as u64) as usize).filter(|&h| h != o && h != d).collect();
        let mut chain = vec![ap(CITIES[o], rng.below(2))];
        chain.extend(hubs.iter().map(|&h| ap(CITIES[h], rng.below(2))));
        chain.push(ap(CITIES[d], rng.below(2)));
        let h0 = 1 + rng.below(12);
        let tz = ["+03:00", "+09:00", "Z", "+05:00"][rng.below(4) as usize];
        let legs: Vec<Leg> = chain
            .windows(2)
            .enumerate()
            .map(|(k, w)| Leg {
                origin: Some(w[0].clone()),
                destination: Some(w[1].clone()),
                departure_at: Some(format!("{day}T{:02}:{:02}:00{tz}", h0 + 4 * k as u64, rng.below(60))),
                arrival_at: (rng.below(10) > 0).then(|| format!("{day}T{:02}:{:02}:00{tz}", h0 + 4 * k as u64 + 2, rng.below(60))),
                flight_number: Some(format!("{}", rng.below(50))),
                carrier: Some("SU".into()),
            })
            .collect();
        let points: Vec<TransferPoint> = hubs.iter().enumerate().map(|(k, _)| TransferPoint { code: Some(chain[k + 1].clone()), to: Some(chain[k + 1].clone()), minutes: (rng.below(5) > 0).then(|| 40 + rng.below(600) as i64), ..Default::default() }).collect();
        let mut t = super::tests::ticket(CITIES[o], CITIES[d], day, (1000 + rng.below(30) * 500) as f64);
        t.origin_airport = Some(chain[0].clone());
        t.destination_airport = Some(chain[chain.len() - 1].clone());
        t.departure_at = legs[0].departure_at.clone();
        t.arrival_at = if rng.below(4) == 0 { None } else { legs.last().unwrap().arrival_at.clone() };
        t.duration = Some(100 + rng.below(900) as i64);
        t.transfers = hubs.len() as i64;
        t.chain = chain;
        t.legs = legs;
        t.transfer_points = Some(points);
        t.baggage = Some(Baggage { known: true, included: rng.below(2) == 0, pieces: None, kg: None });
        t
    }

    fn row(c: &FlightCols, r: usize) -> String {
        format!(
            "{} {}/{}>{}/{} {} t{} d{} h{} b{} p{:?} l{:?} {} {} {} {}",
            c.leg[r], c.orig_city(r), c.orig_airport(r), c.dest(r), c.dest_airport(r), c.price[r], c.transfers[r], c.duration[r], c.hidden[r], c.bag_incl[r], c.pts_min[r], c.layover[r], c.dep_ts[r], c.arr_ts[r], c.dep_ord[r], c.arr_ord[r]
        )
    }

    #[test]
    fn store_columns_match_tickets() {
        let mut rng = Rng(0x2545F4914F6CDD1D);
        let s = Arc::new(LakeStore::new());
        let cutoff = ymd("2026-10-01").unwrap();
        for day in ["2026-11-01", "2026-11-02", "2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06"] {
            for (o, city) in CITIES.iter().enumerate() {
                let ts: Vec<Ticket> = (0..20 + rng.below(30)).map(|_| random_ticket(&mut rng, o, day)).collect();
                let key = format!("tickets/fetched=2026-10-02/origin={city}/{city}-ANY__{day}__08-00-00Z.parquet");
                s.apply(&parse_file_key(&key).unwrap(), &cols(&ts), cutoff).unwrap();
            }
        }
        s.set_ready();
        let st = |kind: &str, codes: &[&str], a: &str, b: &str| serde_json::json!({"kind": kind, "codes": codes, "window": [a, b], "radiusKm": 0});
        let scenarios = vec![
            vec![st("cities", &["MOW"], "", ""), st("cities", &["IST"], "2026-11-01", "2026-11-03"), st("cities", &["MOW"], "", "")],
            vec![st("cities", &["MOW"], "", ""), st("any", &[], "2026-11-01", "2026-11-02"), st("cities", &["MOW"], "", "")],
            vec![st("cities", &["MOW"], "", ""), st("any", &[], "2026-11-01", "2026-11-02"), st("any", &[], "2026-11-03", "2026-11-04"), st("cities", &["MOW"], "", "")],
            vec![st("any", &[], "2026-11-01", "2026-11-01"), st("cities", &["SEL"], "2026-11-02", "2026-11-04"), st("cities", &["TAS"], "", "")],
            vec![st("cities", &["MOW", "ALA"], "", ""), st("cities", &["IST", "TBS"], "2026-11-01", "2026-11-02"), st("cities", &["DXB"], "2026-11-04", "2026-11-05"), st("cities", &["MOW"], "", "")],
        ];
        for stops_json in scenarios {
            let n = stops_json.len();
            let q = serde_json::json!({"stops": stops_json, "cities": vec![serde_json::json!({}); n], "legs": vec![serde_json::json!({}); n - 1], "tripLength": [0, null]});
            let pq = PlanQuery::from_value(&q).unwrap();
            let stops = parse_stops(&pq.stops);
            let run = |store: &dyn TicketStore| -> FlightCols {
                let view = store_view(Some(store), &stops);
                let mut ac = HashMap::new();
                collect_plan(&stops, &NoSeries, view.as_ref(), &NoProgress, &mut ac, 1).unwrap()
            };
            let fast = run(&SharedStore(s.clone()));
            let slow = run(&ViaTickets(SharedStore(s.clone())));
            let mut a: Vec<String> = (0..fast.len()).map(|r| row(&fast, r)).collect();
            let mut b: Vec<String> = (0..slow.len()).map(|r| row(&slow, r)).collect();
            a.sort();
            b.sort();
            assert!(!a.is_empty(), "{q}");
            assert_eq!(a, b, "{q}");
            assert!(fast.hidden.iter().any(|&h| h), "есть hidden-city: {q}");
            // полные рейсы строк из колонок — те же, что из билетов
            for r in (0..fast.len()).step_by(7) {
                let f = fast.flight(r);
                assert_eq!(f.destination.as_deref().map(str::to_uppercase).unwrap_or_default(), fast.dest(r), "{q}");
                assert_eq!(f.hidden_city.is_some(), fast.hidden[r]);
            }
            let price = |t: &FlightCols| -> Vec<f64> {
                let ctx = build_ctx(&stops, t, Some(&pq));
                let mut check = |_: usize| Ok(());
                search_cheapest(&ctx, 300, Some(&pq), &mut check).unwrap().iter().map(|c| c.iter().map(|&fi| t.price[fi]).sum()).collect()
            };
            assert_eq!(price(&fast), price(&slow), "{q}");
        }
    }
}
