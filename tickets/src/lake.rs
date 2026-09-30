//! Файлы озера коллектора (`collector/lake.py`): разбор ключа файла и чтение Parquet в строки.
//!
//! Ключ: `tickets/fetched=<день загрузки>/origin=<город>/<A>-<B>__<день>[..<день>][__<параметры>]__<HH-MM-SS>Z.parquet`
//! (и прежняя раскладка `tickets/date=<день>/origin=X/<A>-<B>[__<параметры>]__<день>T<HH-MM-SS>Z.parquet`,
//! окно — параметром `to=`). Одна колонка Parquet = одно поле билета (`collector/lake.SCHEMA`),
//! вложенные части — JSON-строками.

use std::sync::OnceLock;

use arrow::array::{Array, BooleanArray, Float64Array, Int16Array, Int32Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use regex::Regex;

pub const TICKETS_PREFIX: &str = "tickets";

/// Что известно о серии по имени файла.
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
        let mut out = Vec::new();
        let mut d = self.day;
        while d <= self.day_to {
            out.push(d);
            d = d.succ_opt().unwrap();
        }
        out
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

/// Разбор ключа файла озера (None — файл не по схеме).
pub fn parse_file_key(key: &str) -> Option<FileMeta> {
    if let Some(m) = re_new().captures(key) {
        let day = NaiveDate::parse_from_str(&m["day"], "%Y-%m-%d").ok()?;
        let day_to = m.name("day_to").and_then(|s| NaiveDate::parse_from_str(s.as_str(), "%Y-%m-%d").ok()).unwrap_or(day);
        let stamp = format!("{}T{}", &m["date"], &m["time"]);
        let observed = chrono::NaiveDateTime::parse_from_str(&stamp, "%Y-%m-%dT%H-%M-%S").ok()?.and_utc();
        let params: Vec<&str> = m.name("params").map(|p| p.as_str().split(',').filter(|s| !s.is_empty()).collect()).unwrap_or_default();
        return Some(FileMeta { origin: any_none(&m["origin"]), destination: any_none(&m["dest"]), day, day_to, params_key: params.join("&"), observed });
    }
    let m = re_legacy().captures(key)?;
    let day = NaiveDate::parse_from_str(&m["day"], "%Y-%m-%d").ok()?;
    let observed = chrono::NaiveDateTime::parse_from_str(&m["stamp"], "%Y-%m-%dT%H-%M-%S").ok()?.and_utc();
    let params: Vec<&str> = m.name("params").map(|p| p.as_str().split(',').filter(|s| !s.is_empty()).collect()).unwrap_or_default();
    let day_to = params.iter().find_map(|p| p.strip_prefix("to=")).and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()).unwrap_or(day);
    let params: Vec<&str> = params.into_iter().filter(|p| !p.starts_with("to=")).collect();
    Some(FileMeta { origin: any_none(&m["origin"]), destination: any_none(&m["dest"]), day, day_to, params_key: params.join("&"), observed })
}

/// Строка билета в схеме озера (плюс разобранные dep_day и via для индексов склада).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Row {
    pub series_id: Option<i64>,
    pub observed_at: Option<String>,
    pub search_origin: Option<String>,
    pub search_destination: Option<String>,
    pub search_date: Option<String>,
    pub origin: Option<String>,
    pub destination: Option<String>,
    pub origin_airport: Option<String>,
    pub destination_airport: Option<String>,
    pub departure_at: Option<String>,
    pub arrival_at: Option<String>,
    pub duration: Option<i32>,
    pub duration_to: Option<i32>,
    pub transfers: Option<i16>,
    pub airline: Option<String>,
    pub flight_number: Option<String>,
    pub price: Option<f64>,
    pub currency: Option<String>,
    pub link: Option<String>,
    pub chain_json: Option<String>,
    pub legs_json: Option<String>,
    pub transfer_points_json: Option<String>,
    pub baggage_code: Option<String>,
    pub baggage_known: Option<bool>,
    pub baggage_included: Option<bool>,
    pub baggage_pieces: Option<i16>,
    pub baggage_kg: Option<i16>,
    pub source: Option<String>,
}

impl Row {
    /// День вылета серии: search_date, иначе дата из departure_at.
    pub fn dep_day(&self) -> Option<NaiveDate> {
        let s = self.search_date.as_deref().filter(|s| s.len() >= 10).or(self.departure_at.as_deref().filter(|s| s.len() >= 10))?;
        NaiveDate::parse_from_str(&s[..10], "%Y-%m-%d").ok()
    }

    /// Коды аэропортов пересадок (`code` и `to` каждой точки) — для выборки hidden-city.
    pub fn via(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        let Some(raw) = self.transfer_points_json.as_deref() else { return out };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(raw) else { return out };
        for p in v.as_array().map(|a| a.as_slice()).unwrap_or(&[]) {
            for k in ["code", "to"] {
                if let Some(c) = p.get(k).and_then(|c| c.as_str()).map(|c| c.trim().to_uppercase()).filter(|c| !c.is_empty()) {
                    if !out.contains(&c) {
                        out.push(c);
                    }
                }
            }
        }
        out
    }
}

fn str_col(b: &RecordBatch, name: &str) -> Vec<Option<String>> {
    let n = b.num_rows();
    match b.column_by_name(name).and_then(|c| c.as_any().downcast_ref::<StringArray>()) {
        Some(a) => (0..n).map(|i| if a.is_null(i) { None } else { Some(a.value(i).to_string()) }).collect(),
        None => match b.column_by_name(name).and_then(|c| c.as_any().downcast_ref::<arrow::array::LargeStringArray>()) {
            Some(a) => (0..n).map(|i| if a.is_null(i) { None } else { Some(a.value(i).to_string()) }).collect(),
            None => vec![None; n],
        },
    }
}

fn i64_col(b: &RecordBatch, name: &str) -> Vec<Option<i64>> {
    let n = b.num_rows();
    let Some(c) = b.column_by_name(name) else { return vec![None; n] };
    if let Some(a) = c.as_any().downcast_ref::<Int64Array>() {
        return (0..n).map(|i| if a.is_null(i) { None } else { Some(a.value(i)) }).collect();
    }
    if let Some(a) = c.as_any().downcast_ref::<Int32Array>() {
        return (0..n).map(|i| if a.is_null(i) { None } else { Some(a.value(i) as i64) }).collect();
    }
    if let Some(a) = c.as_any().downcast_ref::<Int16Array>() {
        return (0..n).map(|i| if a.is_null(i) { None } else { Some(a.value(i) as i64) }).collect();
    }
    if let Some(a) = c.as_any().downcast_ref::<Float64Array>() {
        return (0..n).map(|i| if a.is_null(i) { None } else { Some(a.value(i) as i64) }).collect();
    }
    vec![None; n]
}

fn f64_col(b: &RecordBatch, name: &str) -> Vec<Option<f64>> {
    let n = b.num_rows();
    let Some(c) = b.column_by_name(name) else { return vec![None; n] };
    if let Some(a) = c.as_any().downcast_ref::<Float64Array>() {
        return (0..n).map(|i| if a.is_null(i) { None } else { Some(a.value(i)) }).collect();
    }
    i64_col(b, name).into_iter().map(|v| v.map(|x| x as f64)).collect()
}

fn bool_col(b: &RecordBatch, name: &str) -> Vec<Option<bool>> {
    let n = b.num_rows();
    match b.column_by_name(name).and_then(|c| c.as_any().downcast_ref::<BooleanArray>()) {
        Some(a) => (0..n).map(|i| if a.is_null(i) { None } else { Some(a.value(i)) }).collect(),
        None => vec![None; n],
    }
}

/// Строки одной RecordBatch схемы озера.
pub fn rows_from_batch(b: &RecordBatch) -> Vec<Row> {
    let n = b.num_rows();
    if n == 0 {
        return Vec::new();
    }
    let s = |name: &str| str_col(b, name);
    let (series_id, observed_at) = (i64_col(b, "series_id"), s("observed_at"));
    let (search_origin, search_destination, search_date) = (s("search_origin"), s("search_destination"), s("search_date"));
    let (origin, destination, origin_airport, destination_airport) = (s("origin"), s("destination"), s("origin_airport"), s("destination_airport"));
    let (departure_at, arrival_at, duration, duration_to, transfers) = (s("departure_at"), s("arrival_at"), i64_col(b, "duration"), i64_col(b, "duration_to"), i64_col(b, "transfers"));
    let (airline, flight_number, price, currency, link) = (s("airline"), s("flight_number"), f64_col(b, "price"), s("currency"), s("link"));
    let (chain_json, legs_json, transfer_points_json) = (s("chain_json"), s("legs_json"), s("transfer_points_json"));
    let (baggage_code, baggage_known, baggage_included) = (s("baggage_code"), bool_col(b, "baggage_known"), bool_col(b, "baggage_included"));
    let (baggage_pieces, baggage_kg, source) = (i64_col(b, "baggage_pieces"), i64_col(b, "baggage_kg"), s("source"));
    (0..n)
        .map(|r| Row {
            series_id: series_id[r],
            observed_at: observed_at[r].clone(),
            search_origin: search_origin[r].clone(),
            search_destination: search_destination[r].clone(),
            search_date: search_date[r].clone(),
            origin: origin[r].clone(),
            destination: destination[r].clone(),
            origin_airport: origin_airport[r].clone(),
            destination_airport: destination_airport[r].clone(),
            departure_at: departure_at[r].clone(),
            arrival_at: arrival_at[r].clone(),
            duration: duration[r].map(|v| v as i32),
            duration_to: duration_to[r].map(|v| v as i32),
            transfers: transfers[r].map(|v| v as i16),
            airline: airline[r].clone(),
            flight_number: flight_number[r].clone(),
            price: price[r],
            currency: currency[r].clone(),
            link: link[r].clone(),
            chain_json: chain_json[r].clone(),
            legs_json: legs_json[r].clone(),
            transfer_points_json: transfer_points_json[r].clone(),
            baggage_code: baggage_code[r].clone(),
            baggage_known: baggage_known[r],
            baggage_included: baggage_included[r],
            baggage_pieces: baggage_pieces[r].map(|v| v as i16),
            baggage_kg: baggage_kg[r].map(|v| v as i16),
            source: source[r].clone(),
        })
        .collect()
}

/// Все строки Parquet-файла озера.
pub fn read_parquet(data: Bytes) -> Result<Vec<Row>, String> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(data).map_err(|e| format!("parquet: {e}"))?.with_batch_size(8192).build().map_err(|e| format!("parquet: {e}"))?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| format!("parquet: {e}"))?;
        out.extend(rows_from_batch(&batch));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_new_and_legacy_keys() {
        let m = parse_file_key("tickets/fetched=2026-09-30/origin=MOW/MOW-ANY__2026-10-01..2026-10-05__12-34-56Z.parquet").unwrap();
        assert_eq!(m.origin.as_deref(), Some("MOW"));
        assert_eq!(m.destination, None);
        assert_eq!(m.days().len(), 5);
        assert_eq!(m.params_key, "");
        assert_eq!(m.observed.to_rfc3339(), "2026-09-30T12:34:56+00:00");
        let m = parse_file_key("tickets/fetched=2026-09-30/origin=ANY/ANY-SEL__2026-10-01__direct=1,min=100__00-00-01Z.parquet").unwrap();
        assert_eq!(m.origin, None);
        assert_eq!(m.destination.as_deref(), Some("SEL"));
        assert_eq!(m.params_key, "direct=1&min=100");
        assert_eq!(m.days(), vec![m.day]);
        let m = parse_file_key("tickets/date=2026-10-01/origin=LED/LED-ANY__to=2026-10-03__2026-09-29T10-00-00Z.parquet").unwrap();
        assert_eq!(m.origin.as_deref(), Some("LED"));
        assert_eq!(m.days().len(), 3);
        assert_eq!(m.params_key, "");
        assert!(parse_file_key("coverage/latest.parquet").is_none());
    }

    #[test]
    fn via_from_transfer_points() {
        let r = Row { transfer_points_json: Some(r#"[{"code":"IST","to":"SAW"},{"code":"DXB","to":"DXB"}]"#.into()), ..Default::default() };
        assert_eq!(r.via(), vec!["IST", "SAW", "DXB"]);
        assert!(Row::default().via().is_empty());
        let r = Row { search_date: Some("2026-10-01".into()), ..Default::default() };
        assert_eq!(r.dep_day(), NaiveDate::from_ymd_opt(2026, 10, 1));
    }
}
