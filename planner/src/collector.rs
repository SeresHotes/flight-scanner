//! HTTP-клиент коллектора (docs/COLLECTOR.md): задание в очередь с приоритетом
//! приложения, прогресс long-poll'ом, результат — Arrow IPC-поток в схеме озера
//! (`collector/lake.SCHEMA`), разбор по колонкам; JSON — если коллектор старый.

use std::io::Cursor;
use std::time::Duration;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use serde_json::Value;

use crate::collect::{CollectError, SeriesFetcher, SeriesResult};
use crate::flightcols::{bool_col, f64_col, i64_col, str_col};
use crate::ticket::{Baggage, Leg, Ticket, TransferPoint};

pub const ARROW_MEDIA_TYPE: &str = "application/vnd.apache.arrow.stream";
pub const DEFAULT_TIMEOUT: f64 = 30.0;
pub const POLL_WAIT: f64 = 20.0;

pub fn collector_url() -> Option<String> {
    std::env::var("COLLECTOR_URL").ok().filter(|s| !s.is_empty())
}

#[derive(Clone)]
pub struct CollectorClient {
    base: String,
    http: reqwest::blocking::Client,
    timeout: f64,
    poll_wait: f64,
    /// Свежесть серии, с которой планировщик просит серии (FETCH_CACHE_TTL_SECONDS).
    pub ttl_seconds: Option<f64>,
}

impl CollectorClient {
    pub fn new(base_url: &str) -> CollectorClient {
        CollectorClient {
            base: base_url.trim_end_matches('/').to_string(),
            http: reqwest::blocking::Client::builder().build().expect("reqwest"),
            timeout: DEFAULT_TIMEOUT,
            poll_wait: POLL_WAIT,
            ttl_seconds: None,
        }
    }

    pub fn with_ttl(mut self, ttl: f64) -> Self {
        self.ttl_seconds = Some(ttl);
        self
    }

    fn post(&self, path: &str, payload: &Value) -> Result<Value, CollectError> {
        let r = self
            .http
            .post(format!("{}{path}", self.base))
            .json(payload)
            .timeout(Duration::from_secs_f64(self.timeout))
            .send()
            .map_err(|e| CollectError::Failed(format!("коллектор недоступен: {e}")))?;
        let status = r.status();
        if status.as_u16() >= 400 {
            let text = r.text().unwrap_or_default();
            return Err(CollectError::Failed(format!("коллектор {path}: HTTP {} {}", status.as_u16(), text.chars().take(200).collect::<String>())));
        }
        r.json().map_err(|e| CollectError::Failed(format!("коллектор {path}: {e}")))
    }

    fn get_response(&self, path: &str, params: &[(&str, String)], timeout: Option<f64>) -> Result<reqwest::blocking::Response, CollectError> {
        let r = self
            .http
            .get(format!("{}{path}", self.base))
            .query(params)
            .timeout(Duration::from_secs_f64(timeout.unwrap_or(self.timeout)))
            .send()
            .map_err(|e| CollectError::Failed(format!("коллектор недоступен: {e}")))?;
        let status = r.status();
        if status.as_u16() >= 400 {
            let text = r.text().unwrap_or_default();
            return Err(CollectError::Failed(format!("коллектор {path}: HTTP {} {}", status.as_u16(), text.chars().take(200).collect::<String>())));
        }
        Ok(r)
    }

    fn get(&self, path: &str, params: &[(&str, String)], timeout: Option<f64>) -> Result<Value, CollectError> {
        self.get_response(path, params, timeout)?.json().map_err(|e| CollectError::Failed(format!("коллектор {path}: {e}")))
    }

    pub fn health(&self) -> Result<Value, CollectError> {
        self.get("/v1/health", &[], None)
    }

    pub fn has_series(&self, origin: Option<&str>, dest: Option<&str>, day: &str, params_key: &str, pages: Option<i64>, ttl_seconds: Option<f64>) -> Result<bool, CollectError> {
        let mut params: Vec<(&str, String)> = vec![("day", day.to_string()), ("params_key", params_key.to_string())];
        if let Some(o) = origin {
            params.push(("origin", o.to_string()));
        }
        if let Some(d) = dest {
            params.push(("destination", d.to_string()));
        }
        if let Some(p) = pages {
            params.push(("min_pages", p.to_string()));
        }
        if let Some(t) = ttl_seconds {
            params.push(("ttl_seconds", format!("{t}")));
        }
        Ok(self.get("/v1/series/exists", &params, None)?.get("exists").and_then(|v| v.as_bool()).unwrap_or(false))
    }

    /// Результат задания: Arrow IPC (метаданные — JSON в X-Series-Meta) или JSON.
    fn result(&self, job_id: &str) -> Result<SeriesResult, CollectError> {
        let r = self.get_response(&format!("/v1/requests/{job_id}/result"), &[("format", "arrow".into())], Some(self.timeout.max(120.0)))?;
        let ctype = r.headers().get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
        if ctype.starts_with(ARROW_MEDIA_TYPE) {
            let meta: Value = r.headers().get("x-series-meta").and_then(|v| v.to_str().ok()).and_then(|s| serde_json::from_str(s).ok()).unwrap_or(Value::Null);
            let bytes = r.bytes().map_err(|e| CollectError::Failed(format!("коллектор result: {e}")))?;
            let tickets = tickets_from_ipc(&bytes)?;
            return Ok(SeriesResult {
                tickets,
                pages: meta.get("pages").and_then(|v| v.as_i64()).unwrap_or(0),
                exhausted: meta.get("exhausted").and_then(|v| v.as_bool()).unwrap_or(false),
                error: meta.get("error").and_then(|v| v.as_bool()).unwrap_or(false),
                cached: meta.get("cached").and_then(|v| v.as_bool()).unwrap_or(false),
            });
        }
        let v: Value = r.json().map_err(|e| CollectError::Failed(format!("коллектор result: {e}")))?;
        let tickets: Vec<Ticket> = v.get("tickets").cloned().map(|t| serde_json::from_value(t).unwrap_or_default()).unwrap_or_default();
        Ok(SeriesResult {
            tickets,
            pages: v.get("pages").and_then(|x| x.as_i64()).unwrap_or(0),
            exhausted: v.get("exhausted").and_then(|x| x.as_bool()).unwrap_or(false),
            error: v.get("error").and_then(|x| x.as_bool()).unwrap_or(false),
            cached: v.get("cached").and_then(|x| x.as_bool()).unwrap_or(false),
        })
    }
}

impl SeriesFetcher for CollectorClient {
    fn fetch(&self, origin: Option<&str>, dest: Option<&str>, day: &str, max_pages: i64, on_page: &dyn Fn(i64, i64)) -> Result<SeriesResult, CollectError> {
        let mut job = self.post(
            "/v1/fetch",
            &serde_json::json!({
                "origin": origin, "destination": dest, "day": day,
                "value_min": null, "value_max": null, "direct": null, "with_baggage": null,
                "max_pages": max_pages, "client": "app", "ttl_seconds": self.ttl_seconds, "wait": 0,
            }),
        )?;
        let mut seen = 0i64;
        let mut tick = |pages: i64, tickets: i64| {
            while seen < pages {
                seen += 1;
                on_page(seen, tickets);
            }
        };
        let status = |j: &Value| j.get("status").and_then(|s| s.as_str()).unwrap_or("").to_string();
        while !matches!(status(&job).as_str(), "done" | "error") {
            tick(job.get("pages").and_then(|v| v.as_i64()).unwrap_or(0), job.get("tickets").and_then(|v| v.as_i64()).unwrap_or(0));
            let id = job.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
            job = self.get(&format!("/v1/requests/{id}"), &[("wait", format!("{}", self.poll_wait))], Some(self.poll_wait + self.timeout))?;
        }
        let id = job.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let result = self.result(&id)?;
        tick(result.pages, result.tickets.len() as i64);
        Ok(result)
    }
}

/// Билеты из Arrow IPC-потока в схеме озера (ровно те словари, что
/// `core.series_arrow.tickets_from_table`).
pub fn tickets_from_ipc(bytes: &[u8]) -> Result<Vec<Ticket>, CollectError> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(|e| CollectError::Failed(format!("arrow: {e}")))?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| CollectError::Failed(format!("arrow: {e}")))?;
        out.extend(tickets_from_batch(&batch)?);
    }
    Ok(out)
}

pub fn tickets_from_batch(b: &RecordBatch) -> Result<Vec<Ticket>, CollectError> {
    let n = b.num_rows();
    if n == 0 {
        return Ok(Vec::new());
    }
    let e = |s: String| CollectError::Failed(format!("arrow: {s}"));
    let s = |name: &str| str_col(b, name).map_err(e);
    let i = |name: &str| i64_col(b, name).map_err(e);
    let f = |name: &str| f64_col(b, name).map_err(e);
    let bl = |name: &str| -> Result<Vec<bool>, CollectError> { if b.column_by_name(name).is_some() { bool_col(b, name).map_err(e) } else { Ok(vec![false; n]) } };
    let (search_origin, search_destination, search_date) = (s("search_origin")?, s("search_destination")?, s("search_date")?);
    let (origin, destination, origin_airport, destination_airport) = (s("origin")?, s("destination")?, s("origin_airport")?, s("destination_airport")?);
    let (departure_at, arrival_at, duration, duration_to, transfers) = (s("departure_at")?, s("arrival_at")?, i("duration")?, i("duration_to")?, i("transfers")?);
    let (airline, flight_number, price, currency, link) = (s("airline")?, s("flight_number")?, f("price")?, s("currency")?, s("link")?);
    let (baggage_code, source) = (s("baggage_code")?, s("source")?);
    let (chain_json, legs_json, points_json) = (s("chain_json")?, s("legs_json")?, s("transfer_points_json")?);
    let (known, included, pieces, kg) = (bl("baggage_known")?, bl("baggage_included")?, i("baggage_pieces")?, i("baggage_kg")?);
    let mut out = Vec::with_capacity(n);
    for r in 0..n {
        let chain: Vec<String> = chain_json[r].as_deref().and_then(|j| serde_json::from_str(j).ok()).unwrap_or_default();
        let legs: Vec<Leg> = legs_json[r].as_deref().and_then(|j| serde_json::from_str(j).ok()).unwrap_or_default();
        let transfer_points: Vec<TransferPoint> = points_json[r].as_deref().and_then(|j| serde_json::from_str(j).ok()).unwrap_or_default();
        out.push(Ticket {
            origin: origin[r].clone(),
            destination: destination[r].clone(),
            origin_airport: origin_airport[r].clone(),
            destination_airport: destination_airport[r].clone(),
            departure_at: departure_at[r].clone(),
            arrival_at: arrival_at[r].clone(),
            duration: duration[r],
            duration_to: duration_to[r],
            transfers: transfers[r].unwrap_or(0),
            airline: airline[r].clone(),
            flight_number: flight_number[r].clone(),
            price: price[r],
            currency: currency[r].clone(),
            link: link[r].clone(),
            chain,
            legs,
            transfer_points: Some(transfer_points),
            baggage: Some(Baggage { known: known[r], included: included[r], pieces: pieces[r], kg: kg[r] }),
            baggage_code: baggage_code[r].clone(),
            source: source[r].clone(),
            search_origin: search_origin[r].clone(),
            search_destination: search_destination[r].clone(),
            search_date: search_date[r].clone(),
            hidden_city: None,
            layover_minutes: None,
        });
    }
    Ok(out)
}

/// Вспомогательное для тестов (и интеграционных): серия → Arrow IPC в схеме озера.
pub mod testing {
    use super::*;
    use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int16Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use std::sync::Arc;

    /// Таблица в схеме озера → IPC-поток (как table_to_ipc коллектора).
    pub fn lake_ipc(rows: &[Ticket]) -> Vec<u8> {
        let s = |f: &dyn Fn(&Ticket) -> Option<String>| -> ArrayRef { Arc::new(StringArray::from(rows.iter().map(f).collect::<Vec<_>>())) };
        let fields = vec![
            Field::new("search_origin", DataType::Utf8, true),
            Field::new("search_destination", DataType::Utf8, true),
            Field::new("search_date", DataType::Utf8, true),
            Field::new("origin", DataType::Utf8, true),
            Field::new("destination", DataType::Utf8, true),
            Field::new("origin_airport", DataType::Utf8, true),
            Field::new("destination_airport", DataType::Utf8, true),
            Field::new("departure_at", DataType::Utf8, true),
            Field::new("arrival_at", DataType::Utf8, true),
            Field::new("duration", DataType::Int32, true),
            Field::new("duration_to", DataType::Int32, true),
            Field::new("transfers", DataType::Int16, true),
            Field::new("airline", DataType::Utf8, true),
            Field::new("flight_number", DataType::Utf8, true),
            Field::new("price", DataType::Float64, true),
            Field::new("currency", DataType::Utf8, true),
            Field::new("link", DataType::Utf8, true),
            Field::new("chain_json", DataType::Utf8, true),
            Field::new("legs_json", DataType::Utf8, true),
            Field::new("transfer_points_json", DataType::Utf8, true),
            Field::new("baggage_code", DataType::Utf8, true),
            Field::new("baggage_known", DataType::Boolean, true),
            Field::new("baggage_included", DataType::Boolean, true),
            Field::new("baggage_pieces", DataType::Int16, true),
            Field::new("baggage_kg", DataType::Int16, true),
            Field::new("source", DataType::Utf8, true),
        ];
        let cols: Vec<ArrayRef> = vec![
            s(&|t| t.search_origin.clone()),
            s(&|t| t.search_destination.clone()),
            s(&|t| t.search_date.clone()),
            s(&|t| t.origin.clone()),
            s(&|t| t.destination.clone()),
            s(&|t| t.origin_airport.clone()),
            s(&|t| t.destination_airport.clone()),
            s(&|t| t.departure_at.clone()),
            s(&|t| t.arrival_at.clone()),
            Arc::new(Int32Array::from(rows.iter().map(|t| t.duration.map(|d| d as i32)).collect::<Vec<_>>())),
            Arc::new(Int32Array::from(rows.iter().map(|t| t.duration_to.map(|d| d as i32)).collect::<Vec<_>>())),
            Arc::new(Int16Array::from(rows.iter().map(|t| Some(t.transfers as i16)).collect::<Vec<_>>())),
            s(&|t| t.airline.clone()),
            s(&|t| t.flight_number.clone()),
            Arc::new(Float64Array::from(rows.iter().map(|t| t.price).collect::<Vec<_>>())),
            s(&|t| t.currency.clone()),
            s(&|t| t.link.clone()),
            s(&|t| serde_json::to_string(&t.chain).ok()),
            s(&|t| serde_json::to_string(&t.legs).ok()),
            s(&|t| serde_json::to_string(&t.transfer_points.clone().unwrap_or_default()).ok()),
            s(&|t| t.baggage_code.clone()),
            Arc::new(BooleanArray::from(rows.iter().map(|t| t.baggage.as_ref().map(|b| b.known)).collect::<Vec<_>>())),
            Arc::new(BooleanArray::from(rows.iter().map(|t| t.baggage.as_ref().map(|b| b.included)).collect::<Vec<_>>())),
            Arc::new(Int16Array::from(rows.iter().map(|t| t.baggage.as_ref().and_then(|b| b.pieces).map(|p| p as i16)).collect::<Vec<_>>())),
            Arc::new(Int16Array::from(rows.iter().map(|t| t.baggage.as_ref().and_then(|b| b.kg).map(|p| p as i16)).collect::<Vec<_>>())),
            s(&|t| t.source.clone()),
        ];
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
        let mut buf = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }
        buf
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use super::testing::lake_ipc;

    #[test]
    fn ipc_roundtrip() {
        let t = crate::collect::tests::ticket(&["SVO", "PEK", "ICN"], "MOW", "SEL", "2026-10-15", 36127.0);
        let mut t2 = t.clone();
        t2.baggage = Some(Baggage { known: true, included: true, pieces: Some(1), kg: Some(23) });
        let bytes = lake_ipc(&[t.clone(), t2.clone()]);
        let back = tickets_from_ipc(&bytes).unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].chain, t.chain);
        assert_eq!(back[0].legs, t.legs);
        assert_eq!(back[0].transfer_points, t.transfer_points);
        assert_eq!(back[0].price, Some(36127.0));
        assert_eq!(back[1].baggage.as_ref().unwrap().kg, Some(23));
        assert_eq!(back[0].transfers, 1);
        assert!(tickets_from_ipc(&lake_ipc(&[])).unwrap().is_empty());
    }
}
