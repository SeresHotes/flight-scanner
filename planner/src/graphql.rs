//! Прямой клиент GraphQL Data API Travelpayouts (`prices_one_way`) — локальный запуск
//! без коллектора (зеркало `core.graphql_api`): страницы по 400, 60 запросов/мин,
//! нормализация билета в тот же словарь, что и озеро коллектора; кэш серий в памяти
//! (`hot.ticket_cache`, TTL суток).

use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::collect::{CollectError, SeriesFetcher, SeriesResult};
use crate::dates::minutes_between_aware;
use crate::hot;
use crate::ticket::{Baggage, Leg, Ticket, TransferPoint};

pub const GRAPHQL_URL: &str = "https://api.travelpayouts.com/graphql/v1/query";
pub const PAGE_LIMIT: i64 = 400;
pub const MAX_PAGES: i64 = 100;
pub const MAX_OFFSET: i64 = 14_800;
pub const MIN_INTERVAL_SECONDS: f64 = 1.0;
pub const REQUEST_TIMEOUT: u64 = 30;
pub const SOURCE: &str = "graphql";

const QUERY: &str = r#"
query Tickets($p: ParamsOneWay!, $limit: Int!, $offset: Int!) {
  prices_one_way(params: $p, paging: {limit: $limit, offset: $offset},
                 grouping: NONE, sorting: VALUE_ASC, currency: "rub") {
    value currency main_airline baggage_code with_baggage number_of_changes
    departure_at origin_city_iata destination_city_iata ticket_link
    segments {
      transfers { at to country_code duration_seconds visa_required night_transfer }
      flight_legs { origin destination departure_at arrival_at flight_number operating_carrier }
    }
  }
}
"#;

pub fn require_token() -> Result<String, CollectError> {
    std::env::var("TRAVELPAYOUTS_TOKEN").ok().filter(|s| !s.is_empty()).ok_or_else(|| CollectError::Failed("TRAVELPAYOUTS_TOKEN не найден в окружении/.env".into()))
}

/// `ParamsOneWay` для запроса: пустой конец — «ANY», хотя бы один конец обязателен.
pub fn build_params(origin: Option<&str>, destination: Option<&str>, day: &str) -> Result<Value, CollectError> {
    if origin.is_none() && destination.is_none() {
        return Err(CollectError::Failed("нужен хотя бы один из origin/destination".into()));
    }
    let mut p = json!({"depart_date_min": day, "depart_date_max": day});
    if let Some(o) = origin {
        p["origin"] = json!(o.to_uppercase());
    }
    if let Some(d) = destination {
        p["destination"] = json!(d.to_uppercase());
    }
    Ok(p)
}

static LAST_REQUEST: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();

/// Держим ≥ MIN_INTERVAL_SECONDS между реальными обращениями (60/мин).
fn throttle() {
    let last = LAST_REQUEST.get_or_init(|| Mutex::new(None));
    let mut guard = last.lock().unwrap();
    if let Some(t) = *guard {
        let elapsed = t.elapsed().as_secs_f64();
        if elapsed < MIN_INTERVAL_SECONDS {
            std::thread::sleep(Duration::from_secs_f64(MIN_INTERVAL_SECONDS - elapsed));
        }
    }
    *guard = Some(Instant::now());
}

/// Одна страница сырых билетов.
pub fn query_page(http: &reqwest::blocking::Client, params: &Value, offset: i64, limit: i64) -> Result<Vec<Value>, CollectError> {
    throttle();
    let body = json!({"query": QUERY, "variables": {"p": params, "limit": limit, "offset": offset}});
    let resp = http
        .post(GRAPHQL_URL)
        .header("X-Access-Token", require_token()?)
        .header("Content-Type", "application/json")
        .json(&body)
        .timeout(Duration::from_secs(REQUEST_TIMEOUT))
        .send()
        .map_err(|e| CollectError::Failed(format!("graphql: {e}")))?;
    let status = resp.status();
    if status.as_u16() == 429 {
        let retry_after = resp.headers().get("retry-after").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<f64>().ok());
        return Err(CollectError::RateLimited(retry_after));
    }
    let payload: Value = resp.json().unwrap_or(Value::Null);
    if let Some(errors) = payload.get("errors").filter(|e| !e.is_null() && !(e.is_array() && e.as_array().unwrap().is_empty())) {
        return Err(CollectError::Failed(errors.to_string().chars().take(500).collect()));
    }
    if status.as_u16() >= 400 {
        return Err(CollectError::Failed(format!("graphql: HTTP {}", status.as_u16())));
    }
    Ok(payload.get("data").and_then(|d| d.get("prices_one_way")).and_then(|v| v.as_array()).cloned().unwrap_or_default())
}

/// Серия: все страницы одного под-запроса (направление × день).
pub fn fetch_series(http: &reqwest::blocking::Client, origin: Option<&str>, destination: Option<&str>, day: &str, max_pages: i64, on_page: &dyn Fn(i64, i64)) -> Result<SeriesResult, CollectError> {
    let params = build_params(origin, destination, day)?;
    let mut out = SeriesResult::default();
    while out.pages < max_pages {
        let offset = out.pages * PAGE_LIMIT;
        if offset > MAX_OFFSET {
            break;
        }
        let raw = match query_page(http, &params, offset, PAGE_LIMIT) {
            Ok(r) => r,
            Err(e @ CollectError::RateLimited(_)) => return Err(e), // как в Python: 429 роняет джобу
            Err(e) => {
                eprintln!("[graphql] {}→{} {day} стр. {}: {e}", origin.unwrap_or("ANY"), destination.unwrap_or("ANY"), out.pages + 1);
                out.error = true;
                break;
            }
        };
        out.pages += 1;
        let n = raw.len() as i64;
        for t in &raw {
            if let Some(f) = normalize_ticket(t, origin, destination, day) {
                out.tickets.push(f);
            }
        }
        on_page(out.pages, out.tickets.len() as i64);
        if n < PAGE_LIMIT {
            out.exhausted = true;
            break;
        }
    }
    Ok(out)
}

/// `1PC23` → {known, included=True, pieces=1, kg=23}; `0PC` — без багажа; пусто — по with_baggage.
pub fn parse_baggage_code(code: Option<&str>, with_baggage: Option<bool>) -> Baggage {
    let code = code.unwrap_or("").trim().to_uppercase();
    if let Some((pieces_s, kg_s)) = code.split_once("PC") {
        let digits = |x: &str| x.chars().all(|c| c.is_ascii_digit());
        if !pieces_s.is_empty() && digits(pieces_s) && digits(kg_s) {
            let pieces: i64 = pieces_s.parse().unwrap_or(0);
            return Baggage {
                known: true,
                included: pieces > 0,
                pieces: if pieces > 0 { Some(pieces) } else { None },
                kg: if kg_s.is_empty() { None } else { kg_s.parse().ok() },
            };
        }
    }
    match with_baggage {
        Some(w) => Baggage { known: true, included: w, pieces: None, kg: None },
        None => Baggage { known: false, included: false, pieces: None, kg: None },
    }
}

fn s(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(|x| x.to_string())
}

/// Сырой билет GraphQL → нормализованный (None — без сегментов).
pub fn normalize_ticket(raw: &Value, search_origin: Option<&str>, search_destination: Option<&str>, search_date: &str) -> Option<Ticket> {
    let segments = raw.get("segments").and_then(|v| v.as_array())?;
    let first = segments.first()?;
    let legs_raw = first.get("flight_legs").and_then(|v| v.as_array()).filter(|a| !a.is_empty())?;
    let transfers_raw = first.get("transfers").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    let legs: Vec<Leg> = legs_raw
        .iter()
        .map(|l| Leg { origin: s(l, "origin"), destination: s(l, "destination"), departure_at: s(l, "departure_at"), arrival_at: s(l, "arrival_at"), flight_number: s(l, "flight_number"), carrier: s(l, "operating_carrier") })
        .collect();
    let mut chain: Vec<String> = vec![legs[0].origin.clone().unwrap_or_default()];
    for l in &legs {
        let o = l.origin.clone().unwrap_or_default();
        if chain.last() != Some(&o) {
            chain.push(o);
        }
        chain.push(l.destination.clone().unwrap_or_default());
    }
    let departure_at = s(raw, "departure_at").or_else(|| legs[0].departure_at.clone());
    let arrival_at = legs.last().unwrap().arrival_at.clone();
    let duration = minutes_between_aware(departure_at.as_deref().unwrap_or(""), arrival_at.as_deref().unwrap_or(""));
    let air: Vec<Option<i64>> = legs.iter().map(|l| minutes_between_aware(l.departure_at.as_deref().unwrap_or(""), l.arrival_at.as_deref().unwrap_or(""))).collect();
    let duration_to = if air.iter().all(|m| m.is_some()) { Some(air.iter().map(|m| m.unwrap()).sum()) } else { None };
    let transfer_points: Vec<TransferPoint> = transfers_raw
        .iter()
        .map(|t| TransferPoint {
            code: s(t, "at"),
            to: s(t, "to").or_else(|| s(t, "at")),
            country: s(t, "country_code"),
            minutes: t.get("duration_seconds").and_then(|v| v.as_i64()).map(|sec| sec / 60),
            night: t.get("night_transfer").and_then(|v| v.as_bool()).unwrap_or(false),
            visa: t.get("visa_required").and_then(|v| v.as_bool()).unwrap_or(false),
        })
        .collect();
    let transfers = raw.get("number_of_changes").and_then(|v| v.as_i64()).unwrap_or_else(|| if transfers_raw.is_empty() { legs.len() as i64 - 1 } else { transfers_raw.len() as i64 });
    let mut link = s(raw, "ticket_link").unwrap_or_default();
    if !link.is_empty() && !link.starts_with("/search") {
        link = format!("/search{link}");
    }
    Some(Ticket {
        origin: s(raw, "origin_city_iata").or_else(|| search_origin.map(|x| x.to_string())),
        destination: s(raw, "destination_city_iata").or_else(|| search_destination.map(|x| x.to_string())),
        origin_airport: chain.first().cloned(),
        destination_airport: chain.last().cloned(),
        departure_at,
        arrival_at,
        duration,
        duration_to,
        transfers,
        airline: s(raw, "main_airline"),
        flight_number: legs[0].flight_number.clone(),
        price: raw.get("value").and_then(|v| v.as_f64()),
        currency: Some(s(raw, "currency").unwrap_or_else(|| "rub".into())),
        link: Some(link),
        chain,
        legs,
        transfer_points: Some(transfer_points),
        baggage: Some(parse_baggage_code(raw.get("baggage_code").and_then(|v| v.as_str()), raw.get("with_baggage").and_then(|v| v.as_bool()))),
        baggage_code: s(raw, "baggage_code"),
        source: Some(SOURCE.into()),
        search_origin: search_origin.map(|x| x.to_string()),
        search_destination: search_destination.map(|x| x.to_string()),
        search_date: Some(search_date.to_string()),
        hidden_city: None,
        layover_minutes: None,
        src: None,
    })
}

/// Источник серий без коллектора: прямой GraphQL с TTL-кэшем серий в памяти (`hot`).
pub struct DirectFetcher {
    http: reqwest::blocking::Client,
    ttl_seconds: f64,
}

impl DirectFetcher {
    pub fn new(ttl_seconds: f64) -> DirectFetcher {
        DirectFetcher { http: reqwest::blocking::Client::builder().build().expect("reqwest"), ttl_seconds }
    }
}

impl SeriesFetcher for DirectFetcher {
    fn fetch(&self, origin: Option<&str>, dest: Option<&str>, day: &str, max_pages: i64, on_page: &dyn Fn(i64, i64)) -> Result<SeriesResult, CollectError> {
        if let Some(cached) = hot::ticket_cache_get(origin, dest, day, "", self.ttl_seconds, Some(max_pages)) {
            return Ok(cached);
        }
        let series = fetch_series(&self.http, origin, dest, day, max_pages, on_page)?;
        if !series.error {
            hot::ticket_cache_put(origin, dest, day, "", &series);
        }
        Ok(series)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn baggage_codes() {
        let b = parse_baggage_code(Some("1PC23"), None);
        assert!(b.known && b.included && b.pieces == Some(1) && b.kg == Some(23));
        let b = parse_baggage_code(Some("0PC"), Some(true));
        assert!(b.known && !b.included && b.pieces.is_none());
        let b = parse_baggage_code(Some("1PC"), None);
        assert!(b.included && b.kg.is_none());
        let b = parse_baggage_code(None, Some(true));
        assert!(b.known && b.included);
        assert!(!parse_baggage_code(Some(""), None).known);
    }

    #[test]
    fn normalizes_fixture() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/fixtures/graphql_tickets.json");
        let text = std::fs::read_to_string(path).unwrap();
        let v: Value = serde_json::from_str(&text).unwrap();
        let raw = &v["mow_sel"][0];
        let t = normalize_ticket(raw, Some("MOW"), Some("SEL"), "2026-10-15").unwrap();
        assert_eq!(t.origin.as_deref(), Some("MOW"));
        assert_eq!(t.chain, vec!["DME", "SVX", "HRB", "FOC", "ICN"]);
        assert_eq!(t.origin_airport.as_deref(), Some("DME"));
        assert_eq!(t.destination_airport.as_deref(), Some("ICN"));
        assert_eq!(t.transfers, 3);
        assert_eq!(t.price, Some(36127.0));
        assert_eq!(t.duration, Some(3150));
        assert_eq!(t.transfer_points.as_ref().unwrap()[0].minutes, Some(150));
        assert!(t.transfer_points.as_ref().unwrap()[1].night);
        assert!(t.link.as_deref().unwrap().starts_with("/search/MOW1510SEL1?t="));
        assert!(!t.baggage.as_ref().unwrap().included);
        let t2 = normalize_ticket(&v["mow_sel"][1], Some("MOW"), Some("SEL"), "2026-10-15").unwrap();
        assert_eq!(t2.baggage.as_ref().unwrap().kg, Some(15));
        assert!(build_params(None, None, "2026-10-15").is_err());
    }
}
