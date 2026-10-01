//! Клиент склада билетов (`tickets/`, docs/TICKETS.md): текущее состояние серий X→ANY в
//! Postgres. Сбор джобы берёт рейсы отсюда одним запросом на плечо (Arrow IPC в схеме озера —
//! тот же разбор, что у ответа коллектора), а в GraphQL через коллектор ходит только за
//! городами и днями, которых в складе нет (`coverage`).

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use serde_json::Value;

use crate::collect::CollectError;
use crate::collector::tickets_from_ipc;
use crate::ticket::Ticket;

/// Плечи «любой → любой» тянут сотни тысяч строк — ждём долго.
pub const DEFAULT_TIMEOUT: f64 = 900.0;
/// Потолок строк одного запроса к складу: больше — запрос слишком широкий, джоба падает с
/// понятной ошибкой (вместо молчаливого усечения).
pub const STORE_MAX_ROWS: usize = 1_000_000;

pub fn tickets_url() -> Option<String> {
    std::env::var("TICKETS_URL").ok().filter(|s| !s.is_empty())
}

/// Источник рейсов из склада (реализует клиент; тесты — заглушки).
pub trait TicketStore: Send + Sync {
    /// Билеты с днём вылета в [from, to] под фильтры (хотя бы один список непустой):
    /// города вылета, города прилёта, город среди пересадок.
    fn tickets(&self, origins: &[String], dests: &[String], via: &[String], from: &str, to: &str) -> Result<Vec<Ticket>, CollectError>;
    /// Серии склада за дни [from, to]: (город, день) → билетов; пустой список городов — все.
    fn coverage(&self, origins: &[String], from: &str, to: &str) -> Result<HashMap<(String, String), i64>, CollectError>;
    /// По дням [from, to]: сколько серий (городов) в складе.
    fn coverage_days(&self, from: &str, to: &str) -> Result<HashMap<String, i64>, CollectError>;
}

#[derive(Clone)]
pub struct TicketsClient {
    base: String,
    http: reqwest::blocking::Client,
    timeout: f64,
}

impl TicketsClient {
    pub fn new(base_url: &str) -> TicketsClient {
        TicketsClient { base: base_url.trim_end_matches('/').to_string(), http: reqwest::blocking::Client::builder().build().expect("reqwest"), timeout: DEFAULT_TIMEOUT }
    }

    fn get(&self, path: &str, params: &[(&str, String)]) -> Result<reqwest::blocking::Response, CollectError> {
        let r = self.http.get(format!("{}{path}", self.base)).query(params).timeout(Duration::from_secs_f64(self.timeout)).send().map_err(|e| CollectError::Failed(format!("склад билетов недоступен: {e}")))?;
        let status = r.status();
        if status.as_u16() >= 400 {
            let text = r.text().unwrap_or_default();
            return Err(CollectError::Failed(format!("склад билетов {path}: HTTP {} {}", status.as_u16(), text.chars().take(200).collect::<String>())));
        }
        Ok(r)
    }

    pub fn health(&self) -> Result<Value, CollectError> {
        self.get("/v1/health", &[])?.json().map_err(|e| CollectError::Failed(format!("склад билетов health: {e}")))
    }
}

impl TicketStore for TicketsClient {
    fn tickets(&self, origins: &[String], dests: &[String], via: &[String], from: &str, to: &str) -> Result<Vec<Ticket>, CollectError> {
        let mut params: Vec<(&str, String)> = vec![("from", from.to_string()), ("to", to.to_string())];
        if !origins.is_empty() {
            params.push(("origin", origins.join(",")));
        }
        if !dests.is_empty() {
            params.push(("destination", dests.join(",")));
        }
        if !via.is_empty() {
            params.push(("via", via.join(",")));
        }
        params.push(("limit", STORE_MAX_ROWS.to_string()));
        let r = self.get("/v1/tickets", &params)?;
        let count: usize = r.headers().get("x-tickets-count").and_then(|v| v.to_str().ok()).and_then(|s| s.parse().ok()).unwrap_or(0);
        if count >= STORE_MAX_ROWS {
            let side = if !origins.is_empty() { format!("из {} городов", origins.len()) } else { format!("в {} городов", dests.len()) };
            return Err(CollectError::Failed(format!("Слишком широкий запрос: плечо {side} за {from}..{to} даёт больше {STORE_MAX_ROWS} рейсов. Сузьте окна дат или задайте города вместо «любых».")));
        }
        let bytes = r.bytes().map_err(|e| CollectError::Failed(format!("склад билетов tickets: {e}")))?;
        tickets_from_ipc(&bytes)
    }

    fn coverage(&self, origins: &[String], from: &str, to: &str) -> Result<HashMap<(String, String), i64>, CollectError> {
        let mut params: Vec<(&str, String)> = vec![("from", from.to_string()), ("to", to.to_string())];
        if !origins.is_empty() {
            params.push(("origin", origins.join(",")));
        }
        let v: Value = self.get("/v1/coverage", &params)?.json().map_err(|e| CollectError::Failed(format!("склад билетов coverage: {e}")))?;
        let mut out = HashMap::new();
        for s in v.get("series").and_then(|s| s.as_array()).map(|a| a.as_slice()).unwrap_or(&[]) {
            let (Some(o), Some(d)) = (s.get("origin").and_then(|x| x.as_str()), s.get("day").and_then(|x| x.as_str())) else { continue };
            out.insert((o.to_uppercase(), d.to_string()), s.get("tickets").and_then(|x| x.as_i64()).unwrap_or(0));
        }
        Ok(out)
    }

    fn coverage_days(&self, from: &str, to: &str) -> Result<HashMap<String, i64>, CollectError> {
        let v: Value = self.get("/v1/coverage/days", &[("from", from.to_string()), ("to", to.to_string())])?.json().map_err(|e| CollectError::Failed(format!("склад билетов coverage/days: {e}")))?;
        let mut out = HashMap::new();
        for d in v.get("days").and_then(|s| s.as_array()).map(|a| a.as_slice()).unwrap_or(&[]) {
            if let Some(day) = d.get("day").and_then(|x| x.as_str()) {
                out.insert(day.to_string(), d.get("series").and_then(|x| x.as_i64()).unwrap_or(0));
            }
        }
        Ok(out)
    }
}

/// Покрытие склада под запрос: какие (город, день) X→ANY есть и по каким дням есть хоть
/// одна серия (ANY-плечи). Считается один раз на оценку/джобу.
#[derive(Debug, Default, Clone)]
pub struct Coverage {
    pub series: HashSet<(String, String)>,
    pub days: HashSet<String>,
}

impl Coverage {
    pub fn has_series(&self, origin: &str, day: &str) -> bool {
        self.series.contains(&(origin.to_uppercase(), day.to_string()))
    }
    pub fn has_day(&self, day: &str) -> bool {
        self.days.contains(day)
    }
}

/// Покрытие склада по городам вылета `origins` и дням [from, to] (для оценки и сбора).
pub fn coverage_for(store: &dyn TicketStore, origins: &[String], from: &str, to: &str) -> Result<Coverage, CollectError> {
    let mut out = Coverage::default();
    if !origins.is_empty() {
        out.series = store.coverage(origins, from, to)?.into_keys().collect();
    }
    out.days = store.coverage_days(from, to)?.into_iter().filter(|(_, n)| *n > 0).map(|(d, _)| d).collect();
    Ok(out)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Склад-заглушка: билеты по (origin, день); покрыты все дни и города, у которых есть записи.
    pub struct MapStore {
        pub by_series: Mutex<HashMap<(String, String), Vec<Ticket>>>,
        pub calls: Mutex<Vec<String>>,
    }

    impl MapStore {
        pub fn new() -> Self {
            MapStore { by_series: Mutex::new(HashMap::new()), calls: Mutex::new(Vec::new()) }
        }
        pub fn put(&self, origin: &str, day: &str, tickets: Vec<Ticket>) {
            self.by_series.lock().unwrap().insert((origin.into(), day.into()), tickets);
        }
    }

    impl TicketStore for MapStore {
        fn tickets(&self, origins: &[String], dests: &[String], via: &[String], from: &str, to: &str) -> Result<Vec<Ticket>, CollectError> {
            self.calls.lock().unwrap().push(format!("tickets o={} d={} via={} {from}..{to}", origins.join(","), dests.join(","), via.join(",")));
            let map = self.by_series.lock().unwrap();
            let mut out = Vec::new();
            for ((o, day), tickets) in map.iter() {
                if day.as_str() < from || day.as_str() > to {
                    continue;
                }
                if !origins.is_empty() && !origins.contains(o) {
                    continue;
                }
                for t in tickets {
                    if !dests.is_empty() && !t.side_codes(true).iter().any(|c| dests.contains(c)) {
                        continue;
                    }
                    if !via.is_empty() && !t.transfer_points.as_deref().unwrap_or(&[]).iter().any(|p| via.contains(&p.code.clone().unwrap_or_default())) {
                        continue;
                    }
                    out.push(t.clone());
                }
            }
            // как ORDER BY dep_day, origin, price у настоящего склада
            out.sort_by(|a, b| a.search_date.cmp(&b.search_date).then(a.origin.cmp(&b.origin)).then(a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal)));
            Ok(out)
        }

        fn coverage(&self, origins: &[String], from: &str, to: &str) -> Result<HashMap<(String, String), i64>, CollectError> {
            let map = self.by_series.lock().unwrap();
            Ok(map.iter().filter(|((o, d), _)| (origins.is_empty() || origins.contains(o)) && d.as_str() >= from && d.as_str() <= to).map(|(k, v)| (k.clone(), v.len() as i64)).collect())
        }

        fn coverage_days(&self, from: &str, to: &str) -> Result<HashMap<String, i64>, CollectError> {
            let map = self.by_series.lock().unwrap();
            let mut out: HashMap<String, i64> = HashMap::new();
            for (_, d) in map.keys() {
                if d.as_str() >= from && d.as_str() <= to {
                    *out.entry(d.clone()).or_default() += 1;
                }
            }
            Ok(out)
        }
    }
}
