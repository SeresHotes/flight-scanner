//! Склад билетов для сбора джобы (docs/TICKETS.md): текущее состояние серий X→ANY —
//! в памяти планировщика (`lakestore`, наполняет `lakesync` из озера). Сбор берёт рейсы
//! отсюда одним запросом на плечо, а в GraphQL через коллектор ходит только за городами и
//! днями, которых в складе нет (`coverage`).

use std::collections::{HashMap, HashSet};

use crate::collect::CollectError;
use crate::flightcols::{Fl, JobCodes};
use crate::ticket::Ticket;

/// Потолка строк выборки склада нет (02.10.2026): ограничивает только память VM. Кончилась —
/// ядро убивает контейнер планировщика (`mem_limit` в compose.prod.yml), docker поднимает
/// его заново (`restart: always`), склад — из снапшота за ~10 с.
pub const STORE_MAX_ROWS: usize = usize::MAX;

/// Источник рейсов из склада (реализует `lakestore::SharedStore`; тесты — заглушки).
pub trait TicketStore: Send + Sync {
    /// Билеты с днём вылета в [from, to] под фильтры (хотя бы один список непустой):
    /// города вылета, города прилёта, город среди пересадок.
    fn tickets(&self, origins: &[String], dests: &[String], via: &[String], from: &str, to: &str) -> Result<Vec<Ticket>, CollectError>;
    /// Серии склада за дни [from, to]: (город, день) → билетов; пустой список городов — все.
    fn coverage(&self, origins: &[String], from: &str, to: &str) -> Result<HashMap<(String, String), i64>, CollectError>;
    /// По дням [from, to]: сколько серий (городов) в складе.
    fn coverage_days(&self, from: &str, to: &str) -> Result<HashMap<String, i64>, CollectError>;
    /// Рейсы сбора (компактные строки) под фильтры городов вылета / прилёта за дни
    /// [from, to], в порядке `tickets`. По умолчанию — из полных билетов; склад в памяти
    /// копирует свои колонки без разбора билетов.
    fn flights(&self, origins: &[String], dests: &[String], from: &str, to: &str, codes: &JobCodes) -> Result<Vec<Fl>, CollectError> {
        Ok(self.tickets(origins, dests, &[], from, to)?.into_iter().map(|t| Fl::from_ticket(t, codes)).collect())
    }
    /// Словарь кодов склада (номер → код): коды джобы начинаются с него.
    fn code_names(&self) -> Vec<String> {
        Vec::new()
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
