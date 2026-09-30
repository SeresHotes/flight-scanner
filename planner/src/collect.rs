//! Сбор рейсов по плечам запроса (зеркало `core.planner.collect_plan`).
//!
//! Источник — серии «направление × день» (все билеты дня, страницы по 400):
//!   город → город   пары A×B, обычно одна страница; плюс hidden-city: вся A→ANY дня,
//!                   из неё — билеты через хаб B дешевле лучшего A→B того дня;
//!   город → любой   A→ANY по дням целиком; hidden-city выходит бесплатно:
//!                   билет A→H→X даёт и рейс A→H;
//!   любой → город   ANY→B по дням; hidden-city нет.
//! «Запрос» в оценке = страница; прогресс идёт по страницам и добивается до оценки
//! в конце серии, поэтому счётчик доходит ровно до total (и при попадании в кэш).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::stops::{collect_view, leg_dates, Stop, PAGES_ANY, PAGES_CITY};
use crate::ticket::Ticket;

pub const MAX_PAGES: i64 = 100;

/// Серия от источника (коллектор или прямой GraphQL).
#[derive(Debug, Default, Clone)]
pub struct SeriesResult {
    pub tickets: Vec<Ticket>,
    pub pages: i64,
    pub exhausted: bool,
    pub error: bool,
    pub cached: bool,
}

#[derive(Debug, Clone)]
pub enum CollectError {
    Cancelled,
    /// HTTP 429 источника: превышен лимит ручки (retry_after — секунды из заголовка).
    RateLimited(Option<f64>),
    Failed(String),
}

impl std::fmt::Display for CollectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CollectError::Cancelled => write!(f, "сбор отменён"),
            CollectError::RateLimited(after) => write!(f, "429 rate limited (retry after {})", after.map(|a| a.to_string()).unwrap_or_else(|| "None".into())),
            CollectError::Failed(s) => write!(f, "{s}"),
        }
    }
}

/// Источник серий: origin/dest — None для ANY; `on_page(page, tickets_so_far)` — после
/// каждой страницы (зовётся из потоков сбора).
pub trait SeriesFetcher: Send + Sync {
    fn fetch(&self, origin: Option<&str>, dest: Option<&str>, day: &str, max_pages: i64, on_page: &dyn Fn(i64, i64)) -> Result<SeriesResult, CollectError>;
}

/// Хуки прогресса сбора (реализует StageReporter воркера).
pub trait CollectProgress: Send + Sync {
    /// Тик на страницу (и добивку до оценки серии).
    fn tick(&self) -> Result<(), CollectError>;
    /// Перед началом плеча i.
    fn leg(&self, i: usize) -> Result<(), CollectError>;
}

pub struct NoProgress;
impl CollectProgress for NoProgress {
    fn tick(&self) -> Result<(), CollectError> {
        Ok(())
    }
    fn leg(&self, _i: usize) -> Result<(), CollectError> {
        Ok(())
    }
}

/// Одна серия целиком, без ценового коридора. Прогресс: тик на каждую страницу, в конце
/// добивка до `pages` (оценка серии).
fn run_series(fetch: &dyn SeriesFetcher, origin: Option<&str>, dest: Option<&str>, day: &str, pages: i64, progress: &dyn CollectProgress) -> Result<Vec<Ticket>, CollectError> {
    let ticks = AtomicUsize::new(0);
    let failed: Mutex<Option<CollectError>> = Mutex::new(None);
    let on_page = |_page: i64, _n: i64| {
        if (ticks.load(Ordering::Relaxed) as i64) < pages {
            ticks.fetch_add(1, Ordering::Relaxed);
            if let Err(e) = progress.tick() {
                *failed.lock().unwrap() = Some(e);
            }
        }
    };
    let series = fetch.fetch(origin, dest, day, pages.max(MAX_PAGES), &on_page)?;
    if let Some(e) = failed.into_inner().unwrap() {
        return Err(e);
    }
    let done = ticks.load(Ordering::Relaxed) as i64;
    for _ in done..pages {
        progress.tick()?;
    }
    Ok(series.tickets)
}

/// fetch_unit по каждой единице сбора; результаты — в порядке units. workers > 1 —
/// параллельно. Ошибка/отмена — снимаем ещё не начатые.
fn fetch_units<U: Send + Sync, R: Send>(units: &[U], workers: usize, fetch_unit: &(dyn Fn(&U) -> Result<R, CollectError> + Sync)) -> Result<Vec<R>, CollectError> {
    if workers <= 1 || units.len() <= 1 {
        return units.iter().map(fetch_unit).collect();
    }
    let n = units.len();
    let results: Mutex<Vec<Option<Result<R, CollectError>>>> = Mutex::new((0..n).map(|_| None).collect());
    let next = AtomicUsize::new(0);
    let abort = AtomicBool::new(false);
    std::thread::scope(|scope| {
        for _ in 0..workers.min(n) {
            scope.spawn(|| loop {
                if abort.load(Ordering::Relaxed) {
                    break;
                }
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= n {
                    break;
                }
                let r = fetch_unit(&units[i]);
                if r.is_err() {
                    abort.store(true, Ordering::Relaxed);
                }
                results.lock().unwrap()[i] = Some(r);
            });
        }
    });
    let mut out = Vec::with_capacity(n);
    for r in results.into_inner().unwrap() {
        match r {
            Some(Ok(v)) => out.push(v),
            Some(Err(e)) => return Err(e),
            None => return Err(CollectError::Failed("сбор прерван".into())),
        }
    }
    Ok(out)
}

/// Порог hidden-city по группе key(f): цена лучшего ПРЯМОГО обычного рейса, если
/// прямой есть, иначе лучшего вообще.
fn threshold<K: std::hash::Hash + Eq>(flights: &[&Ticket], key: &dyn Fn(&Ticket) -> K) -> HashMap<K, f64> {
    let mut best_direct: HashMap<K, f64> = HashMap::new();
    let mut best_any: HashMap<K, f64> = HashMap::new();
    for f in flights {
        if f.hidden_city.is_some() {
            continue;
        }
        let price = f.price();
        if f.transfers == 0 {
            let e = best_direct.entry(key(f)).or_insert(f64::INFINITY);
            if price < *e {
                *e = price;
            }
        }
        let e = best_any.entry(key(f)).or_insert(f64::INFINITY);
        if price < *e {
            *e = price;
        }
    }
    for (k, v) in best_direct {
        best_any.insert(k, v);
    }
    best_any
}

/// Пополняет карту аэропорт → город из концов билетов (PKX → BJS).
fn airport_city_learn(flights: &[Ticket], airport_city: &mut HashMap<String, String>) {
    for f in flights {
        if f.hidden_city.is_some() {
            continue;
        }
        for (apt, city) in [(&f.destination_airport, &f.destination), (&f.origin_airport, &f.origin)] {
            if let (Some(a), Some(c)) = (apt.as_deref().filter(|s| !s.is_empty()), city.as_deref().filter(|s| !s.is_empty())) {
                airport_city.entry(a.to_uppercase()).or_insert_with(|| c.to_uppercase());
            }
        }
    }
}

/// Виртуальные рейсы hidden-city из билетов с пересадками: для каждой пересадки, чей
/// аэропорт hub_city(apt) распознан как остановка, — рейс «до этой пересадки», если он
/// дешевле порога thresholds[key(рейс)] (без порога — все). Хаб ≠ город вылета.
pub fn hidden_city_flights<K: std::hash::Hash + Eq>(
    tickets: &[Ticket],
    hub_city: &dyn Fn(&str) -> Option<String>,
    thresholds: &HashMap<K, f64>,
    key: &dyn Fn(&Ticket) -> K,
    seen: &mut HashSet<String>,
) -> Vec<Ticket> {
    let mut out = Vec::new();
    for t in tickets {
        let Some(points) = t.transfer_points.as_ref().filter(|p| !p.is_empty()) else { continue };
        if t.legs.is_empty() || t.legs.len() < points.len() + 1 {
            continue;
        }
        for (k, tp) in points.iter().enumerate() {
            let code = tp.code.as_deref().unwrap_or("");
            let Some(city) = hub_city(code) else { continue };
            if city.is_empty() || Some(city.as_str()) == t.origin.as_deref() || Some(code) == t.origin_airport.as_deref() {
                continue;
            }
            let v = t.virtual_flight(k, &city);
            if let Some(thr) = thresholds.get(&key(&v)) {
                if v.price() >= *thr {
                    continue;
                }
            }
            let fk = v.flight_key();
            if !seen.insert(fk) {
                continue;
            }
            out.push(v);
        }
    }
    out
}

fn side_matches(f: &Ticket, dest: bool, allow: &HashSet<String>) -> bool {
    f.side_codes(dest).iter().any(|c| allow.contains(c))
}

/// Собирает рейсы по каждому переходу. Возвращает по плечам: обычные билеты +
/// виртуальные hidden-city. `airport_city` — карта аэропорт → город из накопленных
/// котировок (дополняется по ходу сбора); `workers` — сколько серий плеча качать
/// одновременно. Разбор — всегда в порядке обхода, результат не зависит от workers.
pub fn collect_plan(
    stops: &[Stop],
    fetch: &dyn SeriesFetcher,
    progress: &dyn CollectProgress,
    airport_city: &mut HashMap<String, String>,
    workers: usize,
) -> Result<Vec<Vec<Arc<Ticket>>>, CollectError> {
    let stops = collect_view(stops);
    let mut collected: Vec<Vec<Arc<Ticket>>> = Vec::new();
    for i in 0..stops.len().saturating_sub(1) {
        progress.leg(i)?;
        let (from, to) = (&stops[i], &stops[i + 1]);
        let dates = leg_dates(&stops, i);
        let mut regular: Vec<Ticket> = Vec::new();
        let mut virtual_: Vec<Ticket> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();

        let keep = |flights: Vec<Ticket>, allow_dest: Option<&HashSet<String>>, regular: &mut Vec<Ticket>, seen: &mut HashSet<String>, airport_city: &mut HashMap<String, String>| {
            airport_city_learn(&flights, airport_city);
            for f in flights {
                let key = f.flight_key();
                if seen.contains(&key) {
                    continue;
                }
                if let Some(allow) = allow_dest {
                    if !side_matches(&f, true, allow) {
                        continue;
                    }
                }
                seen.insert(key);
                regular.push(f);
            }
        };

        if from.is_cities() && to.is_cities() {
            let allow_to: HashSet<String> = to.codes.iter().cloned().collect();
            let units: Vec<(String, String)> = dates.iter().flat_map(|d| from.codes.iter().map(move |a| (d.clone(), a.clone()))).collect();
            let to_codes = to.codes.clone();
            let unit = |u: &(String, String)| -> Result<(Vec<Vec<Ticket>>, Vec<Ticket>), CollectError> {
                let (day, a) = u;
                let mut direct = Vec::new();
                for b in &to_codes {
                    direct.push(run_series(fetch, Some(a), Some(b), day, PAGES_CITY, progress)?);
                }
                let hidden = run_series(fetch, Some(a), None, day, PAGES_ANY, progress)?;
                Ok((direct, hidden))
            };
            for (direct, hidden) in fetch_units(&units, workers, &unit)? {
                let all_direct: Vec<Ticket> = direct.into_iter().flatten().collect();
                let refs: Vec<&Ticket> = all_direct.iter().collect();
                let thr = threshold(&refs, &|_| 0u8).get(&0).copied();
                keep(all_direct, Some(&allow_to), &mut regular, &mut seen, airport_city);
                airport_city_learn(&hidden, airport_city);
                let thresholds: HashMap<u8, f64> = match thr {
                    Some(t) if t != 0.0 => HashMap::from([(0u8, t)]),
                    _ => HashMap::new(),
                };
                let ac = &*airport_city;
                let hub = |apt: &str| -> Option<String> {
                    if allow_to.contains(apt) {
                        return Some(apt.to_string());
                    }
                    ac.get(apt).filter(|c| allow_to.contains(*c)).cloned()
                };
                virtual_.extend(hidden_city_flights(&hidden, &hub, &thresholds, &|_| 0u8, &mut seen));
            }
        } else if from.is_cities() {
            let units: Vec<(String, String)> = dates.iter().flat_map(|d| from.codes.iter().map(move |a| (d.clone(), a.clone()))).collect();
            let unit = |u: &(String, String)| run_series(fetch, Some(&u.1), None, &u.0, PAGES_ANY, progress);
            let got_all = fetch_units(&units, workers, &unit)?;
            let mut day_flights: Vec<(String, Vec<Ticket>)> = Vec::new();
            for ((day, _a), got) in units.iter().zip(got_all) {
                keep(got.clone(), None, &mut regular, &mut seen, airport_city);
                match day_flights.iter_mut().find(|(d, _)| d == day) {
                    Some((_, v)) => v.extend(got),
                    None => day_flights.push((day.clone(), got)),
                }
            }
            let key = |f: &Ticket| (f.origin.clone().unwrap_or_default(), f.destination.clone().unwrap_or_default());
            for (_day, flights) in &day_flights {
                let refs: Vec<&Ticket> = flights.iter().collect();
                let thr = threshold(&refs, &key);
                let ac = &*airport_city;
                let hub = |apt: &str| -> Option<String> { Some(ac.get(apt).cloned().unwrap_or_else(|| apt.to_string())) };
                virtual_.extend(hidden_city_flights(flights, &hub, &thr, &key, &mut seen));
            }
        } else {
            let units: Vec<(String, String)> = dates.iter().flat_map(|d| to.codes.iter().map(move |b| (d.clone(), b.clone()))).collect();
            let unit = |u: &(String, String)| run_series(fetch, None, Some(&u.1), &u.0, PAGES_ANY, progress);
            let allow: HashSet<String> = to.codes.iter().cloned().collect();
            for got in fetch_units(&units, workers, &unit)? {
                keep(got, Some(&allow), &mut regular, &mut seen, airport_city);
            }
        }
        let mut leg: Vec<Arc<Ticket>> = regular.into_iter().map(Arc::new).collect();
        leg.extend(virtual_.into_iter().map(Arc::new));
        collected.push(leg);
    }
    Ok(collected)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::ticket::{Leg, TransferPoint};

    /// Билет с цепочкой аэропортов; каждая пересадка — leg.
    pub fn ticket(chain: &[&str], origin_city: &str, dest_city: &str, day: &str, price: f64) -> Ticket {
        let legs: Vec<Leg> = chain
            .windows(2)
            .enumerate()
            .map(|(k, w)| Leg {
                origin: Some(w[0].into()),
                destination: Some(w[1].into()),
                departure_at: Some(format!("{day}T{:02}:00:00+03:00", 8 + k * 4)),
                arrival_at: Some(format!("{day}T{:02}:00:00+03:00", 10 + k * 4)),
                flight_number: Some(format!("{}{}", chain[0], k)),
                carrier: None,
            })
            .collect();
        let points: Vec<TransferPoint> = chain[1..chain.len() - 1]
            .iter()
            .map(|c| TransferPoint { code: Some(c.to_string()), to: Some(c.to_string()), minutes: Some(120), ..Default::default() })
            .collect();
        Ticket {
            origin: Some(origin_city.into()),
            destination: Some(dest_city.into()),
            origin_airport: Some(chain[0].into()),
            destination_airport: Some(chain[chain.len() - 1].into()),
            departure_at: legs[0].departure_at.clone(),
            arrival_at: legs.last().unwrap().arrival_at.clone(),
            duration: Some(120 + 240 * (legs.len() as i64 - 1)),
            transfers: (chain.len() - 2) as i64,
            price: Some(price),
            chain: chain.iter().map(|s| s.to_string()).collect(),
            legs,
            transfer_points: Some(points),
            search_date: Some(day.into()),
            ..Default::default()
        }
    }

    pub struct MapFetcher(pub Mutex<HashMap<(String, String, String), Vec<Ticket>>>, pub Mutex<Vec<(String, String, String)>>);

    impl MapFetcher {
        pub fn new() -> Self {
            MapFetcher(Mutex::new(HashMap::new()), Mutex::new(Vec::new()))
        }
        pub fn put(&self, o: Option<&str>, d: Option<&str>, day: &str, tickets: Vec<Ticket>) {
            self.0.lock().unwrap().insert((o.unwrap_or("").into(), d.unwrap_or("").into(), day.into()), tickets);
        }
    }

    impl SeriesFetcher for MapFetcher {
        fn fetch(&self, origin: Option<&str>, dest: Option<&str>, day: &str, _max_pages: i64, on_page: &dyn Fn(i64, i64)) -> Result<SeriesResult, CollectError> {
            self.1.lock().unwrap().push((origin.unwrap_or("").into(), dest.unwrap_or("").into(), day.into()));
            let tickets = self.0.lock().unwrap().get(&(origin.unwrap_or("").into(), dest.unwrap_or("").into(), day.into())).cloned().unwrap_or_default();
            on_page(1, tickets.len() as i64);
            Ok(SeriesResult { tickets, pages: 1, exhausted: true, error: false, cached: false })
        }
    }

    struct Counter(AtomicUsize);
    impl CollectProgress for Counter {
        fn tick(&self) -> Result<(), CollectError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        fn leg(&self, _i: usize) -> Result<(), CollectError> {
            Ok(())
        }
    }

    #[test]
    fn city_to_city_with_hidden_city() {
        let f = MapFetcher::new();
        f.put(Some("MOW"), Some("BJS"), "2026-11-01", vec![ticket(&["SVO", "PEK"], "MOW", "BJS", "2026-11-01", 20000.0)]);
        f.put(
            Some("MOW"),
            None,
            "2026-11-01",
            vec![
                ticket(&["SVO", "PEK", "SIN"], "MOW", "SIN", "2026-11-01", 15000.0), // дешевле прямого → виртуальный MOW→BJS
                ticket(&["SVO", "PEK", "HKG"], "MOW", "HKG", "2026-11-01", 25000.0), // дороже → нет
                ticket(&["SVO", "PEK"], "MOW", "BJS", "2026-11-01", 20000.0),        // дубль прямого
            ],
        );
        let stops = vec![Stop::new("cities", vec!["MOW"], ["", ""]), Stop::new("cities", vec!["BJS"], ["2026-11-01", "2026-11-01"])];
        let mut ac = HashMap::new();
        let progress = Counter(AtomicUsize::new(0));
        let got = collect_plan(&stops, &f, &progress, &mut ac, 1).unwrap();
        assert_eq!(got.len(), 1);
        let leg = &got[0];
        assert_eq!(leg.len(), 2, "прямой + один виртуальный");
        let v = leg.iter().find(|t| t.hidden_city.is_some()).unwrap();
        assert_eq!(v.destination.as_deref(), Some("BJS"));
        assert_eq!(v.price(), 15000.0);
        assert_eq!(ac.get("PEK").map(String::as_str), Some("BJS"));
        assert_eq!(progress.0.load(Ordering::Relaxed), (PAGES_CITY + PAGES_ANY) as usize);
    }

    #[test]
    fn city_to_any_and_any_to_city() {
        let f = MapFetcher::new();
        f.put(Some("MOW"), None, "2026-11-01", vec![ticket(&["SVO", "IST", "DXB"], "MOW", "DXB", "2026-11-01", 10000.0), ticket(&["SVO", "IST"], "MOW", "IST", "2026-11-01", 12000.0)]);
        // окно плеча 1 — от остановки «любой» (2026-11-01), а не от финала
        f.put(None, Some("MOW"), "2026-11-01", vec![ticket(&["DXB", "SVO"], "DXB", "MOW", "2026-11-01", 9000.0), ticket(&["IST", "SVO"], "IST", "MOW", "2026-11-01", 8000.0)]);
        let stops = vec![
            Stop::new("cities", vec!["MOW"], ["", ""]),
            Stop::new("any", vec![], ["2026-11-01", "2026-11-01"]),
            Stop::new("cities", vec!["MOW"], ["2026-11-03", "2026-11-03"]),
        ];
        let mut ac = HashMap::new();
        let got = collect_plan(&stops, &f, &NoProgress, &mut ac, 4).unwrap();
        assert_eq!(got[0].len(), 3, "два обычных + виртуальный MOW→IST (10000 < 12000)");
        assert_eq!(got[1].len(), 2);
        let calls = f.1.lock().unwrap().len();
        assert_eq!(calls, 2);
    }
}
