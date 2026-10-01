//! Сбор рейсов по плечам запроса.
//!
//! Источник — склад билетов (`tickets.rs`, docs/TICKETS.md): текущие серии X→ANY всех
//! городов, один запрос на плечо; серии «направление × день» через коллектор/GraphQL —
//! только за городами и днями, которых в складе нет (без склада — всё сериями, как раньше):
//!   город → город   все билеты A за дни окна (A→ANY): прямые A→B плюс hidden-city — через
//!                   хаб B дешевле лучшего A→B того дня;
//!   город → любой   A→ANY целиком; hidden-city выходит бесплатно: билет A→H→X даёт и рейс A→H;
//!   любой → город   ANY→B — срез склада по городу прилёта (дни без покрытия — серией ANY→B);
//!   любой → любой   только склад: вылеты из городов прилёта предыдущего плеча, прилёты — в
//!                   города вылета следующего; собирается после плеч с конкретным городом.
//! «Запрос» в оценке = страница; прогресс идёт по страницам и добивается до оценки
//! в конце серии (единицы из склада засчитываются сразу), поэтому счётчик доходит ровно до total.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::stops::{collect_view, leg_dates, Stop, PAGES_ANY, PAGES_CITY};
use crate::ticket::Ticket;
use crate::tickets::{coverage_for, Coverage, TicketStore};

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
    /// n серий плеча взяты из склада билетов (в степпере — «из кэша»).
    fn store_hit(&self, _n: usize) {}
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

/// Серия взята из склада: прогресс сразу на всю её оценку.
fn tick_n(progress: &dyn CollectProgress, pages: i64) -> Result<(), CollectError> {
    for _ in 0..pages {
        progress.tick()?;
    }
    Ok(())
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

/// Город серии билета: город запроса X→ANY (search_origin), иначе город вылета.
fn series_city(t: &Ticket) -> String {
    t.search_origin.as_deref().filter(|s| !s.is_empty()).or(t.origin_city()).unwrap_or("").to_uppercase()
}

fn dep_day(t: &Ticket) -> String {
    t.search_date.as_deref().filter(|s| s.len() >= 10).or(t.departure_at.as_deref().filter(|s| s.len() >= 10)).map(|s| s[..10].to_string()).unwrap_or_default()
}

/// Рейсы склада, разложенные по единицам сбора (день, город запроса) — в порядке units.
fn split_units(tickets: Vec<Ticket>, units: &[(String, String)]) -> Vec<Vec<Ticket>> {
    let mut idx: HashMap<(String, String), usize> = HashMap::new();
    for (i, (day, a)) in units.iter().enumerate() {
        idx.insert((day.clone(), a.to_uppercase()), i);
    }
    let mut out: Vec<Vec<Ticket>> = (0..units.len()).map(|_| Vec::new()).collect();
    for t in tickets {
        if let Some(&i) = idx.get(&(dep_day(&t), series_city(&t))) {
            out[i].push(t);
        }
    }
    out
}

/// Склад для сбора: клиент и покрытие под запрос. None — склада нет (всё через серии).
pub struct StoreView<'a> {
    pub store: &'a dyn TicketStore,
    pub coverage: Coverage,
}

/// Покрытие склада под остановки: X→ANY по всем конкретным городам вылета плеч и по всем
/// дням плеч; при недоступном складе — None (сбор идёт через серии, как раньше).
pub fn store_view<'a>(store: Option<&'a dyn TicketStore>, stops: &[Stop]) -> Option<StoreView<'a>> {
    let store = store?;
    let stops = collect_view(stops);
    let mut origins: Vec<String> = Vec::new();
    let (mut from, mut to): (Option<String>, Option<String>) = (None, None);
    for i in 0..stops.len().saturating_sub(1) {
        if stops[i].is_cities() {
            for c in &stops[i].codes {
                if !origins.contains(c) {
                    origins.push(c.clone());
                }
            }
        }
        let dates = leg_dates(&stops, i);
        if let (Some(a), Some(b)) = (dates.first(), dates.last()) {
            if from.as_deref().map(|f| a.as_str() < f).unwrap_or(true) {
                from = Some(a.clone());
            }
            if to.as_deref().map(|t| b.as_str() > t).unwrap_or(true) {
                to = Some(b.clone());
            }
        }
    }
    let (Some(from), Some(to)) = (from, to) else { return None };
    match coverage_for(store, &origins, &from, &to) {
        Ok(coverage) => Some(StoreView { store, coverage }),
        Err(e) => {
            println!("[collect] склад билетов недоступен, сбор через серии: {e}");
            None
        }
    }
}

/// Собирает рейсы по каждому переходу. Возвращает по плечам: обычные билеты +
/// виртуальные hidden-city. Источник — склад билетов (один запрос на плечо), серии через
/// `fetch` — только за городами и днями, которых в складе нет; без склада — всё сериями.
/// Порядок: сначала плечи с конкретным городом, затем «любой → любой», суженные городами
/// соседних плеч. `airport_city` — карта аэропорт → город из накопленных котировок
/// (дополняется по ходу сбора); `workers` — сколько серий плеча качать одновременно.
/// Разбор — всегда в порядке обхода, результат не зависит от workers.
pub fn collect_plan(
    stops: &[Stop],
    fetch: &dyn SeriesFetcher,
    store: Option<&StoreView>,
    progress: &dyn CollectProgress,
    airport_city: &mut HashMap<String, String>,
    workers: usize,
) -> Result<Vec<Vec<Arc<Ticket>>>, CollectError> {
    let stops = collect_view(stops);
    let legs = stops.len().saturating_sub(1);
    let mut collected: Vec<Option<Vec<Arc<Ticket>>>> = (0..legs).map(|_| None).collect();
    // 1. плечи с конкретным городом хотя бы с одной стороны
    for i in 0..legs {
        if stops[i].is_cities() || stops[i + 1].is_cities() {
            progress.leg(i)?;
            collected[i] = Some(collect_leg(&stops, i, fetch, store, progress, airport_city, workers)?);
        }
    }
    // 2. «любой → любой»: от концов к середине — каждый раз берём плечо, у которого больше
    //    собранных соседей (сначала те, что примыкают к плечам с городами, последним — среднее,
    //    ограниченное с обеих сторон); города вылета — прилёты предыдущего плеча, города
    //    прилёта — вылеты следующего (что из них уже собрано).
    loop {
        let Some(i) = (0..legs)
            .filter(|&i| collected[i].is_none())
            .max_by_key(|&i| {
                let prev = i > 0 && collected[i - 1].is_some();
                let next = collected.get(i + 1).map(|c| c.is_some()).unwrap_or(false);
                (prev as u8 + next as u8, std::cmp::Reverse(i))
            })
        else {
            break;
        };
        progress.leg(i)?;
        let Some(view) = store else {
            return Err(CollectError::Failed("плечо «любой → любой» требует склад билетов (TICKETS_URL)".into()));
        };
        let mut origins: Vec<String> = Vec::new();
        if i > 0 {
            if let Some(prev) = &collected[i - 1] {
                for t in prev.iter() {
                    if let Some(c) = t.dest_city().map(|s| s.to_uppercase()) {
                        if !origins.contains(&c) {
                            origins.push(c);
                        }
                    }
                }
            }
        }
        origins.sort();
        let mut dests: Vec<String> = Vec::new();
        if let Some(Some(next)) = collected.get(i + 1) {
            for t in next.iter() {
                for c in t.side_codes(false) {
                    if !dests.contains(&c) {
                        dests.push(c);
                    }
                }
            }
            dests.sort();
        }
        collected[i] = Some(collect_any_any(&stops, i, view, &origins, &dests, progress, airport_city)?);
    }
    Ok(collected.into_iter().map(|c| c.unwrap_or_default()).collect())
}

/// Разбор рейсов плеча: дедуп по flight_key, фильтр стороны прилёта, карта аэропортов.
struct LegAcc {
    regular: Vec<Ticket>,
    virtual_: Vec<Ticket>,
    seen: HashSet<String>,
}

impl LegAcc {
    fn new() -> LegAcc {
        LegAcc { regular: Vec::new(), virtual_: Vec::new(), seen: HashSet::new() }
    }

    fn keep(&mut self, flights: Vec<Ticket>, allow_dest: Option<&HashSet<String>>, airport_city: &mut HashMap<String, String>) {
        airport_city_learn(&flights, airport_city);
        for f in flights {
            let key = f.flight_key();
            if self.seen.contains(&key) {
                continue;
            }
            if let Some(allow) = allow_dest {
                if !side_matches(&f, true, allow) {
                    continue;
                }
            }
            self.seen.insert(key);
            self.regular.push(f);
        }
    }

    /// hidden-city для серий X→ANY: по дням, хаб — любой известный город (кроме города вылета),
    /// порог — лучший обычный рейс (origin, dest) того дня; allow — только хабы из списка.
    fn hidden_any(&mut self, day_flights: &[(String, Vec<Ticket>)], airport_city: &HashMap<String, String>, allow: Option<&HashSet<String>>) {
        let key = |f: &Ticket| (f.origin.clone().unwrap_or_default(), f.destination.clone().unwrap_or_default());
        for (_day, flights) in day_flights {
            let refs: Vec<&Ticket> = flights.iter().collect();
            let thr = threshold(&refs, &key);
            let hub = |apt: &str| -> Option<String> {
                let city = airport_city.get(apt).cloned().unwrap_or_else(|| apt.to_string());
                match allow {
                    Some(a) if !a.contains(&city) && !a.contains(apt) => None,
                    _ => Some(city),
                }
            };
            let got = hidden_city_flights(flights, &hub, &thr, &key, &mut self.seen);
            self.virtual_.extend(got);
        }
    }

    fn finish(self) -> Vec<Arc<Ticket>> {
        let mut leg: Vec<Arc<Ticket>> = self.regular.into_iter().map(Arc::new).collect();
        leg.extend(self.virtual_.into_iter().map(Arc::new));
        leg
    }
}

/// Плечо с конкретным городом хотя бы с одной стороны.
fn collect_leg(stops: &[Stop], i: usize, fetch: &dyn SeriesFetcher, store: Option<&StoreView>, progress: &dyn CollectProgress, airport_city: &mut HashMap<String, String>, workers: usize) -> Result<Vec<Arc<Ticket>>, CollectError> {
    let (from, to) = (&stops[i], &stops[i + 1]);
    let dates = leg_dates(stops, i);
    let (d0, d1) = (dates[0].clone(), dates[dates.len() - 1].clone());
    let mut acc = LegAcc::new();

    if from.is_cities() {
        // единицы сбора (день, город вылета): из склада — те, что в его покрытии, остальные — сериями
        let units: Vec<(String, String)> = dates.iter().flat_map(|d| from.codes.iter().map(move |a| (d.clone(), a.clone()))).collect();
        let covered: Vec<bool> = units.iter().map(|(d, a)| store.map(|s| s.coverage.has_series(a, d)).unwrap_or(false)).collect();
        let mut per_unit: Vec<Vec<Ticket>> = (0..units.len()).map(|_| Vec::new()).collect();
        if covered.iter().any(|&c| c) {
            let view = store.unwrap();
            let got = view.store.tickets(&from.codes, &[], &[], &d0, &d1)?;
            let split = split_units(got, &units);
            let unit_pages = if to.is_cities() { to.codes.len().max(1) as i64 * PAGES_CITY + PAGES_ANY } else { PAGES_ANY };
            let mut hits = 0;
            for (k, tickets) in split.into_iter().enumerate() {
                if covered[k] {
                    per_unit[k] = tickets;
                    hits += 1;
                    tick_n(progress, unit_pages)?;
                }
            }
            progress.store_hit(hits);
        }
        let missing: Vec<(String, String)> = units.iter().zip(&covered).filter(|(_, c)| !**c).map(|(u, _)| u.clone()).collect();
        if !missing.is_empty() {
            let to_codes = to.codes.clone();
            let is_pair = to.is_cities();
            let unit = |u: &(String, String)| -> Result<Vec<Ticket>, CollectError> {
                let (day, a) = u;
                let mut all = Vec::new();
                if is_pair {
                    for b in &to_codes {
                        all.extend(run_series(fetch, Some(a), Some(b), day, PAGES_CITY, progress)?);
                    }
                }
                all.extend(run_series(fetch, Some(a), None, day, PAGES_ANY, progress)?);
                Ok(all)
            };
            let got = fetch_units(&missing, workers, &unit)?;
            for (u, tickets) in missing.iter().zip(got) {
                let k = units.iter().position(|x| x == u).unwrap();
                per_unit[k] = tickets;
            }
        }
        if to.is_cities() {
            // пара городов: прямые A→B из всех билетов единицы, hidden-city — через хаб B дешевле
            // лучшего прямого того дня (порог по единице)
            let allow_to: HashSet<String> = to.codes.iter().cloned().collect();
            for tickets in per_unit {
                let direct: Vec<Ticket> = tickets.iter().filter(|t| t.hidden_city.is_none() && side_matches(t, true, &allow_to)).cloned().collect();
                let refs: Vec<&Ticket> = direct.iter().collect();
                let thr = threshold(&refs, &|_| 0u8).get(&0).copied();
                acc.keep(direct, Some(&allow_to), airport_city);
                airport_city_learn(&tickets, airport_city);
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
                let got = hidden_city_flights(&tickets, &hub, &thresholds, &|_| 0u8, &mut acc.seen);
                acc.virtual_.extend(got);
            }
        } else {
            // город → любой: всё из X→ANY, hidden-city по дням
            let mut day_flights: Vec<(String, Vec<Ticket>)> = Vec::new();
            for ((day, _a), tickets) in units.iter().zip(per_unit) {
                acc.keep(tickets.clone(), None, airport_city);
                match day_flights.iter_mut().find(|(d, _)| d == day) {
                    Some((_, v)) => v.extend(tickets),
                    None => day_flights.push((day.clone(), tickets)),
                }
            }
            acc.hidden_any(&day_flights, airport_city, None);
        }
    } else {
        // любой → город: из склада по городу прилёта за дни с покрытием, остальные дни — сериями
        let allow: HashSet<String> = to.codes.iter().cloned().collect();
        let covered: Vec<bool> = dates.iter().map(|d| store.map(|s| s.coverage.has_day(d)).unwrap_or(false)).collect();
        if covered.iter().any(|&c| c) {
            let view = store.unwrap();
            let got = view.store.tickets(&[], &to.codes, &[], &d0, &d1)?;
            let hits = covered.iter().filter(|&&c| c).count();
            // дней без покрытия в складе нет по определению — всё полученное относится к покрытым
            acc.keep(got, Some(&allow), airport_city);
            progress.store_hit(hits * to.codes.len().max(1));
            tick_n(progress, hits as i64 * to.codes.len().max(1) as i64 * PAGES_ANY)?;
        }
        let missing: Vec<(String, String)> = dates.iter().zip(&covered).filter(|(_, c)| !**c).flat_map(|(d, _)| to.codes.iter().map(move |b| (d.clone(), b.clone()))).collect();
        if !missing.is_empty() {
            let unit = |u: &(String, String)| run_series(fetch, None, Some(&u.1), &u.0, PAGES_ANY, progress);
            for got in fetch_units(&missing, workers, &unit)? {
                acc.keep(got, Some(&allow), airport_city);
            }
        }
    }
    Ok(acc.finish())
}

/// Плечо «любой → любой» из склада: вылеты из городов прилёта предыдущего плеча (если оно
/// собрано), прилёты — в города вылета следующего (если собрано); хотя бы одна сторона задана.
/// hidden-city — как у X→ANY, хабы — из dests.
fn collect_any_any(stops: &[Stop], i: usize, view: &StoreView, origins: &[String], dests: &[String], progress: &dyn CollectProgress, airport_city: &mut HashMap<String, String>) -> Result<Vec<Arc<Ticket>>, CollectError> {
    let dates = leg_dates(stops, i);
    let (d0, d1) = (dates[0].clone(), dates[dates.len() - 1].clone());
    let mut acc = LegAcc::new();
    if origins.is_empty() && dests.is_empty() {
        return Err(CollectError::Failed(format!("плечо {} «любой → любой» не ограничено ни одной стороной", i + 1)));
    }
    // Известны города вылета: берём их X→ANY целиком и режем по dests сами (hidden-city через
    // хаб из dests требует всех билетов из origins). Известны только города прилёта: срез
    // склада по ним.
    let got = if origins.is_empty() { view.store.tickets(&[], dests, &[], &d0, &d1)? } else { view.store.tickets(origins, &[], &[], &d0, &d1)? };
    let allow: Option<HashSet<String>> = if dests.is_empty() { None } else { Some(dests.iter().cloned().collect()) };
    let mut day_flights: Vec<(String, Vec<Ticket>)> = Vec::new();
    for t in got {
        let day = dep_day(&t);
        match day_flights.iter_mut().find(|(d, _)| *d == day) {
            Some((_, v)) => v.push(t),
            None => day_flights.push((day, vec![t])),
        }
    }
    day_flights.sort_by(|a, b| a.0.cmp(&b.0));
    for (_, flights) in &day_flights {
        airport_city_learn(flights, airport_city);
        acc.keep(flights.clone(), allow.as_ref(), airport_city);
    }
    acc.hidden_any(&day_flights, airport_city, allow.as_ref());
    let covered = dates.iter().filter(|d| view.coverage.has_day(d)).count();
    progress.store_hit(covered);
    tick_n(progress, dates.len() as i64 * PAGES_ANY)?;
    Ok(acc.finish())
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
        let got = collect_plan(&stops, &f, None, &progress, &mut ac, 1).unwrap();
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
        let got = collect_plan(&stops, &f, None, &NoProgress, &mut ac, 4).unwrap();
        assert_eq!(got[0].len(), 3, "два обычных + виртуальный MOW→IST (10000 < 12000)");
        assert_eq!(got[1].len(), 2);
        let calls = f.1.lock().unwrap().len();
        assert_eq!(calls, 2);
    }

    use crate::tickets::tests::MapStore;

    fn with_series(t: Ticket, origin: &str) -> Ticket {
        let mut t = t;
        t.search_origin = Some(origin.into());
        t
    }

    /// Склад: пара и X→ANY берутся из него одним запросом, серии — только за непокрытым
    /// днём; результат тот же, что сериями.
    #[test]
    fn store_serves_covered_units_and_series_fill_the_rest() {
        let f = MapFetcher::new();
        let st = MapStore::new();
        let day1 = "2026-11-01";
        let day2 = "2026-11-02";
        // день 1 — в складе (прямой + hidden-city через PEK), день 2 — только сериями
        st.put("MOW", day1, vec![
            with_series(ticket(&["SVO", "PEK"], "MOW", "BJS", day1, 20000.0), "MOW"),
            with_series(ticket(&["SVO", "PEK", "SIN"], "MOW", "SIN", day1, 15000.0), "MOW"),
        ]);
        f.put(Some("MOW"), Some("BJS"), day2, vec![ticket(&["SVO", "PEK"], "MOW", "BJS", day2, 21000.0)]);
        f.put(Some("MOW"), None, day2, vec![ticket(&["SVO", "PEK", "HKG"], "MOW", "HKG", day2, 25000.0)]);
        let stops = vec![Stop::new("cities", vec!["MOW"], ["", ""]), Stop::new("cities", vec!["BJS"], [day1, day2])];
        let view = store_view(Some(&st), &stops).unwrap();
        assert!(view.coverage.has_series("MOW", day1) && !view.coverage.has_series("MOW", day2));
        let mut ac = HashMap::new();
        let progress = Counter(AtomicUsize::new(0));
        let got = collect_plan(&stops, &f, Some(&view), &progress, &mut ac, 1).unwrap();
        let leg = &got[0];
        assert_eq!(leg.iter().filter(|t| t.hidden_city.is_none()).count(), 2, "прямые 01 и 02");
        assert_eq!(leg.iter().filter(|t| t.hidden_city.is_some()).count(), 1, "виртуальный MOW→BJS из склада (15000 < 20000)");
        assert_eq!(progress.0.load(Ordering::Relaxed), 2 * (PAGES_CITY + PAGES_ANY) as usize);
        let calls = f.1.lock().unwrap();
        assert_eq!(calls.len(), 2, "серии только за день 2: {calls:?}");
        assert!(st.calls.lock().unwrap()[0].starts_with("tickets o=MOW d= via="));
    }

    /// любой → любой: вылеты — прилёты предыдущего плеча, прилёты — вылеты следующего;
    /// собирается после плеч с городами.
    #[test]
    fn any_to_any_narrowed_by_neighbours() {
        let f = MapFetcher::new();
        let st = MapStore::new();
        // Окно плеча — окно остановки вылета: плечо 0 и 1 — 01.11 (окно остановки «любой» 1),
        // плечо 2 — 03.11. MOW → ANY: в IST и DXB; ANY → ANY: из IST/DXB/TAS; ANY → SEL.
        st.put("MOW", "2026-11-01", vec![with_series(ticket(&["SVO", "IST"], "MOW", "IST", "2026-11-01", 100.0), "MOW"), with_series(ticket(&["SVO", "DXB"], "MOW", "DXB", "2026-11-01", 120.0), "MOW")]);
        st.put("IST", "2026-11-01", vec![with_series(ticket(&["IST", "BKK"], "IST", "BKK", "2026-11-01", 300.0), "IST"), with_series(ticket(&["IST", "LON"], "IST", "LON", "2026-11-01", 200.0), "IST")]);
        st.put("DXB", "2026-11-01", vec![with_series(ticket(&["DXB", "HKG", "BKK"], "DXB", "BKK", "2026-11-01", 310.0), "DXB")]);
        st.put("TAS", "2026-11-01", vec![with_series(ticket(&["TAS", "BKK"], "TAS", "BKK", "2026-11-01", 50.0), "TAS")]); // TAS не прилёт плеча 0
        st.put("BKK", "2026-11-03", vec![with_series(ticket(&["BKK", "ICN"], "BKK", "SEL", "2026-11-03", 400.0), "BKK")]);
        st.put("HKG", "2026-11-03", vec![with_series(ticket(&["HKG", "ICN"], "HKG", "SEL", "2026-11-03", 410.0), "HKG")]);
        let stops = vec![
            Stop::new("cities", vec!["MOW"], ["", ""]),
            Stop::new("any", vec![], ["2026-11-01", "2026-11-01"]),
            Stop::new("any", vec![], ["2026-11-03", "2026-11-03"]),
            Stop::new("cities", vec!["SEL"], ["2026-11-05", "2026-11-05"]),
        ];
        let view = store_view(Some(&st), &stops).unwrap();
        let mut ac = HashMap::new();
        let got = collect_plan(&stops, &f, Some(&view), &NoProgress, &mut ac, 1).unwrap();
        assert_eq!(got[0].len(), 2);
        assert_eq!(got[2].len(), 2, "ANY→SEL из BKK и HKG");
        // плечо 1: из IST и DXB (не TAS), только в BKK и HKG (вылеты плеча 2): IST→BKK, DXB→BKK,
        // плюс hidden-city DXB→HKG (хаб HKG — город вылета плеча 2); IST→LON отсечён
        let names: Vec<String> = got[1].iter().map(|t| format!("{}>{}{}", t.origin.as_deref().unwrap(), t.destination.as_deref().unwrap(), if t.hidden_city.is_some() { "*" } else { "" })).collect();
        assert_eq!(names, vec!["DXB>BKK", "IST>BKK", "DXB>HKG*"]);
        assert!(f.1.lock().unwrap().is_empty(), "в серии не ходили");
        let calls = st.calls.lock().unwrap();
        assert!(calls.iter().any(|c| c.starts_with("tickets o=DXB,IST d= via= 2026-11-01")), "{calls:?}");
    }

    #[test]
    fn any_to_any_without_store_fails() {
        let f = MapFetcher::new();
        let stops = vec![Stop::new("cities", vec!["MOW"], ["", ""]), Stop::new("any", vec![], ["2026-11-01", "2026-11-01"]), Stop::new("any", vec![], ["2026-11-03", "2026-11-03"]), Stop::new("cities", vec!["SEL"], ["2026-11-05", "2026-11-05"])];
        let mut ac = HashMap::new();
        assert!(collect_plan(&stops, &f, None, &NoProgress, &mut ac, 1).is_err());
    }
}
