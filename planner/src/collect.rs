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

use rustc_hash::{FxHashMap, FxHashSet};

use crate::flightcols::{ts_ord, FlightCols, FlightColsBuilder, FlightSrc, Fl, JobCodes, Tp, NO_CODE};
use crate::stops::{collect_view, leg_dates, leg_specs, spec_dates, LegSpec, Stop, PAGES_ANY, PAGES_CITY};
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
fn threshold<K: std::hash::Hash + Eq>(flights: &[Fl], key: &dyn Fn(&Fl) -> K) -> HashMap<K, f64> {
    let mut best_direct: HashMap<K, f64> = HashMap::new();
    let mut best_any: HashMap<K, f64> = HashMap::new();
    for f in flights {
        if f.hidden {
            continue;
        }
        if f.transfers == 0 {
            let e = best_direct.entry(key(f)).or_insert(f64::INFINITY);
            if f.price < *e {
                *e = f.price;
            }
        }
        let e = best_any.entry(key(f)).or_insert(f64::INFINITY);
        if f.price < *e {
            *e = f.price;
        }
    }
    for (k, v) in best_direct {
        best_any.insert(k, v);
    }
    best_any
}

/// Карта аэропорт → город номерами кодов джобы.
type ApCity = FxHashMap<u32, u32>;

/// Пополняет карту аэропорт → город из концов рейсов (PKX → BJS).
fn airport_city_learn(flights: &[Fl], ac: &mut ApCity) {
    for f in flights {
        if f.hidden {
            continue;
        }
        for (apt, city) in [(f.dest_airport, f.dest), (f.orig_airport, f.orig_city)] {
            if apt != NO_CODE && city != NO_CODE {
                ac.entry(apt).or_insert(city);
            }
        }
    }
}

/// Пересадки рейса: у строки склада — готовыми колонками блока, у билета серии — из билета.
fn tps_of(f: &Fl, codes: &JobCodes) -> Vec<Tp> {
    match &f.src {
        FlightSrc::Store(s) if codes.store_based => crate::lakestore::store_tps(&s.block, s.row as usize, codes),
        // словарь не от склада (тесты, склад-заглушка): пересадки — из полного билета строки
        FlightSrc::Store(s) => crate::lakestore::materialize(&[&crate::lakestore::StoreRow { hub: None, ..s.clone() }]).ok().and_then(|v| v.into_iter().next()).map(|t| crate::lakestore::ticket_tps(&t, codes)).unwrap_or_default(),
        FlightSrc::Owned(t) => crate::lakestore::ticket_tps(t, codes),
    }
}

/// Виртуальные рейсы hidden-city из рейсов с пересадками: для каждой пересадки, чей
/// аэропорт hub_city(apt) распознан как остановка, — рейс «до этой пересадки», если он
/// дешевле порога thresholds[key(рейс)] (без порога — все). Хаб ≠ город вылета.
fn hidden_city_flights<K: std::hash::Hash + Eq>(
    flights: &[Fl],
    hub_city: &dyn Fn(u32) -> Option<u32>,
    thresholds: &HashMap<K, f64>,
    key: &dyn Fn(&Fl) -> K,
    seen: &mut HashSet<u64>,
    codes: &JobCodes,
) -> Vec<Fl> {
    let mut out = Vec::new();
    for t in flights {
        if t.hidden {
            continue;
        }
        for (k, tp) in tps_of(t, codes).into_iter().enumerate() {
            let Some(city) = hub_city(tp.ap) else { continue };
            if city == NO_CODE || tp.ap == NO_CODE || city == t.orig_city || tp.ap == t.orig_airport {
                continue;
            }
            let v = virtual_fl(t, k, &tp, city, codes);
            if let Some(thr) = thresholds.get(&key(&v)) {
                if v.price >= *thr {
                    continue;
                }
            }
            if !seen.insert(v.key) {
                continue;
            }
            out.push(v);
        }
    }
    out
}

/// Виртуальный рейс «выходим на k-й пересадке в городе city» (как `Ticket::virtual_flight`).
fn virtual_fl(t: &Fl, k: usize, tp: &Tp, city: u32, codes: &JobCodes) -> Fl {
    let (arr_ts, arr_ord) = ts_ord(tp.arr);
    let src = match &t.src {
        FlightSrc::Store(s) => FlightSrc::Store(s.with_hub(k, &codes.name(city))),
        FlightSrc::Owned(o) => FlightSrc::Owned(std::sync::Arc::new(o.virtual_flight(k, &codes.name(city)))),
    };
    Fl { dest: city, dest_airport: tp.ap, transfers: k as i64, duration: tp.dur, arr_ts, arr_ord, pts_min: tp.pmin, hidden: true, key: tp.key, src, ..t.clone() }
}

/// Город или аэропорт прилёта в allow.
fn side_matches(f: &Fl, allow: &FxHashSet<u32>) -> bool {
    allow.contains(&f.dest) || allow.contains(&f.dest_airport)
}

/// Коды → номера джобы (неизвестные коды — нет ни одного рейса с ними, пропускаются).
fn id_set(codes: &JobCodes, list: &[String]) -> FxHashSet<u32> {
    list.iter().filter_map(|c| codes.lookup(c)).collect()
}

/// Рейсы склада, разложенные по единицам сбора (день, город запроса) — в порядке units.
fn split_units(flights: Vec<Fl>, units: &[(String, String)], codes: &JobCodes) -> Vec<Vec<Fl>> {
    let mut idx: HashMap<(i32, u32), usize> = HashMap::new();
    for (i, (day, a)) in units.iter().enumerate() {
        let (Some(d), Some(c)) = (chrono::NaiveDate::parse_from_str(day, "%Y-%m-%d").ok(), codes.lookup(&a.to_uppercase())) else { continue };
        idx.insert((crate::lakestore::day_num(d), c), i);
    }
    let mut out: Vec<Vec<Fl>> = (0..units.len()).map(|_| Vec::new()).collect();
    for f in flights {
        if let Some(&i) = idx.get(&(f.day, f.series)) {
            out[i].push(f);
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
/// соседних плеч. Плечи — `leg_specs`: обычные, затем в обход пропускаемых остановок (у
/// обходного всегда есть конкретный город с одной стороны — `skip_error`). `airport_city` — карта аэропорт → город из накопленных котировок
/// (дополняется по ходу сбора); `workers` — сколько серий плеча качать одновременно.
/// Разбор — всегда в порядке обхода, результат не зависит от workers.
pub fn collect_plan(
    stops: &[Stop],
    fetch: &dyn SeriesFetcher,
    store: Option<&StoreView>,
    progress: &dyn CollectProgress,
    airport_city: &mut HashMap<String, String>,
    workers: usize,
) -> Result<FlightCols, CollectError> {
    let stops = collect_view(stops);
    let specs = leg_specs(&stops);
    let legs = stops.len().saturating_sub(1); // обычные плечи; обходные — за ними
    // словарь кодов джобы: коды склада — теми же номерами (строки склада копируются как есть)
    let codes = Arc::new(JobCodes::new(&store.map(|v| v.store.code_names()).unwrap_or_default()));
    let mut ac: ApCity = airport_city.iter().map(|(a, c)| (codes.id(a), codes.id(c))).filter(|(a, c)| *a != NO_CODE && *c != NO_CODE).collect();
    // рейсы плеча сразу уходят в колонки джобы (из склада — номерами строк)
    let mut out = FlightColsBuilder::new(specs.len(), codes.clone());
    let mut done: Vec<bool> = vec![false; legs];
    // 1. плечи с конкретным городом хотя бы с одной стороны (и все плечи в обход)
    for (i, &l) in specs.iter().enumerate() {
        if stops[l.from].is_cities() || stops[l.to].is_cities() {
            progress.leg(i)?;
            out.push_leg(i, collect_leg(&stops, l, fetch, store, progress, &mut ac, workers, &codes)?);
            if i < legs {
                done[i] = true;
            }
        } else if l.is_bypass() {
            return Err(CollectError::Failed("плечо в обход между двумя «любыми» не поддерживается".into()));
        }
    }
    // 2. «любой → любой»: от концов к середине — каждый раз берём плечо, у которого больше
    //    собранных соседей (сначала те, что примыкают к плечам с городами, последним — среднее,
    //    ограниченное с обеих сторон); города вылета — прилёты предыдущего плеча, города
    //    прилёта — вылеты следующего (что из них уже собрано).
    loop {
        let Some(i) = (0..legs)
            .filter(|&i| !done[i])
            .max_by_key(|&i| {
                let prev = i > 0 && done[i - 1];
                let next = done.get(i + 1).copied().unwrap_or(false);
                (prev as u8 + next as u8, std::cmp::Reverse(i))
            })
        else {
            break;
        };
        progress.leg(i)?;
        let Some(view) = store else {
            return Err(CollectError::Failed("плечо «любой → любой» требует склад билетов (озеро S3_* у планировщика)".into()));
        };
        let mut origins: Vec<String> = if i > 0 && done[i - 1] { out.dest_cities(i - 1) } else { Vec::new() };
        origins.sort();
        let mut dests: Vec<String> = if done.get(i + 1).copied().unwrap_or(false) { out.origin_codes(i + 1) } else { Vec::new() };
        dests.sort();
        collect_any_any(&stops, i, view, &origins, &dests, progress, &mut ac, &mut out)?;
        done[i] = true;
    }
    // выученное по ходу сбора — обратно в карту вызывающего
    for (a, c) in &ac {
        airport_city.entry(codes.name(*a)).or_insert_with(|| codes.name(*c));
    }
    Ok(out.finish())
}

/// Разбор рейсов плеча: дедуп по ключу рейса, фильтр стороны прилёта, карта аэропортов.
struct LegAcc {
    regular: Vec<Fl>,
    virtual_: Vec<Fl>,
    seen: HashSet<u64>,
}

impl LegAcc {
    fn new() -> LegAcc {
        LegAcc { regular: Vec::new(), virtual_: Vec::new(), seen: HashSet::new() }
    }

    fn keep(&mut self, flights: &[Fl], allow_dest: Option<&FxHashSet<u32>>, ac: &mut ApCity) {
        airport_city_learn(flights, ac);
        for f in flights {
            if self.seen.contains(&f.key) {
                continue;
            }
            if let Some(allow) = allow_dest {
                if !side_matches(f, allow) {
                    continue;
                }
            }
            self.seen.insert(f.key);
            self.regular.push(f.clone());
        }
    }

    /// hidden-city для серий X→ANY: по дням, хаб — любой известный город (кроме города вылета),
    /// порог — лучший обычный рейс (origin, dest) того дня; allow — только хабы из списка.
    fn hidden_any(&mut self, day_flights: &[Vec<Fl>], ac: &ApCity, allow: Option<&FxHashSet<u32>>, codes: &JobCodes) {
        let key = |f: &Fl| (f.orig_city, f.dest);
        for flights in day_flights {
            let thr = threshold(flights, &key);
            let hub = |apt: u32| -> Option<u32> {
                let city = ac.get(&apt).copied().unwrap_or(apt);
                match allow {
                    Some(a) if !a.contains(&city) && !a.contains(&apt) => None,
                    _ => Some(city),
                }
            };
            let got = hidden_city_flights(flights, &hub, &thr, &key, &mut self.seen, codes);
            self.virtual_.extend(got);
        }
    }

    fn finish(self) -> Vec<Fl> {
        let mut leg = self.regular;
        leg.extend(self.virtual_);
        leg
    }
}

fn from_tickets(tickets: Vec<Ticket>, codes: &JobCodes) -> Vec<Fl> {
    tickets.into_iter().map(|t| Fl::from_ticket(t, codes)).collect()
}

/// Плечо с конкретным городом хотя бы с одной стороны.
#[allow(clippy::too_many_arguments)]
fn collect_leg(stops: &[Stop], l: LegSpec, fetch: &dyn SeriesFetcher, store: Option<&StoreView>, progress: &dyn CollectProgress, ac: &mut ApCity, workers: usize, codes: &JobCodes) -> Result<Vec<Fl>, CollectError> {
    let (from, to) = (&stops[l.from], &stops[l.to]);
    let dates = spec_dates(stops, l);
    let (d0, d1) = (dates[0].clone(), dates[dates.len() - 1].clone());
    let mut acc = LegAcc::new();

    if from.is_cities() {
        // единицы сбора (день, город вылета): из склада — те, что в его покрытии, остальные — сериями
        let units: Vec<(String, String)> = dates.iter().flat_map(|d| from.codes.iter().map(move |a| (d.clone(), a.clone()))).collect();
        let covered: Vec<bool> = units.iter().map(|(d, a)| store.map(|s| s.coverage.has_series(a, d)).unwrap_or(false)).collect();
        let mut per_unit: Vec<Vec<Fl>> = (0..units.len()).map(|_| Vec::new()).collect();
        if covered.iter().any(|&c| c) {
            let view = store.unwrap();
            let got = view.store.flights(&from.codes, &[], &d0, &d1, codes)?;
            let split = split_units(got, &units, codes);
            let unit_pages = if to.is_cities() { to.codes.len().max(1) as i64 * PAGES_CITY + PAGES_ANY } else { PAGES_ANY };
            let mut hits = 0;
            for (k, flights) in split.into_iter().enumerate() {
                if covered[k] {
                    per_unit[k] = flights;
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
                per_unit[k] = from_tickets(tickets, codes);
            }
        }
        if to.is_cities() {
            // пара городов: прямые A→B из всех билетов единицы, hidden-city — через хаб B дешевле
            // лучшего прямого того дня (порог по единице)
            let allow_to = id_set(codes, &to.codes);
            for flights in per_unit {
                let direct: Vec<Fl> = flights.iter().filter(|t| !t.hidden && side_matches(t, &allow_to)).cloned().collect();
                let thr = threshold(&direct, &|_| 0u8).get(&0).copied();
                acc.keep(&direct, Some(&allow_to), ac);
                airport_city_learn(&flights, ac);
                let thresholds: HashMap<u8, f64> = match thr {
                    Some(t) if t != 0.0 => HashMap::from([(0u8, t)]),
                    _ => HashMap::new(),
                };
                let acr = &*ac;
                let hub = |apt: u32| -> Option<u32> {
                    if allow_to.contains(&apt) {
                        return Some(apt);
                    }
                    acr.get(&apt).filter(|c| allow_to.contains(*c)).copied()
                };
                let got = hidden_city_flights(&flights, &hub, &thresholds, &|_| 0u8, &mut acc.seen, codes);
                acc.virtual_.extend(got);
            }
        } else {
            // город → любой: всё из X→ANY, hidden-city по дням
            let mut day_flights: Vec<(String, Vec<Fl>)> = Vec::new();
            for ((day, _a), flights) in units.iter().zip(per_unit) {
                acc.keep(&flights, None, ac);
                match day_flights.iter_mut().find(|(d, _)| d == day) {
                    Some((_, v)) => v.extend(flights),
                    None => day_flights.push((day.clone(), flights)),
                }
            }
            let groups: Vec<Vec<Fl>> = day_flights.into_iter().map(|(_, v)| v).collect();
            acc.hidden_any(&groups, ac, None, codes);
        }
    } else {
        // любой → город: из склада по городу прилёта за дни с покрытием, остальные дни — сериями
        let allow = id_set(codes, &to.codes);
        let covered: Vec<bool> = dates.iter().map(|d| store.map(|s| s.coverage.has_day(d)).unwrap_or(false)).collect();
        if covered.iter().any(|&c| c) {
            let view = store.unwrap();
            let got = view.store.flights(&[], &to.codes, &d0, &d1, codes)?;
            let hits = covered.iter().filter(|&&c| c).count();
            // дней без покрытия в складе нет по определению — всё полученное относится к покрытым
            // (allow — после выборки: коды города прилёта могли появиться в словаре только с ней)
            let allow = id_set(codes, &to.codes);
            acc.keep(&got, Some(&allow), ac);
            progress.store_hit(hits * to.codes.len().max(1));
            tick_n(progress, hits as i64 * to.codes.len().max(1) as i64 * PAGES_ANY)?;
        }
        let missing: Vec<(String, String)> = dates.iter().zip(&covered).filter(|(_, c)| !**c).flat_map(|(d, _)| to.codes.iter().map(move |b| (d.clone(), b.clone()))).collect();
        if !missing.is_empty() {
            let unit = |u: &(String, String)| run_series(fetch, None, Some(&u.1), &u.0, PAGES_ANY, progress);
            for got in fetch_units(&missing, workers, &unit)? {
                let got = from_tickets(got, codes);
                let allow = if allow.is_empty() { id_set(codes, &to.codes) } else { allow.clone() };
                acc.keep(&got, Some(&allow), ac);
            }
        }
    }
    Ok(acc.finish())
}

/// Сколько дней плеча «любой → любой» разбирать параллельно: COLLECT_THREADS или число ядер.
fn collect_threads() -> usize {
    std::env::var("COLLECT_THREADS").ok().and_then(|v| v.parse().ok()).unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)).max(1)
}

/// Плечо «любой → любой» из склада: вылеты из городов прилёта предыдущего плеча (если оно
/// собрано), прилёты — в города вылета следующего (если собрано); ни одна сторона не
/// задана (все остановки «любые») — весь склад за дни плеча.
/// hidden-city — как у X→ANY, хабы — из dests. По дням: выборка дня → отбор → рейсы сразу
/// в колонки джобы (`out`).
#[allow(clippy::too_many_arguments)]
fn collect_any_any(stops: &[Stop], i: usize, view: &StoreView, origins: &[String], dests: &[String], progress: &dyn CollectProgress, ac: &mut ApCity, out: &mut FlightColsBuilder) -> Result<(), CollectError> {
    let dates = leg_dates(stops, i);
    let codes = out.codes().clone();
    let allow: Option<FxHashSet<u32>> = if dests.is_empty() { None } else { Some(id_set(&codes, dests)) };
    // День = выборка склада → отбор → hidden-city; дни независимы (ключ дедупа содержит дату
    // вылета) — по `collect_threads()` дней параллельно, результат — по порядку дней.
    let day_job = |day: &String, base: &ApCity| -> Result<(Vec<Fl>, usize, ApCity), CollectError> {
        // Известны города вылета: берём их X→ANY целиком и режем по dests сами (hidden-city
        // через хаб из dests требует всех билетов из origins). Известны только города
        // прилёта: срез склада по ним. Ни то ни другое — весь день склада.
        let flights = if origins.is_empty() { view.store.flights(&[], dests, day, day, &codes)? } else { view.store.flights(origins, &[], day, day, &codes)? };
        let n = flights.len();
        let mut ac = base.clone();
        let mut acc = LegAcc::new();
        acc.keep(&flights, allow.as_ref(), &mut ac);
        acc.hidden_any(&[flights], &ac, allow.as_ref(), &codes);
        Ok((acc.finish(), n, ac))
    };
    let threads = collect_threads().min(dates.len()).max(1);
    for batch in dates.chunks(threads) {
        let base = ac.clone();
        let results: Vec<Result<(Vec<Fl>, usize, ApCity), CollectError>> = if batch.len() == 1 {
            vec![day_job(&batch[0], &base)]
        } else {
            std::thread::scope(|s| {
                let handles: Vec<_> = batch.iter().map(|day| s.spawn(|| day_job(day, &base))).collect();
                handles.into_iter().map(|h| h.join().unwrap_or_else(|_| Err(CollectError::Failed("сбор дня упал".into())))).collect()
            })
        };
        for r in results {
            let (kept, _n, learned) = r?;
            for (a, c) in learned {
                ac.entry(a).or_insert(c);
            }
            out.push_leg(i, kept);
        }
    }
    let covered = dates.iter().filter(|d| view.coverage.has_day(d)).count();
    progress.store_hit(covered);
    tick_n(progress, dates.len() as i64 * PAGES_ANY)?;
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::ticket::{Leg, TransferPoint};

    /// Билет с цепочкой аэропортов; каждая пересадка — leg.
    /// Рейсы джобы по плечам (до последнего непустого) — как раньше возвращал сбор.
    pub fn by_legs(t: FlightCols) -> Vec<Vec<std::sync::Arc<Ticket>>> {
        let legs = t.leg.iter().map(|&l| l as usize + 1).max().unwrap_or(0);
        (0..legs).map(|l| t.leg_flights(l)).collect()
    }

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
        let got = by_legs(collect_plan(&stops, &f, None, &progress, &mut ac, 1).unwrap());
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
        let got = by_legs(collect_plan(&stops, &f, None, &NoProgress, &mut ac, 4).unwrap());
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
        let got = by_legs(collect_plan(&stops, &f, Some(&view), &progress, &mut ac, 1).unwrap());
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
        let got = by_legs(collect_plan(&stops, &f, Some(&view), &NoProgress, &mut ac, 1).unwrap());
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

    /// Все остановки «любые»: плечи ни с какой стороны не сужены — весь склад за дни плеча.
    #[test]
    fn all_any_takes_whole_store() {
        let f = MapFetcher::new();
        let st = MapStore::new();
        st.put("MOW", "2026-11-01", vec![with_series(ticket(&["SVO", "IST"], "MOW", "IST", "2026-11-01", 100.0), "MOW")]);
        st.put("TAS", "2026-11-01", vec![with_series(ticket(&["TAS", "BKK"], "TAS", "BKK", "2026-11-01", 50.0), "TAS")]);
        st.put("IST", "2026-11-03", vec![with_series(ticket(&["IST", "LON"], "IST", "LON", "2026-11-03", 200.0), "IST")]);
        st.put("BKK", "2026-11-03", vec![with_series(ticket(&["BKK", "ICN"], "BKK", "SEL", "2026-11-03", 400.0), "BKK")]);
        let stops = vec![
            Stop::new("any", vec![], ["2026-11-01", "2026-11-01"]),
            Stop::new("any", vec![], ["2026-11-03", "2026-11-03"]),
            Stop::new("any", vec![], ["", ""]),
        ];
        let view = store_view(Some(&st), &stops).unwrap();
        let mut ac = HashMap::new();
        let got = by_legs(collect_plan(&stops, &f, Some(&view), &NoProgress, &mut ac, 1).unwrap());
        let names = |l: &Vec<std::sync::Arc<Ticket>>| {
            let mut v: Vec<String> = l.iter().filter(|t| t.hidden_city.is_none()).map(|t| format!("{}>{}", t.origin.as_deref().unwrap(), t.destination.as_deref().unwrap())).collect();
            v.sort();
            v
        };
        // плечо 1 собирается первым (весь склад 01.11), плечо 2 — из его прилётов (IST, BKK)
        assert_eq!(names(&got[0]), vec!["MOW>IST", "TAS>BKK"]);
        assert_eq!(names(&got[1]), vec!["BKK>SEL", "IST>LON"]);
        assert!(f.1.lock().unwrap().is_empty(), "в серии не ходили");
    }

    #[test]
    fn any_to_any_without_store_fails() {
        let f = MapFetcher::new();
        let stops = vec![Stop::new("cities", vec!["MOW"], ["", ""]), Stop::new("any", vec![], ["2026-11-01", "2026-11-01"]), Stop::new("any", vec![], ["2026-11-03", "2026-11-03"]), Stop::new("cities", vec!["SEL"], ["2026-11-05", "2026-11-05"])];
        let mut ac = HashMap::new();
        assert!(collect_plan(&stops, &f, None, &NoProgress, &mut ac, 1).is_err());
    }
}
