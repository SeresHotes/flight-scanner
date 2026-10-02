//! Наборы городов (режим «города» планировщика) — зеркало `core.overview`.
//!
//! Для каждой последовательности городов c0 → c1 → … → cL, которую можно собрать из
//! собранных рейсов под фильтры запроса: минимальная цена цепочки, пересадки у самой
//! дешёвой, минимум пересадок, точное число цепочек. Динамика по префиксам: состояние
//! префикса — матрица «день первого вылета × рейс последнего плеча» (число цепочек,
//! минимальная цена, пересадки при минимуме, минимум пересадок). Расширение на рейсы
//! следующего плеча — по тем же правилам стыковки, что у перебора A*: подходящие
//! предшественники рейса f — отрезок в списке, отсортированном по времени прилёта
//! (число — разность накопленных сумм, минимумы — минимум на отрезке).
//!
//! Выдача — COMBOS_TOP самых дешёвых наборов: обход префиксов отсекает ветки, чья нижняя
//! оценка (минимальная цена префикса + минимальная цена хвоста по городам, как у поиска
//! маршрутов) выше цены K-го найденного набора; результат тот же, что у полного обхода
//! с сортировкой по (minPrice, codes).

use std::cell::RefCell;
use std::collections::{BinaryHeap, HashMap, HashSet};

use rustc_hash::FxHashMap;
use serde::Serialize;

use crate::dates::{date_ordinal, weekend_deadline, DAY_SECONDS};
use crate::flightcols::{FlightCols, NO_CODE};
use crate::nearby::{Hops, HOP_MIN_GAP_MIN};
use crate::planquery::{CityFilter, PlanQuery};
use crate::search::{build_ctx, chain_start, completion_lb, completion_lb_day, day_lb_get, leg_rows, variants, DayLb, RESEARCH_DAY_LB};
use std::sync::atomic::{AtomicUsize, Ordering as AtOrd};

/// ИССЛЕДОВАНИЕ: число стыковок групп (extend) и отсечённых групп.
pub static OV_EXTENDS: AtomicUsize = AtomicUsize::new(0);
pub static OV_PRUNED: AtomicUsize = AtomicUsize::new(0);
pub static OV_EMPTY: AtomicUsize = AtomicUsize::new(0);
use crate::segments::city_pair;
use crate::stops::{Stop, MAX_COMBO_STEPS};

const BIG_TR: i64 = 1 << 20;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Combo {
    pub codes: Vec<String>,
    #[serde(rename = "minPrice")]
    pub min_price: f64,
    #[serde(rename = "transfersAtMin")]
    pub transfers_at_min: i64,
    #[serde(rename = "minTransfers")]
    pub min_transfers: i64,
    pub count: i64,
    /// Пропущенные остановки (номера в запросе): codes — только города варианта.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub skipped: Vec<usize>,
}

#[derive(Debug, Clone, Default)]
pub struct Overview {
    /// COMBOS_TOP самых дешёвых наборов по возрастанию (minPrice, codes).
    pub combos: Vec<Combo>,
    /// Цепочек во всех выданных наборах.
    pub total_count: i64,
    pub cities: HashMap<String, (String, String)>,
    /// Наборов больше, чем выдано: часть вытеснена из K лучших или отсечена по оценке.
    pub truncated: bool,
    /// Обход остановлен по лимиту шагов (MAX_COMBO_STEPS): выданы найденные к этому моменту,
    /// гарантии «ровно K самых дешёвых» нет.
    pub incomplete: bool,
}

/// Рейсы плеча в локальных массивах + индекс по коду вылета. Коды — номера словаря
/// FlightCols (NO_CODE — пусто): сравнение и хэширование чисел, а не строк.
struct Leg {
    dest: Vec<u32>,
    dest_air: Vec<u32>,
    orig_city: Vec<u32>,
    orig_air: Vec<u32>,
    price: Vec<f64>,
    transfers: Vec<i64>,
    dep_ts: Vec<f64>,
    arr_ts: Vec<f64>,
    dep_ord: Vec<i64>,
    arr_ord: Vec<i64>,
    weekend_ok_from: Vec<i64>,
    by_origin: FxHashMap<u32, Vec<usize>>,
}

impl Leg {
    fn new(t: &FlightCols, rows: &[usize]) -> Leg {
        let rows: Vec<usize> = rows.iter().copied().filter(|&r| t.dep_ord[r] >= 0).collect();
        let mut by_origin: FxHashMap<u32, Vec<usize>> = FxHashMap::default();
        for (i, &r) in rows.iter().enumerate() {
            let (c, a) = (t.orig_city_id[r], t.orig_airport_id[r]);
            if c != NO_CODE {
                by_origin.entry(c).or_default().push(i);
            }
            if a != NO_CODE && a != c {
                by_origin.entry(a).or_default().push(i);
            }
        }
        Leg {
            dest: rows.iter().map(|&r| t.dest_id[r]).collect(),
            dest_air: rows.iter().map(|&r| t.dest_airport_id[r]).collect(),
            orig_city: rows.iter().map(|&r| t.orig_city_id[r]).collect(),
            orig_air: rows.iter().map(|&r| t.orig_airport_id[r]).collect(),
            price: rows.iter().map(|&r| t.price[r]).collect(),
            transfers: rows.iter().map(|&r| t.transfers[r]).collect(),
            dep_ts: rows.iter().map(|&r| t.dep_ts[r]).collect(),
            arr_ts: rows.iter().map(|&r| t.arr_ts[r]).collect(),
            dep_ord: rows.iter().map(|&r| t.dep_ord[r]).collect(),
            arr_ord: rows.iter().map(|&r| t.arr_ord[r]).collect(),
            weekend_ok_from: rows.iter().map(|&r| weekend_deadline(t.arr_ord[r])).collect(),
            by_origin,
        }
    }

    fn origin_has(&self, i: usize, code: u32) -> bool {
        self.orig_city[i] == code || self.orig_air[i] == code
    }
}

/// Состояние префикса: D × P (день первого вылета × рейс последнего плеча).
#[derive(Clone)]
struct State {
    n_days: usize,
    n_p: usize,
    cnt: Vec<f64>,
    minp: Vec<f64>,
    tr_at: Vec<i64>,
    mintr: Vec<i64>,
}

impl State {
    fn total(&self) -> f64 {
        self.cnt.iter().sum()
    }
}

/// Минимум на отрезке: разреженная таблица по строке.
struct Sparse<T: Copy + PartialOrd> {
    levels: Vec<Vec<T>>,
}

impl<T: Copy + PartialOrd> Sparse<T> {
    fn new(values: Vec<T>) -> Self {
        let mut levels = vec![values];
        let mut width = 1;
        while width * 2 <= levels[0].len() {
            let prev = levels.last().unwrap();
            let next: Vec<T> = (0..prev.len() - width).map(|i| if prev[i + width] < prev[i] { prev[i + width] } else { prev[i] }).collect();
            levels.push(next);
            width *= 2;
        }
        Sparse { levels }
    }

    /// min(values[lo..hi]), hi > lo.
    fn query(&self, lo: usize, hi: usize) -> T {
        let len = hi - lo;
        let lv = (usize::BITS - 1 - len.leading_zeros()) as usize;
        let tab = &self.levels[lv];
        let a = tab[lo];
        let b = tab[hi - (1 << lv)];
        if b < a { b } else { a }
    }
}

fn upper_bound<T: PartialOrd>(sorted: &[T], value: T) -> usize {
    sorted.partition_point(|x| *x <= value)
}

fn ts_key(v: f64) -> f64 {
    if v.is_nan() { f64::INFINITY } else { v }
}

/// Состояние префикса (по рейсам p) → состояние по рейсам f (D × |f|).
fn extend(state: &State, prev: &Leg, p_idx: &[usize], nxt: &Leg, f_idx: &[usize], cf: Option<&CityFilter>, hop: Option<&[bool]>) -> State {
    let (n_days, n_p, n_f) = (state.n_days, state.n_p, f_idx.len());
    debug_assert_eq!(n_p, p_idx.len());
    let arr_ts: Vec<f64> = p_idx.iter().map(|&p| prev.arr_ts[p]).collect();
    // Вылет — строго в следующий календарный день после прилёта (search::min_depart_ord).
    let mut day_thr: Vec<i64> = p_idx.iter().map(|&p| prev.arr_ord[p] + 1).collect();
    if cf.map(|c| c.require_weekend).unwrap_or(false) {
        for (k, &p) in p_idx.iter().enumerate() {
            day_thr[k] = day_thr[k].max(prev.weekend_ok_from[p]);
        }
    }
    let mut order: Vec<usize> = (0..n_p).collect();
    order.sort_by(|&a, &b| ts_key(arr_ts[a]).partial_cmp(&ts_key(arr_ts[b])).unwrap());
    let ts_s: Vec<f64> = order.iter().map(|&k| ts_key(arr_ts[k])).collect();
    let thr_s: Vec<i64> = order.iter().map(|&k| day_thr[k]).collect();

    let cover = cf.and_then(|c| c.must_cover.as_ref()).map(|c| (date_ordinal(&c[0]).unwrap_or(i64::MAX), date_ordinal(&c[1]).unwrap_or(i64::MIN)));
    let p_ok_s: Vec<bool> = order.iter().map(|&k| cover.map(|(f_ord, _)| prev.arr_ord[p_idx[k]] <= f_ord).unwrap_or(true)).collect();
    let prefix = cf.map(|c| c.max_stay.is_none()).unwrap_or(true);

    let mut lo = vec![0usize; n_f];
    let mut hi = vec![0usize; n_f];
    let mut f_ok = vec![false; n_f];
    for (j, &f) in f_idx.iter().enumerate() {
        let (dep_ord, dep_ts) = (nxt.dep_ord[f], nxt.dep_ts[f]);
        let mut h = upper_bound(&thr_s, dep_ord);
        let mut gap = f64::NEG_INFINITY;
        if hop.map(|h| h[j]).unwrap_or(false) {
            gap = (HOP_MIN_GAP_MIN * 60) as f64;
        }
        if let Some(c) = cf {
            gap = gap.max(c.min_stay as f64 * DAY_SECONDS);
        }
        h = h.min(upper_bound(&ts_s, dep_ts - gap));
        let l = match cf.and_then(|c| c.max_stay) {
            Some(m) => upper_bound(&ts_s, dep_ts - (m as f64 + 1.0) * DAY_SECONDS),
            None => 0,
        };
        let mut ok = h > l;
        if let Some((_, t_ord)) = cover {
            ok = ok && dep_ord >= t_ord;
        }
        lo[j] = l;
        hi[j] = h;
        f_ok[j] = ok;
    }

    let mut out = State {
        n_days,
        n_p: n_f,
        cnt: vec![0.0; n_days * n_f],
        minp: vec![f64::INFINITY; n_days * n_f],
        tr_at: vec![0; n_days * n_f],
        mintr: vec![BIG_TR; n_days * n_f],
    };
    let live: Vec<usize> = (0..n_f).filter(|&j| f_ok[j]).collect();
    // буферы на весь вызов (раньше — новые векторы на каждый день первого вылета)
    let mut csum = vec![0.0; n_p + 1];
    let mut kv = vec![f64::INFINITY; n_p];
    let mut ki = vec![usize::MAX; n_p];
    let mut trs = vec![BIG_TR; n_p];
    for d in 0..n_days {
        let cnt_row = &state.cnt[d * n_p..(d + 1) * n_p];
        let minp_row = &state.minp[d * n_p..(d + 1) * n_p];
        let mintr_row = &state.mintr[d * n_p..(d + 1) * n_p];
        let tr_at_row = &state.tr_at[d * n_p..(d + 1) * n_p];
        let tr0 = tr_at_row.first().copied().unwrap_or(0);
        // Пустая строка (ни одной цепочки с этим днём первого вылета): счётчики 0, цены
        // бесконечны — как и посчитал бы общий путь; минимум пересадок таких ячеек
        // (≥ BIG_TR) на наборы не влияет — ячейки с цепочками всегда меньше.
        if cnt_row.iter().all(|&c| c <= 0.0) {
            for j in 0..n_f {
                let cell = d * n_f + j;
                let f = f_idx[j];
                out.tr_at[cell] = tr0 + nxt.transfers[f];
                out.mintr[cell] = BIG_TR + nxt.transfers[f];
            }
            continue;
        }
        // ключи в отсортированном порядке: (цена, номер) — при равной цене меньший номер
        for pos in 0..n_p {
            let p = order[pos];
            let ok = p_ok_s[pos];
            csum[pos + 1] = csum[pos] + if ok { cnt_row[p] } else { 0.0 };
            if ok {
                kv[pos] = minp_row[p];
                ki[pos] = p;
                trs[pos] = mintr_row[p];
            } else {
                kv[pos] = f64::INFINITY;
                ki[pos] = usize::MAX;
                trs[pos] = BIG_TR;
            }
        }
        // минимумы: накопленные для префиксов (на месте), иначе разреженная таблица
        let sparse = if prefix {
            for pos in 1..n_p {
                if (kv[pos - 1], ki[pos - 1]) < (kv[pos], ki[pos]) {
                    kv[pos] = kv[pos - 1];
                    ki[pos] = ki[pos - 1];
                }
                trs[pos] = trs[pos].min(trs[pos - 1]);
            }
            None
        } else if n_p > 0 {
            let keys: Vec<(f64, usize)> = kv.iter().copied().zip(ki.iter().copied()).collect();
            Some((Sparse::new(keys), Sparse::new(trs.clone())))
        } else {
            None
        };
        for &j in &live {
            let (l, h) = (lo[j], hi[j]);
            let cell = d * n_f + j;
            out.cnt[cell] = csum[h] - csum[l];
            let (best, best_tr) = match &sparse {
                None => ((kv[h - 1], ki[h - 1]), trs[h - 1]),
                Some((sk, st)) => (sk.query(l, h), st.query(l, h)),
            };
            let f = f_idx[j];
            if best.0.is_finite() {
                out.minp[cell] = best.0 + nxt.price[f];
                out.tr_at[cell] = tr_at_row[best.1] + nxt.transfers[f];
            } else {
                out.minp[cell] = f64::INFINITY;
                out.tr_at[cell] = tr0 + nxt.transfers[f];
            }
            out.mintr[cell] = best_tr + nxt.transfers[f];
        }
        for j in 0..n_f {
            if !f_ok[j] {
                let cell = d * n_f + j;
                let f = f_idx[j];
                out.tr_at[cell] = tr0 + nxt.transfers[f];
                out.mintr[cell] = BIG_TR + nxt.transfers[f];
            }
        }
    }
    out
}

/// Сколько наборов держит выдача: 1000 самых дешёвых по минимальной цене.
pub const COMBOS_TOP: usize = 1000;

/// Набор в куче лучших: порядок (minPrice, codes) — тот же, что у итоговой сортировки,
/// поэтому вытеснение из кучи даёт ровно первые K полного списка.
struct Ranked(Combo);

impl PartialEq for Ranked {
    fn eq(&self, o: &Self) -> bool {
        self.cmp(o) == std::cmp::Ordering::Equal
    }
}
impl Eq for Ranked {}
impl PartialOrd for Ranked {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Ranked {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        cmp_combos(&self.0, &o.0)
    }
}

/// Порядок выдачи наборов: (minPrice, коды, пропуски).
fn cmp_combos(a: &Combo, b: &Combo) -> std::cmp::Ordering {
    a.min_price.partial_cmp(&b.min_price).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.codes.cmp(&b.codes)).then_with(|| a.skipped.cmp(&b.skipped))
}

/// K лучших наборов: max-куча по (цена, коды); `cutoff` — цена K-го (порог отсечения
/// веток: набор дороже K-го в выдачу уже не попадёт), пока наборов меньше K — ∞.
struct Best {
    heap: BinaryHeap<Ranked>,
    k: usize,
    /// Что-то не вошло в K: набор вытеснен из кучи или ветка отсечена по оценке.
    truncated: bool,
    /// Стыковок групп (extend) сделано; больше max_steps — обход останавливается.
    steps: usize,
    max_steps: usize,
}

impl Best {
    fn cutoff(&self) -> f64 {
        if self.heap.len() >= self.k { self.heap.peek().map(|r| r.0.min_price).unwrap_or(f64::INFINITY) } else { f64::INFINITY }
    }

    /// Ещё один шаг стыковки; false — лимит исчерпан, дальше не идём.
    fn step(&mut self) -> bool {
        self.steps += 1;
        self.steps <= self.max_steps
    }

    fn exhausted(&self) -> bool {
        self.steps > self.max_steps
    }

    fn push(&mut self, c: Combo) {
        self.heap.push(Ranked(c));
        if self.heap.len() > self.k {
            self.heap.pop();
            self.truncated = true;
        }
    }
}

/// {combos по цене, totalCount, cities}: COMBOS_TOP самых дешёвых наборов.
pub fn build_overview(stops: &[Stop], table: &FlightCols, query: Option<&PlanQuery>) -> Overview {
    build_overview_top(stops, table, query, COMBOS_TOP)
}

/// `top` самых дешёвых наборов по минимальной цене — ровно те, что дал бы полный обход с
/// сортировкой по (minPrice, codes). Ветки, чья нижняя оценка (минимальная цена префикса +
/// минимальная цена хвоста по городам, `search::completion_lb`) выше цены K-го найденного
/// набора, не раскрываются; дети обходятся по возрастанию оценки, чтобы порог сжимался раньше.
pub fn build_overview_top(stops: &[Stop], table: &FlightCols, query: Option<&PlanQuery>, top: usize) -> Overview {
    build_overview_limited(stops, table, query, top, MAX_COMBO_STEPS)
}

/// То же с лимитом шагов стыковки (extend): при исчерпании — `incomplete`. С пропускаемыми
/// остановками — по каждому варианту маршрута, затем `top` самых дешёвых из всех.
pub fn build_overview_limited(stops: &[Stop], table: &FlightCols, query: Option<&PlanQuery>, top: usize, max_steps: usize) -> Overview {
    if let Some(q) = query.filter(|q| q.variant.is_none()) {
        let vars = variants(stops, q);
        if vars.len() > 1 {
            let parts: Vec<Overview> = vars.iter().map(|(vs, vq)| build_overview_limited(vs, table, Some(vq), top, max_steps)).collect();
            return merge_overviews(parts, top);
        }
    }
    build_overview_one(stops, table, query, top, max_steps)
}

/// Наборы нескольких вариантов маршрута: `top` самых дешёвых из всех.
fn merge_overviews(parts: Vec<Overview>, top: usize) -> Overview {
    let mut out = Overview::default();
    for p in parts {
        out.truncated |= p.truncated;
        out.incomplete |= p.incomplete;
        out.combos.extend(p.combos);
        out.cities.extend(p.cities);
    }
    out.combos.sort_by(cmp_combos);
    if out.combos.len() > top {
        out.combos.truncate(top);
        out.truncated = true;
    }
    out.total_count = out.combos.iter().map(|c| c.count).sum();
    out
}

fn build_overview_one(stops: &[Stop], table: &FlightCols, query: Option<&PlanQuery>, top: usize, max_steps: usize) -> Overview {
    let skipped: Vec<usize> = query.and_then(|q| q.variant.as_ref()).map(|v| v.skipped.clone()).unwrap_or_default();
    let last = stops.len().saturating_sub(1);
    if last < 1 || top == 0 {
        return Overview::default();
    }
    let rows = leg_rows(table, last, query);
    let legs: Vec<Leg> = (0..last).map(|i| Leg::new(table, &rows[i])).collect();
    let city_filters: HashMap<usize, &CityFilter> = (1..last).filter_map(|i| query.and_then(|q| q.city_filter(i)).map(|cf| (i, cf))).collect();
    let trip = query.map(|q| q.trip_length).unwrap_or((0, None));
    let trip_active = trip.0 != 0 || trip.1.is_some();
    let start_ord = date_ordinal(&chain_start(stops, query)).unwrap_or(0);
    let hops = Hops::new(stops);
    let n_codes = table.codes.len();
    // Разрешённые города прилёта остановки — маской по номерам кодов (None — любой);
    // коды, которых нет в рейсах, прилётом всё равно не встретятся.
    let allow: Vec<Option<Vec<bool>>> = (0..=last)
        .map(|i| {
            hops.arrive_allowed(i).map(|set| {
                let mut bits = vec![false; n_codes];
                for code in &set {
                    if let Some(id) = table.code_id(code) {
                        bits[id as usize] = true;
                    }
                }
                bits
            })
        })
        .collect();
    // Нижняя оценка хвоста по городу (без дат) — та же, что у поиска маршрутов.
    let ctx = build_ctx(stops, table, query);
    let lb = completion_lb(&ctx);
    let day_lb: Option<DayLb> = if RESEARCH_DAY_LB.load(AtOrd::Relaxed) { Some(completion_lb_day(&ctx)) } else { None };

    let mut best = Best { heap: BinaryHeap::new(), k: top, truncated: false, steps: 0, max_steps };

    struct Env<'a> {
        legs: &'a [Leg],
        hops: &'a Hops<'a>,
        table: &'a FlightCols,
        allow: &'a [Option<Vec<bool>>],
        departs: RefCell<FxHashMap<(usize, u32), Vec<u32>>>,
        city_filters: &'a HashMap<usize, &'a CityFilter>,
        trip: (i64, Option<i64>),
        trip_active: bool,
        last: usize,
        lb: &'a [FxHashMap<u32, f64>],
        day_lb: Option<&'a DayLb>,
        skipped: &'a [usize],
    }

    impl Env<'_> {
        /// Минимальная цена хвоста из города `city` на остановке `stop` (0 у финала;
        /// None — из этого города до финала не добраться вовсе).
        fn tail(&self, stop: usize, city: u32) -> Option<f64> {
            if stop == self.last { Some(0.0) } else { self.lb[stop].get(&city).copied() }
        }

        /// То же с учётом дня прилёта (прототип оценки по городу + дню).
        fn tail_day(&self, stop: usize, city: u32, arr_ord: i64) -> Option<f64> {
            if stop == self.last {
                return Some(0.0);
            }
            match self.day_lb {
                Some(dl) => day_lb_get(dl, stop, city, arr_ord, false),
                None => self.lb[stop].get(&city).copied(),
            }
        }
    }

    /// Hops::departs по номерам кодов (один раз на (плечо, город)).
    fn departs(env: &Env, k: usize, city: u32) -> Vec<u32> {
        if let Some(v) = env.departs.borrow().get(&(k, city)) {
            return v.clone();
        }
        let got: Vec<u32> = env.hops.departs(k, &env.table.codes[city as usize]).iter().filter_map(|c| env.table.code_id(c)).collect();
        env.departs.borrow_mut().insert((k, city), got.clone());
        got
    }

    fn onward(env: &Env, k: usize, city: u32) -> Vec<usize> {
        let leg = &env.legs[k];
        let mut parts: Vec<usize> = Vec::new();
        for d in departs(env, k, city) {
            if let Some(v) = leg.by_origin.get(&d) {
                parts.extend(v.iter().copied());
            }
        }
        parts.sort_unstable();
        parts.dedup();
        parts
    }

    /// Разрез кандидатов плеча k по городу прилёта с правилами остановки k+1 (группы —
    /// в порядке первого появления города, как раньше).
    fn groups(env: &Env, leg: &Leg, f_idx: &[usize], k: usize, seq: &[u32]) -> Vec<(u32, Vec<usize>)> {
        let allow = env.allow[k + 1].as_ref();
        let city = *seq.last().unwrap();
        let mut slot: FxHashMap<u32, usize> = FxHashMap::default();
        let mut out: Vec<(u32, Vec<usize>)> = Vec::new();
        for &fi in f_idx {
            let dest = leg.dest[fi];
            if dest == NO_CODE || dest == city || leg.origin_has(fi, dest) {
                continue;
            }
            match allow {
                None => {
                    if seq.contains(&dest) {
                        continue;
                    }
                }
                Some(bits) => {
                    let air = leg.dest_air[fi];
                    if !bits[dest as usize] && (air == NO_CODE || !bits[air as usize]) {
                        continue;
                    }
                }
            }
            match slot.get(&dest) {
                Some(&g) => out[g].1.push(fi),
                None => {
                    slot.insert(dest, out.len());
                    out.push((dest, vec![fi]));
                }
            }
        }
        out
    }

    /// Группы плеча k с предварительной оценкой: минимальная цена префикса + самый дешёвый
    /// рейс группы + хвост из города группы; по возрастанию оценки. Группы без хвоста
    /// (из города не добраться до финала) отброшены.
    fn ranked_groups(env: &Env, leg: &Leg, f_idx: &[usize], k: usize, seq: &[u32], prefix_min: f64) -> Vec<(f64, u32, Vec<usize>)> {
        let mut out: Vec<(f64, u32, Vec<usize>)> = groups(env, leg, f_idx, k, seq)
            .into_iter()
            .filter_map(|(dest, fis)| {
                env.tail(k + 1, dest)?;
                // лучший рейс группы с его хвостом (по дню прилёта, если включена оценка по дню)
                let fmin = fis.iter().filter_map(|&fi| env.tail_day(k + 1, dest, leg.arr_ord[fi]).map(|t| leg.price[fi] + t)).fold(f64::INFINITY, f64::min);
                if !fmin.is_finite() {
                    return None;
                }
                Some((prefix_min + fmin, dest, fis))
            })
            .collect();
        out.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal).then(a.1.cmp(&b.1)));
        out
    }

    fn state_min(state: &State) -> f64 {
        state.minp.iter().copied().fold(f64::INFINITY, f64::min)
    }

    /// Точная нижняя оценка префикса: минимум по ячейкам (minp + хвост из города по дню прилёта рейса).
    fn exact_bound(env: &Env, state: &State, leg: &Leg, f_idx: &[usize], stop: usize, dest: u32) -> f64 {
        let n_f = state.n_p;
        let mut best = f64::INFINITY;
        for (j, &fi) in f_idx.iter().enumerate() {
            let Some(tail) = env.tail_day(stop, dest, leg.arr_ord[fi]) else { continue };
            for d in 0..state.n_days {
                let v = state.minp[d * n_f + j];
                if v.is_finite() && v + tail < best {
                    best = v + tail;
                }
            }
        }
        best
    }

    fn finish(env: &Env, seq: &[u32], leg: &Leg, f_idx: &[usize], state: &State, first_days: &[i64], best: &mut Best) {
        let (n_days, n_f) = (state.n_days, state.n_p);
        let mut cnt = state.cnt.clone();
        let mut minp = state.minp.clone();
        let mut mintr = state.mintr.clone();
        if env.trip_active {
            for d in 0..n_days {
                for j in 0..n_f {
                    let days = (leg.arr_ord[f_idx[j]] - first_days[d]).max(1);
                    let ok = days >= env.trip.0 && env.trip.1.map(|hi| days <= hi).unwrap_or(true);
                    if !ok {
                        let cell = d * n_f + j;
                        cnt[cell] = 0.0;
                        minp[cell] = f64::INFINITY;
                        mintr[cell] = BIG_TR;
                    }
                }
            }
        }
        let total: f64 = cnt.iter().sum();
        if total <= 0.0 {
            return;
        }
        let mut flat = 0;
        for cell in 0..n_days * n_f {
            if minp[cell] < minp[flat] {
                flat = cell;
            }
        }
        best.push(Combo {
            codes: seq.iter().map(|&c| env.table.codes[c as usize].clone()).collect(),
            min_price: minp[flat],
            transfers_at_min: state.tr_at[flat],
            min_transfers: mintr.iter().copied().min().unwrap_or(BIG_TR),
            count: total.round() as i64,
            skipped: env.skipped.to_vec(),
        });
    }

    fn expand(env: &Env, k: usize, seq: &[u32], prev: &Leg, p_idx: &[usize], state: &State, first_days: &[i64], best: &mut Best) {
        let leg = &env.legs[k];
        let city = *seq.last().unwrap();
        let f_all = onward(env, k, city);
        if f_all.is_empty() {
            return;
        }
        let prefix_min = state_min(state);
        for (bound, dest, fis) in ranked_groups(env, leg, &f_all, k, seq, prefix_min) {
            if bound > best.cutoff() {
                best.truncated = true;
                break; // группы по возрастанию оценки — дальше только дороже
            }
            if !best.step() {
                return;
            }
            let hop: Vec<bool> = fis.iter().map(|&fi| !leg.origin_has(fi, city)).collect();
            let new_state = extend(state, prev, p_idx, leg, &fis, env.city_filters.get(&k).copied(), Some(&hop));
            OV_EXTENDS.fetch_add(1, AtOrd::Relaxed);
            if new_state.total() <= 0.0 {
                OV_EMPTY.fetch_add(1, AtOrd::Relaxed);
                continue;
            }
            // точная оценка после стыковки: минимум нового префикса + хвост (по ячейкам —
            // у каждой свой последний рейс и день прилёта)
            let exact = exact_bound(env, &new_state, leg, &fis, k + 1, dest);
            if exact > best.cutoff() {
                OV_PRUNED.fetch_add(1, AtOrd::Relaxed);
                best.truncated = true;
                continue;
            }
            let mut new_seq = seq.to_vec();
            new_seq.push(dest);
            if k + 1 == env.last {
                finish(env, &new_seq, leg, &fis, &new_state, first_days, best);
            } else {
                expand(env, k + 1, &new_seq, leg, &fis, &new_state, first_days, best);
            }
        }
    }

    let env = Env { legs: &legs, hops: &hops, table, allow: &allow, departs: RefCell::new(FxHashMap::default()), city_filters: &city_filters, trip, trip_active, last, lb: &lb, day_lb: day_lb.as_ref(), skipped: &skipped };
    let leg0 = &legs[0];
    let mut starts: Vec<u32> = Vec::new();
    for code in &crate::search::start_codes(stops, table, query) {
        for d in hops.departs(0, code) {
            if let Some(id) = table.code_id(&d) {
                if !starts.contains(&id) {
                    starts.push(id);
                }
            }
        }
    }
    // Первые группы всех стартов вместе — по возрастанию оценки (0 + рейс + хвост).
    let mut first_groups: Vec<(f64, u32, u32, Vec<usize>)> = Vec::new();
    for &start in &starts {
        let Some(f_all) = leg0.by_origin.get(&start) else { continue };
        let seq0 = vec![start];
        for (bound, dest, fis) in ranked_groups(&env, leg0, f_all, 0, &seq0, 0.0) {
            first_groups.push((bound, start, dest, fis));
        }
    }
    first_groups.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
    for (bound, start, dest, fis) in first_groups {
        if bound > best.cutoff() {
            best.truncated = true;
            break;
        }
        if best.exhausted() {
            break;
        }
        let ok: Vec<bool> = fis.iter().map(|&fi| leg0.dep_ord[fi] >= start_ord).collect();
        let n_f = fis.len();
        let (first_days, cnt): (Vec<i64>, Vec<f64>) = if trip_active {
            let mut days: Vec<i64> = fis.iter().map(|&fi| leg0.dep_ord[fi]).collect();
            days.sort_unstable();
            days.dedup();
            let mut cnt = vec![0.0; days.len() * n_f];
            for (d, &day) in days.iter().enumerate() {
                for j in 0..n_f {
                    if leg0.dep_ord[fis[j]] == day && ok[j] {
                        cnt[d * n_f + j] = 1.0;
                    }
                }
            }
            (days, cnt)
        } else {
            (vec![0], ok.iter().map(|&o| if o { 1.0 } else { 0.0 }).collect())
        };
        let n_days = first_days.len();
        let mut state = State { n_days, n_p: n_f, cnt, minp: vec![f64::INFINITY; n_days * n_f], tr_at: vec![0; n_days * n_f], mintr: vec![BIG_TR; n_days * n_f] };
        for d in 0..n_days {
            for j in 0..n_f {
                let cell = d * n_f + j;
                state.tr_at[cell] = leg0.transfers[fis[j]];
                if state.cnt[cell] > 0.0 {
                    state.minp[cell] = leg0.price[fis[j]];
                    state.mintr[cell] = leg0.transfers[fis[j]];
                }
            }
        }
        if state.total() <= 0.0 {
            continue;
        }
        let exact = exact_bound(&env, &state, leg0, &fis, 1, dest);
        if exact > best.cutoff() {
            best.truncated = true;
            continue;
        }
        let seq = vec![start, dest];
        if last == 1 {
            finish(&env, &seq, leg0, &fis, &state, &first_days, &mut best);
        } else {
            expand(&env, 1, &seq, leg0, &fis, &state, &first_days, &mut best);
        }
    }

    let truncated = best.truncated;
    let incomplete = best.exhausted();
    let mut combos: Vec<Combo> = best.heap.into_iter().map(|r| r.0).collect();
    combos.sort_by(cmp_combos);
    let total_count = combos.iter().map(|c| c.count).sum();
    let mut codes: Vec<String> = combos.iter().flat_map(|c| c.codes.iter().cloned()).collect::<HashSet<_>>().into_iter().collect();
    codes.sort();
    let cities = codes.into_iter().map(|c| { let p = city_pair(&c); (c, p) }).collect();
    Overview { combos, total_count, cities, truncated, incomplete }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::tests::{flight, Rng};
    use crate::search::{build_ctx, search_cheapest};
    use serde_json::json;

    fn stops() -> Vec<Stop> {
        vec![
            Stop::new("cities", vec!["MOW"], ["", ""]),
            Stop::new("any", vec![], ["2026-11-01", "2026-11-07"]),
            Stop::new("cities", vec!["SEL"], ["2026-11-01", "2026-11-07"]),
            Stop::new("any", vec![], ["2026-11-01", "2026-11-07"]),
            Stop::new("cities", vec!["MOW"], ["", ""]),
        ]
    }

    fn random_collected(seed: u64) -> Vec<Vec<std::sync::Arc<crate::ticket::Ticket>>> {
        random_collected_to(seed, &["MOW"])
    }

    /// `last` — города прилёта последнего плеча (у «любой» последней остановки — несколько).
    fn random_collected_to(seed: u64, last: &[&'static str]) -> Vec<Vec<std::sync::Arc<crate::ticket::Ticket>>> {
        let mut rng = Rng(seed * 104729 + 3);
        let cities: [Vec<&str>; 5] = [vec!["MOW"], vec!["IST", "DXB", "DOH", "AUH"], vec!["SEL"], vec!["HKG", "BKK", "SIN", "IST"], last.to_vec()];
        let mut collected = Vec::new();
        for i in 0..4 {
            let mut flights = Vec::new();
            for o in &cities[i] {
                for d in &cities[i + 1] {
                    if o == d {
                        continue;
                    }
                    for _ in 0..(1 + rng.below(4)) {
                        let day = 1 + rng.below(7) as u32;
                        let h = rng.below(23) as u32;
                        let mut t = (*flight(o, d, day, h, (50 + rng.below(451)) as f64)).clone();
                        let arr_day = if h < 18 { day } else { (day + 1).min(7) };
                        t.arrival_at = Some(format!("2026-11-{arr_day:02}T{:02}:30:00+03:00", (h + 5) % 24));
                        t.transfers = rng.below(3) as i64;
                        t.baggage = Some(crate::ticket::Baggage { known: true, included: rng.below(10) < 6, pieces: None, kg: None });
                        t.duration = Some(300);
                        t.flight_number = Some(format!("{}", flights.len()));
                        flights.push(std::sync::Arc::new(t));
                    }
                }
            }
            collected.push(flights);
        }
        collected
    }

    fn query(extra: serde_json::Value) -> PlanQuery {
        let mut v = json!({"stops": stops().iter().map(|s| json!({"kind": s.kind, "codes": s.codes, "window": s.window})).collect::<Vec<_>>(), "maxResults": 100000});
        for (k, val) in extra.as_object().unwrap() {
            v[k] = val.clone();
        }
        PlanQuery::from_value(&v).unwrap()
    }

    #[test]
    fn overview_matches_full_enumeration() {
        let cases = vec![
            (1u64, json!({})),
            (2, json!({"cities": [{}, {"minStay": 1, "maxStay": 3}, {"requireWeekend": true}, {"minStay": 0}, {}]})),
            (3, json!({"legs": [{"maxTransfers": 1}, {"baggage": "included"}, {}, {"maxTransfers": 0}]})),
            (4, json!({"tripLength": [3, 6]})),
            (5, json!({"cities": [{}, {"mustCover": ["2026-11-02", "2026-11-03"]}, {}, {"maxStay": 2}, {}], "tripLength": [2, null], "legs": [{}, {}, {"maxTransfers": 1}, {}]})),
        ];
        let ends = [(false, false), (true, false), (false, true), (true, true)];
        for (seed, extra, (any_first, any_last)) in cases.into_iter().flat_map(|(s, e)| ends.map(|f| (s, e.clone(), f))) {
            let mut stops = stops();
            if any_first {
                // «любая» первая остановка: старты — все города вылета первого плеча
                stops[0] = Stop { kind: "any".into(), codes: Vec::new(), ..stops[0].clone() };
            }
            if any_last {
                // «любая» последняя: прилёт в любой ещё не посещённый город
                stops[4] = Stop { kind: "any".into(), codes: Vec::new(), ..stops[4].clone() };
            }
            let collected = if any_last { random_collected_to(seed, &["MOW", "PAR", "TYO", "DXB"]) } else { random_collected(seed) };
            let table = FlightCols::from_collected(&collected);
            let q = query(extra.clone());
            let got = build_overview(&stops, &table, Some(&q));
            // эталон: полный A* с теми же фильтрами (проверен против перебора в search)
            let ctx = build_ctx(&stops, &table, Some(&q));
            let mut check = |_: usize| Ok(());
            let chains = search_cheapest(&ctx, 100000, Some(&q), &mut check).unwrap();
            assert!(!chains.is_empty() || !extra.as_object().unwrap().is_empty(), "seed {seed}: пусто ({any_first}, {any_last})");
            let mut expected: HashMap<Vec<String>, (i64, f64, i64, i64)> = HashMap::new();
            for c in &chains {
                let mut codes = vec![table.orig_city(c[0]).to_string()];
                codes.extend(c.iter().map(|&fi| table.dest(fi).to_string()));
                let price: f64 = c.iter().map(|&fi| table.price[fi]).sum();
                let tr: i64 = c.iter().map(|&fi| table.transfers[fi]).sum();
                let e = expected.entry(codes).or_insert((0, f64::INFINITY, 0, 99));
                e.0 += 1;
                if price < e.1 {
                    e.1 = price;
                    e.2 = tr;
                }
                e.3 = e.3.min(tr);
            }
            assert_eq!(got.total_count as usize, chains.len(), "seed {seed} {extra}");
            assert_eq!(got.combos.iter().map(|c| c.codes.clone()).collect::<HashSet<_>>(), expected.keys().cloned().collect::<HashSet<_>>(), "seed {seed}");
            for c in &got.combos {
                let e = &expected[&c.codes];
                assert_eq!(c.count, e.0, "count {c:?}");
                assert_eq!(c.min_price, e.1, "minPrice {c:?}");
                assert_eq!(c.min_transfers, e.3, "minTransfers {c:?}");
            }
            let prices: Vec<f64> = got.combos.iter().map(|c| c.min_price).collect();
            let mut sorted = prices.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
            assert_eq!(prices, sorted);
            assert_eq!(got.cities.keys().cloned().collect::<HashSet<_>>(), got.combos.iter().flat_map(|c| c.codes.clone()).collect::<HashSet<_>>());
        }
    }

    /// Плечи с большим числом городов: наборов много больше K.
    fn wide_collected(seed: u64) -> Vec<Vec<std::sync::Arc<crate::ticket::Ticket>>> {
        let mut rng = Rng(seed * 7 + 11);
        let mid: Vec<&str> = vec!["IST", "DXB", "DOH", "AUH", "TAS", "ALA", "EVN", "TBS", "BKK", "SIN", "HKG", "DEL"];
        let cities: [Vec<&str>; 5] = [vec!["MOW"], mid.clone(), vec!["SEL"], mid.clone(), vec!["MOW"]];
        let mut collected = Vec::new();
        for i in 0..4 {
            let mut flights = Vec::new();
            for o in &cities[i] {
                for d in &cities[i + 1] {
                    if o == d {
                        continue;
                    }
                    for _ in 0..(2 + rng.below(3)) {
                        let day = 1 + rng.below(20) as u32;
                        let h = rng.below(23) as u32;
                        let mut t = (*flight(o, d, day, h, (50 + rng.below(451)) as f64)).clone();
                        let arr_day = if h < 18 { day } else { (day + 1).min(20) };
                        t.arrival_at = Some(format!("2026-11-{arr_day:02}T{:02}:30:00+03:00", (h + 5) % 24));
                        t.transfers = rng.below(3) as i64;
                        t.duration = Some(300);
                        t.flight_number = Some(format!("{}", flights.len()));
                        flights.push(std::sync::Arc::new(t));
                    }
                }
            }
            collected.push(flights);
        }
        collected
    }

    /// Топ-K с отсечением — ровно первые K полного списка (сортировка по (minPrice, codes)),
    /// флаг truncated — когда наборов больше K.
    #[test]
    fn top_k_matches_full_enumeration() {
        for seed in 1..=4 {
            let stops: Vec<Stop> = stops().into_iter().map(|s| Stop { window: if s.window[0].is_empty() { s.window } else { ["2026-11-01".into(), "2026-11-20".into()] }, ..s }).collect();
            let table = FlightCols::from_collected(&wide_collected(seed));
            let q = PlanQuery::from_value(&json!({"stops": stops.iter().map(|s| json!({"kind": s.kind, "codes": s.codes, "window": s.window})).collect::<Vec<_>>(), "cities": [{}, {"minStay": 1}, {}, {}, {}], "maxResults": 100000})).unwrap();
            let full = build_overview_top(&stops, &table, Some(&q), usize::MAX);
            assert!(full.combos.len() > 20, "мало наборов: {}", full.combos.len());
            assert!(!full.truncated);
            for k in [1usize, 3, 10, 17, full.combos.len() - 1, full.combos.len(), full.combos.len() + 5] {
                let got = build_overview_top(&stops, &table, Some(&q), k);
                let want: Vec<&Combo> = full.combos.iter().take(k).collect();
                assert_eq!(got.combos.len(), want.len(), "seed {seed} k {k}");
                for (g, w) in got.combos.iter().zip(want.iter()) {
                    assert_eq!(g, *w, "seed {seed} k {k}");
                }
                assert_eq!(got.truncated, k < full.combos.len(), "seed {seed} k {k}");
                assert_eq!(got.total_count, want.iter().map(|c| c.count).sum::<i64>());
                assert_eq!(got.cities.keys().cloned().collect::<HashSet<_>>(), got.combos.iter().flat_map(|c| c.codes.clone()).collect::<HashSet<_>>());
            }
        }
    }

    /// Лимит шагов: обход останавливается, выдаются найденные, флаг incomplete.
    #[test]
    fn step_limit_marks_incomplete() {
        let stops: Vec<Stop> = stops().into_iter().map(|s| Stop { window: if s.window[0].is_empty() { s.window } else { ["2026-11-01".into(), "2026-11-20".into()] }, ..s }).collect();
        let table = FlightCols::from_collected(&wide_collected(2));
        let q = PlanQuery::from_value(&json!({"stops": stops.iter().map(|s| json!({"kind": s.kind, "codes": s.codes, "window": s.window})).collect::<Vec<_>>(), "maxResults": 100000})).unwrap();
        let full = build_overview_top(&stops, &table, Some(&q), usize::MAX);
        assert!(!full.incomplete);
        let cut = build_overview_limited(&stops, &table, Some(&q), usize::MAX, 3);
        assert!(cut.incomplete);
        assert!(cut.combos.len() < full.combos.len());
        for c in &cut.combos {
            assert!(full.combos.contains(c));
        }
    }

    #[test]
    fn same_day_connection_gives_no_combos() {
        let three = vec![
            Stop::new("cities", vec!["MOW"], ["", ""]),
            Stop::new("any", vec![], ["2026-11-01", "2026-11-03"]),
            Stop::new("cities", vec!["SEL"], ["2026-11-01", "2026-11-03"]),
        ];
        let q = PlanQuery::from_value(&json!({"stops": three.iter().map(|s| json!({"kind": s.kind, "codes": s.codes, "window": s.window})).collect::<Vec<_>>(), "maxResults": 10})).unwrap();
        let first = vec![flight("MOW", "IST", 1, 10, 100.0)]; // прилёт 01.11 14:00
        let same_day = FlightCols::from_collected(&[first.clone(), vec![flight("IST", "SEL", 1, 20, 100.0)]]);
        assert!(build_overview(&three, &same_day, Some(&q)).combos.is_empty());
        let next_day = FlightCols::from_collected(&[first, vec![flight("IST", "SEL", 2, 0, 100.0)]]);
        let got = build_overview(&three, &next_day, Some(&q));
        assert_eq!(got.combos.len(), 1);
        assert_eq!(got.total_count, 1);
    }

    #[test]
    fn empty_and_two_stops() {
        let two = vec![Stop::new("cities", vec!["MOW"], ["", ""]), Stop::new("cities", vec!["IST", "DXB"], ["2026-11-01", "2026-11-02"])];
        let mk = |d: &str, day: u32, price: f64, tr: i64| {
            let mut t = (*flight("MOW", d, day, 10, price)).clone();
            t.transfers = tr;
            std::sync::Arc::new(t)
        };
        let collected = vec![vec![mk("IST", 1, 100.0, 0), mk("IST", 2, 90.0, 1), mk("DXB", 2, 120.0, 0)]];
        let table = FlightCols::from_collected(&collected);
        let q = PlanQuery::from_value(&json!({"stops": two.iter().map(|s| json!({"kind": s.kind, "codes": s.codes, "window": s.window})).collect::<Vec<_>>(), "maxResults": 10})).unwrap();
        let got = build_overview(&two, &table, Some(&q));
        assert_eq!(
            got.combos,
            vec![
                Combo { codes: vec!["MOW".into(), "IST".into()], min_price: 90.0, transfers_at_min: 1, min_transfers: 0, count: 2, skipped: vec![] },
                Combo { codes: vec!["MOW".into(), "DXB".into()], min_price: 120.0, transfers_at_min: 0, min_transfers: 0, count: 1, skipped: vec![] },
            ]
        );
        let empty = FlightCols::from_collected(&[]);
        assert!(build_overview(&two, &empty, Some(&q)).combos.is_empty());
    }
}
