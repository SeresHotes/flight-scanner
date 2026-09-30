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

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use rustc_hash::FxHashMap;
use serde::Serialize;

use crate::dates::{date_ordinal, weekend_deadline, DAY_SECONDS};
use crate::flightcols::{FlightCols, NO_CODE};
use crate::nearby::{Hops, HOP_MIN_GAP_MIN};
use crate::planquery::{CityFilter, PlanQuery};
use crate::search::leg_rows;
use crate::segments::city_pair;
use crate::stops::{leg_dates, Stop};

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
}

#[derive(Debug, Clone, Default)]
pub struct Overview {
    pub combos: Vec<Combo>,
    pub total_count: i64,
    pub cities: HashMap<String, (String, String)>,
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
    let mut day_thr: Vec<i64> = p_idx.iter().map(|&p| prev.arr_ord[p]).collect();
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

/// {combos по цене, totalCount, cities}.
pub fn build_overview(stops: &[Stop], table: &FlightCols, query: Option<&PlanQuery>) -> Overview {
    let last = stops.len().saturating_sub(1);
    if last < 1 {
        return Overview::default();
    }
    let budget = query.and_then(|q| q.max_cost);
    let mut rows = leg_rows(table, last, query);
    if let Some(b) = budget {
        for r in rows.iter_mut() {
            r.retain(|&x| table.price[x] <= b);
        }
    }
    let legs: Vec<Leg> = (0..last).map(|i| Leg::new(table, &rows[i])).collect();
    let city_filters: HashMap<usize, &CityFilter> = (1..last).filter_map(|i| query.and_then(|q| q.city_filter(i)).map(|cf| (i, cf))).collect();
    let trip = query.map(|q| q.trip_length).unwrap_or((0, None));
    let trip_active = trip.0 != 0 || trip.1.is_some();
    let start_ord = date_ordinal(&leg_dates(stops, 0)[0]).unwrap_or(0);
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

    let mut combos: Vec<Combo> = Vec::new();
    let mut codes_used: HashSet<u32> = HashSet::new();

    struct Env<'a> {
        legs: &'a [Leg],
        hops: &'a Hops<'a>,
        table: &'a FlightCols,
        allow: &'a [Option<Vec<bool>>],
        departs: RefCell<FxHashMap<(usize, u32), Vec<u32>>>,
        city_filters: &'a HashMap<usize, &'a CityFilter>,
        trip: (i64, Option<i64>),
        trip_active: bool,
        budget: Option<f64>,
        last: usize,
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

    fn finish(env: &Env, seq: &[u32], leg: &Leg, f_idx: &[usize], state: &State, first_days: &[i64], combos: &mut Vec<Combo>, used: &mut HashSet<u32>) {
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
        if let Some(b) = env.budget {
            for cell in 0..n_days * n_f {
                if !(minp[cell] <= b) {
                    cnt[cell] = 0.0;
                    minp[cell] = f64::INFINITY;
                    mintr[cell] = BIG_TR;
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
        combos.push(Combo {
            codes: seq.iter().map(|&c| env.table.codes[c as usize].clone()).collect(),
            min_price: minp[flat],
            transfers_at_min: state.tr_at[flat],
            min_transfers: mintr.iter().copied().min().unwrap_or(BIG_TR),
            count: total.round() as i64,
        });
        used.extend(seq.iter().copied());
    }

    fn expand(env: &Env, k: usize, seq: &[u32], prev: &Leg, p_idx: &[usize], state: &State, first_days: &[i64], combos: &mut Vec<Combo>, used: &mut HashSet<u32>) {
        let leg = &env.legs[k];
        let city = *seq.last().unwrap();
        let f_all = onward(env, k, city);
        if f_all.is_empty() {
            return;
        }
        for (dest, fis) in groups(env, leg, &f_all, k, seq) {
            let hop: Vec<bool> = fis.iter().map(|&fi| !leg.origin_has(fi, city)).collect();
            let new_state = extend(state, prev, p_idx, leg, &fis, env.city_filters.get(&k).copied(), Some(&hop));
            if new_state.total() <= 0.0 {
                continue;
            }
            let mut new_seq = seq.to_vec();
            new_seq.push(dest);
            if k + 1 == env.last {
                finish(env, &new_seq, leg, &fis, &new_state, first_days, combos, used);
            } else {
                expand(env, k + 1, &new_seq, leg, &fis, &new_state, first_days, combos, used);
            }
        }
    }

    let env = Env { legs: &legs, hops: &hops, table, allow: &allow, departs: RefCell::new(FxHashMap::default()), city_filters: &city_filters, trip, trip_active, budget, last };
    let leg0 = &legs[0];
    let mut starts: Vec<u32> = Vec::new();
    for code in &stops[0].codes {
        for d in hops.departs(0, code) {
            if let Some(id) = table.code_id(&d) {
                if !starts.contains(&id) {
                    starts.push(id);
                }
            }
        }
    }
    for &start in &starts {
        let Some(f_all) = leg0.by_origin.get(&start) else { continue };
        let seq0 = vec![start];
        for (dest, fis) in groups(&env, leg0, f_all, 0, &seq0) {
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
            let seq = vec![start, dest];
            if last == 1 {
                finish(&env, &seq, leg0, &fis, &state, &first_days, &mut combos, &mut codes_used);
            } else {
                expand(&env, 1, &seq, leg0, &fis, &state, &first_days, &mut combos, &mut codes_used);
            }
        }
    }

    combos.sort_by(|a, b| a.min_price.partial_cmp(&b.min_price).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.codes.cmp(&b.codes)));
    let total_count = combos.iter().map(|c| c.count).sum();
    let mut codes: Vec<String> = codes_used.into_iter().map(|c| table.codes[c as usize].clone()).collect();
    codes.sort();
    let cities = codes.into_iter().map(|c| { let p = city_pair(&c); (c, p) }).collect();
    Overview { combos, total_count, cities }
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
        let mut rng = Rng(seed * 104729 + 3);
        let cities: [Vec<&str>; 5] = [vec!["MOW"], vec!["IST", "DXB", "DOH", "AUH"], vec!["SEL"], vec!["HKG", "BKK", "SIN", "IST"], vec!["MOW"]];
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
            (6, json!({"maxCost": 600})),
        ];
        for (seed, extra) in cases {
            let stops = stops();
            let collected = random_collected(seed);
            let table = FlightCols::from_collected(&collected);
            let q = query(extra.clone());
            let got = build_overview(&stops, &table, Some(&q));
            // эталон: полный A* с теми же фильтрами (проверен против перебора в search)
            let ctx = build_ctx(&stops, &table, Some(&q));
            let mut check = |_: usize| Ok(());
            let chains = search_cheapest(&ctx, 100000, q.max_cost, Some(&q), &mut check).unwrap();
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
                Combo { codes: vec!["MOW".into(), "IST".into()], min_price: 90.0, transfers_at_min: 1, min_transfers: 0, count: 2 },
                Combo { codes: vec!["MOW".into(), "DXB".into()], min_price: 120.0, transfers_at_min: 0, min_transfers: 0, count: 1 },
            ]
        );
        let empty = FlightCols::from_collected(&[]);
        assert!(build_overview(&two, &empty, Some(&q)).combos.is_empty());
    }
}
