//! Движок планировщика на Rust: перебор A* самых дешёвых цепочек (core.planner.
//! _search_cheapest) и наборы городов (core.overview.build_overview) — порт один в
//! один, включая выбор при равных ценах. Вход — колонки рейсов джобы (core.flightcols)
//! с кодами городов/аэропортов числами (-1 — пусто) и строки плеч после фильтров
//! плеча; переезд в соседний город (радиус > 0) не поддержан — его считает Python.
use numpy::{IntoPyArray, PyArray1, PyReadonlyArray1};
use pyo3::prelude::*;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

const DAY: f64 = 86400.0;
const HOP_GAP: f64 = 240.0 * 60.0; // core.nearby.HOP_MIN_GAP_MIN
const BIG_TR: i64 = 1 << 20;
const INF: f64 = f64::INFINITY;

struct Cols<'a> {
    oc: &'a [i32],
    oa: &'a [i32],
    dest: &'a [i32],
    da: &'a [i32],
    price: &'a [f64],
    transfers: &'a [i32],
    dep_ts: &'a [f64],
    arr_ts: &'a [f64],
    dep_ord: &'a [i64],
    arr_ord: &'a [i64],
}

#[derive(Clone, Copy)]
struct CityF {
    min_stay: i64,
    max_stay: Option<i64>,
    cover: Option<(i64, i64)>,
    weekend: bool,
}

/// Первый день D ≥ arr, при котором [arr, D] покрывает и сб, и вс (overview._weekend_deadline).
fn weekend_deadline(o: i64) -> i64 {
    let wd = (o + 6).rem_euclid(7);
    o + if wd == 6 { 6 } else if wd == 5 { 1 } else { 6 - wd }
}

fn allowed(allow: &Option<HashSet<i32>>, dest: i32, da: i32) -> bool {
    match allow {
        None => true,
        Some(s) => s.contains(&dest) || (da >= 0 && s.contains(&da)),
    }
}

fn parse_filters(raw: Vec<Option<(i64, i64, i64, i64, bool)>>) -> Vec<Option<CityF>> {
    raw.into_iter()
        .map(|o| {
            o.map(|(min_stay, max_stay, c0, c1, weekend)| CityF {
                min_stay,
                max_stay: if max_stay < 0 { None } else { Some(max_stay) },
                cover: if c0 < 0 { None } else { Some((c0, c1)) },
                weekend,
            })
        })
        .collect()
}

fn parse_allow(stop_any: &[bool], stop_codes: &[Vec<i32>]) -> Vec<Option<HashSet<i32>>> {
    stop_any
        .iter()
        .zip(stop_codes)
        .map(|(&any, codes)| if any { None } else { Some(codes.iter().copied().collect()) })
        .collect()
}

// ----------------------------------- A* --------------------------------------

fn by_origin(c: &Cols, rows: &[i64]) -> HashMap<i32, Vec<u32>> {
    let mut m: HashMap<i32, Vec<u32>> = HashMap::new();
    for &r in rows {
        let r = r as usize;
        let (a, b) = (c.oc[r], c.oa[r]);
        if a >= 0 {
            m.entry(a).or_default().push(r as u32);
        }
        if b >= 0 && b != a {
            m.entry(b).or_default().push(r as u32);
        }
    }
    m
}

fn completion_lb(last: usize, allow: &[Option<HashSet<i32>>], bo: &[HashMap<i32, Vec<u32>>], c: &Cols) -> Vec<HashMap<i32, f64>> {
    let mut lb: Vec<HashMap<i32, f64>> = vec![HashMap::new(); last + 1];
    for i in (0..last).rev() {
        let terminal = i + 1 == last;
        let mut dep = HashMap::new();
        for (&city, rows) in &bo[i] {
            let mut best = INF;
            for &r in rows {
                let r = r as usize;
                let d = c.dest[r];
                if d < 0 || d == city || !allowed(&allow[i + 1], d, c.da[r]) {
                    continue;
                }
                let tail = if terminal {
                    0.0
                } else {
                    match lb[i + 1].get(&d) {
                        Some(t) => *t,
                        None => continue,
                    }
                };
                let cand = c.price[r] + tail;
                if cand < best {
                    best = cand;
                }
            }
            if best < INF {
                dep.insert(city, best);
            }
        }
        lb[i] = dep;
    }
    lb
}

struct Cand {
    keys: Vec<f64>,
    idxs: Vec<u32>,
    hop: Vec<bool>,
}

struct Node {
    i: usize,
    city: i32,
    arrive_ord: i64,
    arrive_ts: f64,
    g: f64,
    visited: Vec<i32>,
    path: Vec<u32>,
}

#[derive(PartialEq)]
struct Entry {
    f: f64,
    seq: u64,
    node: usize,
    k: usize,
}
impl Eq for Entry {}
impl Ord for Entry {
    fn cmp(&self, o: &Self) -> Ordering {
        // BinaryHeap — max-куча; нужна min по (f, seq), как heapq в Python
        o.f.total_cmp(&self.f).then(o.seq.cmp(&self.seq))
    }
}
impl PartialOrd for Entry {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}

fn stay_ok(cf: &CityF, arr_ts: f64, arr_ord: i64, dep_ts: f64, dep_ord: i64) -> bool {
    let days = ((dep_ts - arr_ts) / DAY).floor() as i64;
    if days < cf.min_stay || cf.max_stay.map_or(false, |m| days > m) {
        return false;
    }
    if let Some((c0, c1)) = cf.cover {
        if !(arr_ord <= c0 && dep_ord >= c1) {
            return false;
        }
    }
    if cf.weekend && !(dep_ts >= arr_ts && weekend_deadline(arr_ord) <= dep_ord) {
        return false;
    }
    true
}

struct Search<'a> {
    c: Cols<'a>,
    last: usize,
    allow: Vec<Option<HashSet<i32>>>,
    any_stop: Vec<bool>,
    cf: Vec<Option<CityF>>,
    bo: Vec<HashMap<i32, Vec<u32>>>,
    lb: Vec<HashMap<i32, f64>>,
    cands: HashMap<(usize, i32), Cand>,
    max_cost: Option<f64>,
    nodes: Vec<Node>,
    heap: BinaryHeap<Entry>,
    seq: u64,
}

impl<'a> Search<'a> {
    fn cand(&mut self, i: usize, city: i32) -> &Cand {
        if !self.cands.contains_key(&(i, city)) {
            let terminal = i + 1 == self.last;
            let c = &self.c;
            let mut pairs: Vec<(f64, u32, bool)> = Vec::new();
            if let Some(rows) = self.bo[i].get(&city) {
                for &fi in rows {
                    let r = fi as usize;
                    let d = c.dest[r];
                    if d < 0 || d == city || c.dep_ord[r] < 0 || !allowed(&self.allow[i + 1], d, c.da[r]) {
                        continue;
                    }
                    let tail = if terminal {
                        0.0
                    } else {
                        match self.lb[i + 1].get(&d) {
                            Some(t) => *t,
                            None => continue,
                        }
                    };
                    pairs.push((c.price[r] + tail, fi, city != c.oc[r] && city != c.oa[r]));
                }
            }
            pairs.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            let cand = Cand {
                keys: pairs.iter().map(|p| p.0).collect(),
                idxs: pairs.iter().map(|p| p.1).collect(),
                hop: pairs.iter().map(|p| p.2).collect(),
            };
            self.cands.insert((i, city), cand);
        }
        &self.cands[&(i, city)]
    }

    fn push_next(&mut self, node_id: usize, start: usize) {
        let (i, city) = (self.nodes[node_id].i, self.nodes[node_id].city);
        self.cand(i, city);
        let cand = &self.cands[&(i, city)];
        let n = &self.nodes[node_id];
        let any_next = self.any_stop[i + 1];
        let cf = self.cf[i];
        let c = &self.c;
        for k in start..cand.idxs.len() {
            let f = n.g + cand.keys[k];
            if let Some(mc) = self.max_cost {
                if f > mc {
                    return; // список отсортирован — дальше дороже
                }
            }
            let fi = cand.idxs[k] as usize;
            if c.dep_ord[fi] < n.arrive_ord {
                continue;
            }
            if cand.hop[k] && i > 0 && !(c.dep_ts[fi] - n.arrive_ts >= HOP_GAP) {
                continue;
            }
            if any_next && n.visited.contains(&c.dest[fi]) {
                continue;
            }
            if let Some(cf) = cf {
                if !stay_ok(&cf, n.arrive_ts, n.arrive_ord, c.dep_ts[fi], c.dep_ord[fi]) {
                    continue;
                }
            }
            self.heap.push(Entry { f, seq: self.seq, node: node_id, k });
            self.seq += 1;
            return;
        }
    }
}

/// Цепочки (по last рейсов-строк таблицы) по возрастанию цены — плоский массив.
#[pyfunction]
#[pyo3(signature = (stop_any, stop_codes, leg_rows, oc, oa, dest, da, price, transfers, dep_ts, arr_ts,
                    dep_ord, arr_ord, city_filters, trip_length, max_results, max_cost, start_ord))]
#[allow(clippy::too_many_arguments)]
fn search<'py>(
    py: Python<'py>,
    stop_any: Vec<bool>,
    stop_codes: Vec<Vec<i32>>,
    leg_rows: Vec<PyReadonlyArray1<'py, i64>>,
    oc: PyReadonlyArray1<'py, i32>,
    oa: PyReadonlyArray1<'py, i32>,
    dest: PyReadonlyArray1<'py, i32>,
    da: PyReadonlyArray1<'py, i32>,
    price: PyReadonlyArray1<'py, f64>,
    transfers: PyReadonlyArray1<'py, i32>,
    dep_ts: PyReadonlyArray1<'py, f64>,
    arr_ts: PyReadonlyArray1<'py, f64>,
    dep_ord: PyReadonlyArray1<'py, i64>,
    arr_ord: PyReadonlyArray1<'py, i64>,
    city_filters: Vec<Option<(i64, i64, i64, i64, bool)>>,
    trip_length: Option<(i64, i64)>,
    max_results: usize,
    max_cost: Option<f64>,
    start_ord: i64,
) -> PyResult<Bound<'py, PyArray1<i64>>> {
    let c = Cols {
        oc: oc.as_slice()?,
        oa: oa.as_slice()?,
        dest: dest.as_slice()?,
        da: da.as_slice()?,
        price: price.as_slice()?,
        transfers: transfers.as_slice()?,
        dep_ts: dep_ts.as_slice()?,
        arr_ts: arr_ts.as_slice()?,
        dep_ord: dep_ord.as_slice()?,
        arr_ord: arr_ord.as_slice()?,
    };
    let last = stop_any.len() - 1;
    let allow = parse_allow(&stop_any, &stop_codes);
    let rows: Vec<&[i64]> = leg_rows.iter().map(|r| r.as_slice()).collect::<Result<_, _>>()?;
    let bo: Vec<HashMap<i32, Vec<u32>>> = rows.iter().map(|r| by_origin(&c, r)).collect();
    let lb = completion_lb(last, &allow, &bo, &c);
    let mut s = Search {
        c,
        last,
        allow,
        any_stop: stop_any,
        cf: parse_filters(city_filters),
        bo,
        lb,
        cands: HashMap::new(),
        max_cost,
        nodes: Vec::new(),
        heap: BinaryHeap::new(),
        seq: 0,
    };
    let start_ts = (start_ord - 719163) as f64 * DAY;
    for &start in &stop_codes[0] {
        match s.lb[0].get(&start) {
            None => continue,
            Some(&tail) => {
                if s.max_cost.map_or(false, |mc| tail > mc) {
                    continue;
                }
            }
        }
        s.nodes.push(Node { i: 0, city: start, arrive_ord: start_ord, arrive_ts: start_ts, g: 0.0,
                            visited: vec![start], path: Vec::new() });
        let id = s.nodes.len() - 1;
        s.push_next(id, 0);
    }
    // trip_length: (lo, hi), hi < 0 — без потолка; None — фильтра нет
    let mut out: Vec<i64> = Vec::new();
    let mut found = 0usize;
    while found < max_results {
        let Some(e) = s.heap.pop() else { break };
        s.push_next(e.node, e.k + 1); // брат занимает место извлечённого
        let (i, city) = (s.nodes[e.node].i, s.nodes[e.node].city);
        let fi = s.cands[&(i, city)].idxs[e.k] as usize;
        if i + 1 == last {
            let n = &s.nodes[e.node];
            let first = if n.path.is_empty() { fi } else { n.path[0] as usize };
            if let Some((lo, hi)) = trip_length {
                let days = (s.c.arr_ord[fi] - s.c.dep_ord[first]).max(1);
                if days < lo || (hi >= 0 && days > hi) {
                    continue;
                }
            }
            out.extend(n.path.iter().map(|&x| x as i64));
            out.push(fi as i64);
            found += 1;
            continue;
        }
        let n = &s.nodes[e.node];
        let d = s.c.dest[fi];
        let mut visited = n.visited.clone();
        visited.push(d);
        let mut path = n.path.clone();
        path.push(fi as u32);
        let node = Node { i: i + 1, city: d, arrive_ord: s.c.arr_ord[fi], arrive_ts: s.c.arr_ts[fi],
                          g: n.g + s.c.price[fi], visited, path };
        s.nodes.push(node);
        let id = s.nodes.len() - 1;
        s.push_next(id, 0);
    }
    Ok(out.into_pyarray(py))
}

// ------------------------------ наборы городов --------------------------------

struct Leg {
    dest: Vec<i32>,
    da: Vec<i32>,
    oc: Vec<i32>,
    oa: Vec<i32>,
    price: Vec<f64>,
    transfers: Vec<i64>,
    dep_ts: Vec<f64>,
    arr_ts: Vec<f64>,
    dep_ord: Vec<i64>,
    arr_ord: Vec<i64>,
    wok: Vec<i64>,
    by_origin: HashMap<i32, Vec<usize>>,
}

impl Leg {
    fn new(c: &Cols, rows: &[i64], budget: Option<f64>) -> Leg {
        let rows: Vec<usize> = rows
            .iter()
            .map(|&r| r as usize)
            .filter(|&r| budget.map_or(true, |b| c.price[r] <= b) && c.dep_ord[r] >= 0)
            .collect();
        let mut by_origin: HashMap<i32, Vec<usize>> = HashMap::new();
        for (i, &r) in rows.iter().enumerate() {
            let (a, b) = (c.oc[r], c.oa[r]);
            if a >= 0 {
                by_origin.entry(a).or_default().push(i);
            }
            if b >= 0 && b != a {
                by_origin.entry(b).or_default().push(i);
            }
        }
        let arr_ord: Vec<i64> = rows.iter().map(|&r| c.arr_ord[r]).collect();
        Leg {
            dest: rows.iter().map(|&r| c.dest[r]).collect(),
            da: rows.iter().map(|&r| c.da[r]).collect(),
            oc: rows.iter().map(|&r| c.oc[r]).collect(),
            oa: rows.iter().map(|&r| c.oa[r]).collect(),
            price: rows.iter().map(|&r| c.price[r]).collect(),
            transfers: rows.iter().map(|&r| c.transfers[r] as i64).collect(),
            dep_ts: rows.iter().map(|&r| c.dep_ts[r]).collect(),
            arr_ts: rows.iter().map(|&r| c.arr_ts[r]).collect(),
            dep_ord: rows.iter().map(|&r| c.dep_ord[r]).collect(),
            wok: arr_ord.iter().map(|&o| weekend_deadline(o)).collect(),
            arr_ord,
            by_origin,
        }
    }
}

/// D × P: число цепочек, минимальная цена, пересадки при минимуме, минимум пересадок.
struct St {
    d: usize,
    p: usize,
    cnt: Vec<f64>,
    minp: Vec<f64>,
    tr_at: Vec<i64>,
    mintr: Vec<i64>,
}

/// Минимум на отрезках [lo, hi) — накопленный (все lo = 0) или разреженная таблица.
struct RangeMin<T: Copy> {
    levels: Vec<Vec<T>>,
}
impl<T: Copy> RangeMin<T> {
    fn new(v: Vec<T>, prefix: bool, less: impl Fn(&T, &T) -> bool) -> Self {
        if prefix {
            let mut acc = v;
            for k in 1..acc.len() {
                if less(&acc[k - 1], &acc[k]) {
                    acc[k] = acc[k - 1];
                }
            }
            return RangeMin { levels: vec![acc] };
        }
        let mut levels = vec![v];
        let mut w = 1;
        while w * 2 <= levels[0].len() {
            let prev = levels.last().unwrap();
            let next: Vec<T> = (0..prev.len() - w)
                .map(|i| if less(&prev[i + w], &prev[i]) { prev[i + w] } else { prev[i] })
                .collect();
            levels.push(next);
            w *= 2;
        }
        RangeMin { levels }
    }
    fn query(&self, lo: usize, hi: usize, prefix: bool, less: impl Fn(&T, &T) -> bool) -> T {
        if prefix {
            return self.levels[0][hi - 1];
        }
        let len = hi - lo;
        let lv = (usize::BITS - 1 - len.leading_zeros()) as usize;
        let (a, b) = (self.levels[lv][lo], self.levels[lv][hi - (1 << lv)]);
        if less(&b, &a) { b } else { a }
    }
}

fn key_less(a: &(f64, usize), b: &(f64, usize)) -> bool {
    a.0 < b.0 || (a.0 == b.0 && a.1 < b.1)
}

/// overview._extend: стыковка префикса (рейсы p) с рейсами f — отрезки по прилёту.
fn extend(st: &St, prev: &Leg, p_idx: &[usize], nxt: &Leg, f_idx: &[usize], cf: Option<CityF>, hop: &[bool]) -> St {
    let (nd, np, nf) = (st.d, st.p, f_idx.len());
    let arr_ts: Vec<f64> = p_idx.iter().map(|&j| prev.arr_ts[j]).collect();
    let thr: Vec<i64> = p_idx
        .iter()
        .map(|&j| if cf.map_or(false, |c| c.weekend) { prev.arr_ord[j].max(prev.wok[j]) } else { prev.arr_ord[j] })
        .collect();
    let mut order: Vec<usize> = (0..np).collect();
    order.sort_by(|&a, &b| arr_ts[a].total_cmp(&arr_ts[b]).then(a.cmp(&b)));
    let ts_s: Vec<f64> = order.iter().map(|&k| arr_ts[k]).collect();
    let thr_s: Vec<i64> = order.iter().map(|&k| thr[k]).collect();
    let p_ok_s: Vec<bool> = order
        .iter()
        .map(|&k| cf.and_then(|c| c.cover).map_or(true, |(c0, _)| prev.arr_ord[p_idx[k]] <= c0))
        .collect();
    let hop_any = hop.iter().any(|&h| h);
    let prefix = cf.map_or(true, |c| c.max_stay.is_none());
    let mut lo = vec![0usize; nf];
    let mut hi = vec![0usize; nf];
    let mut f_ok = vec![false; nf];
    for f in 0..nf {
        let (dord, dts) = (nxt.dep_ord[f_idx[f]], nxt.dep_ts[f_idx[f]]);
        let mut h = thr_s.partition_point(|&x| x <= dord);
        let mut gap = f64::NEG_INFINITY;
        if hop_any && hop[f] {
            gap = HOP_GAP;
        }
        if let Some(c) = cf {
            gap = gap.max(c.min_stay as f64 * DAY);
        }
        let lim = dts - gap;
        h = h.min(ts_s.partition_point(|&x| x <= lim));
        let l = match cf.and_then(|c| c.max_stay) {
            Some(m) if !prefix => {
                let lim_lo = dts - (m + 1) as f64 * DAY;
                ts_s.partition_point(|&x| x <= lim_lo)
            }
            _ => 0,
        };
        lo[f] = l;
        hi[f] = h;
        f_ok[f] = h > l && cf.and_then(|c| c.cover).map_or(true, |(_, c1)| dord >= c1);
    }
    let mut out = St {
        d: nd,
        p: nf,
        cnt: vec![0.0; nd * nf],
        minp: vec![INF; nd * nf],
        tr_at: vec![0; nd * nf],
        mintr: vec![BIG_TR; nd * nf],
    };
    for d in 0..nd {
        let row = d * np;
        let mut csum = vec![0.0f64; np + 1];
        for k in 0..np {
            csum[k + 1] = csum[k] + if p_ok_s[k] { st.cnt[row + order[k]] } else { 0.0 };
        }
        let keys: Vec<(f64, usize)> = (0..np)
            .map(|k| (if p_ok_s[k] { st.minp[row + order[k]] } else { INF }, order[k]))
            .collect();
        let trs: Vec<i64> = (0..np).map(|k| if p_ok_s[k] { st.mintr[row + order[k]] } else { BIG_TR }).collect();
        let rk = RangeMin::new(keys, prefix, key_less);
        let rt = RangeMin::new(trs, prefix, |a: &i64, b: &i64| a < b);
        for f in 0..nf {
            let o = d * nf + f;
            let mut arg = 0usize;
            if f_ok[f] {
                out.cnt[o] = csum[hi[f]] - csum[lo[f]];
                let best = rk.query(lo[f], hi[f], prefix, key_less);
                if best.0 < INF {
                    arg = best.1;
                    out.minp[o] = st.minp[row + arg];
                }
                out.mintr[o] = rt.query(lo[f], hi[f], prefix, |a: &i64, b: &i64| a < b);
            }
            let fr = f_idx[f];
            out.minp[o] += nxt.price[fr];
            out.tr_at[o] = st.tr_at[row + arg] + nxt.transfers[fr];
            out.mintr[o] += nxt.transfers[fr];
        }
    }
    out
}

struct Overview<'a> {
    legs: Vec<Leg>,
    last: usize,
    allow: Vec<Option<HashSet<i32>>>,
    cf: Vec<Option<CityF>>,
    trip: Option<(i64, i64)>,
    budget: Option<f64>,
    combos: Vec<(Vec<i32>, f64, i64, i64, f64)>,
    _p: std::marker::PhantomData<&'a ()>,
}

impl<'a> Overview<'a> {
    fn groups(&self, k: usize, f_all: &[usize], seq: &[i32]) -> Vec<(i32, Vec<usize>)> {
        let leg = &self.legs[k];
        let allow = &self.allow[k + 1];
        let city = *seq.last().unwrap();
        let mut pos: HashMap<i32, usize> = HashMap::new();
        let mut out: Vec<(i32, Vec<usize>)> = Vec::new();
        for &fi in f_all {
            let dest = leg.dest[fi];
            if dest < 0 || dest == city || dest == leg.oc[fi] || dest == leg.oa[fi] {
                continue;
            }
            if allow.is_none() && seq.contains(&dest) {
                continue;
            }
            if !allowed(allow, dest, leg.da[fi]) {
                continue;
            }
            match pos.get(&dest) {
                Some(&p) => out[p].1.push(fi),
                None => {
                    pos.insert(dest, out.len());
                    out.push((dest, vec![fi]));
                }
            }
        }
        out
    }

    fn finish(&mut self, seq: Vec<i32>, k: usize, f_idx: &[usize], st: &St, first_days: &[i64]) {
        let leg = &self.legs[k];
        let nf = f_idx.len();
        let mut total = 0.0f64;
        let mut best = INF;
        let mut best_at = usize::MAX;
        let mut mintr_min = i64::MAX;
        for d in 0..st.d {
            for f in 0..nf {
                let o = d * nf + f;
                let (mut cnt, mut minp, mut mintr) = (st.cnt[o], st.minp[o], st.mintr[o]);
                if let Some((lo, hi)) = self.trip {
                    let days = (leg.arr_ord[f_idx[f]] - first_days[d]).max(1);
                    if !(days >= lo && (hi < 0 || days <= hi)) {
                        cnt = 0.0;
                        minp = INF;
                        mintr = BIG_TR;
                    }
                }
                if let Some(b) = self.budget {
                    if !(minp <= b) {
                        cnt = 0.0;
                        minp = INF;
                        mintr = BIG_TR;
                    }
                }
                total += cnt;
                if best_at == usize::MAX || minp < best {
                    best = minp;
                    best_at = o;
                }
                mintr_min = mintr_min.min(mintr);
            }
        }
        if total <= 0.0 {
            return;
        }
        self.combos.push((seq, best, st.tr_at[best_at], mintr_min, total));
    }

    fn expand(&mut self, k: usize, seq: &[i32], p_idx: &[usize], st: &St, first_days: &[i64]) {
        let city = *seq.last().unwrap();
        let Some(f_all) = self.legs[k].by_origin.get(&city).cloned() else { return };
        for (dest, fis) in self.groups(k, &f_all, seq) {
            let leg = &self.legs[k];
            let hop: Vec<bool> = fis.iter().map(|&fi| city != leg.oc[fi] && city != leg.oa[fi]).collect();
            let ns = extend(st, &self.legs[k - 1], p_idx, leg, &fis, self.cf[k], &hop);
            if !ns.cnt.iter().any(|&x| x > 0.0) {
                continue;
            }
            let mut new_seq = seq.to_vec();
            new_seq.push(dest);
            if k + 1 == self.last {
                self.finish(new_seq, k, &fis, &ns, first_days);
            } else {
                self.expand(k + 1, &new_seq, &fis, &ns, first_days);
            }
        }
    }
}

/// Наборы городов: [(коды, minPrice, transfersAtMin, minTransfers, count)] в порядке
/// обхода (сортировку и имена делает Python).
#[pyfunction]
#[pyo3(signature = (stop_any, stop_codes, leg_rows, oc, oa, dest, da, price, transfers, dep_ts, arr_ts,
                    dep_ord, arr_ord, city_filters, trip_length, budget, start_ord))]
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
fn overview<'py>(
    stop_any: Vec<bool>,
    stop_codes: Vec<Vec<i32>>,
    leg_rows: Vec<PyReadonlyArray1<'py, i64>>,
    oc: PyReadonlyArray1<'py, i32>,
    oa: PyReadonlyArray1<'py, i32>,
    dest: PyReadonlyArray1<'py, i32>,
    da: PyReadonlyArray1<'py, i32>,
    price: PyReadonlyArray1<'py, f64>,
    transfers: PyReadonlyArray1<'py, i32>,
    dep_ts: PyReadonlyArray1<'py, f64>,
    arr_ts: PyReadonlyArray1<'py, f64>,
    dep_ord: PyReadonlyArray1<'py, i64>,
    arr_ord: PyReadonlyArray1<'py, i64>,
    city_filters: Vec<Option<(i64, i64, i64, i64, bool)>>,
    trip_length: Option<(i64, i64)>,
    budget: Option<f64>,
    start_ord: i64,
) -> PyResult<Vec<(Vec<i32>, f64, i64, i64, f64)>> {
    let c = Cols {
        oc: oc.as_slice()?,
        oa: oa.as_slice()?,
        dest: dest.as_slice()?,
        da: da.as_slice()?,
        price: price.as_slice()?,
        transfers: transfers.as_slice()?,
        dep_ts: dep_ts.as_slice()?,
        arr_ts: arr_ts.as_slice()?,
        dep_ord: dep_ord.as_slice()?,
        arr_ord: arr_ord.as_slice()?,
    };
    let last = stop_any.len() - 1;
    let rows: Vec<&[i64]> = leg_rows.iter().map(|r| r.as_slice()).collect::<Result<_, _>>()?;
    let mut ov = Overview {
        legs: rows.iter().map(|r| Leg::new(&c, r, budget)).collect(),
        last,
        allow: parse_allow(&stop_any, &stop_codes),
        cf: parse_filters(city_filters),
        trip: trip_length,
        budget,
        combos: Vec::new(),
        _p: std::marker::PhantomData,
    };
    let mut starts: Vec<i32> = Vec::new();
    for &code in &stop_codes[0] {
        if !starts.contains(&code) {
            starts.push(code);
        }
    }
    for start in starts {
        let Some(f_all) = ov.legs[0].by_origin.get(&start).cloned() else { continue };
        for (dest, fis) in ov.groups(0, &f_all, &[start]) {
            let leg0 = &ov.legs[0];
            let ok: Vec<bool> = fis.iter().map(|&fi| leg0.dep_ord[fi] >= start_ord).collect();
            let first_days: Vec<i64> = if ov.trip.is_some() {
                let mut v: Vec<i64> = fis.iter().map(|&fi| leg0.dep_ord[fi]).collect();
                v.sort_unstable();
                v.dedup();
                v
            } else {
                vec![0]
            };
            let (nd, np) = (first_days.len(), fis.len());
            let mut st = St { d: nd, p: np, cnt: vec![0.0; nd * np], minp: vec![INF; nd * np],
                              tr_at: vec![0; nd * np], mintr: vec![BIG_TR; nd * np] };
            for d in 0..nd {
                for (j, &fi) in fis.iter().enumerate() {
                    let o = d * np + j;
                    let hit = if ov.trip.is_some() { leg0.dep_ord[fi] == first_days[d] && ok[j] } else { ok[j] };
                    st.cnt[o] = if hit { 1.0 } else { 0.0 };
                    st.tr_at[o] = leg0.transfers[fi];
                    if hit {
                        st.minp[o] = leg0.price[fi];
                        st.mintr[o] = leg0.transfers[fi];
                    }
                }
            }
            let seq = vec![start, dest];
            if last == 1 {
                ov.finish(seq, 0, &fis, &st, &first_days);
            } else {
                ov.expand(1, &seq, &fis, &st, &first_days);
            }
        }
    }
    Ok(ov.combos)
}

#[pymodule]
fn planner_engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(search, m)?)?;
    m.add_function(wrap_pyfunction!(overview, m)?)?;
    Ok(())
}
