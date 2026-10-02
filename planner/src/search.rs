//! Стыковка цепочек A → B → C → … (зеркало `core.planner`: _completion_lb,
//! _search_cheapest, _pack_compact, materialize, build_combo_routes).
//!
//! best-first (A*) выдаёт цепочки по возрастанию цены и останавливается на N.
//! Эвристика h = минимальная цена «хвоста» (`completion_lb`) — admissible и consistent,
//! поэтому первые N извлечённых = N самых дешёвых. Раскрытие ленивое: онворды города
//! на плече i заранее отсортированы по price + h (список общий для всех узлов —
//! `Candidates`), и в куче лежит только лучший ещё не выданный ребёнок узла.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
#[cfg(test)]
use std::collections::HashSet;
use std::rc::Rc;
#[cfg(test)]
use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtOrd};

// ---------------------------- ИССЛЕДОВАНИЕ (не в прод) ----------------------------
/// Нижняя оценка по (город, день прилёта) вместо (город): переключатель прототипа.
pub static RESEARCH_DAY_LB: AtomicBool = AtomicBool::new(false);
/// Счётчики перебора: извлечения из кучи, положенные в кучу, кандидаты, отброшенные по дате,
/// узлы без единого валидного ребёнка, построенные списки кандидатов (записей).
pub static ST_POPS: AtomicUsize = AtomicUsize::new(0);
pub static ST_PUSHES: AtomicUsize = AtomicUsize::new(0);
pub static ST_SKIP_DATE: AtomicUsize = AtomicUsize::new(0);
pub static ST_DEAD_NODES: AtomicUsize = AtomicUsize::new(0);
pub static ST_CAND_ENTRIES: AtomicUsize = AtomicUsize::new(0);
pub static ST_NODES: AtomicUsize = AtomicUsize::new(0);

pub fn research_reset() {
    for c in [&ST_POPS, &ST_PUSHES, &ST_SKIP_DATE, &ST_DEAD_NODES, &ST_CAND_ENTRIES, &ST_NODES] {
        c.store(0, AtOrd::Relaxed);
    }
}

pub fn research_stats() -> serde_json::Value {
    json!({
        "pops": ST_POPS.load(AtOrd::Relaxed), "pushes": ST_PUSHES.load(AtOrd::Relaxed),
        "skip_date": ST_SKIP_DATE.load(AtOrd::Relaxed), "dead_nodes": ST_DEAD_NODES.load(AtOrd::Relaxed),
        "cand_entries": ST_CAND_ENTRIES.load(AtOrd::Relaxed), "nodes": ST_NODES.load(AtOrd::Relaxed),
    })
}

/// Оценка хвоста по (город, день прилёта): для остановки i и города c — список
/// (день вылета d, минимальная цена хвоста при вылете в день ≥ d), по убыванию d с
/// суффиксным минимумом. Запрос: минимум по вылетам в день > прилёт (с старта — ≥).
pub type DayLb = Vec<FxHashMap<u32, Vec<(i64, f64)>>>;

pub fn completion_lb_day(ctx: &Ctx) -> DayLb {
    let t = ctx.table;
    let last = ctx.last;
    let mut lb: DayLb = vec![FxHashMap::default(); ctx.stops.len()];
    for i in (0..last).rev() {
        let terminal_next = i + 1 == last;
        // (город вылета, день вылета) → лучшая цена рейс + хвост
        let mut best: FxHashMap<(u32, i64), f64> = FxHashMap::default();
        for (&city, rows) in &ctx.legs_by_origin[i] {
            for &r in rows {
                let dest = t.dest_id[r];
                if dest == NO_CODE || dest == city || t.dep_ord[r] < 0 || !ctx.dest_allowed(i + 1, r) {
                    continue;
                }
                let tail = if terminal_next { 0.0 } else { match day_lb_get(&lb, i + 1, dest, t.arr_ord[r], false) { Some(v) => v, None => continue } };
                let cand = t.price[r] + tail;
                let e = best.entry((city, t.dep_ord[r])).or_insert(f64::INFINITY);
                if cand < *e {
                    *e = cand;
                }
            }
        }
        let mut per_city: FxHashMap<u32, Vec<(i64, f64)>> = FxHashMap::default();
        for ((city, day), v) in best {
            per_city.entry(city).or_default().push((day, v));
        }
        for list in per_city.values_mut() {
            list.sort_by(|a, b| b.0.cmp(&a.0)); // по убыванию дня
            let mut run = f64::INFINITY;
            for item in list.iter_mut() {
                run = run.min(item.1);
                item.1 = run; // суффиксный минимум: лучший хвост при вылете в день ≥ item.0
            }
        }
        // прототип: переезды к соседям (radius) не учитываем — для многих ANY подряд радиус 0
        lb[i] = per_city;
    }
    lb
}

/// Минимум хвоста из города при прилёте в день arrive (вылет строго позже; со старта — в тот же день).
pub fn day_lb_get(lb: &DayLb, i: usize, city: u32, arrive: i64, first: bool) -> Option<f64> {
    let list = lb.get(i)?.get(&city)?;
    let min_dep = if first { arrive } else { arrive + 1 };
    // список по убыванию дня: первая запись с днём >= min_dep, идя с конца (меньшие дни) — берём
    // последнюю запись, у которой day >= min_dep
    let pos = list.partition_point(|(d, _)| *d >= min_dep);
    if pos == 0 { None } else { Some(list[pos - 1].1) }
}
// ---------------------------------------------------------------------------------

use serde_json::{json, Value};

use crate::dates::{date_ordinal, date_only, naive_seconds, parse_naive, shift_date, stay_days_secs, weekend_covered, ordinal};
use crate::flightcols::{FlightCols, NO_CODE};
use crate::nearby::{distance_km, Hops, HOP_MIN_GAP_MIN};
use crate::planquery::{trip_length_ok, CityFilter, PlanQuery};
use crate::segments::{city_pair, make_segment, Segment};
use crate::stops::{leg_dates, Stop, COMBO_MAX_RESULTS, FINAL_STAY_DAYS};

#[derive(Debug)]
pub struct Aborted;

/// Хук шага перебора: считает шаги, сообщает прогресс, прерывает перебор.
pub type StepCheck<'a> = dyn FnMut(usize) -> Result<(), Aborted> + 'a;

pub struct Ctx<'a> {
    pub stops: &'a [Stop],
    pub table: &'a FlightCols,
    /// Строки плеча по номеру кода вылета (город и аэропорт).
    pub legs_by_origin: Vec<FxHashMap<u32, Vec<usize>>>,
    pub chain_start: String,
    pub last: usize,
    pub hops: Hops<'a>,
    /// Разрешённые города прилёта остановки — маска по номерам кодов таблицы (None — любой).
    pub allow: Vec<Option<Vec<bool>>>,
    /// Коды вне рейсов (старты, соседи в радиусе) — номера после кодов таблицы.
    extra: RefCell<(Vec<String>, FxHashMap<String, u32>)>,
    departs: RefCell<FxHashMap<(usize, u32), Rc<Vec<u32>>>>,
}

impl<'a> Ctx<'a> {
    /// Номер кода: из словаря таблицы или (для кодов вне рейсов) дополнительный.
    pub fn code_id(&self, code: &str) -> u32 {
        if let Some(id) = self.table.code_id(code) {
            return id;
        }
        let mut extra = self.extra.borrow_mut();
        if let Some(&id) = extra.1.get(code) {
            return id;
        }
        let id = (self.table.codes.len() + extra.0.len()) as u32;
        extra.0.push(code.to_string());
        extra.1.insert(code.to_string(), id);
        id
    }

    pub fn code_name(&self, id: u32) -> String {
        let n = self.table.codes.len();
        if (id as usize) < n {
            self.table.codes[id as usize].clone()
        } else {
            self.extra.borrow().0[id as usize - n].clone()
        }
    }

    /// Hops::departs по номерам (коды без рейсов отброшены — из них не улететь).
    pub fn departs(&self, i: usize, city: u32) -> Rc<Vec<u32>> {
        if let Some(v) = self.departs.borrow().get(&(i, city)) {
            return v.clone();
        }
        let got: Rc<Vec<u32>> = Rc::new(self.hops.departs(i, &self.code_name(city)).iter().filter_map(|c| self.table.code_id(c)).collect());
        self.departs.borrow_mut().insert((i, city), got.clone());
        got
    }

    /// Город или аэропорт прилёта строки r разрешён остановке i.
    pub fn dest_allowed(&self, i: usize, r: usize) -> bool {
        match &self.allow[i] {
            None => true,
            Some(bits) => {
                let (d, a) = (self.table.dest_id[r], self.table.dest_airport_id[r]);
                (d != NO_CODE && bits[d as usize]) || (a != NO_CODE && bits[a as usize])
            }
        }
    }
}

/// Строки рейсов каждого плеча, прошедшие фильтр плеча (до перебора).
pub fn leg_rows(table: &FlightCols, legs: usize, query: Option<&PlanQuery>) -> Vec<Vec<usize>> {
    (0..legs)
        .map(|i| {
            let rows = table.rows(i);
            match query.and_then(|q| q.legs.get(i)) {
                Some(lf) if !lf.is_open() => rows
                    .into_iter()
                    .filter(|&r| lf.accepts(table.hidden[r], table.transfers[r], table.duration[r], table.pts_min[r], table.layover[r], table.bag_incl[r]))
                    .collect(),
                _ => rows,
            }
        })
        .collect()
}

pub fn build_ctx<'a>(stops: &'a [Stop], table: &'a FlightCols, query: Option<&PlanQuery>) -> Ctx<'a> {
    let legs = stops.len().saturating_sub(1);
    let rows = leg_rows(table, legs, query);
    let legs_by_origin = rows.iter().map(|r| table.by_origin_ids(r)).collect();
    let chain_start = leg_dates(stops, 0).into_iter().next().unwrap_or_else(|| crate::stops::DEFAULT_START.to_string());
    let hops = Hops::new(stops);
    let n_codes = table.codes.len();
    let allow = (0..stops.len())
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
    Ctx {
        stops,
        table,
        legs_by_origin,
        chain_start,
        last: legs,
        hops,
        allow,
        extra: RefCell::new((Vec::new(), FxHashMap::default())),
        departs: RefCell::new(FxHashMap::default()),
    }
}

/// Нижняя оценка стоимости «хвоста»: lb[i][город] — минимально возможная суммарная
/// цена, чтобы, прилетев в город на остановку i, добраться до финала (только цены,
/// разрешённые города и переезды — без времени и повторов; admissible). Ключи — номера кодов.
pub fn completion_lb(ctx: &Ctx) -> Vec<FxHashMap<u32, f64>> {
    let t = ctx.table;
    let last = ctx.last;
    let mut lb: Vec<FxHashMap<u32, f64>> = vec![FxHashMap::default(); ctx.stops.len()];
    for i in (0..last).rev() {
        let terminal_next = i + 1 == last;
        let mut dep: FxHashMap<u32, f64> = FxHashMap::default();
        for (&city, rows) in &ctx.legs_by_origin[i] {
            let mut best = f64::INFINITY;
            for &r in rows {
                let dest = t.dest_id[r];
                if dest == NO_CODE || dest == city {
                    continue;
                }
                if !ctx.dest_allowed(i + 1, r) {
                    continue;
                }
                let tail = if terminal_next {
                    0.0
                } else {
                    match lb[i + 1].get(&dest) {
                        Some(v) => *v,
                        None => continue,
                    }
                };
                let cand = t.price[r] + tail;
                if cand < best {
                    best = cand;
                }
            }
            if best < f64::INFINITY {
                dep.insert(city, best);
            }
        }
        if ctx.hops.radius[i] <= 0.0 {
            lb[i] = dep;
            continue;
        }
        let mut arrivals: FxHashSet<u32> = dep.keys().copied().collect();
        for &d in dep.keys() {
            for n in crate::nearby::neighbors(&ctx.code_name(d), ctx.hops.radius[i]) {
                arrivals.insert(ctx.code_id(&n));
            }
        }
        let mut cur = FxHashMap::default();
        for a in arrivals {
            let best = ctx.departs(i, a).iter().filter_map(|d| dep.get(d)).cloned().fold(f64::INFINITY, f64::min);
            if best < f64::INFINITY {
                cur.insert(a, best);
            }
        }
        lb[i] = cur;
    }
    lb
}

/// Онворд-рейсы (плечо i, город прилёта), отсортированные по price + lb хвоста.
struct CandList {
    keys: Vec<f64>,
    idxs: Vec<usize>,
    hop: Vec<bool>,
}

struct Candidates<'c, 'a> {
    ctx: &'c Ctx<'a>,
    lb: &'c [FxHashMap<u32, f64>],
    day_lb: Option<&'c DayLb>,
    cache: FxHashMap<(usize, u32), Rc<CandList>>,
}

impl<'c, 'a> Candidates<'c, 'a> {
    fn get(&mut self, i: usize, city: u32) -> Rc<CandList> {
        if let Some(got) = self.cache.get(&(i, city)) {
            return got.clone();
        }
        let built = Rc::new(self.build(i, city));
        self.cache.insert((i, city), built.clone());
        built
    }

    fn build(&self, i: usize, city: u32) -> CandList {
        let ctx = self.ctx;
        let t = ctx.table;
        let terminal = i + 1 == ctx.last;
        let mut pairs: Vec<(f64, usize, bool)> = Vec::new();
        let departs = ctx.departs(i, city);
        let mut seen: FxHashSet<usize> = FxHashSet::default();
        for &d in departs.iter() {
            let Some(rows) = ctx.legs_by_origin[i].get(&d) else { continue };
            for &fi in rows {
                if departs.len() > 1 && !seen.insert(fi) {
                    continue;
                }
                let dest = t.dest_id[fi];
                if dest == NO_CODE || dest == city || dest == d || t.dep_ord[fi] < 0 {
                    continue;
                }
                if !ctx.dest_allowed(i + 1, fi) {
                    continue;
                }
                let tail = if terminal {
                    0.0
                } else if let Some(dl) = self.day_lb {
                    match day_lb_get(dl, i + 1, dest, t.arr_ord[fi], false) {
                        Some(v) => v,
                        None => continue,
                    }
                } else {
                    match self.lb[i + 1].get(&dest) {
                        Some(v) => *v,
                        None => continue,
                    }
                };
                pairs.push((t.price[fi] + tail, fi, city != t.orig_city_id[fi] && city != t.orig_airport_id[fi]));
            }
        }
        ST_CAND_ENTRIES.fetch_add(pairs.len(), AtOrd::Relaxed);
        pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal).then(a.1.cmp(&b.1)));
        CandList { keys: pairs.iter().map(|p| p.0).collect(), idxs: pairs.iter().map(|p| p.1).collect(), hop: pairs.iter().map(|p| p.2).collect() }
    }
}

struct Node {
    i: usize,
    city: u32,
    arrive_ord: i64,
    arrive_ts: f64,
    g: f64,
    parent: Option<(usize, usize)>, // (узел, рейс)
    /// Кандидаты (плечо i, город) — чтобы не искать их в кэше на каждом шаге.
    list: Rc<CandList>,
}

struct HeapItem {
    f: f64,
    seq: u64,
    node: usize,
    k: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, o: &Self) -> bool {
        self.seq == o.seq
    }
}
impl Eq for HeapItem {}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for HeapItem {
    fn cmp(&self, o: &Self) -> Ordering {
        // BinaryHeap — max-heap: меньший f (и seq) должен быть «больше»
        o.f.partial_cmp(&self.f).unwrap_or(Ordering::Equal).then(o.seq.cmp(&self.seq))
    }
}

/// Первый день, когда можно вылетать с остановки i, прилетев в неё в день arrive_ord:
/// со старта (i == 0, прилёт виртуальный — начало окна) — тот же день, дальше — строго
/// следующий календарный день (вылет в день прилёта не стыкуется, даже позже по времени).
pub fn min_depart_ord(i: usize, arrive_ord: i64) -> i64 {
    if i == 0 { arrive_ord } else { arrive_ord + 1 }
}

/// Хватает ли времени на переезд в соседний город.
fn gap_ok(arrive_ts: f64, depart_ts: f64) -> bool {
    depart_ts - arrive_ts >= (HOP_MIN_GAP_MIN * 60) as f64
}

/// Фильтр пребывания в промежуточном городе: дни между прилётом и вылетом,
/// обязательное окно, выходные. cover — окно «покрыть даты» номерами дней.
fn stay_ok(cf: &CityFilter, cover: Option<Option<(i64, i64)>>, arrive_ts: f64, arrive_ord: i64, depart_ts: f64, depart_ord: i64) -> bool {
    let days = stay_days_secs(arrive_ts, depart_ts);
    if days < cf.min_stay {
        return false;
    }
    if let Some(m) = cf.max_stay {
        if days > m {
            return false;
        }
    }
    if let Some(cover) = cover {
        let Some((a, b)) = cover else { return false };
        if !(arrive_ord <= a && depart_ord >= b) {
            return false;
        }
    }
    if cf.require_weekend && !weekend_covered(arrive_ts, arrive_ord, depart_ts, depart_ord) {
        return false;
    }
    true
}

/// Города старта цепочек: города первой остановки, а у «любой» первой остановки — все
/// города вылета рейсов первого плеча (иначе стартов нет и маршрутов ноль).
pub fn start_codes(stops: &[Stop], table: &FlightCols) -> Vec<String> {
    if stops.first().map(|s| s.is_cities() && !s.codes.is_empty()).unwrap_or(false) {
        return stops[0].codes.clone();
    }
    let mut ids: Vec<u32> = table.rows(0).into_iter().map(|r| table.orig_city_id[r]).filter(|&id| id != crate::flightcols::NO_CODE).collect();
    ids.sort_unstable();
    ids.dedup();
    let mut out: Vec<String> = ids.into_iter().map(|id| table.code(id).to_string()).filter(|c| !c.is_empty()).collect();
    out.sort();
    out
}

/// Цепочки (индексы рейсов в table) по возрастанию цены, не больше max_results.
pub fn search_cheapest(ctx: &Ctx, max_results: usize, query: Option<&PlanQuery>, check: &mut StepCheck) -> Result<Vec<Vec<usize>>, Aborted> {
    let t = ctx.table;
    let last = ctx.last;
    let lb = completion_lb(ctx);
    let day_lb_store = if RESEARCH_DAY_LB.load(AtOrd::Relaxed) { Some(completion_lb_day(ctx)) } else { None };
    let mut cands = Candidates { ctx, lb: &lb, day_lb: day_lb_store.as_ref(), cache: FxHashMap::default() };
    let mut heap: BinaryHeap<HeapItem> = BinaryHeap::new();
    let mut nodes: Vec<Node> = Vec::new();
    let mut seq: u64 = 0;
    // фильтры городов и их окна «покрыть даты» (номера дней) — по номеру остановки
    let city_filters: Vec<Option<(&CityFilter, Option<Option<(i64, i64)>>)>> = (0..ctx.stops.len())
        .map(|i| {
            if i == 0 || i >= last {
                return None;
            }
            query.and_then(|q| q.city_filter(i)).map(|cf| {
                let cover = cf.must_cover.as_ref().map(|c| match (date_ordinal(&c[0]), date_ordinal(&c[1])) {
                    (Some(a), Some(b)) => Some((a, b)),
                    _ => None,
                });
                (cf, cover)
            })
        })
        .collect();
    let any_next: Vec<bool> = (0..ctx.stops.len()).map(|i| i + 1 < ctx.stops.len() && ctx.stops[i + 1].kind == "any").collect();
    let trip = query.map(|q| q.trip_length).filter(|tl| tl.0 != 0 || tl.1.is_some());

    let start_dt = parse_naive(&format!("{}T00:00:00", ctx.chain_start)).expect("chain_start");
    let start_ord = ordinal(start_dt.date());
    let start_ts = naive_seconds(start_dt);

    fn visited(nodes: &[Node], mut idx: usize, dest: u32) -> bool {
        loop {
            if nodes[idx].city == dest {
                return true;
            }
            match nodes[idx].parent {
                Some((p, _)) => idx = p,
                None => return false,
            }
        }
    }

    // Кладёт в кучу первого валидного ребёнка узла, начиная с позиции start.
    let push_next = |node_idx: usize, start: usize, nodes: &Vec<Node>, heap: &mut BinaryHeap<HeapItem>, seq: &mut u64| {
        let node = &nodes[node_idx];
        let i = node.i;
        let list = &node.list;
        let cf = city_filters[i];
        for k in start..list.idxs.len() {
            let f = node.g + list.keys[k];
            let fi = list.idxs[k];
            if t.dep_ord[fi] < min_depart_ord(i, node.arrive_ord) {
                ST_SKIP_DATE.fetch_add(1, AtOrd::Relaxed);
                continue;
            }
            if list.hop[k] && i > 0 && !gap_ok(node.arrive_ts, t.dep_ts[fi]) {
                continue;
            }
            if any_next[i] && visited(nodes, node_idx, t.dest_id[fi]) {
                continue;
            }
            if let Some((cf, cover)) = cf {
                if !stay_ok(cf, cover, node.arrive_ts, node.arrive_ord, t.dep_ts[fi], t.dep_ord[fi]) {
                    continue;
                }
            }
            heap.push(HeapItem { f, seq: *seq, node: node_idx, k });
            *seq += 1;
            ST_PUSHES.fetch_add(1, AtOrd::Relaxed);
            return;
        }
        if start == 0 {
            ST_DEAD_NODES.fetch_add(1, AtOrd::Relaxed);
        }
    };

    for start in &start_codes(ctx.stops, t) {
        let sid = ctx.code_id(start);
        if !lb[0].contains_key(&sid) {
            continue;
        }
        if let Some(dl) = &day_lb_store {
            if day_lb_get(dl, 0, sid, start_ord, true).is_none() {
                continue;
            }
        }
        let list = cands.get(0, sid);
        nodes.push(Node { i: 0, city: sid, arrive_ord: start_ord, arrive_ts: start_ts, g: 0.0, parent: None, list });
        let idx = nodes.len() - 1;
        push_next(idx, 0, &nodes, &mut heap, &mut seq);
    }

    let mut out: Vec<Vec<usize>> = Vec::new();
    loop {
        if out.len() >= max_results {
            break;
        }
        let Some(item) = heap.pop() else { break };
        ST_POPS.fetch_add(1, AtOrd::Relaxed);
        check(out.len())?;
        push_next(item.node, item.k + 1, &nodes, &mut heap, &mut seq);
        let i = nodes[item.node].i;
        let fi = nodes[item.node].list.idxs[item.k];
        if i + 1 == last {
            let mut chain = vec![fi];
            let mut cur = item.node;
            while let Some((p, pf)) = nodes[cur].parent {
                chain.push(pf);
                cur = p;
            }
            chain.reverse();
            if let Some(tl) = trip {
                let days = (t.arr_ord[*chain.last().unwrap()] - t.dep_ord[chain[0]]).max(1);
                if !trip_length_ok(days, tl) {
                    continue;
                }
            }
            out.push(chain);
            continue;
        }
        let dest = t.dest_id[fi];
        let list = cands.get(i + 1, dest);
        ST_NODES.fetch_add(1, AtOrd::Relaxed);
        nodes.push(Node { i: i + 1, city: dest, arrive_ord: t.arr_ord[fi], arrive_ts: t.arr_ts[fi], g: nodes[item.node].g + t.price[fi], parent: Some((item.node, fi)), list });
        let idx = nodes.len() - 1;
        push_next(idx, 0, &nodes, &mut heap, &mut seq);
    }
    Ok(out)
}

// ---------------------------- компактный результат ---------------------------

/// Компактный результат стыковки (контракт compact-v1): уникальные сегменты один раз,
/// цепочки — индексы сегментов, дни в городах, маска выходных, длина поездки.
pub struct View {
    pub count: usize,
    pub legs: usize,
    pub chain_start: String,
    pub final_stay_days: i64,
    pub any_stops: Vec<bool>,
    pub cities: HashMap<String, (String, String)>,
    pub segments: Vec<Segment>,
    pub chains: Vec<u32>,
    pub days: Vec<i16>,
    pub weekend: Vec<u32>,
    pub total_days: Vec<i16>,
}

#[cfg(test)]
fn stay_of(arrive: &str, depart: &str) -> (i64, bool) {
    (crate::dates::stay_between(arrive, depart), crate::dates::has_both_weekend_days(arrive, depart))
}

/// Дни в городах и маска выходных по остановкам (по строкам сегментов — эталон для
/// chain_stays_num в тестах).
#[cfg(test)]
fn chain_stays(segs: &[&Segment], start_iso: &str) -> (Vec<i16>, u32) {
    let mut days = Vec::with_capacity(segs.len() + 1);
    let mut mask = 0u32;
    let mut arrive = start_iso.to_string();
    for k in 0..=segs.len() {
        let (d, wk) = if k < segs.len() {
            stay_of(&arrive, segs[k].departure_at.as_deref().unwrap_or(""))
        } else {
            let depart = format!("{}T00:00:00", shift_date(&date_only(&arrive), FINAL_STAY_DAYS));
            let (d, wk) = stay_of(&arrive, &depart);
            (d.max(1), wk)
        };
        days.push(d.max(0) as i16);
        mask |= (wk as u32) << k;
        if k < segs.len() {
            arrive = segs[k].arrival_at.clone().unwrap_or_default();
        }
    }
    (days, mask)
}

/// Длина поездки — от даты первого вылета до даты последнего прилёта (эталон).
#[cfg(test)]
fn trip_days(segs: &[&Segment]) -> i64 {
    if segs.is_empty() {
        return 1;
    }
    let first = date_only(segs[0].departure_at.as_deref().unwrap_or(""));
    let last = date_only(segs[segs.len() - 1].arrival_at.as_deref().unwrap_or(""));
    crate::dates::stay_between(&first, &last).max(1)
}

/// Номер дня 1970-01-01 (dates::ordinal): полночь дня ord — (ord − EPOCH_ORD) суток.
const EPOCH_ORD: i64 = 719_163;

fn midnight_secs(ord: i64) -> f64 {
    (ord - EPOCH_ORD) as f64 * DAY_SECS
}

const DAY_SECS: f64 = 86_400.0;

/// chain_stays по числовым колонкам рейсов цепочки (без разбора дат-строк): дни в
/// городах и маска «оба выходных» — прилёт в первый город в начале окна (T00:00),
/// последний город — FINAL_STAY_DAYS от даты прилёта.
fn chain_stays_num(t: &FlightCols, chain: &[usize], start_ord: i64) -> (Vec<i16>, u32) {
    let mut days = Vec::with_capacity(chain.len() + 1);
    let mut mask = 0u32;
    let (mut a_ts, mut a_ord) = (midnight_secs(start_ord), start_ord);
    for k in 0..=chain.len() {
        let (d_ts, d_ord) = if k < chain.len() {
            (t.dep_ts[chain[k]], t.dep_ord[chain[k]])
        } else {
            (midnight_secs(a_ord + FINAL_STAY_DAYS), a_ord + FINAL_STAY_DAYS)
        };
        let (mut d, wk) = if a_ts.is_nan() || d_ts.is_nan() {
            (0, false) // дата не разобралась — как у разбора строк
        } else {
            (stay_days_secs(a_ts, d_ts), weekend_covered(a_ts, a_ord, d_ts, d_ord))
        };
        if k == chain.len() {
            d = d.max(1);
        }
        days.push(d.max(0) as i16);
        mask |= (wk as u32) << k;
        if k < chain.len() {
            a_ts = t.arr_ts[chain[k]];
            a_ord = t.arr_ord[chain[k]];
        }
    }
    (days, mask)
}

/// trip_days по номерам дней: от даты первого вылета до даты последнего прилёта.
fn trip_days_num(t: &FlightCols, chain: &[usize]) -> i64 {
    match (chain.first(), chain.last()) {
        (Some(&first), Some(&last)) => (t.arr_ord[last] - t.dep_ord[first]).max(1),
        _ => 1,
    }
}

pub fn pack_compact(ctx: &Ctx, chains: &[Vec<usize>]) -> View {
    let legs = ctx.last;
    let mut seg_of: HashMap<usize, u32> = HashMap::new();
    let mut segments: Vec<Segment> = Vec::new();
    let mut out_chains: Vec<u32> = Vec::with_capacity(chains.len() * legs);
    // рейсы цепочек — одним проходом по файлу, а не всей колонкой
    let used: Vec<usize> = chains.iter().flatten().copied().collect();
    ctx.table.prefetch(&used);
    for chain in chains {
        for &fi in chain {
            let si = match seg_of.get(&fi) {
                Some(s) => *s,
                None => {
                    let s = segments.len() as u32;
                    segments.push(make_segment(&ctx.table.flight(fi)));
                    seg_of.insert(fi, s);
                    s
                }
            };
            out_chains.push(si);
        }
    }
    let start_iso = format!("{}T00:00:00", ctx.chain_start);
    let count = if legs > 0 { out_chains.len() / legs } else { 0 };
    let (mut days, mut weekend, mut total_days) = (Vec::with_capacity(count * (legs + 1)), Vec::with_capacity(count), Vec::with_capacity(count));
    let start_ord = date_ordinal(&ctx.chain_start).unwrap_or(0);
    for chain in chains.iter().take(count) {
        let (d, mask) = chain_stays_num(ctx.table, chain, start_ord);
        days.extend(d);
        weekend.push(mask);
        total_days.push(trip_days_num(ctx.table, chain) as i16);
    }
    let mut cities = HashMap::new();
    for s in &segments {
        for code in [&s.origin, &s.destination] {
            if let Some(c) = code.as_deref().filter(|c| !c.is_empty()) {
                cities.entry(c.to_string()).or_insert_with(|| city_pair(c));
            }
        }
    }
    View {
        count,
        legs,
        chain_start: start_iso,
        final_stay_days: FINAL_STAY_DAYS,
        any_stops: ctx.stops.iter().map(|s| s.kind == "any").collect(),
        cities,
        segments,
        chains: out_chains,
        days,
        weekend,
        total_days,
    }
}

/// Стыковка под фильтры query: компактный результат N самых дешёвых цепочек.
pub fn build_itineraries_compact(stops: &[Stop], table: &FlightCols, max_results: usize, query: Option<&PlanQuery>, check: &mut StepCheck) -> Result<View, Aborted> {
    let ctx = build_ctx(stops, table, query);
    let chains = search_cheapest(&ctx, max_results, query, check)?;
    Ok(pack_compact(&ctx, &chains))
}

fn depart_from(code: &str, seg: Option<&Segment>, names: &dyn Fn(&str) -> (String, String)) -> Option<Value> {
    let seg = seg?;
    let origin = seg.origin.as_deref().unwrap_or("").to_uppercase();
    if origin.is_empty() || origin == code.to_uppercase() {
        return None;
    }
    let km = distance_km(code, &origin);
    let (city, flag) = names(&origin);
    Some(json!({"code": origin, "city": city, "flag": flag, "km": km.map(|k| k.round() as i64)}))
}

impl View {
    /// Коды городов цепочки n: старт + прилёты всех плеч.
    pub fn chain_codes(&self, n: usize) -> Vec<String> {
        let base = n * self.legs;
        let mut out = vec![self.segments[self.chains[base] as usize].origin.clone().unwrap_or_default()];
        for k in 0..self.legs {
            out.push(self.segments[self.chains[base + k] as usize].destination.clone().unwrap_or_default());
        }
        out
    }

    /// Itinerary цепочки n: остановки с прилётом/вылетом/днями/выходными, сегменты, суммы.
    pub fn materialize(&self, n: usize) -> Value {
        let legs = self.legs;
        let stop_count = legs + 1;
        let segments: Vec<&Segment> = (0..legs).map(|k| &self.segments[self.chains[n * legs + k] as usize]).collect();
        let names = |c: &str| self.cities.get(c).cloned().unwrap_or_else(|| (c.to_string(), String::new()));
        let mut stops = Vec::with_capacity(stop_count);
        for k in 0..stop_count {
            let code = if k == 0 { segments[0].origin.clone() } else { segments[k - 1].destination.clone() }.unwrap_or_default();
            let arrive = if k == 0 { self.chain_start.clone() } else { segments[k - 1].arrival_at.clone().unwrap_or_default() };
            let depart = if k < legs {
                segments[k].departure_at.clone().unwrap_or_default()
            } else {
                format!("{}T00:00:00", shift_date(&date_only(&arrive), self.final_stay_days))
            };
            let (city, flag) = names(&code);
            let mut stop = json!({
                "code": code,
                "city": city,
                "flag": flag,
                "arrive": arrive,
                "depart": depart,
                "days": self.days[n * stop_count + k],
                "weekendCovered": (self.weekend[n] >> k) & 1 == 1,
                "resolvedFromAny": self.any_stops.get(k).copied().unwrap_or(false),
            });
            if k > 0 && k < legs {
                if let Some(df) = depart_from(&code, Some(segments[k]), &names) {
                    stop["departFrom"] = df;
                }
            }
            stops.push(stop);
        }
        json!({
            "id": n + 1,
            "stops": stops,
            "segments": segments,
            "total_price": segments.iter().map(|s| s.price).sum::<f64>(),
            "total_days": self.total_days[n],
            "total_transfers": segments.iter().map(|s| s.transfers).sum::<i64>(),
            "travel_minutes": segments.iter().map(|s| s.duration.unwrap_or(0)).sum::<i64>(),
        })
    }

    /// Страница маршрутов по возрастанию цены.
    pub fn routes_page(&self, offset: usize, limit: usize) -> Value {
        let total = self.count;
        let end = total.min(offset + limit);
        let items: Vec<Value> = (offset.min(end)..end).map(|n| self.materialize(n)).collect();
        json!({"total": total, "offset": offset, "limit": limit, "items": items})
    }
}

pub fn combo_key(codes: &[String]) -> String {
    codes.join("-")
}

/// Маршруты выбранных наборов: компактные виды наборов + общий порядок по цене.
/// JSON маршрута собирается только для запрошенной страницы (page) — API отдаёт по
/// 50, а наборы дают тысячи маршрутов.
pub struct ComboRoutes {
    views: Vec<(String, View)>,
    /// (вид, номер цепочки) по возрастанию цены — как прежняя сортировка готовых JSON.
    order: Vec<(usize, usize)>,
}

impl ComboRoutes {
    pub fn len(&self) -> usize {
        self.order.len()
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    /// Itinerary позиций offset..offset+limit (id — позиция + 1, combo — ключ набора).
    pub fn page(&self, offset: usize, limit: usize) -> Vec<Value> {
        let end = self.order.len().min(offset.saturating_add(limit));
        (offset.min(end)..end)
            .map(|pos| {
                let (v, n) = self.order[pos];
                let (key, view) = &self.views[v];
                let mut it = view.materialize(n);
                it["combo"] = Value::String(key.clone());
                it["id"] = json!(pos + 1);
                it
            })
            .collect()
    }
}

/// Маршруты выбранных наборов городов: на каждый набор — тот же A* по сохранённым
/// рейсам, но остановки зафиксированы кодами набора.
pub fn build_combo_views(stops: &[Stop], table: &FlightCols, combos: &[Vec<String>], query: Option<&PlanQuery>) -> ComboRoutes {
    let mut views: Vec<(String, View)> = Vec::new();
    for codes in combos {
        if codes.len() != stops.len() {
            continue;
        }
        let fixed: Vec<Stop> = codes
            .iter()
            .zip(stops)
            .map(|(code, stop)| Stop { kind: "cities".into(), codes: vec![code.clone()], window: stop.window.clone(), radius_km: stop.radius_km, exact: true })
            .collect();
        let mut check = |_: usize| Ok(());
        let Ok(view) = build_itineraries_compact(&fixed, table, COMBO_MAX_RESULTS as usize, query, &mut check) else { continue };
        views.push((combo_key(codes), view));
    }
    // цена маршрута — сумма цен сегментов в порядке плеч (как total_price в materialize)
    let mut priced: Vec<(f64, usize, usize)> = Vec::new();
    for (v, (_, view)) in views.iter().enumerate() {
        for n in 0..view.count {
            let price: f64 = (0..view.legs).map(|k| view.segments[view.chains[n * view.legs + k] as usize].price).sum();
            priced.push((price, v, n));
        }
    }
    priced.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal)); // устойчивая, как прежде
    ComboRoutes { views, order: priced.into_iter().map(|(_, v, n)| (v, n)).collect() }
}

/// Все маршруты выбранных наборов готовыми Itinerary (по возрастанию цены, с combo и id).
pub fn build_combo_routes(stops: &[Stop], table: &FlightCols, combos: &[Vec<String>], query: Option<&PlanQuery>) -> Vec<Value> {
    let routes = build_combo_views(stops, table, combos, query);
    routes.page(0, routes.len())
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::ticket::Ticket;

    pub fn flight(origin: &str, dest: &str, day: u32, hour: u32, price: f64) -> Arc<Ticket> {
        Arc::new(Ticket {
            origin: Some(origin.into()),
            origin_airport: Some(origin.into()),
            destination: Some(dest.into()),
            destination_airport: Some(dest.into()),
            departure_at: Some(format!("2026-11-{day:02}T{hour:02}:00:00+03:00")),
            duration: Some(240),
            price: Some(price),
            transfers: (price as i64) % 2,
            transfer_points: Some(vec![]),
            ..Default::default()
        })
    }

    /// Простой ГПСЧ (детерминированные случаи без внешних крейтов).
    pub struct Rng(pub u64);
    impl Rng {
        pub fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        pub fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    const CITIES: [&str; 6] = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"];

    pub fn random_case(seed: u64) -> (Vec<Stop>, Vec<Vec<Arc<Ticket>>>) {
        let mut rng = Rng(seed * 7919 + 17);
        let stops = vec![
            Stop::new("cities", vec!["MOW"], ["", ""]),
            Stop::new("any", vec![], ["2026-11-01", "2026-11-06"]),
            Stop::new("cities", vec!["SEL", "TYO"], ["2026-11-03", "2026-11-12"]),
            Stop::new("any", vec![], ["2026-11-08", "2026-11-16"]),
            Stop::new("cities", vec!["MOW"], ["", ""]),
        ];
        let mut leg = |origins: &[&str], dests: &[&str], days: std::ops::Range<u32>| -> Vec<Arc<Ticket>> {
            (0..20)
                .map(|_| {
                    let o = origins[rng.below(origins.len() as u64) as usize];
                    let d = dests[rng.below(dests.len() as u64) as usize];
                    let day = days.start + rng.below((days.end - days.start) as u64) as u32;
                    flight(o, d, day, rng.below(24) as u32, (rng.below(60) + 1) as f64 * 100.0)
                })
                .collect()
        };
        let all_mow: Vec<&str> = CITIES.iter().copied().chain(["MOW"]).collect();
        let all_sel: Vec<&str> = CITIES.iter().copied().chain(["SEL"]).collect();
        let collected = vec![
            leg(&["MOW"], &CITIES, 1..7),
            leg(&CITIES, &["SEL", "TYO"], 3..13),
            leg(&["SEL", "TYO"], &all_mow, 8..17),
            leg(&all_sel, &["MOW"], 8..20),
        ];
        (stops, collected)
    }

    /// Полный перебор-эталон: все цепочки по правилам онвордов (без фильтров).
    pub fn brute_force(stops: &[Stop], table: &FlightCols) -> Vec<(f64, Vec<usize>)> {
        let ctx = build_ctx(stops, table, None);
        let t = table;
        let mut out = Vec::new();
        fn dfs(ctx: &Ctx, t: &FlightCols, i: usize, city: &str, arr_ord: i64, arr_ts: f64, chosen: &mut Vec<usize>, visited: &mut Vec<String>, g: f64, out: &mut Vec<(f64, Vec<usize>)>) {
            if i == ctx.last {
                out.push((g, chosen.clone()));
                return;
            }
            let allow_next = ctx.hops.arrive_allowed(i + 1);
            let mut seen = HashSet::new();
            for d in ctx.hops.departs(i, city) {
                let rows: &[usize] = t.code_id(&d).and_then(|id| ctx.legs_by_origin[i].get(&id)).map(|v| v.as_slice()).unwrap_or(&[]);
                for &fi in rows {
                    if !seen.insert(fi) {
                        continue;
                    }
                    if t.dep_ord[fi] < 0 || t.dep_ord[fi] < min_depart_ord(i, arr_ord) {
                        continue;
                    }
                    let (oc, oa) = t.origin_codes(fi);
                    if i > 0 && city != oc && city != oa && !gap_ok(arr_ts, t.dep_ts[fi]) {
                        continue;
                    }
                    let dest = t.dest(fi).to_string();
                    if dest.is_empty() || dest == city || dest == d {
                        continue;
                    }
                    if allow_next.is_none() && visited.contains(&dest) {
                        continue;
                    }
                    if !t.dest_in(fi, allow_next.as_ref()) {
                        continue;
                    }
                    chosen.push(fi);
                    visited.push(dest.clone());
                    dfs(ctx, t, i + 1, &dest, t.arr_ord[fi], t.arr_ts[fi], chosen, visited, g + t.price[fi], out);
                    visited.pop();
                    chosen.pop();
                }
            }
        }
        let start_dt = parse_naive(&format!("{}T00:00:00", ctx.chain_start)).unwrap();
        for start in &start_codes(stops, t) {
            dfs(&ctx, t, 0, start, ordinal(start_dt.date()), naive_seconds(start_dt), &mut Vec::new(), &mut vec![start.clone()], 0.0, &mut out);
        }
        out.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        out
    }

    /// Вылет следующего плеча — строго в более поздний календарный день, чем прилёт
    /// предыдущего: тот же день не стыкуется даже позже по времени (и при открытом
    /// фильтре города, где раньше проходил вылет в тот же день и даже раньше прилёта).
    #[test]
    fn next_leg_departs_strictly_next_day() {
        let stops = vec![
            Stop::new("cities", vec!["MOW"], ["", ""]),
            Stop::new("any", vec![], ["2026-11-01", "2026-11-03"]),
            Stop::new("cities", vec!["SEL"], ["2026-11-01", "2026-11-03"]),
        ];
        // MOW→IST прилетает 01.11 в 14:00 (вылет 10:00 + 4 ч)
        let first = vec![flight("MOW", "IST", 1, 10, 100.0)];
        let same_day_later = vec![flight("IST", "SEL", 1, 20, 100.0)];
        let same_day_earlier = vec![flight("IST", "SEL", 1, 8, 100.0)];
        let next_day = vec![flight("IST", "SEL", 2, 0, 100.0)];
        for (onward, want) in [(same_day_later, 0usize), (same_day_earlier, 0), (next_day, 1)] {
            let table = FlightCols::from_collected(&[first.clone(), onward]);
            let ctx = build_ctx(&stops, &table, None);
            let mut check = |_: usize| Ok(());
            let got = search_cheapest(&ctx, 10, None, &mut check).unwrap();
            assert_eq!(got.len(), want);
            assert_eq!(brute_force(&stops, &table).len(), want);
        }
        // старт виртуальный: первое плечо может вылетать в первый день окна
        let table = FlightCols::from_collected(&[first.clone(), vec![flight("IST", "SEL", 2, 0, 100.0)]]);
        let ctx = build_ctx(&stops, &table, None);
        assert_eq!(ctx.chain_start, "2026-11-01");
        let mut check = |_: usize| Ok(());
        assert_eq!(search_cheapest(&ctx, 10, None, &mut check).unwrap().len(), 1);
    }

    #[test]
    fn numeric_stays_match_segment_strings() {
        // дни в городах, маска выходных и длина поездки по числовым колонкам ==
        // прежний расчёт по строкам дат сегментов
        let mut checked = 0;
        for seed in 1..=8 {
            let (stops, collected) = random_case(seed);
            let table = FlightCols::from_collected(&collected);
            let ctx = build_ctx(&stops, &table, None);
            let mut check = |_: usize| Ok(());
            let chains = search_cheapest(&ctx, 500, None, &mut check).unwrap();
            let start_iso = format!("{}T00:00:00", ctx.chain_start);
            let start_ord = date_ordinal(&ctx.chain_start).unwrap();
            for chain in &chains {
                let segs_owned: Vec<Segment> = chain.iter().map(|&fi| make_segment(&table.flight(fi))).collect();
                let segs: Vec<&Segment> = segs_owned.iter().collect();
                assert_eq!(chain_stays_num(&table, chain, start_ord), chain_stays(&segs, &start_iso), "seed {seed} {chain:?}");
                assert_eq!(trip_days_num(&table, chain), trip_days(&segs), "seed {seed} {chain:?}");
                checked += 1;
            }
        }
        assert!(checked > 100, "мало цепочек: {checked}");
    }

    #[test]
    fn lazy_search_matches_full_enumeration() {
        for seed in 1..=8 {
            let (stops, collected) = random_case(seed);
            let table = FlightCols::from_collected(&collected);
            let full = brute_force(&stops, &table);
            let ctx = build_ctx(&stops, &table, None);
            for n in [1usize, 7, 50, full.len(), full.len() + 10] {
                let mut check = |_: usize| Ok(());
                let got = search_cheapest(&ctx, n, None, &mut check).unwrap();
                let prices: Vec<f64> = got.iter().map(|c| c.iter().map(|&fi| table.price[fi]).sum()).collect();
                let want: Vec<f64> = full.iter().take(n).map(|p| p.0).collect();
                assert_eq!(prices, want, "seed {seed}, n {n}");
            }
        }
    }

    /// Первая остановка «любая»: старт — из любого города вылета первого плеча (раньше
    /// стартов не было и маршрутов — ноль: ANY → SEL → TAS на проде).
    #[test]
    fn any_first_stop_matches_full_enumeration() {
        for seed in 1..=8 {
            let (mut stops, collected) = random_case(seed);
            stops[0] = Stop { kind: "any".into(), codes: Vec::new(), ..stops[0].clone() };
            let table = FlightCols::from_collected(&collected);
            let full = brute_force(&stops, &table);
            assert!(!full.is_empty(), "seed {seed}: перебор пуст");
            let ctx = build_ctx(&stops, &table, None);
            let mut check = |_: usize| Ok(());
            let got = search_cheapest(&ctx, full.len() + 10, None, &mut check).unwrap();
            let prices: Vec<f64> = got.iter().map(|c| c.iter().map(|&fi| table.price[fi]).sum()).collect();
            let want: Vec<f64> = full.iter().map(|p| p.0).collect();
            assert_eq!(prices, want, "seed {seed}");
        }
    }

    #[test]
    fn aborts_on_check() {
        let (stops, collected) = random_case(3);
        let table = FlightCols::from_collected(&collected);
        let ctx = build_ctx(&stops, &table, None);
        let mut steps = 0;
        let mut abort = |_: usize| {
            steps += 1;
            if steps > 3 { Err(Aborted) } else { Ok(()) }
        };
        assert!(search_cheapest(&ctx, 10_000, None, &mut abort).is_err());
    }

    #[test]
    fn compact_view_materializes() {
        let (stops, collected) = random_case(5);
        let table = FlightCols::from_collected(&collected);
        let mut check = |_: usize| Ok(());
        let view = build_itineraries_compact(&stops, &table, 40, None, &mut check).unwrap();
        assert!(view.count > 0 && view.count <= 40);
        assert_eq!(view.chains.len(), view.count * 4);
        assert_eq!(view.days.len(), view.count * 5);
        let it = view.materialize(0);
        assert_eq!(it["id"], 1);
        assert_eq!(it["stops"].as_array().unwrap().len(), 5);
        assert_eq!(it["stops"][0]["code"], "MOW");
        assert_eq!(it["stops"][4]["code"], "MOW");
        assert!(it["stops"][1]["resolvedFromAny"].as_bool().unwrap());
        assert_eq!(it["stops"][0]["arrive"], "2026-11-01T00:00:00");
        assert!(it["total_days"].as_i64().unwrap() >= 1);
        let page = view.routes_page(0, 10);
        assert_eq!(page["items"].as_array().unwrap().len(), 10.min(view.count));
        // цены по возрастанию
        let prices: Vec<f64> = (0..view.count).map(|n| view.materialize(n)["total_price"].as_f64().unwrap()).collect();
        let mut sorted = prices.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(prices, sorted);
        // маршруты набора: тот же A* с зафиксированными городами
        let codes = view.chain_codes(0);
        let routes = build_combo_routes(&stops, &table, &[codes.clone()], None);
        assert!(!routes.is_empty());
        assert_eq!(routes[0]["combo"], combo_key(&codes));
        assert_eq!(routes[0]["total_price"], it["total_price"]);
    }

    #[test]
    fn city_filters_and_trip_length() {
        let (stops, collected) = random_case(2);
        let table = FlightCols::from_collected(&collected);
        let q = PlanQuery::from_value(&json!({
            "stops": stops.iter().map(|s| json!({"kind": s.kind, "codes": s.codes, "window": s.window})).collect::<Vec<_>>(),
            "cities": [{}, {"minStay": 1, "maxStay": 3}, {"requireWeekend": true}, {}, {}],
            "tripLength": [3, 12],
            "maxResults": 100000
        }))
        .unwrap();
        let ctx = build_ctx(&stops, &table, Some(&q));
        let mut check = |_: usize| Ok(());
        let got = search_cheapest(&ctx, 100000, Some(&q), &mut check).unwrap();
        // все выданные цепочки удовлетворяют фильтрам
        for chain in &got {
            let s1 = stay_days_secs(table.arr_ts[chain[0]], table.dep_ts[chain[1]]);
            assert!((1..=3).contains(&s1));
            assert!(weekend_covered(table.arr_ts[chain[1]], table.arr_ord[chain[1]], table.dep_ts[chain[2]], table.dep_ord[chain[2]]));
            let days = (table.arr_ord[chain[3]] - table.dep_ord[chain[0]]).max(1);
            assert!((3..=12).contains(&days));
        }
        // и это ровно те цепочки полного перебора, что проходят фильтры
        let full = brute_force(&stops, &table);
        let want: Vec<f64> = full
            .iter()
            .filter(|(_, c)| {
                let s1 = stay_days_secs(table.arr_ts[c[0]], table.dep_ts[c[1]]);
                let wk = weekend_covered(table.arr_ts[c[1]], table.arr_ord[c[1]], table.dep_ts[c[2]], table.dep_ord[c[2]]);
                let days = (table.arr_ord[c[3]] - table.dep_ord[c[0]]).max(1);
                (1..=3).contains(&s1) && wk && (3..=12).contains(&days)
            })
            .map(|p| p.0)
            .collect();
        let prices: Vec<f64> = got.iter().map(|c| c.iter().map(|&fi| table.price[fi]).sum()).collect();
        assert_eq!(prices, want);
    }
}
