//! Остановки запроса, окна плеч и оценка объёма сбора (зеркало `core.planner`:
//! Stop, parse_stops, collect_view, _leg_window/_leg_dates, estimate_plan, plan_series).

use serde::Serialize;
use serde_json::Value;

use crate::dates::{date_range, shift_date};
use crate::nearby::{clamp_radius, Hops};
use crate::planquery::StopSpec;
use crate::segments::city_info;

pub const SECONDS_PER_REQUEST: f64 = 1.0;
/// Предохранитель: столько ХОЛОДНЫХ страниц (не в кэше) за один сбор, ~35 мин.
pub const MAX_REQUESTS: i64 = 2000;
/// Потолок суммарной ширины окон дат всех плеч (дни): объём выборки из склада растёт с числом
/// «любых» подряд и шириной окон. Совпадает с frontend/src/planner/validation.ts.
pub const MAX_TOTAL_WINDOW_DAYS: i64 = 60;
/// Потолок шагов перебора маршрутов (извлечений из кучи) на одну стыковку.
pub const MAX_SEARCH_STEPS: usize = 3_000_000;
/// Потолок стыковок групп (extend) при обходе наборов городов.
pub const MAX_COMBO_STEPS: usize = 2_000_000;
/// Потолок рейсов одной джобы (все плечи). С рейсами джобы номерами строк склада
/// (01.10.2026) — ~1,2 КБ пика на рейс вместо ~4–6 КБ: 1,25 млн рейсов (MOW → ANY×4 → MOW
/// по 3 дня, синтетика `examples/job_bench.rs`) — 1,5 ГБ сверх склада; раньше 300 тыс. —
/// 1,7 ГБ RSS, 1,8 млн — OOM на 8 ГБ VM.
pub const MAX_JOB_FLIGHTS: usize = 2_000_000;
/// Потолок max_results: компактный перебор держит его в памяти VM (4 ГБ).
pub const MAX_RESULTS: i64 = 1_000_000;
/// Сколько самых дешёвых цепочек строит джоба, если запрос не задал.
pub const DEFAULT_MAX_RESULTS: i64 = 5_000;
/// Сколько самых дешёвых цепочек строится на один выбранный набор.
pub const COMBO_MAX_RESULTS: i64 = 2_000;
pub const DEFAULT_START: &str = "2026-11-01";
pub const DEFAULT_LEG_DAYS: i64 = 7;
pub const FINAL_STAY_DAYS: i64 = 5;
/// Оценка серии GraphQL в «запросах» (страницах по 400 билетов).
pub const PAGES_CITY: i64 = 1;
pub const PAGES_ANY: i64 = 12;

pub fn is_valid_max_results(n: Option<i64>) -> bool {
    matches!(n, Some(n) if (1..=MAX_RESULTS).contains(&n))
}

/// Остановка запроса: набор городов (kind='cities') или «любой» + окно дат.
/// radius_km — можно улететь дальше из соседнего города; exact — прилёт строго в codes.
#[derive(Debug, Clone, PartialEq)]
pub struct Stop {
    pub kind: String,
    pub codes: Vec<String>,
    pub window: [String; 2],
    pub radius_km: i64,
    pub exact: bool,
}

impl Stop {
    pub fn new(kind: &str, codes: Vec<&str>, window: [&str; 2]) -> Stop {
        Stop {
            kind: kind.to_string(),
            codes: codes.into_iter().filter(|c| !c.is_empty()).map(|c| c.to_uppercase()).collect(),
            window: [window[0].to_string(), window[1].to_string()],
            radius_km: 0,
            exact: false,
        }
    }

    pub fn from_spec(s: &StopSpec) -> Stop {
        Stop { kind: s.kind.clone(), codes: s.codes.clone(), window: s.window.clone(), radius_km: s.radius_km, exact: false }
    }

    /// Принимаем и codes:[...], и airports:[{code}] (как во фронтовом PlannerStop).
    pub fn from_value(d: &Value) -> Stop {
        let codes: Vec<String> = match d.get("codes").and_then(|v| v.as_array()) {
            Some(a) => a.iter().filter_map(|v| v.as_str()).map(|s| s.to_string()).collect(),
            None => d
                .get("airports")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|x| x.get("code").and_then(|c| c.as_str()).map(|s| s.to_string())).collect())
                .unwrap_or_default(),
        };
        let win = d.get("window").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        let w0 = win.first().and_then(|v| v.as_str()).unwrap_or("").to_string();
        let w1 = win.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string();
        Stop {
            kind: d.get("kind").and_then(|v| v.as_str()).unwrap_or("cities").to_string(),
            codes: codes.into_iter().filter(|c| !c.is_empty()).map(|c| c.to_uppercase()).collect(),
            window: [w0, w1],
            radius_km: clamp_radius(d.get("radiusKm")),
            exact: false,
        }
    }

    pub fn is_cities(&self) -> bool {
        self.kind == "cities"
    }
}

pub fn parse_stops(specs: &[StopSpec]) -> Vec<Stop> {
    specs.iter().map(Stop::from_spec).collect()
}

/// Остановки для сбора и оценки: к городам остановки с радиусом добавлены соседи.
pub fn collect_view(stops: &[Stop]) -> Vec<Stop> {
    let hops = Hops::new(stops);
    if !hops.active() {
        return stops.to_vec();
    }
    stops
        .iter()
        .enumerate()
        .map(|(i, s)| {
            if s.is_cities() {
                Stop { kind: s.kind.clone(), codes: hops.collect_codes(i), window: s.window.clone(), radius_km: 0, exact: false }
            } else {
                s.clone()
            }
        })
        .collect()
}

fn has_window(w: &[String; 2]) -> bool {
    !w[0].is_empty() && !w[1].is_empty()
}

/// Окно плеча i: заданное окно того конца, у кого оно есть.
pub fn leg_window(stops: &[Stop], i: usize) -> Option<&[String; 2]> {
    if has_window(&stops[i].window) {
        return Some(&stops[i].window);
    }
    if has_window(&stops[i + 1].window) {
        return Some(&stops[i + 1].window);
    }
    None
}

/// Конкретные даты сбора плеча i (с дефолтом, если окон нигде нет).
pub fn leg_dates(stops: &[Stop], i: usize) -> Vec<String> {
    match leg_window(stops, i) {
        Some(w) => date_range(&w[0], &w[1]),
        None => date_range(DEFAULT_START, &shift_date(DEFAULT_START, DEFAULT_LEG_DAYS - 1)),
    }
}

pub fn leg_days(stops: &[Stop], i: usize) -> i64 {
    match leg_window(stops, i) {
        Some(w) => date_range(&w[0], &w[1]).len() as i64,
        None => DEFAULT_LEG_DAYS,
    }
}

/// Страниц на плечо i: город→город — пары A×B по PAGES_CITY плюс hidden-city A→ANY
/// по PAGES_ANY на город A; с «любым» концом — по PAGES_ANY на каждый конкретный
/// город другого конца. Всё × дней окна.
pub fn leg_requests(stops: &[Stop], i: usize) -> i64 {
    let (from, to) = (&stops[i], &stops[i + 1]);
    let days = leg_days(stops, i);
    if from.is_cities() && to.is_cities() {
        let a = from.codes.len().max(1) as i64;
        let b = to.codes.len().max(1) as i64;
        return days * (a * b * PAGES_CITY + a * PAGES_ANY);
    }
    let anchor = if from.is_cities() { from } else { to };
    days * anchor.codes.len().max(1) as i64 * PAGES_ANY
}

pub fn stop_label(stop: &Stop) -> String {
    if stop.kind == "any" {
        return "Любой город".into();
    }
    if stop.codes.is_empty() {
        return "Город не выбран".into();
    }
    if stop.codes.len() == 1 {
        let c = &stop.codes[0];
        return format!("{} ({c})", city_info(c).city);
    }
    stop.codes.join("/")
}

/// Серия сбора «направление × день»: (плечо, origin, dest, день, страниц в оценке).
#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    pub leg: usize,
    pub origin: Option<String>,
    pub dest: Option<String>,
    pub day: String,
    pub pages: i64,
}

/// Ровно те серии, что запросит сбор (включая A→ANY под hidden-city).
pub fn plan_series(stops: &[Stop]) -> Vec<Series> {
    let stops = collect_view(stops);
    let mut out = Vec::new();
    for i in 0..stops.len().saturating_sub(1) {
        let (from, to) = (&stops[i], &stops[i + 1]);
        for day in leg_dates(&stops, i) {
            if from.is_cities() && to.is_cities() {
                for a in &from.codes {
                    for b in &to.codes {
                        out.push(Series { leg: i, origin: Some(a.clone()), dest: Some(b.clone()), day: day.clone(), pages: PAGES_CITY });
                    }
                    out.push(Series { leg: i, origin: Some(a.clone()), dest: None, day: day.clone(), pages: PAGES_ANY });
                }
            } else if from.is_cities() {
                for a in &from.codes {
                    out.push(Series { leg: i, origin: Some(a.clone()), dest: None, day: day.clone(), pages: PAGES_ANY });
                }
            } else if to.is_cities() {
                for b in &to.codes {
                    out.push(Series { leg: i, origin: None, dest: Some(b.clone()), day: day.clone(), pages: PAGES_ANY });
                }
            } else {
                // любой → любой: только склад билетов, одна «серия» на день (PAGES_ANY в оценке)
                out.push(Series { leg: i, origin: None, dest: None, day: day.clone(), pages: PAGES_ANY });
            }
        }
    }
    out
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EstimateLeg {
    #[serde(rename = "fromLabel")]
    pub from_label: String,
    #[serde(rename = "toLabel")]
    pub to_label: String,
    pub days: i64,
    pub requests: i64,
    pub cached: i64,
    #[serde(rename = "anyLeg")]
    pub any_leg: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Estimate {
    pub requests: i64,
    /// Страниц, которые не пойдут в источник: серии уже в складе билетов или в кэше серий.
    pub cached: i64,
    pub cold: i64,
    pub seconds: i64,
    pub legs: Vec<EstimateLeg>,
    /// Откуда берутся рейсы: "tickets" (склад) или "collector" (серии через коллектор/GraphQL).
    pub source: String,
}

/// Оценка объёма сбора: всего страниц, сколько уже в кэше серий (`is_cached`),
/// сколько холодных и время по холодным.
pub fn estimate_plan(stops: &[Stop], is_cached: Option<&dyn Fn(&Series) -> bool>) -> Estimate {
    let stops = collect_view(stops);
    let mut cached_by_leg: Vec<i64> = vec![0; stops.len()];
    if let Some(probe) = is_cached {
        for s in plan_series(&stops) {
            if probe(&s) {
                cached_by_leg[s.leg] += s.pages;
            }
        }
    }
    let mut legs = Vec::new();
    let (mut requests, mut cached) = (0, 0);
    for i in 0..stops.len().saturating_sub(1) {
        let reqs = leg_requests(&stops, i);
        legs.push(EstimateLeg {
            from_label: stop_label(&stops[i]),
            to_label: stop_label(&stops[i + 1]),
            days: leg_days(&stops, i),
            requests: reqs,
            cached: cached_by_leg[i],
            any_leg: stops[i].kind == "any" || stops[i + 1].kind == "any",
        });
        requests += reqs;
        cached += cached_by_leg[i];
    }
    let cold = (requests - cached).max(0);
    Estimate { requests, cached, cold, seconds: (cold as f64 * SECONDS_PER_REQUEST).round() as i64, legs, source: "collector".into() }
}

/// Суммарная ширина окон всех плеч (дни).
pub fn total_window_days(stops: &[Stop]) -> i64 {
    (0..stops.len().saturating_sub(1)).map(|i| leg_days(stops, i)).sum()
}

pub fn request_count(stops: &[Stop]) -> i64 {
    let stops = collect_view(stops);
    (0..stops.len().saturating_sub(1)).map(|i| leg_requests(&stops, i)).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimate_counts_pages() {
        let stops = vec![
            Stop::new("cities", vec!["MOW"], ["", ""]),
            Stop::new("cities", vec!["IST", "DXB"], ["2026-11-01", "2026-11-03"]),
            Stop::new("any", vec![], ["2026-11-05", "2026-11-06"]),
            Stop::new("cities", vec!["MOW"], ["", ""]),
        ];
        let est = estimate_plan(&stops, None);
        // окно плеча — от его первой остановки: плечо 0: 3 дня × (1×2×1 + 1×12) = 42;
        // плечо 1: 3 дня × 2 × 12 = 72; плечо 2: 2 дня × 1 × 12 = 24
        assert_eq!(est.legs.iter().map(|l| l.requests).collect::<Vec<_>>(), vec![42, 72, 24]);
        assert_eq!(est.requests, 138);
        assert_eq!(est.cold, 138);
        assert_eq!(plan_series(&stops).iter().map(|s| s.pages).sum::<i64>(), 138);
        assert!(est.legs[1].any_leg);
        let probe = |s: &Series| s.leg == 0;
        let est2 = estimate_plan(&stops, Some(&probe));
        assert_eq!(est2.cached, 42);
        assert_eq!(est2.cold, 96);
        assert_eq!(leg_dates(&stops, 0).len(), 3);
    }

    #[test]
    fn total_window_days_sums_legs() {
        let stops = vec![
            Stop::new("cities", vec!["MOW"], ["", ""]),
            Stop::new("any", vec![], ["2026-11-01", "2026-11-10"]),
            Stop::new("any", vec![], ["2026-11-05", "2026-11-24"]),
            Stop::new("cities", vec!["TBS"], ["", ""]),
        ];
        // плечо 0 — окно остановки 1 (10 дн.), плечо 1 — остановки 1 (10), плечо 2 — остановки 2 (20)
        assert_eq!(total_window_days(&stops), 40);
        assert!(total_window_days(&stops) <= MAX_TOTAL_WINDOW_DAYS);
        assert_eq!(plan_series(&stops).iter().filter(|s| s.origin.is_none() && s.dest.is_none()).count(), 10, "любой → любой: серия на день");
    }

    #[test]
    fn default_window() {
        let stops = vec![Stop::new("cities", vec!["MOW"], ["", ""]), Stop::new("cities", vec!["SEL"], ["", ""])];
        assert_eq!(leg_dates(&stops, 0)[0], DEFAULT_START);
        assert_eq!(leg_days(&stops, 0), DEFAULT_LEG_DAYS);
        assert_eq!(request_count(&stops), 7 * 13);
    }
}
