//! Бенчмарк на сохранённых рейсах джобы: те же этапы, что у API (чтение Parquet,
//! A* до 5000 цепочек, наборы городов, страница маршрутов, маршруты двух наборов,
//! перестыковка под другой фильтр). Печатает JSON с минимальным временем из повторов.
//!
//!     cargo run --release --example bench -- plan_flights/<job>.parquet params.json [повторов]

use std::time::Instant;

use flights_planner::flightcols::FlightCols;
use flights_planner::overview::build_overview;
use flights_planner::planquery::PlanQuery;
use flights_planner::search::{build_combo_routes, build_itineraries_compact};
use flights_planner::stops::{parse_stops, DEFAULT_MAX_RESULTS};
use serde_json::{json, Map, Value};

fn timed<T>(label: &str, repeats: usize, out: &mut Map<String, Value>, mut f: impl FnMut() -> T) -> T {
    let mut best = f64::INFINITY;
    let mut res = None;
    for _ in 0..repeats {
        let t = Instant::now();
        res = Some(f());
        best = best.min(t.elapsed().as_secs_f64());
    }
    out.insert(label.into(), json!((best * 1000.0).round() / 1000.0));
    res.unwrap()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (path, params_path) = (&args[1], &args[2]);
    let repeats: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(3);
    let params: Value = serde_json::from_str(&std::fs::read_to_string(params_path).unwrap()).unwrap();
    let mut pq = PlanQuery::from_value(&params).unwrap();
    if pq.max_results.is_none() {
        pq.max_results = Some(DEFAULT_MAX_RESULTS);
    }
    let stops = parse_stops(&pq.stops);
    let mut alt_params = params.clone();
    for l in alt_params["legs"].as_array_mut().unwrap() {
        l["maxTransfers"] = json!(1);
    }
    let mut alt = PlanQuery::from_value(&alt_params).unwrap();
    if alt.max_results.is_none() {
        alt.max_results = Some(DEFAULT_MAX_RESULTS);
    }
    let mut out = Map::new();
    let mut ok = |_: usize| Ok(());

    let table = timed("read_parquet", repeats, &mut out, || FlightCols::read(path).unwrap());
    out.insert("rows".into(), json!(table.len()));
    let view = timed("astar_5000", repeats, &mut out, || {
        build_itineraries_compact(&stops, &table, pq.max_results.unwrap() as usize, Some(&pq), &mut ok).unwrap()
    });
    out.insert("count".into(), json!(view.count));
    let ov = timed("overview", repeats, &mut out, || build_overview(&stops, &table, Some(&pq)));
    out.insert("combos".into(), json!(ov.combos.len()));
    out.insert("totalCount".into(), json!(ov.total_count));
    let page = timed("routes_page_50", repeats, &mut out, || view.routes_page(0, 50));
    out.insert("top_price".into(), page["items"].get(0).map(|i| i["total_price"].clone()).unwrap_or(Value::Null));
    let mut by_count: Vec<&flights_planner::overview::Combo> = ov.combos.iter().collect();
    by_count.sort_by(|a, b| b.count.cmp(&a.count).then(a.min_price.partial_cmp(&b.min_price).unwrap()));
    let combos: Vec<String> = by_count.iter().take(2).map(|c| flights_planner::search::combo_key(&c.codes, &c.skipped)).collect();
    let cr = timed("combo_routes_2", repeats, &mut out, || build_combo_routes(&stops, &table, &combos, Some(&pq)));
    out.insert("combo_routes".into(), json!(cr.len()));
    let alt_view = timed("alt_filter_astar", repeats, &mut out, || {
        build_itineraries_compact(&stops, &table, alt.max_results.unwrap() as usize, Some(&alt), &mut ok).unwrap()
    });
    out.insert("alt_count".into(), json!(alt_view.count));
    let alt_ov = timed("alt_filter_overview", repeats, &mut out, || build_overview(&stops, &table, Some(&alt)));
    out.insert("alt_combos".into(), json!(alt_ov.combos.len()));
    // Этап после сбора: рейсы → колонки → Parquet (как put_plan_flights). Один раз: flight(i) кэшируется.
    let fresh = FlightCols::read(path).unwrap();
    let t = Instant::now();
    fresh.prefetch(&(0..fresh.len()).collect::<Vec<_>>());
    let tickets: Vec<std::sync::Arc<flights_planner::ticket::Ticket>> = (0..fresh.len()).map(|i| fresh.flight(i)).collect();
    out.insert("load_all_flights".into(), json!((t.elapsed().as_secs_f64() * 1000.0).round() / 1000.0));
    let t = Instant::now();
    let cols = FlightCols::from_collected(&[tickets]);
    out.insert("from_collected".into(), json!((t.elapsed().as_secs_f64() * 1000.0).round() / 1000.0));
    let tmp = std::env::temp_dir().join("bench_rs.parquet");
    let t = Instant::now();
    cols.write(&tmp.to_string_lossy()).unwrap();
    out.insert("write_parquet".into(), json!((t.elapsed().as_secs_f64() * 1000.0).round() / 1000.0));
    out.insert("written_bytes".into(), json!(std::fs::metadata(&tmp).map(|m| m.len()).unwrap_or(0)));
    std::fs::remove_file(&tmp).ok();
    println!("{}", Value::Object(out));
}
