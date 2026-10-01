//! ИССЛЕДОВАНИЕ (задача 6): нижняя оценка хвоста по городу vs по (город, день прилёта).
//! На сохранённых рейсах джобы (plan_flights/<job>.parquet + params.json) запускает A* и
//! наборы в обоих режимах и печатает шаги, «мёртвую» работу, время и память оценки.
//!
//!     cargo run --release --example lb_research -- plan_flights/<job>.parquet params.json [max_results]

use std::sync::atomic::Ordering;
use std::time::Instant;

use flights_planner::flightcols::FlightCols;
use flights_planner::overview::{build_overview, OV_EMPTY, OV_EXTENDS, OV_PRUNED};
use flights_planner::planquery::PlanQuery;
use flights_planner::search::{build_ctx, completion_lb, completion_lb_day, research_reset, research_stats, search_cheapest, RESEARCH_DAY_LB};
use flights_planner::stops::parse_stops;
use serde_json::{json, Value};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (path, params_path) = (&args[1], &args[2]);
    let max_results: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(5000);
    let params: Value = serde_json::from_str(&std::fs::read_to_string(params_path).unwrap()).unwrap();
    let pq = PlanQuery::from_value(&params).unwrap();
    let stops = parse_stops(&pq.stops);
    let table = FlightCols::read(path).unwrap();
    println!("рейсов {}, плеч {}, max_results {max_results}", table.len(), stops.len() - 1);

    // размер оценок
    let ctx = build_ctx(&stops, &table, Some(&pq));
    let t = Instant::now();
    let lb = completion_lb(&ctx);
    let t_city = t.elapsed().as_secs_f64();
    let n_city: usize = lb.iter().map(|m| m.len()).sum();
    let t = Instant::now();
    let dl = completion_lb_day(&ctx);
    let t_day = t.elapsed().as_secs_f64();
    let n_day: usize = dl.iter().map(|m| m.values().map(|v| v.len()).sum::<usize>()).sum();
    println!("оценка по городу: {n_city} записей, {:.3} с; по (город, день): {n_day} записей (~{} КБ), {:.3} с", t_city, n_day * 16 / 1024, t_day);
    // насколько оценка по дню выше (точнее) оценки по городу на стартах
    let start_ord = flights_planner::dates::date_ordinal(&ctx.chain_start).unwrap_or(0);
    for start in &stops[0].codes {
        let sid = ctx.code_id(start);
        let c = lb[0].get(&sid).copied();
        let d = flights_planner::search::day_lb_get(&dl, 0, sid, start_ord, true);
        println!("старт {start}: lb по городу {:?}, по дню {:?}", c, d);
    }

    let mut out = serde_json::Map::new();
    for (mode, flag) in [("city", false), ("day", true)] {
        RESEARCH_DAY_LB.store(flag, Ordering::Relaxed);
        research_reset();
        let ctx = build_ctx(&stops, &table, Some(&pq));
        let t = Instant::now();
        let mut check = |_: usize| Ok(());
        let chains = search_cheapest(&ctx, max_results, Some(&pq), &mut check).unwrap();
        let secs = t.elapsed().as_secs_f64();
        let mut st = research_stats();
        st["seconds"] = json!((secs * 1000.0).round() / 1000.0);
        st["chains"] = json!(chains.len());
        st["top_price"] = json!(chains.first().map(|c| c.iter().map(|&fi| table.price[fi]).sum::<f64>()));
        st["last_price"] = json!(chains.last().map(|c| c.iter().map(|&fi| table.price[fi]).sum::<f64>()));
        // наборы
        for c in [&OV_EXTENDS, &OV_PRUNED, &OV_EMPTY] {
            c.store(0, Ordering::Relaxed);
        }
        let t = Instant::now();
        let ov = build_overview(&stops, &table, Some(&pq));
        st["combos_seconds"] = json!((t.elapsed().as_secs_f64() * 1000.0).round() / 1000.0);
        st["combos"] = json!(ov.combos.len());
        st["combos_truncated"] = json!(ov.truncated);
        st["combos_extends"] = json!(OV_EXTENDS.load(Ordering::Relaxed));
        st["combos_pruned_after_extend"] = json!(OV_PRUNED.load(Ordering::Relaxed));
        st["combos_empty_after_extend"] = json!(OV_EMPTY.load(Ordering::Relaxed));
        st["combos_top_price"] = json!(ov.combos.first().map(|c| c.min_price));
        st["combos_kth_price"] = json!(ov.combos.last().map(|c| c.min_price));
        println!("{mode}: {}", serde_json::to_string(&st).unwrap());
        out.insert(mode.into(), st);
    }
    println!("{}", serde_json::to_string_pretty(&Value::Object(out)).unwrap());
}
