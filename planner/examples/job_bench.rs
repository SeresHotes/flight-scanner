//! Стенд «джоба на складе в памяти»: синтетический склад размером с прод (города по
//! закону Ципфа, ~100 тыс. билетов в день), затем типовые джобы — сбор из склада, рейсы
//! колонками, запись Parquet, стыковка. Время и прирост пиковой памяти по этапам.
//!
//!     cargo run --release --example job_bench -- [дней склада=14] [билетов в день=100000]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use chrono::{Duration, NaiveDate};
use flights_planner::collect::{collect_plan, store_view, CollectError, NoProgress, SeriesFetcher, SeriesResult};
use flights_planner::lakestore::{parse_file_key, LakeCols, LakeStore, SharedStore};
use flights_planner::planquery::PlanQuery;
use flights_planner::stops::parse_stops;
use flights_planner::ticket::{Baggage, Leg, Ticket, TransferPoint};
use flights_planner::worker::build_view;
use serde_json::json;

struct NoSeries;
impl SeriesFetcher for NoSeries {
    fn fetch(&self, _o: Option<&str>, _d: Option<&str>, _day: &str, _p: i64, _cb: &dyn Fn(i64, i64)) -> Result<SeriesResult, CollectError> {
        Ok(SeriesResult { tickets: Vec::new(), pages: 1, exhausted: true, error: false, cached: true })
    }
}

fn status_kb(name: &str) -> u64 {
    let text = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    text.lines().find(|l| l.starts_with(name)).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse().ok()).unwrap_or(0)
}

fn rss_mb() -> u64 {
    status_kb("VmRSS:") / 1024
}

fn peak_mb() -> u64 {
    status_kb("VmHWM:") / 1024
}

/// Сброс пика RSS (VmHWM) — Linux /proc/self/clear_refs.
fn reset_peak() {
    let _ = std::fs::write("/proc/self/clear_refs", "5");
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    /// Номер города по Ципфу: популярные чаще.
    fn zipf(&mut self, n: usize) -> usize {
        let u = (self.next() % 1_000_000) as f64 / 1_000_000.0;
        ((n as f64).powf(u) as usize).saturating_sub(1).min(n - 1)
    }
}

fn city(i: usize) -> String {
    if i == 0 {
        return "MOW".into();
    }
    let b = |k: usize| (b'A' + (k % 26) as u8) as char;
    format!("{}{}{}", b(i / 676), b(i / 26), b(i))
}

fn make_ticket(rng: &mut Rng, o: usize, d: usize, day: &str, cities: usize) -> Ticket {
    let (oc, dc) = (city(o), city(d));
    let hubs: Vec<usize> = (0..rng.below(3)).map(|_| rng.zipf(60)).filter(|&h| h != o && h != d).collect();
    let mut chain = vec![format!("{oc}1")];
    chain.extend(hubs.iter().map(|&h| format!("{}1", city(h))));
    chain.push(format!("{dc}1"));
    let h0 = 5 + rng.below(14);
    let legs: Vec<Leg> = chain
        .windows(2)
        .enumerate()
        .map(|(k, w)| Leg {
            origin: Some(w[0].clone()),
            destination: Some(w[1].clone()),
            departure_at: Some(format!("{day}T{:02}:{:02}:00+03:00", (h0 as usize + k * 4) % 24, rng.below(60))),
            arrival_at: Some(format!("{day}T{:02}:{:02}:00+03:00", (h0 as usize + k * 4 + 2) % 24, rng.below(60))),
            flight_number: Some(format!("{}", 100 + rng.below(8000))),
            carrier: Some(["SU", "TK", "PC", "EK", "QR", "HY", "S7"][rng.below(7) as usize].into()),
        })
        .collect();
    let points: Vec<TransferPoint> = hubs.iter().map(|&h| TransferPoint { code: Some(format!("{}1", city(h))), to: Some(format!("{}1", city(h))), minutes: Some(60 + rng.below(600) as i64), ..Default::default() }).collect();
    let dur = 120 + 240 * hubs.len() as i64 + rng.below(200) as i64;
    let _ = cities;
    Ticket {
        origin: Some(oc.clone()),
        destination: Some(dc.clone()),
        origin_airport: Some(format!("{oc}1")),
        destination_airport: Some(format!("{dc}1")),
        departure_at: legs[0].departure_at.clone(),
        arrival_at: None,
        duration: Some(dur),
        duration_to: Some(dur),
        transfers: hubs.len() as i64,
        airline: legs[0].carrier.clone(),
        flight_number: legs[0].flight_number.clone(),
        price: Some((3000 + rng.below(80000)) as f64),
        currency: Some("rub".into()),
        link: Some(format!("/search/{oc}{}{dc}1?t={}{:016x}{:016x}_{}", &day[8..10], legs[0].carrier.clone().unwrap(), rng.next(), rng.next(), rng.below(100000))),
        chain,
        legs,
        transfer_points: Some(points),
        baggage: Some(Baggage { known: true, included: rng.below(2) == 0, pieces: Some(1), kg: None }),
        baggage_code: Some("1".into()),
        source: Some("graphql".into()),
        search_origin: Some(oc),
        search_date: Some(day.into()),
        ..Default::default()
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let days: i64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(14);
    let per_day: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let cities = 1500usize;
    let start = NaiveDate::from_ymd_opt(2026, 11, 1).unwrap();
    let cutoff = start - Duration::days(1);
    // плотность города ~ Ципф: MOW ~ per_day/11
    let weights: Vec<f64> = (0..cities).map(|i| 1.0 / ((i + 1) as f64).powf(0.9)).collect();
    let wsum: f64 = weights.iter().sum();
    let store = Arc::new(LakeStore::new());
    let mut rng = Rng(0x9e3779b97f4a7c15);
    let rss0 = rss_mb();
    let t = Instant::now();
    let mut rows = 0usize;
    for dd in 0..days {
        let day = (start + Duration::days(dd)).to_string();
        for o in 0..cities {
            let n = ((per_day as f64 * weights[o] / wsum).round() as usize).max(1);
            let ts: Vec<Ticket> = (0..n).map(|_| {
                let mut d = rng.zipf(cities);
                if d == o {
                    d = (d + 1) % cities;
                }
                make_ticket(&mut rng, o, d, &day, cities)
            }).collect();
            rows += n;
            let key = format!("tickets/fetched=2026-10-01/origin={0}/{0}-ANY__{day}__08-00-00Z.parquet", city(o));
            store.apply(&parse_file_key(&key).unwrap(), &LakeCols::from_tickets(&ts), cutoff).unwrap();
        }
    }
    store.set_ready();
    let h = store.health();
    println!("склад: {rows} билетов, {} серий, {} МБ блоков, RSS +{} МБ, наполнение {:.1} с", h["series"], h["memory_mb"], rss_mb().saturating_sub(rss0), t.elapsed().as_secs_f64());

    // снапшот: холодные части уходят в файл, в памяти — только горячие колонки
    let snap = std::env::temp_dir().join("job_bench.snap");
    let t = Instant::now();
    store.save_snapshot(&snap.to_string_lossy()).unwrap();
    let h = store.health();
    println!("после снапшота ({:.1} с): {} МБ в памяти, {} МБ холодных частей на диске, RSS +{} МБ", t.elapsed().as_secs_f64(), h["memory_mb"], h["cold_disk_mb"], rss_mb().saturating_sub(rss0));
    let d = |n: i64| (start + Duration::days(n)).to_string();
    let st = |kind: &str, codes: &[&str], a: &str, b: &str| json!({"kind": kind, "codes": codes, "window": [a, b], "radiusKm": 0});
    let scenarios = vec![
        ("MOW → AAB(7 дн) → MOW", vec![st("cities", &["MOW"], "", ""), st("cities", &["AAB"], &d(0), &d(6)), st("cities", &["MOW"], "", "")]),
        ("MOW → ANY(3 дн) → MOW", vec![st("cities", &["MOW"], "", ""), st("any", &[], &d(0), &d(2)), st("cities", &["MOW"], "", "")]),
        ("MOW → ANY(7 дн) → MOW", vec![st("cities", &["MOW"], "", ""), st("any", &[], &d(0), &d(6)), st("cities", &["MOW"], "", "")]),
        ("MOW → ANY → ANY → MOW (по 3 дн)", vec![st("cities", &["MOW"], "", ""), st("any", &[], &d(0), &d(2)), st("any", &[], &d(3), &d(5)), st("cities", &["MOW"], "", "")]),
        ("ANY → AAB(5 дн) → AAC", vec![st("any", &[], &d(0), &d(0)), st("cities", &["AAB"], &d(1), &d(5)), st("cities", &["AAC"], &d(6), &d(10))]),
        ("ANY → ANY (3 дн)", vec![st("any", &[], &d(0), &d(2)), st("any", &[], "", "")]),
        ("ANY → ANY (7 дн)", vec![st("any", &[], &d(0), &d(6)), st("any", &[], "", "")]),
        ("ANY → ANY (14 дн)", vec![st("any", &[], &d(0), &d(13)), st("any", &[], "", "")]),
        ("ANY → ANY → ANY (по 3 дн)", vec![st("any", &[], &d(0), &d(2)), st("any", &[], &d(3), &d(5)), st("any", &[], "", "")]),
        ("MOW → ANY×4 → MOW (по 3 дн)", vec![st("cities", &["MOW"], "", ""), st("any", &[], &d(0), &d(2)), st("any", &[], &d(3), &d(5)), st("any", &[], &d(6), &d(8)), st("any", &[], &d(9), &d(11)), st("cities", &["MOW"], "", "")]),
    ];
    let shared = SharedStore(store.clone());
    println!("| сценарий | рейсов | сбор, с | стыковка, с | маршрутов | пик сверх склада, МБ | держит таблица, МБ |");
    println!("|---|---|---|---|---|---|---|");
    for (label, stops_json) in scenarios {
        let n = stops_json.len();
        let q = json!({"stops": stops_json, "cities": vec![json!({}); n], "legs": vec![json!({}); n - 1], "tripLength": [0, null], "maxResults": 1000});
        let pq = PlanQuery::from_value(&q).unwrap();
        let stops = parse_stops(&pq.stops);
        let base = rss_mb();
        reset_peak();
        let t = Instant::now();
        let view = store_view(Some(&shared), &stops);
        let mut airport_city: HashMap<String, String> = HashMap::new();
        let collected = match collect_plan(&stops, &NoSeries, view.as_ref(), &NoProgress, &mut airport_city, 1) {
            Ok(c) => c,
            Err(e) => {
                println!("| {label} | ошибка: {e} | | | | {} | |", peak_mb().saturating_sub(base));
                continue;
            }
        };
        let t_collect = t.elapsed().as_secs_f64();
        let flights = collected.len();
        let table = collected;
        let t = Instant::now();
        let mut progress = |_f: usize, _e: usize| Ok(());
        let res = build_view(&stops, &table, &pq, &mut progress, &|_| {});
        let t_build = t.elapsed().as_secs_f64();
        let count = res.map(|r| r.view.count).unwrap_or(0);
        let held = rss_mb().saturating_sub(base);
        println!("| {label} | {flights} | {t_collect:.2} | {t_build:.2} | {count} | {} | {held} |", peak_mb().saturating_sub(base));
        drop(table);
    }
}
