//! Замер наборов на запросе с тремя «любыми» подряд на синтетических рейсах в масштабе
//! склада: MOW → ANY → ANY → ANY → MOW, ~N городов, по окну 7 дней на плечо.
//! Запуск: cargo run --release --example combos_any3 [cities] [flights_per_city_day]

use std::sync::Arc;
use std::time::Instant;

use flights_planner::flightcols::FlightCols;
use flights_planner::overview::{build_overview, build_overview_top};
use flights_planner::stops::Stop;
use flights_planner::ticket::Ticket;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn flight(o: &str, d: &str, day: u32, hour: u32, price: f64, rng: &mut Rng) -> Arc<Ticket> {
    let arr_day = if hour < 18 { day } else { day + 1 };
    Arc::new(Ticket {
        origin: Some(o.into()),
        origin_airport: Some(o.into()),
        destination: Some(d.into()),
        destination_airport: Some(d.into()),
        departure_at: Some(format!("2026-11-{day:02}T{hour:02}:00:00+03:00")),
        arrival_at: Some(format!("2026-11-{arr_day:02}T{:02}:30:00+03:00", (hour + 5) % 24)),
        duration: Some(300),
        price: Some(price),
        transfers: rng.below(3) as i64,
        transfer_points: Some(vec![]),
        ..Default::default()
    })
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n_cities: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(300);
    let per_day: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(3);
    let cities: Vec<String> = (0..n_cities).map(|i| format!("{}{}{}", (b'A' + (i / 676) as u8) as char, (b'A' + (i / 26 % 26) as u8) as char, (b'A' + (i % 26) as u8) as char)).filter(|c| c != "MOW").collect();
    let mut rng = Rng(42);
    let stops = vec![
        Stop::new("cities", vec!["MOW"], ["", ""]),
        Stop::new("any", vec![], ["2026-11-01", "2026-11-07"]),
        Stop::new("any", vec![], ["2026-11-03", "2026-11-12"]),
        Stop::new("any", vec![], ["2026-11-06", "2026-11-18"]),
        Stop::new("cities", vec!["MOW"], ["", ""]),
    ];
    let windows = [(1u32, 7u32), (3, 12), (6, 18), (9, 24)];
    let mut collected: Vec<Vec<Arc<Ticket>>> = Vec::new();
    for (i, (d0, d1)) in windows.iter().enumerate() {
        let mut leg = Vec::new();
        let origins: Vec<&str> = if i == 0 { vec!["MOW"] } else { cities.iter().map(String::as_str).collect() };
        for o in &origins {
            for day in *d0..=*d1 {
                for _ in 0..per_day {
                    let d = if i == 3 { "MOW".to_string() } else { cities[rng.below(cities.len() as u64) as usize].clone() };
                    if d == *o {
                        continue;
                    }
                    leg.push(flight(o, &d, day, rng.below(23) as u32, (30 + rng.below(600)) as f64 * 100.0, &mut rng));
                }
            }
        }
        collected.push(leg);
    }
    let rows: usize = collected.iter().map(|l| l.len()).sum();
    let t0 = Instant::now();
    let table = FlightCols::from_collected(&collected);
    println!("рейсов {rows}, таблица за {:.2} с", t0.elapsed().as_secs_f64());
    let t1 = Instant::now();
    let ov = build_overview(&stops, &table, None);
    println!("топ-1000: {} наборов, truncated={}, {:.2} с, minPrice[0]={}", ov.combos.len(), ov.truncated, t1.elapsed().as_secs_f64(), ov.combos.first().map(|c| c.min_price).unwrap_or(0.0));
    if args.get(3).map(|s| s == "full").unwrap_or(false) {
        let t2 = Instant::now();
        let full = build_overview_top(&stops, &table, None, usize::MAX);
        println!("полный: {} наборов, {:.2} с", full.combos.len(), t2.elapsed().as_secs_f64());
        assert_eq!(&full.combos[..ov.combos.len()], &ov.combos[..]);
        println!("совпадает с полным");
    }
}
