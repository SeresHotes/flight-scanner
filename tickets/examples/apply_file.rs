//! Локальный замер: применить файл озера к складу и сделать выборку.
//! TICKETS_PG_URL=postgres://… cargo run --release --example apply_file <файл.parquet> <ключ озера> [повторов]

use std::time::Instant;

use chrono::Utc;
use flights_tickets::arrow_out::ipc_from_rows;
use flights_tickets::store::{Store, TicketsQuery};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (path, key) = (&args[1], &args[2]);
    let repeats: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1);
    let url = std::env::var("TICKETS_PG_URL").expect("TICKETS_PG_URL");
    let store = Store::connect(&url, 4).unwrap();
    store.migrate().await.unwrap();
    let data = bytes::Bytes::from(std::fs::read(path).unwrap());
    for i in 0..repeats {
        // разные ключи (время) — чтобы каждый повтор был «более свежей» серией и заменял прежнюю
        let k = key.replace("__19-", &format!("__{:02}-", 19 + i as u32 % 5));
        let t = Instant::now();
        let st = store.apply_file(&k, Utc::now(), data.clone()).await.unwrap();
        println!("apply {}: days {} rows {} skipped {} already {} — {:.3} с", i, st.days, st.rows, st.skipped_days, st.already, t.elapsed().as_secs_f64());
    }
    let meta = flights_tickets::lake::parse_file_key(key).unwrap();
    let q = TicketsQuery { origins: vec![meta.origin.clone().unwrap()], from: meta.day, to: meta.day_to, ..Default::default() };
    let t = Instant::now();
    let rows = store.tickets(&q).await.unwrap();
    let t_q = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let ipc = ipc_from_rows(&rows);
    println!("выборка X→ANY: {} строк за {:.3} с, IPC {} байт за {:.3} с", rows.len(), t_q, ipc.len(), t.elapsed().as_secs_f64());
    let q2 = TicketsQuery { destinations: vec!["IST".into()], from: meta.day, to: meta.day_to, ..Default::default() };
    let t = Instant::now();
    let rows2 = store.tickets(&q2).await.unwrap();
    println!("выборка ANY→IST: {} строк за {:.3} с", rows2.len(), t.elapsed().as_secs_f64());
    let q3 = TicketsQuery { via: vec!["IST".into()], from: meta.day, to: meta.day_to, ..Default::default() };
    let rows3 = store.tickets(&q3).await.unwrap();
    println!("через IST: {} строк", rows3.len());
}
