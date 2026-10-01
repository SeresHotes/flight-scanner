//! Запуск склада билетов. Переменные окружения:
//!   TICKETS_PG_URL           — postgres://user:pass@host:5432/db (обязательно)
//!   COLLECTOR_URL            — коллектор для сверки с озером (без него — только пуш и выборки)
//!   PORT                     — порт HTTP (8002)
//!   TICKETS_SYNC_SECONDS     — интервал сверки (300)
//!   TICKETS_SYNC_WORKERS     — параллельных загрузок файлов при сверке (4)
//!   TICKETS_MAX_ROWS         — потолок строк одного ответа /v1/tickets (2000000)
//!   TICKETS_RETENTION_HOURS  — как часто удалять прошедшие дни (1)

use std::sync::Arc;
use std::time::Duration;

use flights_tickets::api::{router, AppState};
use flights_tickets::store::Store;
use flights_tickets::sync::Syncer;

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).ok().filter(|s| !s.is_empty()).unwrap_or_else(|| default.to_string())
}

#[tokio::main]
async fn main() {
    let url = std::env::var("TICKETS_PG_URL").ok().filter(|s| !s.is_empty()).expect("TICKETS_PG_URL не задан");
    let port: u16 = env_or("PORT", "8002").parse().expect("PORT");
    let sync_secs: u64 = env_or("TICKETS_SYNC_SECONDS", "300").parse().expect("TICKETS_SYNC_SECONDS");
    let workers: usize = env_or("TICKETS_SYNC_WORKERS", "4").parse().expect("TICKETS_SYNC_WORKERS");
    let max_rows: i64 = env_or("TICKETS_MAX_ROWS", "2000000").parse().expect("TICKETS_MAX_ROWS");
    let retention_hours: u64 = env_or("TICKETS_RETENTION_HOURS", "1").parse().expect("TICKETS_RETENTION_HOURS");

    let store = Store::connect(&url, workers + 8).expect("postgres");
    // Postgres в compose может подниматься дольше нас — ждём.
    let mut attempt = 0;
    loop {
        match store.migrate().await {
            Ok(()) => break,
            Err(e) if attempt < 60 => {
                attempt += 1;
                println!("[tickets] postgres не готов ({e}), повтор через 2 с");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => panic!("postgres: {e}"),
        }
    }
    let collector = std::env::var("COLLECTOR_URL").ok().filter(|s| !s.is_empty());
    let syncer = collector.as_deref().map(|c| Syncer::new(c, store.clone(), workers));
    if let Some(s) = syncer.clone() {
        tokio::spawn(s.run_forever(Duration::from_secs(sync_secs)));
    }
    {
        let st = store.clone();
        tokio::spawn(async move {
            loop {
                // День вылета — местный, UTC-вчера ещё может быть «сегодня» где-то восточнее → запас сутки.
                let before = chrono::Utc::now().date_naive().pred_opt().unwrap();
                match st.retention(before).await {
                    Ok((t, s)) if t + s > 0 => println!("[tickets] ретеншн: удалено билетов {t}, серий {s} (дни до {before})"),
                    Ok(_) => {}
                    Err(e) => println!("[tickets] ретеншн: {e}"),
                }
                tokio::time::sleep(Duration::from_secs(retention_hours.max(1) * 3600)).await;
            }
        });
    }
    let app = Arc::new(AppState { store, syncer, max_rows });
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await.expect("bind");
    println!("[tickets] старт: порт {port}, коллектор {}, сверка каждые {sync_secs} с × {workers} загрузок", collector.as_deref().unwrap_or("—"));
    axum::serve(listener, router(app)).await.expect("serve");
}
