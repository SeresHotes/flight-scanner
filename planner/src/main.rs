//! Точка входа: `flights-planner` слушает 0.0.0.0:PORT (по умолчанию 8000).
//! Переменные: FLIGHT_DB (data/flights.db), COLLECTOR_URL, TRAVELPAYOUTS_TOKEN (без
//! коллектора), GEO_PATH, CITY_NAMES_PATH, AIRPORT_NETWORK_PATH; склад билетов — S3_*
//! (или LAKE_LOCAL_ROOT), LAKE_* (`lakesync`).

use flights_planner::api::{router, AppState};
use flights_planner::collector::collector_url;
use flights_planner::lakesync;
use flights_planner::{hot, nearby, segments};

/// Простейший .env: KEY=VALUE построчно, без перекрытия уже заданных переменных.
fn load_dotenv() {
    let Ok(text) = std::fs::read_to_string(".env") else { return };
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else { continue };
        let k = k.trim().strip_prefix("export ").unwrap_or(k.trim()).trim();
        let v = v.trim().trim_matches('"').trim_matches('\'');
        if std::env::var_os(k).is_none() {
            // SAFETY: старт процесса, других потоков ещё нет.
            unsafe { std::env::set_var(k, v) };
        }
    }
}

fn main() {
    load_dotenv();
    let db_path = hot::default_db();
    let app = match AppState::new(&db_path) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("[startup] БД {db_path}: {e}");
            std::process::exit(1);
        }
    };
    // Статичные справочники — один раз на процесс.
    segments::city_names();
    nearby::geo();
    let (quotes, stale) = {
        let conn = app.conn.lock().unwrap();
        // Джобы, не пережившие прошлый рестарт, висят в running — помечаем error.
        let stale = hot::fail_stale_jobs(&conn, "прервана рестартом сервера").unwrap_or(0);
        // С коллектором серии живут в озере: локальный кэш серий не нужен и не должен расти.
        if collector_url().is_some() {
            let _ = hot::drop_ticket_cache(&conn);
        }
        (hot::count_quotes(&conn).unwrap_or(0), stale)
    };
    println!(
        "[startup] котировок в БД: {quotes}; зависших джоб сброшено: {stale}; коллектор: {}",
        collector_url().unwrap_or_else(|| "нет (прямой GraphQL)".into())
    );
    // Склад билетов в памяти: снапшот с диска, затем озеро (S3 или LAKE_LOCAL_ROOT) в фоне.
    if lakesync::bootstrap(&db_path).is_none() {
        println!("[startup] склад билетов: нет (озеро не задано) — рейсы только сериями");
    }
    // карта аэропорт → город для страницы динамики — прогрев в фоне (проход по quotes)
    {
        let db = db_path.clone();
        std::thread::spawn(move || {
            let t = std::time::Instant::now();
            let n = hot::airport_city_map_cached(&db).len();
            println!("[startup] карта аэропорт → город: {n} за {:.1} с", t.elapsed().as_secs_f64());
        });
    }
    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(8000);
    let rt = tokio::runtime::Runtime::new().expect("tokio");
    rt.block_on(async move {
        let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await.expect("bind");
        println!("[startup] слушаю 0.0.0.0:{port}");
        axum::serve(listener, router(app)).await.expect("serve");
    });
}
