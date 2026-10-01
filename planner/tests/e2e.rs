//! Сквозной тест: мок коллектора и мок склада билетов (оба — Arrow IPC в схеме озера) +
//! приложение планировщика. Запуск джобы → статус до done → наборы городов → маршруты →
//! маршруты набора → другой фильтр (стыковка в фоне) → повторный запуск переиспользует джобу.
//! Склад покрывает часть дней (MOW 01–02.11, IST 02–03.11), остальное джоба берёт сериями
//! у коллектора — итог тот же, что был сериями целиком.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Path, Query, State};
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};

use flights_planner::api::{router, AppState};
use flights_planner::collector::testing::lake_ipc;
use flights_planner::ticket::{Leg, Ticket, TransferPoint};

fn ticket(chain: &[&str], origin_city: &str, dest_city: &str, day: &str, hour: usize, price: f64) -> Ticket {
    let legs: Vec<Leg> = chain
        .windows(2)
        .enumerate()
        .map(|(k, w)| Leg {
            origin: Some(w[0].into()),
            destination: Some(w[1].into()),
            departure_at: Some(format!("{day}T{:02}:00:00+03:00", hour + k * 4)),
            arrival_at: Some(format!("{day}T{:02}:00:00+03:00", hour + 2 + k * 4)),
            flight_number: Some(format!("{}{}", chain[0], k)),
            carrier: None,
        })
        .collect();
    let points: Vec<TransferPoint> = chain[1..chain.len() - 1].iter().map(|c| TransferPoint { code: Some(c.to_string()), to: Some(c.to_string()), minutes: Some(120), ..Default::default() }).collect();
    Ticket {
        origin: Some(origin_city.into()),
        destination: Some(dest_city.into()),
        origin_airport: Some(chain[0].into()),
        destination_airport: Some(chain[chain.len() - 1].into()),
        departure_at: legs[0].departure_at.clone(),
        arrival_at: legs.last().unwrap().arrival_at.clone(),
        duration: Some(120 + 240 * (legs.len() as i64 - 1)),
        duration_to: Some(120 * legs.len() as i64),
        transfers: (chain.len() - 2) as i64,
        price: Some(price),
        chain: chain.iter().map(|s| s.to_string()).collect(),
        legs,
        transfer_points: Some(points),
        baggage: Some(flights_planner::ticket::Baggage { known: true, included: price > 15000.0, pieces: None, kg: None }),
        search_date: Some(day.into()),
        link: Some(format!("/search/{origin_city}0111{dest_city}1?t=X")),
        ..Default::default()
    }
}

type Series = HashMap<(String, String, String), Vec<Ticket>>;

struct Mock {
    series: Series,
    /// Склад билетов: (город, день) → билеты X→ANY.
    store: HashMap<(String, String), Vec<Ticket>>,
    store_calls: std::sync::Mutex<Vec<String>>,
}

async fn store_tickets(State(m): State<Arc<Mock>>, Query(q): Query<HashMap<String, String>>) -> impl IntoResponse {
    let list = |k: &str| -> Vec<String> { q.get(k).map(|v| v.split(',').filter(|s| !s.is_empty()).map(|s| s.to_uppercase()).collect()).unwrap_or_default() };
    let (origins, dests) = (list("origin"), list("destination"));
    let (from, to) = (q["from"].clone(), q.get("to").cloned().unwrap_or(q["from"].clone()));
    m.store_calls.lock().unwrap().push(format!("o={} d={} {from}..{to}", origins.join(","), dests.join(",")));
    let mut out = Vec::new();
    for ((o, day), tickets) in &m.store {
        if *day < from || *day > to || (!origins.is_empty() && !origins.contains(o)) {
            continue;
        }
        for t in tickets {
            if dests.is_empty() || t.side_codes(true).iter().any(|c| dests.contains(c)) {
                out.push(t.clone());
            }
        }
    }
    ([(header::CONTENT_TYPE, "application/vnd.apache.arrow.stream".to_string()), (header::HeaderName::from_static("x-tickets-count"), out.len().to_string())], lake_ipc(&out))
}

async fn store_coverage(State(m): State<Arc<Mock>>, Query(q): Query<HashMap<String, String>>) -> Json<Value> {
    let origins: Vec<String> = q.get("origin").map(|v| v.split(',').map(|s| s.to_uppercase()).collect()).unwrap_or_default();
    let (from, to) = (q["from"].clone(), q.get("to").cloned().unwrap_or(q["from"].clone()));
    let series: Vec<Value> = m.store.iter().filter(|((o, d), _)| *d >= from && *d <= to && (origins.is_empty() || origins.contains(o))).map(|((o, d), t)| json!({"origin": o, "day": d, "fetched_at": "2026-09-30T00:00:00Z", "tickets": t.len()})).collect();
    Json(json!({"series": series, "count": series.len()}))
}

async fn store_days(State(m): State<Arc<Mock>>, Query(q): Query<HashMap<String, String>>) -> Json<Value> {
    let (from, to) = (q["from"].clone(), q.get("to").cloned().unwrap_or(q["from"].clone()));
    let mut days: HashMap<String, i64> = HashMap::new();
    for (_, d) in m.store.keys() {
        if *d >= from && *d <= to {
            *days.entry(d.clone()).or_default() += 1;
        }
    }
    Json(json!({"days": days.iter().map(|(d, n)| json!({"day": d, "series": n, "tickets": 0})).collect::<Vec<_>>()}))
}

async fn store_health() -> Json<Value> {
    Json(json!({"status": "ok", "series": 4}))
}

async fn mock_health() -> Json<Value> {
    Json(json!({"status": "ok", "series": 3}))
}

async fn mock_fetch(State(m): State<Arc<Mock>>, Json(body): Json<Value>) -> Json<Value> {
    let key = (
        body["origin"].as_str().unwrap_or("").to_string(),
        body["destination"].as_str().unwrap_or("").to_string(),
        body["day"].as_str().unwrap_or("").to_string(),
    );
    let n = m.series.get(&key).map(|v| v.len()).unwrap_or(0);
    let id = format!("{}|{}|{}", key.0, key.1, key.2);
    Json(json!({"id": id, "status": "done", "pages": 1, "tickets": n}))
}

async fn mock_status(Path(id): Path<String>) -> Json<Value> {
    Json(json!({"id": id, "status": "done", "pages": 1}))
}

async fn mock_result(State(m): State<Arc<Mock>>, Path(id): Path<String>, Query(q): Query<HashMap<String, String>>) -> impl IntoResponse {
    let parts: Vec<&str> = id.split('|').collect();
    let key = (parts[0].to_string(), parts[1].to_string(), parts[2].to_string());
    let tickets = m.series.get(&key).cloned().unwrap_or_default();
    assert_eq!(q.get("format").map(String::as_str), Some("arrow"));
    let meta = json!({"pages": 1, "exhausted": true, "error": false, "cached": true});
    (
        [(header::CONTENT_TYPE, "application/vnd.apache.arrow.stream".to_string()), (header::HeaderName::from_static("x-series-meta"), meta.to_string())],
        lake_ipc(&tickets),
    )
}

async fn mock_exists(State(m): State<Arc<Mock>>, Query(q): Query<HashMap<String, String>>) -> Json<Value> {
    let key = (q.get("origin").cloned().unwrap_or_default(), q.get("destination").cloned().unwrap_or_default(), q.get("day").cloned().unwrap_or_default());
    Json(json!({"exists": m.series.contains_key(&key)}))
}

/// Один мок на оба сервиса: коллектор (/v1/fetch…) и склад билетов (/v1/tickets…).
fn mock_router(series: Series, store: HashMap<(String, String), Vec<Ticket>>) -> Router {
    Router::new()
        .route("/v1/health", get(mock_health))
        .route("/v1/fetch", post(mock_fetch))
        .route("/v1/requests/{id}", get(mock_status))
        .route("/v1/requests/{id}/result", get(mock_result))
        .route("/v1/series/exists", get(mock_exists))
        .route("/store/v1/health", get(store_health))
        .route("/store/v1/tickets", get(store_tickets))
        .route("/store/v1/coverage", get(store_coverage))
        .route("/store/v1/coverage/days", get(store_days))
        .with_state(Arc::new(Mock { series, store, store_calls: std::sync::Mutex::new(Vec::new()) }))
}

/// Склад: X→ANY города за день — все билеты из города (прямые A→B входят в A→ANY).
fn store(series: &Series) -> HashMap<(String, String), Vec<Ticket>> {
    let mut out: HashMap<(String, String), Vec<Ticket>> = HashMap::new();
    for (o, day) in [("MOW", "2026-11-01"), ("MOW", "2026-11-02"), ("IST", "2026-11-02"), ("IST", "2026-11-03")] {
        let mut tickets: Vec<Ticket> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for ((so, _sd, sday), ts) in series {
            if so == o && sday == day {
                for t in ts {
                    if seen.insert(t.flight_key()) {
                        tickets.push(t.clone());
                    }
                }
            }
        }
        out.insert((o.to_string(), day.to_string()), tickets);
    }
    out
}

fn series() -> Series {
    let mut s: Series = HashMap::new();
    let put = |s: &mut Series, o: &str, d: &str, day: &str, t: Vec<Ticket>| {
        s.insert((o.to_string(), d.to_string(), day.to_string()), t);
    };
    // плечо 0: MOW → IST, окно остановки IST 2026-11-01..02
    put(&mut s, "MOW", "IST", "2026-11-01", vec![ticket(&["SVO", "IST"], "MOW", "IST", "2026-11-01", 8, 20000.0)]);
    put(&mut s, "MOW", "IST", "2026-11-02", vec![ticket(&["SVO", "IST"], "MOW", "IST", "2026-11-02", 8, 21000.0), ticket(&["VKO", "SAW", "IST"], "MOW", "IST", "2026-11-02", 6, 12000.0)]);
    // hidden-city: MOW→IST→DXB дешевле прямого MOW→IST того дня → виртуальный рейс
    put(&mut s, "MOW", "", "2026-11-01", vec![ticket(&["SVO", "IST", "DXB"], "MOW", "DXB", "2026-11-01", 8, 15000.0), ticket(&["SVO", "IST"], "MOW", "IST", "2026-11-01", 8, 20000.0)]);
    put(&mut s, "MOW", "", "2026-11-02", vec![]);
    put(&mut s, "MOW", "IST", "2026-11-03", vec![]);
    put(&mut s, "MOW", "", "2026-11-03", vec![]);
    // плечо 1: IST → MOW, окно от остановки IST (2026-11-01..03); финал без окна.
    // Вылет — строго на следующий день после прилёта: прилетевшие 01.11 улетают 02 или 03, прилетевшие 02.11 — только 03.
    put(&mut s, "IST", "MOW", "2026-11-01", vec![]);
    put(&mut s, "IST", "MOW", "2026-11-02", vec![ticket(&["IST", "SVO"], "IST", "MOW", "2026-11-02", 20, 9000.0), ticket(&["IST", "VKO"], "IST", "MOW", "2026-11-02", 22, 7000.0)]);
    put(&mut s, "IST", "MOW", "2026-11-03", vec![ticket(&["IST", "SVO"], "IST", "MOW", "2026-11-03", 20, 9500.0), ticket(&["IST", "VKO"], "IST", "MOW", "2026-11-03", 22, 7500.0)]);
    put(&mut s, "IST", "", "2026-11-01", vec![]);
    put(&mut s, "IST", "", "2026-11-02", vec![]);
    put(&mut s, "IST", "", "2026-11-03", vec![]);
    s
}

async fn get_json(client: &reqwest::Client, url: &str) -> Value {
    client.get(url).send().await.unwrap().json().await.unwrap()
}

async fn wait_done(client: &reqwest::Client, url: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let st = get_json(client, url).await;
        let status = st["status"].as_str().unwrap_or("");
        if status == "done" || status == "error" {
            return st;
        }
        assert!(Instant::now() < deadline, "джоба не завершилась: {st}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plan_job_end_to_end() {
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    let all = series();
    tokio::spawn(async move { axum::serve(mock_listener, mock_router(all.clone(), store(&all))).await.unwrap() });
    // SAFETY: переменные читаются позже из рабочих потоков; здесь их ещё нет.
    unsafe {
        std::env::set_var("COLLECTOR_URL", format!("http://{mock_addr}"));
        std::env::set_var("TICKETS_URL", format!("http://{mock_addr}/store"));
        std::env::set_var("AIRPORT_NETWORK_PATH", "/nonexistent/airport_network.json");
        std::env::set_var("GEO_PATH", "../core/geo.json");
        std::env::set_var("CITY_NAMES_PATH", "../core/city_names.json");
    }
    let dir = std::env::temp_dir().join(format!("planner-e2e-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("flights.db").to_string_lossy().to_string();
    let app = AppState::new(&db).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router(app)).await.unwrap() });
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    let health = get_json(&client, &format!("{base}/api/health")).await;
    assert_eq!(health["status"], "ok");
    assert_eq!(health["collector"]["status"], "ok");
    assert_eq!(health["tickets"]["status"], "ok");

    let airports = get_json(&client, &format!("{base}/api/airports?q=mos")).await;
    assert_eq!(airports["airports"][0]["code"], "MOW");

    let query = json!({
        "stops": [
            {"kind": "cities", "codes": ["MOW"], "window": ["", ""], "radiusKm": 0},
            {"kind": "cities", "codes": ["IST"], "window": ["2026-11-01", "2026-11-03"], "radiusKm": 0},
            {"kind": "cities", "codes": ["MOW"], "window": ["", ""], "radiusKm": 0}
        ],
        "cities": [{}, {}, {}],
        "legs": [{}, {}],
        "tripLength": [0, null]
    });
    let est: Value = client.post(format!("{base}/api/plan/estimate")).json(&query).send().await.unwrap().json().await.unwrap();
    assert_eq!(est["requests"], 3 * 13 + 3 * 13);
    assert_eq!(est["cached"], est["requests"], "всё либо в складе, либо в кэше серий мока: {est}");
    assert_eq!(est["cold"], 0);
    assert_eq!(est["source"], "tickets");

    let run: Value = client.post(format!("{base}/api/plan/run")).json(&query).send().await.unwrap().json().await.unwrap();
    assert_eq!(run["status"], "collecting", "{run}");
    assert_eq!(run["mode"], "routes");
    let job_id = run["job_id"].as_str().unwrap().to_string();

    let st = wait_done(&client, &format!("{base}/api/plan/jobs/{job_id}")).await;
    assert_eq!(st["status"], "done", "{st}");
    assert_eq!(st["progress"], st["total"]);
    // из склада: MOW 01–02 и IST 02–03 (4 единицы), сериями из кэша коллектора: MOW 03 и IST 01 (по 2 серии)
    assert_eq!(st["stage"]["cached"], 8, "из склада и кэша: {st}");
    // цепочки: прилетевшие в IST 01.11 (прямой + виртуальный) × 4 обратных (02 и 03.11) +
    // прилетевшие 02.11 (два обычных) × 2 обратных 03.11 = 12; тот же день не стыкуется
    assert_eq!(st["summary"]["count"], 12, "{st}");
    assert_eq!(st["summary"]["combos"], 1);
    assert_eq!(st["summary"]["totalCount"], 12);

    let combos = get_json(&client, &format!("{base}/api/plan/jobs/{job_id}/combos")).await;
    assert_eq!(combos["status"], "ok");
    assert_eq!(combos["items"][0]["codes"], json!(["MOW", "IST", "MOW"]));
    assert_eq!(combos["items"][0]["minPrice"], 19500.0);
    assert_eq!(combos["items"][0]["count"], 12);
    assert_eq!(combos["cities"]["MOW"][0], "Moscow");

    let routes = get_json(&client, &format!("{base}/api/plan/jobs/{job_id}/routes?limit=3")).await;
    assert_eq!(routes["status"], "ok");
    assert_eq!(routes["count"], 12);
    assert_eq!(routes["total"], 12);
    assert_eq!(routes["items"].as_array().unwrap().len(), 3);
    // самый дешёвый: VKO→SAW→IST 02.11 (12000) + IST→VKO 03.11 (7500)
    let first = &routes["items"][0];
    assert_eq!(first["total_price"], 19500.0);
    assert_eq!(first["stops"][1]["code"], "IST");
    assert_eq!(first["segments"][0]["origin_airport"], "VKO");
    assert_eq!(first["segments"][0]["transfer_points"][0]["code"], "SAW");
    assert!(first["segments"][0]["link"].as_str().unwrap().starts_with("https://www.aviasales.ru/search/MOW0211IST1"));
    // виртуальный hidden-city рейс — с блоком hidden_city и ссылкой на реальный билет
    let all = get_json(&client, &format!("{base}/api/plan/jobs/{job_id}/routes?limit=50")).await;
    let hidden = all["items"].as_array().unwrap().iter().find(|it| it["segments"][0]["hidden_city"].is_object()).expect("hidden-city маршрут");
    assert_eq!(hidden["segments"][0]["hidden_city"]["final"], "DXB");
    assert_eq!(hidden["segments"][0]["price"], 15000.0);
    assert!(hidden["segments"][0]["link"].as_str().unwrap().starts_with("https://www.aviasales.ru/search/MOW0111DXB1"));

    let by_combo = get_json(&client, &format!("{base}/api/plan/jobs/{job_id}/routes?combos=MOW-IST-MOW&limit=100")).await;
    assert_eq!(by_combo["total"], 12);
    assert_eq!(by_combo["items"][0]["combo"], "MOW-IST-MOW");

    // другие фильтры — стыковка в фоне, потом сводка под них
    let f = urlencoding(&json!({"cities": [{}, {}, {}], "legs": [{"hiddenCity": false, "maxTransfers": 0}, {}], "tripLength": [0, null]}).to_string());
    let st2 = wait_done(&client, &format!("{base}/api/plan/jobs/{job_id}?f={f}")).await;
    assert_eq!(st2["status"], "done", "{st2}");
    // без hidden-city и с прямыми на плече 0: MOW→IST 01.11 (20000) × 4 обратных 02/03.11 + 02.11 (21000) × 2 обратных 03.11
    assert_eq!(st2["summary"]["count"], 6, "{st2}");
    let combos2 = get_json(&client, &format!("{base}/api/plan/jobs/{job_id}/combos?f={f}")).await;
    assert_eq!(combos2["items"][0]["count"], 6);
    assert_eq!(combos2["items"][0]["minPrice"], 27000.0);

    // повторный запуск тех же остановок — та же джоба
    let again: Value = client.post(format!("{base}/api/plan/run")).json(&query).send().await.unwrap().json().await.unwrap();
    assert_eq!(again["status"], "done");
    assert_eq!(again["reused"], true);
    assert_eq!(again["job_id"], job_id);

    let rescued = client.post(format!("{base}/api/jobs/rescue")).send().await.unwrap().json::<Value>().await.unwrap();
    assert_eq!(rescued["rescued"], json!([]));

    // некорректный фильтр и мало остановок
    let bad: Value = client.post(format!("{base}/api/plan/run")).json(&json!({"stops": [{"kind": "cities", "codes": ["MOW"]}]})).send().await.unwrap().json().await.unwrap();
    assert_eq!(bad["status"], "invalid");
    let bad2: Value = client.post(format!("{base}/api/plan/run")).json(&json!({"stops": [{"codes": ["MOW"]}, {"codes": ["IST"]}], "legs": [{"baggage": "x"}]})).send().await.unwrap().json().await.unwrap();
    assert_eq!(bad2["status"], "invalid");
    assert_eq!(get_json(&client, &format!("{base}/api/plan/jobs/nope")).await["status"], "not_found");

    // файл рейсов джобы лежит рядом с БД
    assert!(dir.join("plan_flights").join(format!("{job_id}.parquet")).exists());
    std::fs::remove_dir_all(&dir).ok();
}

fn urlencoding(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
