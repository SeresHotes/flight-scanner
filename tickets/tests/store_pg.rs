//! Интеграционный тест склада на живом Postgres: TICKETS_TEST_PG_URL=postgres://… (без
//! переменной тест пропускается). Файл окна → серии; повтор не дублирует; более свежая
//! серия заменяет, более старая — нет; выборки X→ANY / ANY→Y / пара / списки / via;
//! покрытие; ретеншн; HTTP /v1/tickets отдаёт Arrow IPC в схеме озера.

use std::sync::Arc;

use bytes::Bytes;
use chrono::{NaiveDate, TimeZone, Utc};
use flights_tickets::api::{router, AppState};
use flights_tickets::arrow_out::parquet_from_rows;
use flights_tickets::lake::{rows_from_batch, Row};
use flights_tickets::store::{Store, TicketsQuery};

fn row(origin: &str, dest: &str, day: &str, price: f64, via: &[&str]) -> Row {
    let pts: Vec<serde_json::Value> = via.iter().map(|c| serde_json::json!({"code": c, "to": c, "minutes": 120})).collect();
    let mut chain = vec![format!("{origin}A")];
    chain.extend(via.iter().map(|c| c.to_string()));
    chain.push(format!("{dest}A"));
    Row {
        search_origin: Some(origin.into()),
        search_destination: Some(String::new()),
        search_date: Some(day.into()),
        origin: Some(origin.into()),
        destination: Some(dest.into()),
        origin_airport: Some(format!("{origin}A")),
        destination_airport: Some(format!("{dest}A")),
        departure_at: Some(format!("{day}T10:00:00+03:00")),
        arrival_at: Some(format!("{day}T16:00:00+03:00")),
        duration: Some(360),
        transfers: Some(via.len() as i16),
        price: Some(price),
        currency: Some("rub".into()),
        link: Some(format!("/search/{origin}0101{dest}1?t=x")),
        chain_json: Some(serde_json::to_string(&chain).unwrap()),
        legs_json: Some("[]".into()),
        transfer_points_json: Some(serde_json::to_string(&pts).unwrap()),
        baggage_known: Some(true),
        baggage_included: Some(price > 1000.0),
        source: Some("test".into()),
        ..Default::default()
    }
}

fn key(origin: &str, day: &str, day_to: Option<&str>, stamp: &str) -> String {
    let days = match day_to {
        Some(t) => format!("{day}..{t}"),
        None => day.to_string(),
    };
    format!("tickets/fetched=2026-09-30/origin={origin}/{origin}-ANY__{days}__{stamp}Z.parquet")
}

#[tokio::test]
async fn store_roundtrip() {
    let Ok(url) = std::env::var("TICKETS_TEST_PG_URL") else {
        eprintln!("TICKETS_TEST_PG_URL не задан — тест пропущен");
        return;
    };
    let store = Store::connect(&url, 4).unwrap();
    {
        let cfg: tokio_postgres::Config = url.parse().unwrap();
        let (c, conn) = cfg.connect(tokio_postgres::NoTls).await.unwrap();
        tokio::spawn(async move { let _ = conn.await; });
        c.batch_execute("DROP TABLE IF EXISTS tickets, series, lake_files, airport_city").await.unwrap();
    }
    store.migrate().await.unwrap();

    // окно MOW 01..03.10: день 02 пустой
    let rows = vec![
        row("MOW", "IST", "2026-10-01", 20000.0, &[]),
        row("MOW", "DXB", "2026-10-01", 15000.0, &["IST"]),
        row("MOW", "SEL", "2026-10-03", 40000.0, &["PEK"]),
    ];
    let k1 = key("MOW", "2026-10-01", Some("2026-10-03"), "10-00-00");
    let t1 = Utc.with_ymd_and_hms(2026, 9, 30, 10, 0, 0).unwrap();
    let st = store.apply_file(&k1, t1, Bytes::from(parquet_from_rows(&rows))).await.unwrap();
    assert_eq!((st.days, st.rows, st.skipped_days, st.already), (3, 3, 0, false));
    let cov = store.coverage(&["MOW".into()], NaiveDate::from_ymd_opt(2026, 10, 1).unwrap(), NaiveDate::from_ymd_opt(2026, 10, 5).unwrap()).await.unwrap();
    assert_eq!(cov.iter().map(|c| (c.day.as_str(), c.tickets)).collect::<Vec<_>>(), vec![("2026-10-01", 2), ("2026-10-02", 0), ("2026-10-03", 1)]);

    // повтор того же файла — no-op, без дублей
    let again = store.apply_file(&k1, t1, Bytes::from(parquet_from_rows(&rows))).await.unwrap();
    assert!(again.already);
    let q = |origins: &[&str], dests: &[&str], via: &[&str]| TicketsQuery {
        origins: origins.iter().map(|s| s.to_string()).collect(),
        destinations: dests.iter().map(|s| s.to_string()).collect(),
        via: via.iter().map(|s| s.to_string()).collect(),
        from: NaiveDate::from_ymd_opt(2026, 10, 1).unwrap(),
        to: NaiveDate::from_ymd_opt(2026, 10, 7).unwrap(),
        limit: None,
    };
    assert_eq!(store.tickets(&q(&["MOW"], &[], &[])).await.unwrap().len(), 3);

    // другой город и более свежая серия того же дня MOW 01.10: заменяет целиком
    let k2 = key("LED", "2026-10-01", None, "11-00-00");
    store.apply_file(&k2, Utc.with_ymd_and_hms(2026, 9, 30, 11, 0, 0).unwrap(), Bytes::from(parquet_from_rows(&[row("LED", "IST", "2026-10-01", 9000.0, &[])]))).await.unwrap();
    let k3 = key("MOW", "2026-10-01", None, "12-00-00");
    let st3 = store.apply_file(&k3, Utc.with_ymd_and_hms(2026, 9, 30, 12, 0, 0).unwrap(), Bytes::from(parquet_from_rows(&[row("MOW", "IST", "2026-10-01", 21000.0, &["SVX"])]))).await.unwrap();
    assert_eq!((st3.days, st3.rows), (1, 1));
    let mow = store.tickets(&q(&["MOW"], &[], &[])).await.unwrap();
    assert_eq!(mow.len(), 2, "01.10 заменён одним билетом + 03.10");
    assert_eq!(mow[0].price, Some(21000.0));
    // более старый файл того же дня — пропускается
    let k_old = key("MOW", "2026-10-01", None, "09-00-00");
    let st_old = store.apply_file(&k_old, Utc.with_ymd_and_hms(2026, 9, 30, 9, 0, 0).unwrap(), Bytes::from(parquet_from_rows(&[row("MOW", "IST", "2026-10-01", 1.0, &[])]))).await.unwrap();
    assert_eq!((st_old.days, st_old.skipped_days, st_old.rows), (0, 1, 0));
    assert_eq!(store.tickets(&q(&["MOW"], &[], &[])).await.unwrap().len(), 2);

    // ANY→IST: MOW и LED; пара LED→IST; списки с обеих сторон; через город SVX (аэропорт из карты)
    let ist = store.tickets(&q(&[], &["IST"], &[])).await.unwrap();
    assert_eq!(ist.iter().map(|r| r.origin.clone().unwrap()).collect::<Vec<_>>(), vec!["LED", "MOW"]);
    assert_eq!(store.tickets(&q(&["LED"], &["IST"], &[])).await.unwrap().len(), 1);
    assert_eq!(store.tickets(&q(&["LED", "MOW"], &["IST", "SEL"], &[])).await.unwrap().len(), 3);
    let via = store.tickets(&q(&[], &[], &["SVX"])).await.unwrap();
    assert_eq!(via.len(), 1);
    assert_eq!(via[0].destination.as_deref(), Some("IST"));
    // via по городу расширяется до аэропортов: PEK известен как аэропорт? нет — в карте только *A;
    // проверяем карту аэропорт → город: MOWA → MOW
    assert!(store.codes_of_city("MOW").await.unwrap().contains(&"MOWA".to_string()));
    assert!(store.tickets(&TicketsQuery { from: NaiveDate::from_ymd_opt(2026, 10, 1).unwrap(), to: NaiveDate::from_ymd_opt(2026, 10, 1).unwrap(), ..Default::default() }).await.is_err());

    // файл ANY→Y и файл с параметрами в склад не идут, но помечаются применёнными
    let k_any = "tickets/fetched=2026-09-30/origin=ANY/ANY-MOW__2026-10-01__13-00-00Z.parquet";
    let st_any = store.apply_file(k_any, Utc::now(), Bytes::from(parquet_from_rows(&[row("LED", "MOW", "2026-10-01", 5.0, &[])]))).await.unwrap();
    assert!(st_any.already);
    assert_eq!(store.applied_files(&[k_any.to_string()]).await.unwrap().len(), 1);

    // дни по покрытию и ретеншн
    let days = store.coverage_days(NaiveDate::from_ymd_opt(2026, 10, 1).unwrap(), NaiveDate::from_ymd_opt(2026, 10, 3).unwrap()).await.unwrap();
    assert_eq!(days.iter().map(|d| (d.day.as_str(), d.series, d.tickets)).collect::<Vec<_>>(), vec![("2026-10-01", 2, 2), ("2026-10-02", 1, 0), ("2026-10-03", 1, 1)]);
    let (t, s) = store.retention(NaiveDate::from_ymd_opt(2026, 10, 3).unwrap()).await.unwrap();
    assert_eq!((t, s), (2, 3));
    assert_eq!(store.tickets(&q(&["MOW", "LED"], &[], &[])).await.unwrap().len(), 1);
    let stats = store.stats().await.unwrap();
    assert_eq!(stats["series"], 1);
    assert_eq!(stats["tickets"], 1);

    // HTTP: Arrow IPC в схеме озера + счётчик; json-формат; покрытие
    let app = Arc::new(AppState { store: store.clone(), syncer: None, max_rows: 1000 });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router(app)).await.unwrap() });
    let client = reqwest::Client::new();
    let r = client.get(format!("http://{addr}/v1/tickets?origin=mow&from=2026-10-01&to=2026-10-07")).send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers().get("x-tickets-count").unwrap(), "1");
    assert!(r.headers().get("content-type").unwrap().to_str().unwrap().starts_with("application/vnd.apache.arrow.stream"));
    let bytes = r.bytes().await.unwrap();
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes.to_vec()), None).unwrap();
    let back: Vec<Row> = reader.map(|b| rows_from_batch(&b.unwrap())).flatten().collect();
    assert_eq!(back.len(), 1);
    assert_eq!(back[0].destination.as_deref(), Some("SEL"));
    assert_eq!(back[0].transfer_points_json.as_deref(), Some(r#"[{"code":"PEK","minutes":120,"to":"PEK"}]"#));
    let j: serde_json::Value = client.get(format!("http://{addr}/v1/tickets?destination=SEL&from=2026-10-03&format=json")).send().await.unwrap().json().await.unwrap();
    assert_eq!(j["count"], 1);
    assert_eq!(j["tickets"][0]["transfer_points"][0]["code"], "PEK");
    let bad = client.get(format!("http://{addr}/v1/tickets?from=2026-10-03")).send().await.unwrap();
    assert_eq!(bad.status(), 400);
    let cov: serde_json::Value = client.get(format!("http://{addr}/v1/coverage?origin=MOW&from=2026-10-01&to=2026-10-09")).send().await.unwrap().json().await.unwrap();
    assert_eq!(cov["count"], 1);
    let h: serde_json::Value = client.get(format!("http://{addr}/v1/health")).send().await.unwrap().json().await.unwrap();
    assert_eq!(h["status"], "ok");
    // пуш файла через HTTP
    let k4 = key("KZN", "2026-10-05", None, "14-00-00");
    let r = client.post(format!("http://{addr}/v1/files?key={}&created_at=2026-09-30T14:00:00Z", urlencoding(&k4))).body(parquet_from_rows(&[row("KZN", "IST", "2026-10-05", 7000.0, &[])])).send().await.unwrap();
    assert_eq!(r.status(), 200, "{}", r.text().await.unwrap());
    let cov: serde_json::Value = client.get(format!("http://{addr}/v1/coverage/days?from=2026-10-05")).send().await.unwrap().json().await.unwrap();
    assert_eq!(cov["days"][0]["tickets"], 1);
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
