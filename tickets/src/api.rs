//! HTTP склада билетов (внутренний, порт 8002; наружу не публикуется).
//!
//! - GET  /v1/health                       — статус, размер склада, журнал файлов
//! - POST /v1/files?key=&created_at=       — файл озера (Parquet в теле) → серии склада (пуш коллектора)
//! - GET  /v1/tickets?origin=&destination=&via=&from=&to=[&limit=][&format=arrow|json]
//!        — билеты за дни вылета [from, to]: из города (X→ANY), в город (ANY→Y), пара,
//!          списки с обеих сторон (ANY→ANY между соседними плечами), через город (hidden-city).
//!          Ответ по умолчанию — Arrow IPC в схеме озера (как у коллектора), X-Tickets-Count.
//! - GET  /v1/coverage?origin=A,B&from=&to= — серии склада (город, день, fetched_at, билетов)
//! - GET  /v1/coverage/days?from=&to=       — по дням: сколько серий и билетов
//! - POST /v1/sync                          — внеочередная сверка с озером

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, NaiveDate, Utc};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::arrow_out::{ipc_from_rows, ARROW_MEDIA_TYPE};
use crate::store::{Store, TicketsQuery};
use crate::sync::Syncer;

pub struct AppState {
    pub store: Store,
    pub syncer: Option<Syncer>,
    pub max_rows: i64,
}

fn bad(msg: impl Into<String>) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"detail": msg.into()}))).into_response()
}

fn fail(msg: impl Into<String>) -> Response {
    let m: String = msg.into();
    println!("[tickets] 500: {m}");
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"detail": m}))).into_response()
}

fn codes(s: &Option<String>) -> Vec<String> {
    s.as_deref().unwrap_or("").split(',').map(|c| c.trim().to_uppercase()).filter(|c| !c.is_empty()).collect()
}

fn day(s: &str) -> Result<NaiveDate, Response> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|_| bad(format!("дата не по формату YYYY-MM-DD: {s:?}")))
}

async fn health(State(app): State<Arc<AppState>>) -> Response {
    match app.store.stats().await {
        Ok(mut st) => {
            st["status"] = json!("ok");
            Json(st).into_response()
        }
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"status": "error", "error": e}))).into_response(),
    }
}

#[derive(Deserialize)]
struct FileQuery {
    key: String,
    created_at: Option<String>,
}

async fn post_file(State(app): State<Arc<AppState>>, Query(q): Query<FileQuery>, body: Bytes) -> Response {
    let created_at = match q.created_at.as_deref() {
        Some(s) => match DateTime::parse_from_rfc3339(s) {
            Ok(t) => t.with_timezone(&Utc),
            Err(_) => return bad(format!("created_at не RFC3339: {s:?}")),
        },
        None => Utc::now(),
    };
    match app.store.apply_file(&q.key, created_at, body).await {
        Ok(st) => Json(serde_json::to_value(st).unwrap_or(Value::Null)).into_response(),
        Err(e) => fail(e),
    }
}

#[derive(Deserialize)]
struct TicketsParams {
    origin: Option<String>,
    destination: Option<String>,
    via: Option<String>,
    from: String,
    to: Option<String>,
    limit: Option<i64>,
    #[serde(default)]
    format: Option<String>,
}

async fn tickets(State(app): State<Arc<AppState>>, Query(p): Query<TicketsParams>) -> Response {
    let from = match day(&p.from) {
        Ok(d) => d,
        Err(r) => return r,
    };
    let to = match p.to.as_deref() {
        Some(s) => match day(s) {
            Ok(d) => d,
            Err(r) => return r,
        },
        None => from,
    };
    if to < from {
        return bad("to раньше from");
    }
    let q = TicketsQuery { origins: codes(&p.origin), destinations: codes(&p.destination), via: codes(&p.via), from, to, limit: Some(p.limit.unwrap_or(app.max_rows).clamp(1, app.max_rows)) };
    if q.origins.is_empty() && q.destinations.is_empty() && q.via.is_empty() {
        return bad("нужен хотя бы один из origin/destination/via");
    }
    let rows = match app.store.tickets(&q).await {
        Ok(r) => r,
        Err(e) => return fail(e),
    };
    let n = rows.len();
    if p.format.as_deref() == Some("json") {
        let items: Vec<Value> = rows.iter().map(row_json).collect();
        return Json(json!({"count": n, "tickets": items})).into_response();
    }
    let ipc = tokio::task::spawn_blocking(move || ipc_from_rows(&rows)).await.unwrap_or_default();
    ([(header::CONTENT_TYPE, ARROW_MEDIA_TYPE.to_string()), (header::HeaderName::from_static("x-tickets-count"), n.to_string())], ipc).into_response()
}

fn row_json(r: &crate::lake::Row) -> Value {
    let js = |s: &Option<String>| s.as_deref().and_then(|x| serde_json::from_str::<Value>(x).ok()).unwrap_or(Value::Array(vec![]));
    json!({
        "origin": r.origin, "destination": r.destination, "origin_airport": r.origin_airport, "destination_airport": r.destination_airport,
        "departure_at": r.departure_at, "arrival_at": r.arrival_at, "duration": r.duration, "duration_to": r.duration_to,
        "transfers": r.transfers.unwrap_or(0), "airline": r.airline, "flight_number": r.flight_number, "price": r.price,
        "currency": r.currency, "link": r.link, "chain": js(&r.chain_json), "legs": js(&r.legs_json), "transfer_points": js(&r.transfer_points_json),
        "baggage": {"known": r.baggage_known.unwrap_or(false), "included": r.baggage_included.unwrap_or(false), "pieces": r.baggage_pieces, "kg": r.baggage_kg},
        "baggage_code": r.baggage_code, "source": r.source, "search_origin": r.search_origin, "search_destination": r.search_destination, "search_date": r.search_date,
    })
}

#[derive(Deserialize)]
struct CoverageParams {
    origin: Option<String>,
    from: String,
    to: Option<String>,
}

async fn coverage(State(app): State<Arc<AppState>>, Query(p): Query<CoverageParams>) -> Response {
    let from = match day(&p.from) {
        Ok(d) => d,
        Err(r) => return r,
    };
    let to = match p.to.as_deref().map(day) {
        Some(Ok(d)) => d,
        Some(Err(r)) => return r,
        None => from,
    };
    match app.store.coverage(&codes(&p.origin), from, to).await {
        Ok(rows) => Json(json!({"series": rows, "count": rows.len()})).into_response(),
        Err(e) => fail(e),
    }
}

async fn coverage_days(State(app): State<Arc<AppState>>, Query(p): Query<CoverageParams>) -> Response {
    let from = match day(&p.from) {
        Ok(d) => d,
        Err(r) => return r,
    };
    let to = match p.to.as_deref().map(day) {
        Some(Ok(d)) => d,
        Some(Err(r)) => return r,
        None => from,
    };
    match app.store.coverage_days(from, to).await {
        Ok(rows) => Json(json!({"days": rows})).into_response(),
        Err(e) => fail(e),
    }
}

async fn sync_now(State(app): State<Arc<AppState>>) -> Response {
    let Some(s) = app.syncer.clone() else { return bad("COLLECTOR_URL не задан — сверять нечего") };
    match s.run_once(false).await {
        Ok((done, failed)) => Json(json!({"applied": done, "failed": failed})).into_response(),
        Err(e) => fail(e),
    }
}

pub fn router(app: Arc<AppState>) -> Router {
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/files", post(post_file))
        .route("/v1/tickets", get(tickets))
        .route("/v1/coverage", get(coverage))
        .route("/v1/coverage/days", get(coverage_days))
        .route("/v1/sync", post(sync_now))
        .layer(axum::extract::DefaultBodyLimit::max(512 * 1024 * 1024))
        .with_state(app)
}
