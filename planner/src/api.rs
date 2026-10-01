//! HTTP-приложение планировщика v2 (зеркало `api/main.py`, docs/PLANNER_V2.md).
//!
//! - GET  /api/health                         — статус, котировки, серии в кэше
//! - GET  /api/airports?q=&limit=             — автокомплит городов/аэропортов
//! - POST /api/plan/estimate                  — оценка объёма сбора
//! - POST /api/plan/run                       — запуск джобы по PlanQuery (дедуп по хэшу)
//! - GET  /api/plan/jobs/{id}                 — прогресс + сводка
//! - GET  /api/plan/jobs/{id}/combos|routes   — страницы наборов городов / маршрутов
//! - POST /api/jobs/rescue                    — сброс зависших джоб
//! - GET  /api/dynamics?origin&destination&from&to&history — история цен направления из озера

use std::collections::HashMap;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};

use axum::extract::{Path, Query, State};
use axum::http::HeaderValue;
use axum::routing::{get, post};
use axum::{Json, Router};
use rusqlite::Connection;
use serde::Deserialize;
use serde_json::{json, Value};
use tower_http::compression::predicate::SizeAbove;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::airports::search_airports;
use crate::collector::{collector_url, CollectorClient};
use crate::hot::{self, Job};
use crate::planquery::PlanQuery;
use crate::search::{build_combo_views, Aborted, ComboRoutes};
use crate::collect::store_view;
use crate::stops::{estimate_plan, is_valid_max_results, parse_stops, total_window_days, Estimate, Series, DEFAULT_MAX_RESULTS, MAX_REQUESTS, MAX_RESULTS, MAX_SEARCH_STEPS, MAX_TOTAL_WINDOW_DAYS};
use crate::worker::{self, CancelSet, ViewResult, FETCH_CACHE_TTL_SECONDS};

/// Сколько джоба в running может молчать, прежде чем /jobs/rescue сочтёт её зависшей.
pub const HUNG_JOB_SECONDS: i64 = 60;
pub const RESCUED_JOB_ERROR: &str = "Сбор завис и был сброшен. Попробуйте сузить маршрут или даты.";
/// Готовая джоба переиспользуется, пока свежи серии.
pub const PLAN_JOB_TTL_SECONDS: i64 = 24 * 3600;
pub const PLAN_VIEW_CACHE_SIZE: usize = 4;

type Task = Box<dyn FnOnce() + Send + 'static>;

/// Однопоточный исполнитель (как ThreadPoolExecutor(max_workers=1)).
pub struct Executor {
    tx: Mutex<Sender<Task>>,
}

impl Executor {
    pub fn new(name: &str) -> Executor {
        let (tx, rx) = channel::<Task>();
        std::thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                while let Ok(task) = rx.recv() {
                    task();
                }
            })
            .expect("executor thread");
        Executor { tx: Mutex::new(tx) }
    }

    pub fn submit(&self, task: Task) {
        let _ = self.tx.lock().unwrap().send(task);
    }
}

/// Готовый вид джобы: результат под фильтры + кэш маршрутов выбранных наборов.
pub struct ViewEntry {
    pub result: ViewResult,
    pub query: PlanQuery,
    pub combo_routes: Mutex<Vec<(String, Arc<ComboRoutes>)>>,
}

/// Строящийся вид: этап, прогресс стыковки, ошибка.
#[derive(Default)]
pub struct ViewState {
    pub stage: String,
    pub build: Option<Value>,
    pub error: Option<String>,
}

pub struct AppState {
    pub db_path: String,
    pub conn: Mutex<Connection>,
    pub cancel: Arc<CancelSet>,
    pub jobs_lock: Mutex<()>,
    pub executor: Executor,
    pub view_executor: Executor,
    /// LRU видов по (джоба, view_key): последний использованный — в конце.
    pub views: Mutex<Vec<((String, String), Arc<ViewEntry>)>>,
    pub view_state: Mutex<HashMap<(String, String), Arc<Mutex<ViewState>>>>,
}

impl AppState {
    pub fn new(db_path: &str) -> Result<Arc<AppState>, String> {
        let conn = hot::connect(db_path)?;
        hot::init_db(&conn)?;
        Ok(Arc::new(AppState {
            db_path: db_path.to_string(),
            conn: Mutex::new(conn),
            cancel: Arc::new(CancelSet::default()),
            jobs_lock: Mutex::new(()),
            executor: Executor::new("plan-worker"),
            view_executor: Executor::new("view-builder"),
            views: Mutex::new(Vec::new()),
            view_state: Mutex::new(HashMap::new()),
        }))
    }

    fn collector(&self) -> Option<CollectorClient> {
        collector_url().map(|u| CollectorClient::new(&u))
    }

    /// Кладёт готовый вид в кэш (вытесняя самый старый) и снимает состояние стройки.
    pub fn put_view(&self, job_id: &str, pq: &PlanQuery, result: ViewResult) {
        let key = (job_id.to_string(), pq.view_key());
        let entry = Arc::new(ViewEntry { result, query: pq.clone(), combo_routes: Mutex::new(Vec::new()) });
        let mut views = self.views.lock().unwrap();
        views.retain(|(k, _)| *k != key);
        views.push((key.clone(), entry));
        while views.len() > PLAN_VIEW_CACHE_SIZE {
            views.remove(0);
        }
        self.view_state.lock().unwrap().remove(&key);
    }

    fn get_view(&self, key: &(String, String)) -> Option<Arc<ViewEntry>> {
        let mut views = self.views.lock().unwrap();
        let pos = views.iter().position(|(k, _)| k == key)?;
        let item = views.remove(pos);
        let entry = item.1.clone();
        views.push(item);
        Some(entry)
    }

    /// (готовый вид, None) или (None, состояние стыковки) — тогда стыковка идёт/запущена.
    fn view(self: &Arc<Self>, job: &Job, pq: &PlanQuery) -> (Option<Arc<ViewEntry>>, Option<Arc<Mutex<ViewState>>>) {
        let key = (job.id.clone(), pq.view_key());
        if let Some(entry) = self.get_view(&key) {
            return (Some(entry), None);
        }
        let (state, start) = {
            let mut states = self.view_state.lock().unwrap();
            match states.get(&key) {
                Some(s) => (s.clone(), false),
                None => {
                    let s = Arc::new(Mutex::new(ViewState { stage: "build".into(), build: None, error: None }));
                    states.insert(key.clone(), s.clone());
                    (s, true)
                }
            }
        };
        if start {
            let app = self.clone();
            let job_id = job.id.clone();
            let pq = pq.clone();
            let st = state.clone();
            self.view_executor.submit(Box::new(move || build_view_task(app, job_id, pq, st)));
        }
        match self.get_view(&key) {
            Some(entry) => (Some(entry), None),
            None => (None, Some(state)),
        }
    }
}

/// Стыковка под фильтры из сохранённых рейсов джобы (фон, view_executor).
fn build_view_task(app: Arc<AppState>, job_id: String, pq: PlanQuery, state: Arc<Mutex<ViewState>>) {
    let outcome = (|| -> Result<ViewResult, String> {
        let table = hot::get_plan_flights(&app.db_path, &job_id)?.ok_or("Рейсы этого поиска не сохранились — запустите поиск заново.")?;
        let stops = parse_stops(&pq.stops);
        let limit = pq.max_results;
        let mut progress = |found: usize, explored: usize| -> Result<(), Aborted> {
            state.lock().unwrap().build = Some(json!({"found": found, "limit": limit, "explored": explored}));
            if explored > MAX_SEARCH_STEPS { Err(Aborted) } else { Ok(()) }
        };
        let on_stage = |key: &str| state.lock().unwrap().stage = key.to_string();
        worker::build_view(&stops, &table, &pq, &mut progress, &on_stage).map_err(|_| worker::STEP_LIMIT_ERROR.to_string())
    })();
    match outcome {
        Ok(result) => app.put_view(&job_id, &pq, result),
        Err(e) => {
            println!("[view] {job_id}: {e}");
            state.lock().unwrap().error = Some(e);
        }
    }
}

// --------------------------------- health ------------------------------------

async fn health(State(app): State<Arc<AppState>>) -> Json<Value> {
    let out = tokio::task::spawn_blocking(move || {
        let (quotes, series) = {
            let conn = app.conn.lock().unwrap();
            (hot::count_quotes(&conn).unwrap_or(0), hot::count_ticket_series(&conn))
        };
        let mut out = json!({"status": "ok", "quotes": quotes, "ticket_series": series});
        if let Some(client) = app.collector() {
            out["collector"] = match client.health() {
                Ok(v) => v,
                Err(e) => json!({"status": "unreachable", "error": e.to_string()}),
            };
        }
        if let Some(store) = crate::lakestore::global() {
            out["tickets"] = store.health();
        }
        out["process"] = process_memory();
        out
    })
    .await
    .unwrap_or_else(|e| json!({"status": "error", "error": e.to_string()}));
    Json(out)
}

/// Память процесса из /proc/self/status: текущая (VmRSS) и пиковая (VmHWM), МБ.
fn process_memory() -> Value {
    let text = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let kb = |name: &str| text.lines().find(|l| l.starts_with(name)).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse::<u64>().ok());
    json!({"rss_mb": kb("VmRSS:").map(|k| k / 1024), "peak_mb": kb("VmHWM:").map(|k| k / 1024)})
}

// -------------------------------- airports -----------------------------------

#[derive(Deserialize)]
struct AirportsQuery {
    #[serde(default)]
    q: String,
    #[serde(default = "default_airport_limit")]
    limit: usize,
}

fn default_airport_limit() -> usize {
    10
}

async fn airports(Query(q): Query<AirportsQuery>) -> Json<Value> {
    let items = tokio::task::spawn_blocking(move || search_airports(&q.q, q.limit)).await.unwrap_or_default();
    Json(json!({"airports": items}))
}

// -------------------------------- dynamics -----------------------------------

#[derive(Deserialize)]
struct DynamicsParams {
    #[serde(default)]
    origin: String,
    #[serde(default)]
    destination: String,
    #[serde(default)]
    from: String,
    #[serde(default)]
    to: String,
    #[serde(default)]
    history: Option<i64>,
}

/// История цен направления по снимкам озера (`dynamics`): рейсы и цены «рейс × снимок».
async fn dynamics(State(app): State<Arc<AppState>>, Query(p): Query<DynamicsParams>) -> Json<Value> {
    use crate::dynamics::{DynamicsQuery, DEFAULT_HISTORY_DAYS, MAX_DAYS, MAX_HISTORY_DAYS};
    let origin = p.origin.trim().to_uppercase();
    let destination = p.destination.trim().to_uppercase();
    let day = |s: &str| chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").ok();
    let (Some(from), to) = (day(&p.from), day(&p.to)) else {
        return Json(json!({"error": "Нужна дата вылета (from=ГГГГ-ММ-ДД)."}));
    };
    let to = to.unwrap_or(from);
    if origin.is_empty() || destination.is_empty() || origin == destination {
        return Json(json!({"error": "Нужны два разных города: откуда и куда."}));
    }
    if to < from || (to - from).num_days() >= MAX_DAYS {
        return Json(json!({"error": format!("Дни вылета: от 1 до {MAX_DAYS} подряд.")}));
    }
    let history_days = p.history.unwrap_or(DEFAULT_HISTORY_DAYS).clamp(1, MAX_HISTORY_DAYS);
    let out = tokio::task::spawn_blocking(move || {
        // серии озера лежат по городу вылета: аэропорт → его город (по накопленным котировкам)
        let origin_city = {
            let conn = app.conn.lock().unwrap();
            hot::airport_city_map(&conn).ok().and_then(|m| m.get(&origin).cloned()).unwrap_or_else(|| origin.clone())
        };
        crate::dynamics::handle(DynamicsQuery { origin, origin_city, destination, from, to, history_days })
    })
    .await
    .unwrap_or_else(|e| json!({"error": e.to_string()}));
    Json(out)
}

// -------------------------------- rescue -------------------------------------

async fn rescue_jobs(State(app): State<Arc<AppState>>) -> Json<Value> {
    let hung = tokio::task::spawn_blocking(move || {
        let _guard = app.jobs_lock.lock().unwrap();
        let conn = app.conn.lock().unwrap();
        let hung = hot::find_hung_jobs(&conn, HUNG_JOB_SECONDS).unwrap_or_default();
        for job_id in &hung {
            app.cancel.request(job_id);
            let _ = hot::update_job(&conn, job_id, &[("status", json!("error")), ("error", json!(RESCUED_JOB_ERROR))]);
        }
        if !hung.is_empty() {
            println!("[rescue] сброшены зависшие джобы: {}", hung.join(", "));
        }
        hung
    })
    .await
    .unwrap_or_default();
    Json(json!({"status": "ok", "rescued": hung}))
}

// ------------------------- планировщик цепочек A→B→C --------------------------

fn job_stage(job: &Job) -> Value {
    job.stage_json.as_deref().and_then(|s| serde_json::from_str(s).ok()).unwrap_or(Value::Null)
}

/// Оценка объёма: что уже есть в складе билетов (одно покрытие на запрос: X→ANY города × дни,
/// ANY-плечи — дни хотя бы с одной серией), остальное — проба кэша серий у коллектора (озеро)
/// или, без коллектора, в локальном ticket_cache.
fn estimate(app: &AppState, query: &PlanQuery) -> Estimate {
    let stops = parse_stops(&query.stops);
    // склад ещё грузится — оценка без него (не ждём загрузку в HTTP-запросе)
    let store = crate::lakestore::global().filter(|s| s.is_ready()).map(crate::lakestore::SharedStore);
    let view = store_view(store.as_ref().map(|s| s as &dyn crate::tickets::TicketStore), &stops);
    let client = app.collector();
    let probe = |s: &Series| -> bool {
        if let Some(v) = &view {
            let covered = match (&s.origin, &s.dest) {
                (Some(o), _) => v.coverage.has_series(o, &s.day),
                (None, _) => v.coverage.has_day(&s.day),
            };
            if covered {
                return true;
            }
        }
        if s.origin.is_none() && s.dest.is_none() {
            return false; // любой → любой — только склад
        }
        match &client {
            Some(c) => c.has_series(s.origin.as_deref(), s.dest.as_deref(), &s.day, "", Some(s.pages), Some(FETCH_CACHE_TTL_SECONDS)).unwrap_or(false),
            None => {
                let conn = app.conn.lock().unwrap();
                hot::ticket_cache_has(&conn, s.origin.as_deref(), s.dest.as_deref(), &s.day, "", FETCH_CACHE_TTL_SECONDS, Some(s.pages))
            }
        }
    };
    let mut est = estimate_plan(&stops, Some(&probe));
    if view.is_some() {
        est.source = "tickets".into();
    }
    est
}

async fn plan_estimate(State(app): State<Arc<AppState>>, Json(body): Json<Value>) -> Json<Value> {
    let out = tokio::task::spawn_blocking(move || match PlanQuery::from_value(&body) {
        Ok(q) => serde_json::to_value(estimate(&app, &q)).unwrap_or(Value::Null),
        Err(e) => json!({"status": "invalid", "message": format!("Некорректный фильтр: {e}")}),
    })
    .await
    .unwrap_or(Value::Null);
    Json(out)
}

/// Общий запуск сбора: проверки, дедуп по остановкам (collect_key), постановка в очередь.
fn start_plan_job(app: &Arc<AppState>, mut query: PlanQuery, fresh: bool) -> Value {
    let stops = parse_stops(&query.stops);
    if stops.len() < 2 {
        return json!({"status": "invalid", "message": "Нужно минимум две остановки."});
    }
    if query.max_results.is_none() {
        query.max_results = Some(DEFAULT_MAX_RESULTS);
    }
    if !is_valid_max_results(query.max_results) {
        return json!({"status": "invalid", "message": format!("Лимит маршрутов вне диапазона 1…{MAX_RESULTS}.")});
    }
    let days = total_window_days(&stops);
    if days > MAX_TOTAL_WINDOW_DAYS {
        return json!({"status": "too_wide", "message": format!("Суммарная ширина окон дат — {days} дн., максимум {MAX_TOTAL_WINDOW_DAYS}. Сузьте диапазоны.")});
    }
    let est = estimate(app, &query);
    if est.requests == 0 {
        return json!({"status": "invalid", "message": "Задайте окна дат для остановок."});
    }
    if est.cold > MAX_REQUESTS {
        return json!({
            "status": "too_wide",
            "message": format!("Слишком широкие окна: ~{} страниц надо загрузить из источника (лимит {MAX_REQUESTS}, в кэше уже {}). Сузьте диапазоны дат.", est.cold, est.cached),
            "estimate": est,
        });
    }
    let key = query.collect_key();
    // fresh — новая джоба даже при готовой с тем же ключом (замеры, scripts/prod_bench.py)
    let existing = if fresh { None } else {
        let conn = app.conn.lock().unwrap();
        hot::find_job_by_key(&conn, &key, PLAN_JOB_TTL_SECONDS).unwrap_or(None)
    };
    if let Some(job) = existing {
        return json!({
            "status": if job.status != "done" { "collecting" } else { "done" },
            "job_id": job.id, "total": job.total, "mode": query.mode(), "reused": true,
        });
    }
    let job_id = uuid::Uuid::new_v4().simple().to_string()[..12].to_string();
    let mut payload = query.as_value();
    payload["kind"] = json!("plan");
    payload["max_results"] = json!(query.max_results);
    {
        let conn = app.conn.lock().unwrap();
        if let Err(e) = hot::create_job(&conn, &job_id, &payload, est.requests, Some(&worker::initial_stage()), Some(&key)) {
            return json!({"status": "error", "message": e});
        }
    }
    let app2 = app.clone();
    let jid = job_id.clone();
    let q = query.clone();
    app.executor.submit(Box::new(move || {
        let on_view = |pq: &PlanQuery, result: ViewResult| app2.put_view(&jid, pq, result);
        worker::run_plan_collection(&app2.db_path, &jid, &q, app2.cancel.clone(), &on_view);
    }));
    json!({"status": "collecting", "job_id": job_id, "total": est.requests, "mode": query.mode()})
}

async fn plan_run(State(app): State<Arc<AppState>>, Json(body): Json<Value>) -> Json<Value> {
    let out = tokio::task::spawn_blocking(move || match PlanQuery::from_value(&body) {
        Ok(q) => start_plan_job(&app, q, body.get("fresh").and_then(|v| v.as_bool()).unwrap_or(false)),
        Err(e) => json!({"status": "invalid", "message": format!("Некорректный фильтр: {e}")}),
    })
    .await
    .unwrap_or(Value::Null);
    Json(out)
}

/// Запрос вида: остановки джобы + фильтры из f (JSON); без f — фильтры запуска.
fn job_query(job: &Job, f: Option<&str>) -> Result<PlanQuery, String> {
    let params: Value = serde_json::from_str(job.params_json.as_deref().unwrap_or("{}")).map_err(|e| e.to_string())?;
    let mut base = PlanQuery::from_value(&params)?;
    if base.max_results.is_none() {
        base.max_results = Some(DEFAULT_MAX_RESULTS);
    }
    let Some(f) = f.filter(|s| !s.is_empty()) else { return Ok(base) };
    let filters: Value = serde_json::from_str(f).map_err(|e| e.to_string())?;
    let mut q = base.with_filters(&filters)?;
    if !is_valid_max_results(q.max_results) {
        q.max_results = base.max_results;
    }
    Ok(q)
}

/// Готовый вид для страниц /combos и /routes (None — ещё строится/нет джобы).
fn job_view(app: &Arc<AppState>, job_id: &str, f: Option<&str>) -> Option<Arc<ViewEntry>> {
    let job = {
        let conn = app.conn.lock().unwrap();
        hot::get_job(&conn, job_id).ok().flatten()?
    };
    if job.status != "done" {
        return None;
    }
    let pq = job_query(&job, f).ok()?;
    app.view(&job, &pq).0
}

#[derive(Deserialize)]
struct CombosQuery {
    #[serde(default)]
    offset: i64,
    #[serde(default = "default_combos_limit")]
    limit: i64,
    #[serde(default = "default_sort")]
    sort: String,
    f: Option<String>,
}

fn default_combos_limit() -> i64 {
    100
}
fn default_sort() -> String {
    "price".into()
}

async fn plan_job_combos(State(app): State<Arc<AppState>>, Path(job_id): Path<String>, Query(q): Query<CombosQuery>) -> Json<Value> {
    let out = tokio::task::spawn_blocking(move || {
        let Some(entry) = job_view(&app, &job_id, q.f.as_deref()) else { return json!({"status": "not_ready"}) };
        let overview = &entry.result.combos;
        let limit = q.limit.clamp(1, 500) as usize;
        let offset = q.offset.max(0) as usize;
        if overview.combos.is_empty() {
            return json!({"status": "ok", "total": 0, "totalCount": 0, "truncated": false, "offset": offset, "limit": limit, "items": [], "cities": {}});
        }
        let mut combos: Vec<&crate::overview::Combo> = overview.combos.iter().collect();
        match q.sort.as_str() {
            "count" => combos.sort_by(|a, b| b.count.cmp(&a.count).then(a.min_price.partial_cmp(&b.min_price).unwrap_or(std::cmp::Ordering::Equal))),
            "transfers" => combos.sort_by(|a, b| a.transfers_at_min.cmp(&b.transfers_at_min).then(a.min_price.partial_cmp(&b.min_price).unwrap_or(std::cmp::Ordering::Equal))),
            _ => {}
        }
        let end = combos.len().min(offset + limit);
        let page: Vec<&crate::overview::Combo> = combos[offset.min(end)..end].to_vec();
        let mut cities = serde_json::Map::new();
        for c in &page {
            for code in &c.codes {
                if !cities.contains_key(code) {
                    let (name, flag) = overview.cities.get(code).cloned().unwrap_or_else(|| (code.clone(), String::new()));
                    cities.insert(code.clone(), json!([name, flag]));
                }
            }
        }
        json!({"status": "ok", "total": combos.len(), "totalCount": overview.total_count, "truncated": overview.truncated, "incomplete": overview.incomplete, "offset": offset, "limit": limit, "items": page, "cities": cities})
    })
    .await
    .unwrap_or(Value::Null);
    Json(out)
}

#[derive(Deserialize)]
struct RoutesQuery {
    #[serde(default)]
    offset: i64,
    #[serde(default = "default_routes_limit")]
    limit: i64,
    combos: Option<String>,
    f: Option<String>,
}

fn default_routes_limit() -> i64 {
    50
}

/// Маршруты выбранных наборов — по требованию из сохранённых рейсов джобы с фильтрами
/// вида, кэш по набору ключей в записи вида.
fn combo_routes(app: &AppState, job_id: &str, entry: &ViewEntry, wanted: &[String]) -> Option<Arc<ComboRoutes>> {
    let mut keys: Vec<String> = wanted.to_vec();
    keys.sort();
    keys.dedup();
    let key = keys.join(",");
    if let Some((_, items)) = entry.combo_routes.lock().unwrap().iter().find(|(k, _)| *k == key) {
        return Some(items.clone());
    }
    let table = hot::get_plan_flights(&app.db_path, job_id).ok().flatten()?;
    let stops = parse_stops(&entry.query.stops);
    let combos: Vec<Vec<String>> = keys.iter().map(|c| c.split('-').map(|s| s.to_string()).collect()).collect();
    let items = Arc::new(build_combo_views(&stops, &table, &combos, Some(&entry.query)));
    let mut cache = entry.combo_routes.lock().unwrap();
    if cache.len() >= 8 {
        cache.remove(0);
    }
    cache.push((key, items.clone()));
    Some(items)
}

async fn plan_job_routes(State(app): State<Arc<AppState>>, Path(job_id): Path<String>, Query(q): Query<RoutesQuery>) -> Json<Value> {
    let out = tokio::task::spawn_blocking(move || {
        let Some(entry) = job_view(&app, &job_id, q.f.as_deref()) else { return json!({"status": "not_ready"}) };
        let limit = q.limit.clamp(1, 200) as usize;
        let offset = q.offset.max(0) as usize;
        let wanted: Vec<String> = q.combos.as_deref().unwrap_or("").split(',').map(|c| c.trim().to_uppercase()).filter(|c| !c.is_empty()).collect();
        if wanted.is_empty() {
            let mut page = entry.result.view.routes_page(offset, limit);
            page["status"] = json!("ok");
            page["count"] = json!(entry.result.view.count);
            return page;
        }
        let Some(items) = combo_routes(&app, &job_id, &entry, &wanted) else { return json!({"status": "not_ready"}) };
        json!({"status": "ok", "count": items.len(), "total": items.len(), "offset": offset, "limit": limit, "items": items.page(offset, limit)})
    })
    .await
    .unwrap_or(Value::Null);
    Json(out)
}

#[derive(Deserialize)]
struct StatusQuery {
    f: Option<String>,
}

/// Прогресс джобы: сбор рейсов, затем стыковка под фильтры f. По готовности — сводка.
async fn plan_job_status(State(app): State<Arc<AppState>>, Path(job_id): Path<String>, Query(q): Query<StatusQuery>) -> Json<Value> {
    let out = tokio::task::spawn_blocking(move || {
        let job = {
            let conn = app.conn.lock().unwrap();
            hot::get_job(&conn, &job_id).ok().flatten()
        };
        let Some(job) = job else { return json!({"status": "not_found"}) };
        let mut out = json!({"status": job.status, "progress": job.progress, "total": job.total, "error": job.error, "stage": job_stage(&job)});
        if job.status != "done" {
            return out;
        }
        let pq = match job_query(&job, q.f.as_deref()) {
            Ok(pq) => pq,
            Err(e) => {
                out["status"] = json!("error");
                out["error"] = json!(format!("Некорректные фильтры: {e}"));
                return out;
            }
        };
        let (entry, state) = app.view(&job, &pq);
        if let Some(entry) = entry {
            out["summary"] = json!({
                "count": entry.result.view.count,
                "combos": entry.result.combos.combos.len(),
                "totalCount": entry.result.combos.total_count,
                "truncated": entry.result.combos.truncated,
                "incomplete": entry.result.combos.incomplete,
            });
            return out;
        }
        let st = state.expect("view state");
        let (stage_key, build, error) = {
            let s = st.lock().unwrap();
            (s.stage.clone(), s.build.clone(), s.error.clone())
        };
        if let Some(err) = error {
            app.view_state.lock().unwrap().remove(&(job_id.clone(), pq.view_key()));
            out["status"] = json!("error");
            out["error"] = json!(err);
            return out;
        }
        let mut stage = if out["stage"].is_object() { out["stage"].clone() } else { worker::initial_stage() };
        stage["key"] = json!(stage_key);
        stage["step"] = Value::Null;
        stage["build"] = build.unwrap_or(Value::Null);
        out["status"] = json!("running");
        out["progress"] = json!(job.total);
        out["stage"] = stage;
        out
    })
    .await
    .unwrap_or(Value::Null);
    Json(out)
}

/// CORS: фронт может ходить с другого origin (vite dev/preview, прямой :8000).
fn is_local_origin(origin: &HeaderValue) -> bool {
    let Ok(s) = origin.to_str() else { return false };
    let rest = s.strip_prefix("http://").or_else(|| s.strip_prefix("https://"));
    let Some(rest) = rest else { return false };
    let (host, port) = match rest.split_once(':') {
        Some((h, p)) => (h, Some(p)),
        None => (rest, None),
    };
    (host == "localhost" || host == "127.0.0.1") && port.map(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit())).unwrap_or(true)
}

pub fn router(app: Arc<AppState>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(|origin, _| is_local_origin(origin)))
        .allow_methods(tower_http::cors::Any)
        .allow_headers(tower_http::cors::Any);
    Router::new()
        .route("/api/health", get(health))
        .route("/api/airports", get(airports))
        .route("/api/plan/estimate", post(plan_estimate))
        .route("/api/plan/run", post(plan_run))
        .route("/api/plan/gather", post(plan_run))
        .route("/api/plan/jobs/{job_id}", get(plan_job_status))
        .route("/api/plan/jobs/{job_id}/combos", get(plan_job_combos))
        .route("/api/plan/jobs/{job_id}/routes", get(plan_job_routes))
        .route("/api/jobs/rescue", post(rescue_jobs))
        .route("/api/dynamics", get(dynamics))
        .layer(CompressionLayer::new().gzip(true).compress_when(SizeAbove::new(1024)))
        .layer(cors)
        .with_state(app)
}
