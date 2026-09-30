//! Фоновая джоба планировщика (зеркало `api.worker`): серии билетов от коллектора
//! (без COLLECTOR_URL — прямой GraphQL с кэшем серий), стыковка цепочек (search),
//! наборы городов (overview), прогресс в таблице jobs, котировки — в SQLite.
//! Выполняется в потоке однопоточного исполнителя.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use rusqlite::Connection;
use serde_json::{json, Value};

use crate::collect::{collect_plan, CollectError, CollectProgress, SeriesFetcher, SeriesResult};
use crate::collector::{collector_url, CollectorClient};
use crate::flightcols::FlightCols;
use crate::graphql::DirectFetcher;
use crate::hot;
use crate::overview::{build_overview, Overview};
use crate::planquery::PlanQuery;
use crate::search::{build_itineraries_compact, Aborted, View};
use crate::stops::{estimate_plan, parse_stops, request_count, Stop};

/// Свежесть кэша серий: источник сам отдаёт кэш цен с задержкой ~суток.
pub const FETCH_CACHE_TTL_SECONDS: f64 = 24.0 * 3600.0;
/// Сколько серий джоба тянет у коллектора одновременно; прямой GraphQL — по одной.
pub const COLLECTOR_FETCH_WORKERS: usize = 8;
/// Как часто стыковка пишет прогресс в jobs (и держит свежим updated_at).
pub const BUILD_FLUSH_SECONDS: f64 = 0.5;

/// Отмена зависших джоб — кооперативная: /api/jobs/rescue кладёт id сюда, воркер
/// проверяет флаг на каждом запросе к источнику и на каждом шаге стыковки.
#[derive(Default)]
pub struct CancelSet(Mutex<HashSet<String>>);

impl CancelSet {
    pub fn request(&self, job_id: &str) {
        self.0.lock().unwrap().insert(job_id.to_string());
    }
    pub fn is_requested(&self, job_id: &str) -> bool {
        self.0.lock().unwrap().contains(job_id)
    }
    pub fn forget(&self, job_id: &str) {
        self.0.lock().unwrap().remove(job_id);
    }
}

const PLAN_STAGES: [(&str, &str); 4] = [("queued", "В очереди"), ("fetch", "Загрузка рейсов"), ("build", "Стыковка цепочек"), ("combos", "Наборы городов")];

/// Этап свежесозданной джобы (ждёт своей очереди).
pub fn initial_stage() -> Value {
    json!({
        "key": "queued",
        "stages": PLAN_STAGES.iter().map(|(k, l)| json!({"key": k, "label": l})).collect::<Vec<_>>(),
        "step": null, "cached": 0, "flights": null, "build": null,
    })
}

struct ReporterInner {
    conn: Connection,
    state: Value,
    progress: i64,
    last_flush: Option<Instant>,
}

/// Пишет в jobs текущий этап сбора: этап, шаг загрузки (переход i из N и сколько его
/// страниц уже получено), сколько серий взято из кэша, прогресс стыковки.
pub struct StageReporter {
    job_id: String,
    steps: Vec<(String, i64)>,
    cancel: Arc<CancelSet>,
    inner: Mutex<ReporterInner>,
}

impl StageReporter {
    pub fn new(conn: Connection, job_id: &str, steps: Vec<(String, i64)>, cancel: Arc<CancelSet>) -> StageReporter {
        StageReporter { job_id: job_id.to_string(), steps, cancel, inner: Mutex::new(ReporterInner { conn, state: initial_stage(), progress: 0, last_flush: None }) }
    }

    fn flush(&self, inner: &mut ReporterInner, fields: &[(&str, Value)]) -> Result<(), CollectError> {
        if self.cancel.is_requested(&self.job_id) {
            return Err(CollectError::Cancelled);
        }
        let mut all: Vec<(&str, Value)> = vec![("stage_json", Value::String(inner.state.to_string()))];
        all.extend(fields.iter().cloned());
        hot::update_job(&inner.conn, &self.job_id, &all).map_err(CollectError::Failed)
    }

    pub fn stage(&self, key: &str, fields: &[(&str, Value)]) -> Result<(), CollectError> {
        let mut inner = self.inner.lock().unwrap();
        inner.state["key"] = json!(key);
        inner.state["step"] = Value::Null;
        self.flush(&mut inner, fields)
    }

    pub fn step(&self, index: usize) -> Result<(), CollectError> {
        let (label, requests) = self.steps.get(index).cloned().unwrap_or_default();
        let mut inner = self.inner.lock().unwrap();
        inner.state["step"] = json!({"index": index, "count": self.steps.len(), "label": label, "done": 0, "total": requests});
        self.flush(&mut inner, &[])
    }

    pub fn cache_hit(&self) {
        let mut inner = self.inner.lock().unwrap();
        let n = inner.state["cached"].as_i64().unwrap_or(0);
        inner.state["cached"] = json!(n + 1);
    }

    pub fn flights(&self, n: usize) {
        self.inner.lock().unwrap().state["flights"] = json!(n);
    }

    /// Прогресс стыковки: не чаще BUILD_FLUSH_SECONDS (и сразу на первом шаге).
    pub fn build_progress(&self, found: usize, limit: Option<i64>, explored: usize) -> Result<(), CollectError> {
        let mut inner = self.inner.lock().unwrap();
        inner.state["build"] = json!({"found": found, "limit": limit, "explored": explored});
        let due = inner.last_flush.map(|t| t.elapsed().as_secs_f64() >= BUILD_FLUSH_SECONDS).unwrap_or(true);
        if due {
            inner.last_flush = Some(Instant::now());
            self.flush(&mut inner, &[])?;
        }
        Ok(())
    }
}

impl CollectProgress for StageReporter {
    fn tick(&self) -> Result<(), CollectError> {
        let mut inner = self.inner.lock().unwrap();
        inner.progress += 1;
        if let Some(done) = inner.state["step"].get("done").and_then(|d| d.as_i64()) {
            inner.state["step"]["done"] = json!(done + 1);
        }
        let progress = inner.progress;
        self.flush(&mut inner, &[("progress", json!(progress))])
    }

    fn leg(&self, i: usize) -> Result<(), CollectError> {
        self.step(i)
    }
}

/// Обёртка источника: считает попадания в кэш (серии с cached=true).
struct CountingFetcher<'a> {
    inner: Box<dyn SeriesFetcher + 'a>,
    on_hit: &'a (dyn Fn() + Sync),
}

impl<'a> SeriesFetcher for CountingFetcher<'a> {
    fn fetch(&self, origin: Option<&str>, dest: Option<&str>, day: &str, max_pages: i64, on_page: &dyn Fn(i64, i64)) -> Result<SeriesResult, CollectError> {
        let r = self.inner.fetch(origin, dest, day, max_pages, on_page)?;
        if r.cached {
            (self.on_hit)();
        }
        Ok(r)
    }
}

/// Источник серий для джобы и сколько серий тянуть одновременно: коллектор, если
/// задан COLLECTOR_URL (прод), — параллельно; иначе прямой GraphQL — по одной.
pub fn make_fetcher(db_path: &str) -> (Box<dyn SeriesFetcher>, usize) {
    match collector_url() {
        Some(url) => (Box::new(CollectorClient::new(&url).with_ttl(FETCH_CACHE_TTL_SECONDS)), COLLECTOR_FETCH_WORKERS),
        None => (Box::new(DirectFetcher::new(db_path, FETCH_CACHE_TTL_SECONDS)), 1),
    }
}

/// Результат джобы под фильтры («вид»): компактные цепочки + наборы городов.
pub struct ViewResult {
    pub view: View,
    pub combos: Overview,
}

/// Стыковка под фильтры pq из рейсов джобы. Зовут воркер сразу после сбора и API,
/// когда те же рейсы смотрят с другими фильтрами.
pub fn build_view(stops: &[Stop], table: &FlightCols, pq: &PlanQuery, on_progress: &mut dyn FnMut(usize, usize) -> Result<(), Aborted>, on_stage: &dyn Fn(&str)) -> Result<ViewResult, Aborted> {
    let max_results = pq.max_results.unwrap_or(crate::stops::DEFAULT_MAX_RESULTS).max(1) as usize;
    let mut explored = 0usize;
    let mut check = |found: usize| {
        explored += 1;
        on_progress(found, explored)
    };
    let view = build_itineraries_compact(stops, table, max_results, pq.max_cost, Some(pq), &mut check)?;
    on_stage("combos");
    let combos = build_overview(stops, table, Some(pq));
    Ok(ViewResult { view, combos })
}

/// Джоба = сбор рейсов по остановкам запроса, рейсы — в plan_flights; пока они в
/// памяти, стыковка под фильтры запроса и вид в `on_view` (кэш API) до выставления
/// done; котировки — в SQLite уже после done.
pub fn run_plan_collection(db_path: &str, job_id: &str, pq: &PlanQuery, cancel: Arc<CancelSet>, on_view: &dyn Fn(&PlanQuery, ViewResult)) {
    let conn = match hot::connect(db_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[worker] plan job {job_id}: {e}");
            return;
        }
    };
    let outcome = run_inner(&conn, db_path, job_id, pq, cancel.clone(), on_view);
    match outcome {
        Ok(()) => {}
        Err(CollectError::Cancelled) => println!("[worker] plan job {job_id} сброшена как зависшая"),
        Err(e @ CollectError::RateLimited(_)) => {
            let msg = e.to_string();
            let _ = hot::update_job(&conn, job_id, &[("status", json!("error")), ("error", json!(msg))]);
            println!("[worker] plan job {job_id} error: {e}");
        }
        Err(CollectError::Failed(e)) => {
            let _ = hot::update_job(&conn, job_id, &[("status", json!("error")), ("error", json!(e))]);
            println!("[worker] plan job {job_id} error: {e}");
        }
    }
    cancel.forget(job_id);
}

fn run_inner(conn: &Connection, db_path: &str, job_id: &str, pq: &PlanQuery, cancel: Arc<CancelSet>, on_view: &dyn Fn(&PlanQuery, ViewResult)) -> Result<(), CollectError> {
    let stops = parse_stops(&pq.stops);
    let total = request_count(&stops);
    let steps: Vec<(String, i64)> = estimate_plan(&stops, None).legs.iter().map(|l| (format!("{} → {}", l.from_label, l.to_label), l.requests)).collect();
    let rep_conn = hot::connect(db_path).map_err(CollectError::Failed)?;
    let rep = StageReporter::new(rep_conn, job_id, steps, cancel.clone());
    rep.stage("fetch", &[("status", json!("running")), ("total", json!(total)), ("progress", json!(0))])?;

    let (inner, workers) = make_fetcher(db_path);
    let on_hit = || rep.cache_hit();
    let fetcher = CountingFetcher { inner, on_hit: &on_hit };
    let mut airport_city = hot::airport_city_map(conn).map_err(CollectError::Failed)?;
    let collected = collect_plan(&stops, &fetcher, &rep, &mut airport_city, workers)?;
    rep.flights(collected.iter().map(|v| v.len()).sum());
    let table = FlightCols::from_collected(&collected);
    hot::put_plan_flights(conn, db_path, job_id, &table).map_err(CollectError::Failed)?;
    rep.stage("build", &[])?;

    let limit = pq.max_results;
    let mut progress = |found: usize, explored: usize| rep.build_progress(found, limit, explored).map_err(|_| Aborted);
    let on_stage = |key: &str| {
        let _ = rep.stage(key, &[]);
    };
    let result = match build_view(&stops, &table, pq, &mut progress, &on_stage) {
        Ok(r) => r,
        Err(Aborted) => return Err(CollectError::Cancelled),
    };
    if cancel.is_requested(job_id) {
        return Err(CollectError::Cancelled);
    }
    let count = result.view.count;
    on_view(pq, result);
    hot::update_job(conn, job_id, &[("status", json!("done"))]).map_err(CollectError::Failed)?;
    println!("[worker] plan job {job_id} done: {count} цепочек");

    // Котировки планировщику не нужны, они копят карту аэропорт → город и статистику:
    // пишем после done, сбой тут не портит готовую джобу. Виртуальные рейсы — не котировки.
    let flights: Vec<_> = collected.iter().flatten().filter(|f| f.hidden_city.is_none()).cloned().collect();
    let mut qconn = hot::connect(db_path).map_err(CollectError::Failed)?;
    if let Err(e) = hot::upsert_quotes(&mut qconn, &flights, &crate::dates::now_iso()) {
        println!("[worker] plan job {job_id}: save quotes failed: {e}");
    }
    Ok(())
}
