//! Горячее состояние планировщика — в памяти, без БД: джобы (статус, этап, параметры) и
//! кэш серий прямого режима GraphQL. Джоба при создании и смене статуса пишется маленьким
//! JSON-файлом `plan_jobs/<job>.json` (ссылки на результаты переживают деплой), рейсы
//! джобы — Parquet `plan_flights/<job>.parquet`; оба — рядом с `FLIGHT_DB` (только как
//! якорь каталога данных; SQLite с 02.10.2026 нет). Склад билетов — `lakestore`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use chrono::Duration;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::collect::SeriesResult;
use crate::dates::{now_iso, now_naive, parse_naive};
use crate::flightcols::FlightCols;
use crate::ticket::Ticket;

/// Якорь каталога данных: файлы джоб, рейсов и снапшот склада — рядом с этим путём.
pub fn default_db() -> String {
    std::env::var("FLIGHT_DB").unwrap_or_else(|_| "data/flights.db".into())
}

pub type DbResult<T> = Result<T, String>;

fn err<E: std::fmt::Display>(e: E) -> String {
    e.to_string()
}

fn data_dir(db_path: &str) -> PathBuf {
    if db_path == ":memory:" {
        return std::env::temp_dir();
    }
    Path::new(db_path).parent().map(|p| p.to_path_buf()).filter(|p| !p.as_os_str().is_empty()).unwrap_or_else(|| PathBuf::from("."))
}

/// Карта аэропорт → город без склада (локально, без озера): пустая — сбор дополняет её
/// концами билетов. Со складом — `lakestore::airport_city_map`.
pub fn airport_city_map_cached(_db_path: &str) -> Arc<HashMap<String, String>> {
    static EMPTY: OnceLock<Arc<HashMap<String, String>>> = OnceLock::new();
    EMPTY.get_or_init(|| Arc::new(HashMap::new())).clone()
}

// ----------------------------- plan_flights ----------------------------------

/// Джоба живёт сутки — файлы старше двух суток удаляем.
pub const PLAN_FLIGHTS_KEEP_SECONDS: i64 = 2 * 24 * 3600;

fn plan_flights_dir(db_path: &str) -> PathBuf {
    data_dir(db_path).join("plan_flights")
}

pub fn plan_flights_path(db_path: &str, job_id: &str) -> PathBuf {
    plan_flights_dir(db_path).join(format!("{job_id}.parquet"))
}

/// Удаляет из каталога файлы с расширением ext старше PLAN_FLIGHTS_KEEP_SECONDS.
fn prune_dir(dir: &Path, ext: &str) {
    let cutoff = std::time::SystemTime::now() - std::time::Duration::from_secs(PLAN_FLIGHTS_KEEP_SECONDS as u64);
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for e in entries.flatten() {
        let p = e.path();
        if p.extension().map(|x| x == ext).unwrap_or(false) && e.metadata().and_then(|m| m.modified()).map(|m| m < cutoff).unwrap_or(false) {
            let _ = std::fs::remove_file(&p);
        }
    }
}

/// Сохраняет рейсы джобы (колонки) для видов под другие фильтры и маршрутов наборов;
/// чистит файлы старых джоб.
pub fn put_plan_flights(db_path: &str, job_id: &str, table: &FlightCols) -> DbResult<()> {
    let path = plan_flights_path(db_path, job_id);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(err)?;
    }
    let tmp = path.with_extension("parquet.tmp");
    table.write(&tmp.to_string_lossy())?;
    std::fs::rename(&tmp, &path).map_err(err)?;
    if let Some(parent) = path.parent() {
        prune_dir(parent, "parquet");
    }
    Ok(())
}

/// Рейсы джобы колонками или None (файла нет).
pub fn get_plan_flights(db_path: &str, job_id: &str) -> DbResult<Option<FlightCols>> {
    let path = plan_flights_path(db_path, job_id);
    if !path.exists() {
        return Ok(None);
    }
    FlightCols::read(&path.to_string_lossy()).map(Some)
}

// ----------------------------- ticket cache ----------------------------------

/// Кэш серий прямого режима GraphQL (без коллектора) — в памяти процесса.
type SeriesKey = (String, String, String, String);
struct CachedSeries {
    at: Instant,
    pages: i64,
    exhausted: bool,
    tickets: Vec<Ticket>,
}

fn ticket_cache() -> &'static Mutex<HashMap<SeriesKey, CachedSeries>> {
    static CACHE: OnceLock<Mutex<HashMap<SeriesKey, CachedSeries>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn series_key(origin: Option<&str>, dest: Option<&str>, day: &str, params_key: &str) -> SeriesKey {
    (origin.unwrap_or("").to_string(), dest.unwrap_or("").to_string(), day.to_string(), params_key.to_string())
}

pub fn count_ticket_series() -> i64 {
    ticket_cache().lock().unwrap().len() as i64
}

fn fresh(c: &CachedSeries, ttl_seconds: f64, min_pages: Option<i64>) -> bool {
    c.at.elapsed().as_secs_f64() <= ttl_seconds && (c.exhausted || min_pages.map(|m| c.pages >= m).unwrap_or(true))
}

/// Серия GraphQL из кэша, если свежее TTL и достаточно полная (обрезанная серия
/// годится, только если у неё не меньше min_pages страниц).
pub fn ticket_cache_get(origin: Option<&str>, dest: Option<&str>, day: &str, params_key: &str, ttl_seconds: f64, min_pages: Option<i64>) -> Option<SeriesResult> {
    let cache = ticket_cache().lock().unwrap();
    let c = cache.get(&series_key(origin, dest, day, params_key)).filter(|c| fresh(c, ttl_seconds, min_pages))?;
    Some(SeriesResult { tickets: c.tickets.clone(), pages: c.pages, exhausted: c.exhausted, error: false, cached: true })
}

pub fn ticket_cache_has(origin: Option<&str>, dest: Option<&str>, day: &str, params_key: &str, ttl_seconds: f64, min_pages: Option<i64>) -> bool {
    ticket_cache().lock().unwrap().get(&series_key(origin, dest, day, params_key)).map(|c| fresh(c, ttl_seconds, min_pages)).unwrap_or(false)
}

pub fn ticket_cache_put(origin: Option<&str>, dest: Option<&str>, day: &str, params_key: &str, series: &SeriesResult) {
    let mut cache = ticket_cache().lock().unwrap();
    // сутки на серию — хватает и для локальной работы; старше — вон
    cache.retain(|_, c| c.at.elapsed().as_secs() < 24 * 3600);
    cache.insert(series_key(origin, dest, day, params_key), CachedSeries { at: Instant::now(), pages: series.pages, exhausted: series.exhausted, tickets: series.tickets.clone() });
}

// -------------------------------- jobs ---------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub params_json: Option<String>,
    pub status: String,
    pub progress: i64,
    pub total: i64,
    pub error: Option<String>,
    pub stage_json: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    #[serde(default)]
    pub query_key: Option<String>,
}

/// Джобы каталога данных: в памяти, на диске — по JSON-файлу на джобу.
struct JobStore {
    dir: PathBuf,
    jobs: Mutex<HashMap<String, Job>>,
    /// Сколько джоб при загрузке были в pending/running (прерваны рестартом).
    stale: usize,
}

const STALE_ERROR: &str = "прервана рестартом сервера";

impl JobStore {
    fn load(dir: PathBuf) -> JobStore {
        prune_dir(&dir, "json");
        let mut jobs = HashMap::new();
        let mut stale = 0;
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.extension().map(|x| x != "json").unwrap_or(true) {
                    continue;
                }
                let Some(mut job) = std::fs::read_to_string(&p).ok().and_then(|t| serde_json::from_str::<Job>(&t).ok()) else { continue };
                if job.status == "pending" || job.status == "running" {
                    job.status = "error".into();
                    job.error = Some(STALE_ERROR.into());
                    job.updated_at = Some(now_iso());
                    stale += 1;
                    let _ = write_job(&dir, &job);
                }
                jobs.insert(job.id.clone(), job);
            }
        }
        JobStore { dir, jobs: Mutex::new(jobs), stale }
    }
}

fn write_job(dir: &Path, job: &Job) -> DbResult<()> {
    std::fs::create_dir_all(dir).map_err(err)?;
    let path = dir.join(format!("{}.json", job.id));
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec(job).map_err(err)?).map_err(err)?;
    std::fs::rename(&tmp, &path).map_err(err)
}

/// Хранилище джоб каталога данных db_path (одно на каталог — тесты держат свои).
fn store(db_path: &str) -> Arc<JobStore> {
    static STORES: OnceLock<Mutex<HashMap<PathBuf, Arc<JobStore>>>> = OnceLock::new();
    let dir = data_dir(db_path).join("plan_jobs");
    let mut all = STORES.get_or_init(|| Mutex::new(HashMap::new())).lock().unwrap();
    all.entry(dir.clone()).or_insert_with(|| Arc::new(JobStore::load(dir))).clone()
}

pub fn create_job(db_path: &str, job_id: &str, params: &Value, total: i64, stage: Option<&Value>, query_key: Option<&str>) -> DbResult<()> {
    let s = store(db_path);
    let now = now_iso();
    let job = Job {
        id: job_id.to_string(),
        params_json: Some(params.to_string()),
        status: "pending".into(),
        progress: 0,
        total,
        error: None,
        stage_json: stage.map(|v| v.to_string()),
        created_at: Some(now.clone()),
        updated_at: Some(now),
        query_key: query_key.map(String::from),
    };
    write_job(&s.dir, &job)?;
    s.jobs.lock().unwrap().insert(job_id.to_string(), job);
    Ok(())
}

fn older_than(iso: Option<&str>, seconds: i64) -> bool {
    let cutoff = now_naive() - Duration::seconds(seconds);
    iso.and_then(parse_naive).map(|t| t < cutoff).unwrap_or(true)
}

/// Свежая живая или готовая джоба с тем же ключом сбора.
pub fn find_job_by_key(db_path: &str, query_key: &str, ttl_seconds: i64) -> Option<Job> {
    let s = store(db_path);
    let jobs = s.jobs.lock().unwrap();
    jobs.values()
        .filter(|j| j.query_key.as_deref() == Some(query_key) && matches!(j.status.as_str(), "pending" | "running" | "done") && !older_than(j.created_at.as_deref(), ttl_seconds))
        .max_by(|a, b| a.created_at.cmp(&b.created_at))
        .cloned()
}

/// Обновление полей джобы: status, progress, total, error, stage_json (+ updated_at).
/// На диск — только при смене статуса (прогресс и этап живут в памяти).
pub fn update_job(db_path: &str, job_id: &str, fields: &[(&str, Value)]) -> DbResult<()> {
    if fields.is_empty() {
        return Ok(());
    }
    let s = store(db_path);
    let mut jobs = s.jobs.lock().unwrap();
    let Some(job) = jobs.get_mut(job_id) else { return Ok(()) };
    let mut persist = false;
    for (k, v) in fields {
        let text = || match v {
            Value::Null => None,
            Value::String(s) => Some(s.clone()),
            other => Some(other.to_string()),
        };
        match *k {
            "status" => {
                job.status = text().unwrap_or_default();
                persist = true;
            }
            "progress" => job.progress = v.as_i64().unwrap_or(job.progress),
            "total" => job.total = v.as_i64().unwrap_or(job.total),
            "error" => job.error = text(),
            "stage_json" => job.stage_json = text(),
            "params_json" => job.params_json = text(),
            _ => {}
        }
    }
    job.updated_at = Some(now_iso());
    if persist {
        write_job(&s.dir, job)?;
    }
    Ok(())
}

pub fn get_job(db_path: &str, job_id: &str) -> Option<Job> {
    store(db_path).jobs.lock().unwrap().get(job_id).cloned()
}

/// id джоб в running, которые не обновлялись дольше idle_seconds.
pub fn find_hung_jobs(db_path: &str, idle_seconds: i64) -> Vec<String> {
    let s = store(db_path);
    let jobs = s.jobs.lock().unwrap();
    jobs.values().filter(|j| j.status == "running" && older_than(j.updated_at.as_deref(), idle_seconds)).map(|j| j.id.clone()).collect()
}

/// Сколько джоб было прервано прошлым рестартом (помечены error при загрузке).
pub fn fail_stale_jobs(db_path: &str) -> usize {
    store(db_path).stale
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jobs_roundtrip_and_restart() {
        let dir = std::env::temp_dir().join(format!("hot-{}", uuid::Uuid::new_v4()));
        let db = dir.join("flights.db").to_string_lossy().to_string();
        create_job(&db, "abc", &serde_json::json!({"kind": "plan"}), 10, Some(&serde_json::json!({"key": "queued"})), Some("k1")).unwrap();
        let job = get_job(&db, "abc").unwrap();
        assert_eq!(job.status, "pending");
        assert_eq!(job.total, 10);
        assert!(find_job_by_key(&db, "k1", 3600).is_some());
        update_job(&db, "abc", &[("status", serde_json::json!("running")), ("progress", serde_json::json!(3))]).unwrap();
        assert_eq!(get_job(&db, "abc").unwrap().progress, 3);
        assert!(find_hung_jobs(&db, 60).is_empty());
        // «рестарт»: новое хранилище из файлов — running становится error
        let reloaded = JobStore::load(data_dir(&db).join("plan_jobs"));
        assert_eq!(reloaded.stale, 1);
        let j = reloaded.jobs.lock().unwrap().get("abc").cloned().unwrap();
        assert_eq!((j.status.as_str(), j.error.as_deref()), ("error", Some(STALE_ERROR)));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn series_cache() {
        let t = Ticket { origin: Some("MOW".into()), destination: Some("SEL".into()), price: Some(100.0), ..Default::default() };
        let series = SeriesResult { tickets: vec![t], pages: 1, exhausted: true, error: false, cached: false };
        ticket_cache_put(Some("MOW"), None, "2026-10-15", "hot-test", &series);
        assert!(ticket_cache_has(Some("MOW"), None, "2026-10-15", "hot-test", 3600.0, Some(1)));
        assert!(!ticket_cache_has(Some("MOW"), None, "2026-10-15", "hot-test", -1.0, None));
        let got = ticket_cache_get(Some("MOW"), None, "2026-10-15", "hot-test", 3600.0, Some(12)).unwrap();
        assert!(got.cached && got.tickets.len() == 1);
    }
}
