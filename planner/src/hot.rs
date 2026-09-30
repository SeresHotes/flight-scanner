//! Горячее хранилище: SQLite (WAL) с котировками и джобами (та же схема и файл, что у
//! Python-планировщика: `storage/hot.py`). Рейсы джобы — Parquet-файл в
//! `plan_flights/<job>.parquet` рядом с БД (core.flightcols).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Duration;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;

use crate::collect::SeriesResult;
use crate::dates::{now_iso, now_naive, parse_naive};
use crate::flightcols::FlightCols;
use crate::ticket::Ticket;

pub fn default_db() -> String {
    std::env::var("FLIGHT_DB").unwrap_or_else(|_| "data/flights.db".into())
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS quotes (
    origin              TEXT NOT NULL,
    destination         TEXT NOT NULL,
    origin_airport      TEXT,
    dest_airport        TEXT,
    departure_at        TEXT NOT NULL,
    price               REAL,
    airline             TEXT,
    flight_number       TEXT,
    transfers           INTEGER,
    duration            INTEGER,
    link                TEXT,
    direct              INTEGER,
    observed_at         TEXT,
    search_origin       TEXT,
    search_destination  TEXT,
    search_date         TEXT,
    PRIMARY KEY (origin, destination, departure_at, airline, flight_number)
);
CREATE INDEX IF NOT EXISTS idx_quotes_route ON quotes (origin, destination, departure_at);
CREATE INDEX IF NOT EXISTS idx_quotes_observed ON quotes (observed_at);

CREATE TABLE IF NOT EXISTS jobs (
    id          TEXT PRIMARY KEY,
    params_json TEXT,
    status      TEXT,
    progress    INTEGER DEFAULT 0,
    total       INTEGER DEFAULT 0,
    result_json TEXT,
    error       TEXT,
    stage_json  TEXT,
    created_at  TEXT,
    updated_at  TEXT
);

CREATE TABLE IF NOT EXISTS plan_flights (
    job_id      TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL,
    data        BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS ticket_cache (
    origin       TEXT NOT NULL,
    destination  TEXT NOT NULL,
    search_date  TEXT NOT NULL,
    params_key   TEXT NOT NULL,
    fetched_at   TEXT NOT NULL,
    pages        INTEGER NOT NULL,
    exhausted    INTEGER NOT NULL,
    data_json    TEXT NOT NULL,
    PRIMARY KEY (origin, destination, search_date, params_key)
);
";

pub type DbResult<T> = Result<T, String>;

fn err<E: std::fmt::Display>(e: E) -> String {
    format!("sqlite: {e}")
}

pub fn connect(db_path: &str) -> DbResult<Connection> {
    if db_path != ":memory:" {
        if let Some(parent) = Path::new(db_path).parent() {
            std::fs::create_dir_all(parent).map_err(err)?;
        }
    }
    let conn = Connection::open(db_path).map_err(err)?;
    conn.busy_timeout(std::time::Duration::from_secs(10)).map_err(err)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").map_err(err)?;
    Ok(conn)
}

pub fn init_db(conn: &Connection) -> DbResult<()> {
    conn.execute_batch(SCHEMA).map_err(err)?;
    add_column_if_missing(conn, "jobs", "stage_json", "TEXT")?;
    add_column_if_missing(conn, "jobs", "query_key", "TEXT")?;
    conn.execute_batch("CREATE INDEX IF NOT EXISTS idx_jobs_query_key ON jobs (query_key)").map_err(err)?;
    Ok(())
}

fn add_column_if_missing(conn: &Connection, table: &str, column: &str, decl: &str) -> DbResult<()> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})")).map_err(err)?;
    let cols: Vec<String> = stmt.query_map([], |r| r.get::<_, String>(1)).map_err(err)?.filter_map(Result::ok).collect();
    if !cols.iter().any(|c| c == column) {
        conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} {decl}")).map_err(err)?;
    }
    Ok(())
}

// ------------------------------- quotes --------------------------------------

/// Котировки — из рейсов сбора (карта аэропорт → город, статистика). Upsert «свежайшее».
pub fn upsert_quotes(conn: &mut Connection, flights: &[Arc<Ticket>], observed_at: &str) -> DbResult<usize> {
    let sql = "INSERT INTO quotes (origin, destination, origin_airport, dest_airport, departure_at, price, airline, \
               flight_number, transfers, duration, link, direct, observed_at, search_origin, search_destination, search_date) \
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16) \
               ON CONFLICT(origin, destination, departure_at, airline, flight_number) DO UPDATE SET \
               origin_airport=excluded.origin_airport, dest_airport=excluded.dest_airport, price=excluded.price, \
               transfers=excluded.transfers, duration=excluded.duration, link=excluded.link, direct=excluded.direct, \
               observed_at=excluded.observed_at, search_origin=excluded.search_origin, \
               search_destination=excluded.search_destination, search_date=excluded.search_date \
               WHERE excluded.observed_at >= quotes.observed_at";
    let tx = conn.transaction().map_err(err)?;
    let mut n = 0;
    {
        let mut stmt = tx.prepare(sql).map_err(err)?;
        for f in flights {
            let (Some(origin), Some(dest), Some(dep)) = (f.origin_city(), f.dest_city(), f.departure_at.as_deref().filter(|s| !s.is_empty())) else { continue };
            stmt.execute(params![
                origin,
                dest,
                f.origin_airport,
                f.destination_airport,
                dep,
                f.price,
                f.airline,
                f.flight_number,
                f.transfers,
                f.duration,
                f.link,
                if f.transfers == 0 { 1 } else { 0 },
                observed_at,
                f.search_origin,
                f.search_destination,
                f.search_date,
            ])
            .map_err(err)?;
            n += 1;
        }
    }
    tx.commit().map_err(err)?;
    Ok(n)
}

pub fn count_quotes(conn: &Connection) -> DbResult<i64> {
    conn.query_row("SELECT COUNT(*) FROM quotes", [], |r| r.get(0)).map_err(err)
}

/// Аэропорт → код города по всем накопленным котировкам (PEK → BJS, ICN → SEL).
pub fn airport_city_map(conn: &Connection) -> DbResult<HashMap<String, String>> {
    let mut out = HashMap::new();
    for (apt_col, city_col) in [("origin_airport", "origin"), ("dest_airport", "destination")] {
        let mut stmt = conn
            .prepare(&format!("SELECT DISTINCT {apt_col}, {city_col} FROM quotes WHERE {apt_col} IS NOT NULL AND {city_col} IS NOT NULL"))
            .map_err(err)?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))).map_err(err)?;
        for row in rows.flatten() {
            if !row.0.is_empty() && !row.1.is_empty() {
                out.entry(row.0.to_uppercase()).or_insert_with(|| row.1.to_uppercase());
            }
        }
    }
    Ok(out)
}

// ----------------------------- plan_flights ----------------------------------

/// Джоба живёт сутки — файлы старше двух суток удаляем.
pub const PLAN_FLIGHTS_KEEP_SECONDS: i64 = 2 * 24 * 3600;

fn plan_flights_dir(db_path: &str) -> PathBuf {
    let base = if db_path == ":memory:" {
        std::env::temp_dir()
    } else {
        Path::new(db_path).parent().map(|p| p.to_path_buf()).unwrap_or_else(std::env::temp_dir)
    };
    base.join("plan_flights")
}

pub fn plan_flights_path(db_path: &str, job_id: &str) -> PathBuf {
    plan_flights_dir(db_path).join(format!("{job_id}.parquet"))
}

/// Сохраняет рейсы джобы (колонки) для видов под другие фильтры и маршрутов наборов;
/// чистит файлы старых джоб.
pub fn put_plan_flights(conn: &Connection, db_path: &str, job_id: &str, table: &FlightCols) -> DbResult<()> {
    let path = plan_flights_path(db_path, job_id);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(err)?;
    }
    let tmp = path.with_extension("parquet.tmp");
    table.write(&tmp.to_string_lossy())?;
    std::fs::rename(&tmp, &path).map_err(err)?;
    let cutoff = std::time::SystemTime::now() - std::time::Duration::from_secs(PLAN_FLIGHTS_KEEP_SECONDS as u64);
    if let Some(parent) = path.parent() {
        if let Ok(entries) = std::fs::read_dir(parent) {
            for e in entries.flatten() {
                let p = e.path();
                if p.extension().map(|x| x == "parquet").unwrap_or(false) {
                    if let Ok(meta) = e.metadata() {
                        if meta.modified().map(|m| m < cutoff).unwrap_or(false) {
                            let _ = std::fs::remove_file(&p);
                        }
                    }
                }
            }
        }
    }
    let cutoff_iso = (now_naive() - Duration::seconds(PLAN_FLIGHTS_KEEP_SECONDS)).format("%Y-%m-%dT%H:%M:%S%.6f").to_string();
    conn.execute("DELETE FROM plan_flights WHERE created_at < ?1", params![cutoff_iso]).map_err(err)?;
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

pub fn count_ticket_series(conn: &Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM ticket_cache", [], |r| r.get(0)).unwrap_or(0)
}

pub fn drop_ticket_cache(conn: &Connection) -> DbResult<()> {
    conn.execute_batch("DROP TABLE IF EXISTS ticket_cache").map_err(err)
}

fn age_seconds(fetched_at: &str) -> f64 {
    match parse_naive(fetched_at) {
        Some(dt) => (now_naive() - dt).num_milliseconds() as f64 / 1000.0,
        None => f64::INFINITY,
    }
}

/// Серия GraphQL из кэша, если свежее TTL и достаточно полная (обрезанная серия
/// годится, только если у неё не меньше min_pages страниц).
pub fn ticket_cache_get(conn: &Connection, origin: Option<&str>, dest: Option<&str>, day: &str, params_key: &str, ttl_seconds: f64, min_pages: Option<i64>) -> DbResult<Option<SeriesResult>> {
    let row: Option<(String, i64, i64, String)> = conn
        .query_row(
            "SELECT fetched_at, pages, exhausted, data_json FROM ticket_cache WHERE origin=?1 AND destination=?2 AND search_date=?3 AND params_key=?4",
            params![origin.unwrap_or(""), dest.unwrap_or(""), day, params_key],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()
        .map_err(err)?;
    let Some((fetched_at, pages, exhausted, data)) = row else { return Ok(None) };
    if age_seconds(&fetched_at) > ttl_seconds {
        return Ok(None);
    }
    if exhausted == 0 && min_pages.map(|m| pages < m).unwrap_or(false) {
        return Ok(None);
    }
    let tickets: Vec<Ticket> = serde_json::from_str(&data).unwrap_or_default();
    Ok(Some(SeriesResult { tickets, pages, exhausted: exhausted != 0, error: false, cached: true }))
}

pub fn ticket_cache_has(conn: &Connection, origin: Option<&str>, dest: Option<&str>, day: &str, params_key: &str, ttl_seconds: f64, min_pages: Option<i64>) -> bool {
    let row: Option<(String, i64, i64)> = conn
        .query_row(
            "SELECT fetched_at, pages, exhausted FROM ticket_cache WHERE origin=?1 AND destination=?2 AND search_date=?3 AND params_key=?4",
            params![origin.unwrap_or(""), dest.unwrap_or(""), day, params_key],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .unwrap_or(None);
    let Some((fetched_at, pages, exhausted)) = row else { return false };
    if age_seconds(&fetched_at) > ttl_seconds {
        return false;
    }
    exhausted != 0 || min_pages.map(|m| pages >= m).unwrap_or(true)
}

pub fn ticket_cache_put(conn: &Connection, origin: Option<&str>, dest: Option<&str>, day: &str, params_key: &str, series: &SeriesResult) -> DbResult<()> {
    let data = serde_json::to_string(&series.tickets).map_err(err)?;
    conn.execute(
        "INSERT INTO ticket_cache (origin, destination, search_date, params_key, fetched_at, pages, exhausted, data_json) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ON CONFLICT(origin, destination, search_date, params_key) DO UPDATE SET \
         fetched_at=excluded.fetched_at, pages=excluded.pages, exhausted=excluded.exhausted, data_json=excluded.data_json",
        params![origin.unwrap_or(""), dest.unwrap_or(""), day, params_key, now_iso(), series.pages, series.exhausted as i64, data],
    )
    .map_err(err)?;
    Ok(())
}

// -------------------------------- jobs ---------------------------------------

#[derive(Debug, Clone)]
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
}

fn job_from_row(r: &rusqlite::Row) -> rusqlite::Result<Job> {
    Ok(Job {
        id: r.get("id")?,
        params_json: r.get("params_json")?,
        status: r.get::<_, Option<String>>("status")?.unwrap_or_default(),
        progress: r.get::<_, Option<i64>>("progress")?.unwrap_or(0),
        total: r.get::<_, Option<i64>>("total")?.unwrap_or(0),
        error: r.get("error")?,
        stage_json: r.get("stage_json")?,
        created_at: r.get("created_at")?,
        updated_at: r.get("updated_at")?,
    })
}

pub fn create_job(conn: &Connection, job_id: &str, params: &Value, total: i64, stage: Option<&Value>, query_key: Option<&str>) -> DbResult<()> {
    let now = now_iso();
    conn.execute(
        "INSERT INTO jobs (id, params_json, status, progress, total, stage_json, query_key, created_at, updated_at) \
         VALUES (?1, ?2, 'pending', 0, ?3, ?4, ?5, ?6, ?7)",
        params![job_id, params.to_string(), total, stage.map(|s| s.to_string()), query_key, now, now],
    )
    .map_err(err)?;
    Ok(())
}

/// Свежая живая или готовая джоба с тем же ключом сбора.
pub fn find_job_by_key(conn: &Connection, query_key: &str, ttl_seconds: i64) -> DbResult<Option<Job>> {
    let cutoff = (now_naive() - Duration::seconds(ttl_seconds)).format("%Y-%m-%dT%H:%M:%S%.6f").to_string();
    conn.query_row(
        "SELECT * FROM jobs WHERE query_key=?1 AND status IN ('pending', 'running', 'done') AND created_at >= ?2 ORDER BY created_at DESC LIMIT 1",
        params![query_key, cutoff],
        job_from_row,
    )
    .optional()
    .map_err(err)
}

/// Обновление полей джобы: status, progress, total, error, stage_json (+ updated_at).
pub fn update_job(conn: &Connection, job_id: &str, fields: &[(&str, Value)]) -> DbResult<()> {
    if fields.is_empty() {
        return Ok(());
    }
    let mut sets: Vec<String> = Vec::new();
    let mut vals: Vec<rusqlite::types::Value> = Vec::new();
    for (k, v) in fields {
        sets.push(format!("{k}=?"));
        vals.push(match v {
            Value::Null => rusqlite::types::Value::Null,
            Value::Bool(b) => rusqlite::types::Value::Integer(*b as i64),
            Value::Number(n) => n.as_i64().map(rusqlite::types::Value::Integer).unwrap_or_else(|| rusqlite::types::Value::Real(n.as_f64().unwrap_or(0.0))),
            Value::String(s) => rusqlite::types::Value::Text(s.clone()),
            other => rusqlite::types::Value::Text(other.to_string()),
        });
    }
    sets.push("updated_at=?".into());
    vals.push(rusqlite::types::Value::Text(now_iso()));
    vals.push(rusqlite::types::Value::Text(job_id.to_string()));
    let sql = format!("UPDATE jobs SET {} WHERE id=?", sets.join(", "));
    conn.execute(&sql, rusqlite::params_from_iter(vals)).map_err(err)?;
    Ok(())
}

pub fn get_job(conn: &Connection, job_id: &str) -> DbResult<Option<Job>> {
    conn.query_row("SELECT * FROM jobs WHERE id=?1", params![job_id], job_from_row).optional().map_err(err)
}

/// id джоб в running, которые не обновлялись дольше idle_seconds.
pub fn find_hung_jobs(conn: &Connection, idle_seconds: i64) -> DbResult<Vec<String>> {
    let cutoff = (now_naive() - Duration::seconds(idle_seconds)).format("%Y-%m-%dT%H:%M:%S%.6f").to_string();
    let mut stmt = conn.prepare("SELECT id FROM jobs WHERE status='running' AND updated_at < ?1").map_err(err)?;
    let rows = stmt.query_map(params![cutoff], |r| r.get::<_, String>(0)).map_err(err)?;
    Ok(rows.flatten().collect())
}

/// Помечает зависшие джобы (pending/running) как error — при старте сервера.
pub fn fail_stale_jobs(conn: &Connection, error: &str) -> DbResult<usize> {
    conn.execute("UPDATE jobs SET status='error', error=?1, updated_at=?2 WHERE status IN ('pending', 'running')", params![error, now_iso()])
        .map_err(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jobs_roundtrip() {
        let conn = connect(":memory:").unwrap();
        init_db(&conn).unwrap();
        create_job(&conn, "abc", &serde_json::json!({"kind": "plan"}), 10, Some(&serde_json::json!({"key": "queued"})), Some("k1")).unwrap();
        let job = get_job(&conn, "abc").unwrap().unwrap();
        assert_eq!(job.status, "pending");
        assert_eq!(job.total, 10);
        assert!(find_job_by_key(&conn, "k1", 3600).unwrap().is_some());
        update_job(&conn, "abc", &[("status", serde_json::json!("running")), ("progress", serde_json::json!(3))]).unwrap();
        assert_eq!(get_job(&conn, "abc").unwrap().unwrap().progress, 3);
        assert!(find_hung_jobs(&conn, 60).unwrap().is_empty());
        assert_eq!(fail_stale_jobs(&conn, "рестарт").unwrap(), 1);
        assert!(find_job_by_key(&conn, "k1", 3600).unwrap().is_none());
    }

    #[test]
    fn quotes_and_cache() {
        let mut conn = connect(":memory:").unwrap();
        init_db(&conn).unwrap();
        let t = Arc::new(Ticket { origin: Some("MOW".into()), destination: Some("SEL".into()), origin_airport: Some("SVO".into()), destination_airport: Some("ICN".into()), departure_at: Some("2026-10-15T08:00:00".into()), price: Some(100.0), ..Default::default() });
        assert_eq!(upsert_quotes(&mut conn, &[t.clone()], "2026-09-30T00:00:00").unwrap(), 1);
        assert_eq!(count_quotes(&conn).unwrap(), 1);
        assert_eq!(airport_city_map(&conn).unwrap().get("ICN").map(String::as_str), Some("SEL"));
        let series = SeriesResult { tickets: vec![(*t).clone()], pages: 1, exhausted: true, error: false, cached: false };
        ticket_cache_put(&conn, Some("MOW"), None, "2026-10-15", "", &series).unwrap();
        assert!(ticket_cache_has(&conn, Some("MOW"), None, "2026-10-15", "", 3600.0, Some(1)));
        assert!(!ticket_cache_has(&conn, Some("MOW"), None, "2026-10-15", "", -1.0, None));
        let got = ticket_cache_get(&conn, Some("MOW"), None, "2026-10-15", "", 3600.0, Some(12)).unwrap().unwrap();
        assert!(got.cached && got.tickets.len() == 1);
        assert_eq!(count_ticket_series(&conn), 1);
    }
}
