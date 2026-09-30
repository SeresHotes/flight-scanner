//! Postgres-склад: текущее состояние серий «город вылета × день» (одна строка на билет),
//! замена серии целиком в одной транзакции, выборки для планировщика, журнал применённых
//! файлов озера, ретеншн прошедших дней.

use std::collections::{BTreeMap, HashSet};

use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use futures_util::pin_mut;
use serde::Serialize;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::NoTls;

use crate::lake::{parse_file_key, read_parquet, Row};

pub type DbResult<T> = Result<T, String>;

fn err<E: std::fmt::Display>(e: E) -> String {
    e.to_string()
}

/// Колонки таблицы tickets в схеме озера (порядок — как в collector/lake.SCHEMA).
pub const LAKE_COLS: [&str; 28] = [
    "series_id", "observed_at", "search_origin", "search_destination", "search_date", "origin", "destination", "origin_airport", "destination_airport", "departure_at", "arrival_at", "duration", "duration_to", "transfers", "airline", "flight_number", "price", "currency", "link", "chain_json", "legs_json", "transfer_points_json", "baggage_code", "baggage_known", "baggage_included", "baggage_pieces", "baggage_kg", "source",
];

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS series (
    origin      text NOT NULL,
    dep_day     date NOT NULL,
    fetched_at  timestamptz NOT NULL,
    tickets     integer NOT NULL,
    file_key    text NOT NULL,
    PRIMARY KEY (origin, dep_day)
);
CREATE INDEX IF NOT EXISTS series_day ON series (dep_day);

CREATE TABLE IF NOT EXISTS lake_files (
    key         text PRIMARY KEY,
    created_at  timestamptz NOT NULL,
    applied_at  timestamptz NOT NULL DEFAULT now(),
    rows        integer NOT NULL
);
CREATE INDEX IF NOT EXISTS lake_files_created ON lake_files (created_at);

CREATE TABLE IF NOT EXISTS airport_city (
    airport     text PRIMARY KEY,
    city        text NOT NULL
);
CREATE INDEX IF NOT EXISTS airport_city_city ON airport_city (city);

CREATE TABLE IF NOT EXISTS tickets (
    dep_day              date NOT NULL,
    via                  text[] NOT NULL DEFAULT '{}',
    series_id            bigint,
    observed_at          text,
    search_origin        text NOT NULL,
    search_destination   text,
    search_date          text,
    origin               text,
    destination          text,
    origin_airport       text,
    destination_airport  text,
    departure_at         text,
    arrival_at           text,
    duration             integer,
    duration_to          integer,
    transfers            smallint,
    airline              text,
    flight_number        text,
    price                double precision,
    currency             text,
    link                 text,
    chain_json           text,
    legs_json            text,
    transfer_points_json text,
    baggage_code         text,
    baggage_known        boolean,
    baggage_included     boolean,
    baggage_pieces       smallint,
    baggage_kg           smallint,
    source               text
);
CREATE INDEX IF NOT EXISTS tickets_series ON tickets (search_origin, dep_day);
CREATE INDEX IF NOT EXISTS tickets_origin_day ON tickets (origin, dep_day);
CREATE INDEX IF NOT EXISTS tickets_dest_day ON tickets (destination, dep_day);
CREATE INDEX IF NOT EXISTS tickets_day ON tickets (dep_day);
CREATE INDEX IF NOT EXISTS tickets_via ON tickets USING gin (via);
"#;

#[derive(Clone)]
pub struct Store {
    pool: Pool,
}

#[derive(Debug, Default, Serialize, Clone, PartialEq)]
pub struct ApplyStats {
    pub key: String,
    pub days: usize,
    pub rows: usize,
    pub skipped_days: usize,
    /// Файл уже применён с тем же created_at — ничего не делали.
    pub already: bool,
}

#[derive(Debug, Default, Clone)]
pub struct TicketsQuery {
    pub origins: Vec<String>,
    pub destinations: Vec<String>,
    /// Город/аэропорт среди промежуточных: расширяется до аэропортов города.
    pub via: Vec<String>,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct SeriesRow {
    pub origin: String,
    pub day: String,
    pub fetched_at: String,
    pub tickets: i32,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct DayRow {
    pub day: String,
    pub series: i64,
    pub tickets: i64,
}

impl Store {
    pub fn connect(url: &str, max_size: usize) -> DbResult<Store> {
        let cfg: tokio_postgres::Config = url.parse().map_err(|e| format!("TICKETS_PG_URL: {e}"))?;
        let mgr = Manager::from_config(cfg, NoTls, ManagerConfig { recycling_method: RecyclingMethod::Fast });
        let pool = Pool::builder(mgr).max_size(max_size).build().map_err(err)?;
        Ok(Store { pool })
    }

    async fn client(&self) -> DbResult<deadpool_postgres::Object> {
        self.pool.get().await.map_err(|e| format!("postgres pool: {e}"))
    }

    pub async fn migrate(&self) -> DbResult<()> {
        let c = self.client().await?;
        c.batch_execute(SCHEMA).await.map_err(err)
    }

    pub async fn ping(&self) -> DbResult<()> {
        let c = self.client().await?;
        c.simple_query("SELECT 1").await.map(|_| ()).map_err(err)
    }

    /// Файлы озера, уже применённые с этим created_at.
    pub async fn applied_files(&self, keys: &[String]) -> DbResult<BTreeMap<String, DateTime<Utc>>> {
        let c = self.client().await?;
        let rows = c.query("SELECT key, created_at FROM lake_files WHERE key = ANY($1)", &[&keys]).await.map_err(err)?;
        Ok(rows.into_iter().map(|r| (r.get::<_, String>(0), r.get::<_, DateTime<Utc>>(1))).collect())
    }

    /// Самый поздний created_at применённых файлов (с него продолжает сверка).
    pub async fn last_applied(&self) -> DbResult<Option<DateTime<Utc>>> {
        let c = self.client().await?;
        let row = c.query_one("SELECT max(created_at) FROM lake_files", &[]).await.map_err(err)?;
        Ok(row.get(0))
    }

    /// Файл озера → серии склада. Одна транзакция: по каждому дню окна — если в складе
    /// серия не новее, удалить её билеты, вставить новые (COPY BINARY), обновить строку
    /// серии; пустые дни окна тоже становятся сериями (0 билетов — день покрыт).
    /// Повторное применение того же файла — no-op по содержимому (те же строки).
    pub async fn apply_file(&self, key: &str, created_at: DateTime<Utc>, data: Bytes) -> DbResult<ApplyStats> {
        let meta = parse_file_key(key).ok_or_else(|| format!("ключ файла не по схеме озера: {key}"))?;
        // Склад = серии X→ANY без параметров (покрытие всех городов краулером). Файлы
        // ANY→Y, A→B и с ценовыми коридорами только отмечаем в журнале, чтобы сверка их не
        // перебирала снова.
        let Some(origin) = meta.origin.clone().filter(|_| meta.destination.is_none() && meta.params_key.is_empty()) else {
            let c = self.client().await?;
            c.execute(
                "INSERT INTO lake_files (key, created_at, applied_at, rows) VALUES ($1, $2, now(), 0) \
                 ON CONFLICT (key) DO UPDATE SET created_at = excluded.created_at, applied_at = now()",
                &[&key, &created_at],
            )
            .await
            .map_err(err)?;
            return Ok(ApplyStats { key: key.into(), already: true, ..Default::default() });
        };
        let rows = tokio::task::spawn_blocking(move || read_parquet(data)).await.map_err(err)??;
        let mut by_day: BTreeMap<NaiveDate, Vec<Row>> = meta.days().into_iter().map(|d| (d, Vec::new())).collect();
        for r in rows {
            let Some(d) = r.dep_day() else { continue };
            by_day.entry(d).or_default().push(r);
        }
        let fetched = meta.observed;

        let mut c = self.client().await?;
        let tx = c.transaction().await.map_err(err)?;
        if let Some(r) = tx.query_opt("SELECT created_at FROM lake_files WHERE key = $1", &[&key]).await.map_err(err)? {
            let prev: DateTime<Utc> = r.get(0);
            if prev == created_at {
                tx.commit().await.map_err(err)?;
                return Ok(ApplyStats { key: key.into(), already: true, ..Default::default() });
            }
        }
        let mut stats = ApplyStats { key: key.into(), ..Default::default() };
        let mut to_copy: Vec<Row> = Vec::new();
        let mut airports: HashSet<(String, String)> = HashSet::new();
        for (day, day_rows) in &by_day {
            let cur = tx.query_opt("SELECT fetched_at FROM series WHERE origin = $1 AND dep_day = $2 FOR UPDATE", &[&origin, day]).await.map_err(err)?;
            if let Some(r) = cur {
                let have: DateTime<Utc> = r.get(0);
                if have > fetched {
                    stats.skipped_days += 1;
                    continue; // в складе уже более свежая серия этого дня
                }
            }
            tx.execute("DELETE FROM tickets WHERE search_origin = $1 AND dep_day = $2", &[&origin, day]).await.map_err(err)?;
            tx.execute(
                "INSERT INTO series (origin, dep_day, fetched_at, tickets, file_key) VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (origin, dep_day) DO UPDATE SET fetched_at = excluded.fetched_at, tickets = excluded.tickets, file_key = excluded.file_key",
                &[&origin, day, &fetched, &(day_rows.len() as i32), &key],
            )
            .await
            .map_err(err)?;
            stats.days += 1;
            for r in day_rows {
                for (apt, city) in [(&r.origin_airport, &r.origin), (&r.destination_airport, &r.destination)] {
                    if let (Some(a), Some(c)) = (apt.as_deref().filter(|s| !s.is_empty()), city.as_deref().filter(|s| !s.is_empty())) {
                        if a != c {
                            airports.insert((a.to_uppercase(), c.to_uppercase()));
                        }
                    }
                }
            }
            to_copy.extend(day_rows.iter().cloned());
        }
        if !to_copy.is_empty() {
            copy_rows(&tx, &origin, &to_copy).await?;
            stats.rows = to_copy.len();
        }
        if !airports.is_empty() {
            let (a, c): (Vec<String>, Vec<String>) = airports.into_iter().unzip();
            tx.execute("INSERT INTO airport_city (airport, city) SELECT * FROM unnest($1::text[], $2::text[]) ON CONFLICT (airport) DO NOTHING", &[&a, &c]).await.map_err(err)?;
        }
        tx.execute(
            "INSERT INTO lake_files (key, created_at, applied_at, rows) VALUES ($1, $2, now(), $3) \
             ON CONFLICT (key) DO UPDATE SET created_at = excluded.created_at, applied_at = now(), rows = excluded.rows",
            &[&key, &created_at, &(stats.rows as i32)],
        )
        .await
        .map_err(err)?;
        tx.commit().await.map_err(err)?;
        Ok(stats)
    }

    /// Аэропорты города по накопленной карте (+ сам код).
    pub async fn codes_of_city(&self, city: &str) -> DbResult<Vec<String>> {
        let c = self.client().await?;
        let rows = c.query("SELECT airport FROM airport_city WHERE city = $1", &[&city]).await.map_err(err)?;
        let mut out = vec![city.to_string()];
        out.extend(rows.into_iter().map(|r| r.get::<_, String>(0)));
        Ok(out)
    }

    /// Билеты под фильтры: дни вылета в [from, to], город(а) вылета / прилёта, пересадка
    /// через город (расширяется до его аэропортов). Хотя бы одно из трёх условий обязательно.
    pub async fn tickets(&self, q: &TicketsQuery) -> DbResult<Vec<Row>> {
        if q.origins.is_empty() && q.destinations.is_empty() && q.via.is_empty() {
            return Err("нужен хотя бы один из origin/destination/via".into());
        }
        let mut via_codes: Vec<String> = Vec::new();
        for v in &q.via {
            via_codes.extend(self.codes_of_city(v).await?);
        }
        via_codes.sort();
        via_codes.dedup();
        let mut sql = format!("SELECT {} FROM tickets WHERE dep_day BETWEEN $1 AND $2", LAKE_COLS.join(", "));
        let mut params: Vec<Box<dyn ToSql + Sync + Send>> = vec![Box::new(q.from), Box::new(q.to)];
        if !q.origins.is_empty() {
            params.push(Box::new(q.origins.clone()));
            sql.push_str(&format!(" AND origin = ANY(${})", params.len()));
        }
        if !q.destinations.is_empty() {
            params.push(Box::new(q.destinations.clone()));
            sql.push_str(&format!(" AND destination = ANY(${})", params.len()));
        }
        if !q.via.is_empty() {
            params.push(Box::new(via_codes));
            sql.push_str(&format!(" AND via && ${}", params.len()));
        }
        sql.push_str(" ORDER BY dep_day, origin, price");
        if let Some(l) = q.limit {
            params.push(Box::new(l));
            sql.push_str(&format!(" LIMIT ${}", params.len()));
        }
        let c = self.client().await?;
        let refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref() as &(dyn ToSql + Sync)).collect();
        let rows = c.query(&sql, &refs).await.map_err(err)?;
        Ok(rows.iter().map(row_from_pg).collect())
    }

    /// Покрытие: серии городов за дни [from, to].
    pub async fn coverage(&self, origins: &[String], from: NaiveDate, to: NaiveDate) -> DbResult<Vec<SeriesRow>> {
        let c = self.client().await?;
        let rows = if origins.is_empty() {
            c.query("SELECT origin, dep_day, fetched_at, tickets FROM series WHERE dep_day BETWEEN $1 AND $2 ORDER BY origin, dep_day", &[&from, &to]).await
        } else {
            c.query("SELECT origin, dep_day, fetched_at, tickets FROM series WHERE dep_day BETWEEN $1 AND $2 AND origin = ANY($3) ORDER BY origin, dep_day", &[&from, &to, &origins]).await
        }
        .map_err(err)?;
        Ok(rows
            .into_iter()
            .map(|r| SeriesRow { origin: r.get(0), day: r.get::<_, NaiveDate>(1).to_string(), fetched_at: r.get::<_, DateTime<Utc>>(2).to_rfc3339(), tickets: r.get(3) })
            .collect())
    }

    /// По дням: сколько серий (городов) и билетов в складе.
    pub async fn coverage_days(&self, from: NaiveDate, to: NaiveDate) -> DbResult<Vec<DayRow>> {
        let c = self.client().await?;
        let rows = c.query("SELECT dep_day, count(*), coalesce(sum(tickets), 0)::bigint FROM series WHERE dep_day BETWEEN $1 AND $2 GROUP BY dep_day ORDER BY dep_day", &[&from, &to]).await.map_err(err)?;
        Ok(rows.into_iter().map(|r| DayRow { day: r.get::<_, NaiveDate>(0).to_string(), series: r.get(1), tickets: r.get(2) }).collect())
    }

    /// Прошедшие дни вылета — вон (строго раньше `before`).
    pub async fn retention(&self, before: NaiveDate) -> DbResult<(u64, u64)> {
        let c = self.client().await?;
        let t = c.execute("DELETE FROM tickets WHERE dep_day < $1", &[&before]).await.map_err(err)?;
        let s = c.execute("DELETE FROM series WHERE dep_day < $1", &[&before]).await.map_err(err)?;
        Ok((t, s))
    }

    pub async fn stats(&self) -> DbResult<serde_json::Value> {
        let c = self.client().await?;
        let s = c.query_one("SELECT count(*), coalesce(sum(tickets), 0)::bigint, min(dep_day)::text, max(dep_day)::text, count(distinct origin), max(fetched_at) FROM series", &[]).await.map_err(err)?;
        let f = c.query_one("SELECT count(*), max(created_at), max(applied_at) FROM lake_files", &[]).await.map_err(err)?;
        let size = c.query_one("SELECT pg_database_size(current_database())", &[]).await.map_err(err)?;
        Ok(serde_json::json!({
            "series": s.get::<_, i64>(0),
            "tickets": s.get::<_, i64>(1),
            "day_min": s.get::<_, Option<String>>(2),
            "day_max": s.get::<_, Option<String>>(3),
            "cities": s.get::<_, i64>(4),
            "last_fetched_at": s.get::<_, Option<DateTime<Utc>>>(5).map(|d| d.to_rfc3339()),
            "files": f.get::<_, i64>(0),
            "last_file_created_at": f.get::<_, Option<DateTime<Utc>>>(1).map(|d| d.to_rfc3339()),
            "last_applied_at": f.get::<_, Option<DateTime<Utc>>>(2).map(|d| d.to_rfc3339()),
            "db_bytes": size.get::<_, i64>(0),
        }))
    }
}

fn row_from_pg(r: &tokio_postgres::Row) -> Row {
    Row {
        series_id: r.get("series_id"),
        observed_at: r.get("observed_at"),
        search_origin: r.get("search_origin"),
        search_destination: r.get("search_destination"),
        search_date: r.get("search_date"),
        origin: r.get("origin"),
        destination: r.get("destination"),
        origin_airport: r.get("origin_airport"),
        destination_airport: r.get("destination_airport"),
        departure_at: r.get("departure_at"),
        arrival_at: r.get("arrival_at"),
        duration: r.get("duration"),
        duration_to: r.get("duration_to"),
        transfers: r.get("transfers"),
        airline: r.get("airline"),
        flight_number: r.get("flight_number"),
        price: r.get("price"),
        currency: r.get("currency"),
        link: r.get("link"),
        chain_json: r.get("chain_json"),
        legs_json: r.get("legs_json"),
        transfer_points_json: r.get("transfer_points_json"),
        baggage_code: r.get("baggage_code"),
        baggage_known: r.get("baggage_known"),
        baggage_included: r.get("baggage_included"),
        baggage_pieces: r.get("baggage_pieces"),
        baggage_kg: r.get("baggage_kg"),
        source: r.get("source"),
    }
}

/// COPY BINARY строк серии в tickets (dep_day и via — вычисляемые, остальное — колонки озера).
async fn copy_rows(tx: &tokio_postgres::Transaction<'_>, origin: &str, rows: &[Row]) -> DbResult<()> {
    let cols = format!("dep_day, via, {}", LAKE_COLS.join(", "));
    let sink = tx.copy_in(&format!("COPY tickets ({cols}) FROM STDIN BINARY")).await.map_err(err)?;
    let types = [
        Type::DATE, Type::TEXT_ARRAY, Type::INT8, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::INT4, Type::INT4, Type::INT2, Type::TEXT, Type::TEXT, Type::FLOAT8, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::BOOL, Type::BOOL, Type::INT2, Type::INT2, Type::TEXT,
    ];
    let writer = BinaryCopyInWriter::new(sink, &types);
    pin_mut!(writer);
    for r in rows {
        let dep_day = r.dep_day().ok_or_else(|| "билет без дня вылета".to_string())?;
        let via = r.via();
        let search_origin = r.search_origin.clone().filter(|s| !s.is_empty()).unwrap_or_else(|| origin.to_string());
        let vals: [&(dyn ToSql + Sync); 30] = [
            &dep_day, &via, &r.series_id, &r.observed_at, &search_origin, &r.search_destination, &r.search_date, &r.origin, &r.destination, &r.origin_airport, &r.destination_airport, &r.departure_at, &r.arrival_at, &r.duration, &r.duration_to, &r.transfers, &r.airline, &r.flight_number, &r.price, &r.currency, &r.link, &r.chain_json, &r.legs_json, &r.transfer_points_json, &r.baggage_code, &r.baggage_known, &r.baggage_included, &r.baggage_pieces, &r.baggage_kg, &r.source,
        ];
        writer.as_mut().write(&vals).await.map_err(err)?;
    }
    writer.finish().await.map_err(err)?;
    Ok(())
}
