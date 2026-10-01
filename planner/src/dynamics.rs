//! Динамика цен по направлению (страница `/dynamics`): история из озера коллектора.
//!
//! Каждая повторная выборка серии — новый файл озера (`tickets/fetched=<день>/origin=X/…`),
//! поэтому «сколько стоил билет, если смотреть в разные дни» — это все файлы X→ANY (и X→Y)
//! без ценовых коридоров, чьё окно дней вылета задевает запрошенные дни. Файл = снимок
//! (момент загрузки из имени), рейс = маршрут (цепочка аэропортов, вылет, номера рейсов).
//!
//! Склад в памяти (`lakestore`) держит только последний снимок дня, поэтому история
//! читается из озера напрямую (только список и GET) и ни на что больше не влияет.
//! Фильтры (время вылета/прилёта, пересадки, багаж, авиакомпании) — на фронте: ответ
//! компактный (рейсы + тройки «рейс × снимок → цена»).

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use chrono::{NaiveDate, Utc};
use serde::Serialize;
use serde_json::{json, Value};

use crate::lakestore::{parse_file_key, read_lake_parquet, FileMeta, LakeCols};
use crate::lakesync::{lake_from_env, Lake};
use crate::segments::{booking_url, city_info};

/// Сколько дней вылета подряд можно смотреть разом.
pub const MAX_DAYS: i64 = 7;
/// Глубина истории по дню загрузки, дней.
pub const DEFAULT_HISTORY_DAYS: i64 = 60;
pub const MAX_HISTORY_DAYS: i64 = 180;
/// Потолок файлов на запрос (страховка от слишком широкого окна).
pub const MAX_FILES: usize = 800;
const WORKERS: usize = 16;
const CACHE_TTL: Duration = Duration::from_secs(10 * 60);
const CACHE_SIZE: usize = 16;

fn lake() -> Option<Arc<dyn Lake>> {
    static LAKE: OnceLock<Option<Arc<dyn Lake>>> = OnceLock::new();
    LAKE.get_or_init(lake_from_env).clone()
}

#[derive(Debug, Clone)]
pub struct DynamicsQuery {
    /// Код города или аэропорта вылета, как выбрал пользователь.
    pub origin: String,
    /// Город запроса в озере (для аэропорта — его город).
    pub origin_city: String,
    pub destination: String,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub history_days: i64,
}

impl DynamicsQuery {
    fn cache_key(&self) -> String {
        format!("{}|{}|{}|{}|{}|{}", self.origin, self.origin_city, self.destination, self.from, self.to, self.history_days)
    }
}

/// Рейс (маршрут) в ответе.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Flight {
    pub day: String,
    pub departure_at: String,
    pub arrival_at: Option<String>,
    pub duration: Option<i64>,
    pub transfers: i64,
    pub airline: Option<String>,
    /// Номера рейсов по плечам: «SU 1860», «A4 123».
    pub flights: Vec<String>,
    /// Аэропорты по цепочке: вылет, пересадки, прилёт.
    pub chain: Vec<String>,
    /// Коды пересадок (как у билета).
    pub via: Vec<String>,
    pub origin_airport: Option<String>,
    pub destination_airport: Option<String>,
    /// Ссылка на самый свежий билет рейса.
    pub link: Option<String>,
}

/// Снимок = файл озера (момент загрузки) и какие из запрошенных дней он покрыл.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Snapshot {
    pub at: String,
    pub days: Vec<String>,
}

/// Что дал один файл: снимок и билеты под направление.
struct FileRows {
    observed: i64,
    days: Vec<NaiveDate>,
    /// (ключ рейса, рейс, цена, багаж: 1 включён, 0 нет, -1 неизвестно).
    rows: Vec<(String, Flight, f64, i8)>,
}

fn up(s: Option<&str>) -> String {
    s.unwrap_or("").to_uppercase()
}

fn leg_label(carrier: Option<&str>, number: Option<&str>) -> String {
    let c = carrier.unwrap_or("").trim();
    let n = number.unwrap_or("").trim();
    if n.is_empty() {
        return c.to_string();
    }
    if c.is_empty() || n.starts_with(c) { n.to_string() } else { format!("{c} {n}") }
}

/// Строки файла под направление и дни [from, to].
fn rows_of(q: &DynamicsQuery, meta: &FileMeta, cols: &LakeCols) -> FileRows {
    let mut rows = Vec::new();
    let origin_is_city = q.origin == q.origin_city;
    for r in 0..cols.n {
        let Some(day) = cols.dep_day(r) else { continue };
        if day < q.from || day > q.to {
            continue;
        }
        let t = cols.ticket(r);
        let (Some(price), Some(dep)) = (t.price, t.departure_at.clone().filter(|s| !s.is_empty())) else { continue };
        let dest_ok = up(t.destination.as_deref()) == q.destination || up(t.destination_airport.as_deref()) == q.destination;
        let origin_ok = (origin_is_city && up(t.origin.as_deref()) == q.origin) || up(t.origin_airport.as_deref()) == q.origin;
        if !dest_ok || !origin_ok || t.hidden_city.is_some() {
            continue;
        }
        let flights: Vec<String> = if t.legs.is_empty() {
            vec![leg_label(t.airline.as_deref(), t.flight_number.as_deref())]
        } else {
            t.legs.iter().map(|l| leg_label(l.carrier.as_deref(), l.flight_number.as_deref())).collect()
        };
        let chain: Vec<String> = if t.chain.is_empty() {
            [t.origin_airport.clone(), t.destination_airport.clone()].into_iter().flatten().collect()
        } else {
            t.chain.clone()
        };
        let via: Vec<String> = t.transfer_points.as_deref().unwrap_or(&[]).iter().filter_map(|p| p.code.clone()).collect();
        let key = format!("{}|{}|{}", chain.join(">"), dep, flights.join(","));
        let bag = match &t.baggage {
            Some(b) if b.known => i8::from(b.included),
            _ => -1,
        };
        let flight = Flight {
            day: day.to_string(),
            departure_at: dep,
            arrival_at: t.arrival(),
            duration: t.duration,
            transfers: t.transfers,
            airline: t.airline.clone(),
            flights,
            chain,
            via,
            origin_airport: t.origin_airport.clone(),
            destination_airport: t.destination_airport.clone(),
            link: booking_url(t.link.as_deref()),
        };
        rows.push((key, flight, price, bag));
    }
    let days = meta.days().into_iter().filter(|d| *d >= q.from && *d <= q.to).collect();
    FileRows { observed: meta.observed.timestamp(), days, rows }
}

/// Файлы озера под запрос: X→ANY и X→Y без параметров, окно задевает [from, to].
pub fn pick_files(q: &DynamicsQuery, keys: &[String]) -> Vec<(String, FileMeta)> {
    let mut out: Vec<(String, FileMeta)> = keys
        .iter()
        .filter_map(|k| parse_file_key(k).map(|m| (k.clone(), m)))
        .filter(|(_, m)| {
            m.origin.as_deref() == Some(q.origin_city.as_str())
                && m.params_key.is_empty()
                && m.destination.as_deref().map(|d| d == q.destination).unwrap_or(true)
                && m.day <= q.to
                && m.day_to >= q.from
        })
        .collect();
    out.sort_by_key(|(_, m)| m.observed);
    out
}

/// Ключи файлов города вылета за последние `history_days` дней загрузки.
fn list_keys(lake: &dyn Lake, q: &DynamicsQuery) -> Result<Vec<String>, String> {
    let today = Utc::now().date_naive();
    // загрузка не раньше чем за history_days до сегодня и не позже последнего дня вылета
    let last = today.min(q.to);
    let first = (today - chrono::Duration::days(q.history_days)).min(last);
    let days: Vec<NaiveDate> = first.iter_days().take_while(|d| *d <= last).collect();
    let next = AtomicUsize::new(0);
    let out = Mutex::new((Vec::new(), None::<String>));
    std::thread::scope(|s| {
        for _ in 0..WORKERS.min(days.len().max(1)) {
            s.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                let Some(d) = days.get(i) else { break };
                let res = lake.list(&format!("tickets/fetched={d}/origin={}/", q.origin_city));
                let mut o = out.lock().unwrap();
                match res {
                    Ok(keys) => o.0.extend(keys),
                    Err(e) => o.1 = Some(e),
                }
            });
        }
    });
    let (keys, err) = out.into_inner().unwrap();
    match err {
        Some(e) if keys.is_empty() => Err(e),
        _ => Ok(keys),
    }
}

/// Собирает ответ из строк файлов (снимки по времени, рейсы по вылету).
fn assemble(q: &DynamicsQuery, files: Vec<FileRows>, failed: usize) -> Value {
    // снимки: один на момент загрузки (два файла одной секунды склеиваются)
    let mut snap_days: BTreeMap<i64, Vec<NaiveDate>> = BTreeMap::new();
    for f in &files {
        let e = snap_days.entry(f.observed).or_default();
        e.extend(f.days.iter().copied());
        e.sort();
        e.dedup();
    }
    let snap_ix: HashMap<i64, usize> = snap_days.keys().enumerate().map(|(i, t)| (*t, i)).collect();
    let snapshots: Vec<Snapshot> = snap_days
        .iter()
        .map(|(t, days)| Snapshot {
            at: chrono::DateTime::from_timestamp(*t, 0).unwrap_or_default().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            days: days.iter().map(|d| d.to_string()).collect(),
        })
        .collect();
    // рейсы: описание — из самого свежего снимка, цена — минимум на (рейс, снимок, багаж)
    let mut flights: HashMap<String, (i64, Flight)> = HashMap::new();
    let mut prices: HashMap<(String, usize, i8), f64> = HashMap::new();
    for f in files {
        let si = snap_ix[&f.observed];
        for (key, flight, price, bag) in f.rows {
            let p = prices.entry((key.clone(), si, bag)).or_insert(f64::INFINITY);
            *p = p.min(price);
            match flights.get_mut(&key) {
                Some((t, cur)) if *t <= f.observed => {
                    *t = f.observed;
                    *cur = flight;
                }
                Some(_) => {}
                None => {
                    flights.insert(key, (f.observed, flight));
                }
            }
        }
    }
    let mut order: Vec<(String, Flight)> = flights.into_iter().map(|(k, (_, f))| (k, f)).collect();
    order.sort_by(|a, b| a.1.departure_at.cmp(&b.1.departure_at).then_with(|| a.1.transfers.cmp(&b.1.transfers)).then_with(|| a.0.cmp(&b.0)));
    let flight_ix: HashMap<&str, usize> = order.iter().enumerate().map(|(i, (k, _))| (k.as_str(), i)).collect();
    let mut obs: Vec<(usize, usize, f64, i8)> = prices.iter().map(|((k, s, b), p)| (flight_ix[k.as_str()], *s, *p, *b)).collect();
    obs.sort_by(|a, b| (a.0, a.1, a.3).cmp(&(b.0, b.1, b.3)));
    let (oc, dc) = (city_info(&q.origin), city_info(&q.destination));
    json!({
        "origin": q.origin,
        "origin_city": q.origin_city,
        "destination": q.destination,
        "origin_name": oc.city,
        "destination_name": dc.city,
        "from": q.from.to_string(),
        "to": q.to.to_string(),
        "history_days": q.history_days,
        "files": snapshots.len(),
        "files_failed": failed,
        "snapshots": snapshots,
        "flights": order.into_iter().map(|(_, f)| f).collect::<Vec<_>>(),
        "obs": obs.into_iter().map(|(f, s, p, b)| json!([f, s, p, b])).collect::<Vec<_>>(),
    })
}

/// История цен направления из озера.
pub fn run(lake: &dyn Lake, q: &DynamicsQuery) -> Result<Value, String> {
    let keys = list_keys(lake, q)?;
    let files = pick_files(q, &keys);
    if files.len() > MAX_FILES {
        return Err(format!("Слишком много снимков ({}): сузьте дни вылета или глубину истории.", files.len()));
    }
    let next = AtomicUsize::new(0);
    let results: Mutex<(Vec<FileRows>, usize)> = Mutex::new((Vec::new(), 0));
    std::thread::scope(|s| {
        for _ in 0..WORKERS.min(files.len().max(1)) {
            s.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                let Some((key, meta)) = files.get(i) else { break };
                let res = lake.get(key).and_then(read_lake_parquet).map(|cols| rows_of(q, meta, &cols));
                let mut r = results.lock().unwrap();
                match res {
                    Ok(rows) => r.0.push(rows),
                    Err(e) => {
                        eprintln!("[dynamics] {key}: {e}");
                        r.1 += 1;
                    }
                }
            });
        }
    });
    let (rows, failed) = results.into_inner().unwrap();
    Ok(assemble(q, rows, failed))
}

type Cache = Mutex<Vec<(String, Instant, Arc<Value>)>>;

fn cache() -> &'static Cache {
    static CACHE: OnceLock<Cache> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(Vec::new()))
}

/// Ответ ручки `/api/dynamics` (с кэшем на 10 минут): `{error}` — понятный текст.
pub fn handle(q: DynamicsQuery) -> Value {
    let key = q.cache_key();
    {
        let mut c = cache().lock().unwrap();
        c.retain(|(_, t, _)| t.elapsed() < CACHE_TTL);
        if let Some((_, _, v)) = c.iter().find(|(k, _, _)| *k == key) {
            return (**v).clone();
        }
    }
    let Some(lake) = lake() else {
        return json!({"error": "Озеро билетов не подключено (нет S3_* / LAKE_LOCAL_ROOT) — истории цен нет."});
    };
    let t0 = Instant::now();
    match run(lake.as_ref(), &q) {
        Ok(mut v) => {
            v["seconds"] = json!((t0.elapsed().as_secs_f64() * 10.0).round() / 10.0);
            println!("[dynamics] {}→{} {}..{}: снимков {}, рейсов {} — {:.1} с", q.origin, q.destination, q.from, q.to, v["files"], v["flights"].as_array().map(|a| a.len()).unwrap_or(0), t0.elapsed().as_secs_f64());
            let v = Arc::new(v);
            let mut c = cache().lock().unwrap();
            c.push((key, Instant::now(), v.clone()));
            while c.len() > CACHE_SIZE {
                c.remove(0);
            }
            (*v).clone()
        }
        Err(e) => json!({"error": e}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lakesync::LocalLake;
    use crate::lakestore::tests::ticket;
    use parquet::arrow::ArrowWriter;

    fn write_file(root: &std::path::Path, key: &str, tickets: &[crate::ticket::Ticket]) {
        let ipc = crate::collector::testing::lake_ipc(tickets);
        let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(ipc), None).unwrap();
        let schema = reader.schema();
        let path = root.join(key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut w = ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
        for b in reader {
            w.write(&b.unwrap()).unwrap();
        }
        w.close().unwrap();
    }

    fn q(origin: &str, city: &str, dest: &str, day: NaiveDate) -> DynamicsQuery {
        DynamicsQuery { origin: origin.into(), origin_city: city.into(), destination: dest.into(), from: day, to: day, history_days: 30 }
    }

    #[test]
    fn history_of_one_flight_across_snapshots() {
        let root = std::env::temp_dir().join(format!("dyn-{}", uuid::Uuid::new_v4()));
        let today = Utc::now().date_naive();
        let day = today + chrono::Duration::days(20);
        let ds = day.to_string();
        let f1 = today - chrono::Duration::days(5);
        let f2 = today - chrono::Duration::days(2);
        let mut late = ticket("MOW", "EVN", &ds, 15000.0);
        late.departure_at = Some(format!("{ds}T18:00:00+03:00"));
        late.legs[0].departure_at = late.departure_at.clone();
        let mut bag = ticket("MOW", "EVN", &ds, 12500.0);
        bag.baggage = Some(crate::ticket::Baggage { known: true, included: true, pieces: Some(1), kg: Some(23) });
        // снимок 1: рейс 10:00 за 10 000 и 11 000 (два агента), вечерний, чужое направление
        write_file(&root, &format!("tickets/fetched={f1}/origin=MOW/MOW-ANY__{ds}..{}__08-00-00Z.parquet", day + chrono::Duration::days(1)), &[ticket("MOW", "EVN", &ds, 10000.0), ticket("MOW", "EVN", &ds, 11000.0), late, ticket("MOW", "IST", &ds, 5000.0)]);
        // снимок 2: подорожал, плюс тариф с багажом; вечернего уже нет
        write_file(&root, &format!("tickets/fetched={f2}/origin=MOW/MOW-ANY__{ds}__09-30-00Z.parquet"), &[ticket("MOW", "EVN", &ds, 12000.0), bag]);
        // мимо: коридор цен, другой город, другой день
        write_file(&root, &format!("tickets/fetched={f2}/origin=MOW/MOW-ANY__{ds}__min=1,max=99999__10-00-00Z.parquet"), &[ticket("MOW", "EVN", &ds, 1.0)]);
        write_file(&root, &format!("tickets/fetched={f2}/origin=LED/LED-ANY__{ds}__10-00-00Z.parquet"), &[ticket("LED", "EVN", &ds, 1.0)]);
        let other = (day + chrono::Duration::days(3)).to_string();
        write_file(&root, &format!("tickets/fetched={f2}/origin=MOW/MOW-ANY__{other}__10-00-00Z.parquet"), &[ticket("MOW", "EVN", &other, 1.0)]);

        let lake = LocalLake::new(&root.to_string_lossy());
        let v = run(&lake, &q("MOW", "MOW", "EVN", day)).unwrap();
        assert_eq!(v["files"], 2, "{v}");
        let snaps = v["snapshots"].as_array().unwrap();
        assert_eq!(snaps[0]["at"], format!("{f1}T08:00:00Z"));
        assert_eq!(snaps[0]["days"], json!([ds]));
        let flights = v["flights"].as_array().unwrap();
        assert_eq!(flights.len(), 2);
        assert_eq!(flights[0]["flights"], json!(["SU 100"]));
        assert_eq!(flights[0]["chain"], json!(["MOWA", "ISTA", "EVNA"]));
        assert!(flights[1]["departure_at"].as_str().unwrap().contains("T18:00"));
        // рейс 10:00: снимок 0 — минимум 10 000 без багажа; снимок 1 — 12 000 и 12 500 с багажом
        assert_eq!(v["obs"], json!([[0, 0, 10000.0, 0], [0, 1, 12000.0, 0], [0, 1, 12500.0, 1], [1, 0, 15000.0, 0]]));

        // аэропорт вылета: город берётся из запроса, совпадение — по аэропорту
        let v = run(&lake, &q("MOWA", "MOW", "EVN", day)).unwrap();
        assert_eq!(v["flights"].as_array().unwrap().len(), 2);
        let v = run(&lake, &q("SVO", "MOW", "EVN", day)).unwrap();
        assert_eq!(v["flights"].as_array().unwrap().len(), 0);
        assert_eq!(v["files"], 2);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn picks_only_plain_series_of_origin() {
        let day = NaiveDate::from_ymd_opt(2026, 10, 30).unwrap();
        let keys: Vec<String> = [
            "tickets/fetched=2026-10-01/origin=MOW/MOW-ANY__2026-10-29..2026-10-31__08-00-00Z.parquet",
            "tickets/fetched=2026-10-01/origin=MOW/MOW-EVN__2026-10-30__08-00-01Z.parquet",
            "tickets/fetched=2026-10-01/origin=MOW/MOW-IST__2026-10-30__08-00-02Z.parquet",
            "tickets/fetched=2026-10-01/origin=MOW/MOW-ANY__2026-10-31__08-00-03Z.parquet",
            "tickets/fetched=2026-10-01/origin=MOW/MOW-ANY__2026-10-30__direct=1__08-00-04Z.parquet",
            "tickets/fetched=2026-10-01/origin=ANY/ANY-EVN__2026-10-30__08-00-05Z.parquet",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let got: Vec<String> = pick_files(&q("MOW", "MOW", "EVN", day), &keys).into_iter().map(|(k, _)| k).collect();
        assert_eq!(got, keys[..2].to_vec());
    }
}
