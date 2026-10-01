//! Наполнение склада в памяти (`lakestore`) прямо из озера коллектора, без коллектора:
//! S3 (те же `S3_*`, что у коллектора; только список и чтение) или локальный каталог
//! `LAKE_LOCAL_ROOT` (dev/тесты).
//!
//! 1. Старт: снапшот с диска (`LAKE_SNAPSHOT_PATH`, по умолчанию `store.snap` рядом с БД) —
//!    склад готов через секунды; без снапшота — готов после первого полного прохода.
//! 2. Полный проход: список всего `tickets/`, по каждому (город, день) — самый свежий файл
//!    X→ANY; качаются только файлы, у которых есть день новее склада. На старте и раз в
//!    `LAKE_FULL_SYNC_SECONDS` (6 ч) — страховка от пропусков.
//! 3. Дельта раз в `LAKE_SYNC_SECONDS` (120): список `tickets/fetched=<сегодня>/` и
//!    `<вчера>/` (UTC) — файлы раскладываются по дню загрузки, новые появляются только там.
//! 4. Ретеншн (дни раньше вчера) и снапшот раз в `LAKE_SNAPSHOT_SECONDS` (3600), если
//!    склад менялся.
//!
//! Журнал применённых файлов не нужен: файл, чьи дни в складе не старше его, пропускается.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{NaiveDate, Utc};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

use crate::lakestore::{parse_file_key, read_lake_parquet, FileMeta, LakeStore};

/// Источник файлов озера.
pub trait Lake: Send + Sync {
    /// Ключи под префиксом.
    fn list(&self, prefix: &str) -> Result<Vec<String>, String>;
    fn get(&self, key: &str) -> Result<bytes::Bytes, String>;
    fn describe(&self) -> String;

    /// Последние `n` байт файла и его полный размер (чтение Parquet по частям: футер).
    fn get_tail(&self, key: &str, n: u64) -> Result<(bytes::Bytes, u64), String> {
        let all = self.get(key)?;
        let len = all.len() as u64;
        Ok((all.slice(len.saturating_sub(n) as usize..), len))
    }

    /// Байты [start, start + len) файла.
    fn get_range(&self, key: &str, start: u64, len: u64) -> Result<bytes::Bytes, String> {
        let all = self.get(key)?;
        let end = (start + len).min(all.len() as u64);
        Ok(all.slice(start.min(end) as usize..end as usize))
    }
}

// ---------------------------------------------------------------- S3 (SigV4)

pub struct S3Lake {
    endpoint: String,
    host: String,
    region: String,
    bucket: String,
    access_key: String,
    secret_key: String,
    http: reqwest::blocking::Client,
}

fn uri_encode(s: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn hmac(key: &[u8], data: &str) -> Vec<u8> {
    let mut m = <Hmac<Sha256> as KeyInit>::new_from_slice(key).expect("hmac");
    m.update(data.as_bytes());
    m.finalize().into_bytes().to_vec()
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&apos;", "'").replace("&amp;", "&")
}

fn xml_tags<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(a) = rest.find(&open) {
        let after = &rest[a + open.len()..];
        let Some(b) = after.find(&close) else { break };
        out.push(&after[..b]);
        rest = &after[b + close.len()..];
    }
    out
}

impl S3Lake {
    pub fn new(endpoint: &str, region: &str, bucket: &str, access_key: &str, secret_key: &str) -> S3Lake {
        let endpoint = endpoint.trim_end_matches('/').to_string();
        let host = endpoint.split("://").nth(1).unwrap_or(&endpoint).split('/').next().unwrap_or("").to_string();
        let http = reqwest::blocking::Client::builder().timeout(Duration::from_secs(120)).build().expect("reqwest");
        S3Lake { endpoint, host, region: region.into(), bucket: bucket.into(), access_key: access_key.into(), secret_key: secret_key.into(), http }
    }

    /// Заголовок Authorization (SigV4) для GET без тела: путь и строка запроса уже
    /// закодированы, amz_date — `YYYYMMDDTHHMMSSZ`.
    fn authorization(&self, path: &str, qs: &str, amz_date: &str) -> String {
        let payload = "UNSIGNED-PAYLOAD";
        let date = &amz_date[..8];
        let canonical = format!("GET\n{path}\n{qs}\nhost:{}\nx-amz-content-sha256:{payload}\nx-amz-date:{amz_date}\n\nhost;x-amz-content-sha256;x-amz-date\n{payload}", self.host);
        let scope = format!("{date}/{}/s3/aws4_request", self.region);
        let to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}", sha256_hex(canonical.as_bytes()));
        let k = hmac(format!("AWS4{}", self.secret_key).as_bytes(), date);
        let k = hmac(&k, &self.region);
        let k = hmac(&k, "s3");
        let k = hmac(&k, "aws4_request");
        let sig = hex::encode(hmac(&k, &to_sign));
        format!("AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={sig}", self.access_key)
    }

    /// GET path-style `/<bucket>/<key>` с подписью AWS SigV4.
    fn signed_get(&self, key: &str, query: &[(&str, String)]) -> Result<reqwest::blocking::Response, String> {
        self.signed_get_range(key, query, None)
    }

    /// То же с заголовком Range (не подписывается — SigV4 его не требует).
    fn signed_get_range(&self, key: &str, query: &[(&str, String)], range: Option<String>) -> Result<reqwest::blocking::Response, String> {
        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let path = if key.is_empty() { format!("/{}", self.bucket) } else { format!("/{}/{}", self.bucket, uri_encode(key, true)) };
        let mut q: Vec<(String, String)> = query.iter().map(|(k, v)| (uri_encode(k, false), uri_encode(v, false))).collect();
        q.sort();
        let qs = q.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
        let payload = "UNSIGNED-PAYLOAD";
        let auth = self.authorization(&path, &qs, &amz_date);
        let url = if qs.is_empty() { format!("{}{path}", self.endpoint) } else { format!("{}{path}?{qs}", self.endpoint) };
        let mut req = self.http.get(url).header("x-amz-date", amz_date).header("x-amz-content-sha256", payload).header("authorization", auth);
        if let Some(r) = range {
            req = req.header("range", r);
        }
        let r = req.send().map_err(|e| format!("S3: {e}"))?;
        if !r.status().is_success() {
            let status = r.status();
            let body = r.text().unwrap_or_default();
            return Err(format!("S3 {key}: HTTP {status} {}", body.chars().take(300).collect::<String>()));
        }
        Ok(r)
    }
}

impl Lake for S3Lake {
    fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut q = vec![("list-type", "2".to_string()), ("prefix", prefix.to_string()), ("max-keys", "1000".to_string())];
            if let Some(t) = &token {
                q.push(("continuation-token", t.clone()));
            }
            let xml = self.signed_get("", &q)?.text().map_err(|e| format!("S3 list: {e}"))?;
            out.extend(xml_tags(&xml, "Key").into_iter().map(xml_unescape));
            let truncated = xml_tags(&xml, "IsTruncated").first().map(|v| *v == "true").unwrap_or(false);
            token = xml_tags(&xml, "NextContinuationToken").first().map(|t| xml_unescape(t));
            if !truncated || token.is_none() {
                return Ok(out);
            }
        }
    }

    fn get(&self, key: &str) -> Result<bytes::Bytes, String> {
        self.signed_get(key, &[])?.bytes().map_err(|e| format!("S3 {key}: {e}"))
    }

    fn get_tail(&self, key: &str, n: u64) -> Result<(bytes::Bytes, u64), String> {
        let r = self.signed_get_range(key, &[], Some(format!("bytes=-{n}")))?;
        // 206: «bytes a-b/total»; 200 — файл меньше n, пришёл целиком
        let total = r.headers().get("content-range").and_then(|v| v.to_str().ok()).and_then(|v| v.rsplit('/').next()).and_then(|t| t.parse::<u64>().ok());
        let body = r.bytes().map_err(|e| format!("S3 {key}: {e}"))?;
        let len = total.unwrap_or(body.len() as u64);
        Ok((body, len))
    }

    fn get_range(&self, key: &str, start: u64, len: u64) -> Result<bytes::Bytes, String> {
        if len == 0 {
            return Ok(bytes::Bytes::new());
        }
        let r = self.signed_get_range(key, &[], Some(format!("bytes={start}-{}", start + len - 1)))?;
        let partial = r.status() == reqwest::StatusCode::PARTIAL_CONTENT;
        let body = r.bytes().map_err(|e| format!("S3 {key}: {e}"))?;
        if partial {
            return Ok(body);
        }
        let end = (start + len).min(body.len() as u64);
        Ok(body.slice(start.min(end) as usize..end as usize))
    }

    fn describe(&self) -> String {
        format!("s3://{}", self.bucket)
    }
}

// ---------------------------------------------------------------- локальный каталог

pub struct LocalLake {
    root: std::path::PathBuf,
}

impl LocalLake {
    pub fn new(root: &str) -> LocalLake {
        LocalLake { root: root.into() }
    }
}

impl Lake for LocalLake {
    fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
        let mut out = Vec::new();
        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            let Ok(rd) = std::fs::read_dir(&dir) else { continue };
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if let Ok(rel) = p.strip_prefix(&self.root) {
                    let key = rel.to_string_lossy().replace('\\', "/");
                    if key.starts_with(prefix) {
                        out.push(key);
                    }
                }
            }
        }
        out.sort();
        Ok(out)
    }

    fn get(&self, key: &str) -> Result<bytes::Bytes, String> {
        std::fs::read(self.root.join(key)).map(bytes::Bytes::from).map_err(|e| format!("{key}: {e}"))
    }

    fn get_tail(&self, key: &str, n: u64) -> Result<(bytes::Bytes, u64), String> {
        let len = std::fs::metadata(self.root.join(key)).map_err(|e| format!("{key}: {e}"))?.len();
        let start = len.saturating_sub(n);
        Ok((self.get_range(key, start, len - start)?, len))
    }

    fn get_range(&self, key: &str, start: u64, len: u64) -> Result<bytes::Bytes, String> {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = std::fs::File::open(self.root.join(key)).map_err(|e| format!("{key}: {e}"))?;
        f.seek(SeekFrom::Start(start)).map_err(|e| format!("{key}: {e}"))?;
        let mut buf = Vec::with_capacity(len as usize);
        f.take(len).read_to_end(&mut buf).map_err(|e| format!("{key}: {e}"))?;
        Ok(bytes::Bytes::from(buf))
    }

    fn describe(&self) -> String {
        format!("file://{}", self.root.display())
    }
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).ok().filter(|s| !s.is_empty()).unwrap_or_else(|| default.to_string())
}

fn env_secs(name: &str, default: u64) -> Duration {
    Duration::from_secs(std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default))
}

/// Озеро из окружения: S3 (все S3_BUCKET/S3_ACCESS_KEY/S3_SECRET_KEY заданы) или каталог
/// LAKE_LOCAL_ROOT, если он есть. PLANNER_LAKE=0 — склад выключен.
pub fn lake_from_env() -> Option<Arc<dyn Lake>> {
    if std::env::var("PLANNER_LAKE").map(|v| v == "0" || v == "false").unwrap_or(false) {
        return None;
    }
    let var = |n: &str| std::env::var(n).ok().filter(|s| !s.is_empty());
    if let (Some(bucket), Some(ak), Some(sk)) = (var("S3_BUCKET"), var("S3_ACCESS_KEY"), var("S3_SECRET_KEY")) {
        let endpoint = env_or("S3_ENDPOINT", "https://storage.yandexcloud.net");
        let region = env_or("S3_REGION", "ru-central1");
        return Some(Arc::new(S3Lake::new(&endpoint, &region, &bucket, &ak, &sk)));
    }
    let root = var("LAKE_LOCAL_ROOT")?;
    std::path::Path::new(&root).is_dir().then(|| Arc::new(LocalLake::new(&root)) as Arc<dyn Lake>)
}

// ---------------------------------------------------------------- сверка

/// Дни раньше этого (вчера по UTC — запас на часовые пояса) складу не нужны.
pub fn cutoff_day() -> NaiveDate {
    Utc::now().date_naive().pred_opt().unwrap()
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct SyncStats {
    pub listed: usize,
    pub needed: usize,
    pub applied: usize,
    pub failed: usize,
    pub rows: usize,
}

/// Из ключей — файлы, нужные складу: по каждому (город, день) не раньше cutoff — самый
/// свежий файл, и только если он новее серии склада.
pub fn plan_files(store: &LakeStore, keys: &[String], cutoff: NaiveDate) -> Vec<(String, FileMeta)> {
    let metas: Vec<(&String, FileMeta)> = keys.iter().filter_map(|k| parse_file_key(k).filter(|m| m.store_origin().is_some()).map(|m| (k, m))).collect();
    let mut best: HashMap<(&str, NaiveDate), i64> = HashMap::new();
    for (_, m) in &metas {
        let o = m.store_origin().unwrap();
        for d in m.days().into_iter().filter(|d| *d >= cutoff) {
            let e = best.entry((o, d)).or_insert(i64::MIN);
            *e = (*e).max(m.observed.timestamp());
        }
    }
    let mut out: Vec<(String, FileMeta)> = Vec::new();
    for (k, m) in &metas {
        let o = m.store_origin().unwrap();
        let ts = m.observed.timestamp();
        let wanted = m.days().into_iter().filter(|d| *d >= cutoff).any(|d| best.get(&(o, d)) == Some(&ts) && store.fetched(o, d).map(|f| f < ts).unwrap_or(true));
        if wanted {
            out.push(((*k).clone(), m.clone()));
        }
    }
    // старые раньше — при обрыве на полпути склад остаётся согласованным по дням
    out.sort_by_key(|(_, m)| m.observed);
    out
}

/// Качает и применяет файлы в `workers` потоков; ход — в status склада.
pub fn apply_files(store: &LakeStore, lake: &dyn Lake, files: &[(String, FileMeta)], cutoff: NaiveDate, workers: usize) -> SyncStats {
    let next = AtomicUsize::new(0);
    let stats = Mutex::new(SyncStats { needed: files.len(), ..Default::default() });
    {
        let mut st = store.status.lock().unwrap();
        st.files_total = files.len();
        st.files_done = 0;
    }
    std::thread::scope(|s| {
        for _ in 0..workers.max(1).min(files.len().max(1)) {
            s.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                let Some((key, meta)) = files.get(i) else { break };
                let res = lake.get(key).and_then(read_lake_parquet).and_then(|cols| store.apply(meta, &cols, cutoff));
                let mut st = stats.lock().unwrap();
                match res {
                    Ok(a) => {
                        st.applied += 1;
                        st.rows += a.rows;
                    }
                    Err(e) => {
                        st.failed += 1;
                        let mut s = store.status.lock().unwrap();
                        s.files_failed += 1;
                        s.last_error = Some(format!("{key}: {e}"));
                    }
                }
                store.status.lock().unwrap().files_done += 1;
            });
        }
    });
    stats.into_inner().unwrap()
}

/// Один проход: полный (весь `tickets/`) или дельта (папки загрузки сегодня и вчера).
pub fn sync_once(store: &LakeStore, lake: &dyn Lake, full: bool, workers: usize) -> Result<SyncStats, String> {
    let cutoff = cutoff_day();
    let keys = if full {
        lake.list("tickets/")?
    } else {
        let today = Utc::now().date_naive();
        let mut keys = lake.list(&format!("tickets/fetched={}/", today.pred_opt().unwrap()))?;
        keys.extend(lake.list(&format!("tickets/fetched={today}/"))?);
        keys
    };
    let files = plan_files(store, &keys, cutoff);
    let mut stats = apply_files(store, lake, &files, cutoff, workers);
    stats.listed = keys.len();
    store.retention(cutoff);
    Ok(stats)
}

pub struct SyncConfig {
    pub snapshot_path: String,
    pub sync_every: Duration,
    pub full_every: Duration,
    pub snapshot_every: Duration,
    pub workers: usize,
}

impl SyncConfig {
    pub fn from_env(db_path: &str) -> SyncConfig {
        let dir = std::path::Path::new(db_path).parent().map(|p| p.to_path_buf()).unwrap_or_default();
        SyncConfig {
            snapshot_path: env_or("LAKE_SNAPSHOT_PATH", &dir.join("store.snap").to_string_lossy()),
            sync_every: env_secs("LAKE_SYNC_SECONDS", 120),
            full_every: env_secs("LAKE_FULL_SYNC_SECONDS", 6 * 3600),
            snapshot_every: env_secs("LAKE_SNAPSHOT_SECONDS", 3600),
            workers: std::env::var("LAKE_SYNC_WORKERS").ok().and_then(|s| s.parse().ok()).unwrap_or(16),
        }
    }
}

/// Склад процесса из окружения: озеро задано — создаёт склад, делает его глобальным и
/// запускает фоновую сверку; иначе None (рейсы только сериями).
pub fn bootstrap(db_path: &str) -> Option<Arc<LakeStore>> {
    let lake = lake_from_env()?;
    let cfg = SyncConfig::from_env(db_path);
    println!("[startup] склад билетов: {} (снапшот {})", lake.describe(), cfg.snapshot_path);
    let store = Arc::new(LakeStore::new());
    crate::lakestore::set_global(store.clone());
    start(store.clone(), lake, cfg);
    Some(store)
}

fn set_state(store: &LakeStore, state: &str) {
    store.status.lock().unwrap().state = state.to_string();
}

/// Фоновый поток склада: снапшот → полный проход → дельты, ретеншн, снапшоты.
pub fn start(store: Arc<LakeStore>, lake: Arc<dyn Lake>, cfg: SyncConfig) {
    std::thread::Builder::new()
        .name("lake-sync".into())
        .spawn(move || {
            let t0 = Instant::now();
            set_state(&store, "snapshot");
            match store.load_snapshot(&cfg.snapshot_path, cutoff_day()) {
                Ok(n) => {
                    println!("[lake] снапшот {}: {n} серий за {:.1} с", cfg.snapshot_path, t0.elapsed().as_secs_f64());
                    store.status.lock().unwrap().loaded_from_snapshot = true;
                    store.set_ready();
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => println!("[lake] снапшота нет — полная загрузка из {}", lake.describe()),
                Err(e) => println!("[lake] снапшот {} не прочитан ({e}) — полная загрузка из {}", cfg.snapshot_path, lake.describe()),
            }
            let mut last_full: Option<Instant> = None;
            let mut last_snapshot = Instant::now();
            loop {
                let full = last_full.map(|t| t.elapsed() >= cfg.full_every).unwrap_or(true);
                set_state(&store, if full { "full-sync" } else { "delta-sync" });
                let t = Instant::now();
                match sync_once(&store, lake.as_ref(), full, cfg.workers) {
                    Ok(s) => {
                        if full {
                            last_full = Some(Instant::now());
                        }
                        if full || s.applied > 0 || s.failed > 0 {
                            println!("[lake] {}: ключей {}, нужно {}, применено {} ({} строк), ошибок {} — {:.1} с", if full { "полный проход" } else { "дельта" }, s.listed, s.needed, s.applied, s.rows, s.failed, t.elapsed().as_secs_f64());
                        }
                        let mut st = store.status.lock().unwrap();
                        st.last_sync_at = Some(Utc::now().to_rfc3339());
                        st.last_sync_seconds = Some(t.elapsed().as_secs_f64());
                        drop(st);
                        if full && !store.is_ready() {
                            println!("[lake] склад готов за {:.1} с", t0.elapsed().as_secs_f64());
                            store.set_ready();
                        }
                    }
                    Err(e) => {
                        println!("[lake] сверка не удалась: {e}");
                        store.status.lock().unwrap().last_error = Some(e);
                    }
                }
                // первый снапшот — сразу после загрузки с нуля (рестарт не качает озеро снова)
                let never = { let st = store.status.lock().unwrap(); !st.loaded_from_snapshot && st.snapshot_at.is_none() };
                if store.dirty.load(Ordering::Relaxed) && (never || last_snapshot.elapsed() >= cfg.snapshot_every) {
                    set_state(&store, "snapshot-write");
                    let t = Instant::now();
                    match store.save_snapshot(&cfg.snapshot_path) {
                        Ok(n) => {
                            println!("[lake] снапшот записан: {n} серий за {:.1} с", t.elapsed().as_secs_f64());
                            store.status.lock().unwrap().snapshot_at = Some(Utc::now().to_rfc3339());
                        }
                        Err(e) => println!("[lake] снапшот не записан: {e}"),
                    }
                    last_snapshot = Instant::now();
                }
                set_state(&store, "idle");
                std::thread::sleep(cfg.sync_every);
            }
        })
        .expect("lake-sync thread");
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn sigv4_matches_botocore() {
        // эталон — botocore S3SigV4Auth (UNSIGNED-PAYLOAD, те же заголовки и время)
        let lake = S3Lake::new("https://storage.yandexcloud.net", "ru-central1", "flights-lake", "AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY");
        let sig = |path: &str, qs: &str| lake.authorization(path, qs, "20261001T120000Z").rsplit("Signature=").next().unwrap().to_string();
        let key = format!("/flights-lake/{}", uri_encode("tickets/fetched=2026-10-01/origin=MOW/MOW-ANY__2026-10-10__08-00-00Z.parquet", true));
        assert_eq!(sig(&key, ""), "8951f98cb4fa10d1cde972b07c09d55e6bb1b1395485962ad25b7051f9856b27");
        let mut q: Vec<(String, String)> = [("list-type", "2"), ("prefix", "tickets/fetched=2026-10-01/"), ("max-keys", "1000"), ("continuation-token", "a/b=")].iter().map(|(k, v)| (uri_encode(k, false), uri_encode(v, false))).collect();
        q.sort();
        let qs = q.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
        assert_eq!(sig("/flights-lake", &qs), "a2e3b7e646c00651d43153c05ba099f37ac3139ae438094134b7541af78e3c17");
        assert_eq!(uri_encode("tickets/fetched=2026-10-01/origin=MOW/a b.parquet", true), "tickets/fetched%3D2026-10-01/origin%3DMOW/a%20b.parquet");
        let xml = "<ListBucketResult><IsTruncated>true</IsTruncated><Contents><Key>a&amp;b</Key></Contents><Contents><Key>c</Key></Contents><NextContinuationToken>t1</NextContinuationToken></ListBucketResult>";
        assert_eq!(xml_tags(xml, "Key").into_iter().map(xml_unescape).collect::<Vec<_>>(), vec!["a&b", "c"]);
        assert_eq!(xml_tags(xml, "NextContinuationToken"), vec!["t1"]);
    }

    #[test]
    fn local_lake_full_and_delta() {
        let root = std::env::temp_dir().join(format!("lake-{}", uuid::Uuid::new_v4()));
        let day = (Utc::now().date_naive() + chrono::Duration::days(10)).to_string();
        let today = Utc::now().date_naive();
        let old_fetch = today - chrono::Duration::days(3);
        write_file(&root, &format!("tickets/fetched={old_fetch}/origin=MOW/MOW-ANY__{day}__08-00-00Z.parquet"), &[ticket("MOW", "IST", &day, 100.0)]);
        write_file(&root, &format!("tickets/fetched={today}/origin=MOW/MOW-ANY__{day}__00-00-01Z.parquet"), &[ticket("MOW", "AYT", &day, 90.0)]);
        write_file(&root, &format!("tickets/fetched={today}/origin=ANY/ANY-AYT__{day}__min=1,max=2__00-00-01Z.parquet"), &[ticket("KZN", "AYT", &day, 1.0)]);
        let lake = LocalLake::new(&root.to_string_lossy());
        let store = LakeStore::new();
        let s = sync_once(&store, &lake, true, 4).unwrap();
        // старый файл MOW того же дня не качается: свежее — сегодняшний
        assert_eq!((s.listed, s.needed, s.applied, s.failed), (3, 1, 1, 0));
        let d = NaiveDate::parse_from_str(&day, "%Y-%m-%d").unwrap();
        let got = store.select(&["MOW".into()], &[], &[], d, d, 100).unwrap();
        assert_eq!(got[0].destination.as_deref(), Some("AYT"));
        // повтор — ничего не качаем; новый файл дня — дельта его находит
        assert_eq!(sync_once(&store, &lake, false, 4).unwrap().needed, 0);
        write_file(&root, &format!("tickets/fetched={today}/origin=MOW/MOW-ANY__{day}__23-59-59Z.parquet"), &[ticket("MOW", "SEL", &day, 80.0), ticket("MOW", "IST", &day, 70.0)]);
        write_file(&root, &format!("tickets/fetched={today}/origin=LED/LED-ANY__{day}__23-59-59Z.parquet"), &[]);
        let s = sync_once(&store, &lake, false, 4).unwrap();
        assert_eq!((s.needed, s.applied), (2, 2));
        assert_eq!(store.select(&["MOW".into()], &[], &[], d, d, 100).unwrap().len(), 2);
        assert_eq!(store.series_in(&["LED".into()], d, d).get(&("LED".to_string(), day.clone())), Some(&0));
        std::fs::remove_dir_all(root).unwrap();
    }
}
