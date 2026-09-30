//! Сверка с озером через коллектор: список файлов индекса (`GET /v1/lake/files?since=`),
//! файлы, которых нет в журнале склада (или с другим created_at), — забираем
//! (`GET /v1/lake/file?key=`) и применяем. Первый проход на пустом складе = разовая
//! заливка всего озера; дальше — догоняем то, что не дошло пушем коллектора
//! (рестарты, недоступность). Работает в несколько параллельных загрузок.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::Deserialize;
use tokio::sync::Semaphore;

use crate::store::Store;

#[derive(Debug, Deserialize, Clone)]
pub struct LakeFile {
    pub key: String,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub bytes: i64,
}

#[derive(Debug, Deserialize)]
struct FilesResponse {
    files: Vec<LakeFile>,
}

#[derive(Clone)]
pub struct Syncer {
    pub collector: String,
    pub http: reqwest::Client,
    pub store: Store,
    pub workers: usize,
}

/// Сколько назад от последнего применённого файла смотреть при очередной сверке:
/// файлы пишутся не строго по порядку created_at (окна дописываются позже).
const LOOKBACK: Duration = Duration::from_secs(6 * 3600);

impl Syncer {
    pub fn new(collector: &str, store: Store, workers: usize) -> Syncer {
        Syncer { collector: collector.trim_end_matches('/').to_string(), http: reqwest::Client::builder().timeout(Duration::from_secs(120)).build().expect("reqwest"), store, workers: workers.max(1) }
    }

    async fn list_files(&self, since: Option<DateTime<Utc>>) -> Result<Vec<LakeFile>, String> {
        let mut req = self.http.get(format!("{}/v1/lake/files", self.collector));
        if let Some(s) = since {
            req = req.query(&[("since", s.to_rfc3339())]);
        }
        let r = req.send().await.map_err(|e| format!("коллектор недоступен: {e}"))?;
        if !r.status().is_success() {
            return Err(format!("коллектор /v1/lake/files: HTTP {}", r.status()));
        }
        Ok(r.json::<FilesResponse>().await.map_err(|e| format!("коллектор /v1/lake/files: {e}"))?.files)
    }

    async fn fetch_file(&self, key: &str) -> Result<bytes::Bytes, String> {
        let r = self.http.get(format!("{}/v1/lake/file", self.collector)).query(&[("key", key)]).send().await.map_err(|e| format!("коллектор недоступен: {e}"))?;
        if !r.status().is_success() {
            return Err(format!("коллектор /v1/lake/file {key}: HTTP {}", r.status()));
        }
        r.bytes().await.map_err(|e| format!("коллектор /v1/lake/file {key}: {e}"))
    }

    /// Один проход сверки: сколько файлов применено (и сколько с ошибкой).
    pub async fn run_once(&self, full: bool) -> Result<(usize, usize), String> {
        let since = if full { None } else { self.store.last_applied().await?.map(|t| t - LOOKBACK) };
        let listed = self.list_files(since).await?;
        if listed.is_empty() {
            return Ok((0, 0));
        }
        // что уже применено с тем же created_at — пропускаем (проверка пачками)
        let mut todo: Vec<LakeFile> = Vec::new();
        for chunk in listed.chunks(5000) {
            let keys: Vec<String> = chunk.iter().map(|f| f.key.clone()).collect();
            let known = self.store.applied_files(&keys).await?;
            for f in chunk {
                if known.get(&f.key) != Some(&f.created_at) {
                    todo.push(f.clone());
                }
            }
        }
        if todo.is_empty() {
            return Ok((0, 0));
        }
        // старые раньше новых: при конфликте дня побеждает более свежая серия и так, но
        // порядок делает журнал монотонным
        todo.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.key.cmp(&b.key)));
        let total = todo.len();
        println!("[tickets] сверка: {total} файлов к применению (из {} в списке)", listed.len());
        let started = Instant::now();
        let sem = Arc::new(Semaphore::new(self.workers));
        let done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let failed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut handles = Vec::new();
        for f in todo {
            let permit = sem.clone().acquire_owned().await.map_err(|e| e.to_string())?;
            let me = self.clone();
            let (done, failed) = (done.clone(), failed.clone());
            handles.push(tokio::spawn(async move {
                let _p = permit;
                match me.fetch_file(&f.key).await {
                    Ok(data) => match me.store.apply_file(&f.key, f.created_at, data).await {
                        Ok(_) => {
                            let n = done.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                            if n % 5000 == 0 {
                                println!("[tickets] сверка: применено {n}/{total}, {:.0} с", started.elapsed().as_secs_f64());
                            }
                        }
                        Err(e) => {
                            failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            println!("[tickets] файл {}: {e}", f.key);
                        }
                    },
                    Err(e) => {
                        failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        println!("[tickets] {e}");
                    }
                }
            }));
        }
        for h in handles {
            let _ = h.await;
        }
        let (d, f) = (done.load(std::sync::atomic::Ordering::Relaxed), failed.load(std::sync::atomic::Ordering::Relaxed));
        println!("[tickets] сверка завершена: применено {d}, ошибок {f}, {:.0} с", started.elapsed().as_secs_f64());
        Ok((d, f))
    }

    /// Фоновый цикл: полная сверка на старте (заливка), затем инкрементальная раз в interval.
    pub async fn run_forever(self, interval: Duration) {
        let mut first = true;
        loop {
            match self.run_once(first).await {
                Ok(_) => first = false,
                Err(e) => println!("[tickets] сверка не удалась: {e}"),
            }
            tokio::time::sleep(interval).await;
        }
    }
}
