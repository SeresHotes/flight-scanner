"""Движок коллектора: очередь серий с приоритетами и один воркер-поток.

Единица очереди — серия (направление × день × параметры), единица работы —
одна страница GraphQL. После каждой страницы серия возвращается в очередь, а
воркер берёт самое приоритетное задание: запрос приложения (`app`) вытесняет
фоновую серию (`crawl`) на границе страницы, та продолжает с того же offset,
когда очередь приложения пуста. Одинаковые серии от разных клиентов склеиваются;
свежая серия из индекса отдаётся без обращения к источнику.

Готовая серия уходит в индекс и в буфер озера (LakeWriter); результат для
приложения хранится в памяти до первого чтения `result()`."""
import heapq
import threading
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from collector.index import Index, series_key, utcnow
from collector.lake import LakeWriter, TICKETS_PREFIX
from collector.ratelimit import RateLimiter
from core import graphql_api

PRIORITY = {"app": 0, "crawl": 1}
MAX_NETWORK_RETRIES = 3
GIB = 1024 ** 3


class NotReady(Exception):
    """Результат запрошен у незавершённого задания."""


@dataclass
class SeriesRequest:
    origin: Optional[str]
    destination: Optional[str]
    day: str
    value_min: Optional[int] = None
    value_max: Optional[int] = None
    direct: Optional[bool] = None
    with_baggage: Optional[bool] = None
    max_pages: int = graphql_api.MAX_PAGES

    def __post_init__(self) -> None:
        self.origin = (self.origin or None) and self.origin.upper()
        self.destination = (self.destination or None) and self.destination.upper()
        if not self.origin and not self.destination:
            raise ValueError("нужен хотя бы один из origin/destination")
        self.max_pages = max(1, int(self.max_pages))

    @property
    def params_key(self) -> str:
        return graphql_api.params_key(self.value_min, self.value_max, self.direct, self.with_baggage)

    @property
    def key(self) -> tuple:
        return series_key(self.origin, self.destination, self.day, self.params_key)

    def params(self) -> Dict[str, Any]:
        return graphql_api.build_params(self.origin, self.destination, self.day,
                                        value_min=self.value_min, value_max=self.value_max,
                                        direct=self.direct, with_baggage=self.with_baggage)

    @property
    def label(self) -> str:
        return f"{self.origin or 'ANY'}→{self.destination or 'ANY'} {self.day}" + (
            f" [{self.params_key}]" if self.params_key else "")


@dataclass
class Job:
    req: SeriesRequest
    client: str
    priority: int
    seq: int
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    created: float = 0.0
    started: Optional[float] = None
    finished: Optional[float] = None
    pages: int = 0
    tickets: List[Dict[str, Any]] = field(default_factory=list)
    exhausted: bool = False
    error: Optional[str] = None
    done: bool = False
    cached: bool = False
    consumed: bool = False
    keep_result: bool = True
    retries: int = 0
    series_id: Optional[int] = None
    series_row: Optional[Dict[str, Any]] = None
    event: threading.Event = field(default_factory=threading.Event)

    @property
    def status(self) -> str:
        if self.done:
            return "error" if self.error else "done"
        return "running" if self.started is not None else "queued"

    def public(self) -> Dict[str, Any]:
        tickets = (self.series_row or {}).get("tickets", 0) if self.cached else len(self.tickets)
        return {"id": self.id, "status": self.status, "client": self.client,
                "priority": "app" if self.priority == 0 else "crawl",
                "pages": self.pages, "tickets": tickets,
                "exhausted": self.exhausted, "cached": self.cached, "error": self.error,
                "series": self.req.label}


class Counters:
    """Накопительные счётчики для /v1/stats (метрики считают дельты по минутам)."""

    def __init__(self):
        self._lock = threading.Lock()
        self._c: Counter = Counter()
        self._app_latency_total = 0.0
        self._app_latency_n = 0

    def inc(self, name: str, n: int = 1) -> None:
        with self._lock:
            self._c[name] += n

    def latency(self, seconds: float) -> None:
        with self._lock:
            self._app_latency_total += seconds
            self._app_latency_n += 1

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            out = dict(self._c)
            out["app_latency_avg_s"] = (round(self._app_latency_total / self._app_latency_n, 2)
                                        if self._app_latency_n else None)
            out["app_latency_total_s"] = round(self._app_latency_total, 2)
            out["app_latency_n"] = self._app_latency_n
            return out


class Engine:
    def __init__(self, settings, store, index: Index, page_fn=None, limiter: Optional[RateLimiter] = None,
                 clock=time.monotonic, sleep=time.sleep, now=utcnow):
        self.settings = settings
        self.store = store
        self.index = index
        self.writer = LakeWriter(store, index)
        self.limiter = limiter or RateLimiter(settings.rate_per_minute, clock=clock, sleep=sleep)
        self._session = requests.Session()
        self._page_fn = page_fn or self._query_page
        self._clock = clock
        self._sleep = sleep
        self._now = now
        self.counters = Counters()
        self._cv = threading.Condition()
        self._heap: List[tuple] = []
        self._seq = 0
        self._by_key: Dict[tuple, Job] = {}
        self._jobs: Dict[str, Job] = {}
        self._running: Optional[Job] = None
        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []
        self._last_retention = clock()
        self.started_at = now()
        self.crawler_stats: Dict[str, Any] = {}
        self.bucket_stats: Dict[str, Any] = {}

    # ------------------------------ lifecycle ------------------------------

    def start(self) -> None:
        self.reconcile_files()
        self._stop.clear()
        for name, target in (("collector-worker", self._worker), ("collector-housekeeping", self._housekeeping)):
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()
            self._threads.append(t)

    def stop(self, timeout: float = 10.0) -> None:
        self._stop.set()
        with self._cv:
            self._cv.notify_all()
        for t in self._threads:
            t.join(timeout=timeout)
        self._threads = []

    def reconcile_files(self) -> int:
        """Файлы озера, неизвестные индексу (потерянный индекс), — в учёт объёма."""
        try:
            objects = self.store.list(TICKETS_PREFIX + "/")
        except Exception as e:  # озеро недоступно — не валим старт
            print(f"[collector] сверка озера не удалась: {e}")
            return 0
        return self.index.import_files((o.key, o.size) for o in objects)

    # ------------------------------ submit ---------------------------------

    def submit(self, req: SeriesRequest, client: str = "app",
               ttl_seconds: Optional[float] = None) -> Job:
        if client not in PRIORITY:
            raise ValueError(f"неизвестный клиент {client!r}")
        priority = PRIORITY[client]
        ttl = self.settings.ttl_seconds if ttl_seconds is None else ttl_seconds
        self.counters.inc(f"submitted_{client}")
        with self._cv:
            existing = self._by_key.get(req.key)
            if existing is not None and not existing.done:
                self.counters.inc("dedup")
                if priority < existing.priority:
                    existing.priority = priority
                    heapq.heappush(self._heap, (priority, existing.seq, existing))
                    self._cv.notify()
                if req.max_pages > existing.req.max_pages:
                    existing.req.max_pages = req.max_pages
                if client == "app":
                    existing.keep_result = True
                return existing
            row = self.index.fresh(req.origin, req.destination, req.day, req.params_key, ttl,
                                   min_pages=req.max_pages, now=self._now())
            if row is not None and self._readable(row):
                self.counters.inc(f"cache_hit_{client}")
                job = Job(req=req, client=client, priority=priority, seq=-1, created=self._clock(),
                          pages=row["pages"], exhausted=bool(row["exhausted"]), done=True,
                          cached=True, series_row=row, series_id=row["id"])
                job.finished = job.created
                job.event.set()
                self._jobs[job.id] = job
                return job
            self._seq += 1
            job = Job(req=req, client=client, priority=priority, seq=self._seq,
                      created=self._clock(), keep_result=(client == "app"))
            self._jobs[job.id] = job
            self._by_key[req.key] = job
            heapq.heappush(self._heap, (priority, job.seq, job))
            self._cv.notify()
            return job

    def _readable(self, row: Dict[str, Any]) -> bool:
        """У свежей серии есть файл в озере (запись могла сорваться — тогда перечитаем)."""
        return bool(row.get("file_key"))

    def exists(self, origin: Optional[str], destination: Optional[str], day: str, params_key: str,
               min_pages: Optional[int] = None, ttl_seconds: Optional[float] = None) -> bool:
        ttl = self.settings.ttl_seconds if ttl_seconds is None else ttl_seconds
        row = self.index.fresh(origin, destination, day, params_key, ttl, min_pages=min_pages,
                               now=self._now())
        return row is not None and self._readable(row)

    def get_job(self, job_id: str) -> Optional[Job]:
        with self._cv:
            return self._jobs.get(job_id)

    def wait(self, job: Job, timeout: Optional[float] = None) -> bool:
        return job.event.wait(timeout)

    def result(self, job: Job) -> Dict[str, Any]:
        """Серия в формате graphql_api.fetch_series (+ cached). Билеты отдаются
        один раз: после чтения задание держит только метаданные."""
        if not job.done:
            raise NotReady(job.id)
        if job.cached:
            tickets = self._load(job.series_row)
            return {"tickets": tickets, "pages": job.pages, "exhausted": job.exhausted,
                    "error": False, "cached": True}
        if job.consumed and job.series_id is not None and not job.error:
            row = self.index.get(job.req.origin, job.req.destination, job.req.day, job.req.params_key)
            tickets = self._load(row) if row and row["id"] == job.series_id else []
        else:
            tickets, job.tickets, job.consumed = job.tickets, [], True
        return {"tickets": tickets, "pages": job.pages, "exhausted": job.exhausted,
                "error": bool(job.error), "cached": False}

    def _load(self, row: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not row or int(row.get("tickets") or 0) == 0 or not row.get("file_key"):
            return []
        return self.writer.read(row["file_key"], int(row["row_group"] or 0))

    # ------------------------------ worker ---------------------------------

    def _worker(self) -> None:
        while not self._stop.is_set():
            with self._cv:
                while not self._heap and not self._stop.is_set():
                    self._cv.wait(timeout=1.0)
                if self._stop.is_set():
                    return
            self.step()

    def step(self) -> Optional[Job]:
        """Одна страница самого приоритетного задания (синхронно; воркер зовёт в
        цикле, тесты — напрямую). None — очередь пуста."""
        with self._cv:
            job = None
            while self._heap:
                prio, _, cand = heapq.heappop(self._heap)
                if cand.done or prio != cand.priority:
                    continue  # устаревшая запись (задание переприоритезировано)
                job = cand
                break
            if job is None:
                return None
            if job.started is None:
                job.started = self._clock()
            self._running = job
        try:
            self._fetch_page(job)
        except Exception as e:  # защита воркера от неожиданного
            print(f"[collector] {job.req.label}: {e!r}")
            self._finish(job, error=repr(e))
        with self._cv:
            self._running = None
            if not job.done:
                heapq.heappush(self._heap, (job.priority, job.seq, job))
        return job

    def _query_page(self, params: Dict[str, Any], offset: int, limit: int) -> List[Dict[str, Any]]:
        return graphql_api.query_page(params, offset, limit, session=self._session, throttle=False)

    def _fetch_page(self, job: Job) -> None:
        offset = job.pages * graphql_api.PAGE_LIMIT
        if offset > graphql_api.MAX_OFFSET:
            self._finish(job)
            return
        self.limiter.wait()
        try:
            raw = self._page_fn(job.req.params(), offset, graphql_api.PAGE_LIMIT)
        except graphql_api.RateLimited as e:
            pause = self.limiter.on_429(e.retry_after)
            self.counters.inc("http_429")
            print(f"[collector] 429 на {job.req.label}, пауза {pause:.0f} с")
            return
        except graphql_api.GraphQLError as e:
            self.counters.inc("source_errors")
            self._finish(job, error=str(e)[:500])
            return
        except (requests.RequestException, ValueError) as e:
            job.retries += 1
            self.counters.inc("network_errors")
            if job.retries > MAX_NETWORK_RETRIES:
                self._finish(job, error=f"network: {e}"[:500])
            else:
                print(f"[collector] {job.req.label} стр. {job.pages + 1}: {e} (повтор {job.retries})")
            return
        self.limiter.on_success()
        job.retries = 0
        job.pages += 1
        self.counters.inc(f"pages_{job.client}")
        for t in raw:
            f = graphql_api.normalize_ticket(t, job.req.origin, job.req.destination, job.req.day)
            if f is not None:
                job.tickets.append(f)
        if len(raw) < graphql_api.PAGE_LIMIT:
            job.exhausted = True
        if job.exhausted or job.pages >= job.req.max_pages:
            self._finish(job)

    def _finish(self, job: Job, error: Optional[str] = None) -> None:
        now = self._now()
        req = job.req
        if error is None:
            sid = self.index.put(req.origin, req.destination, req.day, req.params_key,
                                 pages=job.pages, exhausted=job.exhausted, tickets=len(job.tickets),
                                 client=job.client, fetched_at=now)
            job.series_id = sid
            try:
                self.writer.write(sid, req.origin, req.destination, req.day, req.params_key,
                                  job.tickets, now)
            except Exception as e:  # озеро недоступно: серия без файла → кэшем не считается
                self.counters.inc("lake_write_errors")
                print(f"[collector] запись {req.label} в озеро не удалась: {e!r}")
            cities: Counter = Counter()
            for t in job.tickets:
                cities[t.get("destination") or ""] += 1
                cities[t.get("origin") or ""] += 1
            cities.pop("", None)
            self.index.touch_cities(dict(cities), now=now)
            self.counters.inc(f"series_{job.client}")
            self.counters.inc("tickets", len(job.tickets))
        else:
            # Ошибка источника: помним, чтобы сборщик не долбил направление постоянно,
            # но кэшем такая серия не считается (index.fresh пропускает error=1).
            self.index.put(req.origin, req.destination, req.day, req.params_key,
                           pages=job.pages, exhausted=False, tickets=0, error=True,
                           client=job.client, fetched_at=now)
            self.counters.inc(f"failed_{job.client}")
        job.error = error
        job.done = True
        job.finished = self._clock()
        if job.client == "app" or job.keep_result:
            self.counters.latency(job.finished - job.created)
        with self._cv:
            if self._by_key.get(req.key) is job:
                self._by_key.pop(req.key, None)
        if not job.keep_result:
            job.tickets = []
        job.event.set()

    # --------------------------- housekeeping ------------------------------

    def _housekeeping(self) -> None:
        while not self._stop.wait(1.0):
            try:
                self._purge_jobs()
                if self._clock() - self._last_retention >= self.settings.retention_interval_seconds:
                    self._last_retention = self._clock()
                    self.run_retention()
                    self.refresh_bucket_stats()
            except Exception as e:
                print(f"[collector] housekeeping: {e!r}")

    def _purge_jobs(self) -> None:
        cutoff = self._clock() - self.settings.job_keep_seconds
        with self._cv:
            stale = [jid for jid, j in self._jobs.items()
                     if j.done and (j.finished or 0) < cutoff]
            for jid in stale:
                self._jobs.pop(jid, None)

    def refresh_bucket_stats(self) -> Dict[str, Any]:
        """Объём и число объектов всего бакета (листинг S3; для метрик, раз в интервал
        ретеншна — на сотни тысяч объектов это сотни запросов, чаще не нужно)."""
        try:
            objects = self.store.list("")
        except Exception as e:
            print(f"[collector] листинг бакета не удался: {e!r}")
            return self.bucket_stats
        self.bucket_stats = {"bytes": sum(o.size for o in objects), "objects": len(objects),
                             "at": self._now().isoformat(timespec="seconds")}
        return self.bucket_stats

    def run_retention(self) -> List[str]:
        """Озеро больше LAKE_MAX_GB → удаляем самые старые файлы до 95 % порога."""
        max_bytes = int(self.settings.lake_max_gb * GIB)
        total = self.index.files_bytes()
        if max_bytes <= 0 or total <= max_bytes:
            return []
        target = max_bytes * 0.95
        doomed: List[str] = []
        for f in self.index.files_oldest_first():
            if total <= target:
                break
            doomed.append(f["key"])
            total -= int(f["bytes"])
        if doomed:
            self.store.delete(doomed)
            self.index.delete_files(doomed)
            self.counters.inc("files_deleted", len(doomed))
            print(f"[collector] ретеншн: удалено {len(doomed)} файлов, озеро {total / GIB:.1f} ГБ")
        return doomed

    # -------------------------------- stats --------------------------------

    def queue_stats(self) -> Dict[str, Any]:
        with self._cv:
            queued: Counter = Counter()
            seen = set()
            for prio, _, job in self._heap:
                if job.done or prio != job.priority or job.id in seen:
                    continue
                seen.add(job.id)
                queued["app" if prio == 0 else "crawl"] += 1
            running = self._running.public() if self._running else None
            jobs = len(self._jobs)
        return {"queued_app": queued["app"], "queued_crawl": queued["crawl"],
                "running": running, "jobs_in_memory": jobs}

    def queue_keys(self) -> List[Dict[str, Any]]:
        """Серии в очереди и в работе: сборщик исключает их из подачи, чтобы каждый
        тик добавлять новые, а не повторять уже стоящие (склейка сделала бы их no-op)."""
        with self._cv:
            seen = set()
            out = []
            jobs = ([self._running] if self._running else []) + [j for _, _, j in self._heap]
            for job in jobs:
                if job.done or job.id in seen:
                    continue
                seen.add(job.id)
                out.append({"origin": job.req.origin, "destination": job.req.destination,
                            "day": job.req.day, "params_key": job.req.params_key,
                            "priority": "app" if job.priority == 0 else "crawl"})
        return out

    def stats(self) -> Dict[str, Any]:
        st = self.queue_stats()
        st.update({
            "counters": self.counters.snapshot(),
            "rate_per_minute": self.limiter.per_minute,
            "total_429": self.limiter.total_429,
            "lake": {"files": len(self.index.files_oldest_first()), "bytes": self.index.files_bytes(),
                     "max_bytes": int(self.settings.lake_max_gb * GIB),
                     "files_written": self.writer.files_written},
            "index": self.index.age_stats(now=self._now()),
            "cities": len(self.index.cities()),
            "started_at": self.started_at.isoformat(timespec="seconds"),
            "crawler": self.crawler_stats,
        })
        return st
