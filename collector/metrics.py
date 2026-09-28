"""Ops-метрики коллектора и снимок покрытия → Parquet в озеро (для Grafana через
ClickHouse, docs/COLLECTOR.md, фаза 3).

Раз в METRICS_INTERVAL_SECONDS (60) снимается строка: нагрузка машины (/proc),
объём озера и бакета, очередь, страницы/серии/429/ошибки за минуту по клиентам,
задержка ответа приложению, возраст серий, сводка сборщика. Строки копятся и
раз в METRICS_FLUSH_ROWS пишутся файлом `ops_metrics/date=YYYY-MM-DD/<HH-MM-SS>.parquet`.
Раз в COVERAGE_SNAPSHOT_SECONDS (600) перезаписывается `coverage/latest.parquet` —
покрытие «город × день» для тепловой карты свежести."""
import io
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from collector.index import parse_ts

OPS_PREFIX = "ops_metrics"
COVERAGE_KEY = "coverage/latest.parquet"

# Счётчики движка, по которым считаем дельту за интервал.
_DELTA_COUNTERS = ["pages_app", "pages_crawl", "series_app", "series_crawl", "failed_app",
                   "failed_crawl", "cache_hit_app", "cache_hit_crawl", "submitted_app",
                   "submitted_crawl", "http_429", "source_errors", "network_errors",
                   "lake_write_errors", "tickets", "files_deleted"]

OPS_SCHEMA = pa.schema([
    ("ts", pa.timestamp("s", tz="UTC")),
    ("cpu_pct", pa.float64()), ("mem_pct", pa.float64()), ("disk_used_pct", pa.float64()),
    ("load1", pa.float64()),
    ("lake_bytes", pa.int64()), ("lake_files", pa.int64()), ("lake_max_bytes", pa.int64()),
    ("bucket_bytes", pa.int64()), ("bucket_objects", pa.int64()),
    ("queued_app", pa.int64()), ("queued_crawl", pa.int64()), ("running", pa.int8()),
    ("rate_per_minute", pa.float64()),
] + [(f"{c}_1m", pa.int64()) for c in _DELTA_COUNTERS] + [
    ("app_latency_avg_s", pa.float64()),
    ("series_total", pa.int64()), ("series_age_p50_h", pa.float64()), ("series_age_max_h", pa.float64()),
    ("cities", pa.int64()),
    ("crawler_pairs", pa.int64()), ("crawler_fresh", pa.int64()), ("crawler_stale", pa.int64()),
    ("crawler_missing", pa.int64()), ("crawler_errors", pa.int64()),
    ("crawler_pass_progress", pa.float64()), ("crawler_submitted", pa.int64()),
    ("crawler_quarantined_cities", pa.int64()),
])

COVERAGE_SCHEMA = pa.schema([
    ("snapshot_at", pa.timestamp("s", tz="UTC")),
    ("origin", pa.string()), ("day", pa.date32()), ("fetched_at", pa.timestamp("s", tz="UTC")),
    ("age_h", pa.float64()), ("pages", pa.int32()), ("tickets", pa.int32()),
    ("exhausted", pa.bool_()), ("error", pa.bool_()),
])


class HostStats:
    """Нагрузка машины из /proc (в контейнере /proc показывает CPU/память хоста) и
    statvfs каталога данных (bind-mount диска VM). Без /proc (macOS, тесты) — None."""

    def __init__(self, proc: str = "/proc", disk_path: str = "data"):
        self.proc = Path(proc)
        self.disk_path = disk_path
        self._cpu_prev: Optional[tuple] = None

    def _cpu_times(self) -> Optional[tuple]:
        try:
            fields = (self.proc / "stat").read_text().splitlines()[0].split()[1:]
        except (OSError, IndexError):
            return None
        values = [int(x) for x in fields]
        idle = values[3] + (values[4] if len(values) > 4 else 0)
        return idle, sum(values)

    def cpu_pct(self) -> Optional[float]:
        cur = self._cpu_times()
        if cur is None:
            return None
        prev, self._cpu_prev = self._cpu_prev, cur
        if prev is None or cur[1] <= prev[1]:
            return None
        return round(100.0 * (1.0 - (cur[0] - prev[0]) / (cur[1] - prev[1])), 1)

    def mem_pct(self) -> Optional[float]:
        try:
            info = {}
            for line in (self.proc / "meminfo").read_text().splitlines():
                key, _, rest = line.partition(":")
                info[key] = int(rest.strip().split()[0])
            return round(100.0 * (1.0 - info["MemAvailable"] / info["MemTotal"]), 1)
        except (OSError, KeyError, ValueError, ZeroDivisionError):
            return None

    def load1(self) -> Optional[float]:
        try:
            return float((self.proc / "loadavg").read_text().split()[0])
        except (OSError, ValueError, IndexError):
            return None

    def disk_used_pct(self) -> Optional[float]:
        try:
            st = os.statvfs(self.disk_path)
        except OSError:
            return None
        total = st.f_blocks * st.f_frsize
        return round(100.0 * (1.0 - st.f_bavail * st.f_frsize / total), 1) if total else None

    def sample(self) -> Dict[str, Optional[float]]:
        return {"cpu_pct": self.cpu_pct(), "mem_pct": self.mem_pct(),
                "disk_used_pct": self.disk_used_pct(), "load1": self.load1()}


class Metrics:
    def __init__(self, engine, host: Optional[HostStats] = None, interval: float = 60,
                 flush_rows: int = 5, coverage_interval: float = 600,
                 now=lambda: datetime.now(timezone.utc)):
        self.engine = engine
        self.host = host or HostStats(disk_path=os.path.dirname(engine.settings.db_path) or ".")
        self.interval = interval
        self.flush_rows = flush_rows
        self.coverage_interval = coverage_interval
        self._now = now
        self._rows: List[Dict[str, Any]] = []
        self._prev_counters: Optional[Dict[str, Any]] = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_coverage = 0.0
        self.files_written = 0
        self.last_row: Optional[Dict[str, Any]] = None

    # ------------------------------- sample --------------------------------

    def sample(self) -> Dict[str, Any]:
        e = self.engine
        now = self._now()
        stats = e.stats()
        counters = stats["counters"]
        prev = self._prev_counters or {}
        row: Dict[str, Any] = {"ts": now}
        row.update(self.host.sample())
        lake = stats["lake"]
        bucket = getattr(e, "bucket_stats", None) or {}
        row.update({
            "lake_bytes": lake["bytes"], "lake_files": lake["files"], "lake_max_bytes": lake["max_bytes"],
            "bucket_bytes": bucket.get("bytes"), "bucket_objects": bucket.get("objects"),
            "queued_app": stats["queued_app"], "queued_crawl": stats["queued_crawl"],
            "running": 1 if stats.get("running") else 0, "rate_per_minute": stats["rate_per_minute"],
        })
        for c in _DELTA_COUNTERS:
            row[f"{c}_1m"] = int(counters.get(c, 0)) - int(prev.get(c, 0))
        lat_n = counters.get("app_latency_n", 0) - prev.get("app_latency_n", 0)
        lat_t = counters.get("app_latency_total_s", 0.0) - prev.get("app_latency_total_s", 0.0)
        row["app_latency_avg_s"] = round(lat_t / lat_n, 2) if lat_n > 0 else None
        idx = stats["index"]
        row.update({"series_total": idx["series"], "series_age_p50_h": idx["age_p50_h"],
                    "series_age_max_h": idx["age_max_h"], "cities": stats["cities"]})
        cr = stats.get("crawler") or {}
        row.update({"crawler_pairs": cr.get("pairs"), "crawler_fresh": cr.get("fresh"),
                    "crawler_stale": cr.get("stale"), "crawler_missing": cr.get("missing"),
                    "crawler_errors": cr.get("errors"), "crawler_pass_progress": cr.get("pass_progress"),
                    "crawler_submitted": cr.get("submitted"),
                    "crawler_quarantined_cities": cr.get("quarantined_cities")})
        self._prev_counters = counters
        self.last_row = row
        self._rows.append(row)
        return row

    # -------------------------------- write --------------------------------

    def flush(self, force: bool = False) -> Optional[str]:
        if not self._rows or (not force and len(self._rows) < self.flush_rows):
            return None
        rows, self._rows = self._rows, []
        table = pa.Table.from_pylist(rows, schema=OPS_SCHEMA)
        sink = io.BytesIO()
        pq.write_table(table, sink, compression="zstd")
        first = rows[0]["ts"].astimezone(timezone.utc)
        key = f"{OPS_PREFIX}/date={first:%Y-%m-%d}/{first:%H-%M-%S}.parquet"
        self.engine.store.put_bytes(key, sink.getvalue())
        self.files_written += 1
        return key

    def write_coverage(self) -> str:
        now = self._now()
        rows = []
        for origin, day, fetched_at, pages, tickets, exhausted, error in self.engine.index.coverage():
            fetched = parse_ts(fetched_at)
            rows.append({"snapshot_at": now, "origin": origin, "day": datetime.fromisoformat(day).date(),
                         "fetched_at": fetched, "age_h": round((now - fetched).total_seconds() / 3600, 2),
                         "pages": pages, "tickets": tickets, "exhausted": bool(exhausted), "error": bool(error)})
        table = pa.Table.from_pylist(rows, schema=COVERAGE_SCHEMA)
        sink = io.BytesIO()
        pq.write_table(table, sink, compression="zstd")
        self.engine.store.put_bytes(COVERAGE_KEY, sink.getvalue())
        return COVERAGE_KEY

    # -------------------------------- loop ---------------------------------

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="collector-metrics", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
        try:
            self.flush(force=True)
        except Exception as e:
            print(f"[metrics] финальный сброс не удался: {e!r}")

    def _loop(self) -> None:
        self.host.cpu_pct()  # первая точка для дельты CPU
        while not self._stop.wait(self.interval):
            try:
                self.sample()
                self.flush()
                if time.monotonic() - self._last_coverage >= self.coverage_interval:
                    self._last_coverage = time.monotonic()
                    self.write_coverage()
            except Exception as e:
                print(f"[metrics] {e!r}")
