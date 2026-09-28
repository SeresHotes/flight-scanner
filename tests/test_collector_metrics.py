"""Ops-метрики и снимок покрытия: дельты счётчиков за интервал, файл в озере,
покрытие с возрастом серий; без /proc — нагрузка машины None."""
from datetime import datetime, timedelta, timezone

import pyarrow.parquet as pq

from collector.engine import SeriesRequest
from collector.metrics import COVERAGE_KEY, HostStats, Metrics
from tests.test_collector_engine import _run, env  # noqa: F401 — фикстура движка


def test_sample_flush_and_coverage(env, tmp_path):  # noqa: F811
    t0 = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
    clock = [t0]
    m = Metrics(env, host=HostStats(proc=str(tmp_path / "no-proc"), disk_path=str(tmp_path)),
                interval=60, flush_rows=2, now=lambda: clock[0])
    env.crawler_stats = {"pairs": 10, "fresh": 4, "stale": 1, "missing": 5, "errors": 0,
                         "pass_progress": 0.5, "submitted": 3, "quarantined_cities": 0}
    row0 = m.sample()
    assert row0["pages_app_1m"] == 0 and row0["cpu_pct"] is None and row0["disk_used_pct"] is not None
    env.submit(SeriesRequest("MOW", "SEL", "2026-10-15"), client="app")
    env.submit(SeriesRequest("SEL", None, "2026-10-15"), client="crawl")
    _run(env)
    clock[0] = t0 + timedelta(minutes=1)
    row1 = m.sample()
    assert (row1["pages_app_1m"], row1["pages_crawl_1m"], row1["series_app_1m"], row1["series_crawl_1m"]) == (3, 1, 1, 1)
    assert row1["series_total"] == 2 and row1["cities"] >= 2 and row1["lake_files"] == 2
    assert row1["crawler_pairs"] == 10 and row1["crawler_pass_progress"] == 0.5
    assert row1["app_latency_avg_s"] is not None
    key = m.flush()  # 2 строки ≥ flush_rows → файл
    assert key and key.startswith("ops_metrics/date=2026-09-28/12-00-00")
    files = [o.key for o in env.store.list("ops_metrics/")]
    assert len(files) == 1 and files[0].startswith("ops_metrics/date=2026-09-28/12-00-00")
    table = pq.read_table(str(env.store.root / files[0]))
    assert table.num_rows == 2 and table.column("pages_app_1m").to_pylist() == [0, 3]
    # Третья строка после ещё минуты — дельты нулевые.
    clock[0] = t0 + timedelta(minutes=2)
    assert m.sample()["pages_app_1m"] == 0

    clock[0] = datetime.now(timezone.utc) + timedelta(hours=3)  # fetched_at серий — реальное время движка
    assert m.write_coverage() == COVERAGE_KEY
    cov = pq.read_table(str(env.store.root / COVERAGE_KEY)).to_pylist()
    assert len(cov) == 1 and cov[0]["origin"] == "SEL" and cov[0]["tickets"] == 3
    assert cov[0]["day"].isoformat() == "2026-10-15" and 2.9 < cov[0]["age_h"] <= 3.1


def test_bucket_stats_refresh(env):  # noqa: F811
    env.submit(SeriesRequest("MOW", "SEL", "2026-10-15"), client="crawl")
    _run(env)
    st = env.refresh_bucket_stats()
    assert st["objects"] == 1 and st["bytes"] == env.index.files_bytes()
