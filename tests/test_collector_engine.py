"""Движок коллектора: приоритеты и вытеснение по страницам, склейка одинаковых
серий, кэш из индекса и озера, ошибки источника, 429, ретеншн, round-trip билета."""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from collector import lake
from collector.config import Settings
from collector.engine import Engine, SeriesRequest
from collector.index import Index
from collector.ratelimit import RateLimiter
from collector.store import LocalStore
from core import graphql_api as g

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "graphql_tickets.json").read_text())


def _raw(i, origin="MOW", dest="SEL"):
    return {"value": 1000 + i, "main_airline": "XX", "number_of_changes": 0,
            "departure_at": "2026-10-15T10:00:00+03:00", "origin_city_iata": origin,
            "destination_city_iata": dest, "ticket_link": "/MOW1510SEL1?t=x",
            "segments": [{"transfers": [], "flight_legs": [{
                "origin": "SVO", "destination": "ICN", "departure_at": "2026-10-15T10:00:00+03:00",
                "arrival_at": "2026-10-15T23:00:00+09:00", "flight_number": str(i)}]}]}


class Source:
    """Сценарий страниц: {(origin, destination): [размеры страниц]}; журнал вызовов."""

    def __init__(self, sizes):
        self.sizes = sizes
        self.calls = []
        self.fail = {}      # (origin, dest) → исключение на следующий вызов

    def __call__(self, params, offset, limit):
        key = (params.get("origin", ""), params.get("destination", ""))
        self.calls.append((key, offset))
        exc = self.fail.pop(key, None)
        if exc is not None:
            raise exc
        idx = offset // g.PAGE_LIMIT
        sizes = self.sizes.get(key, [0])
        n = sizes[idx] if idx < len(sizes) else 0
        dest = key[1] or ("BKK" if idx % 2 else "SEL")
        return [_raw(offset + k, key[0] or "MOW", dest) for k in range(n)]


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


@pytest.fixture
def env(tmp_path):
    clock = Clock()
    settings = Settings(db_path=":memory:", lake_local_root=str(tmp_path / "lake"),
                        s3_bucket=None, rate_per_minute=6000, flush_tickets=10_000,
                        flush_seconds=300, lake_max_gb=1.0, ttl_seconds=3600)
    source = Source({("MOW", "SEL"): [400, 400, 37], ("MOW", ""): [400, 5], ("SEL", ""): [3]})
    limiter = RateLimiter(6000, clock=clock, sleep=clock.sleep)
    engine = Engine(settings, LocalStore(settings.lake_local_root), Index(":memory:"),
                    page_fn=source, limiter=limiter, clock=clock, sleep=clock.sleep)
    engine.source, engine.clock = source, clock
    return engine


def _run(engine, limit=100):
    for _ in range(limit):
        if engine.step() is None:
            return


def test_app_preempts_crawl_on_page_boundary(env):
    crawl = env.submit(SeriesRequest("MOW", "SEL", "2026-10-15"), client="crawl")
    assert env.step() is crawl and crawl.pages == 1 and not crawl.done
    app = env.submit(SeriesRequest("MOW", None, "2026-10-15"), client="app")
    assert env.step() is app          # приложение вытеснило фоновую серию
    assert env.step() is app and app.done and app.pages == 2 and app.exhausted
    assert env.step() is crawl and crawl.pages == 2
    assert env.step() is crawl and crawl.done and crawl.exhausted and crawl.pages == 3
    assert [c for c in env.source.calls] == [(("MOW", "SEL"), 0), (("MOW", ""), 0), (("MOW", ""), 400),
                                             (("MOW", "SEL"), 400), (("MOW", "SEL"), 800)]
    # Результат приложения: 405 билетов; фоновая серия результат не держит.
    res = env.result(app)
    assert (len(res["tickets"]), res["pages"], res["exhausted"], res["cached"]) == (405, 2, True, False)
    assert crawl.tickets == [] and env.index.get("MOW", "SEL", "2026-10-15", "")["tickets"] == 837
    stats = env.stats()
    assert stats["counters"]["pages_app"] == 2 and stats["counters"]["pages_crawl"] == 3
    assert stats["cities"] >= 2  # MOW, SEL, BKK из билетов


def test_same_series_is_merged_and_reprioritized(env):
    crawl = env.submit(SeriesRequest("MOW", "SEL", "2026-10-15", max_pages=2), client="crawl")
    other = env.submit(SeriesRequest("SEL", None, "2026-10-15"), client="crawl")
    app = env.submit(SeriesRequest("MOW", "SEL", "2026-10-15", max_pages=3), client="app")
    assert app is crawl and crawl.priority == 0 and crawl.req.max_pages == 3 and crawl.keep_result
    assert env.step() is crawl          # приоритет поднят — идёт раньше other
    _run(env)
    assert crawl.done and other.done and len(env.result(crawl)["tickets"]) == 837
    assert env.queue_stats()["queued_app"] == 0 and env.queue_stats()["queued_crawl"] == 0
    assert env.stats()["counters"]["dedup"] == 1


def test_cache_hit_from_buffer_then_from_file(env):
    first = env.submit(SeriesRequest("MOW", "SEL", "2026-10-15"), client="app")
    _run(env)
    tickets = env.result(first)["tickets"]
    # Ещё не сброшено в файл — свежая серия читается из буфера.
    second = env.submit(SeriesRequest("MOW", "SEL", "2026-10-15"), client="app")
    assert second.cached and second.done
    assert env.result(second)["tickets"] == tickets and env.stats()["counters"]["cache_hit_app"] == 1
    assert env.source.calls.count((("MOW", "SEL"), 0)) == 1
    # После сброса — из Parquet range-чтением той же row group.
    key = env.writer.flush(force=True)
    assert key and key.startswith("tickets/observed=") and env.index.files_bytes() > 0
    row = env.index.get("MOW", "SEL", "2026-10-15", "")
    assert row["file_key"] == key and row["row_group"] == 0
    third = env.submit(SeriesRequest("MOW", "SEL", "2026-10-15"), client="app")
    assert third.cached and env.result(third)["tickets"] == tickets
    assert env.exists("MOW", "SEL", "2026-10-15", "", min_pages=3)
    assert not env.exists("MOW", "SEL", "2026-10-15", "max=1000")
    # Просят глубже, чем есть в неисчерпанной серии → не кэш.
    env.source.sizes[("MOW", "")] = [400, 400, 400]
    trunc = env.submit(SeriesRequest("MOW", None, "2026-10-15", max_pages=2), client="app")
    _run(env)
    assert trunc.done and not trunc.exhausted
    assert env.submit(SeriesRequest("MOW", None, "2026-10-15", max_pages=2), client="app").cached
    assert not env.submit(SeriesRequest("MOW", None, "2026-10-15", max_pages=3), client="app").cached


def test_stale_series_is_refetched(env):
    env.submit(SeriesRequest("SEL", None, "2026-10-15"), client="crawl")
    _run(env)
    env._now = lambda: datetime.now(timezone.utc) + timedelta(hours=2)   # TTL = 1 ч
    job = env.submit(SeriesRequest("SEL", None, "2026-10-15"), client="crawl")
    assert not job.cached
    _run(env)
    assert env.source.calls.count((("SEL", ""), 0)) == 2


def test_source_error_marks_series_and_is_not_cached(env):
    env.source.fail[("SEL", "")] = g.GraphQLError("city SEL not found")
    job = env.submit(SeriesRequest("SEL", None, "2026-10-15"), client="app")
    _run(env)
    assert job.done and job.error and job.status == "error"
    res = env.result(job)
    assert res["error"] and res["tickets"] == []
    row = env.index.get("SEL", None, "2026-10-15", "")
    assert row["error"] == 1 and not env.exists("SEL", None, "2026-10-15", "")
    assert env.index.coverage()[0][6] is True  # флаг error в покрытии для сборщика
    # Повторный запрос идёт в источник заново.
    again = env.submit(SeriesRequest("SEL", None, "2026-10-15"), client="app")
    assert not again.cached
    _run(env)
    assert again.done and not again.error and len(env.result(again)["tickets"]) == 3


def test_429_pauses_and_requeues_without_losing_pages(env):
    env.source.fail[("MOW", "SEL")] = g.RateLimited(retry_after=7)
    job = env.submit(SeriesRequest("MOW", "SEL", "2026-10-15"), client="app")
    t0 = env.clock()
    assert env.step() is job and job.pages == 0 and not job.done
    assert env.stats()["counters"]["http_429"] == 1
    assert env.step() is job and job.pages == 1
    assert env.clock() - t0 >= 7           # пауза Retry-After выдержана
    _run(env)
    assert job.done and job.pages == 3 and env.limiter.total_429 == 1


def test_network_error_retries_then_fails(env):
    import requests
    job = env.submit(SeriesRequest("SEL", None, "2026-10-15"), client="app")
    for _ in range(4):
        env.source.fail[("SEL", "")] = requests.ConnectionError("down")
        env.step()
    assert job.done and job.error.startswith("network") and env.stats()["counters"]["network_errors"] == 4


def test_retention_deletes_oldest_files(env):
    env.settings.lake_max_gb = 0  # 0 = без ретеншна
    days = ["2026-10-15", "2026-10-16", "2026-10-17"]
    for d in days:
        env.submit(SeriesRequest("MOW", "SEL", d), client="crawl")
        _run(env)
        env.writer.flush(force=True)
    files = env.index.files_oldest_first()
    assert len(files) == 3 and env.run_retention() == []
    total = env.index.files_bytes()
    env.settings.lake_max_gb = (total - 1) / (1024 ** 3)   # чуть меньше объёма → удалить старейший
    doomed = env.run_retention()
    assert doomed == [files[0]["key"]]
    assert not (Path(env.settings.lake_local_root) / files[0]["key"]).exists()
    assert env.index.get("MOW", "SEL", days[0], "") is None          # серия выпала из индекса
    assert env.index.get("MOW", "SEL", days[1], "") is not None
    assert len(env.index.files_oldest_first()) == 2


def test_reconcile_imports_unknown_files(env):
    env.submit(SeriesRequest("MOW", "SEL", "2026-10-15"), client="crawl")
    _run(env)
    key = env.writer.flush(force=True)
    size = env.index.files_bytes()
    fresh = Engine(env.settings, env.store, Index(":memory:"), page_fn=env.source)
    assert fresh.reconcile_files() == 1
    assert fresh.index.files_oldest_first()[0]["key"] == key and fresh.index.files_bytes() == size


def test_ticket_round_trip_through_parquet(tmp_path):
    tickets = [g.normalize_ticket(t, "MOW", None, "2026-10-15") for t in FIXTURE["mow_any"]]
    tickets = [t for t in tickets if t is not None]
    assert tickets
    store = LocalStore(str(tmp_path))
    index = Index(":memory:")
    writer = lake.LakeWriter(store, index)
    sid = index.put("MOW", None, "2026-10-15", "", pages=1, exhausted=True, tickets=len(tickets))
    writer.add(sid, tickets, "2026-09-28T00:00:00+00:00")
    key = writer.flush(force=True)
    assert writer.read(key, 0) == tickets
