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
                        s3_bucket=None, rate_per_minute=6000, lake_max_gb=1.0, ttl_seconds=3600)
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


def test_series_file_per_key_and_cache_hit_from_file(env):
    first = env.submit(SeriesRequest("MOW", "SEL", "2026-10-15"), client="app")
    _run(env)
    tickets = env.result(first)["tickets"]
    # Файл записан сразу по готовности, путь = ключ серии + момент загрузки.
    row = env.index.get("MOW", "SEL", "2026-10-15", "")
    key = row["file_key"]
    assert key.startswith("tickets/fetched=") and "/origin=MOW/MOW-SEL__2026-10-15__" in key
    assert key.endswith("Z.parquet")
    assert row["row_group"] == 0 and (Path(env.settings.lake_local_root) / key).exists()
    meta = lake.parse_file_key(key)
    assert (meta["origin"], meta["destination"], meta["day"], meta["params_key"]) == ("MOW", "SEL", "2026-10-15", "")
    assert env.index.files_bytes() > 0 and env.writer.files_written == 1
    # Повторный запрос — из файла range-чтением, без похода в источник.
    second = env.submit(SeriesRequest("MOW", "SEL", "2026-10-15"), client="app")
    assert second.cached and second.done
    assert env.result(second)["tickets"] == tickets and env.stats()["counters"]["cache_hit_app"] == 1
    assert env.source.calls.count((("MOW", "SEL"), 0)) == 1
    assert env.exists("MOW", "SEL", "2026-10-15", "", min_pages=3)
    assert not env.exists("MOW", "SEL", "2026-10-15", "max=1000")
    # ANY-конец и коридор цен — в пути.
    env.submit(SeriesRequest(None, "SEL", "2026-10-15", value_min=20000, value_max=40000), client="app")
    _run(env)
    key_any = env.index.get(None, "SEL", "2026-10-15", "min=20000&max=40000")["file_key"]
    assert "/origin=ANY/ANY-SEL__2026-10-15__min=20000,max=40000__" in key_any
    assert lake.parse_file_key(key_any)["params_key"] == "min=20000&max=40000"
    # Просят глубже, чем есть в неисчерпанной серии → не кэш.
    env.source.sizes[("MOW", "")] = [400, 400, 400]
    trunc = env.submit(SeriesRequest("MOW", None, "2026-10-15", max_pages=2), client="app")
    _run(env)
    assert trunc.done and not trunc.exhausted
    assert env.submit(SeriesRequest("MOW", None, "2026-10-15", max_pages=2), client="app").cached
    assert not env.submit(SeriesRequest("MOW", None, "2026-10-15", max_pages=3), client="app").cached


def test_empty_series_is_written_as_empty_file(env):
    env.source.sizes[("LED", "")] = [0]
    job = env.submit(SeriesRequest("LED", None, "2026-10-15"), client="crawl")
    _run(env)
    row = env.index.get("LED", None, "2026-10-15", "")
    assert job.done and row["tickets"] == 0 and row["file_key"].endswith("Z.parquet")
    assert env.writer.read(row["file_key"]) == []
    assert env.submit(SeriesRequest("LED", None, "2026-10-15"), client="app").cached


def test_refetch_keeps_history_as_new_file(env):
    env.submit(SeriesRequest("SEL", None, "2026-10-15"), client="crawl")
    _run(env)
    first_key = env.index.get("SEL", None, "2026-10-15", "")["file_key"]
    later = datetime.now(timezone.utc) + timedelta(hours=2)
    env._now = lambda: later
    env.submit(SeriesRequest("SEL", None, "2026-10-15"), client="crawl")
    _run(env)
    second_key = env.index.get("SEL", None, "2026-10-15", "")["file_key"]
    assert second_key != first_key and second_key.endswith(later.strftime("__%H-%M-%SZ.parquet"))
    assert f"/fetched={later:%Y-%m-%d}/" in second_key
    files = [f["key"] for f in env.index.files_oldest_first()]
    assert files == [first_key, second_key]   # старая копия осталась (история цен)


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
    key = env.index.get("MOW", "SEL", "2026-10-15", "")["file_key"]
    size = env.index.files_bytes()
    fresh = Engine(env.settings, env.store, Index(":memory:"), page_fn=env.source)
    assert fresh.reconcile_files() == 1
    imported = fresh.index.files_oldest_first()[0]
    assert imported["key"] == key and fresh.index.files_bytes() == size
    assert imported["created_at"] == lake.parse_file_key(key)["observed"].isoformat()


def test_ticket_round_trip_through_parquet(tmp_path):
    tickets = [g.normalize_ticket(t, "MOW", None, "2026-10-15") for t in FIXTURE["mow_any"]]
    tickets = [t for t in tickets if t is not None]
    assert tickets
    store = LocalStore(str(tmp_path))
    index = Index(":memory:")
    writer = lake.LakeWriter(store, index)
    sid = index.put("MOW", None, "2026-10-15", "", pages=1, exhausted=True, tickets=len(tickets))
    observed = datetime(2026, 9, 28, 16, 31, 35, tzinfo=timezone.utc)
    key = writer.write(sid, "MOW", None, "2026-10-15", "", tickets, observed)
    assert key == "tickets/fetched=2026-09-28/origin=MOW/MOW-ANY__2026-10-15__16-31-35Z.parquet"
    assert writer.read(key) == tickets
    assert lake.parse_file_key("tickets/observed=2026-09-28/part-x.parquet") is None
    assert lake.parse_file_key(key) == {"origin": "MOW", "destination": None, "day": "2026-10-15",
                                        "day_to": None, "params_key": "", "observed": observed}


def test_parse_legacy_date_layout_keys():
    old = lake.parse_file_key("tickets/date=2026-11-08/origin=XCR/XCR-ANY__to=2026-11-29__2026-09-30T07-48-26Z.parquet")
    assert (old["origin"], old["day"], old["day_to"], old["params_key"]) == ("XCR", "2026-11-08", "2026-11-29", "")
    assert old["observed"] == datetime(2026, 9, 30, 7, 48, 26, tzinfo=timezone.utc)
    corr = lake.parse_file_key("tickets/date=2026-10-15/origin=ANY/ANY-SEL__min=1,max=2__2026-09-28T16-32-01Z.parquet")
    assert (corr["origin"], corr["destination"], corr["day_to"], corr["params_key"]) == (None, "SEL", None, "min=1&max=2")
    new = lake.series_file_key(None, "SEL", "2026-10-15", "min=1&max=2", corr["observed"])
    assert new == "tickets/fetched=2026-09-28/origin=ANY/ANY-SEL__2026-10-15__min=1,max=2__16-32-01Z.parquet"
    assert lake.parse_file_key(new) == corr
    # Внутренние коды источника кириллицей (АУР, БИХ) — тоже по схеме.
    cyr = lake.parse_file_key("tickets/date=2026-09-28/origin=АУР/АУР-ANY__2026-09-28T18-33-46Z.parquet")
    assert (cyr["origin"], cyr["destination"], cyr["day"]) == ("АУР", None, "2026-09-28")
    moved = lake.series_file_key("АУР", None, cyr["day"], "", cyr["observed"])
    assert moved == "tickets/fetched=2026-09-28/origin=АУР/АУР-ANY__2026-09-28__18-33-46Z.parquet"
    assert lake.parse_file_key(moved)["origin"] == "АУР"


def test_lake_writer_pushes_file_bytes_to_tickets_store(tmp_path):
    """Записанный файл озера теми же байтами уходит в склад билетов (пуш — ускоритель,
    очередь ограничена, недоступный склад не мешает записи)."""
    from collector.push import TicketsPusher

    class FakeSession:
        def __init__(self):
            self.posts = []

        def post(self, url, params=None, data=None, headers=None, timeout=None):
            self.posts.append((url, params, data))

            class R:
                def raise_for_status(self):
                    pass
            return R()

    session = FakeSession()
    pusher = TicketsPusher("http://tickets:8002/", session=session)
    store = LocalStore(str(tmp_path / "lake"))
    index = Index(":memory:")
    writer = lake.LakeWriter(store, index, pusher=pusher)
    sid = index.put("MOW", None, "2026-10-15", "", pages=1, exhausted=True, tickets=1)
    observed = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
    ticket = g.normalize_ticket(_raw(1, "MOW", "SEL"), "MOW", None, "2026-10-15")
    key = writer.write(sid, "MOW", None, "2026-10-15", "", [ticket], observed)
    assert pusher._q.qsize() == 1
    k, data, obs = pusher._q.get_nowait()
    pusher.send(k, data, obs)
    url, params, body = session.posts[0]
    assert url == "http://tickets:8002/v1/files"
    assert params == {"key": key, "created_at": "2026-09-30T12:00:00+00:00"}
    assert body == writer.read_bytes(key) and body[:4] == b"PAR1"
    # переполнение очереди — файл просто не пушится (догонит сверка)
    small = TicketsPusher("http://tickets:8002", session=session, queue_size=1)
    assert small.push("a", b"x", observed) and not small.push("b", b"y", observed)
    assert small.stats()["dropped"] == 1
