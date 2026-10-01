"""Окна дат: серия «город × диапазон дней» в коллекторе (раскладка по дням в индекс
и один файл озера, продолжение по цене за потолком offset) и нарезка окон в сборщике
по плотности города."""
from datetime import date, datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from collector import lake
from collector.config import Settings as CollectorSettings
from collector.engine import Engine, SeriesRequest
from collector.index import Index
from collector.main import create_app
from collector.ratelimit import RateLimiter
from collector.store import LocalStore
from core import graphql_api as g
from core.collector_client import CollectorClient
from crawler import main as crawler_main
from crawler.config import Settings
from crawler.schedule import Targets, plan
from tests.test_collector_engine import Clock

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
TODAY = date(2026, 9, 28)
T = Targets(near_days=14, near_hours=72, mid_days=60, mid_hours=72, far_hours=168, error_retry_hours=24)


def _raw(i, day, price, origin="MOW", dest="SEL"):
    dep = f"{day}T10:00:00+03:00"
    return {"value": price, "main_airline": "XX", "number_of_changes": 0, "departure_at": dep,
            "origin_city_iata": origin, "destination_city_iata": dest,
            "ticket_link": f"/MOW{i}SEL1?t={i}",
            "segments": [{"transfers": [], "flight_legs": [{
                "origin": "SVO", "destination": "ICN", "departure_at": dep,
                "arrival_at": f"{day}T23:00:00+09:00", "flight_number": str(i)}]}]}


class RangeSource:
    """Источник как у GraphQL: фильтр по окну дат и value_min, сортировка по цене,
    offset/limit; журнал вызовов (params, offset)."""

    def __init__(self, tickets):
        self.tickets = tickets
        self.calls = []

    def __call__(self, params, offset, limit):
        self.calls.append((dict(params), offset))
        lo, hi = params["depart_date_min"], params["depart_date_max"]
        vmin = params.get("value_min")
        rows = [t for t in self.tickets if lo <= t["departure_at"][:10] <= hi
                and (vmin is None or t["value"] >= vmin)]
        rows.sort(key=lambda t: (t["value"], t["ticket_link"]))
        return rows[offset:offset + limit]


def _engine(tmp_path, source):
    clock = Clock()
    settings = CollectorSettings(db_path=":memory:", lake_local_root=str(tmp_path / "lake"),
                                 s3_bucket=None, rate_per_minute=100_000, ttl_seconds=3600)
    return Engine(settings, LocalStore(settings.lake_local_root), Index(":memory:"), page_fn=source,
                  limiter=RateLimiter(100_000, clock=clock, sleep=clock.sleep), clock=clock,
                  sleep=clock.sleep)


def _run(engine, limit=500):
    for _ in range(limit):
        if engine.step() is None:
            return


def test_window_series_is_split_by_day_into_index_and_one_file(tmp_path):
    days = ["2026-10-15", "2026-10-16", "2026-10-17"]
    source = RangeSource([_raw(i, days[i % 2 * 2], 1000 + i) for i in range(5)])  # 15.10 и 17.10
    env = _engine(tmp_path, source)
    job = env.submit(SeriesRequest("MOW", None, days[0], day_to=days[-1], max_pages=100), client="crawl")
    _run(env)
    assert job.done and job.pages == 1 and len(source.calls) == 1
    assert source.calls[0][0]["depart_date_min"] == days[0] and source.calls[0][0]["depart_date_max"] == days[-1]
    rows = [env.index.get("MOW", None, d, "") for d in days]
    assert [r["tickets"] for r in rows] == [3, 0, 2]
    assert [r["pages"] for r in rows] == [1, 0, 0]            # страницы — на первом дне окна
    assert all(r["exhausted"] and r["file_key"] == rows[0]["file_key"] for r in rows)
    key = rows[0]["file_key"]
    assert key.startswith("tickets/fetched=") and "/origin=MOW/MOW-ANY__2026-10-15..2026-10-17__" in key
    meta = lake.parse_file_key(key)
    assert (meta["day"], meta["day_to"]) == ("2026-10-15", "2026-10-17")
    # Каждый день читается своей row group и отдаётся как кэш посуточной серии приложения.
    for d, n in zip(days, (3, 0, 2)):
        hit = env.submit(SeriesRequest("MOW", None, d), client="app")
        tickets = env.result(hit)["tickets"]
        assert hit.cached and len(tickets) == n
        assert all(t["departure_at"][:10] == d and t["search_date"] == d for t in tickets)
    assert env.writer.files_written == 1 and len(source.calls) == 1


def test_window_past_offset_ceiling_continues_by_price(tmp_path, monkeypatch):
    monkeypatch.setattr(g, "MAX_OFFSET", 800)                 # 3 страницы на сегмент
    days = ["2026-10-15", "2026-10-16"]
    # 2 000 билетов, цены с повторами (по 4 билета на цену) — граница цены режет пачку.
    source = RangeSource([_raw(i, days[i % 2], 1000 + i // 4) for i in range(2000)])
    env = _engine(tmp_path, source)
    job = env.submit(SeriesRequest("MOW", None, days[0], day_to=days[1], max_pages=100), client="crawl")
    _run(env)
    assert job.done and job.exhausted and env.counters.snapshot()["price_continuations"] >= 1
    got = sum(env.index.get("MOW", None, d, "")["tickets"] for d in days)
    assert got == 2000                                         # ни потерь, ни дублей
    floors = [c[0].get("value_min") for c in source.calls]
    assert floors[:3] == [None, None, None] and floors[3] is not None
    links = [t["link"] for d in days
             for t in env.result(env.submit(SeriesRequest("MOW", None, d), client="app"))["tickets"]]
    assert len(links) == len(set(links)) == 2000


def test_window_source_error_marks_every_day(tmp_path):
    def boom(params, offset, limit):
        raise g.GraphQLError("city XXX not found")
    env = _engine(tmp_path, boom)
    env.submit(SeriesRequest("XXX", None, "2026-10-15", day_to="2026-10-17"), client="crawl")
    _run(env)
    rows = [env.index.get("XXX", None, d, "") for d in ("2026-10-15", "2026-10-16", "2026-10-17")]
    assert all(r["error"] for r in rows)


def test_day_to_before_day_is_rejected():
    with pytest.raises(ValueError):
        SeriesRequest("MOW", None, "2026-10-15", day_to="2026-10-14")
    assert SeriesRequest("MOW", None, "2026-10-15", day_to="2026-10-15").day_to is None


# ------------------------------- сборщик -----------------------------------

def _row(origin, offset, age_h, tickets, exhausted=True, error=False):
    day = (TODAY + timedelta(days=offset)).isoformat()
    return [origin, day, (NOW - timedelta(hours=age_h)).isoformat(), 1, tickets, exhausted, error]


def test_windows_follow_city_density_and_freshness_tiers():
    cov = [_row("MOW", o, 200, 5000) for o in range(0, 181)]  # 5 000/день → окно 2 дня
    cov += [_row("VDY", o, 200, 3) for o in range(0, 181)]    # почти пусто → окно на весь уровень
    items, s = plan(["MOW", "VDY"], cov, today=TODAY, horizon_days=180, targets=T, now=NOW,
                    window_tickets=10_000)
    vdy = [(i.offset, i.days) for i in items if i.origin == "VDY"]
    assert sorted(vdy) == [(0, 61), (61, 120)]               # граница свежести на 60-м дне
    mow = [i for i in items if i.origin == "MOW"]
    assert all(i.days <= 2 for i in mow) and sum(i.days for i in mow) == 181
    assert s["due"] == 362 and s["windows"] == len(items)
    req = next(i for i in items if i.origin == "VDY" and i.offset == 0).request(100)
    assert req == {"origin": "VDY", "destination": None, "day": "2026-09-28",
                   "day_to": "2026-11-27", "max_pages": 100}


def test_windows_break_on_gaps_and_unknown_city_uses_default():
    cov = [_row("LED", o, 1, 10) for o in (3, 4)]             # свежие дни 3–4 — разрыв
    items, _ = plan(["LED", "NEW"], cov, today=TODAY, horizon_days=10, targets=T, now=NOW,
                    window_tickets=10_000, unknown_window_days=4)
    led = sorted((i.offset, i.days) for i in items if i.origin == "LED")
    assert led == [(0, 3), (5, 6)]
    new = sorted((i.offset, i.days) for i in items if i.origin == "NEW")
    assert new == [(0, 4), (4, 4), (8, 3)]


def test_truncated_series_is_refetched_after_retry_interval():
    cov = [_row("MOW", 0, 30, 4800, exhausted=False), _row("MOW", 1, 30, 4800)]
    items, s = plan(["MOW"], cov, today=TODAY, horizon_days=1, targets=T, now=NOW)
    # обрезанная — «пора»; свежая соседка старше суток — вторым эшелоном (refresh), отдельным окном
    assert [(i.offset, i.reason) for i in items] == [(0, "truncated"), (1, "refresh")] and s["truncated"] == 1


def test_tick_submits_windows_and_collector_splits_them(tmp_path):
    days = [(TODAY + timedelta(days=o)).isoformat() for o in range(3)]
    source = RangeSource([_raw(i, days[i % 3], 1000 + i) for i in range(9)])
    csettings = CollectorSettings(db_path=":memory:", lake_local_root=str(tmp_path / "lake"),
                                  s3_bucket=None, rate_per_minute=100_000)
    engine = Engine(csettings, LocalStore(csettings.lake_local_root), Index(":memory:"), page_fn=source)
    settings = Settings(collector_url="http://testserver", seeds=["MOW"], horizon_days=2,
                        queue_target=5, max_pages=100, window_tickets=10_000, unknown_window_days=30)
    with TestClient(create_app(engine)) as c:
        client = CollectorClient("http://testserver", http=c, poll_wait=1)
        s = crawler_main.tick(client, settings, today=TODAY, now=NOW)
        assert s["submitted"] == 1 and s["windows"] == 1 and s["due"] == 3
        import time
        for _ in range(100):
            if c.get("/v1/health").json()["series"] >= 3:
                break
            time.sleep(0.05)
        rows = {(r[0], r[1]): r[4] for r in c.get("/v1/coverage").json()["rows"]}
        assert rows == {("MOW", d): 3 for d in days}
        assert len(source.calls) == 1


def test_estimate_pages_follows_density_and_window_length():
    from crawler.schedule import densities, estimate_pages
    assert estimate_pages(None, 30) == 1 and estimate_pages(3, 60) == 1
    assert estimate_pages(5000, 2) == 25 and estimate_pages(401, 1) == 2
    cov = [_row("MOW", o, 200, 5000) for o in range(10)] + [_row("X", 0, 1, 0, error=True)]
    assert densities(cov) == {"MOW": 5000}
    items, _ = plan(["MOW"], cov, today=TODAY, horizon_days=9, targets=T, now=NOW, window_tickets=10_000)
    assert all(i.est_pages == 25 for i in items)


def test_tick_fills_queue_by_estimated_pages(tmp_path):
    csettings = CollectorSettings(db_path=":memory:", lake_local_root=str(tmp_path / "lake"),
                                  s3_bucket=None, rate_per_minute=100_000)
    engine = Engine(csettings, LocalStore(csettings.lake_local_root), Index(":memory:"),
                    page_fn=RangeSource([]))
    engine.start = lambda: None                                # воркер не выбирает очередь
    seeds = [f"Q{chr(65 + i // 26)}{chr(65 + i % 26)}" for i in range(20)]
    settings = Settings(collector_url="http://testserver", seeds=seeds, horizon_days=2,
                        queue_pages=5, queue_target=100, max_pages=100, window_tickets=10_000)
    with TestClient(create_app(engine)) as c:
        client = CollectorClient("http://testserver", http=c, poll_wait=1)
        s = crawler_main.tick(client, settings, today=TODAY, now=NOW)
        # 20 городов без данных — окна по одной оценочной странице: подаём ровно 5.
        assert s["submitted"] == 5 and s["submitted_pages_est"] == 5 and s["queued_pages_est"] == 0
        s2 = crawler_main.tick(client, settings, today=TODAY, now=NOW)
        assert s2["queued_pages_est"] == 5 and s2["submitted"] == 0
