"""Фоновый сборщик: срочность пар «город × день», карантин городов с ошибками,
исключение серий из очереди, один тик против коллектора (TestClient)."""
from datetime import date, datetime, timedelta, timezone

from fastapi.testclient import TestClient

from collector.config import Settings as CollectorSettings
from collector.engine import Engine
from collector.index import Index
from collector.main import create_app
from collector.store import LocalStore
from core.collector_client import CollectorClient
from crawler import main as crawler_main
from crawler.config import Settings
from crawler.schedule import Targets, merge_cities, plan
from tests.test_collector_engine import Source

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
TODAY = date(2026, 9, 28)
T = Targets(near_days=14, near_hours=24, mid_days=60, mid_hours=72, far_hours=168, error_retry_hours=24)


def _row(origin, offset, age_h, error=False, tickets=5):
    day = (TODAY + timedelta(days=offset)).isoformat()
    return [origin, day, (NOW - timedelta(hours=age_h)).isoformat(), 1, tickets, True, error]


def test_targets_by_horizon():
    assert (T.hours(0), T.hours(14), T.hours(15), T.hours(60), T.hours(61)) == (24, 24, 72, 72, 168)


def test_merge_cities_seeds_first_then_by_tickets():
    known = [{"code": "sel", "tickets": 10}, {"code": "IST", "tickets": 900}, {"code": "MOW", "tickets": 5},
             {"code": "X", "tickets": 1}, {"code": "ANY", "tickets": 3}]
    assert merge_cities(["MOW", "LED"], known) == ["MOW", "LED", "IST", "SEL"]


def test_missing_pairs_go_near_to_far_and_big_cities_first():
    items, s = plan(["MOW", "SEL"], [], today=TODAY, horizon_days=3, targets=T, now=NOW)
    assert [(i.origin, i.offset) for i in items] == [("MOW", 0), ("SEL", 0), ("MOW", 1), ("SEL", 1),
                                                     ("MOW", 2), ("SEL", 2), ("MOW", 3), ("SEL", 3)]
    assert all(i.reason == "missing" for i in items)
    assert s["pairs"] == 8 and s["missing"] == 8 and s["pass_progress"] == 0 and s["due"] == 8


def test_stale_near_beats_missing_far_only_after_double_target():
    cov = [_row("MOW", 0, age_h=30), _row("MOW", 1, age_h=75)]   # 1.25× и 3.1× цели (24 ч)
    items, s = plan(["MOW"], cov, today=TODAY, horizon_days=100, targets=T, now=NOW)
    order = [(i.offset, i.reason) for i in items]
    # Отсутствующие пары — 2…3 по дальности; день 1 (3.1) обгоняет их все,
    # день 0 (1.25) идёт после всех отсутствующих.
    assert order[0] == (1, "stale") and order[1] == (2, "missing") and order[-1] == (0, "stale")
    assert s["stale"] == 2 and s["fresh"] == 0 and s["missing"] == 99


def test_fresh_pairs_are_skipped_and_far_target_is_a_week():
    cov = [_row("MOW", o, age_h=100) for o in range(0, 181)]  # 100 ч: ближние/средние устарели, дальние свежие
    items, s = plan(["MOW"], cov, today=TODAY, horizon_days=180, targets=T, now=NOW)
    assert s["missing"] == 0 and s["stale"] == 61 and s["fresh"] == 120
    assert max(i.offset for i in items) == 60 and min(i.score for i in items) > 1


def test_error_rows_retry_after_interval_and_quarantine_city():
    cov = [_row("MOW", 0, age_h=1, error=True), _row("MOW", 1, age_h=30, error=True)]
    cov += [_row("XXX", o, age_h=30, error=True, tickets=0) for o in (0, 1, 2)]
    items, s = plan(["MOW", "XXX"], cov, today=TODAY, horizon_days=3, targets=T, now=NOW)
    mow = [(i.offset, i.reason) for i in items if i.origin == "MOW"]
    assert (0, "error") not in mow and (1, "error") in mow          # 1 ч — рано, 30 ч — пора
    xxx = [(i.offset, i.reason) for i in items if i.origin == "XXX"]
    assert xxx == [(1, "error")]                                      # карантин: одна проба
    assert s["quarantined_cities"] == 1 and s["errors"] == 3 and s["pairs"] == 4 + 1


def test_exclude_queued_and_limit():
    items, s = plan(["MOW"], [], today=TODAY, horizon_days=5, targets=T, now=NOW,
                    exclude=[("MOW", TODAY.isoformat())], limit=2)
    assert [i.offset for i in items] == [1, 2] and s["queued_excluded"] == 1 and s["due"] == 5


def test_tick_submits_missing_series_and_reports(tmp_path):
    csettings = CollectorSettings(db_path=":memory:", lake_local_root=str(tmp_path / "lake"),
                                  s3_bucket=None, rate_per_minute=100_000)
    source = Source({("MOW", ""): [3], ("LED", ""): [0]})
    engine = Engine(csettings, LocalStore(csettings.lake_local_root), Index(":memory:"), page_fn=source)
    settings = Settings(collector_url="http://testserver", seeds=["MOW", "LED"], horizon_days=2,
                        queue_target=3, max_pages=5, window_tickets=0)
    with TestClient(create_app(engine)) as c:
        client = CollectorClient("http://testserver", http=c, poll_wait=1)
        s = crawler_main.tick(client, settings, today=TODAY, now=NOW)
        assert s["submitted"] == 3 and s["missing"] == 6 and s["queued_crawl"] == 0
        assert s["cities"] == 2 and s["horizon_days"] == 2
        assert c.get("/v1/stats").json()["crawler"]["submitted"] == 3
        # Дождаться выполнения: 3 серии × 1 страница.
        for _ in range(50):
            if c.get("/v1/health").json()["series"] >= 3:
                break
            import time
            time.sleep(0.05)
        rows = c.get("/v1/coverage").json()["rows"]
        assert sorted((r[0], r[1]) for r in rows) == [("LED", "2026-09-28"), ("MOW", "2026-09-28"),
                                                      ("MOW", "2026-09-29")]
        # Второй тик: три пары свежие, остались три отсутствующие; новых городов из билетов (SEL, BKK)
        # добавились в список.
        s2 = crawler_main.tick(client, settings, today=TODAY, now=NOW + timedelta(seconds=1))
        assert s2["submitted"] == 3 and s2["fresh"] == 3 and s2["cities"] >= 3
