"""Фоновый сборщик: проход по календарю дат вылета (все города по дню, окна по плотности),
курсор прохода, карантин городов с ошибками, исключение серий из очереди, тики против
коллектора (TestClient) со сменой прохода."""
import time
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
from crawler.schedule import merge_cities, sweep
from tests.test_collector_engine import Source

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
TODAY = date(2026, 9, 28)
PASS = NOW - timedelta(hours=10)  # начало текущего прохода


def _row(origin, offset, age_h, error=False, tickets=5):
    day = (TODAY + timedelta(days=offset)).isoformat()
    return [origin, day, (NOW - timedelta(hours=age_h)).isoformat(), 1, tickets, True, error]


def test_merge_cities_seeds_first_then_by_tickets():
    known = [{"code": "sel", "tickets": 10}, {"code": "IST", "tickets": 900}, {"code": "MOW", "tickets": 5},
             {"code": "X", "tickets": 1}, {"code": "ANY", "tickets": 3}]
    assert merge_cities(["MOW", "LED"], known) == ["MOW", "LED", "IST", "SEL"]


def test_calendar_order_all_cities_per_day():
    items, s = sweep(["MOW", "SEL"], [], today=TODAY, horizon_days=3, pass_started=PASS, window_tickets=None)
    assert [(i.origin, i.offset) for i in items] == [("MOW", 0), ("SEL", 0), ("MOW", 1), ("SEL", 1),
                                                     ("MOW", 2), ("SEL", 2), ("MOW", 3), ("SEL", 3)]
    assert all(i.reason == "missing" for i in items)
    assert s["pairs"] == 8 and s["missing"] == 8 and s["done"] == 0 and s["due"] == 8
    assert s["sweep_offset"] == 0 and s["sweep_day"] == "2026-09-28" and not s["pass_done"]


def test_cursor_is_first_day_not_done_in_this_pass():
    # дни 0–1 обработаны в этом проходе (5 ч назад), день 2 — с прошлого (30 ч), день 3 — тоже
    cov = [_row(c, o, 5) for c in ("MOW", "SEL") for o in (0, 1)]
    cov += [_row(c, o, 30) for c in ("MOW", "SEL") for o in (2, 3)]
    items, s = sweep(["MOW", "SEL"], cov, today=TODAY, horizon_days=3, pass_started=PASS, window_tickets=None)
    assert [(i.origin, i.offset, i.reason) for i in items] == [
        ("MOW", 2, "update"), ("SEL", 2, "update"), ("MOW", 3, "update"), ("SEL", 3, "update")]
    assert s["done"] == 4 and s["update"] == 4 and s["sweep_offset"] == 2 and s["pass_progress"] == 0.5
    # всё обработано — проход окончен, подавать нечего
    done = [_row(c, o, 5) for c in ("MOW", "SEL") for o in range(4)]
    items, s = sweep(["MOW", "SEL"], done, today=TODAY, horizon_days=3, pass_started=PASS)
    assert items == [] and s["pass_done"] and s["sweep_offset"] == 4 and s["sweep_day"] is None


def test_small_city_window_runs_ahead_of_cursor():
    # маленький город берёт весь горизонт одним окном; курсор держит Москва (по дню)
    cov = [_row("MOW", o, 30, tickets=9000) for o in range(6)] + [_row("VDY", o, 30, tickets=2) for o in range(6)]
    items, s = sweep(["MOW", "VDY"], cov, today=TODAY, horizon_days=5, pass_started=PASS)
    assert [(i.origin, i.offset, i.days) for i in items][:3] == [("MOW", 0, 1), ("VDY", 0, 6), ("MOW", 1, 1)]


def test_errors_count_as_done_and_quarantine_city():
    cov = [_row("MOW", 0, 1, error=True), _row("MOW", 1, 30, error=True), _row("MOW", 2, 30)]
    cov += [_row("XXX", o, 30, error=True, tickets=0) for o in (0, 1, 2)]
    items, s = sweep(["MOW", "XXX"], cov, today=TODAY, horizon_days=2, pass_started=PASS, window_tickets=None)
    mow = [i.offset for i in items if i.origin == "MOW"]
    assert mow == [1, 2]                                      # ошибка в этом проходе — до следующего
    xxx = [(i.offset, i.reason) for i in items if i.origin == "XXX"]
    assert xxx == [(0, "quarantine")]                         # карантин: одна проба (сегодня)
    assert s["quarantined_cities"] == 1 and s["errors"] == 3 and s["pairs"] == 3 + 1


def test_exclude_queued():
    items, s = sweep(["MOW"], [], today=TODAY, horizon_days=4, pass_started=PASS,
                     exclude=[("MOW", TODAY.isoformat())], window_tickets=None)
    assert [i.offset for i in items] == [1, 2, 3, 4] and s["queued_excluded"] == 1
    assert s["sweep_offset"] == 0                             # в очереди — ещё не обработан


def _wait_series(c, n):
    for _ in range(100):
        if c.get("/v1/health").json()["series"] >= n:
            return
        time.sleep(0.05)


def test_tick_sweeps_and_starts_next_pass(tmp_path):
    csettings = CollectorSettings(db_path=":memory:", lake_local_root=str(tmp_path / "lake"),
                                  s3_bucket=None, rate_per_minute=100_000)
    source = Source({("MOW", ""): [3], ("LED", ""): [0]})
    engine = Engine(csettings, LocalStore(csettings.lake_local_root), Index(":memory:"), page_fn=source)
    # SEL — заранее в списке: он же приходит направлением в билетах MOW (иначе новый город
    # посреди прохода вернул бы курсор на начало — это отдельный тест ниже)
    settings = Settings(collector_url="http://testserver", seeds=["MOW", "LED", "SEL"], horizon_days=1,
                        queue_target=3, max_pages=5, window_tickets=0, min_pass_hours=24)
    with TestClient(create_app(engine)) as c:
        client = CollectorClient("http://testserver", http=c, poll_wait=1)
        start = datetime.now(timezone.utc) - timedelta(minutes=1)  # серии коллектор пишет по реальным часам
        s = crawler_main.tick(client, settings, today=TODAY, now=start)
        assert s["pass"] == 1 and s["submitted"] == 3 and s["missing"] == 6 and s["sweep_offset"] == 0
        assert client.crawler_state()["pass_started"] == start.isoformat(timespec="seconds")
        assert c.get("/v1/stats").json()["crawler"]["submitted"] == 3
        _wait_series(c, 3)
        # день 0 у всех обработан — курсор на дне 1, подаётся день 1 всех городов
        s2 = crawler_main.tick(client, settings, today=TODAY, now=start + timedelta(minutes=2))
        assert s2["sweep_offset"] == 1 and s2["submitted"] == 3 and s2["done"] == 3 and s2["cities"] == 3
        _wait_series(c, 6)
        # проход окончен, но младше суток — новый не начинается
        s3 = crawler_main.tick(client, settings, today=TODAY, now=start + timedelta(hours=1))
        assert s3["pass_done"] and s3["pass"] == 1 and s3["submitted"] == 0
        # через сутки — проход 2 с начала горизонта (кэш коллектора не мешает: ttl от начала прохода)
        later = start + timedelta(hours=25)
        s4 = crawler_main.tick(client, settings, today=TODAY, now=later)
        assert s4["pass"] == 2 and s4["sweep_offset"] == 0 and s4["submitted"] == 3
        assert s4["previous_pass_hours"] == 25.0


def test_new_city_mid_pass_is_caught_up_from_start():
    cov = [_row("MOW", o, 5) for o in range(3)] + [_row("MOW", 3, 30)]
    items, s = sweep(["MOW", "NEW"], cov, today=TODAY, horizon_days=3, pass_started=PASS, window_tickets=None)
    assert [(i.origin, i.offset) for i in items] == [("NEW", 0), ("NEW", 1), ("NEW", 2), ("MOW", 3), ("NEW", 3)]
    assert s["sweep_offset"] == 0
