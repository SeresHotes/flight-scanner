"""BFS-сбор X → ANY: окна дат «неделя через месяц», порядок обхода, лимиты, резюм."""
from datetime import date

from core import anyscan


def test_date_windows_week_per_month():
    dates = anyscan.date_windows(date(2026, 10, 5), months=3, days=7)
    assert len(dates) == 21
    assert dates[:7] == [f"2026-10-{d:02d}" for d in range(5, 12)]
    assert dates[7] == "2026-11-05" and dates[14] == "2026-12-05"


def test_date_windows_clamps_month_end():
    dates = anyscan.date_windows(date(2026, 1, 31), months=2, days=1)
    assert dates == ["2026-01-31", "2026-02-28"]


def _flight(origin, dest, day, price, transfers=0, number=0):
    return {"origin": origin, "origin_airport": origin, "destination": dest,
            "destination_airport": dest, "departure_at": f"{day}T10:00:00",
            "price": price, "transfers": transfers, "airline": "XX",
            "flight_number": f"{origin}{dest}{number}", "duration": 100}


# из MOW в IST два билета (хаб), в EVN один → IST раньше EVN несмотря на алфавит
NEIGHBOURS = {"MOW": ["IST", "IST", "EVN"], "EVN": ["MOW", "BKK"], "IST": ["MOW"], "BKK": []}


def _fake_fetch(calls):
    def fetch(origin=None, destination=None, departure_at=None, allow_indirect=False, **_):
        calls.append((origin, destination, departure_at, allow_indirect))
        # в обоих режимах один и тот же прямой — должен схлопнуться в один билет
        return {"data": [_flight(origin, d, departure_at, 100 + i, number=i)
                         for i, d in enumerate(NEIGHBOURS[origin])]}
    return fetch


def test_bfs_order_and_dedup():
    calls, saved = [], []
    state = anyscan.ScanState.initial(["2026-10-05", "2026-10-06"], "MOW")
    anyscan.scan_any(state, fetch_fn=_fake_fetch(calls),
                     on_city_done=lambda c, f: saved.append((c, len(f))))
    assert state.done == ["MOW", "IST", "EVN", "BKK"]   # в ширину, хабы (IST) первыми
    assert state.queue == []
    # на город: 2 дня × 2 режима; всего 4 города
    assert state.requests == 16 and len(calls) == 16
    assert calls[:2] == [("MOW", None, "2026-10-05", True), ("MOW", None, "2026-10-06", True)]
    assert saved[0] == ("MOW", 6)  # 3 билета × 2 дня, дубли режимов схлопнуты


def test_limits_and_resume():
    calls = []
    state = anyscan.ScanState.initial(["2026-10-05"], "MOW")
    anyscan.scan_any(state, fetch_fn=_fake_fetch(calls), max_cities=1)
    assert state.done == ["MOW"] and state.queue == ["IST", "EVN"]

    restored = anyscan.ScanState.from_dict(state.as_dict())
    anyscan.scan_any(restored, fetch_fn=_fake_fetch(calls), max_requests=4)
    # 2 запроса на город: MOW уже сделан (2), ещё влезает один город (IST)
    assert restored.done == ["MOW", "IST"] and restored.queue == ["EVN"]
    assert restored.requests == 4


def test_should_stop_between_cities():
    state = anyscan.ScanState.initial(["2026-10-05"], "MOW")
    stops = iter([False, True])
    anyscan.scan_any(state, fetch_fn=_fake_fetch([]), should_stop=lambda: next(stops))
    assert state.done == ["MOW"]
