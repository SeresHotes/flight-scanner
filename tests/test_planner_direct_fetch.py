"""Сбор плеча: прямые рейсы запрашиваются отдельно (direct=true).

prices_for_dates с direct=false отдаёт по направлению один самый дешёвый билет на
дату. Если он стыковочный, прямой рейс чуть дороже в ответ не попадает — поэтому
collect_plan на каждый день делает и запрос «только прямые», склеивая ответы без
дублей. Оценка числа запросов учитывает оба режима.
"""
from core.planner import Stop, collect_plan, estimate_plan, request_count

CITY_INFO = lambda code: {"city": code, "country": "", "flag": ""}

STOPS = [
    Stop("cities", ["MOW"], ["", ""]),
    Stop("cities", ["TAO"], ["2026-10-27", "2026-10-28"]),
]


def _flight(dest, dep, price, transfers, number):
    return {
        "origin": "MOW", "origin_airport": "SVO",
        "destination": dest, "destination_airport": dest,
        "departure_at": dep, "price": price, "transfers": transfers,
        "airline": "XX", "flight_number": number, "duration": 500,
    }


CONNECTING = _flight("TAO", "2026-10-27T23:15:00+03:00", 25664, 1, "8004")
DIRECT = _flight("TAO", "2026-10-27T18:40:00+03:00", 28711, 0, "496")
DIRECT_28 = _flight("TAO", "2026-10-28T18:40:00+03:00", 20000, 0, "496")
OTHER_CITY = _flight("PEK", "2026-10-27T10:00:00+03:00", 15000, 0, "100")


def _fake_fetch(calls):
    def fetch(origin=None, destination=None, departure_at=None, allow_indirect=False, **_):
        calls.append((origin, destination, departure_at, allow_indirect))
        if departure_at == "2026-10-27":
            # С пересадками — только самый дешёвый; прямой — лишь в direct=true.
            return {"data": [dict(CONNECTING), dict(OTHER_CITY)] if allow_indirect
                    else [dict(DIRECT), dict(OTHER_CITY)]}
        # 28-го самый дешёвый — прямой: он приходит в обоих ответах.
        return {"data": [dict(DIRECT_28)]}
    return fetch


def test_collect_requests_direct_and_indirect_for_each_day():
    calls = []
    collect_plan(STOPS, fetch_fn=_fake_fetch(calls))
    assert sorted(calls) == sorted([
        ("MOW", None, "2026-10-27", True), ("MOW", None, "2026-10-27", False),
        ("MOW", None, "2026-10-28", True), ("MOW", None, "2026-10-28", False),
    ])


def test_collect_keeps_direct_behind_cheaper_connection_and_dedupes():
    collected = collect_plan(STOPS, fetch_fn=_fake_fetch([]))
    got = sorted((f["departure_at"], f["flight_number"]) for f in collected[0])
    assert got == [
        ("2026-10-27T18:40:00+03:00", "496"),   # прямой, дороже стыковочного
        ("2026-10-27T23:15:00+03:00", "8004"),  # стыковочный
        ("2026-10-28T18:40:00+03:00", "496"),   # пришёл дважды — оставлен один
    ]


def test_estimate_counts_both_modes():
    est = estimate_plan(STOPS, CITY_INFO)
    assert est["legs"][0]["requests"] == 4  # 1 якорь × 2 дня × 2 режима
    assert est["requests"] == request_count(STOPS) == 4
