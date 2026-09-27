"""core/overview.build_overview: наборы городов совпадают с полным перебором A*.

Перекрёстная проверка: по каждому набору городов число цепочек и минимальная цена
из динамики равны тем же величинам, посчитанным по цепочкам build_itineraries с
теми же фильтрами (max_results большой — перебор полный)."""
import random
from collections import defaultdict

import pytest

from core.overview import build_overview
from core.planner import Stop, build_itineraries
from core.planquery import PlanQuery

CITY = lambda code: {"city": code, "country": "", "flag": ""}  # noqa: E731


def _flight(origin, dest, dep, arr, price, transfers=0, baggage=True, number="1"):
    return {
        "origin": origin, "origin_airport": origin, "destination": dest, "destination_airport": dest,
        "departure_at": dep, "arrival_at": arr, "duration": 300, "duration_to": 300,
        "price": price, "transfers": transfers, "airline": "XX", "flight_number": number, "link": "",
        "chain": [origin, dest], "legs": [], "transfer_points": [],
        "baggage": {"known": True, "included": baggage, "pieces": None, "kg": None},
    }


def _random_collected(seed, stops, days=tuple("2026-11-0%d" % d for d in range(1, 8))):
    rnd = random.Random(seed)
    days = list(days)
    cities = {0: ["MOW"], 1: ["IST", "DXB", "DOH", "AUH"], 2: ["SEL"], 3: ["HKG", "BKK", "SIN", "IST"], 4: ["MOW"]}
    collected = {}
    for i in range(len(stops) - 1):
        flights = []
        for o in cities[i]:
            for d in cities[i + 1]:
                if o == d:
                    continue
                for _ in range(rnd.randint(1, 4)):
                    day = rnd.choice(days)
                    h = rnd.randint(0, 22)
                    dep = f"{day}T{h:02d}:00:00+03:00"
                    arr_day = day if h < 18 else days[min(len(days) - 1, days.index(day) + 1)]
                    arr = f"{arr_day}T{(h + 5) % 24:02d}:30:00+03:00"
                    flights.append(_flight(o, d, dep, arr, rnd.randint(50, 500), rnd.randint(0, 2),
                                           baggage=rnd.random() < 0.6, number=str(len(flights))))
        collected[i] = flights
    return collected


STOPS = [Stop("cities", ["MOW"], ["", ""]), Stop("any", [], ["2026-11-01", "2026-11-07"]),
         Stop("cities", ["SEL"], ["2026-11-01", "2026-11-07"]), Stop("any", [], ["2026-11-01", "2026-11-07"]),
         Stop("cities", ["MOW"], ["", ""])]


def _query(extra):
    return PlanQuery.from_dict({"stops": [{"kind": s.kind, "codes": s.codes, "window": s.window} for s in STOPS],
                                "maxResults": 100000, **extra})


def _expected(collected, q):
    itins = build_itineraries(STOPS, collected, city_info=CITY, max_results=100000, query=q)
    by_combo = defaultdict(lambda: {"count": 0, "minPrice": float("inf"), "minTransfers": 99})
    for it in itins:
        key = tuple(s["code"] for s in it["stops"])
        agg = by_combo[key]
        agg["count"] += 1
        if it["total_price"] < agg["minPrice"]:
            agg["minPrice"] = it["total_price"]
            agg["transfersAtMin"] = it["total_transfers"]
        agg["minTransfers"] = min(agg["minTransfers"], it["total_transfers"])
    return by_combo, len(itins)


@pytest.mark.parametrize("seed,extra", [
    (1, {}),
    (2, {"cities": [{}, {"minStay": 1, "maxStay": 3}, {"requireWeekend": True}, {"minStay": 0}, {}]}),
    (3, {"legs": [{"maxTransfers": 1}, {"baggage": "included"}, {}, {"maxTransfers": 0}]}),
    (4, {"tripLength": [3, 6]}),
    (5, {"cities": [{}, {"mustCover": ["2026-11-02", "2026-11-03"]}, {}, {"maxStay": 2}, {}],
         "tripLength": [2, None], "legs": [{}, {}, {"maxTransfers": 1}, {}]}),
])
def test_overview_matches_full_enumeration(seed, extra):
    collected = _random_collected(seed, STOPS)
    q = _query(extra)
    got = build_overview(STOPS, collected, q, city_info=CITY)
    expected, total = _expected(collected, q)
    assert got["totalCount"] == total
    assert {tuple(c["codes"]) for c in got["combos"]} == set(expected)
    for c in got["combos"]:
        e = expected[tuple(c["codes"])]
        assert c["count"] == e["count"], c
        assert c["minPrice"] == e["minPrice"], c
        assert c["minTransfers"] == e["minTransfers"], c
    prices = [c["minPrice"] for c in got["combos"]]
    assert prices == sorted(prices)
    assert set(got["cities"]) == {code for c in got["combos"] for code in c["codes"]}


def test_overview_empty_and_two_stops():
    two = [Stop("cities", ["MOW"], ["", ""]), Stop("cities", ["IST", "DXB"], ["2026-11-01", "2026-11-02"])]
    collected = {0: [_flight("MOW", "IST", "2026-11-01T10:00:00", "2026-11-01T14:00:00", 100),
                     _flight("MOW", "IST", "2026-11-02T10:00:00", "2026-11-02T14:00:00", 90, transfers=1),
                     _flight("MOW", "DXB", "2026-11-02T10:00:00", "2026-11-02T15:00:00", 120)]}
    q = PlanQuery.from_dict({"stops": [{"kind": s.kind, "codes": s.codes, "window": s.window} for s in two],
                             "maxResults": 10})
    got = build_overview(two, collected, q, city_info=CITY)
    assert got["combos"] == [
        {"codes": ["MOW", "IST"], "minPrice": 90.0, "transfersAtMin": 1, "minTransfers": 0, "count": 2},
        {"codes": ["MOW", "DXB"], "minPrice": 120.0, "transfersAtMin": 0, "minTransfers": 0, "count": 1},
    ]
    assert build_overview(two, {}, q, city_info=CITY)["combos"] == []
