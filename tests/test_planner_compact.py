"""Ленивый best-first и компактный результат планировщика.

Ленивый A* (в куче только лучший невыданный ребёнок узла) должен выдавать ровно те
же N самых дешёвых цепочек, что и полный перебор; компактный формат — нести те же
данные, что и словари Itinerary (дни, выходные, длина поездки, сегменты).
"""
import json
import random

from core.planner import (
    Stop,
    build_itineraries,
    build_itineraries_compact,
    compact_to_json,
)

CITY_INFO = lambda code: {"city": f"City {code}", "country": "", "flag": "🏳"}
CITIES = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]


def _flight(origin, dest, day, hour, price):
    return {
        "origin": origin, "origin_airport": origin,
        "destination": dest, "destination_airport": dest,
        "departure_at": f"2026-11-{day:02d}T{hour:02d}:00:00+03:00",
        "duration": 240, "price": price, "transfers": price % 2,
    }


def _random_case(seed):
    rnd = random.Random(seed)
    stops = [
        Stop("cities", ["MOW"], ["", ""]),
        Stop("any", [], ["2026-11-01", "2026-11-06"]),
        Stop("cities", ["SEL", "TYO"], ["2026-11-03", "2026-11-12"]),
        Stop("any", [], ["2026-11-08", "2026-11-16"]),
        Stop("cities", ["MOW"], ["", ""]),
    ]

    def leg(origins, dests, days):
        return [_flight(rnd.choice(origins), rnd.choice(dests), rnd.choice(days),
                        rnd.randint(0, 23), rnd.randint(1, 60) * 100)
                for _ in range(20)]  # полный перебор-эталон должен оставаться быстрым

    collected = {
        0: leg(["MOW"], CITIES, range(1, 7)),
        1: leg(CITIES, ["SEL", "TYO"], range(3, 13)),
        2: leg(["SEL", "TYO"], CITIES + ["MOW"], range(8, 17)),
        3: leg(CITIES + ["SEL"], ["MOW"], range(8, 20)),
    }
    return stops, collected


def _prices(itins):
    return [it["total_price"] for it in itins]


def test_lazy_search_matches_full_enumeration():
    for seed in range(8):
        stops, collected = _random_case(seed)
        full = build_itineraries(stops, collected, city_info=CITY_INFO)
        for n in (1, 7, 50, len(full), len(full) + 10):
            top = build_itineraries(stops, collected, city_info=CITY_INFO, max_results=n)
            assert _prices(top) == _prices(full)[:n], (seed, n)


def test_lazy_search_respects_max_cost():
    stops, collected = _random_case(3)
    full = build_itineraries(stops, collected, city_info=CITY_INFO)
    budget = full[len(full) // 2]["total_price"]
    top = build_itineraries(stops, collected, city_info=CITY_INFO, max_results=10_000, max_cost=budget)
    assert _prices(top) == [p for p in _prices(full) if p <= budget]


def test_compact_matches_itineraries():
    stops, collected = _random_case(5)
    itins = build_itineraries(stops, collected, city_info=CITY_INFO, max_results=40)
    res = json.loads(compact_to_json(
        build_itineraries_compact(stops, collected, max_results=40, city_info=CITY_INFO)))

    legs = res["legs"]
    assert res["format"] == "compact-v1"
    assert res["count"] == len(itins) and legs == len(stops) - 1
    for n, it in enumerate(itins):
        segs = [res["segments"][res["chains"][n * legs + k]] for k in range(legs)]
        assert segs == it["segments"]
        assert res["days"][n * (legs + 1):(n + 1) * (legs + 1)] == [s["days"] for s in it["stops"]]
        mask = res["weekend"][n]
        assert [bool(mask >> k & 1) for k in range(legs + 1)] == [s["weekendCovered"] for s in it["stops"]]
        assert res["total_days"][n] == it["total_days"]
    assert res["any_stops"] == [False, True, False, True, False]
    assert res["cities"]["MOW"] == ["City MOW", "🏳"]


def test_trip_length_is_first_departure_to_last_arrival():
    """Длина поездки не зависит от «пребывания» на концах: ни от начала окна до
    первого вылета, ни от условного пребывания в финальном городе."""
    stops = [
        Stop("cities", ["MOW"], ["", ""]),
        Stop("cities", ["IST"], ["2026-11-01", "2026-11-20"]),
        Stop("cities", ["MOW"], ["", ""]),
    ]
    collected = {0: [_flight("MOW", "IST", 5, 10, 100)], 1: [_flight("IST", "MOW", 9, 22, 100)]}
    (it,) = build_itineraries(stops, collected, city_info=CITY_INFO)
    assert it["total_days"] == 5  # 05.11 вылет → 10.11 прилёт (22:00 + 4 ч)
    res = build_itineraries_compact(stops, collected, max_results=10, city_info=CITY_INFO)
    assert list(res["total_days"]) == [5]


def test_compact_reuses_segments_across_chains():
    stops, collected = _random_case(1)
    res = build_itineraries_compact(stops, collected, max_results=200, city_info=CITY_INFO)
    assert res["count"] > 0
    assert len(res["segments"]) < res["count"] * res["legs"]  # сегменты не дублируются


def test_compact_empty_result():
    stops, _ = _random_case(0)
    res = json.loads(compact_to_json(
        build_itineraries_compact(stops, {}, max_results=10, city_info=CITY_INFO)))
    assert res["count"] == 0 and res["chains"] == [] and res["segments"] == []
