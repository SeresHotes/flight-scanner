"""Переезд в соседний город внутри остановки (core/nearby, Stop.radius_km).

Вена (VIE) и Братислава (BTS) — ~55 км: остановка «Вена, 100 км» разрешает прилететь
в Вену и улететь из Братиславы (и наоборот), с запасом на переезд HOP_MIN_GAP_MIN."""
from core import nearby, planner
from core.overview import build_overview
from core.planner import Stop, build_combo_routes, build_itineraries, build_itineraries_compact, materialize
from core.planquery import PlanQuery

CITY_INFO = lambda code: {"city": code, "country": "", "flag": ""}  # noqa: E731


def _flight(origin, dest, dep, dur=120, price=100):
    return {"origin": origin, "origin_airport": origin, "destination": dest, "destination_airport": dest,
            "departure_at": dep, "duration": dur, "price": price, "transfers": 0}


def _stops(radius):
    return [Stop("cities", ["MOW"], ["", ""]),
            Stop("cities", ["VIE"], ["2026-11-02", "2026-11-06"], radius_km=radius),
            Stop("cities", ["MOW"], ["", ""])]


# MOW→VIE 2.11 12:00–14:00; обратно дешевле из Братиславы
COLLECTED = {
    0: [_flight("MOW", "VIE", "2026-11-02T12:00:00+03:00", price=100)],
    1: [_flight("VIE", "MOW", "2026-11-05T10:00:00+01:00", price=500),
        _flight("BTS", "MOW", "2026-11-05T09:00:00+01:00", price=200),
        _flight("BTS", "MOW", "2026-11-02T15:00:00+01:00", price=50)],   # через час после прилёта — не успеть
}


def test_neighbors_by_city_coordinates():
    assert "BTS" in nearby.neighbors("VIE", 100)
    assert "BTS" not in nearby.neighbors("VIE", 30)
    assert nearby.neighbors("VIE", 0) == ()
    assert 40 < nearby.distance_km("VIE", "BTS") < 70


def test_collect_view_adds_neighbors_only_with_radius():
    assert planner.collect_view(_stops(0))[1].codes == ["VIE"]
    view = planner.collect_view(_stops(100))
    assert view[1].codes[0] == "VIE" and "BTS" in view[1].codes
    series = planner.plan_series(_stops(100))
    assert any(s[1] == "BTS" and s[2] == "MOW" for s in series)   # сбор BTS → MOW
    assert any(s[1] == "MOW" and s[2] == "BTS" for s in series)   # и прилёт в соседа


def test_no_radius_keeps_own_city_only():
    itins = build_itineraries(_stops(0), COLLECTED, city_info=CITY_INFO)
    assert [it["total_price"] for it in itins] == [600]
    assert "departFrom" not in itins[0]["stops"][1]


def test_hop_to_neighbor_with_gap():
    itins = build_itineraries(_stops(100), COLLECTED, city_info=CITY_INFO, max_results=10)
    prices = [it["total_price"] for it in itins]
    assert prices == [300, 600]            # 150 (через час) отсечён запасом на переезд
    hop = itins[0]["stops"][1]
    assert hop["code"] == "VIE"
    assert hop["departFrom"]["code"] == "BTS" and 40 < hop["departFrom"]["km"] < 70


def test_arrive_at_neighbor_depart_from_stop_city():
    collected = {
        0: [_flight("MOW", "BTS", "2026-11-02T12:00:00+03:00", price=80)],
        1: [_flight("VIE", "MOW", "2026-11-05T10:00:00+01:00", price=500)],
    }
    itins = build_itineraries(_stops(100), collected, city_info=CITY_INFO, max_results=10)
    assert len(itins) == 1
    assert itins[0]["stops"][1]["code"] == "BTS"
    assert itins[0]["stops"][1]["departFrom"]["code"] == "VIE"


def test_compact_materialize_and_combo_routes():
    res = build_itineraries_compact(_stops(100), COLLECTED, max_results=10, city_info=CITY_INFO)
    first = materialize(res, 0)
    assert first["total_price"] == 300 and first["stops"][1]["departFrom"]["code"] == "BTS"
    routes = build_combo_routes(_stops(100), COLLECTED, [["MOW", "VIE", "MOW"]], city_info=CITY_INFO)
    assert [r["total_price"] for r in routes] == [300, 600]


def test_overview_counts_hop_chains():
    ov = build_overview(_stops(100), COLLECTED, city_info=CITY_INFO)
    assert len(ov["combos"]) == 1
    c = ov["combos"][0]
    assert c["codes"] == ["MOW", "VIE", "MOW"] and c["count"] == 2 and c["minPrice"] == 300


def test_query_key_unchanged_without_radius():
    raw = {"stops": [{"kind": "cities", "codes": ["MOW"], "window": ["", ""]},
                     {"kind": "cities", "codes": ["VIE"], "window": ["2026-11-02", "2026-11-06"]}]}
    with_zero = {"stops": [dict(s, radiusKm=0) for s in raw["stops"]]}
    assert PlanQuery.from_dict(raw).key() == PlanQuery.from_dict(with_zero).key()
    with_radius = {"stops": [raw["stops"][0], dict(raw["stops"][1], radiusKm=100)]}
    q = PlanQuery.from_dict(with_radius)
    assert q.key() != PlanQuery.from_dict(raw).key()
    assert planner.parse_stops(q.stops)[1].radius_km == 100
