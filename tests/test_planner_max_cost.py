"""Движковые границы планировщика: max_cost (бюджет) и max_results (потолок числа).

max_cost должен резать целые бесперспективные направления по admissible-оценке
минимального «хвоста» (_completion_lb), а не только фильтровать готовый результат.
max_results — оставлять N самых дешёвых цепочек. Без обеих границ — прежнее «всё».
"""
from core.planner import Stop, _completion_lb, _index_leg, build_itineraries

CITY_INFO = lambda code: {"city": code, "country": "", "flag": ""}


def _flight(origin, dest, dep, price, dur=180):
    return {
        "origin": origin, "origin_airport": origin,
        "destination": dest, "destination_airport": dest,
        "departure_at": dep, "duration": dur, "price": price, "transfers": 0,
    }


# MOW → (любой) → DST. Через IST собрать дёшево (200), через NYC — дорого (1000).
STOPS = [
    Stop("cities", ["MOW"], ["", ""]),
    Stop("any", [], ["2026-11-01", "2026-11-10"]),
    Stop("cities", ["DST"], ["", ""]),
]
COLLECTED = {
    0: [
        _flight("MOW", "IST", "2026-11-02T10:00:00", price=100),
        _flight("MOW", "NYC", "2026-11-02T10:00:00", price=100),
    ],
    1: [
        _flight("IST", "DST", "2026-11-05T10:00:00", price=100),   # хвост 100 → итого 200
        _flight("NYC", "DST", "2026-11-05T10:00:00", price=900),   # хвост 900 → итого 1000
    ],
}


def test_no_bounds_returns_all_sorted():
    itins = build_itineraries(STOPS, COLLECTED, city_info=CITY_INFO)
    assert [it["total_price"] for it in itins] == [200, 1000]


def test_max_cost_prunes_expensive_branch():
    """С бюджетом 500 дорогое направление (через NYC, итого 1000) не должно
    попасть в результат — ветка режется по нижней оценке хвоста."""
    itins = build_itineraries(STOPS, COLLECTED, city_info=CITY_INFO, max_cost=500)
    assert len(itins) == 1
    assert itins[0]["total_price"] == 200
    assert [s["code"] for s in itins[0]["stops"]] == ["MOW", "IST", "DST"]


def test_max_cost_can_keep_both():
    itins = build_itineraries(STOPS, COLLECTED, city_info=CITY_INFO, max_cost=1000)
    assert [it["total_price"] for it in itins] == [200, 1000]


def test_max_results_keeps_cheapest_n():
    itins = build_itineraries(STOPS, COLLECTED, city_info=CITY_INFO, max_results=1)
    assert len(itins) == 1
    assert itins[0]["total_price"] == 200  # самая дешёвая, не первая встреченная


def test_completion_lb_is_min_tail_cost():
    """lb[i][city] — минимальная цена хвоста от city перед плечом i до конца."""
    legs_by_origin = {i: _index_leg(COLLECTED[i]) for i in range(len(STOPS) - 1)}
    lb = _completion_lb(STOPS, legs_by_origin)
    assert lb[1]["IST"] == 100   # IST→DST
    assert lb[1]["NYC"] == 900   # NYC→DST
    assert lb[0]["MOW"] == 200   # дешевле всего MOW→IST→DST


def test_start_pruned_when_below_budget_impossible():
    """Бюджет ниже самой дешёвой цепочки — результат пуст, старт отсекается сразу."""
    itins = build_itineraries(STOPS, COLLECTED, city_info=CITY_INFO, max_cost=150)
    assert itins == []
