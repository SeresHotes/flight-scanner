"""Движковые границы планировщика: max_cost (бюджет) и max_results (потолок числа).

max_cost должен резать целые бесперспективные направления по admissible-оценке
минимального «хвоста» (_completion_lb), а не только фильтровать готовый результат.
max_results — оставлять N самых дешёвых цепочек. Без обеих границ — прежнее «всё».
"""
from core.planner import (
    Stop,
    _completion_lb,
    _index_leg,
    build_itineraries,
    is_valid_max_results,
)

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


# Три средних города → три цепочки: итого 200, 400, 600.
COLLECTED3 = {
    0: [
        _flight("MOW", "IST", "2026-11-02T10:00:00", price=100),
        _flight("MOW", "DXB", "2026-11-02T10:00:00", price=100),
        _flight("MOW", "NYC", "2026-11-02T10:00:00", price=100),
    ],
    1: [
        _flight("IST", "DST", "2026-11-05T10:00:00", price=100),   # итого 200
        _flight("DXB", "DST", "2026-11-05T10:00:00", price=300),   # итого 400
        _flight("NYC", "DST", "2026-11-05T10:00:00", price=500),   # итого 600
    ],
}


def test_best_first_returns_n_cheapest_from_many():
    """max_results — движковый потолок: N САМЫХ ДЕШЁВЫХ (best-first), не любые N."""
    itins = build_itineraries(STOPS, COLLECTED3, city_info=CITY_INFO, max_results=2)
    assert [it["total_price"] for it in itins] == [200, 400]


def test_max_results_with_cost_bound():
    """Потолок числа и бюджет вместе: дорогая (600) отсекается бюджетом, N не добирается."""
    itins = build_itineraries(STOPS, COLLECTED3, city_info=CITY_INFO, max_results=5, max_cost=400)
    assert [it["total_price"] for it in itins] == [200, 400]


def test_is_valid_max_results_rejects_none():
    """Отказ вместо потолка: None (устаревший клиент / прямой API) невалиден, чтобы
    движок не уходил в безлимитный перебор. Конечные положительные — валидны, без
    верхнего предела.

    Регресс на прод-инцидент: запрос без max_results уходил в безлимит →
    100000 цепочек, 282 МБ ответа, зависание браузера."""
    assert is_valid_max_results(None) is False
    assert is_valid_max_results(0) is False
    assert is_valid_max_results(-5) is False
    assert is_valid_max_results(1) is True
    assert is_valid_max_results(100) is True
    assert is_valid_max_results(100000) is True  # без верхнего потолка
