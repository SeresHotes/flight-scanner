"""overview_graph: компактный граф рёбер для режима «наборы городов» на фронте.

Фронт (planner/overview.ts) считает по нему ВСЕ варианты без движковых границ,
поэтому граф должен повторять семантику стыковки движка: первый переход — только из
стартовых кодов (в т.ч. заданных аэропортом), время — без таймзоны, без дублей.
"""
from core.planner import Stop, overview_graph

CITY_INFO = lambda code: {"city": code.title(), "country": "", "flag": "🏳"}


def _flight(origin, dest, dep, price, **extra):
    return {"origin": origin, "destination": dest, "departure_at": dep,
            "duration": 120, "price": price, "transfers": 0, **extra}


STOPS = [
    Stop("cities", ["ICN"], ["", ""]),            # старт задан кодом аэропорта
    Stop("any", [], ["2026-11-02", "2026-11-05"]),
    Stop("cities", ["MOW"], ["", ""]),
]


def test_graph_edges_and_meta():
    collected = {
        0: [
            _flight("SEL", "IST", "2026-11-02T10:00:00+03:00", 100, origin_airport="ICN"),
            _flight("SEL", "IST", "2026-11-02T10:00:00+03:00", 100, origin_airport="ICN"),  # дубль
            _flight("SEL", "DXB", "2026-11-03T10:00:00", 200, origin_airport="GMP"),          # не старт
        ],
        1: [_flight("IST", "MOW", "2026-11-04T08:00:00", 300)],
    }
    g = overview_graph(STOPS, collected, city_info=CITY_INFO)

    assert g["chain_start"] == "2026-11-02"
    assert g["any"] == [False, True, False]
    assert g["final_stay_days"] > 0
    assert [len(leg) for leg in g["legs"]] == [1, 1]
    e = g["legs"][0][0]
    assert (e["from"], e["to"], e["price"]) == ("SEL", "IST", 100)
    assert e["dep"] == "2026-11-02T10:00:00"  # таймзона срезана, как в parse_datetime
    assert e["arr"] == "2026-11-02T12:00:00"  # прилёт = вылет + duration
    assert set(g["cities"]) >= {"SEL", "IST", "MOW"}
    assert g["cities"]["IST"] == {"city": "Ist", "flag": "🏳"}


def test_empty_collection():
    g = overview_graph(STOPS, {}, city_info=CITY_INFO)
    assert g["legs"] == [[], []]
