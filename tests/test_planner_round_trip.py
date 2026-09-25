"""Кольцевой маршрут: финальная остановка совпадает со стартовой (MOW → … → MOW).

Запрет «без петель/повторов» раньше отсекал ЛЮБОЙ прилёт в уже посещённый город,
включая явно заданный пользователем финал. Старт MOW сидел в visited, поэтому
последнее плечо «… → MOW» не проходило никогда, а нижняя оценка хвоста
(_completion_lb) о visited не знает — A* бесконечно раскрывал частичные цепочки
и не находил ни одной (прод: «перебрано 40к, найдено 0 из 100»).
"""
from core.planner import Stop, build_itineraries

CITY_INFO = lambda code: {"city": code, "country": "", "flag": ""}


def _flight(origin, dest, dep, price=100, dur=180):
    return {
        "origin": origin, "origin_airport": origin,
        "destination": dest, "destination_airport": dest,
        "departure_at": dep, "duration": dur, "price": price, "transfers": 0,
    }


def _codes(itin):
    return [s["code"] for s in itin["stops"]]


def test_round_trip_returns_to_start():
    stops = [
        Stop("cities", ["MOW"], ["", ""]),
        Stop("any", [], ["2026-11-01", "2026-11-10"]),
        Stop("cities", ["SEL"], ["2026-11-05", "2026-11-20"]),
        Stop("any", [], ["2026-11-10", "2026-11-30"]),
        Stop("cities", ["MOW"], ["", ""]),
    ]
    collected = {
        0: [_flight("MOW", "IST", "2026-11-02T10:00:00")],
        1: [_flight("IST", "SEL", "2026-11-06T10:00:00")],
        2: [_flight("SEL", "TYO", "2026-11-12T10:00:00")],
        3: [_flight("TYO", "MOW", "2026-11-20T10:00:00")],
    }
    for max_results in (None, 100):
        itins = build_itineraries(stops, collected, city_info=CITY_INFO, max_results=max_results)
        assert [_codes(it) for it in itins] == [["MOW", "IST", "SEL", "TYO", "MOW"]]


def test_any_stop_still_cannot_revisit():
    # «Любой» не должен разрешаться в уже посещённый город: MOW → IST → MOW → … — мусор.
    stops = [
        Stop("cities", ["MOW"], ["", ""]),
        Stop("any", [], ["2026-11-01", "2026-11-10"]),
        Stop("any", [], ["2026-11-05", "2026-11-20"]),
        Stop("cities", ["DST"], ["", ""]),
    ]
    collected = {
        0: [_flight("MOW", "IST", "2026-11-02T10:00:00")],
        1: [_flight("IST", "MOW", "2026-11-06T10:00:00", price=1),
            _flight("IST", "BER", "2026-11-06T10:00:00", price=500)],
        2: [_flight("MOW", "DST", "2026-11-12T10:00:00", price=1),
            _flight("BER", "DST", "2026-11-12T10:00:00", price=500)],
    }
    itins = build_itineraries(stops, collected, city_info=CITY_INFO, max_results=10)
    assert [_codes(it) for it in itins] == [["MOW", "IST", "BER", "DST"]]
