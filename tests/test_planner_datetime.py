"""Регресс: сборка цепочек не должна падать на датах с tz-суффиксом.

Источник иногда отдаёт departure_at с суффиксом 'Z' (UTC) или явным offset.
parse_datetime обязан всегда возвращать naive datetime — иначе сравнение с
naive-якорём даты цепочки бросает "can't compare offset-naive and offset-aware
datetimes". До фикса это всплывало при сборе после снятия beam-кэпов (DFS стал
доходить до рейсов с таким форматом даты).
"""
from core import aggregate as agg
from core.planner import Stop, build_itineraries

CITY_INFO = lambda code: {"city": code, "country": "", "flag": ""}


def test_parse_datetime_always_naive():
    assert agg.parse_datetime("2026-11-05T10:00:00Z").tzinfo is None       # UTC 'Z'
    assert agg.parse_datetime("2026-11-05T10:00:00+03:00").tzinfo is None  # явный offset
    assert agg.parse_datetime("2026-11-05T10:00:00").tzinfo is None        # naive
    assert agg.parse_datetime("2026-11-05").tzinfo is None                 # только дата


def _flight(origin, dest, dep, dur=180, price=100):
    return {
        "origin": origin, "destination": dest, "departure_at": dep,
        "duration": dur, "price": price, "transfers": 0,
    }


def test_build_itineraries_handles_utc_z_dates():
    """departure_at с 'Z' не роняет сборку при сравнении с naive-якорём."""
    stops = [
        Stop("cities", ["MOW"], ["", ""]),
        Stop("any", [], ["2026-11-01", "2026-11-10"]),
        Stop("cities", ["SEL"], ["", ""]),
    ]
    collected = {
        0: [_flight("MOW", "IST", "2026-11-02T10:00:00Z")],
        1: [_flight("IST", "SEL", "2026-11-05T10:00:00Z", dur=600, price=300)],
    }
    itins = build_itineraries(stops, collected, city_info=CITY_INFO)
    assert len(itins) == 1
    assert [s["code"] for s in itins[0]["stops"]] == ["MOW", "IST", "SEL"]
