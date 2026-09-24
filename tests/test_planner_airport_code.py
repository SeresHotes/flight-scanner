"""Регресс на стыковку цепочек по коду аэропорта, а не только города.

Travelpayouts отдаёт направления на уровне города (Сеул → destination='SEL'),
а конкретный аэропорт кладёт в destination_airport ('ICN'). Если пользователь
задал остановку кодом аэропорта (ICN), планировщик обязан всё равно состыковать
такой рейс — иначе финальная точка не совпадает ни с чем и цепочка пустая.
"""
from core import planner
from core.planner import Stop, _index_leg, _keep, _side_codes, build_itineraries

# Никакой сети/справочника — коды сами себе «города».
CITY_INFO = lambda code: {"city": code, "country": "", "flag": ""}


def _flight(origin, origin_ap, dest, dest_ap, dep, dur=180, price=100):
    return {
        "origin": origin, "origin_airport": origin_ap,
        "destination": dest, "destination_airport": dest_ap,
        "departure_at": dep, "duration": dur, "price": price, "transfers": 0,
    }


def test_side_codes_covers_city_and_airport():
    f = _flight("IST", "IST", "SEL", "ICN", "2026-11-05T10:00:00+03:00")
    assert _side_codes(f, "dest") == {"SEL", "ICN"}
    assert _side_codes(f, "origin") == {"IST"}


def test_keep_matches_by_airport_code():
    f = _flight("IST", "IST", "SEL", "ICN", "2026-11-05T10:00:00+03:00")
    assert _keep(f, "dest", {"ICN"})   # аэропорт
    assert _keep(f, "dest", {"SEL"})   # город
    assert not _keep(f, "dest", {"TYO"})


def test_index_leg_indexes_both_city_and_airport():
    f = _flight("IST", "SAW", "SEL", "ICN", "2026-11-05T10:00:00+03:00")
    idx = _index_leg([f])
    assert f in idx.get("IST", [])   # город вылета
    assert f in idx.get("SAW", [])   # аэропорт вылета


def test_chain_builds_when_final_stop_is_airport_code():
    """Регресс: финальная точка задана кодом аэропорта ICN, а рейс приходит
    с destination='SEL'. До фикса цепочка была пустой."""
    stops = [
        Stop("cities", ["MOW"], ["", ""]),
        Stop("any", [], ["2026-11-01", "2026-11-10"]),
        Stop("cities", ["ICN"], ["", ""]),
    ]
    collected = {
        0: [_flight("MOW", "SVO", "IST", "IST", "2026-11-02T10:00:00+03:00")],
        1: [_flight("IST", "IST", "SEL", "ICN", "2026-11-05T10:00:00+03:00", dur=600, price=300)],
    }
    itins = build_itineraries(stops, collected, city_info=CITY_INFO)
    assert len(itins) == 1
    codes = [s["code"] for s in itins[0]["stops"]]
    assert codes == ["MOW", "IST", "SEL"]  # Сеул как город; ICN совпал по аэропорту


def test_final_stop_city_code_still_matches():
    """Обратная совместимость: та же цепочка при финальной точке SEL (код города)."""
    stops = [
        Stop("cities", ["MOW"], ["", ""]),
        Stop("any", [], ["2026-11-01", "2026-11-10"]),
        Stop("cities", ["SEL"], ["", ""]),
    ]
    collected = {
        0: [_flight("MOW", "SVO", "IST", "IST", "2026-11-02T10:00:00+03:00")],
        1: [_flight("IST", "IST", "SEL", "ICN", "2026-11-05T10:00:00+03:00", dur=600, price=300)],
    }
    itins = build_itineraries(stops, collected, city_info=CITY_INFO)
    assert len(itins) == 1
