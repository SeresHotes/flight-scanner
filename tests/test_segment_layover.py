"""Время на земле в сегменте: duration (весь путь) − duration_to (в воздухе)."""
from core.trip_builder import Builder, layover_minutes


def _city_info(code):
    return {"city": code, "country": "", "flag": ""}


def test_layover_from_duration_to():
    f = {"duration": 560, "duration_to": 285}
    assert layover_minutes(f, 1) == 275


def test_no_layover_for_direct_or_missing():
    assert layover_minutes({"duration": 120, "duration_to": 120}, 0) is None
    assert layover_minutes({"duration": 560}, 1) is None
    assert layover_minutes({"duration": 300, "duration_to": 300}, 1) is None


def test_make_segment_carries_layover_and_points():
    flight = {
        "origin": "SEL", "destination": "CAN", "origin_airport": "ICN", "destination_airport": "CAN",
        "departure_at": "2026-11-14T11:45:00+09:00", "duration": 560, "duration_to": 285,
        "transfers": 1, "price": 9962, "airline": "SC",
        "link": "/search/ICN1411CAN1?t=SC17946567001794686700000560ICNTNACAN_c7e7b2263b2dcdb017a8c0e0edd01fb2_9962",
    }
    seg = Builder(None, _city_info).make_segment(flight)
    assert seg["layover_minutes"] == 275
    assert seg["transfer_points"] == [{"code": "TNA", "city": "TNA"}]
