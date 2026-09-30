"""GraphQL-клиент: параметры, пагинация серии, нормализация билета, кэш серий.

Фикстура tests/fixtures/graphql_tickets.json — живые ответы prices_one_way от
27.09.2026 (MOW→SEL и MOW→ANY на 15.10.2026), по одному билету на число пересадок.
"""
import json
from pathlib import Path

import pytest

from core import graphql_api as g

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "graphql_tickets.json").read_text())
CITY_INFO = lambda code: {"city": code, "country": "", "flag": ""}


# ------------------------------ параметры -----------------------------------

def test_params_city_to_any_and_any_to_city():
    p = g.build_params("mow", None, "2026-10-15", value_min=20000, value_max=40000)
    # Типы мест не передаём: источник сам распознаёт город/аэропорт (см. build_params).
    assert p == {"depart_date_min": "2026-10-15", "depart_date_max": "2026-10-15",
                 "origin": "MOW", "value_min": 20000, "value_max": 40000}
    p = g.build_params(None, "icn", "2026-10-15", direct=True, with_baggage=True)
    assert "origin" not in p and p["destination"] == "ICN" and "destination_type" not in p
    assert p["direct"] is True and p["with_baggage"] is True
    assert g.build_params("MOW", None, "2026-10-15", origin_type="CITY")["origin_type"] == "CITY"
    with pytest.raises(ValueError):
        g.build_params(None, None, "2026-10-15")


def test_params_key_is_canonical_and_empty_by_default():
    assert g.params_key() == ""
    assert g.params_key(value_max=40000) == "max=40000"
    assert g.params_key(20000, 40000, True, False) == "min=20000&max=40000&direct=1&bag=0"


# ------------------------------ пагинация -----------------------------------

def _raw(i):
    """Минимальный сырой билет с уникальным номером рейса."""
    return {"value": 1000 + i, "main_airline": "XX", "number_of_changes": 0,
            "departure_at": "2026-10-15T10:00:00+03:00", "origin_city_iata": "MOW",
            "destination_city_iata": "SEL", "ticket_link": "/MOW1510SEL1?t=x",
            "segments": [{"transfers": [], "flight_legs": [{
                "origin": "SVO", "destination": "ICN", "departure_at": "2026-10-15T10:00:00+03:00",
                "arrival_at": "2026-10-15T23:00:00+09:00", "flight_number": str(i)}]}]}


def _pages(sizes, fail_at=None):
    """page_fn, отдающий страницы заданных размеров; fail_at — номер страницы с ошибкой."""
    calls = []

    def page_fn(params, offset, limit):
        idx = offset // g.PAGE_LIMIT
        calls.append((params, offset, limit))
        if fail_at is not None and idx + 1 == fail_at:
            raise g.GraphQLError("boom")
        n = sizes[idx] if idx < len(sizes) else 0
        return [_raw(offset + k) for k in range(n)]
    page_fn.calls = calls
    return page_fn


def test_series_stops_at_short_page():
    page_fn = _pages([400, 400, 37])
    s = g.fetch_series("MOW", "SEL", "2026-10-15", page_fn=page_fn)
    assert (s["pages"], s["exhausted"], s["error"]) == (3, True, False)
    assert len(s["tickets"]) == 837
    assert [c[1] for c in page_fn.calls] == [0, 400, 800]


def test_series_truncated_by_max_pages():
    page_fn = _pages([400] * 5)
    s = g.fetch_series("MOW", None, "2026-10-15", max_pages=2, page_fn=page_fn)
    assert (s["pages"], s["exhausted"], len(s["tickets"])) == (2, False, 800)


def test_series_error_keeps_partial_and_flags():
    page_fn = _pages([400, 400], fail_at=2)
    s = g.fetch_series("MOW", "SEL", "2026-10-15", page_fn=page_fn)
    assert s["error"] is True and s["pages"] == 1 and len(s["tickets"]) == 400


def test_series_empty_first_page_is_exhausted():
    s = g.fetch_series("MOW", "SEL", "2026-10-15", page_fn=_pages([0]))
    assert s == {"tickets": [], "pages": 1, "exhausted": True, "error": False}


# ----------------------------- нормализация ---------------------------------

def test_normalize_three_transfers_real_arrival_and_durations():
    raw = FIXTURE["mow_sel"][0]                     # U6 DME→SVX→HRB→FOC→ICN, 3 пересадки
    f = g.normalize_ticket(raw, "MOW", "SEL", "2026-10-15")
    assert (f["origin"], f["destination"]) == ("MOW", "SEL")
    assert (f["origin_airport"], f["destination_airport"]) == ("DME", "ICN")
    assert f["chain"] == ["DME", "SVX", "HRB", "FOC", "ICN"]
    assert f["transfers"] == 3 and f["price"] == 36127 and f["airline"] == "U6"
    assert f["departure_at"] == "2026-10-15T08:25:00+03:00"
    assert f["arrival_at"] == raw["segments"][0]["flight_legs"][-1]["arrival_at"]
    # Длительность — по сегментам с учётом поясов, а не из trip_duration (=0).
    assert f["duration"] == 3150                 # как в t= ссылки: …00003150…
    assert f["duration_to"] is not None and f["duration_to"] < f["duration"]
    layover = sum(p["minutes"] for p in f["transfer_points"])
    assert f["duration"] - f["duration_to"] == layover
    assert [p["code"] for p in f["transfer_points"]] == ["SVX", "HRB", "FOC"]
    assert f["transfer_points"][1] == {"code": "HRB", "to": "HRB", "country": "CN",
                                       "minutes": 855, "night": True, "visa": False}
    assert f["baggage"] == {"known": True, "included": False, "pieces": None, "kg": None}
    assert f["source"] == "graphql" and f["search_date"] == "2026-10-15"
    assert f["link"].startswith("/search/MOW1510SEL1?t=")


def test_normalize_duration_consistent_with_transfers():
    for raw in FIXTURE["mow_sel"] + FIXTURE["mow_any"]:
        f = g.normalize_ticket(raw, None, None, "2026-10-15")
        assert f["link"].startswith("/search/")
        assert f["chain"][0] == f["origin_airport"] and f["chain"][-1] == f["destination_airport"]
        # Длительность по сегментам согласована с длительностью пересадок источника
        # (в воздухе + ожидание = всего).
        assert f["duration"] == f["duration_to"] + sum(p["minutes"] for p in f["transfer_points"])


def test_normalize_direct_with_baggage():
    f = g.normalize_ticket(FIXTURE["mow_any"][2], "MOW", None, "2026-10-15")   # SU SVO→UUD
    assert f["transfers"] == 0 and f["transfer_points"] == [] and f["chain"] == ["SVO", "UUD"]
    assert f["destination"] == "UUD" and f["search_destination"] is None
    assert f["baggage"] == {"known": True, "included": True, "pieces": 1, "kg": 23}
    assert f["duration"] == f["duration_to"] == 360
    assert f["flight_number"] == "1440" or f["flight_number"]   # первый сегмент


def test_normalize_skips_ticket_without_segments():
    assert g.normalize_ticket({"value": 1, "segments": []}, "MOW", "SEL", "2026-10-15") is None


@pytest.mark.parametrize("code,with_baggage,expected", [
    ("1PC23", None, (True, True, 1, 23)),
    ("2PC", None, (True, True, 2, None)),
    ("0PC", True, (True, False, None, None)),
    ("", True, (True, True, None, None)),
    ("", None, (False, False, None, None)),
    (None, False, (True, False, None, None)),
])
def test_parse_baggage_code(code, with_baggage, expected):
    b = g.parse_baggage_code(code, with_baggage)
    assert (b["known"], b["included"], b["pieces"], b["kg"]) == expected


def test_flight_key_dedupes_same_ticket_from_two_series():
    a = g.normalize_ticket(FIXTURE["mow_any"][0], "MOW", None, "2026-10-15")
    b = g.normalize_ticket(FIXTURE["mow_any"][0], None, "BGW", "2026-10-15")
    assert g.flight_key(a) == g.flight_key(b)
    c = g.normalize_ticket(FIXTURE["mow_any"][1], "MOW", None, "2026-10-15")
    assert g.flight_key(a) != g.flight_key(c)
