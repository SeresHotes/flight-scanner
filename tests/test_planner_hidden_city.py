"""Hidden-city в планировщике: билет A→C с первой пересадкой в B попадает в список
плеча A→B виртуальным рейсом (цена всего билета, transfers=0, прилёт — оценка),
если он дешевле обычного A→B того же дня. При якорении по A данные уже в ответе
A→ANY; при якорении по B добавляется запрос A→ANY с пересадками."""
from core.planner import Stop, collect_plan, estimate_plan, request_count
from core.trip_builder import Builder, make_city_lookup

CITY_INFO = make_city_lookup({})


def _link(chain, price, fare="TY%7CP1%7CH1%7CL1_1_23%7CCH1%7CR0%7CTBC1"):
    t = f"CZ17778429001777983900002050{''.join(chain)}_hash_{price}"
    return f"/search/MOW0305XXX1?t={t}&static_fare_key={fare}&expected_price={price}"


def _flight(origin, dest, oa, da, dep, price, transfers, chain, number, duration=600):
    return {
        "origin": origin, "origin_airport": oa, "destination": dest, "destination_airport": da,
        "departure_at": dep, "price": price, "transfers": transfers, "airline": "CZ",
        "flight_number": number, "duration": duration, "link": _link(chain, price),
    }


DAY = "2026-10-29"
DIRECT_PKX = _flight("MOW", "BJS", "SVO", "PKX", f"{DAY}T17:15:00+03:00", 36000, 0,
                     ["SVO", "PKX"], "1", duration=455)
VIA_PKX_HRB = _flight("MOW", "HRB", "SVO", "PKX", f"{DAY}T23:15:00+03:00", 25000, 1,
                      ["SVO", "PKX", "HRB"], "2", duration=1100)
VIA_PKX_CAN_EXPENSIVE = _flight("MOW", "CAN", "SVO", "PKX", f"{DAY}T10:00:00+03:00", 40000, 1,
                                ["SVO", "PKX", "CAN"], "3")
VIA_OTHER_HUB = _flight("MOW", "HRB", "SVO", "SVX", f"{DAY}T08:00:00+03:00", 20000, 1,
                        ["SVO", "SVX", "HRB"], "4")
SECOND_HOP_PKX = _flight("MOW", "HRB", "SVO", "PEK", f"{DAY}T09:00:00+03:00", 21000, 2,
                         ["SVO", "URC", "PKX", "HRB"], "5")

STOPS_FROM_ANCHOR = [Stop("cities", ["MOW"], ["", ""]), Stop("cities", ["BJS"], [DAY, DAY])]


def _fetch_from_anchor(calls):
    def fetch(origin=None, destination=None, departure_at=None, allow_indirect=False, **_):
        calls.append((origin, destination, departure_at, allow_indirect))
        assert origin == "MOW" and destination is None
        data = [dict(DIRECT_PKX)]
        if allow_indirect:
            data += [dict(VIA_PKX_HRB), dict(VIA_PKX_CAN_EXPENSIVE), dict(VIA_OTHER_HUB),
                     dict(SECOND_HOP_PKX)]
        return {"data": data}
    return fetch


def test_from_anchor_adds_virtual_flight_without_extra_requests():
    calls = []
    collected = collect_plan(STOPS_FROM_ANCHOR, fetch_fn=_fetch_from_anchor(calls))
    assert len(calls) == 2  # только штатные режимы, доп. запросов нет
    assert request_count(STOPS_FROM_ANCHOR) == 2
    leg = collected[0]
    assert [f["flight_number"] for f in leg] == ["1", "2"]  # прямой + hidden-city через PKX
    v = leg[1]
    assert v["destination"] == "BJS" and v["destination_airport"] == "PKX"
    assert v["transfers"] == 0 and v["price"] == 25000
    assert v["duration"] == 455  # длительность прямого SVO→PKX из той же выборки
    assert v["hidden_city"]["final"] == "HRB" and v["hidden_city"]["chain"] == ["SVO", "PKX", "HRB"]
    assert v["hidden_city"]["baggage"]["included"] is True
    assert v["link"].startswith("/search/")  # сырая ссылка сохранена для покупки


def test_segment_marks_hidden_city_and_links_real_ticket():
    collected = collect_plan(STOPS_FROM_ANCHOR, fetch_fn=_fetch_from_anchor([]))
    seg = Builder(None, CITY_INFO).make_segment(collected[0][1])
    assert seg["destination"] == "BJS" and seg["direct"] is True
    assert seg["hidden_city"]["final"] == "HRB" and seg["hidden_city"]["arrival_estimated"] is True
    assert seg["link"].startswith("https://www.aviasales.ru/search/MOW0305XXX1?t=")
    assert seg["arrival_at"] == "2026-10-30T06:50:00"  # вылет 23:15 + 455 мин прямого SVO→PKX


STOPS_TO_ANCHOR = [Stop("cities", ["MOW", "LED"], ["", ""]), Stop("cities", ["BJS"], [DAY, DAY])]


def _fetch_to_anchor(calls):
    def fetch(origin=None, destination=None, departure_at=None, allow_indirect=False, **_):
        calls.append((origin, destination, departure_at, allow_indirect))
        if destination == "BJS":            # штатный якорь по B: ANY→BJS
            return {"data": [dict(DIRECT_PKX)]}
        if origin == "MOW":                 # доп. запрос A→ANY под hidden-city
            return {"data": [dict(VIA_PKX_HRB)]}
        return {"data": []}
    return fetch


def test_to_anchor_makes_extra_any_requests_and_counts_them():
    calls = []
    collected = collect_plan(STOPS_TO_ANCHOR, fetch_fn=_fetch_to_anchor(calls))
    assert sorted(calls, key=str) == sorted([
        (None, "BJS", DAY, True), (None, "BJS", DAY, False),
        ("MOW", None, DAY, True), ("LED", None, DAY, True),
    ], key=str)
    assert request_count(STOPS_TO_ANCHOR) == 4 == estimate_plan(STOPS_TO_ANCHOR, CITY_INFO)["requests"]
    assert [f["flight_number"] for f in collected[0]] == ["1", "2"]


def test_no_hidden_city_for_any_stops():
    stops = [Stop("cities", ["MOW"], ["", ""]), Stop("any", [], [DAY, DAY])]
    calls = []
    collected = collect_plan(stops, fetch_fn=_fetch_from_anchor(calls))
    assert len(calls) == 2 and not any(f.get("hidden_city") for f in collected[0])
