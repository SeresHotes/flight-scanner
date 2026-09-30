"""Сбор планировщика через GraphQL и hidden-city.

Город→город: пары A×B по одной странице в день + A→ANY под hidden-city с коридором
«дешевле лучшего A→B того дня». Билет A→H→X с H — аэропортом остановки B становится
виртуальным рейсом A→B: реальное время прилёта в H, transfers = число пересадок до
H, цена всего билета. Город→любой: A→ANY с коридором max_cost, hidden-city во все
промежуточные хабы. Любой→город: ANY→B, без hidden-city.
"""
from core import planner
from core.planner import PAGES_ANY, PAGES_CITY, PAGES_HIDDEN, Stop, collect_plan, estimate_plan, request_count
from core.graphql_api import MAX_PAGES as MAX  # серия берётся целиком, PAGES_* — только оценка
from core.segments import Builder, make_city_lookup

CITY_INFO = make_city_lookup({})
DAY = "2026-10-29"


def _ticket(origin, dest, chain, dep, arr, price, *, legs_times=None, baggage=True, number="1"):
    """Нормализованный билет GraphQL по цепочке аэропортов: пересадки — все точки между
    концами; времена сегментов — legs_times [(dep, arr), …] или равномерно."""
    n = len(chain) - 1
    times = legs_times or [(dep, arr)] * n
    legs = [{"origin": chain[j], "destination": chain[j + 1], "departure_at": times[j][0],
             "arrival_at": times[j][1], "flight_number": f"{number}{j}", "carrier": "CZ"}
            for j in range(n)]
    points = [{"code": chain[j], "to": chain[j], "country": "", "minutes": 120,
               "night": False, "visa": False} for j in range(1, n)]
    return {
        "origin": origin, "origin_airport": chain[0],
        "destination": dest, "destination_airport": chain[-1],
        "departure_at": dep, "arrival_at": arr, "duration": 1000, "duration_to": 800,
        "transfers": n - 1, "airline": "CZ", "flight_number": legs[0]["flight_number"],
        "price": price, "currency": "rub", "link": f"/search/{origin}2910{dest}1?t=x",
        "chain": list(chain), "legs": legs, "transfer_points": points,
        "baggage": {"known": True, "included": baggage, "pieces": 1 if baggage else None,
                    "kg": 23 if baggage else None},
        "baggage_code": "1PC23" if baggage else "0PC", "source": "graphql",
    }


def _series(tickets, pages=1):
    return {"tickets": list(tickets), "pages": pages, "exhausted": True, "error": False}


DIRECT_PKX = _ticket("MOW", "BJS", ["SVO", "PKX"], f"{DAY}T17:15:00+03:00",
                     f"{DAY}T05:50:00+08:00", 36000, number="1")
VIA_PKX_HRB = _ticket("MOW", "HRB", ["SVO", "PKX", "HRB"], f"{DAY}T23:15:00+03:00",
                      f"{DAY[:8]}30T14:00:00+08:00", 25000, number="2",
                      legs_times=[(f"{DAY}T23:15:00+03:00", f"{DAY[:8]}30T11:50:00+08:00"),
                                  (f"{DAY[:8]}30T13:00:00+08:00", f"{DAY[:8]}30T14:00:00+08:00")])
VIA_PKX_CAN_EXPENSIVE = _ticket("MOW", "CAN", ["SVO", "PKX", "CAN"], f"{DAY}T10:00:00+03:00",
                                f"{DAY[:8]}30T10:00:00+08:00", 40000, number="3")
VIA_OTHER_HUB = _ticket("MOW", "HRB", ["SVO", "SVX", "HRB"], f"{DAY}T08:00:00+03:00",
                        f"{DAY[:8]}30T10:00:00+08:00", 20000, number="4")
SECOND_HOP_PEK = _ticket("MOW", "HRB", ["SVO", "URC", "PEK", "HRB"], f"{DAY}T09:00:00+03:00",
                         f"{DAY[:8]}30T20:00:00+08:00", 21000, number="5", baggage=False)

STOPS = [Stop("cities", ["MOW"], ["", ""]), Stop("cities", ["BJS"], [DAY, DAY])]


mins = []   # value_min последних серий (нижняя граница коридора hidden-city)


def _fetch(calls, any_tickets):
    def fetch(origin=None, destination=None, day=None, *, value_max=None, max_pages=None,
              progress_cb=None, value_min=None, **_):
        calls.append((origin, destination, day, value_max, max_pages))
        mins.append(value_min)
        if progress_cb:
            progress_cb(1, 0)
        if destination == "BJS":
            return _series([DIRECT_PKX])
        if destination is None:
            return _series(any_tickets)
        return _series([])
    return fetch


def test_pair_plus_hidden_probe_with_corridor_below_best_regular():
    calls, ticks = [], []
    collected = collect_plan(STOPS, progress_cb=lambda: ticks.append(1),
                             fetch_fn=_fetch(calls, [VIA_PKX_HRB, VIA_PKX_CAN_EXPENSIVE,
                                                     VIA_OTHER_HUB, SECOND_HOP_PEK]),
                             airport_city={"PEK": "BJS"})
    assert calls == [("MOW", "BJS", DAY, None, MAX),
                     ("MOW", None, DAY, 36000, MAX)]   # коридор — лучший прямой A→B
    assert mins[-2:] == [None, 18000]                         # нижняя граница — половина порога
    assert len(ticks) == PAGES_CITY + PAGES_HIDDEN == request_count(STOPS)
    leg = collected[0]
    assert [f["flight_number"] for f in leg] == ["10", "20", "50"]
    v = leg[1]                                            # SVO→PKX→HRB, выходим в PKX
    assert v["destination"] == "BJS" and v["destination_airport"] == "PKX"
    assert v["transfers"] == 0 and v["price"] == 25000
    assert v["arrival_at"] == f"{DAY[:8]}30T11:50:00+08:00"   # реальный прилёт в PKX
    assert v["duration"] == 455 and v["duration_to"] == 455 and v["transfer_points"] == []
    assert v["chain"] == ["SVO", "PKX"] and v["hidden_city"]["chain"] == ["SVO", "PKX", "HRB"]
    assert v["hidden_city"]["final"] == "HRB" and v["hidden_city"]["arrival_estimated"] is False
    assert v["hidden_city"]["baggage"]["included"] is True
    w = leg[2]                                            # SVO→URC→PEK→HRB: PEK — второй хаб
    assert w["destination_airport"] == "PEK" and w["transfers"] == 1
    assert [p["code"] for p in w["transfer_points"]] == ["URC"] and w["chain"] == ["SVO", "URC", "PEK"]
    assert w["hidden_city"]["baggage"]["included"] is False


def test_hidden_city_needs_airport_city_map_for_unseen_hub():
    """PEK не встречался в ответах A→B (там только PKX): без карты аэропорт→город
    хаб не распознаётся — это и был пропуск SVO→PEK→SIN в PR #64."""
    collected = collect_plan(STOPS, fetch_fn=_fetch([], [SECOND_HOP_PEK]))
    assert [f["flight_number"] for f in collected[0]] == ["10"]
    collected = collect_plan(STOPS, fetch_fn=_fetch([], [SECOND_HOP_PEK]), airport_city={"PEK": "BJS"})
    assert [f["flight_number"] for f in collected[0]] == ["10", "50"]


def test_segment_from_virtual_flight_has_real_arrival_and_ticket_link():
    collected = collect_plan(STOPS, fetch_fn=_fetch([], [VIA_PKX_HRB]))
    seg = Builder(None, CITY_INFO).make_segment(collected[0][1])
    assert seg["destination"] == "BJS" and seg["direct"] is True
    assert seg["arrival_at"] == f"{DAY[:8]}30T11:50:00+08:00"
    assert seg["hidden_city"]["final"] == "HRB" and seg["hidden_city"]["arrival_estimated"] is False
    assert seg["link"] == "https://www.aviasales.ru/search/MOW2910HRB1?t=x"
    assert seg["baggage"]["included"] is True and seg["transfer_points"] == []


def test_no_regular_flight_that_day_uses_max_cost_as_corridor():
    calls = []

    def fetch(origin=None, destination=None, day=None, *, value_max=None, max_pages=None, **_):
        calls.append((origin, destination, value_max))
        return _series([VIA_PKX_HRB] if destination is None else [])
    collected = collect_plan(STOPS, fetch_fn=fetch, max_cost=80000, airport_city={"PKX": "BJS"})
    assert calls == [("MOW", "BJS", None), ("MOW", None, 80000)]
    assert [f["flight_number"] for f in collected[0]] == ["20"]   # порога нет — берём


def test_city_to_any_uses_corridor_and_free_hidden_city():
    stops = [Stop("cities", ["MOW"], ["", ""]), Stop("any", [], [DAY, DAY])]
    calls = []
    collected = collect_plan(stops, fetch_fn=_fetch(calls, [DIRECT_PKX, VIA_PKX_HRB, VIA_OTHER_HUB]),
                             max_cost=70000)
    assert calls == [("MOW", None, DAY, 70000, MAX)]
    assert request_count(stops) == PAGES_ANY
    numbers = sorted(f["flight_number"] for f in collected[0])
    # обычные: 10 (SVO→PKX), 20 (→HRB), 40 (→HRB); hidden: 20 в PKX (25000 < 36000 прямого),
    # 40 в SVX (обычных MOW→SVX нет — берём)
    assert numbers == ["10", "20", "20", "40", "40"]
    hidden = [f for f in collected[0] if f.get("hidden_city")]
    assert {(f["destination"], f["destination_airport"]) for f in hidden} == {("BJS", "PKX"), ("SVX", "SVX")}


def test_any_to_city_has_no_hidden_city():
    stops = [Stop("any", [], [DAY, DAY]), Stop("cities", ["BJS"], ["", ""])]
    calls = []
    collected = collect_plan(stops, fetch_fn=_fetch(calls, [VIA_PKX_HRB]))
    assert calls == [(None, "BJS", DAY, None, MAX)]
    assert [f["flight_number"] for f in collected[0]] == ["10"]


def test_estimate_matches_request_count_and_labels():
    stops = [Stop("cities", ["MOW", "LED"], ["", ""]), Stop("cities", ["BJS"], [DAY, "2026-10-30"]),
             Stop("any", [], ["2026-11-02", "2026-11-04"]), Stop("cities", ["MOW"], ["", ""])]
    est = estimate_plan(stops, CITY_INFO)
    assert [l["requests"] for l in est["legs"]] == [
        2 * (2 * 1 * PAGES_CITY + 2 * PAGES_HIDDEN),   # 2 дня × (пары + hidden на город A)
        2 * 1 * PAGES_ANY,                             # BJS → ANY: окно плеча — у BJS (2 дня)
        3 * 1 * PAGES_ANY,                             # ANY → MOW: окно «любого» (3 дня)
    ]
    assert est["requests"] == request_count(stops) == sum(l["requests"] for l in est["legs"])
    assert est["seconds"] == round(est["requests"] * planner.SECONDS_PER_REQUEST)
    assert [l["anyLeg"] for l in est["legs"]] == [False, True, True]


def test_dedupes_same_ticket_across_pairs():
    """Один билет может прийти в двух парах (A→B и A→B' одного города) — оставляем один."""
    stops = [Stop("cities", ["MOW"], ["", ""]), Stop("cities", ["BJS", "PKX"], [DAY, DAY])]
    calls = []

    def fetch(origin=None, destination=None, day=None, **_):
        calls.append(destination)
        return _series([DIRECT_PKX] if destination else [])
    collected = collect_plan(stops, fetch_fn=fetch)
    assert calls == ["BJS", "PKX", None]
    assert len(collected[0]) == 1
