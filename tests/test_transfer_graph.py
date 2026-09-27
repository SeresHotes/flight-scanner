"""Граф пересадок: рёбра из цепочки link, наблюдённые пересадки, запросы
«пересадки по направлению» и hidden-city «через H дешевле прямого A→H»."""
import json

from core.transfer_graph import TransferGraph, build_from_db, ticket_from_row
from storage import hot


def _link(airline, chain, fare="TY%7CP1%7CH1%7CL1_1_23%7CCH1%7CR0%7CTBC1", price=1000):
    t = f"{airline}17778429001777983900002050{''.join(chain)}_hash_{price}"
    return f"/search/MOW0305XXX1?t={t}&static_fare_key={fare}&expected_price={price}"


def _row(origin, dest, chain, day, price, airline="CZ", fare=None, link=True):
    transfers = len(chain) - 2
    kwargs = {"fare": fare} if fare else {}
    return {
        "origin": origin, "destination": dest, "origin_airport": chain[0], "dest_airport": chain[-1],
        "departure_at": f"{day}T10:00:00+03:00", "price": price, "airline": airline,
        "transfers": transfers, "duration": 500,
        "link": _link(airline, chain, price=price, **kwargs) if link else None,
        "observed_at": "2026-09-27T00:00:00",
    }


NETWORK = {"SVO": {"country": "RU"}, "PKX": {"country": "CN"}, "PEK": {"country": "CN"},
           "HRB": {"country": "CN"}, "CAN": {"country": "CN"}, "ICN": {"country": "KR"}}

ROWS = [
    _row("MOW", "BJS", ["SVO", "PKX"], "2026-10-29", 36000, "MU"),        # прямой в PKX
    _row("MOW", "BJS", ["SVO", "PEK"], "2026-10-30", 31000, "CA"),        # прямой в PEK
    _row("MOW", "HRB", ["SVO", "PKX", "HRB"], "2026-10-29", 24000, "CZ"),  # дешевле прямого того же дня
    _row("MOW", "CAN", ["SVO", "PKX", "CAN"], "2026-11-02", 40000, "CZ"),  # дороже — отпадает
    _row("MOW", "ICN", ["SVO", "PEK", "ICN"], "2026-11-05", 20000, "CA", fare="TY%7CL0"),  # прямого в тот день нет
    _row("MOW", "CAN", ["SVO", "CAN"], "2026-10-29", 30000, "CZ", link=False),  # прямой без link
]


def _graph():
    g = TransferGraph(NETWORK)
    g.add_rows(ROWS)
    return g


def test_ticket_from_row_fallback_without_link():
    t = ticket_from_row(ROWS[-1])
    assert t.airports == ["SVO", "CAN"] and t.baggage.known is False
    assert ticket_from_row({**ROWS[-1], "transfers": 1}) is None  # пересадка без link — цепочка неизвестна


def test_edges_and_transfers():
    g = _graph()
    assert g.stats() == {"nodes": 6, "edges": 6, "nonstop_tickets": 3,
                         "transfer_tickets": 3, "skipped_rows": 0}
    e = g.edges[("SVO", "PKX")]
    assert e["tickets"] == 3 and e["nonstop_tickets"] == 1 and e["nonstop_min_price"] == 36000
    assert e["airlines"] == {"MU", "CZ"}
    assert g.edges[("PKX", "HRB")]["nonstop_tickets"] == 0
    assert g.resolve("MOW") == {"SVO"} and g.resolve("BJS") == {"PKX", "PEK"}
    assert g.resolve("PKX") == {"PKX"}


def test_transfers_for_direction():
    res = _graph().transfers_for("MOW", "CAN")
    assert res["nonstop_min_price"] == 30000 and res["nonstop_tickets"] == 1
    assert [v["via"] for v in res["via"]] == [["PKX"]]
    assert res["via"][0]["min_price"] == 40000 and res["via"][0]["airlines"] == ["CZ"]


def test_hidden_city_via_city_code():
    res = _graph().hidden_city("MOW", "BJS")
    assert res["direct_min_price"] == 31000 and res["direct_days"] == 2
    got = [(t["date"], t["hub"], t["price"], t["direct_price"], t["direct_same_day"], t["saving"])
           for t in res["tickets"]]
    # ICN: прямого 05.11 не было → сравнение с min за всё время; HRB: прямой того же дня.
    assert got == [
        ("2026-10-29", "PKX", 24000, 36000, True, 12000),
        ("2026-11-05", "PEK", 20000, 31000, False, 11000),
    ]
    hrb = res["tickets"][0]
    assert hrb["dropped"] == ["HRB"] and hrb["next_leg_domestic"] is True
    assert hrb["baggage"]["included"] is True and hrb["url"].startswith("https://www.aviasales.ru/")
    icn = res["tickets"][1]
    assert icn["next_leg_domestic"] is False and icn["baggage"]["included"] is False


def test_hidden_city_same_day_only_and_airport_code():
    g = _graph()
    assert [t["hub"] for t in g.hidden_city("MOW", "BJS", same_day_only=True)["tickets"]] == ["PKX"]
    assert [t["hub"] for t in g.hidden_city("SVO", "PEK")["tickets"]] == ["PEK"]


def test_hidden_city_without_any_direct():
    res = _graph().hidden_city("MOW", "HRB")  # HRB — только конечная, не хаб
    assert res["tickets"] == [] and res["direct_min_price"] is None


def test_roundtrip_json_and_db():
    g = _graph()
    restored = TransferGraph.from_dict(json.loads(json.dumps(g.to_dict())), NETWORK)
    assert restored.stats()["edges"] == 6
    assert restored.hidden_city("MOW", "BJS")["tickets"][0]["saving"] == 12000

    conn = hot.connect(":memory:")
    hot.init_db(conn)
    hot.upsert_quotes(conn, [{**r, "flight_number": str(i)} for i, r in enumerate(ROWS)])
    from_db = build_from_db(conn, NETWORK)
    assert from_db.stats()["transfer_tickets"] == 3
