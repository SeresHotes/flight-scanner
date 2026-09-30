"""Рейсы джобы колонками (core.flightcols): Parquet туда-обратно, маска фильтра плеча
== LegFilter.accepts, векторные «выходные» == поштучные, хранение и перенос старого
формата (gzip JSON в SQLite) в файл."""
import gzip
import itertools
import json

import numpy as np

from core.flightcols import FlightCols
from core.overview import _weekend_deadline, _weekend_deadlines
from core.planquery import LegFilter
from storage import hot
from tests.test_planner_hidden_city import DIRECT_PKX, SECOND_HOP_PEK, VIA_OTHER_HUB, VIA_PKX_HRB, _ticket


def _flights():
    no_dep = {**DIRECT_PKX, "flight_number": "9", "departure_at": None}
    by_duration = {**VIA_OTHER_HUB, "arrival_at": None, "duration": 600, "flight_number": "8"}
    rest = {"origin": "LED", "destination": "IST", "departure_at": "2026-10-30T08:00:00Z", "price": None,
            "value": 5000, "transfers": 1, "layover_minutes": 45, "search_destination": "IST"}
    unknown_pts = {**SECOND_HOP_PEK, "transfer_points": [{"code": "URC", "minutes": None}], "flight_number": "7"}
    hidden = {**VIA_PKX_HRB, "hidden_city": {"final": "HRB"}, "flight_number": "6"}
    return {0: [DIRECT_PKX, VIA_PKX_HRB, SECOND_HOP_PEK, no_dep, by_duration],
            1: [rest, unknown_pts, hidden,
                _ticket("BJS", "SEL", ["PEK", "ICN"], "2026-10-31T01:00:00+08:00", "2026-10-31T04:00:00+09:00",
                        9000, baggage=False)]}


def test_parquet_round_trip_keeps_columns_and_flights(tmp_path):
    collected = _flights()
    cols = FlightCols.from_collected(collected)
    path = str(tmp_path / "j.parquet")
    cols.write(path)
    back = FlightCols.read(path)
    for k in ("orig_city", "orig_airport", "dest", "dest_airport", "dep_iso", "arr_iso", "dep_day", "arr_day",
              "price_list"):
        assert getattr(back, k) == getattr(cols, k), k
    for k in ("leg", "price", "transfers", "duration", "hidden", "bag_incl", "dep_ord", "arr_ord"):
        assert np.array_equal(getattr(back, k), getattr(cols, k)), k
    for k in ("pts_min", "layover", "dep_ts", "arr_ts"):
        assert np.array_equal(getattr(back, k), getattr(cols, k), equal_nan=True), k
    flat = [f for leg in sorted(collected) for f in collected[leg]]
    assert [back.flight(i) for i in range(len(back))] == flat
    assert back.dest[5] == "IST" and back.price_list[5] == 5000          # search_destination, value
    assert back.dep_iso[3] is None and back.dep_ord[3] == -1             # без вылета
    assert back.arr_iso[4] == "2026-10-29T18:00:00"                      # прилёт по длительности


def test_leg_filter_mask_matches_accepts():
    collected = _flights()
    cols = FlightCols.from_collected(collected)
    flat = [f for leg in sorted(collected) for f in collected[leg]]
    rows = np.arange(len(flat))
    variants = itertools.product([-1, 0, 1], [0, 60, 200], [[0, None], [300, 900]],
                                 ["any", "included", "none"], [True, False])
    for mt, lay, travel, bag, hidden in variants:
        leg = LegFilter(max_transfers=mt, min_layover_min=lay, travel_min=travel, baggage=bag, hidden_city=hidden)
        assert leg.mask(cols, rows).tolist() == [leg.accepts(f) for f in flat], leg


def test_vector_weekend_deadline_matches_scalar():
    ords = np.arange(739000, 739100, dtype=np.int64)
    assert _weekend_deadlines(ords).tolist() == [_weekend_deadline(int(o)) for o in ords]


def test_store_writes_file_and_migrates_old_blob(tmp_path):
    conn = hot.connect(str(tmp_path / "jobs.db"))
    hot.init_db(conn)
    hot.put_plan_flights(conn, "new", _flights())
    assert hot.plan_flights_path(conn, "new").exists()
    assert len(hot.get_plan_flights(conn, "new")) == 9

    blob = gzip.compress(json.dumps({str(k): v for k, v in _flights().items()}).encode())
    conn.execute("INSERT INTO plan_flights (job_id, created_at, data) VALUES ('old', '2026-09-30', ?)", (blob,))
    got = hot.get_plan_flights(conn, "old")
    assert len(got) == 9 and hot.plan_flights_path(conn, "old").exists()
    assert conn.execute("SELECT COUNT(*) FROM plan_flights WHERE job_id='old'").fetchone()[0] == 0
    assert hot.get_plan_flights(conn, "nope") is None
