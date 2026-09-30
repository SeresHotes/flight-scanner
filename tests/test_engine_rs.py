"""Rust-движок (engine/, planner_engine) == Python-движок: цепочки (с порядком при
равных ценах), наборы городов на случайных рейсах со всеми фильтрами. Без собранного
модуля тест пропускается (maturin build -m engine/Cargo.toml)."""
import itertools
import random

import pytest

pytest.importorskip("planner_engine")

from core import engine_rs, planner  # noqa: E402
from core.flightcols import FlightCols  # noqa: E402
from core.overview import build_overview  # noqa: E402
from core.planner import Stop  # noqa: E402
from core.planquery import PlanQuery  # noqa: E402

CITY_INFO = lambda code: {"city": code, "country": "", "flag": ""}  # noqa: E731
CITIES = ["IST", "DXB", "TAS", "ALA", "BAK", "EVN"]


def _flights(rnd: random.Random, legs):
    """Плечи: [(откуда-коды, куда-коды)] — случайные рейсы по дням 1–12.11, много равных цен."""
    out = {}
    for i, (froms, tos) in enumerate(legs):
        fl = []
        for _ in range(rnd.randrange(20, 60)):
            o, d = rnd.choice(froms), rnd.choice(tos)
            if o == d:
                continue
            day = rnd.randrange(1, 13)
            h = rnd.randrange(0, 24)
            dur = rnd.randrange(2, 30)
            dep = f"2026-11-{day:02d}T{h:02d}:00:00+03:00"
            arr_h = h + dur
            arr = f"2026-11-{day + arr_h // 24:02d}T{arr_h % 24:02d}:00:00+03:00"
            tr = rnd.randrange(0, 3)
            fl.append({"origin": o, "destination": d, "origin_airport": o + "A" if rnd.random() < 0.2 else o,
                       "destination_airport": d, "departure_at": dep, "arrival_at": arr, "duration": dur * 60,
                       "duration_to": dur * 50, "transfers": tr, "price": float(rnd.choice([100, 150, 200, 300])),
                       "flight_number": str(rnd.randrange(1000)), "airline": "XX",
                       "transfer_points": [{"code": "HUB", "minutes": rnd.choice([30, 90, 200])}] * tr,
                       "baggage": {"known": True, "included": rnd.random() < 0.5}})
        out[i] = fl
    return out


QUERIES = [
    {},
    {"tripLength": [0, 8]},
    {"tripLength": [3, None], "maxCost": 700},
    {"cities": [{}, {"minStay": 1, "maxStay": 4}, {"requireWeekend": True}, {}]},
    {"cities": [{}, {"mustCover": ["2026-11-05", "2026-11-06"]}, {"minStay": 2}, {}], "tripLength": [2, 11]},
    {"legs": [{"maxTransfers": 1}, {"baggage": "included"}, {"minLayoverMin": 60}]},
]


@pytest.mark.parametrize("seed,qi", list(itertools.product(range(4), range(len(QUERIES)))))
def test_rust_engine_matches_python(seed, qi):
    rnd = random.Random(seed)
    stops = [Stop("cities", ["MOW"], ["", ""]), Stop("any", [], ["2026-11-01", "2026-11-06"]),
             Stop("cities", ["TAS", "ALA"], ["2026-11-03", "2026-11-10"]), Stop("cities", ["MOW"], ["", ""])]
    legs = [(["MOW"], CITIES), (CITIES, ["TAS", "ALA"]), (["TAS", "ALA"], ["MOW"])]
    table = FlightCols.from_collected(_flights(rnd, legs))
    raw = {"stops": [{"kind": s.kind, "codes": s.codes, "window": s.window} for s in stops],
           **QUERIES[qi], "maxResults": 300}
    pq = PlanQuery.from_dict(raw)
    assert engine_rs.available(stops)
    py = planner.build_itineraries_compact(stops, table, max_results=300, city_info=CITY_INFO,
                                           max_cost=pq.max_cost, query=pq)
    rs = engine_rs.build_itineraries_compact(stops, table, max_results=300, city_info=CITY_INFO,
                                             max_cost=pq.max_cost, query=pq)
    assert planner.compact_to_json(rs) == planner.compact_to_json(py)
    assert engine_rs.build_overview(stops, table, pq, city_info=CITY_INFO) == \
        build_overview(stops, table, pq, city_info=CITY_INFO)
