"""Обвязка Rust-движка (engine/, модуль planner_engine): те же результаты, что у
core.planner.build_itineraries_compact и core.overview.build_overview, но перебор A*
и наборы городов считает Rust. Коды городов/аэропортов → числа, фильтры плеча —
здесь (LegFilter.mask), сегменты результата — общий planner._pack_compact.

Переезд в соседний город (radiusKm > 0) Rust не умеет — available() = False, и
вызывающий считает Python-версией."""
from array import array
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np

from core import planner
from core.flightcols import FlightCols, as_cols

try:
    import planner_engine as _rs
except ImportError:          # модуль не собран — только Python-движок
    _rs = None


def available(stops: List[planner.Stop]) -> bool:
    return _rs is not None and all(not (s.radius_km or 0) for s in stops)


def _codes(table: FlightCols):
    """(словарь код → id, id-массивы orig_city, orig_airport, dest, dest_airport); '' → -1."""
    got = getattr(table, "_rs_codes", None)
    if got is None:
        vocab: Dict[str, int] = {}

        def enc(values):
            out = np.empty(len(values), dtype=np.int32)
            for i, s in enumerate(values):
                out[i] = vocab.setdefault(s, len(vocab)) if s else -1
            return out
        got = table._rs_codes = (vocab, enc(table.orig_city), enc(table.orig_airport),
                                 enc(table.dest), enc(table.dest_airport))
    return got


def _ord(day: str) -> int:
    return datetime.fromisoformat(day).date().toordinal()


def _args(stops, table: FlightCols, query):
    vocab, oc, oa, dest, da = _codes(table)
    last = len(stops) - 1
    rows = planner._leg_rows(table, last, query)
    filters = []
    for i in range(len(stops)):
        cf = query.cities[i] if query is not None and i < len(query.cities) else None
        if 1 <= i < last and cf is not None and not cf.is_open():
            cover = cf.must_cover
            filters.append((cf.min_stay, cf.max_stay if cf.max_stay is not None else -1,
                            _ord(cover[0]) if cover else -1, _ord(cover[1]) if cover else -1,
                            bool(cf.require_weekend)))
        else:
            filters.append(None)
    trip = query.trip_length if query is not None else [0, None]
    trip_arg = ((trip[0] or 0, trip[1] if trip[1] is not None else -1)
                if (trip[0] or trip[1] is not None) else None)
    stop_any = [s.kind == "any" for s in stops]
    stop_codes = [[vocab.get(c, -2) for c in s.codes] if s.kind == "cities" else [] for s in stops]
    cols = (oc, oa, dest, da, table.price, table.transfers.astype(np.int32), table.dep_ts, table.arr_ts,
            table.dep_ord.astype(np.int64), table.arr_ord.astype(np.int64))
    start_ord = _ord(planner._leg_dates(stops, 0)[0])
    return stop_any, stop_codes, [rows[i].astype(np.int64) for i in range(last)], cols, filters, trip_arg, start_ord


def build_itineraries_compact(stops, collected, max_results: int, city_info=None,
                              max_cost: Optional[float] = None, query=None, **_ignored) -> Dict[str, Any]:
    """planner.build_itineraries_compact на Rust (без should_stop/on_progress)."""
    table = as_cols(collected)
    if city_info is None:
        city_info = planner.make_city_lookup()
    stop_any, stop_codes, rows, cols, filters, trip, start_ord = _args(stops, table, query)
    flat = _rs.search(stop_any, stop_codes, rows, *cols, filters, trip, max_results, max_cost, start_ord)
    chain_start = planner._leg_dates(stops, 0)[0]
    ctx = (stops, None, planner.Builder(None, city_info), city_info, chain_start, len(stops) - 1)
    return planner._pack_compact(ctx, table, array("i", flat.tolist()), len(stops) - 1)


def build_overview(stops, collected, query=None, city_info=None) -> Dict[str, Any]:
    """core.overview.build_overview на Rust."""
    if city_info is None:
        city_info = planner.make_city_lookup()
    if len(stops) < 2:
        return {"combos": [], "totalCount": 0, "cities": {}}
    table = as_cols(collected)
    vocab = _codes(table)[0]
    names = {v: k for k, v in vocab.items()}
    stop_any, stop_codes, rows, cols, filters, trip, start_ord = _args(stops, table, query)
    budget = query.max_cost if query is not None else None
    raw = _rs.overview(stop_any, stop_codes, rows, *cols, filters, trip, budget, start_ord)
    combos = [{"codes": [names[c] for c in codes], "minPrice": float(minp), "transfersAtMin": int(tr),
               "minTransfers": int(mintr), "count": int(round(total))}
              for codes, minp, tr, mintr, total in raw]
    combos.sort(key=lambda c: (c["minPrice"], c["codes"]))
    used = sorted({c for it in combos for c in it["codes"]})
    cities = {c: [city_info(c)["city"], city_info(c)["flag"]] for c in used}
    return {"combos": combos, "totalCount": sum(c["count"] for c in combos), "cities": cities}
