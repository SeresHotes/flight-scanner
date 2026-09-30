"""Наборы городов (режим «города» планировщика) — на бэке.

Для каждой последовательности городов c0 → c1 → … → cL, которую можно собрать из
собранных рейсов под фильтры запроса, считаем: минимальную цену цепочки, число
пересадок у самой дешёвой, минимум пересадок, точное число цепочек. Цепочек
перечислять нельзя (экспоненциально), поэтому — динамика по префиксам:

состояние префикса c0…ck — матрица «день первого вылета × рейс последнего плеча
(c(k-1) → ck)» с числом цепочек, минимальной ценой, пересадками при минимуме и
минимумом пересадок. Расширение на все города следующего плеча разом: матрица
совместимости «рейс-предшественник × рейс-продолжение» по тем же правилам, что у
перебора planner._search_cheapest (вылет не раньше дня прилёта, фильтры пребывания
промежуточной остановки — дни между прилётом и вылетом, обязательное окно, оба
выходных; повторы городов — только у «любой»), затем свёртка numpy и разрез по
городу прилёта. Длина поездки — на последнем плече по дню первого вылета.
Граница max_results не действует: обзор оценивает все варианты. Бюджет max_cost —
фильтр: рейсы дороже него не участвуют, а ячейки (день × рейс), где даже самая
дешёвая цепочка дороже бюджета, обнуляются — набор без цепочки в бюджете не
показывается; count остаётся оценкой сверху (цепочки дороже минимума ячейки не
отсекаются — для этого нужна не минимальная цена, а распределение цен).

Времена — «наивные» локальные, как в aggregate.parse_datetime (таймзона срезана),
дни пребывания — floor((вылет − прилёт) / сутки), как planner._stay.
"""
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from core.dates import parse_datetime
from core.network import load_airport_network
from core.planner import (FINAL_STAY_DAYS, Stop, _allowed, _apply_leg_filters, _index_leg,  # noqa
                          _leg_dates, _price_of, _side_codes, arrival_of, make_city_lookup)
from core.nearby import HOP_MIN_GAP_MIN, Hops
from core.planquery import PlanQuery

_EPOCH = datetime(1970, 1, 1)
_DAY = 86400.0
_INF = float("inf")


def _naive_seconds(iso: str) -> float:
    return (parse_datetime(iso) - _EPOCH).total_seconds()


def _ordinal(iso: str) -> int:
    return parse_datetime(iso).date().toordinal()


def _weekend_deadline(arr_ord: int) -> int:
    """Первый день D ≥ arr, при котором [arr, D] содержит и сб, и вс (см.
    planner._has_both_weekend_days): вылет не раньше D — выходные покрыты."""
    d = arr_ord
    sat = sun = False
    while True:
        wd = (d + 6) % 7   # toordinal: 1 = понедельник → (d+6)%7: 0 пн … 5 сб, 6 вс
        sat = sat or wd == 5
        sun = sun or wd == 6
        if sat and sun:
            return d
        d += 1


class _Leg:
    """Рейсы плеча в массивах + индекс по коду вылета и группировка по городу прилёта."""

    def __init__(self, flights: List[Dict[str, Any]]):
        self.flights = flights
        n = len(flights)
        self.dest = [(f.get("destination") or f.get("search_destination") or "").upper() for f in flights]
        self.price = np.array([_price_of(f) for f in flights], dtype=float)
        self.transfers = np.array([int(f.get("transfers") or 0) for f in flights], dtype=np.int32)
        dep = [f.get("departure_at") for f in flights]
        arr = [arrival_of(f) for f in flights]
        self.dep_ts = np.array([_naive_seconds(d) for d in dep], dtype=float)
        self.arr_ts = np.array([_naive_seconds(a) for a in arr], dtype=float)
        self.dep_ord = np.array([_ordinal(d) for d in dep], dtype=np.int64)
        self.arr_ord = np.array([_ordinal(a) for a in arr], dtype=np.int64)
        self.weekend_ok_from = np.array([_weekend_deadline(int(o)) for o in self.arr_ord], dtype=np.int64)
        # индекс по коду вылета (город И аэропорт, как planner._index_leg)
        by_origin: Dict[str, List[int]] = {}
        for i, f in enumerate(flights):
            for code in _side_codes(f, "origin"):
                by_origin.setdefault(code, []).append(i)
        self.by_origin = {c: np.array(ix, dtype=np.int64) for c, ix in by_origin.items()}
        self.dest_codes = [_side_codes(f, "dest") for f in flights]
        self.origin_codes = [_side_codes(f, "origin") for f in flights]


def _compat(prev: _Leg, p_idx: np.ndarray, nxt: _Leg, f_idx: np.ndarray, cf,
            hop: Optional[np.ndarray] = None) -> np.ndarray:
    """Матрица |p| × |f|: можно ли после рейса p лететь рейсом f (правила перебора).
    hop — рейсы f из соседнего города: нужен запас на переезд (nearby.HOP_MIN_GAP_MIN)."""
    arr_ord = prev.arr_ord[p_idx][:, None]
    dep_ord = nxt.dep_ord[f_idx][None, :]
    ok = dep_ord >= arr_ord
    if hop is not None and hop.any():
        gap = nxt.dep_ts[f_idx][None, :] - prev.arr_ts[p_idx][:, None]
        ok &= ~hop[None, :] | (gap >= HOP_MIN_GAP_MIN * 60)
    if cf is not None:
        stay = np.floor((nxt.dep_ts[f_idx][None, :] - prev.arr_ts[p_idx][:, None]) / _DAY)
        ok &= stay >= cf.min_stay
        if cf.max_stay is not None:
            ok &= stay <= cf.max_stay
        if cf.must_cover:
            f_ord = datetime.strptime(cf.must_cover[0], "%Y-%m-%d").date().toordinal()
            t_ord = datetime.strptime(cf.must_cover[1], "%Y-%m-%d").date().toordinal()
            ok &= (arr_ord <= f_ord) & (dep_ord >= t_ord)
        if cf.require_weekend:
            ok &= dep_ord >= prev.weekend_ok_from[p_idx][:, None]
    return ok


def _extend(state, prev: _Leg, p_idx: np.ndarray, nxt: _Leg, f_idx: np.ndarray, cf,
            hop: Optional[np.ndarray] = None):
    """Состояние префикса (по рейсам p) → состояние по рейсам f (D × |f|)."""
    cnt, minp, tr_at, mintr = state
    ok = _compat(prev, p_idx, nxt, f_idx, cf, hop)                     # P × F
    okf = ok.astype(float)
    new_cnt = cnt @ okf                                           # D × F
    big = np.where(ok[None, :, :], minp[:, :, None], _INF)        # D × P × F
    arg = big.argmin(axis=1)                                      # D × F
    new_minp = np.take_along_axis(big, arg[:, None, :], axis=1)[:, 0, :] + nxt.price[f_idx][None, :]
    new_tr_at = np.take_along_axis(tr_at, arg, axis=1) + nxt.transfers[f_idx][None, :]
    big_tr = np.where(ok[None, :, :], mintr[:, :, None], np.int32(1 << 20))
    new_mintr = big_tr.min(axis=1) + nxt.transfers[f_idx][None, :]
    return new_cnt, new_minp, new_tr_at, new_mintr


def build_overview(stops: List[Stop], collected: Dict[int, List[Dict[str, Any]]],
                   query: Optional[PlanQuery] = None, city_info=None) -> Dict[str, Any]:
    """{combos: [{codes, minPrice, transfersAtMin, minTransfers, count}] по цене,
    totalCount, cities: {code: [city, flag]}}."""
    if city_info is None:
        city_info = make_city_lookup(load_airport_network())
    last = len(stops) - 1
    if last < 1:
        return {"combos": [], "totalCount": 0, "cities": {}}
    budget = query.max_cost if query is not None else None
    filtered = _apply_leg_filters(collected, query)
    if budget is not None:
        filtered = {i: [f for f in fl if _price_of(f) <= budget] for i, fl in filtered.items()}
    legs = [_Leg(filtered.get(i, [])) for i in range(last)]
    city_filters = {i: query.cities[i] for i in range(1, last)
                    if query is not None and i < len(query.cities) and not query.cities[i].is_open()}
    trip = query.trip_length if query is not None else [0, None]
    trip_active = bool(trip[0]) or trip[1] is not None
    start_ord = datetime.strptime(_leg_dates(stops, 0)[0], "%Y-%m-%d").date().toordinal()

    combos: List[Dict[str, Any]] = []
    codes_used = set()
    hops = Hops(stops)

    def onward(leg: _Leg, k: int, city: str) -> np.ndarray:
        """Рейсы плеча k после прилёта в city: из него самого и из соседей (переезд)."""
        parts = [leg.by_origin[d] for d in hops.departs(k, city) if d in leg.by_origin]
        if not parts:
            return np.array([], dtype=np.int64)
        return np.unique(np.concatenate(parts)) if len(parts) > 1 else parts[0]

    def groups(leg: _Leg, f_idx: np.ndarray, k: int, seq: Tuple[str, ...]):
        """Разрез кандидатов плеча k по городу прилёта с правилами остановки k+1."""
        allow = hops.arrive_allowed(k + 1)
        out: Dict[str, List[int]] = {}
        city = seq[-1]
        for fi in f_idx.tolist():
            dest = leg.dest[fi]
            if not dest or dest == city or dest in leg.origin_codes[fi]:
                continue
            if allow is None and dest in seq:
                continue
            if allow is not None and not (leg.dest_codes[fi] & allow):
                continue
            out.setdefault(dest, []).append(fi)
        return out

    def finish(seq: Tuple[str, ...], leg: _Leg, f_idx: np.ndarray, state, first_days: np.ndarray):
        cnt, minp, tr_at, mintr = state
        if trip_active:
            days = np.maximum(1, leg.arr_ord[f_idx][None, :] - first_days[:, None])
            mask = days >= (trip[0] or 0)
            if trip[1] is not None:
                mask &= days <= trip[1]
            cnt = np.where(mask, cnt, 0.0)
            minp = np.where(mask, minp, _INF)
            mintr = np.where(mask, mintr, 1 << 20)
        if budget is not None:
            mask = minp <= budget
            cnt = np.where(mask, cnt, 0.0)
            minp = np.where(mask, minp, _INF)
            mintr = np.where(mask, mintr, 1 << 20)
        total = float(cnt.sum())
        if total <= 0:
            return
        flat = minp.argmin()
        combos.append({
            "codes": list(seq),
            "minPrice": float(minp.reshape(-1)[flat]),
            "transfersAtMin": int(tr_at.reshape(-1)[flat]),
            "minTransfers": int(mintr.min()),
            "count": int(round(total)),
        })
        codes_used.update(seq)

    def expand(k: int, seq: Tuple[str, ...], prev: _Leg, p_idx: np.ndarray, state, first_days):
        """Префикс seq (последнее плечо k-1 рейсами p) → все города плеча k."""
        leg = legs[k]
        f_all = onward(leg, k, seq[-1])
        if not len(f_all):
            return
        for dest, fis in groups(leg, f_all, k, seq).items():
            f_idx = np.array(fis, dtype=np.int64)
            hop = np.array([seq[-1] not in leg.origin_codes[fi] for fi in fis], dtype=bool)
            new_state = _extend(state, prev, p_idx, leg, f_idx, city_filters.get(k), hop)
            if new_state[0].sum() <= 0:
                continue
            new_seq = seq + (dest,)
            if k + 1 == last:
                finish(new_seq, leg, f_idx, new_state, first_days)
            else:
                expand(k + 1, new_seq, leg, f_idx, new_state, first_days)

    leg0 = legs[0]
    # Первый код набора — реальный город вылета (со стартом-соседом — сам сосед).
    starts: List[str] = []
    for code in stops[0].codes:
        for d in hops.departs(0, code):
            if d not in starts:
                starts.append(d)
    for start in starts:
        f_all = leg0.by_origin.get(start)
        if f_all is None:
            continue
        for dest, fis in groups(leg0, f_all, 0, (start,)).items():
            f_idx = np.array(fis, dtype=np.int64)
            ok = leg0.dep_ord[f_idx] >= start_ord
            if trip_active:
                first_days = np.unique(leg0.dep_ord[f_idx])
                cnt = ((leg0.dep_ord[f_idx][None, :] == first_days[:, None]) & ok[None, :]).astype(float)
            else:
                first_days = np.array([0])
                cnt = ok.astype(float)[None, :]
            minp = np.where(cnt > 0, leg0.price[f_idx][None, :], _INF)
            tr_at = np.broadcast_to(leg0.transfers[f_idx][None, :], cnt.shape).copy()
            mintr = np.where(cnt > 0, leg0.transfers[f_idx][None, :], 1 << 20)
            state = (cnt, minp, tr_at, mintr)
            seq = (start, dest)
            if last == 1:
                finish(seq, leg0, f_idx, state, first_days)
            else:
                expand(1, seq, leg0, f_idx, state, first_days)

    combos.sort(key=lambda c: (c["minPrice"], c["codes"]))
    cities = {c: [city_info(c)["city"], city_info(c)["flag"]] for c in sorted(codes_used)}
    return {"combos": combos, "totalCount": sum(c["count"] for c in combos), "cities": cities}
