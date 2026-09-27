"""Единый запрос планировщика (PlanQuery): скелет + все фильтры одним объектом.

Контракт с фронтом (docs/PLANNER_V2.md, «PlanQuery»):

    stops[]:      {kind: cities|any, codes[], window[start, end]}
    cities[]:     {minStay, maxStay, mustCover: [a, b] | null, requireWeekend}   # == stops
    legs[]:       {maxTransfers, minLayoverMin, travelMin: [lo, hi],
                   baggage: any|included|none, hiddenCity}                       # == stops - 1
    tripLength:   [lo, hi]      maxCost: number|null      maxResults: number

Все фильтры необязательны: отсутствующие = «без ограничений», поэтому запрос старого
фронта (только stops/max_results/max_cost) остаётся валидным. Семантика фильтров
повторяет прежнюю клиентскую (frontend/src/planner/filtering.ts): фильтры
пребывания действуют только у промежуточных остановок, у концов — нет.

Фильтры плеча применяются к списку рейсов ДО построения (filter_leg_flights),
фильтры городов — внутри перебора (planner._search_cheapest), длина поездки —
при выдаче цепочки. key() — хэш канонического запроса: по нему джобы
дедуплицируются и переиспользуются.
"""
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

BAGGAGE_MODES = ("any", "included", "none")


@dataclass
class CityFilter:
    min_stay: int = 0
    max_stay: Optional[int] = None            # None — без потолка
    must_cover: Optional[List[str]] = None    # [a, b] — пребывание обязано покрыть окно
    require_weekend: bool = False             # сб и вс внутри пребывания

    @staticmethod
    def from_dict(d: Optional[Dict[str, Any]]) -> "CityFilter":
        d = d or {}
        cover = d.get("mustCover") or None
        if cover and not (len(cover) == 2 and cover[0] and cover[1]):
            cover = None
        return CityFilter(
            min_stay=int(d.get("minStay") or 0),
            max_stay=int(d["maxStay"]) if d.get("maxStay") is not None else None,
            must_cover=[cover[0], cover[1]] if cover else None,
            require_weekend=bool(d.get("requireWeekend")),
        )

    def is_open(self) -> bool:
        return (self.min_stay <= 0 and self.max_stay is None and not self.must_cover
                and not self.require_weekend)

    def as_dict(self) -> Dict[str, Any]:
        return {"minStay": self.min_stay, "maxStay": self.max_stay,
                "mustCover": self.must_cover, "requireWeekend": self.require_weekend}


@dataclass
class LegFilter:
    max_transfers: int = -1                   # -1 — любое число
    min_layover_min: int = 0                  # минимум ожидания на КАЖДОЙ пересадке
    travel_min: List[Optional[int]] = field(default_factory=lambda: [0, None])
    baggage: str = "any"                      # any | included | none
    hidden_city: bool = True                  # показывать виртуальные рейсы hidden-city

    @staticmethod
    def from_dict(d: Optional[Dict[str, Any]]) -> "LegFilter":
        d = d or {}
        travel = d.get("travelMin") or [0, None]
        baggage = d.get("baggage") or "any"
        if baggage not in BAGGAGE_MODES:
            raise ValueError(f"baggage: {baggage!r}")
        return LegFilter(
            max_transfers=int(d.get("maxTransfers", -1) if d.get("maxTransfers") is not None else -1),
            min_layover_min=int(d.get("minLayoverMin") or 0),
            travel_min=[int(travel[0] or 0), int(travel[1]) if len(travel) > 1 and travel[1] is not None else None],
            baggage=baggage,
            hidden_city=bool(d.get("hiddenCity", True)),
        )

    def as_dict(self) -> Dict[str, Any]:
        return {"maxTransfers": self.max_transfers, "minLayoverMin": self.min_layover_min,
                "travelMin": list(self.travel_min), "baggage": self.baggage,
                "hiddenCity": self.hidden_city}

    def accepts(self, f: Dict[str, Any]) -> bool:
        """Проходит ли рейс фильтр плеча (нормализованный билет или рейс REST)."""
        if not self.hidden_city and f.get("hidden_city"):
            return False
        transfers = int(f.get("transfers") or 0)
        if self.max_transfers >= 0 and transfers > self.max_transfers:
            return False
        duration = int(f.get("duration") or 0)
        lo, hi = self.travel_min
        if duration < (lo or 0) or (hi is not None and duration > hi):
            return False
        if self.min_layover_min > 0 and transfers:
            points = f.get("transfer_points") or []
            minutes = [p.get("minutes") for p in points if isinstance(p, dict)]
            if minutes and all(m is not None for m in minutes):
                if min(minutes) < self.min_layover_min:
                    return False
            elif f.get("layover_minutes") is not None and transfers == 1:
                if f["layover_minutes"] < self.min_layover_min:
                    return False
            # иначе ожидание неизвестно — не отсекаем
        if self.baggage != "any":
            bag = f.get("baggage") or {}
            included = bool(bag.get("included"))
            if self.baggage == "included" and not included:
                return False
            if self.baggage == "none" and included:
                return False
        return True


@dataclass
class PlanQuery:
    stops: List[Dict[str, Any]]
    cities: List[CityFilter]
    legs: List[LegFilter]
    trip_length: List[Optional[int]]          # [lo, hi]; hi None — без потолка
    max_cost: Optional[float]
    max_results: Optional[int]

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "PlanQuery":
        stops = [{"kind": s.get("kind", "cities"),
                  "codes": [c.upper() for c in (s.get("codes") or
                                                 [a.get("code") for a in (s.get("airports") or [])]) if c],
                  "window": list(s.get("window") or ["", ""])}
                 for s in d.get("stops") or []]
        n = len(stops)
        cities_raw = d.get("cities") or []
        legs_raw = d.get("legs") or []
        cities = [CityFilter.from_dict(cities_raw[k] if k < len(cities_raw) else None) for k in range(n)]
        legs = [LegFilter.from_dict(legs_raw[k] if k < len(legs_raw) else None) for k in range(max(0, n - 1))]
        trip = d.get("tripLength") or [0, None]
        max_cost = d.get("maxCost", d.get("max_cost"))
        max_results = d.get("maxResults", d.get("max_results"))
        return PlanQuery(
            stops=stops, cities=cities, legs=legs,
            trip_length=[int(trip[0] or 0), int(trip[1]) if len(trip) > 1 and trip[1] is not None else None],
            max_cost=float(max_cost) if max_cost is not None else None,
            max_results=int(max_results) if max_results is not None else None,
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "stops": self.stops,
            "cities": [c.as_dict() for c in self.cities],
            "legs": [l.as_dict() for l in self.legs],
            "tripLength": list(self.trip_length),
            "maxCost": self.max_cost,
            "maxResults": self.max_results,
        }

    def key(self) -> str:
        """Хэш канонического запроса — ключ дедупликации джоб и кэша результата."""
        canon = json.dumps(self.as_dict(), sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha1(canon.encode()).hexdigest()[:16]

    def mode(self) -> str:
        """Режим показа: 'combos' (наборы городов), если где-то «любой» или несколько
        городов, иначе 'routes' — сразу маршруты."""
        for s in self.stops:
            if s["kind"] == "any" or len(s["codes"]) > 1:
                return "combos"
        return "routes"

    def has_city_filters(self) -> bool:
        return any(not c.is_open() for c in self.cities)


def filter_leg_flights(flights: List[Dict[str, Any]], leg: Optional[LegFilter]) -> List[Dict[str, Any]]:
    """Рейсы плеча, прошедшие фильтр плеча (None — все)."""
    if leg is None:
        return list(flights)
    return [f for f in flights if leg.accepts(f)]


def trip_length_ok(days: int, trip_length: List[Optional[int]]) -> bool:
    lo, hi = trip_length
    return days >= (lo or 0) and (hi is None or days <= hi)
