"""Переезд в соседний город внутри остановки планировщика.

У остановки может быть радиус (Stop.radius_km): прилетели в город a, а дальше летим из
города d, если d == a или между ними не больше радиуса (по прямой между центрами
городов). Хотя бы один из двух — город самой остановки: остановка «Вена, 100 км»
допускает VIE → BTS и BTS → VIE, но не BTS → PZY. У концов маршрута то же: старт —
вылет из города старта или соседа, финиш — прилёт в город финиша или соседа.

Координаты — core/geo.json (scripts/build_geo.py, справочники Travelpayouts): коды
городов там те же, что в билетах GraphQL. Неизвестный код — соседей нет.
"""
import json
import math
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Tuple

GEO_PATH = Path(__file__).resolve().parent / "geo.json"

# Переезд между городами занимает время: вылет из соседнего города — не раньше, чем
# через столько минут после прилёта (дорога + регистрация). В своём городе, как и
# раньше, достаточно того, что вылет не раньше дня прилёта.
HOP_MIN_GAP_MIN = 240
MAX_RADIUS_KM = 500


@lru_cache(maxsize=1)
def _geo() -> Dict[str, Dict[str, list]]:
    try:
        return json.loads(GEO_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"cities": {}, "airports": {}}


def city_entry(code: str) -> Optional[list]:
    """[lat, lon, ISO2, имя] города; для кода аэропорта — его города."""
    geo = _geo()
    code = (code or "").upper()
    entry = geo["cities"].get(code)
    if entry is None and code in geo["airports"]:
        entry = geo["cities"].get(geo["airports"][code][2])
    return entry


def _point(code: str) -> Optional[Tuple[float, float]]:
    geo = _geo()
    code = (code or "").upper()
    c = geo["cities"].get(code) or geo["airports"].get(code)
    return (c[0], c[1]) if c else None


def _haversine(p: Tuple[float, float], q: Tuple[float, float]) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, (p[0], p[1], q[0], q[1]))
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 6371.0 * 2 * math.asin(math.sqrt(h))


def distance_km(a: str, b: str) -> Optional[float]:
    p, q = _point(a), _point(b)
    return _haversine(p, q) if p and q else None


@lru_cache(maxsize=4096)
def neighbors(code: str, radius_km: float) -> Tuple[str, ...]:
    """Города в радиусе от code (сам город и города его аэропорта не входят), по
    возрастанию расстояния."""
    if not radius_km or radius_km <= 0:
        return ()
    p = _point(code)
    if p is None:
        return ()
    geo = _geo()
    code = code.upper()
    own = {code, geo["airports"].get(code, [None, None, None])[2]}
    out = []
    for other, c in geo["cities"].items():
        if other in own:
            continue
        if abs(c[0] - p[0]) * 111.0 > radius_km:  # грубый отсев по широте
            continue
        d = _haversine(p, (c[0], c[1]))
        if d <= radius_km:
            out.append((d, other))
    return tuple(o for _, o in sorted(out))


def clamp_radius(radius_km) -> int:
    try:
        r = int(radius_km or 0)
    except (TypeError, ValueError):
        return 0
    return max(0, min(MAX_RADIUS_KM, r))


class Hops:
    """Правила переезда по остановкам запроса (stops — planner.Stop с radius_km).

    arrive_allowed(i) — в какие города можно прилететь в остановку i (None — любой);
    departs(i, a)     — из каких городов можно улететь дальше, прилетев в a (a первым);
    collect_codes(i)  — какие города остановки нужно собирать (для сбора и оценки)."""

    def __init__(self, stops):
        self.stops = stops
        self.last = len(stops) - 1
        self.radius = [float(getattr(s, "radius_km", 0) or 0) for s in stops]
        self._departs: Dict[Tuple[int, str], Tuple[str, ...]] = {}

    def active(self) -> bool:
        return any(r > 0 for r in self.radius)

    def _own(self, i: int) -> Optional[set]:
        s = self.stops[i]
        return set(s.codes) if s.kind == "cities" else None

    def expanded(self, i: int) -> List[str]:
        """Коды остановки + их соседи в радиусе (порядок стабильный)."""
        s = self.stops[i]
        out = list(s.codes)
        if self.radius[i] > 0:
            seen = set(out)
            for c in s.codes:
                for n in neighbors(c, self.radius[i]):
                    if n not in seen:
                        seen.add(n)
                        out.append(n)
        return out

    def arrive_allowed(self, i: int) -> Optional[set]:
        s = self.stops[i]
        if s.kind != "cities":
            return None
        if getattr(s, "exact", False):
            return set(s.codes)
        return set(self.expanded(i))

    def departs(self, i: int, a: str) -> Tuple[str, ...]:
        key = (i, a)
        got = self._departs.get(key)
        if got is None:
            got = self._departs[key] = self._compute_departs(i, a)
        return got

    def _compute_departs(self, i: int, a: str) -> Tuple[str, ...]:
        r = self.radius[i]
        s = self.stops[i]
        if r <= 0 or (i == 0 and getattr(s, "exact", False)):
            return (a,)
        own = self._own(i)
        near = neighbors(a, r)
        if own is not None and a not in own:
            near = tuple(n for n in near if n in own)  # прилетели к соседу — улетаем из города остановки
        return (a,) + near

    def collect_codes(self, i: int) -> List[str]:
        return self.expanded(i) if self.stops[i].kind == "cities" else []
