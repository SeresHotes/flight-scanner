"""Каталог сборов: «какие запросы мы делали при сборе».

Важный нюанс: сбор «ICN→MOW с остановкой» — это НЕ один запрос, а два разных:
`ICN → любой` (все направления из ICN) и `любой → MOW` (все направления в MOW).
Это закодировано в каждой котировке полями search_origin / search_destination
(None = «любой»). Здесь мы группируем именно по реальным запросам
(search_origin, search_destination, тип рейсов) — так видно, что собиралось на деле.
"""
import glob
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List


def _flight_type(meta: Dict[str, Any]) -> str:
    ft = meta.get("flight_type")
    if ft:
        return ft
    return "indirect" if meta.get("allow_indirect") else "direct"


def scan_collections(data_dir: str = "data",
                     patterns: Iterable[str] = ("flights_*.json", "mcr_*.json",
                                                "mow_*.json", "data2.json")) -> List[Dict[str, Any]]:
    """Список реальных запросов сбора, сгруппированный по (search_origin, search_destination, direct).

    origin/destination = None означает «любой» (запрос по всем направлениям).
    """
    groups: Dict[tuple, Dict[str, Any]] = {}
    seen = set()
    for pat in patterns:
        for path in sorted(glob.glob(str(Path(data_dir) / pat))):
            name = os.path.basename(path)
            if path in seen or name.startswith("test"):
                continue
            seen.add(path)
            try:
                data = json.load(open(path, encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            if not (isinstance(data, dict) and ("leg1_flights" in data or "leg2_flights" in data)):
                continue

            meta = data.get("metadata") or {}
            direct = _flight_type(meta) != "indirect"
            collected_at = meta.get("collected_at")
            flights = (data.get("leg1_flights") or []) + (data.get("leg2_flights") or [])

            for f in flights:
                so = f.get("search_origin") or None
                sd = f.get("search_destination") or None
                sday = f.get("search_date") or (f.get("departure_at") or "")[:10] or None
                key = (so, sd, direct)
                g = groups.get(key)
                if g is None:
                    g = groups[key] = {
                        "origin": so, "destination": sd, "direct": direct,
                        "dep_from": sday, "dep_to": sday,
                        "collected_at": collected_at, "flights": 0, "_files": set(),
                    }
                g["flights"] += 1
                g["_files"].add(path)
                if sday:
                    if not g["dep_from"] or sday < g["dep_from"]:
                        g["dep_from"] = sday
                    if not g["dep_to"] or sday > g["dep_to"]:
                        g["dep_to"] = sday
                if collected_at and (not g["collected_at"] or collected_at > g["collected_at"]):
                    g["collected_at"] = collected_at

    out = []
    for g in groups.values():
        g["runs"] = len(g.pop("_files"))
        out.append(g)
    return sorted(out, key=lambda g: (g["collected_at"] or "", g["flights"]), reverse=True)
