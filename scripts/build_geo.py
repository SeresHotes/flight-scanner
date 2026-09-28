"""Собирает core/geo.json — координаты городов и аэропортов из справочников
Travelpayouts (data/en/cities.json, data/en/airports.json; docs/travelpayouts/README.md,
раздел «Справочники»). Коды городов там те же, что отдаёт GraphQL (MOW, VIE, BTS),
поэтому по ним планировщик ищет соседние города для пересадки (core/nearby.py).

Берём только города с действующим аэропортом и аэропорты с рейсами:
    {"cities":   {код: [lat, lon, ISO2 страны, имя (en)]},
     "airports": {код: [lat, lon, код города]}}

    poetry run python scripts/build_geo.py            # перезаписать core/geo.json
"""
import argparse
import json
import urllib.request
from pathlib import Path

BASE = "https://api.travelpayouts.com/data/en"
OUT = Path(__file__).resolve().parent.parent / "core" / "geo.json"


def _get(name: str):
    req = urllib.request.Request(f"{BASE}/{name}.json", headers={"Accept-Encoding": "identity"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def _coords(entry):
    c = entry.get("coordinates") or {}
    lat, lon = c.get("lat"), c.get("lon")
    if lat is None or lon is None:
        return None
    return [round(float(lat), 4), round(float(lon), 4)]


def _iata(code) -> bool:
    """Только настоящие IATA-коды: в справочнике встречаются и кириллические («ВЛМ»)."""
    return isinstance(code, str) and len(code) == 3 and code.isascii() and code.isalpha() and code.isupper()


def build(cities, airports):
    out_cities = {}
    for c in cities:
        xy = _coords(c)
        if c.get("has_flightable_airport") and xy and _iata(c.get("code")):
            out_cities[c["code"]] = xy + [c.get("country_code") or "", c.get("name") or c["code"]]
    out_airports = {}
    for a in airports:
        xy = _coords(a)
        if a.get("flightable") and a.get("iata_type") == "airport" and xy and _iata(a.get("code")):
            out_airports[a["code"]] = xy + [a.get("city_code") or ""]
    return {"cities": dict(sorted(out_cities.items())), "airports": dict(sorted(out_airports.items()))}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", default=str(OUT))
    args = p.parse_args()
    geo = build(_get("cities"), _get("airports"))
    Path(args.out).write_text(json.dumps(geo, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"{args.out}: городов {len(geo['cities'])}, аэропортов {len(geo['airports'])}")


if __name__ == "__main__":
    main()
