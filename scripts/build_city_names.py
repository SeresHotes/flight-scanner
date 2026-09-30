"""Собирает core/city_names.json — имена городов по коду для выдачи планировщика
(core/segments.make_city_lookup) из data/airport_network.json (build_airport_network.py).

Выдаче нужны только имя и страна: {код: [имя, ISO2 страны]} — сотни КБ вместо
20 МБ сети аэропортов с координатами и соседями, которую раньше читали на каждую
стыковку и каждый запрос маршрутов (~0.4 с на проде).

    poetry run python scripts/build_city_names.py     # перезаписать core/city_names.json
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "data" / "airport_network.json"
OUT = ROOT / "core" / "city_names.json"


def build(network: dict) -> dict:
    out = {}
    for code, info in sorted(network.items()):
        name = info.get("municipality") or info.get("name")
        if name:
            out[code] = [name, info.get("country") or info.get("iso_country") or ""]
    return out


def main() -> None:
    names = build(json.loads(SRC.read_text(encoding="utf-8")))
    OUT.write_text(json.dumps(names, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"{OUT}: {len(names)} кодов, {OUT.stat().st_size // 1024} КБ")


if __name__ == "__main__":
    main()
