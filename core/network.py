"""Справочник аэропортов data/airport_network.json (build_airport_network.py):
{IATA: {name, municipality, country, coordinates, nearby_airports}}."""
import json
from pathlib import Path
from typing import Dict

DEFAULT_NETWORK_PATH = "data/airport_network.json"


def load_airport_network(network_file: str = DEFAULT_NETWORK_PATH) -> Dict[str, Dict]:
    """Сеть аэропортов; пустой словарь, если файла нет или он битый (имена городов
    тогда берутся из курируемого списка core/airports.CITY_CODES)."""
    path = Path(network_file)
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError) as e:
        print(f"[network] не удалось загрузить {network_file}: {e}")
        return {}
