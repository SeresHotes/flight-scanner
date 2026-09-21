"""Справочник аэропортов и автокомплит точек A/B.

Основа — data/airport_network.json (физические аэропорты: SVO, DME, ICN, …).
Но проект работает на уровне ГОРОДОВ/агломераций (MOW, SEL, BJS…), а таких кодов
в сети нет, да и русские названия там английские. Поэтому сверху подмешиваем
курируемый список городов-агломераций с русскими и английскими именами.
"""
from functools import lru_cache
from typing import Any, Dict, List, Optional

from core import aggregate as agg
from core.trip_builder import flag_emoji

DEFAULT_NETWORK_PATH = "data/airport_network.json"

# (IATA города, русское имя, английское имя, ISO2 страны). Ранжируются выше аэропортов.
CITY_CODES = [
    ("MOW", "Москва", "Moscow", "RU"),
    ("LED", "Санкт-Петербург", "Saint Petersburg", "RU"),
    ("SVX", "Екатеринбург", "Yekaterinburg", "RU"),
    ("OVB", "Новосибирск", "Novosibirsk", "RU"),
    ("KZN", "Казань", "Kazan", "RU"),
    ("AER", "Сочи", "Sochi", "RU"),
    ("SEL", "Сеул", "Seoul", "KR"),
    ("ICN", "Сеул (Инчхон)", "Seoul Incheon", "KR"),
    ("BJS", "Пекин", "Beijing", "CN"),
    ("SHA", "Шанхай", "Shanghai", "CN"),
    ("CAN", "Гуанчжоу", "Guangzhou", "CN"),
    ("HKG", "Гонконг", "Hong Kong", "HK"),
    ("TYO", "Токио", "Tokyo", "JP"),
    ("OSA", "Осака", "Osaka", "JP"),
    ("BKK", "Бангкок", "Bangkok", "TH"),
    ("SGN", "Хошимин", "Ho Chi Minh City", "VN"),
    ("DXB", "Дубай", "Dubai", "AE"),
    ("AUH", "Абу-Даби", "Abu Dhabi", "AE"),
    ("IST", "Стамбул", "Istanbul", "TR"),
    ("LON", "Лондон", "London", "GB"),
    ("PAR", "Париж", "Paris", "FR"),
    ("MIL", "Милан", "Milan", "IT"),
    ("ROM", "Рим", "Rome", "IT"),
    ("BER", "Берлин", "Berlin", "DE"),
    ("BCN", "Барселона", "Barcelona", "ES"),
    ("MAD", "Мадрид", "Madrid", "ES"),
    ("NYC", "Нью-Йорк", "New York", "US"),
    ("ALA", "Алматы", "Almaty", "KZ"),
    ("TAS", "Ташкент", "Tashkent", "UZ"),
    ("DEL", "Дели", "Delhi", "IN"),
]
_CITY_BY_CODE = {c[0]: c for c in CITY_CODES}


@lru_cache(maxsize=4)
def _network(path: str = DEFAULT_NETWORK_PATH) -> Dict[str, Dict[str, Any]]:
    return agg.load_airport_network(path)


def _city_option(entry) -> Dict[str, str]:
    code, ru, _en, country = entry
    return {
        "code": code, "city": ru, "country": country,
        "flag": flag_emoji(country), "label": f"{ru} ({code})",
    }


def _airport_option(code: str, entry: Dict[str, Any]) -> Dict[str, str]:
    city = entry.get("municipality") or entry.get("name") or code
    country = entry.get("country") or ""
    return {
        "code": code, "city": city, "country": country,
        "flag": flag_emoji(country), "label": f"{city} ({code})",
    }


def get_airport(code: str, path: str = DEFAULT_NETWORK_PATH) -> Optional[Dict[str, str]]:
    code = code.upper()
    if code in _CITY_BY_CODE:  # города-агломерации приоритетнее
        return _city_option(_CITY_BY_CODE[code])
    entry = _network(path).get(code)
    return _airport_option(code, entry) if entry else None


def search_airports(q: str, limit: int = 10, path: str = DEFAULT_NETWORK_PATH) -> List[Dict[str, str]]:
    """Автокомплит: сначала города-агломерации (по коду, RU- и EN-названию),
    затем физические аэропорты из сети (по коду и названию)."""
    q = (q or "").strip()
    if not q:
        return []
    qu, ql = q.upper(), q.lower()

    results: List[Dict[str, str]] = []
    used = set()
    for entry in CITY_CODES:
        code, ru, en, _country = entry
        if code == qu or code.startswith(qu) or ql in ru.lower() or ql in en.lower():
            results.append(_city_option(entry))
            used.add(code)

    net = _network(path)
    exact, prefix, contains = [], [], []
    for code, entry in net.items():
        if code in used:
            continue
        muni = (entry.get("municipality") or "").lower()
        name = (entry.get("name") or "").lower()
        if code == qu:
            exact.append((code, entry))
        elif code.startswith(qu):
            prefix.append((code, entry))
        elif ql in muni or ql in name:
            contains.append((code, entry))
    for code, entry in exact + prefix + contains:
        results.append(_airport_option(code, entry))

    return results[:limit]
