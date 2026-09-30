"""Сегмент маршрута из нормализованного билета (core/graphql_api) — общий
контракт карточки на фронте (frontend/src/types.ts Segment): концы (город и
аэропорт), времена, длительность, пересадки с ожиданием, багаж, hidden-city,
ссылка на покупку. Плюс справочник имён городов (make_city_lookup) и мелкая
дата-арифметика поверх core/dates."""
import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Optional

from core.dates import calculate_arrival, calculate_stay_duration, parse_datetime


def flag_emoji(iso2: str) -> str:
    if not iso2 or len(iso2) != 2 or not iso2.isalpha():
        return ""
    return "".join(chr(0x1F1E6 + ord(c.upper()) - ord("A")) for c in iso2)


_CITY_NAMES_PATH = Path(__file__).resolve().parent / "city_names.json"


@lru_cache(maxsize=1)
def _city_names_network() -> dict:
    """core/city_names.json ({код: [имя, ISO2]}, scripts/build_city_names.py) в форме
    сети аэропортов: имя и страна — всё, что нужно выдаче (~230 КБ вместо 20 МБ).
    Справочник статичный — читается один раз на процесс (API грузит его при старте,
    load_city_names), как сеть аэропортов автокомплита и core/geo.json."""
    try:
        names = json.loads(_CITY_NAMES_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return {code: {"municipality": name, "country": country} for code, (name, country) in names.items()}


def load_city_names() -> None:
    """Загрузить справочник имён городов заранее (старт приложения)."""
    _city_names_network()


def make_city_lookup(network: Optional[dict] = None):
    """Имя/страна/флаг города по коду. network — сеть аэропортов (тесты дают {});
    без неё — компактный core/city_names.json."""
    if network is None:
        network = _city_names_network()
    # Метро-коды агломераций (BJS, LON, TYO…) физических аэропортов в сети не имеют,
    # поэтому их имена берём из курируемого справочника — иначе в выдаче остаётся
    # голый код («BJS» вместо «Beijing»). Импорт ленивый: рвём цикл airports↔trip_builder.
    from core.airports import _CITY_BY_CODE

    def city_info(code: str) -> dict:
        info = network.get(code, {})
        name = info.get("municipality") or info.get("name")
        # build_airport_network кладёт ISO2 страны в "country" (iso_country — поле
        # исходного CSV); раньше читали только его, и у городов из сети не было флага.
        country = info.get("country") or info.get("iso_country") or ""
        if not name:
            entry = _CITY_BY_CODE.get(code)
            if entry:
                name, country = entry[2], entry[3]  # английское имя, ISO2 страны
        if not name:  # справочник Travelpayouts (core/geo.json): соседние города и пр.
            from core.nearby import city_entry
            geo = city_entry(code) if code else None
            if geo:
                name, country = geo[3], country or geo[2]
        return {"city": name or code, "country": country, "flag": flag_emoji(country)}
    return city_info


def ddmm(iso_dt: str) -> str:
    d = parse_datetime(iso_dt)
    return f"{d.day:02d}{d.month:02d}"


def booking_link(origin: str, dest: str, depart_at: str) -> str:
    return f"https://www.aviasales.ru/search/{origin}{ddmm(depart_at)}{dest}1"


def arrival_of(flight: dict) -> str:
    if flight.get("arrival_at"):
        return flight["arrival_at"]
    if flight.get("departure_at") and flight.get("duration"):
        return calculate_arrival(flight["departure_at"], flight["duration"])
    return flight.get("departure_at")


def layover_minutes(flight: dict, transfers: int):
    """Суммарное время на земле на пересадках, мин. Data API отдаёт duration (весь
    путь) и duration_to (только в воздухе) — разница и есть ожидание; по отдельным
    пересадкам источник не разбивает. None — прямой рейс или данных нет."""
    total, air = flight.get("duration"), flight.get("duration_to")
    if not transfers or not total or not air or total <= air:
        return None
    return total - air


def date_only(iso_dt: str) -> str:
    return parse_datetime(iso_dt).strftime("%Y-%m-%d")


def stay_between(arrive_iso: str, depart_iso: str) -> int:
    return calculate_stay_duration(arrive_iso, depart_iso)


# --------------------------- построение сегментов ----------------------------

class Builder:
    def __init__(self, cfg, city_info):
        self.cfg = cfg          # не используется (наследие round-trip), оставлен для вызовов Builder(None, city_info)
        self.city_info = city_info

    def make_segment(self, flight: dict) -> dict:
        origin = flight.get("origin") or flight.get("search_origin")
        dest = flight.get("destination") or flight.get("search_destination")
        dep = flight.get("departure_at")
        arr = arrival_of(flight)
        transfers = int(flight.get("transfers") or 0)
        segment = {
            "origin": origin,
            "destination": dest,
            "origin_airport": flight.get("origin_airport") or origin,
            "destination_airport": flight.get("destination_airport") or dest,
            "origin_city": self.city_info(origin)["city"],
            "destination_city": self.city_info(dest)["city"],
            "departure_at": dep,
            "arrival_at": arr,
            "duration": flight.get("duration"),
            "transfers": transfers,
            "direct": transfers == 0,
            "transfer_points": self._points_from_flight(flight)
            if flight.get("transfer_points") is not None
            else self._transfer_points(flight.get("link"), transfers),
            "layover_minutes": layover_minutes(flight, transfers),
            "airline": flight.get("airline"),
            "flight_number": flight.get("flight_number"),
            "price": flight.get("price") or flight.get("value", 0),
            "link": booking_link(origin, dest, dep) if dep else None,
        }
        if flight.get("baggage") is not None:      # GraphQL: багаж явным полем
            segment["baggage"] = flight["baggage"]
        hidden = flight.get("hidden_city")
        if hidden:
            # Виртуальный рейс hidden-city (core.planner.hidden_city_flights): покупать
            # надо реальный билет A→C, поэтому ссылка — на него, а не на поиск A→B.
            from core.linkinfo import booking_url
            segment["hidden_city"] = {
                **hidden, "final_city": self.city_info(hidden.get("final") or "")["city"]}
            segment["link"] = booking_url(flight.get("link")) or segment["link"]
        return segment

    def _points_from_flight(self, flight: dict) -> list:
        """Пересадки нормализованного билета GraphQL (core/graphql_api): код, город,
        минуты ожидания, ночная, виза. Пустой список у прямого рейса."""
        return [{
            "code": p.get("code"), "city": self.city_info(p.get("code") or "")["city"],
            "minutes": p.get("minutes"), "night": bool(p.get("night")),
            "visa": bool(p.get("visa")), "country": p.get("country"),
        } for p in flight.get("transfer_points") or []]

    def _transfer_points(self, link: str, transfers: int) -> list:
        """Города пересадок внутри билета — парсятся из токена ссылки Aviasales
        (цепочка аэропортов вида SVO+TJM+NYA). Возвращает промежуточные точки."""
        if not link or not transfers:
            return []
        m = re.search(r'[?&]t=([^&]+)', link)
        if not m:
            return []
        m2 = re.search(r'([A-Z]{6,})_', m.group(1))
        if not m2:
            return []
        chain = m2.group(1)
        codes = [chain[i:i + 3] for i in range(0, len(chain), 3)]
        if len(codes) != transfers + 2:   # цепочка не бьётся с числом пересадок
            return []
        return [{"code": c, "city": self.city_info(c)["city"]} for c in codes[1:-1]]
