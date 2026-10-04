"""Клиент GraphQL Data API Travelpayouts: `prices_one_way`.

В отличие от REST `prices_for_dates` (один самый дешёвый билет на направление в
день, см. core/collector.py) GraphQL отдаёт **все** билеты на дату: прямые и с
пересадками вместе, с сегментами (аэропорты и времена каждого перелёта), длительностью
каждой пересадки, багажом явным полем. Дизайн — docs/PLANNER_V2.md.

Проверено живыми запросами 27.09.2026:
- `POST https://api.travelpayouts.com/graphql/v1/query`, заголовок `X-Access-Token`
  (тот же TRAVELPAYOUTS_TOKEN). 60 запросов в минуту, ~0.6–1.5 с на страницу.
- `params: ParamsOneWay`: origin/destination — ровно одно значение (список —
  ошибка), `*_type` AIRPORT/CITY/COUNTRY. Без destination — «город → ANY», без
  origin — «ANY → город»; без обоих — ошибка.
- `paging.limit` ≤ 400; `offset + limit` ≤ 15 000 (иначе «too high paging depth»).
- Сортировка VALUE_ASC: без ценового коридора первые страницы ANY-запроса —
  дешёвая ближняя Россия/СНГ; `value_min`/`value_max` режут объём в разы.
- `trip_duration` приходит 0 — длительность считаем по `flight_legs`.

Нормализованный билет (`normalize_ticket`) — надмножество «сырого рейса» REST
(`origin`, `destination`, `origin_airport`, `departure_at`, `duration`, `transfers`,
`price`, `link`, …), поэтому его понимают `trip_builder.make_segment`,
`hot.flights_to_quotes` и планировщик. Новые поля: `arrival_at` (реальный),
`duration_to` (в воздухе), `chain`, `legs`, `transfer_points` с длительностью,
`baggage`, `source="graphql"`.
"""
import json
import os
import re
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

GRAPHQL_URL = "https://api.travelpayouts.com/graphql/v1/query"
PAGE_LIMIT = 400              # больше источник не отдаёт
MAX_PAGES = 100               # предохранитель на серию (40 000 билетов); за потолком offset коллектор продолжает по цене
MAX_OFFSET = 14_600           # offset последней страницы: источник требует offset + limit ≤ 15 000
                              # («too high paging depth»; проверено 04.10.2026), при limit 400 — 37 страниц
MIN_INTERVAL_SECONDS = 1.0    # 60 запросов/мин
REQUEST_TIMEOUT = 30
SOURCE = "graphql"

QUERY = """
query Tickets($p: ParamsOneWay!, $limit: Int!, $offset: Int!) {
  prices_one_way(params: $p, paging: {limit: $limit, offset: $offset},
                 grouping: NONE, sorting: VALUE_ASC, currency: "rub") {
    value currency main_airline baggage_code with_baggage number_of_changes
    departure_at origin_city_iata destination_city_iata ticket_link
    segments {
      transfers { at to country_code duration_seconds visa_required night_transfer }
      flight_legs { origin destination departure_at arrival_at flight_number operating_carrier }
    }
  }
}
"""

_BAGGAGE_CODE_RE = re.compile(r"^(\d+)PC(\d+)?$")


class GraphQLError(RuntimeError):
    """Источник вернул `errors` (невалидные параметры, глубина пагинации и т.п.)."""


class RateLimited(Exception):
    """HTTP 429: превышен лимит ручки. retry_after — секунды из заголовка, если был."""

    def __init__(self, retry_after: Optional[float] = None):
        super().__init__(f"429 rate limited (retry after {retry_after})")
        self.retry_after = retry_after


def require_token() -> str:
    token = os.getenv("TRAVELPAYOUTS_TOKEN")
    if not token:
        raise RuntimeError("TRAVELPAYOUTS_TOKEN не найден в .env файле")
    return token


# ------------------------------ параметры -----------------------------------

def build_params(origin: Optional[str], destination: Optional[str], day: str, *,
                 day_to: Optional[str] = None, value_min: Optional[int] = None,
                 value_max: Optional[int] = None, direct: Optional[bool] = None,
                 with_baggage: Optional[bool] = None, origin_type: Optional[str] = None,
                 destination_type: Optional[str] = None) -> Dict[str, Any]:
    """`ParamsOneWay` для запроса. Пустой конец (None) — «ANY», хотя бы один конец
    обязателен. Коридор цен — `value_min`/`value_max`, целые рубли.

    Тип места по умолчанию НЕ передаём: источник сам распознаёт и город (MOW, SEL),
    и аэропорт (PEK, ICN), а явный тип строгий — `ICN` с CITY даёт «city ICN not
    found», `SEL` с AIRPORT — «airport SEL not found». Остановка у пользователя
    может быть задана любым кодом, поэтому автоопределение и нужно."""
    if not origin and not destination:
        raise ValueError("нужен хотя бы один из origin/destination")
    p: Dict[str, Any] = {"depart_date_min": day, "depart_date_max": day_to or day}
    if origin:
        p["origin"] = origin.upper()
        if origin_type:
            p["origin_type"] = origin_type
    if destination:
        p["destination"] = destination.upper()
        if destination_type:
            p["destination_type"] = destination_type
    if value_min is not None:
        p["value_min"] = int(value_min)
    if value_max is not None:
        p["value_max"] = int(value_max)
    if direct is not None:
        p["direct"] = bool(direct)
    if with_baggage is not None:
        p["with_baggage"] = bool(with_baggage)
    return p


def params_key(value_min: Optional[int] = None, value_max: Optional[int] = None,
               direct: Optional[bool] = None, with_baggage: Optional[bool] = None) -> str:
    """Канонический ключ «прочих» параметров серии — для кэша (storage.hot.ticket_cache).
    Направление и день — отдельные колонки ключа."""
    parts = []
    if value_min is not None:
        parts.append(f"min={int(value_min)}")
    if value_max is not None:
        parts.append(f"max={int(value_max)}")
    if direct is not None:
        parts.append(f"direct={int(bool(direct))}")
    if with_baggage is not None:
        parts.append(f"bag={int(bool(with_baggage))}")
    return "&".join(parts)


# ------------------------------- запросы ------------------------------------

_last_request_at = [0.0]


def _throttle() -> None:
    """Держим ≥ MIN_INTERVAL_SECONDS между реальными обращениями (60/мин)."""
    wait = _last_request_at[0] + MIN_INTERVAL_SECONDS - time.monotonic()
    if wait > 0:
        time.sleep(wait)
    _last_request_at[0] = time.monotonic()


def query_page(params: Dict[str, Any], offset: int = 0, limit: int = PAGE_LIMIT,
               session: Optional[requests.Session] = None, throttle: bool = True) -> List[Dict[str, Any]]:
    """Одна страница сырых билетов. Ошибки источника — GraphQLError, 429 — RateLimited,
    сетевые — requests.RequestException (решает вызывающий: серия помечается error=True).
    throttle=False — темп держит вызывающий (коллектор со своим лимитером)."""
    if throttle:
        _throttle()
    body = {"query": QUERY, "variables": {"p": params, "limit": limit, "offset": offset}}
    headers = {"X-Access-Token": require_token(), "Content-Type": "application/json"}
    http = session or requests
    resp = http.post(GRAPHQL_URL, json=body, headers=headers, timeout=REQUEST_TIMEOUT)
    if resp.status_code == 429:
        retry_after = resp.headers.get("Retry-After")
        try:
            retry_after = float(retry_after) if retry_after else None
        except ValueError:
            retry_after = None
        raise RateLimited(retry_after)
    # Ошибки валидации («city ICN not found») приходят как HTTP 400 с JSON `errors` —
    # разбираем тело раньше raise_for_status, чтобы не потерять сообщение.
    try:
        payload = resp.json()
    except ValueError:
        payload = None
    if isinstance(payload, dict) and payload.get("errors"):
        raise GraphQLError(json.dumps(payload["errors"], ensure_ascii=False)[:500])
    resp.raise_for_status()
    if not isinstance(payload, dict):
        raise GraphQLError("пустой или не-JSON ответ")
    return (payload.get("data") or {}).get("prices_one_way") or []


def fetch_series(origin: Optional[str], destination: Optional[str], day: str, *,
                 value_min: Optional[int] = None, value_max: Optional[int] = None,
                 direct: Optional[bool] = None, with_baggage: Optional[bool] = None,
                 max_pages: int = MAX_PAGES,
                 page_fn: Optional[Callable[..., List[Dict[str, Any]]]] = None,
                 progress_cb: Optional[Callable[[int, int], None]] = None) -> Dict[str, Any]:
    """Серия: все страницы одного под-запроса (направление × день × коридор).

    Возвращает {"tickets": [нормализованные], "pages": n, "exhausted": bool,
    "error": bool}. `exhausted=False` — упёрлись в max_pages/MAX_OFFSET, билеты
    дороже последнего не получены. При сетевой ошибке или ошибке источника на
    любой странице — `error=True` и то, что успели собрать (в кэш не класть).
    page_fn(params, offset, limit) — подмена запроса в тестах.
    progress_cb(page_index, tickets_so_far) — после каждой страницы."""
    params = build_params(origin, destination, day, value_min=value_min,
                          value_max=value_max, direct=direct, with_baggage=with_baggage)
    get_page = page_fn or query_page
    tickets: List[Dict[str, Any]] = []
    pages, exhausted, error = 0, False, False
    while pages < max_pages:
        offset = pages * PAGE_LIMIT
        if offset > MAX_OFFSET:
            break
        try:
            raw = get_page(params, offset, PAGE_LIMIT)
        except (GraphQLError, requests.RequestException, ValueError) as e:
            print(f"[graphql] {origin or 'ANY'}→{destination or 'ANY'} {day} "
                  f"стр. {pages + 1}: {e}")
            error = True
            break
        pages += 1
        for t in raw:
            f = normalize_ticket(t, origin, destination, day)
            if f is not None:
                tickets.append(f)
        if progress_cb:
            progress_cb(pages, len(tickets))
        if len(raw) < PAGE_LIMIT:
            exhausted = True
            break
    return {"tickets": tickets, "pages": pages, "exhausted": exhausted, "error": error}


# ----------------------------- нормализация ---------------------------------

def parse_baggage_code(code: Optional[str], with_baggage: Optional[bool] = None) -> Dict[str, Any]:
    """`1PC23` → {known, included=True, pieces=1, kg=23}; `0PC` → без багажа;
    `1PC` → включён, вес неизвестен; пусто → по `with_baggage`, если он есть."""
    m = _BAGGAGE_CODE_RE.match((code or "").strip().upper())
    if m:
        pieces = int(m.group(1))
        return {"known": True, "included": pieces > 0,
                "pieces": pieces if pieces > 0 else None,
                "kg": int(m.group(2)) if m.group(2) else None}
    if with_baggage is not None:
        return {"known": True, "included": bool(with_baggage), "pieces": None, "kg": None}
    return {"known": False, "included": False, "pieces": None, "kg": None}


def _minutes_between(start: Optional[str], end: Optional[str]) -> Optional[int]:
    """Разница двух ISO-времён с таймзонами в минутах (пояса разные — учитываем)."""
    if not start or not end:
        return None
    try:
        a, b = datetime.fromisoformat(start), datetime.fromisoformat(end)
    except ValueError:
        return None
    if (a.tzinfo is None) != (b.tzinfo is None):
        return None
    return int(round((b - a).total_seconds() / 60))


def normalize_ticket(raw: Dict[str, Any], search_origin: Optional[str],
                     search_destination: Optional[str], search_date: str) -> Optional[Dict[str, Any]]:
    """Сырой билет GraphQL → «сырой рейс» в формате REST + новые поля.

    None — если у билета нет сегментов (таких не видели, но защищаемся)."""
    segments = raw.get("segments") or []
    legs_raw = segments[0].get("flight_legs") if segments else None
    if not legs_raw:
        return None
    transfers_raw = segments[0].get("transfers") or []
    legs = [{
        "origin": l.get("origin"), "destination": l.get("destination"),
        "departure_at": l.get("departure_at"), "arrival_at": l.get("arrival_at"),
        "flight_number": l.get("flight_number"), "carrier": l.get("operating_carrier"),
    } for l in legs_raw]
    # Цепочка аэропортов как в `t=` ссылки: при смене аэропорта на пересадке
    # (прилёт IST, вылет SAW) в цепочке оба — VKO→IST→SAW→BGW.
    chain = [legs[0]["origin"]]
    for l in legs:
        if l["origin"] != chain[-1]:
            chain.append(l["origin"])
        chain.append(l["destination"])
    departure_at = raw.get("departure_at") or legs[0]["departure_at"]
    arrival_at = legs[-1]["arrival_at"]
    duration = _minutes_between(departure_at, arrival_at)
    air = [_minutes_between(l["departure_at"], l["arrival_at"]) for l in legs]
    duration_to = sum(air) if all(m is not None for m in air) else None
    transfer_points = [{
        "code": t.get("at"), "to": t.get("to") or t.get("at"),
        "country": t.get("country_code"),
        "minutes": (int(t["duration_seconds"]) // 60) if t.get("duration_seconds") is not None else None,
        "night": bool(t.get("night_transfer")), "visa": bool(t.get("visa_required")),
    } for t in transfers_raw]
    transfers = raw.get("number_of_changes")
    if transfers is None:
        transfers = len(transfers_raw) or (len(legs) - 1)
    price = raw.get("value")
    link = raw.get("ticket_link") or ""
    if link and not link.startswith("/search"):
        link = "/search" + link          # как в REST: /search/MOW1510SEL1?t=…
    return {
        "origin": raw.get("origin_city_iata") or search_origin,
        "destination": raw.get("destination_city_iata") or search_destination,
        "origin_airport": chain[0],
        "destination_airport": chain[-1],
        "departure_at": departure_at,
        "arrival_at": arrival_at,
        "duration": duration,
        "duration_to": duration_to,
        "transfers": int(transfers),
        "airline": raw.get("main_airline"),
        "flight_number": legs[0]["flight_number"],
        "price": price,
        "currency": raw.get("currency") or "rub",
        "link": link,
        "chain": chain,
        "legs": legs,
        "transfer_points": transfer_points,
        "baggage": parse_baggage_code(raw.get("baggage_code"), raw.get("with_baggage")),
        "baggage_code": raw.get("baggage_code"),
        "source": SOURCE,
        "search_origin": search_origin,
        "search_destination": search_destination,
        "search_date": search_date,
    }


def flight_key(f: Dict[str, Any]) -> tuple:
    """Ключ дедупликации билета между сериями (тот же рейс может прийти из A→B и
    A→ANY): цепочка аэропортов + вылет + номера рейсов + цена."""
    return (tuple(f.get("chain") or ()), f.get("departure_at"),
            tuple(l.get("flight_number") for l in f.get("legs") or ()), f.get("price"))
