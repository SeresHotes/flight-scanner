"""Движок планировщика цепочек A → B → C → …

В отличие от round-trip (core/trip_builder), маршрут — линейная последовательность
остановок. Каждая остановка — НАБОР городов-кандидатов (kind='cities') или «любой»
(kind='any', wildcard в середине). Окно дат «когда ОК быть здесь» задаётся только у
ПРОМЕЖУТОЧНЫХ остановок; у концов оно выводится из соседей.

Сбор экономный: на каждый переход «якорим» сторону с меньшим числом городов и
запрашиваем через неё «все направления» (origin=X,dest=None или origin=None,dest=Y)
— один запрос в день на якорный город, — а другую сторону фильтруем по выбранным
городам. Поэтому конкретные города не дороже «любого». Всё это должно совпадать
с клиентской оценкой (frontend/src/planner/estimate.ts).

Публичный контракт (frontend/src/planner/types.ts):
- estimate_plan(stops)      -> {requests, seconds, legs:[{fromLabel,toLabel,days,requests,anyLeg}]}
- collect_plan(stops, cb)   -> {leg_index: [сырые рейсы]}   (реальные запросы к API)
- build_itineraries(stops, collected) -> [Itinerary]        (чистая сборка)
- build_itineraries_compact(stops, collected, max_results) -> компактный результат
  (таблица уникальных сегментов + плоские массивы индексов; см. _pack_compact)
"""
import heapq
import io
import json
from array import array
from functools import lru_cache
from typing import Any, Callable, Dict, List, Optional

from core import aggregate as agg
from core.collector import collect_leg_data, get_date_range
from core.linkinfo import parse_link
from core.trip_builder import Builder, arrival_of, date_only, make_city_lookup, stay_between

SECONDS_PER_REQUEST = 0.65  # совпадает с planner/estimate.ts
MAX_REQUESTS = 400          # предохранитель от слишком широких окон (2 запроса на день, см. FETCH_MODES)
MAX_RESULTS = 1_000_000     # потолок max_results: компактный перебор держит его в памяти VM (4 ГБ)
DEFAULT_START = "2026-11-01"  # якорь старта, если окон нет нигде (совпадает с mock)
DEFAULT_LEG_DAYS = 7          # ширина окна плеча, если оба конца без окна
FINAL_STAY_DAYS = 5           # пребывание в финальном городе (у конца окна нет)

# Режимы запроса на каждый день (значения allow_indirect). prices_for_dates с
# direct=false отдаёт по направлению лишь САМЫЙ ДЕШЁВЫЙ билет на дату — обычно
# стыковочный, и прямой рейс чуть дороже в ответ не попадает (а потом фильтр
# «только прямые» выкидывает и стыковочный). Поэтому прямые запрашиваем отдельно.
FETCH_MODES = (True, False)

# Hidden-city в плече A→B: билет A→C с пересадкой в B добавляется в список плеча
# как виртуальный рейс A→B (цена всего билета, выходим в B). При якорении по A
# такие билеты уже есть в ответе A→ANY; при якорении по B нужен доп. запрос
# A→ANY (только с пересадками) на каждый день и город A — см. _hidden_series.
HIDDEN_CITY_SPEED_KMH = 750     # оценка времени сегмента A→B по расстоянию, если
HIDDEN_CITY_GROUND_MIN = 40     # прямого A→B в выборке нет (руление/набор высоты)


def is_valid_max_results(n: Optional[int]) -> bool:
    """max_results ОБЯЗАТЕЛЕН и должен быть положительным. Прод — burstable VM (4 ГБ):
    безлимитный перебор (max_results=None) на плотном графе с двумя «any» строит сотни
    тысяч Itinerary → сотни МБ JSON → вешает браузер и почти достаёт до OOM. None
    приходит от устаревшего закешированного фронта или прямого вызова API — такие
    запросы отклоняем (не зажимаем потолком), чтобы движок физически не уходил в
    безлимит."""
    return n is not None and 1 <= n <= MAX_RESULTS

# Тестовый режим: собираем ВСЕ цепочки из имеющихся данных без потолка
# (кэпы beam/per-city/per-sequence тоже сняты). Онворды раскрываются по
# возрастанию цены, показ на фронте — страницами (дефолт 100). ВНИМАНИЕ: на
# плотном графе с несколькими «any»-остановками число цепочек может быть
# огромным и подвесить воркер/память — предел сознательно убран под ручную
# отладку. Если начнёт зависать — вернуть потолок здесь.
_INF = float("inf")


# --------------------------------- модель ------------------------------------

class Stop:
    """Остановка запроса: набор городов (kind='cities') или «любой» + окно дат."""

    def __init__(self, kind: str, codes: List[str], window: List[str]):
        self.kind = kind                                    # 'cities' | 'any'
        self.codes = [c.upper() for c in codes if c]        # пусто для 'any'
        self.window = [window[0] if window else "", window[1] if window and len(window) > 1 else ""]

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Stop":
        # Принимаем и codes:[...], и airports:[{code}] (как во фронтовом PlannerStop).
        codes = d.get("codes")
        if codes is None:
            codes = [a.get("code") for a in (d.get("airports") or [])]
        return Stop(d.get("kind", "cities"), codes or [], d.get("window") or ["", ""])

    def cardinality(self) -> float:
        """Число городов; «любой» = бесконечность (сторону нельзя заякорить)."""
        return max(1, len(self.codes)) if self.kind == "cities" else _INF


def parse_stops(raw: List[Dict[str, Any]]) -> List[Stop]:
    return [Stop.from_dict(d) for d in raw]


# ----------------------------- окна и оценка ---------------------------------

def _leg_window(stops: List[Stop], i: int) -> List[str]:
    """Окно плеча i: заданное окно того конца, у кого оно есть (у концов окна нет)."""
    wi = stops[i].window
    if wi[0] and wi[1]:
        return wi
    wj = stops[i + 1].window
    if wj[0] and wj[1]:
        return wj
    return ["", ""]


def _leg_dates(stops: List[Stop], i: int) -> List[str]:
    """Конкретные даты сбора плеча i (с дефолтом, если окон нигде нет)."""
    win = _leg_window(stops, i)
    if win[0] and win[1]:
        return get_date_range(win[0], win[1])
    end = _shift(DEFAULT_START, DEFAULT_LEG_DAYS - 1)
    return get_date_range(DEFAULT_START, end)


def _shift(date_str: str, days: int) -> str:
    from datetime import datetime, timedelta
    return (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=days)).strftime("%Y-%m-%d")


def _stop_label(stop: Stop, city_info) -> str:
    if stop.kind == "any":
        return "Любой город"
    if not stop.codes:
        return "Город не выбран"
    if len(stop.codes) == 1:
        c = stop.codes[0]
        return f"{city_info(c)['city']} ({c})"
    return "/".join(stop.codes)


def _hidden_extra_cities(stops: List[Stop], i: int) -> int:
    """Сколько городов A требуют доп. запроса A→ANY под hidden-city: плечо заякорено
    по B (у A городов больше), оба конца — конкретные города."""
    from_stop, to_stop = stops[i], stops[i + 1]
    if from_stop.kind != "cities" or to_stop.kind != "cities":
        return 0
    if from_stop.cardinality() <= to_stop.cardinality():
        return 0  # якорь по A: ответ A→ANY уже содержит билеты через B
    return len(from_stop.codes)


def _leg_requests(stops: List[Stop], i: int) -> int:
    """Запросов на плечо i = (меньшая мощность конца) × дней окна × режимов запроса
    + доп. запросы A→ANY (один режим) под hidden-city при якорении по B."""
    anchor = min(stops[i].cardinality(), stops[i + 1].cardinality())
    win = _leg_window(stops, i)
    days = len(get_date_range(win[0], win[1])) if win[0] and win[1] else DEFAULT_LEG_DAYS
    return int(anchor) * days * len(FETCH_MODES) + _hidden_extra_cities(stops, i) * days


def estimate_plan(stops: List[Stop], city_info=None) -> Dict[str, Any]:
    """Оценка объёма сбора цепочки (совпадает с planner/estimate.ts)."""
    if city_info is None:
        city_info = make_city_lookup(agg.load_airport_network())
    legs = []
    requests = 0
    for i in range(len(stops) - 1):
        win = _leg_window(stops, i)
        days = len(get_date_range(win[0], win[1])) if win[0] and win[1] else DEFAULT_LEG_DAYS
        reqs = _leg_requests(stops, i)
        legs.append({
            "fromLabel": _stop_label(stops[i], city_info),
            "toLabel": _stop_label(stops[i + 1], city_info),
            "days": days,
            "requests": reqs,
            "anyLeg": stops[i].kind == "any" or stops[i + 1].kind == "any",
        })
        requests += reqs
    return {"requests": requests, "seconds": round(requests * SECONDS_PER_REQUEST), "legs": legs}


def request_count(stops: List[Stop]) -> int:
    return sum(_leg_requests(stops, i) for i in range(len(stops) - 1))


# --------------------------------- сбор --------------------------------------

def _leg_series(stops: List[Stop], i: int):
    """План под-запросов плеча i с якорением по стороне с меньшим числом городов.

    Каждый элемент: (origin|None, dest|None, dates, allow_side, allow_set) — где
    allow_side ∈ {'dest','origin'} говорит, какую сторону результата фильтровать по
    allow_set (None = не фильтровать, конец 'any')."""
    from_stop, to_stop = stops[i], stops[i + 1]
    dates = _leg_dates(stops, i)
    if from_stop.cardinality() <= to_stop.cardinality():
        # якорим по from: origin=город, dest=None (все направления), фильтр по to
        allow = set(to_stop.codes) if to_stop.kind == "cities" else None
        return [(city, None, dates, "dest", allow) for city in from_stop.codes]
    # якорим по to: origin=None (все направления), dest=город, фильтр по from
    allow = set(from_stop.codes) if from_stop.kind == "cities" else None
    return [(None, city, dates, "origin", allow) for city in to_stop.codes]


def _side_codes(flight: Dict[str, Any], side: str) -> set:
    """Коды рейса на стороне side ∈ {'dest','origin'}: код ГОРОДА и код АЭРОПОРТА.

    Travelpayouts отдаёт направления на уровне города (Сеул → SEL), а конкретный
    аэропорт кладёт отдельным полем (destination_airport → ICN). Остановку
    пользователь может задать любым из этих кодов, поэтому стыковку и фильтры
    матчим по обоим — иначе финальная точка «ICN» не совпадёт ни с одним рейсом,
    у которого destination='SEL', и цепочка не соберётся."""
    if side == "dest":
        city = flight.get("destination") or flight.get("search_destination")
        airport = flight.get("destination_airport")
    else:
        city = flight.get("origin") or flight.get("search_origin")
        airport = flight.get("origin_airport")
    return {c.upper() for c in (city, airport) if c}


def _keep(flight: Dict[str, Any], side: str, allow: Optional[set]) -> bool:
    if allow is None:
        return True
    return bool(_side_codes(flight, side) & allow)


def _flight_key(flight: Dict[str, Any]) -> tuple:
    """Идентичность билета: один и тот же прямой рейс приходит и в ответе с
    пересадками (если он самый дешёвый), и в ответе direct=true."""
    return tuple(flight.get(k) for k in (
        "origin_airport", "destination_airport", "departure_at",
        "airline", "flight_number", "transfers", "price"))


def _hidden_series(stops: List[Stop], i: int):
    """Доп. под-запросы A→ANY (с пересадками) под hidden-city при якорении по B."""
    if not _hidden_extra_cities(stops, i):
        return []
    dates = _leg_dates(stops, i)
    return [(city, None, dates) for city in stops[i].codes]


def collect_plan(stops: List[Stop], progress_cb: Callable[[], None] = None,
                 fetch_fn=None,
                 leg_cb: Callable[[int], None] = None,
                 network: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[int, List[Dict[str, Any]]]:
    """Реально ходит в Travelpayouts: собирает рейсы по каждому переходу.

    Возвращает {индекс_перехода: [сырые рейсы]}. На каждый день — два запроса
    (FETCH_MODES): с пересадками, чтобы фильтр по их числу на фронте имел смысл, и
    только прямые, которые иначе теряются за более дешёвым стыковочным билетом.
    progress_cb() — после каждого запроса,
    leg_cb(i) — перед началом перехода i (для показа этапа в UI).
    fetch_fn позволяет подменить обращение к API (кэширующая обёртка из api.worker).
    network — сеть аэропортов (координаты для оценки прилёта hidden-city; опционально).

    В список плеча A→B дописываются виртуальные рейсы hidden-city: билеты A→C с
    первой пересадкой в B, дешевле обычных A→B того же дня (см. hidden_city_flights)."""
    collected: Dict[int, List[Dict[str, Any]]] = {}
    for i in range(len(stops) - 1):
        if leg_cb:
            leg_cb(i)
        leg_flights: List[Dict[str, Any]] = []
        raw: List[Dict[str, Any]] = []   # все ответы плеча до фильтра — источник hidden-city
        seen = set()
        for origin, dest, dates, side, allow in _leg_series(stops, i):
            for allow_indirect in FETCH_MODES:
                flights = collect_leg_data(
                    origin, dest, dates, leg_name=f"leg{i}",
                    allow_indirect=allow_indirect, progress_cb=progress_cb, fetch_fn=fetch_fn,
                )
                raw += flights
                for f in flights:
                    key = _flight_key(f)
                    if key in seen or not _keep(f, side, allow):
                        continue
                    seen.add(key)
                    leg_flights.append(f)
        for origin, dest, dates in _hidden_series(stops, i):
            raw += collect_leg_data(origin, dest, dates, leg_name=f"leg{i}:hidden",
                                    allow_indirect=True, progress_cb=progress_cb, fetch_fn=fetch_fn)
        leg_flights += hidden_city_flights(raw, leg_flights, stops[i], stops[i + 1], network)
        collected[i] = leg_flights
    return collected


# --------------------------------- hidden-city --------------------------------

def _coords(network: Optional[Dict[str, Dict[str, Any]]], code: str):
    info = (network or {}).get(code) or {}
    raw = info.get("coordinates")
    if not raw:
        return None
    try:
        lat, lon = (float(x) for x in str(raw).split(","))
        return lat, lon
    except ValueError:
        return None


def _geo_minutes(network, a: str, b: str) -> Optional[int]:
    """Оценка времени перелёта a→b по дуге большого круга."""
    import math
    pa, pb = _coords(network, a), _coords(network, b)
    if not pa or not pb:
        return None
    lat1, lon1, lat2, lon2 = map(math.radians, (*pa, *pb))
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    km = 2 * 6371 * math.asin(math.sqrt(h))
    return int(km / HIDDEN_CITY_SPEED_KMH * 60) + HIDDEN_CITY_GROUND_MIN


def _segment_minutes(ticket: Dict[str, Any], hub: str, hub_city: str,
                     regular: List[Dict[str, Any]], network) -> int:
    """Длительность первого сегмента A→B билета: по прямому A→B из той же выборки
    (тот же аэропорт вылета и хаб, иначе любой прямой между этими городами), иначе
    по расстоянию, иначе — доля общей длительности по числу сегментов."""
    origin_airport = ticket.get("origin_airport")
    same_airports, same_cities = [], []
    for f in regular:
        if int(f.get("transfers") or 0) or not f.get("duration") or f.get("hidden_city"):
            continue
        if f.get("destination_airport") != hub and f.get("destination") != hub_city:
            continue
        (same_airports if f.get("origin_airport") == origin_airport else same_cities).append(f["duration"])
    for pool in (same_airports, same_cities):
        if pool:
            return min(pool)
    geo = _geo_minutes(network, origin_airport, hub)
    if geo is not None:
        return geo
    chain = parse_link(ticket.get("link")).airports
    return int((ticket.get("duration") or 0) / max(1, len(chain) - 1))


def hidden_city_flights(raw: List[Dict[str, Any]], regular: List[Dict[str, Any]],
                        from_stop: Stop, to_stop: Stop,
                        network=None) -> List[Dict[str, Any]]:
    """Виртуальные рейсы A→B из билетов A→C с ПЕРВОЙ пересадкой в B (hidden-city).

    Берём билеты из сырых ответов плеча, у которых в цепочке `t=` второй аэропорт —
    аэропорт B (B задан кодом города или аэропорта; аэропорты города узнаём из тех
    же ответов), а конечный пункт — не B. Оставляем только те, что дешевле самого
    дешёвого обычного рейса A→B того же дня вылета (иначе смысла нет). Виртуальный
    рейс: destination=B, transfers=0, duration — оценка первого сегмента, а
    hidden_city — что это за билет на самом деле (финал, цепочка, багаж)."""
    if from_stop.kind != "cities" or to_stop.kind != "cities":
        return []
    allow_to, allow_from = set(to_stop.codes), set(from_stop.codes)
    airport_city: Dict[str, str] = {}
    for f in raw + regular:
        for side, apt in (("destination", "destination_airport"), ("origin", "origin_airport")):
            if f.get(apt) and f.get(side):
                airport_city[f[apt].upper()] = f[side].upper()
    hubs = {apt for apt, city in airport_city.items() if city in allow_to} | allow_to

    best_regular: Dict[str, float] = {}
    for f in regular:
        if f.get("hidden_city"):
            continue
        day = date_only(f.get("departure_at") or "")
        price = _price_of(f)
        if day not in best_regular or price < best_regular[day]:
            best_regular[day] = price

    virtual: List[Dict[str, Any]] = []
    seen = set()
    for f in raw:
        info = parse_link(f.get("link"))
        chain = info.airports
        if len(chain) < 3 or chain[1] not in hubs:
            continue
        if _keep(f, "dest", allow_to) or not _keep(f, "origin", allow_from):
            continue
        day = date_only(f.get("departure_at") or "")
        price = _price_of(f)
        if day in best_regular and price >= best_regular[day]:
            continue
        hub = chain[1]
        hub_city = airport_city.get(hub) or (hub if hub in allow_to else next(iter(allow_to)))
        v = dict(f)
        v.update({
            "destination": hub_city,
            "destination_airport": hub,
            "transfers": 0,
            "duration": _segment_minutes(f, hub, hub_city, regular, network),
            "duration_to": None,
            "hidden_city": {
                "final": f.get("destination"),
                "final_airport": f.get("destination_airport"),
                "chain": chain,
                "full_duration": f.get("duration"),
                "full_transfers": int(f.get("transfers") or 0),
                "baggage": info.baggage.as_dict(),
                "arrival_estimated": True,
            },
        })
        key = _flight_key(v)
        if key in seen:
            continue
        seen.add(key)
        virtual.append(v)
    return virtual


# ------------------------------- сборка цепочек ------------------------------

def _has_both_weekend_days(arrive_iso: str, depart_iso: str) -> bool:
    """Оба выходных (сб И вс) попадают в пребывание [arrive..depart]."""
    from datetime import timedelta
    a = agg.parse_datetime(arrive_iso)
    b = agg.parse_datetime(depart_iso)
    if b < a:
        return False
    sat = sun = False
    cur = a
    while cur.date() <= b.date():
        wd = cur.weekday()  # 5 — сб, 6 — вс
        sat = sat or wd == 5
        sun = sun or wd == 6
        if sat and sun:
            return True
        cur += timedelta(days=1)
    return False


def _index_leg(flights: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Группирует рейсы плеча по коду вылета — и городу, и аэропорту (остановка
    может быть задана любым из них), чтобы стыковка находила рейс по любому коду."""
    by_origin: Dict[str, List[Dict[str, Any]]] = {}
    for f in flights:
        for code in _side_codes(f, "origin"):
            by_origin.setdefault(code, []).append(f)
    return by_origin


def _price_of(f: Dict[str, Any]) -> float:
    p = f.get("price")
    return p if p is not None else f.get("value", 0)


def _allowed(stop: Stop) -> Optional[set]:
    """Множество разрешённых городов остановки; None = «любой»."""
    return set(stop.codes) if stop.kind == "cities" else None


def _completion_lb(stops: List[Stop],
                   legs_by_origin: Dict[int, Dict[str, List[Dict[str, Any]]]]
                   ) -> List[Dict[str, float]]:
    """Нижняя оценка стоимости «хвоста» цепочки для отсечения по бюджету.

    lb[i][city] — минимально возможная суммарная цена, чтобы из `city` перед плечом i
    добраться до финальной остановки, учитывая ТОЛЬКО цены рейсов и разрешённые города
    остановок (БЕЗ ограничений на время вылета и на повторы городов). Это релаксация
    задачи, поэтому оценка никогда не завышает реальную стоимость — ветку, где
    накопленная_цена + lb > бюджета, можно резать, не теряя валидных цепочек
    (admissible-эвристика).

    Считается обратной динамикой по плечам (от последнего перехода к первому) — это
    и есть тот самый граф минимальных цен перелётов: город, из которого дешевле
    бюджета не собрать ни одной цепочки, отсекается целиком, не разворачивая поддерево."""
    last = len(stops) - 1
    lb: List[Dict[str, float]] = [dict() for _ in range(len(stops))]
    for i in range(last - 1, -1, -1):
        allow_next = _allowed(stops[i + 1])
        nxt = lb[i + 1]
        terminal_next = (i + 1 == last)
        cur = lb[i]
        for city, flights in legs_by_origin[i].items():
            best = _INF
            for f in flights:
                dest = (f.get("destination") or f.get("search_destination") or "").upper()
                if not dest or dest == city:
                    continue
                if allow_next is not None and not (_side_codes(f, "dest") & allow_next):
                    continue
                tail = 0.0 if terminal_next else nxt.get(dest)
                if tail is None:  # из dest конца не достичь — этот рейс не ведёт к цели
                    continue
                cand = _price_of(f) + tail
                if cand < best:
                    best = cand
            if best < _INF:
                cur[city] = best
    return lb


def _onward_candidates(stops: List[Stop],
                       legs_by_origin: Dict[int, Dict[str, List[Dict[str, Any]]]],
                       i: int, city: str, arrive_iso: str, visited: set
                       ) -> List[Any]:
    """Валидные онворд-рейсы из `city` на плече i: вылет не раньше прилёта, посадка в
    разрешённом для следующей остановки городе, без петель/повторов. Возвращает пары
    (рейс, город_прилёта).

    Запрет повторов — только для «любой»-остановок: явно заданный город пользователь
    выбрал сам, и повтор там осмыслен (кольцо MOW → … → MOW). Иначе финал, совпадающий
    со стартом, не собирался никогда, а _completion_lb (не знает про visited) держал
    такие ветки живыми — A* перебирал бесконечно, не находя ни одной цепочки."""
    arrive_day = date_only(arrive_iso)
    allow_next = _allowed(stops[i + 1])
    out = []
    for f in legs_by_origin[i].get(city, []):
        dep = f.get("departure_at")
        if not dep or date_only(dep) < arrive_day:  # нельзя вылететь раньше прилёта
            continue
        dest = (f.get("destination") or f.get("search_destination") or "").upper()
        if not dest or dest == city:  # без петель
            continue
        if allow_next is None and dest in visited:  # «любой» не разрешаем в уже посещённый
            continue
        if allow_next is not None and not (_side_codes(f, "dest") & allow_next):
            continue
        out.append((f, dest))
    return out


class SearchAborted(Exception):
    """Перебор прерван извне (should_stop() вернул True) — джобу сбросили как зависшую."""


def build_itineraries(stops: List[Stop], collected: Dict[int, List[Dict[str, Any]]],
                      city_info=None, max_results: Optional[int] = None,
                      max_cost: Optional[float] = None,
                      should_stop: Optional[Callable[[], bool]] = None,
                      on_progress: Optional[Callable[[int, int], None]] = None) -> List[Dict[str, Any]]:
    """Собирает цепочки из собранных плеч. Чистая функция (без I/O).

    max_results — движковый потолок: сколько САМЫХ ДЕШЁВЫХ цепочек вернуть. Это НЕ
    обрезка готового списка, а граница самого перебора — best-first (A*) извлекает
    цепочки по возрастанию цены и ОСТАНАВЛИВАЕТСЯ, набрав N (иначе на плотном графе
    с несколькими «any»-остановками цепочек экспоненциально много и воркер виснет).
    max_cost — верхняя граница суммарной цены: ветка режется по admissible-оценке
    минимального «хвоста» (_completion_lb), не разворачивая бесперспективные
    направления. max_results=None — прежний режим «все цепочки без потолка».
    should_stop() проверяется на каждом шаге перебора: True → SearchAborted (так
    воркер останавливает зависшую стыковку — поток Python снаружи не убить).
    on_progress(found, explored) — тоже на каждом шаге: сколько цепочек уже готово
    и сколько вариантов перебрано (для прогресса в UI; частоту записи режет вызывающий)."""
    ctx = _build_ctx(stops, collected, city_info)
    check = _step_check(should_stop, on_progress)

    if max_results is None:
        return _enumerate_all(ctx, max_cost, check)
    table = _FlightTable(ctx[1])
    stops, _, builder, city_info, chain_start, _ = ctx
    chains = _search_cheapest(ctx, table, max_results, max_cost, check)
    return [_assemble(stops, [table.flights[fi] for fi in chain], builder, city_info, chain_start, n + 1)
            for n, chain in enumerate(chains)]


def build_itineraries_compact(stops: List[Stop], collected: Dict[int, List[Dict[str, Any]]],
                              max_results: int, city_info=None,
                              max_cost: Optional[float] = None,
                              should_stop: Optional[Callable[[], bool]] = None,
                              on_progress: Optional[Callable[[int, int], None]] = None) -> Dict[str, Any]:
    """То же, что build_itineraries с потолком, но результат компактный (_pack_compact).

    Рассчитан на сотни тысяч – миллион цепочек на VM с 4 ГБ: цепочка в памяти — это
    несколько int-индексов рейсов в плоском array, а не словарь с копиями сегментов
    (100k словарей Itinerary + их JSON стоили гигабайты и роняли api по OOM)."""
    ctx = _build_ctx(stops, collected, city_info)
    check = _step_check(should_stop, on_progress)
    table = _FlightTable(ctx[1])
    chains = array("i")
    for chain in _search_cheapest(ctx, table, max_results, max_cost, check):
        chains.extend(chain)
    return _pack_compact(ctx, table, chains, len(stops) - 1)


def _build_ctx(stops: List[Stop], collected: Dict[int, List[Dict[str, Any]]], city_info):
    if city_info is None:
        city_info = make_city_lookup(agg.load_airport_network())
    builder = Builder(None, city_info)  # make_segment использует только city_info
    legs_by_origin = {i: _index_leg(collected.get(i, [])) for i in range(len(stops) - 1)}
    chain_start = _leg_dates(stops, 0)[0]  # первая дата окна нулевого плеча
    last = len(stops) - 1
    return (stops, legs_by_origin, builder, city_info, chain_start, last)


def _step_check(should_stop: Optional[Callable[[], bool]],
                on_progress: Optional[Callable[[int, int], None]]) -> Callable[[int], None]:
    """Хук шага перебора: check(found) считает шаги, сообщает прогресс и прерывает
    перебор, если джобу сбросили."""
    explored = 0

    def check(found: int) -> None:
        nonlocal explored
        explored += 1
        if should_stop is not None and should_stop():
            raise SearchAborted()
        if on_progress is not None:
            on_progress(found, explored)
    return check


def _enumerate_all(ctx, max_cost: Optional[float],
                   check: Callable[[int], None]) -> List[Dict[str, Any]]:
    """Полный перебор всех цепочек (без потолка числа), сортировка по цене.

    Тяжёлый режим для отладки: на плотном графе может строить огромный список — им
    пользуются, только когда max_results не задан. max_cost, если задан, режет ветки
    по нижней оценке «хвоста»."""
    stops, legs_by_origin, builder, city_info, chain_start, last = ctx
    lb = _completion_lb(stops, legs_by_origin) if max_cost is not None else None
    results: List[Dict[str, Any]] = []
    seq = {"id": 1}

    def dfs(i, city, arrive_iso, chosen, visited, g):
        check(len(results))
        if i == last:
            results.append(_assemble(stops, chosen, builder, city_info, chain_start, seq["id"]))
            seq["id"] += 1
            return
        candidates = _onward_candidates(stops, legs_by_origin, i, city, arrive_iso, visited)
        for f, dest in sorted(candidates, key=lambda p: _price_of(p[0])):
            g2 = g + _price_of(f)
            if lb is not None:
                tail = 0.0 if i + 1 == last else lb[i + 1].get(dest)
                if tail is None or g2 + tail > max_cost:
                    continue
            dfs(i + 1, dest, arrival_of(f), chosen + [f], visited | {dest}, g2)

    for start in stops[0].codes:
        if lb is not None:
            tail = lb[0].get(start)
            if tail is None or tail > max_cost:
                continue
        dfs(0, start, f"{chain_start}T00:00:00", [], {start}, 0.0)

    results.sort(key=lambda it: it["total_price"])
    return results


class _FlightTable:
    """Уникальные рейсы всех плеч с предрасчитанными полями стыковки. Перебор
    оперирует int-индексами в этой таблице, а не словарями рейсов."""

    def __init__(self, legs_by_origin: Dict[int, Dict[str, List[Dict[str, Any]]]]):
        self.flights: List[Dict[str, Any]] = []
        self.index: Dict[int, int] = {}  # id(рейс) → индекс (рейс лежит под кодом города И аэропорта)
        for by_origin in legs_by_origin.values():
            for flights in by_origin.values():
                for f in flights:
                    if id(f) not in self.index:
                        self.index[id(f)] = len(self.flights)
                        self.flights.append(f)
        self.dest = [(f.get("destination") or f.get("search_destination") or "").upper()
                     for f in self.flights]
        self.price = [_price_of(f) for f in self.flights]
        self.dep_day = [date_only(f["departure_at"]) if f.get("departure_at") else None
                        for f in self.flights]
        self.arr_day = [date_only(arrival_of(f)) if f.get("departure_at") else None
                        for f in self.flights]


def _search_cheapest(ctx, table: _FlightTable, max_results: int, max_cost: Optional[float],
                     check: Callable[[int], None]):
    """best-first (A*): выдаёт цепочки (кортежи индексов рейсов в table) по
    возрастанию цены и останавливается на N.

    Эвристика h = _completion_lb (минимальная цена «хвоста») — admissible и
    consistent (по построению lb[i][city] ≤ price(ребро) + lb[i+1][dest]), поэтому
    приоритет f = g + h выдаёт готовые цепочки строго по возрастанию суммарной цены:
    первые N извлечённых = N самых дешёвых.

    Главное ограничение — память (прод-VM 4 ГБ, N до миллиона), поэтому раскрытие
    ленивое: онворды города на плече i заранее отсортированы по price + h (список
    общий для всех узлов — _Candidates), и в куче лежит только ЛУЧШИЙ ещё не
    выданный ребёнок узла; следующего брата кладём, когда достают текущего. Куча
    растёт на ≤2 записи за шаг, а не на всех детей сразу. Узел — кортеж, путь —
    связный список (родитель, рейс) без копирования."""
    stops, legs_by_origin, _, _, chain_start, last = ctx
    lb = _completion_lb(stops, legs_by_origin)
    cands = _Candidates(stops, legs_by_origin, table, lb)
    heap: List[Any] = []
    seq = 0  # tie-breaker: не даём heapq сравнивать узлы

    def push_next(node, start: int) -> None:
        """Кладёт в кучу первого валидного ребёнка узла, начиная с позиции start."""
        nonlocal seq
        i, city, arrive_day, visited, g, _ = node
        keys, idxs = cands.get(i, city)
        any_next = stops[i + 1].kind == "any"
        for k in range(start, len(idxs)):
            f = g + keys[k]
            if max_cost is not None and f > max_cost:  # список отсортирован — дальше дороже
                return
            fi = idxs[k]
            if table.dep_day[fi] < arrive_day:  # нельзя вылететь раньше прилёта
                continue
            if any_next and table.dest[fi] in visited:  # «любой» — не в уже посещённый
                continue
            heapq.heappush(heap, (f, seq, node, k))
            seq += 1
            return

    start_day = chain_start  # прилёт в первый город — начало окна (T00:00)
    for start in stops[0].codes:
        tail = lb[0].get(start)
        if tail is None or (max_cost is not None and tail > max_cost):
            continue
        push_next((0, start, start_day, (start,), 0.0, None), 0)

    found = 0
    while heap and found < max_results:
        check(found)
        _, _, node, k = heapq.heappop(heap)
        push_next(node, k + 1)  # брат занимает место извлечённого
        i, city, _, visited, g, path = node
        fi = cands.get(i, city)[1][k]
        if i + 1 == last:  # цепочка готова — и она среди самых дешёвых из оставшихся
            found += 1
            yield _unwind((path, fi))
            continue
        dest = table.dest[fi]
        push_next((i + 1, dest, table.arr_day[fi], visited + (dest,), g + table.price[fi], (path, fi)), 0)


class _Candidates:
    """Онворд-рейсы (плечо i, город), отсортированные по price + lb хвоста за
    городом прилёта. Не зависят от времени прилёта и посещённых — поэтому один
    список на (i, город) делят все узлы. Тупики (хвоста нет), петли и прилёт вне
    разрешённых городов выкинуты сразу; дату и повторы проверяет push_next."""

    def __init__(self, stops, legs_by_origin, table: _FlightTable, lb):
        self.stops, self.legs_by_origin, self.table, self.lb = stops, legs_by_origin, table, lb
        self.last = len(stops) - 1
        self.cache: Dict[Any, Any] = {}

    def get(self, i: int, city: str):
        got = self.cache.get((i, city))
        if got is None:
            got = self.cache[(i, city)] = self._build(i, city)
        return got

    def _build(self, i: int, city: str):
        allow_next = _allowed(self.stops[i + 1])
        terminal = i + 1 == self.last
        pairs = []
        for f in self.legs_by_origin[i].get(city, []):
            fi = self.table.index[id(f)]
            dest = self.table.dest[fi]
            if not dest or dest == city or self.table.dep_day[fi] is None:
                continue
            if allow_next is not None and not (_side_codes(f, "dest") & allow_next):
                continue
            tail = 0.0 if terminal else self.lb[i + 1].get(dest)
            if tail is None:
                continue
            pairs.append((self.table.price[fi] + tail, fi))
        pairs.sort()
        return array("d", (p[0] for p in pairs)), array("i", (p[1] for p in pairs))


def _unwind(path) -> tuple:
    """Связный список (родитель, рейс) → кортеж индексов рейсов от первого плеча."""
    out = []
    while path is not None:
        path, fi = path
        out.append(fi)
    return tuple(reversed(out))


@lru_cache(maxsize=1 << 18)
def _stay(arrive_iso: str, depart_iso: str):
    """(дней между, покрыты ли оба выходных) — мемо: пары дат сильно повторяются."""
    return stay_between(arrive_iso, depart_iso), _has_both_weekend_days(arrive_iso, depart_iso)


def _pack_compact(ctx, table: _FlightTable, chains: array, legs: int) -> Dict[str, Any]:
    """Компактный результат (контракт — frontend/src/planner/compact.ts).

    segments — уникальные сегменты (make_segment), каждый один раз; chains — плоский
    массив, по legs индексов сегментов на цепочку (цепочки по возрастанию цены);
    days — по legs+1 дней в городах; weekend — битовая маска «оба выходных» по
    остановкам; total_days — длина поездки. Коды, даты и суммы фронт выводит сам."""
    stops, _, builder, city_info, chain_start, _ = ctx
    seg_of: Dict[int, int] = {}
    segments: List[Dict[str, Any]] = []
    out_chains = array("i")
    for fi in chains:
        si = seg_of.get(fi)
        if si is None:
            si = seg_of[fi] = len(segments)
            segments.append(builder.make_segment(table.flights[fi]))
        out_chains.append(si)

    start_iso = f"{chain_start}T00:00:00"
    count = len(out_chains) // legs if legs else 0
    days, weekend, total_days = array("h"), array("i"), array("h")
    for n in range(count):
        segs = [segments[out_chains[n * legs + k]] for k in range(legs)]
        stay_days, mask = _chain_stays(segs, start_iso)
        days.extend(stay_days)
        weekend.append(mask)
        total_days.append(_trip_days(segs))

    codes = {s[key] for s in segments for key in ("origin", "destination")}
    return {
        "format": "compact-v1",
        "count": count,
        "legs": legs,
        "chain_start": start_iso,
        "final_stay_days": FINAL_STAY_DAYS,
        "any_stops": [s.kind == "any" for s in stops],
        "cities": {c: [city_info(c)["city"], city_info(c)["flag"]] for c in codes if c},
        "segments": segments,
        "chains": out_chains,
        "days": days,
        "weekend": weekend,
        "total_days": total_days,
    }


def _chain_stays(segs: List[Dict[str, Any]], start_iso: str):
    """Дни в городах и маска выходных по остановкам — та же семантика, что в _assemble."""
    stay_days: List[int] = []
    mask = 0
    arrive = start_iso
    for k in range(len(segs) + 1):
        if k < len(segs):
            depart = segs[k]["departure_at"]
            d, wk = _stay(arrive, depart)
        else:
            depart = f"{_shift(date_only(arrive), FINAL_STAY_DAYS)}T00:00:00"
            d, wk = _stay(arrive, depart)
            d = max(1, d)
        stay_days.append(max(0, d))
        mask |= int(wk) << k
        if k < len(segs):
            arrive = segs[k]["arrival_at"]
    return stay_days, mask


def _trip_days(segs: List[Dict[str, Any]]) -> int:
    """Длина поездки — от даты первого вылета до даты последнего прилёта. Сколько мы
    были в стартовом городе до вылета и сколько пробудем в финальном после прилёта,
    к поездке не относится (дни на концах — условность: начало окна / FINAL_STAY_DAYS)."""
    if not segs:
        return 1
    return max(1, stay_between(date_only(segs[0]["departure_at"]), date_only(segs[-1]["arrival_at"])))


def compact_to_json(result: Dict[str, Any]) -> str:
    """JSON компактного результата. Массивы пишем кусками: json.dumps(list(array))
    на миллионе цепочек создал бы миллионы int-объектов разом."""
    buf = io.StringIO()
    buf.write("{")
    for n, (key, val) in enumerate(result.items()):
        buf.write(("," if n else "") + json.dumps(key) + ":")
        if isinstance(val, array):
            buf.write("[")
            step = 65536
            for off in range(0, len(val), step):
                buf.write(("," if off else "") + ",".join(map(str, val[off:off + step])))
            buf.write("]")
        else:
            buf.write(json.dumps(val, ensure_ascii=False, separators=(",", ":")))
    buf.write("}")
    return buf.getvalue()


# ------------------------- граф для обзора наборов городов --------------------

def _naive(iso: str) -> str:
    """ISO без таймзоны (как parse_datetime) — фронт считает дни пребывания так же."""
    return agg.parse_datetime(iso).strftime("%Y-%m-%dT%H:%M:%S")


def overview_graph(stops: List[Stop], collected: Dict[int, List[Dict[str, Any]]],
                   city_info=None) -> Dict[str, Any]:
    """Компактный граф собранных рейсов для режима «наборы городов» на фронте.

    Список цепочек ограничен max_results/max_cost, а обзор должен оценить ВСЕ
    варианты. Перечислять их нельзя (экспоненциально много), поэтому отдаём сами
    рёбра, а фронт (planner/overview.ts) динамикой по времени прилёта считает по
    каждой последовательности городов min-цену и точное число цепочек — с теми же
    фильтрами, что у списка. Семантика стыковки повторяет _onward_candidates/_assemble:
    from — город вылета (для первого перехода — только рейсы из стартовых кодов),
    to — город прилёта; dep/arr — наивное время.

    legs[i] — рёбра перехода i: {from, to, dep, arr, price, transfers, duration}."""
    if city_info is None:
        city_info = make_city_lookup(agg.load_airport_network())
    starts = set(stops[0].codes) if stops[0].kind == "cities" else set()
    legs: List[List[Dict[str, Any]]] = []
    codes = set(starts)
    for i in range(len(stops) - 1):
        seen = set()
        edges = []
        for f in collected.get(i, []):
            if i == 0 and not (_side_codes(f, "origin") & starts):
                continue
            origin = (f.get("origin") or f.get("search_origin") or "").upper()
            dest = (f.get("destination") or f.get("search_destination") or "").upper()
            dep = f.get("departure_at")
            if not origin or not dest or not dep:
                continue
            edge = {
                "from": origin, "to": dest, "dep": _naive(dep), "arr": _naive(arrival_of(f)),
                "price": _price_of(f) or 0, "transfers": int(f.get("transfers") or 0),
                "duration": f.get("duration") or 0,
            }
            key = tuple(edge.values())
            if key in seen:  # один и тот же рейс из пересекающихся под-запросов
                continue
            seen.add(key)
            edges.append(edge)
            codes.update((origin, dest))
        legs.append(edges)
    cities = {}
    for c in sorted(codes):
        ci = city_info(c)
        cities[c] = {"city": ci["city"], "flag": ci["flag"]}
    return {
        "chain_start": _leg_dates(stops, 0)[0],
        "final_stay_days": FINAL_STAY_DAYS,
        "starts": sorted(starts),
        "any": [s.kind == "any" for s in stops],
        "legs": legs,
        "cities": cities,
    }


def _assemble(stops: List[Stop], chosen: List[Dict[str, Any]], builder: "Builder",
              city_info, chain_start: str, itin_id: int) -> Dict[str, Any]:
    """Собирает объект Itinerary из выбранной цепочки рейсов."""
    segments = [builder.make_segment(f) for f in chosen]
    codes = [chosen[0]["origin"] if chosen else stops[0].codes[0]] + [s["destination"] for s in segments]

    itin_stops = []
    for k, code in enumerate(codes):
        arrive = f"{chain_start}T00:00:00" if k == 0 else segments[k - 1]["arrival_at"]
        if k < len(segments):
            depart = segments[k]["departure_at"]                 # вылет дальше
        else:
            depart = f"{_shift(date_only(arrive), FINAL_STAY_DAYS)}T00:00:00"  # финал: дефолт
        days = stay_between(arrive, depart)
        if k == len(segments):
            days = max(1, days)
        ci = city_info(code)
        itin_stops.append({
            "code": code,
            "city": ci["city"],
            "flag": ci["flag"],
            "arrive": arrive,
            "depart": depart,
            "days": max(0, days),
            "weekendCovered": _has_both_weekend_days(arrive, depart),
            "resolvedFromAny": stops[k].kind == "any",
        })

    total_price = sum(s["price"] or 0 for s in segments)
    total_transfers = sum(s["transfers"] or 0 for s in segments)
    travel_minutes = sum(s["duration"] or 0 for s in segments)
    total_days = _trip_days(segments)

    return {
        "id": itin_id,
        "stops": itin_stops,
        "segments": segments,
        "total_price": total_price,
        "total_days": total_days,
        "total_transfers": total_transfers,
        "travel_minutes": travel_minutes,
    }
