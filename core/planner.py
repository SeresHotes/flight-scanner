"""Движок планировщика цепочек A → B → C → …

Маршрут — линейная последовательность
остановок. Каждая остановка — НАБОР городов-кандидатов (kind='cities') или «любой»
(kind='any', wildcard в середине). Окно дат «когда ОК быть здесь» задаётся только у
ПРОМЕЖУТОЧНЫХ остановок; у концов оно выводится из соседей.

Сбор — через GraphQL Data API (core/graphql_api): все билеты на дату с сегментами,
пересадками и багажом. Город→город запрашиваем парами (одна страница на день),
плечо с «любым» концом — серией «город → ANY» / «ANY → город» с ценовым коридором
и потолком страниц (см. раздел «сбор»). Hidden-city — из тех же ответов A→ANY:
билет A→H→X даёт виртуальный рейс A→H с реальным временем прилёта. Оценка объёма
должна совпадать с клиентской (frontend/src/planner/estimate.ts).

Публичный контракт (frontend/src/planner/types.ts):
- estimate_plan(stops)      -> {requests, seconds, legs:[{fromLabel,toLabel,days,requests,anyLeg}]}
- collect_plan(stops, cb)   -> {leg_index: [нормализованные билеты]}   (реальные запросы к API)
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

from core.dates import get_date_range, parse_datetime
from core.network import load_airport_network
from core.segments import Builder, arrival_of, date_only, make_city_lookup, stay_between

SECONDS_PER_REQUEST = 1.0   # GraphQL: 60 запросов в минуту (совпадает с planner/estimate.ts)
MAX_REQUESTS = 2000         # предохранитель: столько ХОЛОДНЫХ страниц (не в кэше) за один сбор, ~35 мин
MAX_RESULTS = 1_000_000     # потолок max_results: компактный перебор держит его в памяти VM (4 ГБ)
DEFAULT_MAX_RESULTS = 5_000 # сколько самых дешёвых цепочек строит джоба, если запрос не задал
COMBO_MAX_RESULTS = 2_000   # сколько самых дешёвых цепочек строится на один выбранный набор
DEFAULT_START = "2026-11-01"  # якорь старта, если окон нет нигде (совпадает с mock)
DEFAULT_LEG_DAYS = 7          # ширина окна плеча, если оба конца без окна
FINAL_STAY_DAYS = 5           # пребывание в финальном городе (у конца окна нет)

# Оценка серии GraphQL в «запросах» (страницах по 400 билетов), см. раздел «сбор».
# Это же — потолок страниц серии, поэтому прогресс никогда не перерастает оценку.
PAGES_CITY = 1      # город → город: все билеты дня почти всегда в одной странице
PAGES_ANY = 12      # город → любой / любой → город: до 4 800 самых дешёвых билетов в день
PAGES_HIDDEN = 4    # A → ANY под hidden-city с коридором «дешевле лучшего A→B дня»
HIDDEN_MIN_RATIO = 0.5  # нижняя граница коридора hidden-city: доля от порога. Без неё
                        # страницы A→ANY (сортировка по цене) забивает дешёвая ближняя
                        # Россия/СНГ и до зоны транзита через хаб серия не доходит.

# Режимы REST-запроса (allow_indirect) — остались для core/anyscan (сбор X→ANY через
# prices_for_dates); планировщик REST больше не использует.
FETCH_MODES = (True, False)


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
    """Остановка запроса: набор городов (kind='cities') или «любой» + окно дат.

    radius_km — можно улететь дальше из соседнего города в этом радиусе (core/nearby);
    exact — прилёт строго в codes (маршруты выбранного набора городов)."""

    def __init__(self, kind: str, codes: List[str], window: List[str],
                 radius_km: float = 0, exact: bool = False):
        self.kind = kind                                    # 'cities' | 'any'
        self.codes = [c.upper() for c in codes if c]        # пусто для 'any'
        self.window = [window[0] if window else "", window[1] if window and len(window) > 1 else ""]
        self.radius_km = radius_km
        self.exact = exact

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Stop":
        # Принимаем и codes:[...], и airports:[{code}] (как во фронтовом PlannerStop).
        from core.nearby import clamp_radius
        codes = d.get("codes")
        if codes is None:
            codes = [a.get("code") for a in (d.get("airports") or [])]
        return Stop(d.get("kind", "cities"), codes or [], d.get("window") or ["", ""],
                    radius_km=clamp_radius(d.get("radiusKm")))

    def cardinality(self) -> float:
        """Число городов; «любой» = бесконечность (сторону нельзя заякорить)."""
        return max(1, len(self.codes)) if self.kind == "cities" else _INF


def parse_stops(raw: List[Dict[str, Any]]) -> List[Stop]:
    return [Stop.from_dict(d) for d in raw]


def collect_view(stops: List[Stop]) -> List[Stop]:
    """Остановки для сбора и оценки: к городам остановки с радиусом добавлены соседи
    (core/nearby) — рейсы из/в них нужны, чтобы стыковать с переездом."""
    from core.nearby import Hops
    hops = Hops(stops)
    if not hops.active():
        return stops
    return [Stop(s.kind, hops.collect_codes(i), s.window) if s.kind == "cities" else s
            for i, s in enumerate(stops)]


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


def _leg_days(stops: List[Stop], i: int) -> int:
    win = _leg_window(stops, i)
    return len(get_date_range(win[0], win[1])) if win[0] and win[1] else DEFAULT_LEG_DAYS


def _leg_requests(stops: List[Stop], i: int) -> int:
    """Страниц на плечо i (см. раздел «сбор»): город→город — пары A×B по PAGES_CITY
    плюс hidden-city A→ANY по PAGES_HIDDEN на город A; с «любым» концом — по
    PAGES_ANY на каждый конкретный город другого конца. Всё × дней окна."""
    from_stop, to_stop = stops[i], stops[i + 1]
    days = _leg_days(stops, i)
    if from_stop.kind == "cities" and to_stop.kind == "cities":
        a, b = max(1, len(from_stop.codes)), max(1, len(to_stop.codes))
        return days * (a * b * PAGES_CITY + a * PAGES_HIDDEN)
    anchor = from_stop if from_stop.kind == "cities" else to_stop
    return days * max(1, len(anchor.codes)) * PAGES_ANY


def plan_series(stops: List[Stop], max_cost: Optional[float] = None):
    """Детерминированные серии сбора: (плечо, origin|None, dest|None, день, value_min,
    value_max, страниц). Серии hidden-city сюда не входят — их коридор зависит от
    лучшей цены дня и известен только в сборе (в оценке они всегда «холодные»)."""
    stops = collect_view(stops)
    out = []
    vmax = int(max_cost) if max_cost else None
    for i in range(len(stops) - 1):
        from_stop, to_stop = stops[i], stops[i + 1]
        for day in _leg_dates(stops, i):
            if from_stop.kind == "cities" and to_stop.kind == "cities":
                for a in from_stop.codes:
                    for b in to_stop.codes:
                        out.append((i, a, b, day, None, None, PAGES_CITY))
            elif from_stop.kind == "cities":
                for a in from_stop.codes:
                    out.append((i, a, None, day, None, vmax, PAGES_ANY))
            else:
                for b in to_stop.codes:
                    out.append((i, None, b, day, None, vmax, PAGES_ANY))
    return out


def estimate_plan(stops: List[Stop], city_info=None, max_cost: Optional[float] = None,
                  is_cached: Optional[Callable[..., bool]] = None) -> Dict[str, Any]:
    """Оценка объёма сбора цепочки (совпадает с planner/estimate.ts): requests —
    всего страниц (по потолку серий), cached — сколько из них уже в кэше серий,
    cold — сколько реально пойдёт в источник, seconds — по холодным.

    is_cached(origin, dest, day, params_key, pages) → bool — проба кэша (storage.hot);
    без неё всё считается холодным."""
    if city_info is None:
        city_info = make_city_lookup(load_airport_network())
    stops = collect_view(stops)
    from core.graphql_api import params_key
    cached_by_leg: Dict[int, int] = {}
    if is_cached is not None:
        for i, origin, dest, day, vmin, vmax, pages in plan_series(stops, max_cost):
            if is_cached(origin, dest, day, params_key(vmin, vmax), pages):
                cached_by_leg[i] = cached_by_leg.get(i, 0) + pages
    legs = []
    requests = cached = 0
    for i in range(len(stops) - 1):
        days = _leg_days(stops, i)
        reqs = _leg_requests(stops, i)
        legs.append({
            "fromLabel": _stop_label(stops[i], city_info),
            "toLabel": _stop_label(stops[i + 1], city_info),
            "days": days,
            "requests": reqs,
            "cached": cached_by_leg.get(i, 0),
            "anyLeg": stops[i].kind == "any" or stops[i + 1].kind == "any",
        })
        requests += reqs
        cached += cached_by_leg.get(i, 0)
    cold = max(0, requests - cached)
    return {"requests": requests, "cached": cached, "cold": cold,
            "seconds": round(cold * SECONDS_PER_REQUEST), "legs": legs}


def request_count(stops: List[Stop]) -> int:
    stops = collect_view(stops)
    return sum(_leg_requests(stops, i) for i in range(len(stops) - 1))


# --------------------------------- сбор --------------------------------------
#
# Источник — GraphQL prices_one_way (core/graphql_api): все билеты на дату. Планы
# серий по видам плеча (серия = направление × день, страницы по 400):
#   город → город   пары A×B, обычно одна страница; плюс hidden-city: A→ANY с
#                   коридором «дешевле лучшего A→B того дня» (PAGES_HIDDEN);
#   город → любой   A→ANY по дням, коридор value_max = max_cost, потолок PAGES_ANY
#                   (сортировка по цене — теряются только самые дорогие);
#                   hidden-city выходит бесплатно: билет A→H→X даёт и рейс A→H;
#   любой → город   ANY→B по дням, тот же потолок; hidden-city нет (нужен X→ANY).
# «Запрос» в оценке = страница; прогресс идёт по страницам и добивается до оценки
# в конце серии, поэтому счётчик доходит ровно до total (и при попадании в кэш).


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



def _airport_city_learn(flights: List[Dict[str, Any]], airport_city: Dict[str, str]) -> None:
    """Пополняет карту аэропорт → город из концов билетов (PKX → BJS)."""
    for f in flights:
        for side, apt in (("destination", "destination_airport"), ("origin", "origin_airport")):
            if f.get(apt) and f.get(side) and not f.get("hidden_city"):
                airport_city.setdefault(f[apt].upper(), f[side].upper())


def _run_series(fetch_fn, origin: Optional[str], dest: Optional[str], day: str, pages: int,
                value_max: Optional[float], progress_cb,
                value_min: Optional[float] = None) -> List[Dict[str, Any]]:
    """Одна серия с прогрессом: тик на каждую полученную страницу, в конце добивка
    до `pages` (оценка серии), чтобы прогресс всегда сходился с total."""
    ticks = [0]

    def on_page(_page: int, _n: int) -> None:
        if progress_cb and ticks[0] < pages:
            ticks[0] += 1
            progress_cb()

    series = fetch_fn(origin, dest, day, value_min=int(value_min) if value_min else None,
                      value_max=int(value_max) if value_max else None,
                      max_pages=pages, progress_cb=on_page)
    if progress_cb:
        for _ in range(pages - ticks[0]):
            progress_cb()
    return series.get("tickets") or []


def _threshold(flights: List[Dict[str, Any]], key) -> Dict[Any, float]:
    """Порог hidden-city по группе key(f): цена лучшего ПРЯМОГО обычного рейса, если
    прямой есть, иначе лучшего вообще. Виртуальный рейс интересен, только если он
    дешевле этого порога (иначе есть обычный билет не хуже)."""
    best_direct: Dict[Any, float] = {}
    best_any: Dict[Any, float] = {}
    for f in flights:
        if f.get("hidden_city"):
            continue
        k = key(f)
        price = _price_of(f)
        if not int(f.get("transfers") or 0) and price < best_direct.get(k, _INF):
            best_direct[k] = price
        if price < best_any.get(k, _INF):
            best_any[k] = price
    return {**best_any, **best_direct}


def collect_plan(stops: List[Stop], progress_cb: Callable[[], None] = None,
                 fetch_fn=None,
                 leg_cb: Callable[[int], None] = None,
                 network: Optional[Dict[str, Dict[str, Any]]] = None,
                 max_cost: Optional[float] = None,
                 airport_city: Optional[Dict[str, str]] = None) -> Dict[int, List[Dict[str, Any]]]:
    """Собирает рейсы по каждому переходу через GraphQL (см. шапку раздела).

    Возвращает {индекс_перехода: [нормализованные билеты + виртуальные hidden-city]}.
    progress_cb() — на каждую страницу (и добивку до оценки серии),
    leg_cb(i) — перед началом перехода i (этап в UI),
    fetch_fn(origin, dest, day, value_max=, max_pages=, progress_cb=) → серия
      (по умолчанию graphql_api.fetch_series; воркер даёт кэширующую обёртку),
    max_cost — потолок цены всей поездки: коридор value_max для ANY-серий,
    airport_city — карта аэропорт → город из накопленных котировок (дополняется
      по ходу сбора); network — не используется, оставлен для совместимости вызова."""
    from core import graphql_api
    fetch = fetch_fn or graphql_api.fetch_series
    stops = collect_view(stops)
    airport_city = dict(airport_city or {})
    collected: Dict[int, List[Dict[str, Any]]] = {}
    for i in range(len(stops) - 1):
        if leg_cb:
            leg_cb(i)
        from_stop, to_stop = stops[i], stops[i + 1]
        dates = _leg_dates(stops, i)
        regular: List[Dict[str, Any]] = []
        virtual: List[Dict[str, Any]] = []
        seen = set()

        def keep(flights: List[Dict[str, Any]], side: Optional[str], allow: Optional[set]) -> None:
            _airport_city_learn(flights, airport_city)
            for f in flights:
                key = graphql_api.flight_key(f)
                if key in seen or (side and not _keep(f, side, allow)):
                    continue
                seen.add(key)
                regular.append(f)

        if from_stop.kind == "cities" and to_stop.kind == "cities":
            allow_to = set(to_stop.codes)
            for day in dates:
                for a in from_stop.codes:
                    day_regular: List[Dict[str, Any]] = []
                    for b in to_stop.codes:
                        got = _run_series(fetch, a, b, day, PAGES_CITY, None, progress_cb)
                        keep(got, "dest", allow_to)
                        day_regular += got
                    # hidden-city: A→ANY в коридоре [HIDDEN_MIN_RATIO × порог, порог],
                    # порог — лучший обычный A→B этого дня (без него — max_cost)
                    thr = _threshold(day_regular, lambda f: 0).get(0)
                    got = _run_series(fetch, a, None, day, PAGES_HIDDEN, thr or max_cost, progress_cb,
                                      value_min=thr * HIDDEN_MIN_RATIO if thr else None)
                    _airport_city_learn(got, airport_city)
                    hub_city = _hub_resolver(allow_to, airport_city)
                    virtual += hidden_city_flights(got, hub_city, {0: thr} if thr else {},
                                                   lambda f: 0, seen)
        elif from_stop.kind == "cities":                      # город → любой
            day_flights: Dict[str, List[Dict[str, Any]]] = {}
            for day in dates:
                for a in from_stop.codes:
                    got = _run_series(fetch, a, None, day, PAGES_ANY, max_cost, progress_cb)
                    keep(got, None, None)
                    day_flights.setdefault(day, []).extend(got)
            # hidden-city во все промежуточные хабы (кроме города вылета): дешевле
            # лучшего обычного рейса A→хаб того же дня из этой же выборки.
            hub_city = lambda apt, ac=airport_city: ac.get(apt, apt)   # noqa: E731
            for day, flights in day_flights.items():
                thr = _threshold(flights, lambda f: (f.get("origin"), f.get("destination")))
                virtual += hidden_city_flights(
                    flights, hub_city, thr,
                    lambda f: (f.get("origin"), f.get("destination")), seen)
        else:                                                  # любой → город
            for day in dates:
                for b in to_stop.codes:
                    got = _run_series(fetch, None, b, day, PAGES_ANY, max_cost, progress_cb)
                    keep(got, "dest", set(to_stop.codes))
        collected[i] = regular + virtual
    return collected


# --------------------------------- hidden-city --------------------------------

def _hub_resolver(allow_to: set, airport_city: Dict[str, str]):
    """Аэропорт → код остановки B, если это её аэропорт (кодом города или самого
    аэропорта), иначе None."""
    def resolve(apt: str) -> Optional[str]:
        if apt in allow_to:
            return apt
        city = airport_city.get(apt)
        return city if city in allow_to else None
    return resolve


def _virtual_flight(t: Dict[str, Any], k: int, hub_city: str) -> Dict[str, Any]:
    """Виртуальный рейс «выходим на k-й пересадке»: сегменты до хаба, реальное время
    прилёта в хаб, transfers = k, цена всего билета; hidden_city — что это за билет."""
    from core.graphql_api import _minutes_between
    tp = t["transfer_points"][k]
    hub = tp["code"]
    legs = t["legs"][:k + 1]
    arrival = legs[-1]["arrival_at"]
    air = [_minutes_between(l["departure_at"], l["arrival_at"]) for l in legs]
    chain = t["chain"]
    cut = next((j for j, code in enumerate(chain) if j >= 1 and code == hub), len(chain) - 1)
    v = dict(t)
    v.update({
        "destination": hub_city,
        "destination_airport": hub,
        "arrival_at": arrival,
        "duration": _minutes_between(t["departure_at"], arrival),
        "duration_to": sum(air) if all(m is not None for m in air) else None,
        "transfers": k,
        "transfer_points": t["transfer_points"][:k],
        "legs": legs,
        "chain": chain[:cut + 1],
        "hidden_city": {
            "final": t.get("destination"),
            "final_airport": t.get("destination_airport"),
            "chain": chain,
            "full_duration": t.get("duration"),
            "full_transfers": int(t.get("transfers") or 0),
            "baggage": t.get("baggage"),
            "arrival_estimated": False,
        },
    })
    return v


def hidden_city_flights(tickets: List[Dict[str, Any]], hub_city, thresholds: Dict[Any, float],
                        key, seen: Optional[set] = None) -> List[Dict[str, Any]]:
    """Виртуальные рейсы hidden-city из билетов с пересадками.

    Для каждой пересадки билета, чей аэропорт hub_city(apt) распознан как остановка
    (вернул код города), строим рейс «до этой пересадки». Оставляем только те, что
    дешевле порога thresholds[key(виртуальный рейс)] (лучший обычный рейс того же
    дня/направления); без порога — берём все. Хаб не может совпадать с городом
    вылета. seen — общий с обычными рейсами набор ключей для дедупликации."""
    from core.graphql_api import flight_key
    seen = seen if seen is not None else set()
    out: List[Dict[str, Any]] = []
    for t in tickets:
        points = t.get("transfer_points") or []
        if not points or not t.get("legs"):
            continue
        for k, tp in enumerate(points):
            city = hub_city(tp["code"])
            if not city or city == t.get("origin") or tp["code"] == t.get("origin_airport"):
                continue
            v = _virtual_flight(t, k, city)
            thr = thresholds.get(key(v))
            if thr is not None and _price_of(v) >= thr:
                continue
            fk = flight_key(v)
            if fk in seen:
                continue
            seen.add(fk)
            out.append(v)
    return out

# ------------------------------- сборка цепочек ------------------------------

def _has_both_weekend_days(arrive_iso: str, depart_iso: str) -> bool:
    """Оба выходных (сб И вс) попадают в пребывание [arrive..depart]."""
    from datetime import timedelta
    a = parse_datetime(arrive_iso)
    b = parse_datetime(depart_iso)
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
                   legs_by_origin: Dict[int, Dict[str, List[Dict[str, Any]]]],
                   hops=None) -> List[Dict[str, float]]:
    """Нижняя оценка стоимости «хвоста» цепочки для отсечения по бюджету.

    lb[i][city] — минимально возможная суммарная цена, чтобы, ПРИЛЕТЕВ в `city` на
    остановку i, добраться до финальной остановки, учитывая ТОЛЬКО цены рейсов,
    разрешённые города остановок и переезды в соседний город (hops) — БЕЗ ограничений
    на время вылета и на повторы городов. Это релаксация задачи, поэтому оценка никогда
    не завышает реальную стоимость — ветку, где накопленная_цена + lb > бюджета, можно
    резать, не теряя валидных цепочек (admissible-эвристика).

    Считается обратной динамикой по плечам (от последнего перехода к первому) — это
    и есть тот самый граф минимальных цен перелётов: город, из которого дешевле
    бюджета не собрать ни одной цепочки, отсекается целиком, не разворачивая поддерево."""
    from core.nearby import Hops, neighbors
    hops = hops or Hops(stops)
    last = len(stops) - 1
    lb: List[Dict[str, float]] = [dict() for _ in range(len(stops))]
    for i in range(last - 1, -1, -1):
        allow_next = hops.arrive_allowed(i + 1)
        nxt = lb[i + 1]
        terminal_next = (i + 1 == last)
        dep: Dict[str, float] = {}      # вылетев из города на плече i
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
                dep[city] = best
        if hops.radius[i] <= 0:
            lb[i] = dep
            continue
        # прилёт в a → вылет из любого города departs(i, a); a — сам город вылета или сосед
        cur = lb[i]
        arrivals = set(dep)
        for d in dep:
            arrivals.update(neighbors(d, hops.radius[i]))
        for a in arrivals:
            best = min((dep[d] for d in hops.departs(i, a) if d in dep), default=_INF)
            if best < _INF:
                cur[a] = best
    return lb


@lru_cache(maxsize=1 << 16)
def _gap_ok(arrive_iso: str, depart_iso: str) -> bool:
    """Хватает ли времени на переезд в соседний город (nearby.HOP_MIN_GAP_MIN).
    Времена — локальные, таймзона срезана (соседи почти всегда в одном поясе)."""
    from core.nearby import HOP_MIN_GAP_MIN
    a = parse_datetime(arrive_iso).replace(tzinfo=None)
    d = parse_datetime(depart_iso).replace(tzinfo=None)
    return (d - a).total_seconds() >= HOP_MIN_GAP_MIN * 60


def _onward_candidates(stops: List[Stop],
                       legs_by_origin: Dict[int, Dict[str, List[Dict[str, Any]]]],
                       i: int, city: str, arrive_iso: str, visited: set, hops=None
                       ) -> List[Any]:
    """Валидные онворд-рейсы после прилёта в `city` на плече i: вылет не раньше
    прилёта (из соседнего города — с запасом на переезд), посадка в разрешённом для
    следующей остановки городе, без петель/повторов. Возвращает пары (рейс, город_прилёта).

    Запрет повторов — только для «любой»-остановок: явно заданный город пользователь
    выбрал сам, и повтор там осмыслен (кольцо MOW → … → MOW). Иначе финал, совпадающий
    со стартом, не собирался никогда, а _completion_lb (не знает про visited) держал
    такие ветки живыми — A* перебирал бесконечно, не находя ни одной цепочки."""
    from core.nearby import Hops
    hops = hops or Hops(stops)
    arrive_day = date_only(arrive_iso)
    allow_next = hops.arrive_allowed(i + 1)
    out = []
    seen = set()
    for d in hops.departs(i, city):
        for f in legs_by_origin[i].get(d, []):
            if id(f) in seen:
                continue
            seen.add(id(f))
            dep = f.get("departure_at")
            if not dep or date_only(dep) < arrive_day:  # нельзя вылететь раньше прилёта
                continue
            if i > 0 and city not in _side_codes(f, "origin") and not _gap_ok(arrive_iso, dep):
                continue
            dest = (f.get("destination") or f.get("search_destination") or "").upper()
            if not dest or dest == city or dest == d:  # без петель
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
                      on_progress: Optional[Callable[[int, int], None]] = None,
                      query: Optional["PlanQuery"] = None) -> List[Dict[str, Any]]:
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
    ctx = _build_ctx(stops, _apply_leg_filters(collected, query), city_info)
    check = _step_check(should_stop, on_progress)

    if max_results is None:
        return _enumerate_all(ctx, max_cost, check)
    table = _FlightTable(ctx[1])
    stops, _, builder, city_info, chain_start, _ = ctx
    chains = _search_cheapest(ctx, table, max_results, max_cost, check, query)
    return [_assemble(stops, [table.flights[fi] for fi in chain], builder, city_info, chain_start, n + 1)
            for n, chain in enumerate(chains)]


def build_itineraries_compact(stops: List[Stop], collected: Dict[int, List[Dict[str, Any]]],
                              max_results: int, city_info=None,
                              max_cost: Optional[float] = None,
                              should_stop: Optional[Callable[[], bool]] = None,
                              on_progress: Optional[Callable[[int, int], None]] = None,
                              query: Optional["PlanQuery"] = None) -> Dict[str, Any]:
    """То же, что build_itineraries с потолком, но результат компактный (_pack_compact).

    Рассчитан на сотни тысяч – миллион цепочек на VM с 4 ГБ: цепочка в памяти — это
    несколько int-индексов рейсов в плоском array, а не словарь с копиями сегментов
    (100k словарей Itinerary + их JSON стоили гигабайты и роняли api по OOM).

    query (core/planquery.PlanQuery) — фильтры: плеча — к рейсам до перебора,
    городов (дни, окно, выходные) — внутри перебора, длины поездки — при выдаче."""
    ctx = _build_ctx(stops, _apply_leg_filters(collected, query), city_info)
    check = _step_check(should_stop, on_progress)
    table = _FlightTable(ctx[1])
    chains = array("i")
    for chain in _search_cheapest(ctx, table, max_results, max_cost, check, query):
        chains.extend(chain)
    return _pack_compact(ctx, table, chains, len(stops) - 1)


def _apply_leg_filters(collected: Dict[int, List[Dict[str, Any]]],
                       query: Optional["PlanQuery"]) -> Dict[int, List[Dict[str, Any]]]:
    """Фильтры плеча (пересадки, ожидание, длительность, багаж, hidden-city) — к рейсам
    до построения: таблица рейсов сразу меньше, перебор не видит лишнего."""
    if query is None:
        return collected
    from core.planquery import filter_leg_flights
    return {i: filter_leg_flights(flights, query.legs[i] if i < len(query.legs) else None)
            for i, flights in collected.items()}


def _stay_ok(cf, arrive_iso: str, depart_iso: str) -> bool:
    """Фильтр пребывания в промежуточном городе (core/planquery.CityFilter): дни между
    прилётом и вылетом (та же _stay, что и в результате), обязательное окно, выходные."""
    days, weekend = _stay(arrive_iso, depart_iso)
    if days < cf.min_stay or (cf.max_stay is not None and days > cf.max_stay):
        return False
    if cf.must_cover and not (date_only(arrive_iso) <= cf.must_cover[0]
                              and date_only(depart_iso) >= cf.must_cover[1]):
        return False
    if cf.require_weekend and not weekend:
        return False
    return True


def _build_ctx(stops: List[Stop], collected: Dict[int, List[Dict[str, Any]]], city_info):
    if city_info is None:
        city_info = make_city_lookup(load_airport_network())
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
    from core.nearby import Hops
    hops = Hops(stops)
    lb = _completion_lb(stops, legs_by_origin, hops) if max_cost is not None else None
    results: List[Dict[str, Any]] = []
    seq = {"id": 1}

    def dfs(i, city, arrive_iso, chosen, visited, g):
        check(len(results))
        if i == last:
            results.append(_assemble(stops, chosen, builder, city_info, chain_start, seq["id"]))
            seq["id"] += 1
            return
        candidates = _onward_candidates(stops, legs_by_origin, i, city, arrive_iso, visited, hops)
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
        self.dep_iso = [f.get("departure_at") for f in self.flights]
        self.arr_iso = [arrival_of(f) if f.get("departure_at") else None for f in self.flights]
        self.dep_day = [date_only(d) if d else None for d in self.dep_iso]
        self.arr_day = [date_only(a) if a else None for a in self.arr_iso]


def _search_cheapest(ctx, table: _FlightTable, max_results: int, max_cost: Optional[float],
                     check: Callable[[int], None], query: Optional["PlanQuery"] = None):
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
    from core.nearby import Hops
    hops = Hops(stops)
    lb = _completion_lb(stops, legs_by_origin, hops)
    cands = _Candidates(stops, legs_by_origin, table, lb, hops)
    heap: List[Any] = []
    seq = 0  # tie-breaker: не даём heapq сравнивать узлы
    # Фильтры городов действуют у промежуточных остановок (1..last-1): пребывание —
    # между прилётом узла и вылетом кандидата. Длина поездки — при выдаче.
    city_filters = {i: query.cities[i] for i in range(1, last)
                    if query is not None and i < len(query.cities) and not query.cities[i].is_open()}
    trip_length = query.trip_length if query is not None else None
    if trip_length is not None and not (trip_length[0] or trip_length[1] is not None):
        trip_length = None

    def push_next(node, start: int) -> None:
        """Кладёт в кучу первого валидного ребёнка узла, начиная с позиции start."""
        nonlocal seq
        i, city, arrive_day, visited, g, _, arrive_iso = node
        keys, idxs, hop = cands.get(i, city)
        any_next = stops[i + 1].kind == "any"
        cf = city_filters.get(i)
        for k in range(start, len(idxs)):
            f = g + keys[k]
            if max_cost is not None and f > max_cost:  # список отсортирован — дальше дороже
                return
            fi = idxs[k]
            if table.dep_day[fi] < arrive_day:  # нельзя вылететь раньше прилёта
                continue
            if hop[k] and i > 0 and not _gap_ok(arrive_iso, table.dep_iso[fi]):  # переезд к соседу
                continue
            if any_next and table.dest[fi] in visited:  # «любой» — не в уже посещённый
                continue
            if cf is not None and not _stay_ok(cf, arrive_iso, table.dep_iso[fi]):
                continue
            heapq.heappush(heap, (f, seq, node, k))
            seq += 1
            return

    start_day = chain_start  # прилёт в первый город — начало окна (T00:00)
    start_iso = f"{chain_start}T00:00:00"
    for start in stops[0].codes:
        tail = lb[0].get(start)
        if tail is None or (max_cost is not None and tail > max_cost):
            continue
        push_next((0, start, start_day, (start,), 0.0, None, start_iso), 0)

    found = 0
    while heap and found < max_results:
        check(found)
        _, _, node, k = heapq.heappop(heap)
        push_next(node, k + 1)  # брат занимает место извлечённого
        i, city, _, visited, g, path, _ = node
        fi = cands.get(i, city)[1][k]
        if i + 1 == last:  # цепочка готова — и она среди самых дешёвых из оставшихся
            chain = _unwind((path, fi))
            if trip_length is not None:
                from core.planquery import trip_length_ok
                days = max(1, stay_between(table.dep_day[chain[0]], table.arr_day[chain[-1]]))
                if not trip_length_ok(days, trip_length):
                    continue
            found += 1
            yield chain
            continue
        dest = table.dest[fi]
        push_next((i + 1, dest, table.arr_day[fi], visited + (dest,), g + table.price[fi], (path, fi),
                   table.arr_iso[fi]), 0)


class _Candidates:
    """Онворд-рейсы (плечо i, город прилёта), отсортированные по price + lb хвоста за
    городом прилёта. Вылет — из самого города или соседнего в радиусе остановки
    (hops.departs; у таких рейсов флаг hop — push_next проверяет запас на переезд).
    Не зависят от времени прилёта и посещённых — поэтому один список на (i, город)
    делят все узлы. Тупики (хвоста нет), петли и прилёт вне разрешённых городов
    выкинуты сразу; дату и повторы проверяет push_next."""

    def __init__(self, stops, legs_by_origin, table: _FlightTable, lb, hops):
        self.stops, self.legs_by_origin, self.table, self.lb = stops, legs_by_origin, table, lb
        self.hops = hops
        self.last = len(stops) - 1
        self.cache: Dict[Any, Any] = {}

    def get(self, i: int, city: str):
        got = self.cache.get((i, city))
        if got is None:
            got = self.cache[(i, city)] = self._build(i, city)
        return got

    def _build(self, i: int, city: str):
        allow_next = self.hops.arrive_allowed(i + 1)
        terminal = i + 1 == self.last
        pairs = []
        seen = set()
        for d in self.hops.departs(i, city):
            for f in self.legs_by_origin[i].get(d, []):
                fi = self.table.index[id(f)]
                if fi in seen:
                    continue
                seen.add(fi)
                dest = self.table.dest[fi]
                if not dest or dest == city or dest == d or self.table.dep_day[fi] is None:
                    continue
                if allow_next is not None and not (_side_codes(f, "dest") & allow_next):
                    continue
                tail = 0.0 if terminal else self.lb[i + 1].get(dest)
                if tail is None:
                    continue
                pairs.append((self.table.price[fi] + tail, fi, city not in _side_codes(f, "origin")))
        pairs.sort()
        return (array("d", (p[0] for p in pairs)), array("i", (p[1] for p in pairs)),
                bytes(p[2] for p in pairs))


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
            **_depart_from(code, segments[k] if 0 < k < len(segments) else None,
                           lambda c: [city_info(c)["city"], city_info(c)["flag"]]),
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


def _depart_from(code: str, seg: Optional[Dict[str, Any]], names) -> Dict[str, Any]:
    """{departFrom: {code, city, flag, km}} — если из остановки улетаем не из города
    прилёта, а из соседнего (переезд, core/nearby); иначе пусто."""
    if not seg:
        return {}
    origin = (seg.get("origin") or "").upper()
    if not origin or origin == (code or "").upper():
        return {}
    from core.nearby import distance_km
    km = distance_km(code, origin)
    name = names(origin)
    return {"departFrom": {"code": origin, "city": name[0], "flag": name[1],
                           "km": round(km) if km is not None else None}}


# ------------------------ материализация страницы результата ------------------

def chain_codes(result: Dict[str, Any], n: int) -> List[str]:
    """Коды городов цепочки n компактного результата: старт + прилёты всех плеч."""
    legs, chains, segs = result["legs"], result["chains"], result["segments"]
    base = n * legs
    return [segs[chains[base]]["origin"]] + [segs[chains[base + k]]["destination"] for k in range(legs)]


def combo_key(codes: List[str]) -> str:
    return "-".join(codes)


def materialize(result: Dict[str, Any], n: int) -> Dict[str, Any]:
    """Itinerary цепочки n из компактного результата (зеркало compact.ts.materialize):
    остановки с прилётом/вылетом/днями/выходными, сегменты, суммы."""
    legs, chains, segs = result["legs"], result["chains"], result["segments"]
    stop_count = legs + 1
    segments = [segs[chains[n * legs + k]] for k in range(legs)]
    cities = result.get("cities") or {}
    any_stops = result.get("any_stops") or [False] * stop_count
    stops = []
    for k in range(stop_count):
        code = segments[0]["origin"] if k == 0 else segments[k - 1]["destination"]
        arrive = result["chain_start"] if k == 0 else segments[k - 1]["arrival_at"]
        if k < legs:
            depart = segments[k]["departure_at"]
        else:
            depart = f"{_shift(date_only(arrive), result.get('final_stay_days', FINAL_STAY_DAYS))}T00:00:00"
        info = cities.get(code) or [code, ""]
        stops.append({
            "code": code, "city": info[0], "flag": info[1],
            "arrive": arrive, "depart": depart,
            "days": result["days"][n * stop_count + k],
            "weekendCovered": bool((result["weekend"][n] >> k) & 1),
            "resolvedFromAny": bool(any_stops[k]),
            **_depart_from(code, segments[k] if 0 < k < legs else None,
                           lambda c: cities.get(c) or [c, ""]),
        })
    return {
        "id": n + 1,
        "stops": stops,
        "segments": segments,
        "total_price": sum(s.get("price") or 0 for s in segments),
        "total_days": result["total_days"][n],
        "total_transfers": sum(s.get("transfers") or 0 for s in segments),
        "travel_minutes": sum(s.get("duration") or 0 for s in segments),
    }


def combo_index(result: Dict[str, Any]) -> Dict[str, List[int]]:
    """Набор городов → индексы цепочек (в порядке цены). Один проход по результату;
    вызывающий кэширует вместе с разобранным результатом."""
    out: Dict[str, List[int]] = {}
    for n in range(result["count"]):
        out.setdefault(combo_key(chain_codes(result, n)), []).append(n)
    return out


def routes_page(result: Dict[str, Any], offset: int, limit: int,
                combos: Optional[List[str]] = None,
                index: Optional[Dict[str, List[int]]] = None) -> Dict[str, Any]:
    """Страница маршрутов: цепочки по возрастанию цены, при combos — только из
    перечисленных наборов (ключ combo_key), слитые по цене. index — combo_index."""
    if combos:
        index = index if index is not None else combo_index(result)
        picked: List[int] = sorted(n for c in combos for n in index.get(c, []))
        total = len(picked)
        page = picked[offset:offset + limit]
    else:
        total = result["count"]
        page = list(range(offset, min(total, offset + limit)))
    return {"total": total, "offset": offset, "limit": limit,
            "items": [materialize(result, n) for n in page]}


def build_combo_routes(stops: List[Stop], collected: Dict[int, List[Dict[str, Any]]],
                       combos: List[List[str]], query=None, city_info=None,
                       per_combo: int = COMBO_MAX_RESULTS) -> List[Dict[str, Any]]:
    """Маршруты выбранных наборов городов по требованию: на каждый набор — тот же A*
    по сохранённым рейсам джобы, но остановки зафиксированы кодами набора (окна дат
    и фильтры запроса — прежние). Результат — Itinerary по возрастанию цены, у каждого
    поле combo (ключ набора)."""
    if city_info is None:
        city_info = make_city_lookup(load_airport_network())
    max_cost = query.max_cost if query is not None else None
    out: List[Dict[str, Any]] = []
    for codes in combos:
        if len(codes) != len(stops):
            continue
        fixed = [Stop("cities", [code], stop.window, radius_km=stop.radius_km, exact=True)
                 for code, stop in zip(codes, stops)]
        res = build_itineraries_compact(fixed, collected, max_results=per_combo, city_info=city_info,
                                        max_cost=max_cost, query=query)
        key = combo_key(codes)
        for n in range(res["count"]):
            it = materialize(res, n)
            it["combo"] = key
            out.append(it)
    out.sort(key=lambda it: it["total_price"])
    for n, it in enumerate(out):
        it["id"] = n + 1
    return out
