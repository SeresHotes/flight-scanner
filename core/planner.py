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
"""
from typing import Any, Callable, Dict, List, Optional

from core import aggregate as agg
from core.collector import collect_leg_data, get_date_range
from core.trip_builder import Builder, arrival_of, date_only, make_city_lookup, stay_between

SECONDS_PER_REQUEST = 0.65  # совпадает с planner/estimate.ts
MAX_REQUESTS = 200          # предохранитель от слишком широких окон
DEFAULT_START = "2026-11-01"  # якорь старта, если окон нет нигде (совпадает с mock)
DEFAULT_LEG_DAYS = 7          # ширина окна плеча, если оба конца без окна
FINAL_STAY_DAYS = 5           # пребывание в финальном городе (у конца окна нет)

# Собираем ВСЕ цепочки из имеющихся данных (без beam/per-city/per-sequence кэпов).
# Единственный предел — защитный потолок от катастрофического комбинаторного
# взрыва: без него DFS на нескольких «any»-остановках может дать столько цепочек,
# что VM ляжет по памяти. При переполнении сборка останавливается и логируется.
_MAX_ITINERARIES = 100_000  # защитный потолок числа построенных цепочек
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


def _leg_requests(stops: List[Stop], i: int) -> int:
    """Запросов на плечо i = (меньшая мощность конца) × дней окна."""
    anchor = min(stops[i].cardinality(), stops[i + 1].cardinality())
    win = _leg_window(stops, i)
    days = len(get_date_range(win[0], win[1])) if win[0] and win[1] else DEFAULT_LEG_DAYS
    return int(anchor) * days


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


def collect_plan(stops: List[Stop], progress_cb: Callable[[], None] = None) -> Dict[int, List[Dict[str, Any]]]:
    """Реально ходит в Travelpayouts: собирает рейсы по каждому переходу.

    Возвращает {индекс_перехода: [сырые рейсы]}. allow_indirect=True — чтобы фильтр
    по числу пересадок на фронте имел смысл. progress_cb() — после каждого запроса."""
    collected: Dict[int, List[Dict[str, Any]]] = {}
    for i in range(len(stops) - 1):
        leg_flights: List[Dict[str, Any]] = []
        for origin, dest, dates, side, allow in _leg_series(stops, i):
            flights = collect_leg_data(
                origin, dest, dates, leg_name=f"leg{i}",
                allow_indirect=True, progress_cb=progress_cb,
            )
            leg_flights += [f for f in flights if _keep(f, side, allow)]
        collected[i] = leg_flights
    return collected


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


def _onward_sorted(flights: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Все онвард-рейсы по возрастанию цены — раскрываем каждый (без beam-обрезки).

    Порядок по цене нужен, чтобы при упоре в защитный потолок первыми набирались
    дешёвые цепочки, а отсекались дорогие."""
    return sorted(flights, key=_price_of)


def _allowed(stop: Stop) -> Optional[set]:
    """Множество разрешённых городов остановки; None = «любой»."""
    return set(stop.codes) if stop.kind == "cities" else None


def build_itineraries(stops: List[Stop], collected: Dict[int, List[Dict[str, Any]]],
                      city_info=None) -> List[Dict[str, Any]]:
    """Собирает цепочки из собранных плеч. Чистая функция (без I/O)."""
    if city_info is None:
        city_info = make_city_lookup(agg.load_airport_network())
    builder = Builder(None, city_info)  # make_segment использует только city_info

    legs_by_origin = {i: _index_leg(collected.get(i, [])) for i in range(len(stops) - 1)}
    chain_start = _leg_dates(stops, 0)[0]  # первая дата окна нулевого плеча

    results: List[Dict[str, Any]] = []
    seq = {"id": 1}

    def dfs(i: int, city: str, arrive_iso: str, chosen: List[Dict[str, Any]], visited: set):
        if len(results) >= _MAX_ITINERARIES:
            return
        if i == len(stops) - 1:  # дошли до финальной остановки — цепочка готова
            results.append(_assemble(stops, chosen, builder, city_info, chain_start, seq["id"]))
            seq["id"] += 1
            return

        arrive_day = date_only(arrive_iso)
        allow_next = _allowed(stops[i + 1])
        candidates = []
        for f in legs_by_origin[i].get(city, []):
            dep = f.get("departure_at")
            if not dep or date_only(dep) < arrive_day:  # нельзя вылететь раньше прилёта
                continue
            dest = (f.get("destination") or f.get("search_destination") or "").upper()
            if not dest or dest == city or dest in visited:  # без петель/повторов
                continue
            if allow_next is not None and not (_side_codes(f, "dest") & allow_next):
                continue
            candidates.append(f)

        for f in _onward_sorted(candidates):
            if len(results) >= _MAX_ITINERARIES:
                return
            dest = (f.get("destination") or f.get("search_destination")).upper()
            dfs(i + 1, dest, arrival_of(f), chosen + [f], visited | {dest})

    for start in stops[0].codes:  # старт — из каждого города-кандидата первой остановки
        start_arrive = f"{chain_start}T00:00:00"
        dfs(0, start, start_arrive, [], {start})

    results.sort(key=lambda it: it["total_price"])
    return results


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
    first, last = itin_stops[0], itin_stops[-1]
    total_days = max(1, stay_between(first["arrive"], last["depart"]))

    return {
        "id": itin_id,
        "stops": itin_stops,
        "segments": segments,
        "total_price": total_price,
        "total_days": total_days,
        "total_transfers": total_transfers,
        "travel_minutes": travel_minutes,
    }
