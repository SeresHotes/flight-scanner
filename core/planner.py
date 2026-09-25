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
import heapq
from typing import Any, Callable, Dict, List, Optional

from core import aggregate as agg
from core.collector import collect_leg_data, get_date_range
from core.trip_builder import Builder, arrival_of, date_only, make_city_lookup, stay_between

SECONDS_PER_REQUEST = 0.65  # совпадает с planner/estimate.ts
MAX_REQUESTS = 200          # предохранитель от слишком широких окон
DEFAULT_START = "2026-11-01"  # якорь старта, если окон нет нигде (совпадает с mock)
DEFAULT_LEG_DAYS = 7          # ширина окна плеча, если оба конца без окна
FINAL_STAY_DAYS = 5           # пребывание в финальном городе (у конца окна нет)


def is_valid_max_results(n: Optional[int]) -> bool:
    """max_results ОБЯЗАТЕЛЕН и должен быть положительным. Прод — burstable VM (4 ГБ):
    безлимитный перебор (max_results=None) на плотном графе с двумя «any» строит сотни
    тысяч Itinerary → сотни МБ JSON → вешает браузер и почти достаёт до OOM. None
    приходит от устаревшего закешированного фронта или прямого вызова API — такие
    запросы отклоняем (не зажимаем потолком), чтобы движок физически не уходил в
    безлимит."""
    return n is not None and n >= 1

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


def collect_plan(stops: List[Stop], progress_cb: Callable[[], None] = None,
                 fetch_fn=None,
                 leg_cb: Callable[[int], None] = None) -> Dict[int, List[Dict[str, Any]]]:
    """Реально ходит в Travelpayouts: собирает рейсы по каждому переходу.

    Возвращает {индекс_перехода: [сырые рейсы]}. allow_indirect=True — чтобы фильтр
    по числу пересадок на фронте имел смысл. progress_cb() — после каждого запроса,
    leg_cb(i) — перед началом перехода i (для показа этапа в UI).
    fetch_fn позволяет подменить обращение к API (кэширующая обёртка из api.worker)."""
    collected: Dict[int, List[Dict[str, Any]]] = {}
    for i in range(len(stops) - 1):
        if leg_cb:
            leg_cb(i)
        leg_flights: List[Dict[str, Any]] = []
        for origin, dest, dates, side, allow in _leg_series(stops, i):
            flights = collect_leg_data(
                origin, dest, dates, leg_name=f"leg{i}",
                allow_indirect=True, progress_cb=progress_cb, fetch_fn=fetch_fn,
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
    if city_info is None:
        city_info = make_city_lookup(agg.load_airport_network())
    builder = Builder(None, city_info)  # make_segment использует только city_info

    legs_by_origin = {i: _index_leg(collected.get(i, [])) for i in range(len(stops) - 1)}
    chain_start = _leg_dates(stops, 0)[0]  # первая дата окна нулевого плеча
    last = len(stops) - 1
    ctx = (stops, legs_by_origin, builder, city_info, chain_start, last)
    check = _step_check(should_stop, on_progress)

    if max_results is None:
        return _enumerate_all(ctx, max_cost, check)
    return _search_cheapest(ctx, max_results, max_cost, check)


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


def _search_cheapest(ctx, max_results: int, max_cost: Optional[float],
                     check: Callable[[int], None]) -> List[Dict[str, Any]]:
    """best-first (A*): извлекает цепочки по возрастанию цены и останавливается на N.

    Эвристика h = _completion_lb (минимальная цена «хвоста») — admissible и
    consistent (по построению lb[i][city] ≤ price(ребро) + lb[i+1][dest]), поэтому
    приоритет f = g + h выдаёт готовые цепочки строго по возрастанию суммарной цены:
    первые N извлечённых = N самых дешёвых. Перебор ограничен фронтиром до N-й цены
    (+ отсечение по max_cost), а не всем деревом — в этом и суть потолка."""
    stops, legs_by_origin, builder, city_info, chain_start, last = ctx
    lb = _completion_lb(stops, legs_by_origin)

    def tail_lb(i, city):
        return 0.0 if i == last else lb[i].get(city)  # None → из city конца не достичь

    heap: List[Any] = []
    seq = 0  # tie-breaker: не даём heapq сравнивать полезную нагрузку

    def push(i, city, arrive_iso, chosen, visited, g):
        nonlocal seq
        tail = tail_lb(i, city)
        if tail is None:  # тупик — до конца не добраться
            return
        f = g + tail
        if max_cost is not None and f > max_cost:  # даже минимальный исход вне бюджета
            return
        heapq.heappush(heap, (f, seq, i, city, arrive_iso, chosen, visited, g))
        seq += 1

    for start in stops[0].codes:
        push(0, start, f"{chain_start}T00:00:00", [], {start}, 0.0)

    results: List[Dict[str, Any]] = []
    while heap and len(results) < max_results:
        check(len(results))
        _, _, i, city, arrive_iso, chosen, visited, g = heapq.heappop(heap)
        if i == last:  # цепочка готова — и она среди самых дешёвых из оставшихся
            results.append(_assemble(stops, chosen, builder, city_info, chain_start, len(results) + 1))
            continue
        for f, dest in _onward_candidates(stops, legs_by_origin, i, city, arrive_iso, visited):
            push(i + 1, dest, arrival_of(f), chosen + [f], visited | {dest}, g + _price_of(f))

    return results  # уже по возрастанию цены (f=g в готовой цепочке, h консистентна)


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
