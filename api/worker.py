"""Фоновая джоба планировщика: серии билетов от коллектора (core.collector_client;
без COLLECTOR_URL — прямой GraphQL с кэшем серий), стыковка цепочек (core.planner),
наборы городов (core.overview), прогресс в таблице jobs, котировки — в SQLite.
Запускается в потоке однопоточного executor.
"""
import json
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from core import planner
from core.network import load_airport_network
from core.segments import make_city_lookup
from storage import hot

# Свежесть кэша серий (направление, день, коридор): источник (Travelpayouts
# Data API) сам отдаёт кэш цен с задержкой ~суток, поэтому чаще перезапрашивать
# бессмысленно — те же цифры, впустую сожжённые запросы к API.
FETCH_CACHE_TTL_SECONDS = 24 * 3600
# Сколько серий джоба тянет у коллектора одновременно. Попадания в озеро квоту
# источника не тратят, промахи коллектор сам ставит в очередь под лимит; прямой
# GraphQL (без коллектора) — по одной: лимит 60/мин и общее SQLite-соединение кэша.
COLLECTOR_FETCH_WORKERS = 8

# Как часто стыковка пишет прогресс в jobs. Заодно держит свежим updated_at — иначе
# долгая стыковка выглядела бы для /api/jobs/rescue как зависшая джоба.
BUILD_FLUSH_SECONDS = 0.5


# Отмена зависших джоб. Поток Python снаружи не убить, поэтому отмена кооперативная:
# /api/jobs/rescue кладёт id сюда, а воркер проверяет флаг на каждом запросе к API
# (StageReporter) и на каждом шаге стыковки (planner.build_itineraries).
_cancel_requests: Set[str] = set()
_cancel_lock = threading.Lock()


class JobCancelled(Exception):
    """Джобу сбросили извне — воркер бросает сбор, статус уже выставлен сбросившим."""


def request_cancel(job_id: str) -> None:
    with _cancel_lock:
        _cancel_requests.add(job_id)


def is_cancel_requested(job_id: str) -> bool:
    with _cancel_lock:
        return job_id in _cancel_requests


def _forget_cancel(job_id: str) -> None:
    with _cancel_lock:
        _cancel_requests.discard(job_id)


# Этапы джобы для UI (ключи стабильны — фронт рисует по ним степпер). Этапа
# «Сохранение» нет: котировки пишутся уже после done.
_PLAN_STAGES = [("queued", "В очереди"), ("fetch", "Загрузка рейсов"),
                ("build", "Стыковка цепочек"), ("combos", "Наборы городов")]


def initial_stage(kind: str) -> Dict[str, Any]:
    """Этап свежесозданной джобы (ждёт своей очереди в однопоточном executor)."""
    stages = _PLAN_STAGES
    return {"key": "queued", "stages": [{"key": k, "label": lbl} for k, lbl in stages],
            "step": None, "cached": 0, "flights": None, "build": None}


class StageReporter:
    """Пишет в jobs текущий этап сбора: этап, шаг загрузки (переход i из N и
    сколько его запросов уже сделано), сколько ответов взято из кэша.

    steps — [{label, requests}] в порядке обхода сборщиком."""

    def __init__(self, conn, job_id: str, kind: str, steps: List[Dict[str, Any]]):
        self.conn = conn
        self.job_id = job_id
        self.steps = steps
        self.state = initial_stage(kind)
        self.progress = 0
        # tick/cache_hit зовутся из потоков параллельного сбора (collect_plan workers)
        self._lock = threading.Lock()

    def _flush(self, **fields) -> None:
        if is_cancel_requested(self.job_id):  # не перетираем статус сброшенной джобы
            raise JobCancelled()
        hot.update_job(self.conn, self.job_id,
                       stage_json=json.dumps(self.state, ensure_ascii=False), **fields)

    def stage(self, key: str, **fields) -> None:
        self.state["key"] = key
        self.state["step"] = None
        self._flush(**fields)

    def step(self, index: int) -> None:
        info = self.steps[index] if index < len(self.steps) else {"label": "", "requests": 0}
        self.state["step"] = {"index": index, "count": len(self.steps), "label": info["label"],
                              "done": 0, "total": info["requests"]}
        self._flush()

    def cache_hit(self) -> None:
        with self._lock:
            self.state["cached"] += 1  # попадёт в БД вместе со следующим tick()

    def tick(self) -> None:
        with self._lock:
            self.progress += 1
            if self.state["step"] is not None:
                self.state["step"]["done"] += 1
            self._flush(progress=self.progress)

    def flights(self, n: int) -> None:
        self.state["flights"] = n

    def build_progress(self, limit: Optional[int]) -> Callable[[int, int], None]:
        """on_progress для planner.build_itineraries: «найдено X из limit цепочек,
        перебрано M вариантов». Шагов перебора — десятки тысяч в секунду, поэтому
        пишем в БД не чаще BUILD_FLUSH_SECONDS (и сразу на первом шаге)."""
        last_flush = [0.0]

        def report(found: int, explored: int) -> None:
            self.state["build"] = {"found": found, "limit": limit, "explored": explored}
            now = time.monotonic()
            if now - last_flush[0] >= BUILD_FLUSH_SECONDS:
                last_flush[0] = now
                self._flush()
        return report


def make_cached_ticket_fetch(conn, on_cache_hit: Optional[Callable[[], None]] = None,
                             ttl_seconds: float = FETCH_CACHE_TTL_SECONDS,
                             fetch_fn: Optional[Callable[..., Dict[str, Any]]] = None):
    """Обёртка над graphql_api.fetch_series с TTL-кэшем серий (hot.ticket_cache).

    Ключ — направление, день и «прочие» параметры (коридор цен, direct, багаж).
    На попадании возвращает сохранённую серию без обращения к источнику. Обрезанная
    серия переиспользуется, только если в ней не меньше страниц, чем просят сейчас.
    Серии с error=True не кэшируем. fetch_fn — подмена fetch_series в тестах."""
    from core import graphql_api
    real_fetch = fetch_fn or graphql_api.fetch_series

    def fetch(origin=None, destination=None, day=None, *, value_min=None, value_max=None,
              direct=None, with_baggage=None, max_pages=graphql_api.MAX_PAGES, **kw):
        key = graphql_api.params_key(value_min, value_max, direct, with_baggage)
        cached = hot.ticket_cache_get(conn, origin, destination, day, key, ttl_seconds,
                                      min_pages=max_pages)
        if cached is not None:
            if on_cache_hit:
                on_cache_hit()
            return {**cached, "error": False, "cached": True}
        series = real_fetch(origin, destination, day, value_min=value_min,
                            value_max=value_max, direct=direct, with_baggage=with_baggage,
                            max_pages=max_pages, **kw)
        if not series.get("error"):
            hot.ticket_cache_put(conn, origin, destination, day, key, series)
        return series
    return fetch


def make_collector_ticket_fetch(client, on_cache_hit: Optional[Callable[[], None]] = None,
                                ttl_seconds: float = FETCH_CACHE_TTL_SECONDS):
    """fetch_fn планировщика через коллектор (core.collector_client): серия идёт в
    очередь с приоритетом приложения, свежая серия из озера приходит с cached=True."""
    def fetch(origin=None, destination=None, day=None, **kw):
        series = client.fetch_series(origin, destination, day, ttl_seconds=ttl_seconds, **kw)
        if series.get("cached") and on_cache_hit:
            on_cache_hit()
        return series
    return fetch


def make_ticket_fetch(conn, on_cache_hit: Optional[Callable[[], None]] = None):
    """Источник серий для джобы и сколько серий тянуть одновременно: коллектор, если
    задан COLLECTOR_URL (прод), — параллельно; иначе прямой GraphQL с кэшем серий в
    SQLite (локальный запуск без коллектора) — по одной."""
    from core.collector_client import CollectorClient, collector_url
    url = collector_url()
    if url:
        return make_collector_ticket_fetch(CollectorClient(url), on_cache_hit), COLLECTOR_FETCH_WORKERS
    return make_cached_ticket_fetch(conn, on_cache_hit), 1


def _save_quotes(conn, flights: List[Dict[str, Any]], observed_at: str, job_id: str) -> None:
    """Котировки — в горячее хранилище SQLite (карта аэропорт → город, статистика).
    Локального Parquet-озера котировок больше нет: история наблюдений цены живёт в
    озере серий коллектора (docs/COLLECTOR.md); прежнее data/lake на VM набрало
    миллион мелких файлов и только тормозило обслуживание тома."""
    rows = hot.flights_to_quotes(flights, observed_at)
    hot.upsert_quotes(conn, rows)


# ------------------------- планировщик цепочек A→B→C --------------------------

def build_view(stops: List[planner.Stop], collected: Dict[int, List[Dict[str, Any]]], pq,
               city_info=None, on_progress: Optional[Callable[[int, int], None]] = None,
               should_stop: Optional[Callable[[], bool]] = None,
               on_stage: Optional[Callable[[str], None]] = None) -> Dict[str, Any]:
    """Результат джобы под фильтры pq («вид»): компактные цепочки (стыковка) +
    наборы городов (core/overview). Зовут воркер сразу после сбора (рейсы ещё в
    памяти) и API, когда те же рейсы смотрят с другими фильтрами."""
    from core.overview import build_overview
    city_info = city_info or make_city_lookup()
    # Компактный результат (сегменты один раз + плоские массивы индексов): на
    # 100k+ цепочек словари Itinerary и их JSON съедали гигабайты → OOM api.
    result = planner.build_itineraries_compact(
        stops, collected, city_info=city_info, max_results=pq.max_results, max_cost=pq.max_cost,
        should_stop=should_stop, on_progress=on_progress, query=pq)
    if on_stage:
        on_stage("combos")
    # Наборы городов — все варианты под фильтры, без границ max_results/max_cost.
    result["combos"] = build_overview(stops, collected, pq, city_info=city_info)
    return result


def run_plan_collection(db_path: str, job_id: str, raw_stops: List[Dict[str, Any]],
                        max_results: Optional[int] = None,
                        max_cost: Optional[float] = None,
                        query: Optional[Dict[str, Any]] = None,
                        on_view: Optional[Callable[[Any, Dict[str, Any]], None]] = None) -> None:
    """Джоба = сбор рейсов по остановкам запроса (серии «направление × день»), рейсы —
    в plan_flights. Пока они в памяти, сразу стыкуем под фильтры запроса и отдаём
    вид в on_view(pq, result) (кэш API) до выставления done. Котировки — в SQLite.

    query — полный PlanQuery (core/planquery); без него собирается из raw_stops/
    max_results/max_cost (фильтры открыты). Бюджет на сбор не влияет — это фильтр."""
    from core.planquery import PlanQuery
    conn = hot.connect(db_path)
    try:
        pq = PlanQuery.from_dict(query or {"stops": raw_stops, "maxResults": max_results,
                                           "maxCost": max_cost})
        stops = planner.parse_stops(pq.stops)
        total = planner.request_count(stops)
        city_info = make_city_lookup()
        steps = [{"label": f"{leg['fromLabel']} → {leg['toLabel']}", "requests": leg["requests"]}
                 for leg in planner.estimate_plan(stops, city_info)["legs"]]
        rep = StageReporter(conn, job_id, "plan", steps)
        rep.stage("fetch", status="running", total=total, progress=0)

        fetch_fn, workers = make_ticket_fetch(conn, rep.cache_hit)
        collected = planner.collect_plan(stops, progress_cb=rep.tick, fetch_fn=fetch_fn,
                                         leg_cb=rep.step, airport_city=hot.airport_city_map(conn),
                                         workers=workers)
        rep.flights(sum(len(v) for v in collected.values()))
        # Рейсы — колонками в файл джобы: из них строятся виды под другие фильтры и
        # маршруты наборов. Те же колонки — и для первой стыковки.
        from core.flightcols import FlightCols
        table = FlightCols.from_collected(collected)
        hot.put_plan_flights(conn, job_id, table)
        rep.stage("build")
        result = build_view(stops, table, pq, city_info=city_info,
                            on_progress=rep.build_progress(pq.max_results),
                            should_stop=lambda: is_cancel_requested(job_id), on_stage=rep.stage)

        if is_cancel_requested(job_id):  # не перетираем статус сброшенной джобы
            raise JobCancelled()
        if on_view:
            on_view(pq, result)
        count = result["count"]
        del result
        hot.update_job(conn, job_id, status="done")
        print(f"[worker] plan job {job_id} done: {count} цепочек")

        # Котировки планировщику не нужны, они копят карту аэропорт → город и
        # статистику. Поэтому пишем их уже после done, чтобы пользователь не ждал, и
        # сбой тут не портит готовую джобу. Виртуальные рейсы hidden-city — не
        # котировки (такого билета A→B нет), их не пишем.
        flights: List[Dict[str, Any]] = []
        for leg_flights in collected.values():
            flights += [f for f in leg_flights if not f.get("hidden_city")]
        try:
            _save_quotes(conn, flights, datetime.now().isoformat(), job_id)
        except Exception as e:
            print(f"[worker] plan job {job_id}: save quotes failed: {e}")
    except (JobCancelled, planner.SearchAborted):
        print(f"[worker] plan job {job_id} сброшена как зависшая")
    except Exception as e:
        hot.update_job(conn, job_id, status="error", error=str(e))
        print(f"[worker] plan job {job_id} error: {e}")
    finally:
        _forget_cancel(job_id)
        conn.close()
