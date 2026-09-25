"""Фоновый сбор недостающих данных (Фаза 2).

Джоба реально ходит в Travelpayouts через core.collector, обновляет прогресс в
таблице jobs, кладёт котировки в SQLite + Parquet и собирает контракт trip_builder.
Запускается в отдельном потоке (collector синхронный, с rate-limit sleep).
"""
import json
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from core import aggregate as agg
from core import collector
from core import planner
from core.routes import make_route_config
from core.trip_builder import build_payload
from storage import hot, lake

STOP_DAYS = (2, 7)
MAX_REQUESTS = 150  # предохранитель от слишком широких диапазонов
SECONDS_PER_REQUEST = 0.65  # 0.5с rate-limit sleep + ~сеть — для оценки времени сбора
ESTIMATE_TIME_FACTOR = 2  # запас пессимизма в показанной оценке длительности сбора

# Свежесть кэша под-запросов (origin, destination, day): источник (Travelpayouts
# Data API) сам отдаёт кэш цен с задержкой ~суток, поэтому чаще перезапрашивать
# бессмысленно — те же цифры, впустую сожжённые запросы к API.
FETCH_CACHE_TTL_SECONDS = 24 * 3600

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


# Этапы сбора для UI. Ключи стабильны (фронт по ним рисует степпер), подписи —
# под вид джобы: у маршрута A→B после загрузки собираются варианты, у планировщика
# стыкуются цепочки.
_ROUTE_STAGES = [("queued", "В очереди"), ("fetch", "Загрузка рейсов"),
                 ("build", "Сборка вариантов"), ("save", "Сохранение")]
# У планировщика нет этапа «Сохранение»: котировки пишутся уже после done.
_PLAN_STAGES = [("queued", "В очереди"), ("fetch", "Загрузка рейсов"),
                ("build", "Стыковка цепочек")]


def initial_stage(kind: str) -> Dict[str, Any]:
    """Этап свежесозданной джобы (ждёт своей очереди в однопоточном executor)."""
    stages = _PLAN_STAGES if kind == "plan" else _ROUTE_STAGES
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
        self.state["cached"] += 1  # попадёт в БД вместе со следующим tick()

    def tick(self) -> None:
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


def _make_cached_fetch(conn, on_cache_hit: Optional[Callable[[], None]] = None):
    """Обёртка над collector.fetch_flights с TTL-кэшем по (origin, destination, day).

    На попадании возвращает сохранённый ответ без обращения к API (и без rate-limit
    паузы). Сбойные ответы (error=True) и запросы без даты не кэшируем.
    on_cache_hit() — для счётчика «из кэша» в прогрессе джобы."""
    def fetch(origin=None, destination=None, departure_at=None, currency="RUB",
              unique=True, limit=1000, allow_indirect=False):
        direct = 0 if allow_indirect else 1
        if departure_at:
            cached = hot.fetch_cache_get(conn, origin, destination, departure_at,
                                         direct, FETCH_CACHE_TTL_SECONDS)
            if cached is not None:
                if on_cache_hit:
                    on_cache_hit()
                return {"data": cached}
        result = collector.fetch_flights(
            origin=origin, destination=destination, departure_at=departure_at,
            currency=currency, unique=unique, limit=limit, allow_indirect=allow_indirect,
        )
        if departure_at and not result.get("error"):
            hot.fetch_cache_put(conn, origin, destination, departure_at, direct,
                                result.get("data", []))
        return result
    return fetch


def _save_quotes(conn, flights: List[Dict[str, Any]], observed_at: str, job_id: str) -> None:
    """Котировки — в горячее хранилище SQLite + дозапись в озеро (если подключено)."""
    rows = hot.flights_to_quotes(flights, observed_at)
    hot.upsert_quotes(conn, rows)
    if lake.available():
        try:
            lake.append_quotes(rows, part_id=job_id)
        except Exception as e:  # озеро не критично для serving
            print(f"[worker] lake append failed: {e}")


def estimate_requests(params: Dict[str, Any]) -> int:
    return collector.plan_request_count(
        params["origin"], params["destination"],
        params["leg1_dates"], params["leg2_dates"], STOP_DAYS,
    )


def run_collection(db_path: str, job_id: str, params: Dict[str, Any],
                   on_done: Callable[[Tuple[str, str], Dict[str, Any]], None]) -> None:
    conn = hot.connect(db_path)
    origin = params["origin"].upper()
    destination = params["destination"].upper()
    try:
        total = estimate_requests(params)
        steps = collector.route_steps(origin, destination, params["leg1_dates"],
                                      params["leg2_dates"], STOP_DAYS)
        rep = StageReporter(conn, job_id, "route", steps)
        rep.stage("fetch", status="running", total=total, progress=0)

        collected = collector.collect_route(
            origin, destination, params["leg1_dates"], params["leg2_dates"],
            stop_days=STOP_DAYS, progress_cb=rep.tick,
            fetch_fn=_make_cached_fetch(conn, rep.cache_hit), step_cb=rep.step,
        )
        rep.flights(sum(len(collected[ds][leg]) for ds in collected for leg in collected[ds]))
        rep.stage("build")

        network = agg.load_airport_network()
        cfg = make_route_config(origin, destination, stop_days=STOP_DAYS)
        payload = build_payload(cfg, network, collected["plain"], collected["there"], collected["back"])

        now = datetime.now().isoformat()
        payload["meta"]["collected_at"] = now

        # Сохраняем котировки в горячее хранилище + дозаписываем в озеро.
        rep.stage("save")
        flights = []
        for ds in ("plain", "there", "back"):
            flights += collected[ds]["leg1_flights"] + collected[ds]["leg2_flights"]
        _save_quotes(conn, flights, now, job_id)

        on_done((origin, destination), payload)
        hot.update_job(conn, job_id, status="done",
                       result_json=json.dumps({"trips": payload["meta"]["counts"]["total"]}))
        print(f"[worker] job {job_id} done: {payload['meta']['counts']['total']} плеч")
    except JobCancelled:
        print(f"[worker] job {job_id} сброшена как зависшая")
    except Exception as e:
        hot.update_job(conn, job_id, status="error", error=str(e))
        print(f"[worker] job {job_id} error: {e}")
    finally:
        _forget_cancel(job_id)
        conn.close()


# ------------------------- планировщик цепочек A→B→C --------------------------

def run_plan_collection(db_path: str, job_id: str, raw_stops: List[Dict[str, Any]],
                        max_results: Optional[int] = None,
                        max_cost: Optional[float] = None) -> None:
    """Сбор данных под планировщик цепочек: обходит переходы, стыкует цепочки,
    кладёт готовые Itinerary в jobs.result_json. Котировки — в SQLite + озеро.

    max_results/max_cost — движковые границы стыковки (см. planner.build_itineraries):
    режут перебор по бюджету цены и числу самых дешёвых цепочек. max_results=None
    (безлимит) отклоняется на уровне API (см. plan_gather), сюда не доходит."""
    conn = hot.connect(db_path)
    try:
        stops = planner.parse_stops(raw_stops)
        total = planner.request_count(stops)
        from core.trip_builder import make_city_lookup
        city_info = make_city_lookup(agg.load_airport_network())
        steps = [{"label": f"{leg['fromLabel']} → {leg['toLabel']}", "requests": leg["requests"]}
                 for leg in planner.estimate_plan(stops, city_info)["legs"]]
        rep = StageReporter(conn, job_id, "plan", steps)
        rep.stage("fetch", status="running", total=total, progress=0)

        collected = planner.collect_plan(stops, progress_cb=rep.tick,
                                         fetch_fn=_make_cached_fetch(conn, rep.cache_hit),
                                         leg_cb=rep.step)
        rep.flights(sum(len(v) for v in collected.values()))
        rep.stage("build")

        # Компактный результат (сегменты один раз + плоские массивы индексов): на
        # 100k+ цепочек словари Itinerary и их JSON съедали гигабайты → OOM api.
        result = planner.build_itineraries_compact(
            stops, collected, city_info=city_info, max_results=max_results, max_cost=max_cost,
            should_stop=lambda: is_cancel_requested(job_id),
            on_progress=rep.build_progress(max_results))

        if is_cancel_requested(job_id):  # не перетираем статус сброшенной джобы
            raise JobCancelled()
        count = result["count"]
        result_json = planner.compact_to_json(result)
        del result
        hot.update_job(conn, job_id, status="done", result_json=result_json)
        del result_json
        print(f"[worker] plan job {job_id} done: {count} цепочек")

        # Котировки планировщику не нужны (результат — result_json, повторы — fetch_cache),
        # они копят статистику /api/routes и историю цен в озере. Поэтому пишем их уже
        # после done, чтобы пользователь не ждал, и сбой тут не портит готовую джобу.
        flights: List[Dict[str, Any]] = []
        for leg_flights in collected.values():
            flights += leg_flights
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
