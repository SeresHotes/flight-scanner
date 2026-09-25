"""Фоновый сбор недостающих данных (Фаза 2).

Джоба реально ходит в Travelpayouts через core.collector, обновляет прогресс в
таблице jobs, кладёт котировки в SQLite + Parquet и собирает контракт trip_builder.
Запускается в отдельном потоке (collector синхронный, с rate-limit sleep).
"""
import json
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

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


def _make_cached_fetch(conn):
    """Обёртка над collector.fetch_flights с TTL-кэшем по (origin, destination, day).

    На попадании возвращает сохранённый ответ без обращения к API (и без rate-limit
    паузы). Сбойные ответы (error=True) и запросы без даты не кэшируем."""
    def fetch(origin=None, destination=None, departure_at=None, currency="RUB",
              unique=True, limit=1000, allow_indirect=False):
        direct = 0 if allow_indirect else 1
        if departure_at:
            cached = hot.fetch_cache_get(conn, origin, destination, departure_at,
                                         direct, FETCH_CACHE_TTL_SECONDS)
            if cached is not None:
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
        hot.update_job(conn, job_id, status="running", total=total, progress=0)

        counter = {"n": 0}

        def cb():
            counter["n"] += 1
            hot.update_job(conn, job_id, progress=counter["n"])

        collected = collector.collect_route(
            origin, destination, params["leg1_dates"], params["leg2_dates"],
            stop_days=STOP_DAYS, progress_cb=cb, fetch_fn=_make_cached_fetch(conn),
        )

        network = agg.load_airport_network()
        cfg = make_route_config(origin, destination, stop_days=STOP_DAYS)
        payload = build_payload(cfg, network, collected["plain"], collected["there"], collected["back"])

        now = datetime.now().isoformat()
        payload["meta"]["collected_at"] = now

        # Сохраняем котировки в горячее хранилище + дозаписываем в озеро.
        flights = []
        for ds in ("plain", "there", "back"):
            flights += collected[ds]["leg1_flights"] + collected[ds]["leg2_flights"]
        rows = hot.flights_to_quotes(flights, now)
        hot.upsert_quotes(conn, rows)
        if lake.available():
            try:
                lake.append_quotes(rows, part_id=job_id)
            except Exception as e:  # озеро не критично для serving
                print(f"[worker] lake append failed: {e}")

        on_done((origin, destination), payload)
        hot.update_job(conn, job_id, status="done",
                       result_json=json.dumps({"trips": payload["meta"]["counts"]["total"]}))
        print(f"[worker] job {job_id} done: {payload['meta']['counts']['total']} плеч")
    except Exception as e:
        hot.update_job(conn, job_id, status="error", error=str(e))
        print(f"[worker] job {job_id} error: {e}")
    finally:
        conn.close()


# ------------------------- планировщик цепочек A→B→C --------------------------

def run_plan_collection(db_path: str, job_id: str, raw_stops: List[Dict[str, Any]],
                        max_results: Optional[int] = None,
                        max_cost: Optional[float] = None) -> None:
    """Сбор данных под планировщик цепочек: обходит переходы, стыкует цепочки,
    кладёт готовые Itinerary в jobs.result_json. Котировки — в SQLite + озеро.

    max_results/max_cost — движковые границы стыковки (см. planner.build_itineraries):
    режут перебор по бюджету цены и числу самых дешёвых цепочек."""
    # Жёсткий потолок числа цепочек — единственный choke point перед стыковкой в
    # проде. Защищает от безлимитного перебора (None шлёт старый закешированный фронт
    # или прямой вызов API): без него на плотном графе строятся сотни тысяч Itinerary
    # → сотни МБ ответа → зависание браузера и почти-OOM на 4-ГБ VM.
    max_results = planner.clamp_max_results(max_results)

    conn = hot.connect(db_path)
    try:
        stops = planner.parse_stops(raw_stops)
        total = planner.request_count(stops)
        hot.update_job(conn, job_id, status="running", total=total, progress=0)

        counter = {"n": 0}

        def cb():
            counter["n"] += 1
            hot.update_job(conn, job_id, progress=counter["n"])

        collected = planner.collect_plan(stops, progress_cb=cb, fetch_fn=_make_cached_fetch(conn))

        from core.trip_builder import make_city_lookup
        city_info = make_city_lookup(agg.load_airport_network())
        itineraries = planner.build_itineraries(stops, collected, city_info=city_info,
                                                max_results=max_results, max_cost=max_cost)

        now = datetime.now().isoformat()
        flights: List[Dict[str, Any]] = []
        for leg_flights in collected.values():
            flights += leg_flights
        rows = hot.flights_to_quotes(flights, now)
        hot.upsert_quotes(conn, rows)
        if lake.available():
            try:
                lake.append_quotes(rows, part_id=job_id)
            except Exception as e:  # озеро не критично для serving
                print(f"[worker] lake append failed: {e}")

        hot.update_job(conn, job_id, status="done",
                       result_json=json.dumps({"itineraries": itineraries}, ensure_ascii=False))
        print(f"[worker] plan job {job_id} done: {len(itineraries)} цепочек")
    except Exception as e:
        hot.update_job(conn, job_id, status="error", error=str(e))
        print(f"[worker] plan job {job_id} error: {e}")
    finally:
        conn.close()
