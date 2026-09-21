"""Фоновый сбор недостающих данных (Фаза 2).

Джоба реально ходит в Travelpayouts через core.collector, обновляет прогресс в
таблице jobs, кладёт котировки в SQLite + Parquet и собирает контракт trip_builder.
Запускается в отдельном потоке (collector синхронный, с rate-limit sleep).
"""
import json
from datetime import datetime
from typing import Any, Callable, Dict, Tuple

from core import aggregate as agg
from core import collector
from core.routes import make_route_config
from core.trip_builder import build_payload
from storage import hot, lake

STOP_DAYS = (2, 7)
MAX_REQUESTS = 150  # предохранитель от слишком широких диапазонов
SECONDS_PER_REQUEST = 0.65  # 0.5с rate-limit sleep + ~сеть — для оценки времени сбора


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
            stop_days=STOP_DAYS, progress_cb=cb,
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
