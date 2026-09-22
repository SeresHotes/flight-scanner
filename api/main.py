"""FastAPI-приложение Flight Scanner.

Эндпоинты (Фаза 1):
- GET  /api/health              — статус + размер кэша котировок
- GET  /api/airports?q=&limit=  — автокомплит точек A/B
- POST /api/search              — контракт {status:'ok', data:{meta,trips}} для
                                  собранных маршрутов; {status:'needs_backend'} иначе
"""
import glob
import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel

from core import airports as airports_mod
from core.routes import get_route_config, get_route_keys
from core.trip_builder import build_from_config
from storage import hot
from storage import catalog
from api import worker

app = FastAPI(title="Flight Scanner API", version="0.1.0")
# CORS: фронт может ходить с другого origin (vite dev/preview, прямой :8000) —
# без этого браузер блокирует запросы (симптом: Referrer-Policy strict-origin-when-cross-origin).
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"https?://(localhost|127\.0\.0\.1)(:\d+)?",
    allow_methods=["*"],
    allow_headers=["*"],
)
# Ответ /search — крупный JSON (мегабайты), gzip критичен.
app.add_middleware(GZipMiddleware, minimum_size=1024)

# Процесс-локальные кэши: собранные контракты (реестр + дозагруженные джобами) и
# активные джобы по маршруту (для идемпотентности /search).
_payload_cache: Dict[tuple, Dict[str, Any]] = {}
_dynamic_payloads: Dict[tuple, Dict[str, Any]] = {}
_route_jobs: Dict[tuple, str] = {}
_executor = ThreadPoolExecutor(max_workers=1)  # сериализуем сбор (rate-limit к API)
_conn = None

# Собранные джобами маршруты сохраняем на диск, чтобы пережить перезапуск сервера.
COLLECTED_DIR = os.getenv("COLLECTED_DIR", "data/collected")


def _collected_path(key: Tuple[str, str]) -> Path:
    return Path(COLLECTED_DIR) / f"{key[0]}-{key[1]}.json"


def _save_collected(key: Tuple[str, str], payload: Dict[str, Any]) -> None:
    Path(COLLECTED_DIR).mkdir(parents=True, exist_ok=True)
    with open(_collected_path(key), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)


def _load_collected() -> int:
    for path in glob.glob(str(Path(COLLECTED_DIR) / "*.json")):
        try:
            payload = json.load(open(path, encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        m = payload.get("meta", {})
        o, d = m.get("origin_code"), m.get("destination_code")
        if o and d:
            _dynamic_payloads[(o.upper(), d.upper())] = payload
    return len(_dynamic_payloads)


@app.on_event("startup")
def _startup() -> None:
    global _conn
    _conn = hot.connect()
    hot.init_db(_conn)
    # Импорт существующих выгрузок коллектора в горячее хранилище (идемпотентно).
    imported = hot.import_data_dir(_conn, "data")
    restored = _load_collected()  # ранее собранные маршруты (durable)
    print(f"[startup] импортировано котировок: {imported}, всего в БД: {hot.count_quotes(_conn)}; "
          f"восстановлено собранных маршрутов: {restored}")


# --------------------------------- health ------------------------------------

@app.get("/api/health")
def health() -> Dict[str, Any]:
    quotes = hot.count_quotes(_conn) if _conn else 0
    return {"status": "ok", "quotes": quotes, "cached_routes": len(_payload_cache)}


# -------------------------------- airports -----------------------------------

@app.get("/api/airports")
def airports(q: str = "", limit: int = 10) -> Dict[str, Any]:
    return {"airports": airports_mod.search_airports(q, limit=limit)}


# --------------------------------- routes ------------------------------------

def _available_entry(o: str, d: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    m = payload["meta"]
    return {
        "origin": o, "destination": d,
        "title": m["title"], "trips": m["counts"]["total"],
        "region_label": m["region_label"], "region_flag": m["region_flag"],
        "there_price_range": m["there_price_range"],
        "back_price_range": m["back_price_range"],
        "dep_there_range": m["dep_there_range"],
        "dep_back_range": m["dep_back_range"],
        "min_stay": m["min_stay"],
        "stopover_days_range": m["stopover_days_range"],
        "collected_at": m.get("collected_at"),
    }


@app.get("/api/routes")
def routes() -> Dict[str, Any]:
    """Что уже доступно: собранные маршруты (реестр + дозагруженные) + агрегат хранилища."""
    available = []
    seen = set()
    # Маршруты из реестра + все, что дозагрузили джобами (durable).
    for key in list(get_route_keys()) + list(_dynamic_payloads.keys()):
        if key in seen:
            continue
        seen.add(key)
        payload = _build_payload(key[0], key[1])
        if payload is not None:
            available.append(_available_entry(key[0], key[1], payload))
    cov = hot.route_coverage(_conn)
    stored = {
        "total_quotes": cov["total_quotes"],
        "distinct_routes": cov["distinct_routes"],
        "collections": catalog.scan_collections("data"),
    }
    return {"available": available, "stored": stored}


# --------------------------------- search ------------------------------------

class SearchRequest(BaseModel):
    origin: str
    destination: str
    leg1_dates: Optional[List[str]] = None
    leg2_dates: Optional[List[str]] = None
    min_stay: Optional[int] = None


def _build_payload(origin: str, destination: str) -> Optional[Dict[str, Any]]:
    key = (origin.upper(), destination.upper())
    if key in _payload_cache:
        return _payload_cache[key]
    if key in _dynamic_payloads:  # маршрут, дозагруженный джобой
        return _dynamic_payloads[key]
    cfg = get_route_config(origin, destination)
    if cfg is None:
        return None
    payload = build_from_config(cfg)
    # Дата фактического сбора данных по маршруту (из горячего хранилища).
    payload["meta"]["collected_at"] = hot.route_last_observed(_conn, key[0], key[1])
    _payload_cache[key] = payload
    return payload


def _on_job_done(key: Tuple[str, str], payload: Dict[str, Any]) -> None:
    _dynamic_payloads[key] = payload
    _save_collected(key, payload)  # durable — переживёт перезапуск
    # Сбрасываем реестровый кэш: _build_payload берёт _payload_cache раньше
    # _dynamic_payloads, иначе после пересбора отдавался бы старый payload.
    _payload_cache.pop(key, None)
    _route_jobs.pop(key, None)


def _has_dates(req: "SearchRequest") -> bool:
    return bool(req.leg1_dates and len(req.leg1_dates) == 2 and all(req.leg1_dates)
                and req.leg2_dates and len(req.leg2_dates) == 2 and all(req.leg2_dates))


def _within(requested: List[str], covered: List[str]) -> bool:
    """Лежит ли запрошенное окно [start, end] внутри собранного диапазона.

    ISO-даты (YYYY-MM-DD) сравниваются лексикографически = хронологически.
    Пустой собранный диапазон (по плечу ничего нет) не покрывает ничего.
    """
    c_start, c_end = covered[0], covered[1]
    if not c_start or not c_end:
        return False
    return requested[0] >= c_start and requested[1] <= c_end


def _covers_request(payload: Dict[str, Any], req: "SearchRequest") -> bool:
    """Покрывают ли уже собранные окна вылета запрошенный диапазон дат.

    Без дат в запросе считаем покрытым: клик по готовому маршруту подставляет
    даты ровно из его диапазона. Если даты заданы и выходят за собранные окна —
    это запрос НОВЫХ данных, старый payload отдавать нельзя.
    """
    if not _has_dates(req):
        return True
    m = payload["meta"]
    there = m.get("dep_there_range") or ["", ""]
    back = m.get("dep_back_range") or ["", ""]
    return _within(list(req.leg1_dates), there) and _within(list(req.leg2_dates), back)


def _estimate(params: Dict[str, Any]) -> Dict[str, int]:
    requests = worker.estimate_requests(params)
    return {"requests": requests, "seconds": round(requests * worker.SECONDS_PER_REQUEST)}


def _active_job_id(key) -> Optional[str]:
    active = _route_jobs.get(key)
    if not active:
        return None
    job = hot.get_job(_conn, active)
    if job and job["status"] in ("pending", "running"):
        return active
    _route_jobs.pop(key, None)  # завершилась — больше не активна
    return None


@app.post("/api/search")
def search(req: SearchRequest) -> Dict[str, Any]:
    """Оценивает, что нужно, но НИЧЕГО не собирает сам (сбор — только через /collect).

    - есть данные → {status:'ok', data}
    - идёт сбор → {status:'collecting', job_id}
    - не собран, есть даты → {status:'needs_collection', estimate:{requests, seconds}}
    - нет дат / слишком широко → {status:'needs_backend', message}
    """
    key = (req.origin.upper(), req.destination.upper())

    # Отдаём собранное только если запрошенные даты уже покрыты. Запрос дат шире
    # собранного окна — это запрос НОВЫХ данных, старый payload не показываем.
    payload = _build_payload(req.origin, req.destination)
    if payload is not None and _covers_request(payload, req):
        return {"status": "ok", "data": payload}

    active = _active_job_id(key)
    if active:
        return {"status": "collecting", "job_id": active}

    if not _has_dates(req):
        return {"status": "needs_backend", "origin": req.origin, "destination": req.destination,
                "message": "Укажите даты вылета для обоих плеч — тогда можно собрать данные."}

    params = {"origin": key[0], "destination": key[1],
              "leg1_dates": list(req.leg1_dates), "leg2_dates": list(req.leg2_dates)}
    est = _estimate(params)
    if est["requests"] > worker.MAX_REQUESTS:
        return {"status": "needs_backend", "origin": req.origin, "destination": req.destination,
                "message": f"Слишком широкий диапазон дат (~{est['requests']} запросов). Сузьте окна плеч.",
                "estimate": est}

    return {"status": "needs_collection", "origin": req.origin, "destination": req.destination,
            "estimate": est}


@app.post("/api/gather")
def gather(req: SearchRequest) -> Dict[str, Any]:
    """Явный запуск сбора недостающих данных (после подтверждения пользователем).

    Путь НЕ содержит слова 'collect' намеренно: блокировщики рекламы/трекеров
    режут /collect как аналитический эндпоинт (ERR_BLOCKED_BY_CLIENT).
    """
    key = (req.origin.upper(), req.destination.upper())

    # Уже покрыто — собирать нечего, возвращаем готовое. Иначе (даты шире
    # собранного) идём в сбор недостающего окна.
    payload = _build_payload(req.origin, req.destination)
    if payload is not None and _covers_request(payload, req):
        return {"status": "ok", "data": payload}

    active = _active_job_id(key)
    if active:
        return {"status": "collecting", "job_id": active}

    if not _has_dates(req):
        return {"status": "needs_backend", "origin": req.origin, "destination": req.destination,
                "message": "Укажите даты вылета для обоих плеч."}

    params = {"origin": key[0], "destination": key[1],
              "leg1_dates": list(req.leg1_dates), "leg2_dates": list(req.leg2_dates)}
    est = _estimate(params)
    if est["requests"] > worker.MAX_REQUESTS:
        return {"status": "needs_backend", "origin": req.origin, "destination": req.destination,
                "message": f"Слишком широкий диапазон дат (~{est['requests']} запросов). Сузьте окна плеч."}

    job_id = uuid.uuid4().hex[:12]
    hot.create_job(_conn, job_id, params, total=est["requests"])
    _route_jobs[key] = job_id
    _executor.submit(worker.run_collection, hot.DEFAULT_DB, job_id, params, _on_job_done)
    return {"status": "collecting", "job_id": job_id}


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str) -> Dict[str, Any]:
    job = hot.get_job(_conn, job_id)
    if not job:
        return {"status": "not_found"}
    return {
        "status": job["status"],
        "progress": job["progress"],
        "total": job["total"],
        "error": job["error"],
    }
