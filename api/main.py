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
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel

from core import airports as airports_mod
from core import planner
from core.planquery import PlanQuery
from core import transfer_graph
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
# Сериализует check-active-then-create в /gather и чистку в _active_job_id/_on_job_done:
# sync-эндпоинты FastAPI крутятся в пуле потоков, без лока два параллельных /gather
# завели бы две джобы на один маршрут. RLock — т.к. /gather под локом зовёт _active_job_id.
_jobs_lock = threading.RLock()
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
    # Джобы, не пережившие прошлый рестарт, висят в running — помечаем error,
    # иначе маршрут навсегда считался бы «в процессе сбора».
    stale = hot.fail_stale_jobs(_conn)
    print(f"[startup] импортировано котировок: {imported}, всего в БД: {hot.count_quotes(_conn)}; "
          f"восстановлено собранных маршрутов: {restored}; зависших джоб сброшено: {stale}")


# --------------------------------- health ------------------------------------

@app.get("/api/health")
def health() -> Dict[str, Any]:
    quotes = hot.count_quotes(_conn) if _conn else 0
    ticket_series = hot.count_ticket_series(_conn) if _conn else 0
    return {"status": "ok", "quotes": quotes, "ticket_series": ticket_series,
            "cached_routes": len(_payload_cache)}


# ---------------------------- граф пересадок ---------------------------------

# Граф строится из quotes за доли секунды, но не на каждый запрос: держим в памяти
# и перестраиваем не чаще GRAPH_TTL_SECONDS (сбор дописывает quotes постепенно).
GRAPH_TTL_SECONDS = 300
_graph_cache: Dict[str, Any] = {"graph": None, "built_at": 0.0}
_graph_lock = threading.Lock()


def _graph() -> transfer_graph.TransferGraph:
    import time
    with _graph_lock:
        now = time.monotonic()
        if _graph_cache["graph"] is None or now - _graph_cache["built_at"] > GRAPH_TTL_SECONDS:
            from core import aggregate as agg
            network = agg.load_airport_network() if Path("data/airport_network.json").exists() else {}
            _graph_cache["graph"] = transfer_graph.build_from_db(_conn, network)
            _graph_cache["built_at"] = now
        return _graph_cache["graph"]


@app.get("/api/graph/hidden-city")
def graph_hidden_city(origin: str, via: str, same_day: bool = False, limit: int = 50) -> Dict[str, Any]:
    """Hidden-city: билеты origin→…→via→…→X дешевле прямого origin→via (выходим в via)."""
    res = _graph().hidden_city(origin, via, same_day_only=same_day)
    res["total"] = len(res["tickets"])
    res["tickets"] = res["tickets"][:limit]
    return res


@app.get("/api/graph/transfers")
def graph_transfers(origin: str, destination: str, limit: int = 50) -> Dict[str, Any]:
    """Наблюдённые пересадки по направлению origin→destination."""
    res = _graph().transfers_for(origin, destination)
    res["total"] = len(res["via"])
    res["via"] = res["via"][:limit]
    return res


@app.get("/api/graph/stats")
def graph_stats() -> Dict[str, Any]:
    return _graph().stats()


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
    # force=True: пересобрать заново, даже если данные уже есть (кнопка «свежие данные»).
    force: Optional[bool] = None


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
    # Инвалидируем статический кэш реестра — после форс-сбора свежий payload должен
    # победить (в _build_payload _dynamic_payloads проверяется после _payload_cache).
    _payload_cache.pop(key, None)
    _save_collected(key, payload)  # durable — переживёт перезапуск
    with _jobs_lock:
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
    seconds = requests * worker.SECONDS_PER_REQUEST * worker.ESTIMATE_TIME_FACTOR
    return {"requests": requests, "seconds": round(seconds)}


def _refresh_estimate(req: "SearchRequest") -> Optional[Dict[str, int]]:
    """Оценка форс-пересбора для уже собранного маршрута; None — если собрать нельзя
    (нет дат или диапазон шире предохранителя MAX_REQUESTS)."""
    if not _has_dates(req):
        return None
    params = {"origin": req.origin.upper(), "destination": req.destination.upper(),
              "leg1_dates": list(req.leg1_dates), "leg2_dates": list(req.leg2_dates)}
    est = _estimate(params)
    return est if est["requests"] <= worker.MAX_REQUESTS else None


def _active_job_id(key) -> Optional[str]:
    """Активная (pending/running) джоба по маршруту.
    Лениво чистит запись, если джоба уже завершилась (done/error)."""
    with _jobs_lock:
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
        resp: Dict[str, Any] = {"status": "ok", "data": payload}
        # Оценка форс-пересбора: чтобы на результатах показать кнопку «свежие данные»
        # с числом запросов. None → диапазон дат слишком широк или не задан.
        resp["refresh_estimate"] = _refresh_estimate(req)
        return resp

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

    # При force=True не замыкаем на «данные уже есть» — цель именно пересобрать свежие.
    # Иначе отдаём готовое, только если запрошенные даты уже покрыты; шире окна —
    # это запрос новых данных, идём в сбор недостающего.
    if not req.force:
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

    # Check-and-create под локом: без него два параллельных /gather на один маршрут
    # завели бы две джобы (sync-эндпоинты FastAPI исполняются в пуле потоков).
    with _jobs_lock:
        active = _active_job_id(key)
        if active:
            return {"status": "collecting", "job_id": active}
        job_id = uuid.uuid4().hex[:12]
        hot.create_job(_conn, job_id, params, total=est["requests"],
                       stage=worker.initial_stage("route"))
        _route_jobs[key] = job_id
    _executor.submit(worker.run_collection, hot.DEFAULT_DB, job_id, params, _on_job_done)
    return {"status": "collecting", "job_id": job_id}


def _job_stage(job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Текущий этап сбора (см. worker.StageReporter); None у старых джоб без этапа."""
    raw = job.get("stage_json")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


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
        "stage": _job_stage(job),
    }


# Сколько джоба в running может молчать, прежде чем /jobs/rescue сочтёт её
# зависшей. Загрузка обновляет джобу на каждом запросе (~раз в секунду), стыковка —
# каждые worker.BUILD_FLUSH_SECONDS; минута тишины = воркер застрял.
HUNG_JOB_SECONDS = 60
RESCUED_JOB_ERROR = "Сбор завис и был сброшен. Попробуйте сузить маршрут или даты."


@app.post("/api/jobs/rescue")
def rescue_jobs() -> Dict[str, Any]:
    """«Починить»: сбрасывает зависшие джобы, чтобы освободить однопоточный воркер.

    Executor один на всех (max_workers=1), поэтому одна застрявшая джоба держит в
    «В очереди» все остальные. Помечаем её error и просим воркер бросить её
    (кооперативно, см. worker.request_cancel) — очередь двигается дальше."""
    with _jobs_lock:
        hung = hot.find_hung_jobs(_conn, HUNG_JOB_SECONDS)
        for job_id in hung:
            worker.request_cancel(job_id)
            hot.update_job(_conn, job_id, status="error", error=RESCUED_JOB_ERROR)
    if hung:
        print(f"[rescue] сброшены зависшие джобы: {', '.join(hung)}")
    return {"status": "ok", "rescued": hung}


# ------------------------- планировщик цепочек A→B→C --------------------------

class PlanStop(BaseModel):
    kind: str = "cities"               # 'cities' | 'any'
    codes: List[str] = []              # IATA-коды городов-кандидатов (пусто для 'any')
    window: List[str] = ["", ""]       # [start, end], YYYY-MM-DD; ['',''] у концов


class PlanRequest(BaseModel):
    stops: List[PlanStop]
    max_results: Optional[int] = None  # движковый потолок числа цепочек (самые дешёвые)
    max_cost: Optional[float] = None   # верхняя граница суммарной цены — режет DFS сбора


def _stops_payload(req: "PlanRequest") -> List[Dict[str, Any]]:
    return [{"kind": s.kind, "codes": s.codes, "window": s.window} for s in req.stops]


@app.post("/api/plan/estimate")
def plan_estimate(req: PlanRequest) -> Dict[str, Any]:
    """Оценка объёма сбора цепочки (запросы/время + разбивка по переходам)."""
    stops = planner.parse_stops(_stops_payload(req))
    return planner.estimate_plan(stops)


# Разобранные компактные результаты последних джоб — для страниц /routes (JSON на
# сотни тысяч цепочек разбирать на каждый запрос страницы дорого). Две записи:
# пользователь обычно смотрит одну джобу, вторая — на переход между запросами.
PLAN_RESULT_CACHE_SIZE = 2
PLAN_JOB_TTL_SECONDS = 24 * 3600   # готовая джоба переиспользуется, пока свеж fetch_cache
_plan_results: Dict[str, Dict[str, Any]] = {}
_plan_results_lock = threading.Lock()


class PlanQueryRequest(BaseModel):
    """Единый запрос планировщика (core/planquery.PlanQuery): скелет + все фильтры.
    Поля фильтров необязательны — отсутствующие открыты."""
    stops: List[PlanStop]
    cities: Optional[List[Dict[str, Any]]] = None
    legs: Optional[List[Dict[str, Any]]] = None
    tripLength: Optional[List[Optional[int]]] = None
    maxCost: Optional[float] = None
    maxResults: Optional[int] = None


def _start_plan_job(query: PlanQuery) -> Dict[str, Any]:
    """Общий запуск сбора: проверки, дедуп по хэшу запроса, постановка в очередь."""
    stops = planner.parse_stops(query.stops)
    if len(stops) < 2:
        return {"status": "invalid", "message": "Нужно минимум две остановки."}

    # max_results обязателен: без него движок ушёл бы в безлимитный перебор (сотни
    # тысяч цепочек → сотни МБ → зависание/почти-OOM). None шлёт устаревший
    # закешированный фронт или прямой вызов API — отклоняем, а не молча ограничиваем.
    if not planner.is_valid_max_results(query.max_results):
        return {"status": "invalid",
                "message": "Не задан лимит числа маршрутов. Обновите страницу (Ctrl/Cmd+Shift+R) — клиент устарел."}

    est = planner.estimate_plan(stops)
    if est["requests"] == 0:
        return {"status": "invalid", "message": "Задайте окна дат для остановок."}
    if est["requests"] > planner.MAX_REQUESTS:
        return {"status": "too_wide",
                "message": f"Слишком широкие окна (~{est['requests']} запросов). Сузьте диапазоны.",
                "estimate": est}

    key = query.key()
    existing = hot.find_job_by_key(_conn, key, PLAN_JOB_TTL_SECONDS)
    if existing:
        return {"status": "collecting" if existing["status"] != "done" else "done",
                "job_id": existing["id"], "total": existing["total"], "mode": query.mode(),
                "reused": True}

    job_id = uuid.uuid4().hex[:12]
    payload = query.as_dict()
    hot.create_job(_conn, job_id, {"kind": "plan", **payload,
                                   "stops": payload["stops"], "max_results": query.max_results,
                                   "max_cost": query.max_cost},
                   total=est["requests"], stage=worker.initial_stage("plan"), query_key=key)
    _executor.submit(worker.run_plan_collection, hot.DEFAULT_DB, job_id, payload["stops"],
                     query.max_results, query.max_cost, payload)
    return {"status": "collecting", "job_id": job_id, "total": est["requests"], "mode": query.mode()}


@app.post("/api/plan/gather")
def plan_gather(req: PlanRequest) -> Dict[str, Any]:
    """Запуск сбора по одному скелету (фильтры открыты). Прогресс/результат — /plan/jobs/{id}."""
    return _start_plan_job(PlanQuery.from_dict({"stops": _stops_payload(req),
                                                "maxResults": req.max_results,
                                                "maxCost": req.max_cost}))


@app.post("/api/plan/run")
def plan_run(req: PlanQueryRequest) -> Dict[str, Any]:
    """Запуск сбора по единому запросу с фильтрами (PlanQuery). Одинаковый запрос в
    пределах суток переиспользует готовую/идущую джобу (reused=true). mode —
    какой экран открывать: combos (наборы городов) или routes."""
    try:
        query = PlanQuery.from_dict({"stops": [{"kind": s.kind, "codes": s.codes, "window": s.window}
                                               for s in req.stops],
                                     "cities": req.cities, "legs": req.legs,
                                     "tripLength": req.tripLength,
                                     "maxCost": req.maxCost, "maxResults": req.maxResults})
    except ValueError as e:
        return {"status": "invalid", "message": f"Некорректный фильтр: {e}"}
    return _start_plan_job(query)


def _plan_result(job_id: str) -> Optional[Dict[str, Any]]:
    """Разобранный результат готовой джобы (с ленивым индексом наборов), из кэша."""
    with _plan_results_lock:
        entry = _plan_results.get(job_id)
    if entry is not None:
        return entry
    job = hot.get_job(_conn, job_id)
    if not job or job["status"] != "done" or not job["result_json"]:
        return None
    entry = {"result": json.loads(job["result_json"]), "index": None}
    with _plan_results_lock:
        while len(_plan_results) >= PLAN_RESULT_CACHE_SIZE:
            _plan_results.pop(next(iter(_plan_results)))
        _plan_results[job_id] = entry
    return entry


COMBO_SORTS = {"price": lambda c: (c["minPrice"], c["codes"]),
               "count": lambda c: (-c["count"], c["minPrice"]),
               "transfers": lambda c: (c["transfersAtMin"], c["minPrice"])}


@app.get("/api/plan/jobs/{job_id}/combos")
def plan_job_combos(job_id: str, offset: int = 0, limit: int = 100,
                    sort: str = "price") -> Dict[str, Any]:
    """Страница наборов городов готовой джобы (core/overview): {codes, minPrice,
    transfersAtMin, minTransfers, count}; sort — price | count | transfers.
    cities — имена/флаги кодов страницы."""
    entry = _plan_result(job_id)
    if entry is None:
        return {"status": "not_ready"}
    overview = entry["result"].get("combos")
    if not overview:
        return {"status": "ok", "total": 0, "totalCount": 0, "offset": offset, "limit": limit,
                "items": [], "cities": {}}
    combos = overview["combos"]
    key = COMBO_SORTS.get(sort)
    if key is not None and sort != "price":
        combos = sorted(combos, key=key)
    limit = max(1, min(limit, 500))
    offset = max(0, offset)
    page = combos[offset:offset + limit]
    codes = {c for it in page for c in it["codes"]}
    return {"status": "ok", "total": len(combos), "totalCount": overview["totalCount"],
            "offset": offset, "limit": limit, "items": page,
            "cities": {c: overview["cities"].get(c, [c, ""]) for c in codes}}


@app.get("/api/plan/jobs/{job_id}/routes")
def plan_job_routes(job_id: str, offset: int = 0, limit: int = 50,
                    combos: Optional[str] = None) -> Dict[str, Any]:
    """Страница маршрутов готовой джобы (по возрастанию цены), с полными сегментами
    (багаж, пересадки, hidden-city). combos — наборы городов через запятую
    (`MOW-IST-ICN,MOW-DXB-ICN`): только цепочки этих наборов."""
    entry = _plan_result(job_id)
    if entry is None:
        return {"status": "not_ready"}
    limit = max(1, min(limit, 200))
    offset = max(0, offset)
    wanted = [c.strip().upper() for c in combos.split(",") if c.strip()] if combos else None
    if wanted and entry["index"] is None:
        entry["index"] = planner.combo_index(entry["result"])
    page = planner.routes_page(entry["result"], offset, limit, wanted, entry["index"])
    return {"status": "ok", "count": entry["result"]["count"], **page}


@app.get("/api/plan/jobs/{job_id}")
def plan_job_status(job_id: str) -> Dict[str, Any]:
    """Прогресс джобы; по завершении — сводка (summary: цепочек, наборов, всего
    вариантов). Сами данные — страницами: …/combos и …/routes."""
    job = hot.get_job(_conn, job_id)
    if not job:
        return {"status": "not_found"}
    out: Dict[str, Any] = {
        "status": job["status"],
        "progress": job["progress"],
        "total": job["total"],
        "error": job["error"],
        "stage": _job_stage(job),
    }
    if job["status"] == "done":
        entry = _plan_result(job_id)
        if entry is not None:
            res = entry["result"]
            combos = res.get("combos") or {}
            out["summary"] = {"count": res.get("count", 0),
                              "combos": len(combos.get("combos") or []),
                              "totalCount": combos.get("totalCount", 0)}
    return out
