"""FastAPI-приложение Flight Scanner — планировщик v2 (docs/PLANNER_V2.md).

- GET  /api/health                         — статус, котировки, серии в кэше
- GET  /api/airports?q=&limit=             — автокомплит городов/аэропортов
- POST /api/plan/estimate                  — оценка объёма сбора
- POST /api/plan/run                       — запуск джобы по PlanQuery (дедуп по хэшу)
- GET  /api/plan/jobs/{id}                 — прогресс + сводка
- GET  /api/plan/jobs/{id}/combos|routes   — страницы наборов городов / маршрутов
- POST /api/jobs/rescue                    — сброс зависших джоб
"""
import json
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel

from core import airports as airports_mod
from core import planner
from core.planquery import PlanQuery
from storage import hot
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
# Страницы маршрутов с полными сегментами — десятки КБ, gzip полезен.
app.add_middleware(GZipMiddleware, minimum_size=1024)

_jobs_lock = threading.RLock()  # /jobs/rescue: поиск и сброс зависших джоб атомарно
_executor = ThreadPoolExecutor(max_workers=1)  # сериализуем сбор (rate-limit к API)
_conn = None

@app.on_event("startup")
def _startup() -> None:
    global _conn
    _conn = hot.connect()
    hot.init_db(_conn)
    # Джобы, не пережившие прошлый рестарт, висят в running — помечаем error.
    stale = hot.fail_stale_jobs(_conn)
    print(f"[startup] котировок в БД: {hot.count_quotes(_conn)}; зависших джоб сброшено: {stale}")


# --------------------------------- health ------------------------------------

@app.get("/api/health")
def health() -> Dict[str, Any]:
    quotes = hot.count_quotes(_conn) if _conn else 0
    ticket_series = hot.count_ticket_series(_conn) if _conn else 0
    return {"status": "ok", "quotes": quotes, "ticket_series": ticket_series}


# -------------------------------- airports -----------------------------------

@app.get("/api/airports")
def airports(q: str = "", limit: int = 10) -> Dict[str, Any]:
    return {"airports": airports_mod.search_airports(q, limit=limit)}


def _job_stage(job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Текущий этап сбора (см. worker.StageReporter); None у старых джоб без этапа."""
    raw = job.get("stage_json")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


# Сколько джоба в running может молчать, прежде чем /jobs/rescue сочтёт её
# зависшей. Загрузка обновляет джобу на каждой странице (~раз в секунду), стыковка —
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
    radiusKm: int = 0                  # можно улететь дальше из соседнего города (core/nearby)


class PlanRequest(BaseModel):
    stops: List[PlanStop]
    max_results: Optional[int] = None  # движковый потолок числа цепочек (самые дешёвые)
    max_cost: Optional[float] = None   # верхняя граница суммарной цены — режет DFS сбора


def _stops_payload(req: "PlanRequest") -> List[Dict[str, Any]]:
    return [{"kind": s.kind, "codes": s.codes, "window": s.window, "radiusKm": s.radiusKm}
            for s in req.stops]




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


def _cache_probe():
    """is_cached для planner.estimate_plan: серия уже лежит в ticket_cache (TTL воркера)."""
    conn = _conn

    def is_cached(origin, dest, day, key, pages) -> bool:
        return hot.ticket_cache_has(conn, origin, dest, day, key, worker.FETCH_CACHE_TTL_SECONDS,
                                    min_pages=pages)
    return is_cached


def _estimate(query: PlanQuery) -> Dict[str, Any]:
    stops = planner.parse_stops(query.stops)
    return planner.estimate_plan(stops, max_cost=query.max_cost, is_cached=_cache_probe())


def _query_from_request(req: "PlanQueryRequest") -> PlanQuery:
    return PlanQuery.from_dict({"stops": [{"kind": s.kind, "codes": s.codes, "window": s.window,
                                           "radiusKm": s.radiusKm} for s in req.stops],
                                "cities": req.cities, "legs": req.legs, "tripLength": req.tripLength,
                                "maxCost": req.maxCost, "maxResults": req.maxResults})


@app.post("/api/plan/estimate")
def plan_estimate(req: PlanQueryRequest) -> Dict[str, Any]:
    """Оценка объёма сбора по запросу: всего страниц, сколько уже в кэше серий,
    сколько холодных (пойдут в источник) и время по холодным."""
    try:
        return _estimate(_query_from_request(req))
    except ValueError as e:
        return {"status": "invalid", "message": f"Некорректный фильтр: {e}"}


def _start_plan_job(query: PlanQuery) -> Dict[str, Any]:
    """Общий запуск сбора: проверки, дедуп по хэшу запроса, постановка в очередь."""
    stops = planner.parse_stops(query.stops)
    if len(stops) < 2:
        return {"status": "invalid", "message": "Нужно минимум две остановки."}

    # Лимит цепочек — внутренний: фронт его не задаёт, джоба строит DEFAULT_MAX_RESULTS
    # самых дешёвых; маршруты выбранных наборов городов строятся отдельно по требованию.
    if query.max_results is None:
        query.max_results = planner.DEFAULT_MAX_RESULTS
    if not planner.is_valid_max_results(query.max_results):
        return {"status": "invalid", "message": f"Лимит маршрутов вне диапазона 1…{planner.MAX_RESULTS}."}

    est = _estimate(query)
    if est["requests"] == 0:
        return {"status": "invalid", "message": "Задайте окна дат для остановок."}
    # Предохранитель — только по холодным страницам: то, что уже в кэше серий,
    # источник не нагружает, а повтор широкого запроса иначе отказывал бы зря.
    if est["cold"] > planner.MAX_REQUESTS:
        return {"status": "too_wide",
                "message": f"Слишком широкие окна: ~{est['cold']} страниц надо загрузить из источника "
                           f"(лимит {planner.MAX_REQUESTS}, в кэше уже {est['cached']}). "
                           "Сузьте диапазоны дат или задайте бюджет поездки.",
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
        query = _query_from_request(req)
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
    if not wanted:
        page = planner.routes_page(entry["result"], offset, limit)
        return {"status": "ok", "count": entry["result"]["count"], **page}
    items = _combo_routes(job_id, entry, wanted)
    if items is None:
        return {"status": "not_ready"}
    return {"status": "ok", "count": len(items), "total": len(items), "offset": offset, "limit": limit,
            "items": items[offset:offset + limit]}


def _combo_routes(job_id: str, entry: Dict[str, Any], wanted: List[str]) -> Optional[List[Dict[str, Any]]]:
    """Маршруты выбранных наборов — по требованию из сохранённых рейсов джобы
    (planner.build_combo_routes), кэш по набору ключей внутри записи результата."""
    key = ",".join(sorted(set(wanted)))
    cache = entry.setdefault("combo_routes", {})
    if key in cache:
        return cache[key]
    job = hot.get_job(_conn, job_id)
    collected = hot.get_plan_flights(_conn, job_id)
    if not job or collected is None:
        return None
    params = json.loads(job["params_json"] or "{}")
    query = PlanQuery.from_dict(params)
    stops = planner.parse_stops(query.stops)
    combos = [c.split("-") for c in sorted(set(wanted))]
    items = planner.build_combo_routes(stops, collected, combos, query=query)
    if len(cache) >= 8:
        cache.pop(next(iter(cache)))
    cache[key] = items
    return items


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
