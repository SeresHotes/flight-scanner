"""HTTP-сервис коллектора (внутренний, порт 8001; наружу не публикуется).

- GET  /v1/health                        — статус, очередь, озеро
- POST /v1/fetch                         — одна серия; ответ — задание (id, status, pages)
- POST /v1/batch                         — список серий (фоновый сборщик)
- GET  /v1/requests/{id}?wait=           — статус задания (long-poll до wait секунд)
- GET  /v1/requests/{id}/result          — серия: tickets, pages, exhausted, error, cached
- GET  /v1/series/exists                 — есть ли свежая серия (оценка объёма в планировщике)
- GET  /v1/coverage?params_key=&destination=  — покрытие X→ANY для сборщика
- GET  /v1/cities                        — известные города
- GET  /v1/queue                         — серии в очереди/в работе (сборщик не подаёт их повторно)
- GET  /v1/stats, POST /v1/stats/crawler — счётчики / сводка сборщика (метрики)

Запуск: uvicorn collector.main:app --host 0.0.0.0 --port 8001
"""
import json
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field

from collector.config import Settings
from collector.engine import Engine, NotReady, SeriesRequest
from collector.index import Index
from collector.metrics import Metrics
from collector.store import make_store
from core.series_arrow import ARROW_MEDIA_TYPE, table_to_ipc


class FetchItem(BaseModel):
    origin: Optional[str] = None
    destination: Optional[str] = None
    day: str
    day_to: Optional[str] = None   # окно дат (включительно): ответ раскладывается по дням
    value_min: Optional[int] = None
    value_max: Optional[int] = None
    direct: Optional[bool] = None
    with_baggage: Optional[bool] = None
    max_pages: Optional[int] = None


class FetchRequest(FetchItem):
    client: str = "app"
    ttl_seconds: Optional[float] = None
    wait: float = Field(0.0, ge=0, le=60)


class BatchRequest(BaseModel):
    items: List[FetchItem]
    client: str = "crawl"
    ttl_seconds: Optional[float] = None


def build_engine(settings: Optional[Settings] = None) -> Engine:
    settings = settings or Settings()
    return Engine(settings, make_store(settings), Index(settings.db_path))


def create_app(engine: Optional[Engine] = None, settings: Optional[Settings] = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        eng = engine or build_engine(settings)
        app.state.engine = eng
        eng.start()
        metrics = None
        if eng.settings.metrics_enabled:
            metrics = Metrics(eng, interval=eng.settings.metrics_interval_seconds,
                              flush_rows=eng.settings.metrics_flush_rows,
                              coverage_interval=eng.settings.coverage_snapshot_seconds)
            metrics.start()
        app.state.metrics = metrics
        print(f"[collector] старт: серий в индексе {eng.index.count()}, "
              f"озеро {eng.index.files_bytes() / 2**30:.2f} ГБ, {eng.limiter.per_minute:.0f} запр./мин, "
              f"метрики {'вкл' if metrics else 'выкл'}")
        try:
            yield
        finally:
            if metrics:
                metrics.stop()
            eng.stop()

    app = FastAPI(title="Flight Scanner Collector", version="0.1.0", lifespan=lifespan)

    def eng() -> Engine:
        return app.state.engine

    def _request(item: FetchItem) -> SeriesRequest:
        s = eng().settings
        try:
            return SeriesRequest(item.origin, item.destination, item.day, value_min=item.value_min,
                                 value_max=item.value_max, direct=item.direct,
                                 with_baggage=item.with_baggage,
                                 max_pages=min(item.max_pages or s.max_pages, s.max_pages),
                                 day_to=item.day_to)
        except ValueError as e:
            raise HTTPException(400, str(e))

    @app.get("/v1/health")
    def health() -> Dict[str, Any]:
        e = eng()
        st = e.queue_stats()
        return {"status": "ok", "queued_app": st["queued_app"], "queued_crawl": st["queued_crawl"],
                "running": st["running"], "series": e.index.count(),
                "lake_bytes": e.index.files_bytes(), "rate_per_minute": e.limiter.per_minute}

    @app.post("/v1/fetch")
    def fetch(req: FetchRequest) -> Dict[str, Any]:
        e = eng()
        try:
            job = e.submit(_request(req), client=req.client, ttl_seconds=req.ttl_seconds)
        except ValueError as ex:
            raise HTTPException(400, str(ex))
        if req.wait and not job.done:
            e.wait(job, timeout=req.wait)
        return job.public()

    @app.post("/v1/batch")
    def batch(req: BatchRequest) -> Dict[str, Any]:
        e = eng()
        ids = []
        for item in req.items:
            try:
                ids.append(e.submit(_request(item), client=req.client, ttl_seconds=req.ttl_seconds).id)
            except ValueError as ex:
                raise HTTPException(400, str(ex))
        return {"ids": ids, "count": len(ids)}

    @app.get("/v1/requests/{job_id}")
    def request_status(job_id: str, wait: float = 0.0) -> Dict[str, Any]:
        e = eng()
        job = e.get_job(job_id)
        if job is None:
            raise HTTPException(404, "задание не найдено (истёк срок хранения?)")
        if wait and not job.done:
            e.wait(job, timeout=min(wait, 60.0))
        return job.public()

    @app.get("/v1/requests/{job_id}/result")
    def request_result(job_id: str, format: str = "json") -> Response:
        """Билеты серии. format=arrow — Arrow IPC-поток в схеме озера (метаданные —
        JSON в заголовке X-Series-Meta): без разбора в словари и JSON, так планировщик
        берёт серию на порядок быстрее. По умолчанию — JSON {"tickets", …}, собранный
        json.dumps напрямую (jsonable_encoder на тысячах билетов стоил ~0.6 с)."""
        e = eng()
        job = e.get_job(job_id)
        if job is None:
            raise HTTPException(404, "задание не найдено (истёк срок хранения?)")
        try:
            res = e.result(job, as_table=(format == "arrow"))
        except NotReady:
            raise HTTPException(409, "задание ещё выполняется")
        if format == "arrow":
            table = res.pop("table")
            return Response(content=table_to_ipc(table), media_type=ARROW_MEDIA_TYPE,
                            headers={"X-Series-Meta": json.dumps(res)})
        return Response(content=json.dumps(res, ensure_ascii=False), media_type="application/json")

    @app.get("/v1/series/exists")
    def series_exists(day: str, origin: Optional[str] = None, destination: Optional[str] = None,
                      params_key: str = "", min_pages: Optional[int] = None,
                      ttl_seconds: Optional[float] = None) -> Dict[str, Any]:
        if not origin and not destination:
            raise HTTPException(400, "нужен хотя бы один из origin/destination")
        return {"exists": eng().exists(origin, destination, day, params_key, min_pages, ttl_seconds)}

    @app.get("/v1/coverage")
    def coverage(params_key: str = "", destination: str = "") -> Dict[str, Any]:
        rows = eng().index.coverage(params_key=params_key, destination=destination)
        return {"columns": ["origin", "day", "fetched_at", "pages", "tickets", "exhausted", "error"],
                "rows": rows}

    @app.get("/v1/cities")
    def cities() -> Dict[str, Any]:
        return {"cities": eng().index.cities()}

    @app.get("/v1/queue")
    def queue() -> Dict[str, Any]:
        items = eng().queue_keys()
        return {"items": items, "count": len(items)}

    @app.get("/v1/stats")
    def stats() -> Dict[str, Any]:
        return eng().stats()

    @app.post("/v1/stats/crawler")
    def crawler_stats(payload: Dict[str, Any]) -> Dict[str, Any]:
        eng().crawler_stats = payload
        return {"ok": True}

    return app


app = create_app()
