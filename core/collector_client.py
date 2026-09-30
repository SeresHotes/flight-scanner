"""HTTP-клиент коллектора для планировщика (docs/COLLECTOR.md).

`fetch_series` повторяет контракт core.graphql_api.fetch_series (аргументы,
ответ {"tickets", "pages", "exhausted", "error"} + "cached", progress_cb на
каждую страницу), поэтому планировщик подставляет его как fetch_fn без правок.
Задание отправляется в очередь коллектора с приоритетом приложения, прогресс
читается long-poll'ом, результат забирается один раз.

`http` — объект с .post/.get как у requests.Session (в тестах — TestClient)."""
import json
import os
import threading
import time
from typing import Any, Callable, Dict, List, Optional

import requests

from core import graphql_api
from core.series_arrow import ARROW_MEDIA_TYPE, ipc_to_table, tickets_from_table

DEFAULT_TIMEOUT = 30
POLL_WAIT = 20  # long-poll статуса задания, с


class CollectorError(RuntimeError):
    """Коллектор недоступен или ответил ошибкой."""


def collector_url() -> Optional[str]:
    return os.getenv("COLLECTOR_URL") or None


class CollectorClient:
    def __init__(self, base_url: str, http=None, timeout: float = DEFAULT_TIMEOUT,
                 poll_wait: float = POLL_WAIT, sleep: Callable[[float], None] = time.sleep):
        self.base = base_url.rstrip("/")
        self._http = http
        self._local = threading.local()  # requests.Session не потокобезопасна — своя на поток
        self.timeout = timeout
        self.poll_wait = poll_wait
        self._sleep = sleep

    @property
    def http(self):
        if self._http is not None:
            return self._http
        session = getattr(self._local, "session", None)
        if session is None:
            session = self._local.session = requests.Session()
        return session

    def _post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            r = self.http.post(self.base + path, json=payload, timeout=self.timeout)
        except requests.RequestException as e:
            raise CollectorError(f"коллектор недоступен: {e}") from e
        if r.status_code >= 400:
            raise CollectorError(f"коллектор {path}: HTTP {r.status_code} {r.text[:200]}")
        return r.json()

    def _get_response(self, path: str, params: Optional[Dict[str, Any]] = None,
                      timeout: Optional[float] = None):
        try:
            r = self.http.get(self.base + path, params=params or {}, timeout=timeout or self.timeout)
        except requests.RequestException as e:
            raise CollectorError(f"коллектор недоступен: {e}") from e
        if r.status_code >= 400:
            raise CollectorError(f"коллектор {path}: HTTP {r.status_code} {r.text[:200]}")
        return r

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None,
             timeout: Optional[float] = None) -> Dict[str, Any]:
        return self._get_response(path, params, timeout).json()

    def _result(self, job_id: str) -> Dict[str, Any]:
        """Результат задания: Arrow IPC-поток (разбор по колонкам, core.series_arrow),
        а если коллектор ответил JSON (старая версия) — JSON."""
        r = self._get_response(f"/v1/requests/{job_id}/result", {"format": "arrow"},
                               timeout=max(self.timeout, 120))
        if r.headers.get("content-type", "").startswith(ARROW_MEDIA_TYPE):
            meta = json.loads(r.headers.get("x-series-meta") or "{}")
            return {**meta, "tickets": tickets_from_table(ipc_to_table(r.content))}
        return r.json()

    def health(self) -> Dict[str, Any]:
        return self._get("/v1/health")

    def has_series(self, origin: Optional[str], destination: Optional[str], day: str,
                   params_key: str, pages: Optional[int] = None,
                   ttl_seconds: Optional[float] = None) -> bool:
        params: Dict[str, Any] = {"day": day, "params_key": params_key}
        if origin:
            params["origin"] = origin
        if destination:
            params["destination"] = destination
        if pages is not None:
            params["min_pages"] = pages
        if ttl_seconds is not None:
            params["ttl_seconds"] = ttl_seconds
        return bool(self._get("/v1/series/exists", params).get("exists"))

    def fetch_series(self, origin: Optional[str] = None, destination: Optional[str] = None,
                     day: Optional[str] = None, *, value_min: Optional[int] = None,
                     value_max: Optional[int] = None, direct: Optional[bool] = None,
                     with_baggage: Optional[bool] = None, max_pages: int = graphql_api.MAX_PAGES,
                     progress_cb: Optional[Callable[[int, int], None]] = None,
                     client: str = "app", ttl_seconds: Optional[float] = None,
                     **_ignored) -> Dict[str, Any]:
        job = self._post("/v1/fetch", {
            "origin": origin, "destination": destination, "day": day,
            "value_min": value_min, "value_max": value_max, "direct": direct,
            "with_baggage": with_baggage, "max_pages": max_pages, "client": client,
            "ttl_seconds": ttl_seconds, "wait": 0,
        })
        seen = 0

        def tick(pages: int, tickets: int) -> None:
            nonlocal seen
            while seen < pages:
                seen += 1
                if progress_cb:
                    progress_cb(seen, tickets)

        while job.get("status") not in ("done", "error"):
            tick(int(job.get("pages") or 0), int(job.get("tickets") or 0))
            job = self._get(f"/v1/requests/{job['id']}", {"wait": self.poll_wait},
                            timeout=self.poll_wait + self.timeout)
        result = self._result(job["id"])
        tick(int(result.get("pages") or 0), len(result.get("tickets") or []))
        return {"tickets": result.get("tickets") or [], "pages": int(result.get("pages") or 0),
                "exhausted": bool(result.get("exhausted")), "error": bool(result.get("error")),
                "cached": bool(result.get("cached"))}

    def submit_batch(self, items: List[Dict[str, Any]], client: str = "crawl",
                     ttl_seconds: Optional[float] = None) -> List[str]:
        return list(self._post("/v1/batch", {"items": items, "client": client,
                                             "ttl_seconds": ttl_seconds}).get("ids") or [])

    def coverage(self, params_key: str = "", destination: str = "") -> List[List[Any]]:
        return self._get("/v1/coverage", {"params_key": params_key, "destination": destination},
                         timeout=max(self.timeout, 120)).get("rows") or []

    def cities(self) -> List[Dict[str, Any]]:
        return self._get("/v1/cities").get("cities") or []

    def queue(self) -> List[Dict[str, Any]]:
        return self._get("/v1/queue").get("items") or []

    def stats(self) -> Dict[str, Any]:
        return self._get("/v1/stats")

    def report_crawler_stats(self, payload: Dict[str, Any]) -> None:
        self._post("/v1/stats/crawler", payload)
