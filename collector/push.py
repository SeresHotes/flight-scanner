"""Пуш файлов озера в склад билетов (`tickets/`, docs/TICKETS.md): сразу после записи
серии в озеро те же байты Parquet уходят `POST {TICKETS_URL}/v1/files?key=&created_at=`.

Отдельный поток с ограниченной очередью: воркер коллектора не ждёт склада; если склад
недоступен или очередь полна, файл просто не пушится — его подхватит сверка склада с
индексом озера (`GET /v1/lake/files`, `GET /v1/lake/file`). Пуш — ускоритель, источник
истины — озеро."""
import queue
import threading
from datetime import datetime, timezone
from typing import Optional

import requests

QUEUE_SIZE = 200
TIMEOUT = 120


class TicketsPusher:
    def __init__(self, base_url: str, session: Optional[requests.Session] = None,
                 queue_size: int = QUEUE_SIZE, timeout: float = TIMEOUT):
        self.base = base_url.rstrip("/")
        self._session = session or requests.Session()
        self._q: "queue.Queue[tuple]" = queue.Queue(maxsize=queue_size)
        self._timeout = timeout
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.pushed = 0
        self.failed = 0
        self.dropped = 0

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="tickets-push", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=timeout)
            self._thread = None

    def push(self, key: str, data: bytes, observed: datetime) -> bool:
        """Поставить файл в очередь пуша. False — очередь полна (файл дойдёт сверкой)."""
        try:
            self._q.put_nowait((key, data, observed))
            return True
        except queue.Full:
            self.dropped += 1
            return False

    def send(self, key: str, data: bytes, observed: datetime) -> None:
        created = observed.astimezone(timezone.utc).isoformat(timespec="seconds")
        r = self._session.post(f"{self.base}/v1/files", params={"key": key, "created_at": created},
                               data=data, headers={"Content-Type": "application/octet-stream"},
                               timeout=self._timeout)
        r.raise_for_status()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                key, data, observed = self._q.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self.send(key, data, observed)
                self.pushed += 1
            except Exception as e:  # склад недоступен — не наша проблема, догонит сверкой
                self.failed += 1
                if self.failed <= 3 or self.failed % 100 == 0:
                    print(f"[collector] пуш в склад {key}: {e}")

    def stats(self) -> dict:
        return {"url": self.base, "pushed": self.pushed, "failed": self.failed, "dropped": self.dropped,
                "queued": self._q.qsize()}
