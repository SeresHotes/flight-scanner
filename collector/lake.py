"""Parquet-озеро серий: буфер → файл `tickets/observed=YYYY-MM-DD/part-<время>.parquet`.

Внутри файла одна серия = одна row group, поэтому серию можно прочитать одним
range-запросом к S3 (footer + её row group), не скачивая файл целиком. Билет
кладём плоскими колонками (для аналитики) плюс вложенные части JSON-строками
(legs, transfer_points, chain); `from_row` восстанавливает ровно тот словарь,
что отдаёт core.graphql_api.normalize_ticket."""
import io
import json
import secrets
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

TICKETS_PREFIX = "tickets"

SCHEMA = pa.schema([
    ("series_id", pa.int64()),
    ("observed_at", pa.string()),
    ("search_origin", pa.string()),
    ("search_destination", pa.string()),
    ("search_date", pa.string()),
    ("origin", pa.string()),
    ("destination", pa.string()),
    ("origin_airport", pa.string()),
    ("destination_airport", pa.string()),
    ("departure_at", pa.string()),
    ("arrival_at", pa.string()),
    ("duration", pa.int32()),
    ("duration_to", pa.int32()),
    ("transfers", pa.int16()),
    ("airline", pa.string()),
    ("flight_number", pa.string()),
    ("price", pa.float64()),
    ("currency", pa.string()),
    ("link", pa.string()),
    ("chain_json", pa.string()),
    ("legs_json", pa.string()),
    ("transfer_points_json", pa.string()),
    ("baggage_code", pa.string()),
    ("baggage_known", pa.bool_()),
    ("baggage_included", pa.bool_()),
    ("baggage_pieces", pa.int16()),
    ("baggage_kg", pa.int16()),
    ("source", pa.string()),
])

_PLAIN = ["search_origin", "search_destination", "search_date", "origin", "destination",
          "origin_airport", "destination_airport", "departure_at", "arrival_at", "duration",
          "duration_to", "transfers", "airline", "flight_number", "price", "currency", "link",
          "baggage_code", "source"]


def to_row(ticket: Dict[str, Any], series_id: int, observed_at: str) -> Dict[str, Any]:
    bag = ticket.get("baggage") or {}
    row = {k: ticket.get(k) for k in _PLAIN}
    row.update({
        "series_id": int(series_id),
        "observed_at": observed_at,
        "chain_json": json.dumps(ticket.get("chain") or [], ensure_ascii=False),
        "legs_json": json.dumps(ticket.get("legs") or [], ensure_ascii=False),
        "transfer_points_json": json.dumps(ticket.get("transfer_points") or [], ensure_ascii=False),
        "baggage_known": bool(bag.get("known")),
        "baggage_included": bool(bag.get("included")),
        "baggage_pieces": bag.get("pieces"),
        "baggage_kg": bag.get("kg"),
    })
    if row["price"] is not None:
        row["price"] = float(row["price"])
    return row


def from_row(row: Dict[str, Any]) -> Dict[str, Any]:
    t = {k: row.get(k) for k in _PLAIN}
    if t["transfers"] is not None:
        t["transfers"] = int(t["transfers"])
    t["chain"] = json.loads(row.get("chain_json") or "[]")
    t["legs"] = json.loads(row.get("legs_json") or "[]")
    t["transfer_points"] = json.loads(row.get("transfer_points_json") or "[]")
    t["baggage"] = {"known": bool(row.get("baggage_known")), "included": bool(row.get("baggage_included")),
                    "pieces": row.get("baggage_pieces"), "kg": row.get("baggage_kg")}
    # Порядок ключей как у normalize_ticket — для стабильных сравнений в тестах.
    order = ["origin", "destination", "origin_airport", "destination_airport", "departure_at",
             "arrival_at", "duration", "duration_to", "transfers", "airline", "flight_number",
             "price", "currency", "link", "chain", "legs", "transfer_points", "baggage",
             "baggage_code", "source", "search_origin", "search_destination", "search_date"]
    return {k: t[k] for k in order}


def series_table(tickets: List[Dict[str, Any]], series_id: int, observed_at: str) -> pa.Table:
    rows = [to_row(t, series_id, observed_at) for t in tickets]
    return pa.Table.from_pylist(rows, schema=SCHEMA)


def read_series(source, row_group: int) -> List[Dict[str, Any]]:
    """Билеты одной row group файла (source — pyarrow NativeFile или путь)."""
    pf = pq.ParquetFile(source)
    return [from_row(r) for r in pf.read_row_group(row_group).to_pylist()]


def part_key(now: datetime) -> str:
    stamp = now.strftime("%Y%m%dT%H%M%S") + f"{now.microsecond // 1000:03d}Z"
    return f"{TICKETS_PREFIX}/observed={now:%Y-%m-%d}/part-{stamp}-{secrets.token_hex(2)}.parquet"


class LakeWriter:
    """Буфер серий → файл озера. flush по числу билетов или по времени; серии, ещё
    не сброшенные, читаются из буфера (`pending`)."""

    def __init__(self, store, index, flush_tickets: int = 25_000, flush_seconds: float = 300,
                 clock=time.monotonic, now=lambda: datetime.now(timezone.utc)):
        self.store = store
        self.index = index
        self.flush_tickets = flush_tickets
        self.flush_seconds = flush_seconds
        self._clock = clock
        self._now = now
        self._lock = threading.Lock()
        self._buffer: List[Tuple[int, List[Dict[str, Any]], str]] = []
        self._buffered = 0
        self._since = clock()
        self.files_written = 0

    def add(self, series_id: int, tickets: List[Dict[str, Any]], observed_at: str) -> None:
        with self._lock:
            # Повторная выборка той же серии до сброса — старая копия не нужна.
            self._buffer = [b for b in self._buffer if b[0] != series_id]
            self._buffer.append((series_id, tickets, observed_at))
            self._buffered = sum(len(b[1]) for b in self._buffer)
            if len(self._buffer) == 1:
                self._since = self._clock()

    def pending(self, series_id: int) -> Optional[List[Dict[str, Any]]]:
        with self._lock:
            for sid, tickets, _ in self._buffer:
                if sid == series_id:
                    return tickets
        return None

    def due(self) -> bool:
        with self._lock:
            if not self._buffer:
                return False
            return (self._buffered >= self.flush_tickets
                    or self._clock() - self._since >= self.flush_seconds)

    def flush(self, force: bool = False) -> Optional[str]:
        """Пишет буфер одним файлом; возвращает ключ файла или None, если нечего/рано."""
        if not force and not self.due():
            return None
        with self._lock:
            batch, self._buffer, self._buffered = self._buffer, [], 0
        if not batch:
            return None
        sink = io.BytesIO()
        placements: List[Tuple[int, int]] = []
        writer = pq.ParquetWriter(sink, SCHEMA, compression="zstd")
        try:
            rg = 0
            for sid, tickets, observed_at in batch:
                if not tickets:
                    continue  # пустая серия читается как [] без файла
                writer.write_table(series_table(tickets, sid, observed_at),
                                   row_group_size=max(len(tickets), 1))
                placements.append((sid, rg))
                rg += 1
        finally:
            writer.close()
        if not placements:
            return None
        data = sink.getvalue()
        now = self._now()
        key = part_key(now)
        self.store.put_bytes(key, data)
        self.index.attach_file(key, f"{now:%Y-%m-%d}", len(data), placements)
        self.files_written += 1
        return key

    def read(self, file_key: str, row_group: int) -> List[Dict[str, Any]]:
        with self.store.open_input_file(file_key) as f:
            return read_series(f, row_group)
