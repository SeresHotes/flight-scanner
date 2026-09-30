"""Parquet-озеро серий: один файл на выборку (день или окно дат), путь = день загрузки + город:
`tickets/fetched=<день загрузки>/origin=<город>/<A>-<B>__<дни вылета>[__<параметры>]__<HH-MM-SS>Z.parquet`.

Озеро читаемо без индекса (индекс в SQLite — ускоритель: свежесть, покрытие,
объём для ретеншна). Билет кладём плоскими колонками (для аналитики) плюс
вложенные части JSON-строками (legs, transfer_points, chain); `from_row`
восстанавливает ровно тот словарь, что отдаёт core.graphql_api.normalize_ticket."""
import io
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from core.series_arrow import tickets_from_table

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


def read_series_table(source, row_group: int) -> pa.Table:
    """Одна row group файла Arrow-таблицей (source — pyarrow NativeFile или путь)."""
    return pq.ParquetFile(source).read_row_group(row_group)


def read_series(source, row_group: int) -> List[Dict[str, Any]]:
    """Билеты одной row group файла — словарями from_row (разбор по колонкам)."""
    return tickets_from_table(read_series_table(source, row_group))


def series_file_key(origin: Optional[str], destination: Optional[str], day: str,
                    params_key: str, observed: datetime, day_to: Optional[str] = None) -> str:
    """Путь файла = день загрузки + город запроса, в имени — направление, дни вылета
    (день или окно `A..B`), параметры и время загрузки:
    tickets/fetched=<UTC-день загрузки>/origin=<город|ANY>/<A>-<B>__<дни>[__<параметры>]__<HH-MM-SS>Z.parquet
    Файлы только дописываются (повторная выборка — новый файл, история цен), поэтому
    единственное, что у файла однозначно, — когда и по какому городу его загрузили;
    день вылета — колонка (row group на день). Старое уходит целыми папками fetched=."""
    o = (origin or "ANY").upper()
    d = (destination or "ANY").upper()
    days = f"{day}..{day_to}" if day_to and day_to != day else day
    params = f"__{params_key.replace('&', ',')}" if params_key else ""
    at = observed.astimezone(timezone.utc)
    return (f"{TICKETS_PREFIX}/fetched={at:%Y-%m-%d}/origin={o}/"
            f"{o}-{d}__{days}{params}__{at:%H-%M-%S}Z.parquet")


_DAY = r"\d{4}-\d{2}-\d{2}"
_KEY_RE = re.compile(
    r"^" + TICKETS_PREFIX + r"/fetched=(?P<date>" + _DAY + r")/origin=[A-Z]+/"
    r"(?P<origin>[A-Z]+)-(?P<dest>[A-Z]+)__(?P<day>" + _DAY + r")(?:\.\.(?P<day_to>" + _DAY + r"))?"
    r"(?:__(?P<params>[^_]+))?__(?P<time>\d{2}-\d{2}-\d{2})Z\.parquet$")
# Раскладка до 30.09.2026: tickets/date=<день вылета>/origin=X/<A>-<B>[__<параметры>]__<UTC-время>.parquet,
# окно — параметром `to=<последний день>` (scripts/migrate_lake_layout.py переносит в fetched=).
_LEGACY_KEY_RE = re.compile(
    r"^" + TICKETS_PREFIX + r"/date=(?P<day>" + _DAY + r")/origin=[A-Z]+/"
    r"(?P<origin>[A-Z]+)-(?P<dest>[A-Z]+)(?:__(?P<params>[^_]+))?__(?P<stamp>" + _DAY + r"T\d{2}-\d{2}-\d{2})Z\.parquet$")


def parse_file_key(key: str) -> Optional[Dict[str, Any]]:
    """Обратно к ключу серии: {origin, destination, day, day_to, params_key, observed (datetime)}.
    Понимает и прежнюю раскладку date=. None — если файл не по схеме (чужой)."""
    m = _KEY_RE.match(key)
    if m:
        stamp, day, day_to = f"{m.group('date')}T{m.group('time')}", m.group("day"), m.group("day_to")
        params = [p for p in (m.group("params") or "").split(",") if p]
    else:
        m = _LEGACY_KEY_RE.match(key)
        if not m:
            return None
        stamp, day = m.group("stamp"), m.group("day")
        params = [p for p in (m.group("params") or "").split(",") if p]
        day_to = next((p[3:] for p in params if p.startswith("to=")), None)
        params = [p for p in params if not p.startswith("to=")]
    observed = datetime.strptime(stamp, "%Y-%m-%dT%H-%M-%S").replace(tzinfo=timezone.utc)
    return {"origin": None if m.group("origin") == "ANY" else m.group("origin"),
            "destination": None if m.group("dest") == "ANY" else m.group("dest"),
            "day": day, "day_to": day_to, "params_key": "&".join(params), "observed": observed}


class LakeWriter:
    """Пишет серию файлом озера сразу по готовности (один файл = одна серия = одна
    row group) и читает её обратно (из S3 — одним GET). Пустая серия пишется пустым
    файлом: в папке видно, что направление читали и там ничего нет."""

    def __init__(self, store, index):
        self.store = store
        self.index = index
        self.files_written = 0

    def write(self, series_id: int, origin: Optional[str], destination: Optional[str], day: str,
              params_key: str, tickets: List[Dict[str, Any]], observed: datetime) -> str:
        key = series_file_key(origin, destination, day, params_key, observed)
        sink = io.BytesIO()
        table = series_table(tickets, series_id, observed.isoformat(timespec="seconds"))
        pq.write_table(table, sink, compression="zstd", row_group_size=max(len(tickets), 1))
        data = sink.getvalue()
        self.store.put_bytes(key, data)
        self.index.attach_file(key, day, len(data), [(series_id, 0)], observed=observed)
        self.files_written += 1
        return key

    def write_window(self, parts: List[Tuple[int, str, List[Dict[str, Any]]]], origin: Optional[str],
                     destination: Optional[str], day_from: str, day_to: str, params_key: str,
                     observed: datetime) -> str:
        """Окно дат одним файлом: parts — [(series_id, день, билеты)], row group на каждый
        непустой день; в имени — окно: X-ANY__<первый день>..<последний день>__<время>.parquet.
        Пустые дни ссылаются на тот же файл (row group 0, билетов 0 — не читаются)."""
        key = series_file_key(origin, destination, day_from, params_key, observed, day_to=day_to)
        sink = io.BytesIO()
        placements: List[Tuple[int, int]] = []
        stamp = observed.isoformat(timespec="seconds")
        with pq.ParquetWriter(sink, SCHEMA, compression="zstd") as w:
            rg = 0
            for sid, _day, tickets in parts:
                if tickets:
                    w.write_table(series_table(tickets, sid, stamp), row_group_size=len(tickets))
                    placements.append((sid, rg))
                    rg += 1
                else:
                    placements.append((sid, 0))
            if rg == 0:
                w.write_table(series_table([], parts[0][0] if parts else 0, stamp))
        data = sink.getvalue()
        self.store.put_bytes(key, data)
        self.index.attach_file(key, day_from, len(data), placements, observed=observed)
        self.files_written += 1
        return key

    def read(self, file_key: str, row_group: int = 0) -> List[Dict[str, Any]]:
        return tickets_from_table(self.read_table(file_key, row_group))

    def read_table(self, file_key: str, row_group: int = 0) -> pa.Table:
        with self.store.open_input_file(file_key) as f:
            return read_series_table(f, row_group)
