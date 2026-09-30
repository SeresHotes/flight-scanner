"""Parquet-озеро серий: один файл на серию, путь = ключ серии + момент загрузки:
`tickets/date=<день вылета>/origin=<город>/<A>-<B>[__<параметры>]__<UTC-время>.parquet`.

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


def series_file_key(origin: Optional[str], destination: Optional[str], day: str,
                    params_key: str, observed: datetime) -> str:
    """Путь файла серии = её ключ + момент загрузки:
    tickets/date=<день вылета>/origin=<город|ANY>/<A>-<B>[__<параметры>]__<UTC-время>.parquet
    Заходишь в дату — видишь города; у города — когда его читали. Повторная выборка
    той же серии — новый файл рядом (история цен), старые уходят ретеншном."""
    o = (origin or "ANY").upper()
    d = (destination or "ANY").upper()
    params = f"__{params_key.replace('&', ',')}" if params_key else ""
    stamp = observed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    return f"{TICKETS_PREFIX}/date={day}/origin={o}/{o}-{d}{params}__{stamp}.parquet"


_KEY_RE = re.compile(
    r"^" + TICKETS_PREFIX + r"/date=(?P<day>\d{4}-\d{2}-\d{2})/origin=[A-Z]+/"
    r"(?P<origin>[A-Z]+)-(?P<dest>[A-Z]+)(?:__(?P<params>[^_]+))?__(?P<stamp>\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})Z\.parquet$")


def parse_file_key(key: str) -> Optional[Dict[str, Any]]:
    """Обратно к ключу серии: {origin, destination, day, params_key, observed (datetime)}.
    None — если файл не по схеме (чужой/старый)."""
    m = _KEY_RE.match(key)
    if not m:
        return None
    stamp = m.group("stamp")
    observed = datetime.strptime(stamp, "%Y-%m-%dT%H-%M-%S").replace(tzinfo=timezone.utc)
    return {"origin": None if m.group("origin") == "ANY" else m.group("origin"),
            "destination": None if m.group("dest") == "ANY" else m.group("dest"),
            "day": m.group("day"), "params_key": (m.group("params") or "").replace(",", "&"),
            "observed": observed}


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
        непустой день. Путь — как у серии первого дня с `to=<последний день>` в параметрах:
        tickets/date=<первый день>/origin=X/X-ANY__to=<последний день>__<время>.parquet.
        Пустые дни ссылаются на тот же файл (row group 0, билетов 0 — не читаются)."""
        window_key = "&".join(p for p in (params_key, f"to={day_to}") if p)
        key = series_file_key(origin, destination, day_from, window_key, observed)
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
        with self.store.open_input_file(file_key) as f:
            return read_series(f, row_group)
