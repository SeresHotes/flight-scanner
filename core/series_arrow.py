"""Серия билетов как Arrow-таблица (схема озера коллектора, collector/lake.SCHEMA).

Коллектор отдаёт планировщику серию Arrow IPC-потоком: таблица из Parquet озера
уходит как есть, без Python-словарей и JSON. Разбор — по колонкам: плоские поля
одним to_pylist на колонку, вложенные JSON-колонки (chain, legs, transfer_points)
одним json.loads на колонку. На серии MOW→ANY (5 тыс. билетов) это ~0.2 с против
~1.1 с у построчного from_row + JSON-ответа FastAPI (замер на проде 30.09.2026).

tickets_from_table даёт ровно те словари, что collector.lake.from_row и
core.graphql_api.normalize_ticket (тот же порядок ключей)."""
import json
from typing import Any, Dict, List

import pyarrow as pa

ARROW_MEDIA_TYPE = "application/vnd.apache.arrow.stream"

PLAIN = ["search_origin", "search_destination", "search_date", "origin", "destination",
         "origin_airport", "destination_airport", "departure_at", "arrival_at", "duration",
         "duration_to", "transfers", "airline", "flight_number", "price", "currency", "link",
         "baggage_code", "source"]
NESTED = {"chain": "chain_json", "legs": "legs_json", "transfer_points": "transfer_points_json"}
BAGGAGE = ["baggage_known", "baggage_included", "baggage_pieces", "baggage_kg"]
# Порядок ключей как у normalize_ticket — для стабильных сравнений.
ORDER = ["origin", "destination", "origin_airport", "destination_airport", "departure_at",
         "arrival_at", "duration", "duration_to", "transfers", "airline", "flight_number",
         "price", "currency", "link", "chain", "legs", "transfer_points", "baggage",
         "baggage_code", "source", "search_origin", "search_destination", "search_date"]


def _column(table: pa.Table, name: str) -> List[Any]:
    if name in table.column_names:
        return table.column(name).to_pylist()
    return [None] * table.num_rows


def tickets_from_table(table: pa.Table) -> List[Dict[str, Any]]:
    n = table.num_rows
    if not n:
        return []
    cols = {k: _column(table, k) for k in PLAIN + BAGGAGE}
    transfers = cols["transfers"]
    cols["transfers"] = [None if v is None else int(v) for v in transfers]
    for key, col in NESTED.items():
        raw = _column(table, col)
        cols[key] = json.loads("[" + ",".join(s or "[]" for s in raw) + "]")
    known, included = cols["baggage_known"], cols["baggage_included"]
    pieces, kg = cols["baggage_pieces"], cols["baggage_kg"]
    cols["baggage"] = [{"known": bool(known[i]), "included": bool(included[i]),
                        "pieces": pieces[i], "kg": kg[i]} for i in range(n)]
    ordered = [cols[k] for k in ORDER]
    return [dict(zip(ORDER, values)) for values in zip(*ordered)]


def table_to_ipc(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def ipc_to_table(data: bytes) -> pa.Table:
    with pa.ipc.open_stream(pa.py_buffer(data)) as reader:
        return reader.read_all()
