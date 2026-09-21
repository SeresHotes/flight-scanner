"""Архив данных: append-only Parquet-озеро.

Ценность проекта = временной ряд наблюдений цены. Каждый сбор ДОБАВЛЯет файл
`quotes/dt=YYYY-MM-DD/route=A-B/part-<id>.parquet`; ничего не перезаписываем.
В деве пишем на локальный диск; в проде тот же layout ложится в Object Storage.
"""
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    _HAVE_PYARROW = True
except ImportError:  # pragma: no cover
    _HAVE_PYARROW = False

DEFAULT_LAKE_ROOT = "data/lake"

_COLS = [
    "origin", "destination", "origin_airport", "dest_airport", "departure_at",
    "price", "airline", "flight_number", "transfers", "duration", "link",
    "direct", "observed_at", "search_origin", "search_destination", "search_date",
]


def available() -> bool:
    return _HAVE_PYARROW


def _partition(row: Dict[str, Any]) -> tuple:
    dt = (row.get("search_date") or (row.get("departure_at") or "")[:10] or "unknown")
    route = f"{row.get('origin') or 'X'}-{row.get('destination') or 'X'}"
    return dt, route


def append_quotes(rows: Iterable[Dict[str, Any]], root: str = DEFAULT_LAKE_ROOT,
                  part_id: str = None) -> List[str]:
    """Дозаписывает котировки в озеро, партиционируя по дате наблюдения и маршруту.

    Возвращает список записанных файлов. part_id стабилизирует имя (для идемпотентности
    в рамках одной джобы); по умолчанию — случайный uuid.
    """
    if not _HAVE_PYARROW:
        raise RuntimeError("pyarrow не установлен — Parquet-озеро недоступно")

    buckets: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        buckets[_partition(r)].append(r)

    part_id = part_id or uuid.uuid4().hex[:12]
    written = []
    for (dt, route), items in buckets.items():
        out_dir = Path(root) / "quotes" / f"dt={dt}" / f"route={route}"
        out_dir.mkdir(parents=True, exist_ok=True)
        table = pa.table({c: [it.get(c) for it in items] for c in _COLS})
        path = out_dir / f"part-{part_id}.parquet"
        pq.write_table(table, path)
        written.append(str(path))
    return written
