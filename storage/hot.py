"""Горячее хранилище: SQLite (WAL) с котировками и джобами.

Схема — из docs/PLAN.md. Serving-нагрузка скромная, ноль администрирования.
Полная история цен живёт в Parquet-озере (storage/lake.py); здесь — свежайшее.
"""
import glob
import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

DEFAULT_DB = os.getenv("FLIGHT_DB", "data/flights.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS quotes (
    origin              TEXT NOT NULL,
    destination         TEXT NOT NULL,
    origin_airport      TEXT,
    dest_airport        TEXT,
    departure_at        TEXT NOT NULL,
    price               REAL,
    airline             TEXT,
    flight_number       TEXT,
    transfers           INTEGER,
    duration            INTEGER,
    link                TEXT,
    direct              INTEGER,
    observed_at         TEXT,
    search_origin       TEXT,
    search_destination  TEXT,
    search_date         TEXT,
    PRIMARY KEY (origin, destination, departure_at, airline, flight_number)
);
CREATE INDEX IF NOT EXISTS idx_quotes_route ON quotes (origin, destination, departure_at);
CREATE INDEX IF NOT EXISTS idx_quotes_observed ON quotes (observed_at);

CREATE TABLE IF NOT EXISTS jobs (
    id          TEXT PRIMARY KEY,
    params_json TEXT,
    status      TEXT,
    progress    INTEGER DEFAULT 0,
    total       INTEGER DEFAULT 0,
    result_json TEXT,
    error       TEXT,
    stage_json  TEXT,       -- текущий этап сбора для UI (см. api.worker.StageReporter)
    created_at  TEXT,
    updated_at  TEXT
);

-- Кэш ответов Travelpayouts по одному под-запросу (origin, destination, day).
-- Позволяет не перезапрашивать недавно собранные под-запросы (TTL — см. worker).
-- origin/destination хранятся как '' когда в запросе не заданы (ANY-направления);
-- direct различает прямые (1) и с пересадками (0) — это разные ответы API.
CREATE TABLE IF NOT EXISTS fetch_cache (
    origin       TEXT NOT NULL,
    destination  TEXT NOT NULL,
    search_date  TEXT NOT NULL,
    direct       INTEGER NOT NULL,
    fetched_at   TEXT NOT NULL,
    data_json    TEXT NOT NULL,
    PRIMARY KEY (origin, destination, search_date, direct)
);
"""


def connect(db_path: str = DEFAULT_DB) -> sqlite3.Connection:
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    # check_same_thread=False: FastAPI выполняет sync-эндпоинты в пуле потоков,
    # а соединение создаётся на startup. Нагрузка низкая, WAL допускает конкурентное чтение.
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    _add_column_if_missing(conn, "jobs", "stage_json", "TEXT")
    conn.commit()


def _add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, decl: str) -> None:
    """Мини-миграция: CREATE TABLE IF NOT EXISTS не добавляет колонки в уже
    существующую таблицу (прод-БД живёт между деплоями)."""
    cols = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


# ------------------------------- quotes --------------------------------------

_QUOTE_COLS = [
    "origin", "destination", "origin_airport", "dest_airport", "departure_at",
    "price", "airline", "flight_number", "transfers", "duration", "link",
    "direct", "observed_at", "search_origin", "search_destination", "search_date",
]


def _flight_to_quote(f: Dict[str, Any], observed_at: str) -> Dict[str, Any]:
    transfers = int(f.get("transfers") or 0)
    return {
        "origin": f.get("origin") or f.get("search_origin"),
        "destination": f.get("destination") or f.get("search_destination"),
        "origin_airport": f.get("origin_airport"),
        "dest_airport": f.get("destination_airport"),
        "departure_at": f.get("departure_at"),
        "price": f.get("price") if f.get("price") is not None else f.get("value"),
        "airline": f.get("airline"),
        "flight_number": f.get("flight_number"),
        "transfers": transfers,
        "duration": f.get("duration"),
        "link": f.get("link"),
        "direct": 1 if transfers == 0 else 0,
        "observed_at": observed_at,
        "search_origin": f.get("search_origin"),
        "search_destination": f.get("search_destination"),
        "search_date": f.get("search_date"),
    }


def flights_to_quotes(flights: Iterable[Dict[str, Any]], observed_at: str) -> List[Dict[str, Any]]:
    """Преобразует сырые рейсы коллектора в строки-котировки."""
    return [_flight_to_quote(f, observed_at) for f in flights]


def upsert_quotes(conn: sqlite3.Connection, rows: Iterable[Dict[str, Any]]) -> int:
    """Upsert «свежайшее по маршруту+дате+рейсу»: при конфликте перезаписываем,
    только если наблюдение новее по observed_at."""
    placeholders = ", ".join("?" for _ in _QUOTE_COLS)
    updates = ", ".join(f"{c}=excluded.{c}" for c in _QUOTE_COLS if c not in
                        ("origin", "destination", "departure_at", "airline", "flight_number"))
    sql = (
        f"INSERT INTO quotes ({', '.join(_QUOTE_COLS)}) VALUES ({placeholders}) "
        f"ON CONFLICT(origin, destination, departure_at, airline, flight_number) "
        f"DO UPDATE SET {updates} WHERE excluded.observed_at >= quotes.observed_at"
    )
    n = 0
    for r in rows:
        if not r.get("origin") or not r.get("destination") or not r.get("departure_at"):
            continue
        conn.execute(sql, [r.get(c) for c in _QUOTE_COLS])
        n += 1
    conn.commit()
    return n


def import_collector_result(conn: sqlite3.Connection, result: Dict[str, Any],
                            observed_at: Optional[str] = None) -> int:
    """Импортирует выгрузку коллектора ({metadata, leg1_flights, leg2_flights})."""
    meta = result.get("metadata") or {}
    observed_at = observed_at or meta.get("collected_at") or datetime.now().isoformat()
    flights = (result.get("leg1_flights") or []) + (result.get("leg2_flights") or [])
    return upsert_quotes(conn, (_flight_to_quote(f, observed_at) for f in flights))


def import_data_dir(conn: sqlite3.Connection, data_dir: str = "data",
                    patterns: Iterable[str] = ("flights_*.json", "mcr_*.json")) -> int:
    """Импортирует все выгрузки коллектора из каталога data/."""
    total = 0
    seen = set()
    for pat in patterns:
        for path in sorted(glob.glob(str(Path(data_dir) / pat))):
            if path in seen:
                continue
            seen.add(path)
            try:
                data = json.load(open(path, encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            if isinstance(data, dict) and ("leg1_flights" in data or "leg2_flights" in data):
                total += import_collector_result(conn, data)
    return total


def count_quotes(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM quotes").fetchone()[0]


def route_has_data(conn: sqlite3.Connection, origin: str, destination: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM quotes WHERE (origin=? OR search_origin=?) "
        "AND (destination=? OR search_destination=?) LIMIT 1",
        (origin, origin, destination, destination),
    ).fetchone()
    return row is not None


def route_coverage(conn: sqlite3.Connection, limit: int = 50) -> Dict[str, Any]:
    """Агрегат «что уже собрано»: сколько котировок и по каким маршрутам."""
    total = count_quotes(conn)
    distinct = conn.execute(
        "SELECT COUNT(*) FROM (SELECT 1 FROM quotes GROUP BY origin, destination)"
    ).fetchone()[0]
    rows = conn.execute(
        "SELECT origin, destination, COUNT(*) AS quotes, "
        "MIN(substr(departure_at,1,10)) AS dep_from, "
        "MAX(substr(departure_at,1,10)) AS dep_to, "
        "MAX(observed_at) AS last_observed "
        "FROM quotes GROUP BY origin, destination ORDER BY quotes DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return {
        "total_quotes": total,
        "distinct_routes": distinct,
        "routes": [dict(r) for r in rows],
    }


def route_last_observed(conn: sqlite3.Connection, origin: str, destination: str) -> Optional[str]:
    """Когда по маршруту в последний раз собирались данные (max observed_at)."""
    row = conn.execute(
        "SELECT MAX(observed_at) FROM quotes "
        "WHERE (origin=? OR search_origin=?) AND (destination=? OR search_destination=?)",
        (origin, origin, destination, destination),
    ).fetchone()
    return row[0] if row else None


def coverage_gaps(conn: sqlite3.Connection, origin: str, destination: str,
                  dates: List[str]) -> List[str]:
    """Даты из списка, для которых нет ни одной котировки по маршруту (departure date)."""
    gaps = []
    for d in dates:
        row = conn.execute(
            "SELECT 1 FROM quotes WHERE origin=? AND destination=? "
            "AND substr(departure_at,1,10)=? LIMIT 1",
            (origin, destination, d),
        ).fetchone()
        if row is None:
            gaps.append(d)
    return gaps


# ----------------------------- fetch cache -----------------------------------

def _fetch_cache_key(origin: Optional[str], destination: Optional[str]) -> tuple:
    """Нормализует ключ: ANY-направление (origin/destination=None) → ''."""
    return (origin or "", destination or "")


def fetch_cache_get(conn: sqlite3.Connection, origin: Optional[str],
                    destination: Optional[str], search_date: str, direct: int,
                    ttl_seconds: float) -> Optional[List[Dict[str, Any]]]:
    """Список рейсов из кэша, если запрос свежее ttl_seconds; иначе None.

    None означает «надо сходить в API» — как при промахе, так и при протухании.
    """
    o, d = _fetch_cache_key(origin, destination)
    row = conn.execute(
        "SELECT fetched_at, data_json FROM fetch_cache "
        "WHERE origin=? AND destination=? AND search_date=? AND direct=?",
        (o, d, search_date, direct),
    ).fetchone()
    if row is None:
        return None
    age = (datetime.now() - datetime.fromisoformat(row["fetched_at"])).total_seconds()
    if age > ttl_seconds:
        return None
    return json.loads(row["data_json"])


def fetch_cache_put(conn: sqlite3.Connection, origin: Optional[str],
                    destination: Optional[str], search_date: str, direct: int,
                    data: List[Dict[str, Any]]) -> None:
    """Сохраняет ответ API по под-запросу, отметив время (для TTL)."""
    o, d = _fetch_cache_key(origin, destination)
    now = datetime.now().isoformat()
    conn.execute(
        "INSERT INTO fetch_cache (origin, destination, search_date, direct, fetched_at, data_json) "
        "VALUES (?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(origin, destination, search_date, direct) "
        "DO UPDATE SET fetched_at=excluded.fetched_at, data_json=excluded.data_json",
        (o, d, search_date, direct, now, json.dumps(data, ensure_ascii=False)),
    )
    conn.commit()


# -------------------------------- jobs ---------------------------------------

def create_job(conn: sqlite3.Connection, job_id: str, params: Dict[str, Any],
               total: int = 0, stage: Optional[Dict[str, Any]] = None) -> None:
    now = datetime.now().isoformat()
    stage_json = json.dumps(stage, ensure_ascii=False) if stage is not None else None
    conn.execute(
        "INSERT INTO jobs (id, params_json, status, progress, total, stage_json, created_at, updated_at) "
        "VALUES (?, ?, 'pending', 0, ?, ?, ?, ?)",
        (job_id, json.dumps(params, ensure_ascii=False), total, stage_json, now, now),
    )
    conn.commit()


def update_job(conn: sqlite3.Connection, job_id: str, **fields) -> None:
    if not fields:
        return
    fields["updated_at"] = datetime.now().isoformat()
    sets = ", ".join(f"{k}=?" for k in fields)
    conn.execute(f"UPDATE jobs SET {sets} WHERE id=?", [*fields.values(), job_id])
    conn.commit()


def get_job(conn: sqlite3.Connection, job_id: str) -> Optional[Dict[str, Any]]:
    row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    return dict(row) if row else None


def fail_stale_jobs(conn: sqlite3.Connection,
                    error: str = "прервана рестартом сервера") -> int:
    """Помечает зависшие джобы (pending/running) как error.

    Воркер живёт в потоке процесса, поэтому при перезапуске сервера незавершённые
    джобы теряют исполнителя, но остаются в БД в статусе running навсегда. Чистим
    их на старте, чтобы маршрут можно было собрать заново."""
    now = datetime.now().isoformat()
    cur = conn.execute(
        "UPDATE jobs SET status='error', error=?, updated_at=? "
        "WHERE status IN ('pending', 'running')",
        (error, now),
    )
    conn.commit()
    return cur.rowcount
