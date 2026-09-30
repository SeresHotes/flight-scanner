"""Горячее хранилище: SQLite (WAL) с котировками и джобами.

Схема — из docs/PLAN.md. Serving-нагрузка скромная, ноль администрирования.
Полная история цен живёт в Parquet-озере (storage/lake.py); здесь — свежайшее.
"""
import json
import os
import sqlite3
from datetime import datetime, timedelta
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

-- Собранные рейсы джобы планировщика (gzip JSON {плечо: [рейсы]}): из них по
-- требованию строятся маршруты выбранных наборов городов (/routes?combos=).
CREATE TABLE IF NOT EXISTS plan_flights (
    job_id      TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL,
    data        BLOB NOT NULL
);

-- Кэш серий GraphQL (core/graphql_api.fetch_series): все страницы одного под-запроса
-- «направление × день × прочие параметры» (params_key — коридор цен, direct, багаж).
-- exhausted=0 — серия обрезана предохранителем страниц; pages — сколько получено.
CREATE TABLE IF NOT EXISTS ticket_cache (
    origin       TEXT NOT NULL,
    destination  TEXT NOT NULL,
    search_date  TEXT NOT NULL,
    params_key   TEXT NOT NULL,
    fetched_at   TEXT NOT NULL,
    pages        INTEGER NOT NULL,
    exhausted    INTEGER NOT NULL,
    data_json    TEXT NOT NULL,
    PRIMARY KEY (origin, destination, search_date, params_key)
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
    _add_column_if_missing(conn, "jobs", "query_key", "TEXT")   # хэш PlanQuery (дедуп джоб)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_query_key ON jobs (query_key)")
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


def count_quotes(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM quotes").fetchone()[0]


def airport_city_map(conn: sqlite3.Connection) -> Dict[str, str]:
    """Аэропорт → код города по всем накопленным котировкам (PEK → BJS, ICN → SEL).

    Нужен планировщику, чтобы узнать хаб hidden-city в остановке, заданной кодом
    города, даже если в текущем сборе билетов в этот аэропорт не было."""
    out: Dict[str, str] = {}
    for apt_col, city_col in (("origin_airport", "origin"), ("dest_airport", "destination")):
        rows = conn.execute(
            f"SELECT DISTINCT {apt_col}, {city_col} FROM quotes "
            f"WHERE {apt_col} IS NOT NULL AND {city_col} IS NOT NULL")
        for apt, city in rows:
            if apt and city:
                out.setdefault(apt.upper(), city.upper())
    return out


# Рейсы джобы — Parquet-файл колонками (core.flightcols) в plan_flights/ рядом с БД:
# стыковка под новые фильтры читает только числовые колонки, полный рейс — лениво.
# Джоба живёт сутки (api.main.PLAN_JOB_TTL_SECONDS) — файлы старше двух суток удаляем.
PLAN_FLIGHTS_KEEP_SECONDS = 2 * 24 * 3600


def _plan_flights_dir(conn: sqlite3.Connection) -> Path:
    row = conn.execute("PRAGMA database_list").fetchone()
    db_file = row[2] if row is not None else ""
    base = Path(db_file).parent if db_file else Path(os.getenv("TMPDIR", "/tmp"))
    return base / "plan_flights"


def plan_flights_path(conn: sqlite3.Connection, job_id: str) -> Path:
    return _plan_flights_dir(conn) / f"{job_id}.parquet"


def put_plan_flights(conn: sqlite3.Connection, job_id: str, flights) -> None:
    """Сохраняет рейсы джобы (core.flightcols.FlightCols или {плечо: [рейсы]}) для
    видов под другие фильтры и маршрутов наборов; чистит файлы старых джоб."""
    from core.flightcols import as_cols
    path = plan_flights_path(conn, job_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    as_cols(flights).write(str(tmp))
    os.replace(tmp, path)
    cutoff = datetime.now().timestamp() - PLAN_FLIGHTS_KEEP_SECONDS
    for old in path.parent.glob("*.parquet"):
        try:
            if old.stat().st_mtime < cutoff:
                old.unlink()
        except FileNotFoundError:
            pass
    # Прежний формат (gzip JSON в SQLite) больше не пишется; старые строки — вон.
    conn.execute("DELETE FROM plan_flights WHERE created_at < ?",
                 (datetime.fromtimestamp(cutoff).isoformat(),))
    conn.commit()


def get_plan_flights(conn: sqlite3.Connection, job_id: str):
    """Рейсы джобы колонками (core.flightcols.FlightCols) или None. Джобы прежнего
    формата (gzip JSON в plan_flights) читаются и перекладываются в колонки."""
    from core.flightcols import FlightCols
    path = plan_flights_path(conn, job_id)
    if path.exists():
        return FlightCols.read(str(path))
    import gzip
    row = conn.execute("SELECT data FROM plan_flights WHERE job_id=?", (job_id,)).fetchone()
    if row is None:
        return None
    raw = json.loads(gzip.decompress(row["data"]).decode())
    cols = FlightCols.from_collected({int(k): v for k, v in raw.items()})
    try:   # переносим в новый формат один раз — дальше читается файл
        put_plan_flights(conn, job_id, cols)
        conn.execute("DELETE FROM plan_flights WHERE job_id=?", (job_id,))
        conn.commit()
    except OSError as e:
        print(f"[hot] plan_flights {job_id}: перенос в Parquet не удался: {e}")
    return cols


def count_ticket_series(conn: sqlite3.Connection) -> int:
    """Сколько серий GraphQL лежит в ticket_cache (health / проверка деплоя).
    0, если таблицы нет (с коллектором она удалена, см. drop_ticket_cache)."""
    try:
        return conn.execute("SELECT COUNT(*) FROM ticket_cache").fetchone()[0]
    except sqlite3.OperationalError:
        return 0


def drop_ticket_cache(conn: sqlite3.Connection) -> None:
    """С коллектором серии живут в озере — локальный кэш не нужен. Таблица удаляется,
    но файл SQLite не сжимается: на проде это делает deploy/vm-migrate.sh (VACUUM INTO)."""
    conn.execute("DROP TABLE IF EXISTS ticket_cache")
    conn.commit()


# ----------------------------- fetch cache -----------------------------------

def _fetch_cache_key(origin: Optional[str], destination: Optional[str]) -> tuple:
    """Нормализует ключ: ANY-направление (origin/destination=None) → ''."""
    return (origin or "", destination or "")


def ticket_cache_get(conn: sqlite3.Connection, origin: Optional[str],
                     destination: Optional[str], search_date: str, params_key: str,
                     ttl_seconds: float, min_pages: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """Серия GraphQL из кэша: {"tickets", "pages", "exhausted"} — если свежее TTL и
    достаточно полная. Обрезанная серия (exhausted=0) годится, только если у неё не
    меньше min_pages страниц, чем просят сейчас; иначе None — надо перезапросить."""
    o, d = _fetch_cache_key(origin, destination)
    row = conn.execute(
        "SELECT fetched_at, pages, exhausted, data_json FROM ticket_cache "
        "WHERE origin=? AND destination=? AND search_date=? AND params_key=?",
        (o, d, search_date, params_key),
    ).fetchone()
    if row is None:
        return None
    age = (datetime.now() - datetime.fromisoformat(row["fetched_at"])).total_seconds()
    if age > ttl_seconds:
        return None
    if not row["exhausted"] and min_pages is not None and row["pages"] < min_pages:
        return None
    return {"tickets": json.loads(row["data_json"]), "pages": row["pages"],
            "exhausted": bool(row["exhausted"])}


def ticket_cache_has(conn: sqlite3.Connection, origin: Optional[str],
                     destination: Optional[str], search_date: str, params_key: str,
                     ttl_seconds: float, min_pages: Optional[int] = None) -> bool:
    """Есть ли серия в кэше (те же правила, что ticket_cache_get), без чтения данных —
    для оценки «сколько страниц уже в кэше» перед сбором."""
    o, d = _fetch_cache_key(origin, destination)
    row = conn.execute(
        "SELECT fetched_at, pages, exhausted FROM ticket_cache "
        "WHERE origin=? AND destination=? AND search_date=? AND params_key=?",
        (o, d, search_date, params_key),
    ).fetchone()
    if row is None:
        return False
    age = (datetime.now() - datetime.fromisoformat(row["fetched_at"])).total_seconds()
    if age > ttl_seconds:
        return False
    return bool(row["exhausted"]) or min_pages is None or row["pages"] >= min_pages


def ticket_cache_put(conn: sqlite3.Connection, origin: Optional[str],
                     destination: Optional[str], search_date: str, params_key: str,
                     series: Dict[str, Any]) -> None:
    """Сохраняет серию (tickets/pages/exhausted) с отметкой времени для TTL."""
    o, d = _fetch_cache_key(origin, destination)
    now = datetime.now().isoformat()
    conn.execute(
        "INSERT INTO ticket_cache (origin, destination, search_date, params_key, "
        "fetched_at, pages, exhausted, data_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(origin, destination, search_date, params_key) DO UPDATE SET "
        "fetched_at=excluded.fetched_at, pages=excluded.pages, "
        "exhausted=excluded.exhausted, data_json=excluded.data_json",
        (o, d, search_date, params_key, now, int(series.get("pages") or 0),
         int(bool(series.get("exhausted"))),
         json.dumps(series.get("tickets") or [], ensure_ascii=False)),
    )
    conn.commit()


# -------------------------------- jobs ---------------------------------------

def create_job(conn: sqlite3.Connection, job_id: str, params: Dict[str, Any],
               total: int = 0, stage: Optional[Dict[str, Any]] = None,
               query_key: Optional[str] = None) -> None:
    now = datetime.now().isoformat()
    stage_json = json.dumps(stage, ensure_ascii=False) if stage is not None else None
    conn.execute(
        "INSERT INTO jobs (id, params_json, status, progress, total, stage_json, query_key, "
        "created_at, updated_at) VALUES (?, ?, 'pending', 0, ?, ?, ?, ?, ?)",
        (job_id, json.dumps(params, ensure_ascii=False), total, stage_json, query_key, now, now),
    )
    conn.commit()


def find_job_by_key(conn: sqlite3.Connection, query_key: str, ttl_seconds: float) -> Optional[Dict[str, Any]]:
    """Свежая (created_at не старше TTL) живая или готовая джоба с тем же PlanQuery —
    её переиспользуем вместо нового сбора. Джобы с ошибкой не подходят."""
    cutoff = (datetime.now() - timedelta(seconds=ttl_seconds)).isoformat()
    row = conn.execute(
        "SELECT * FROM jobs WHERE query_key=? AND status IN ('pending', 'running', 'done') "
        "AND created_at >= ? ORDER BY created_at DESC LIMIT 1", (query_key, cutoff),
    ).fetchone()
    return dict(row) if row else None


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


def find_hung_jobs(conn: sqlite3.Connection, idle_seconds: float) -> List[str]:
    """id джоб в running, которые не обновлялись дольше idle_seconds.

    Живой сбор обновляет джобу на каждом запросе к API (раз в ~0.5–1с), так что
    долгая тишина = воркер застрял (обычно в переборе стыковки). pending не берём:
    джоба в очереди молчит законно, пока ждёт свободного воркера."""
    cutoff = (datetime.now() - timedelta(seconds=idle_seconds)).isoformat()
    rows = conn.execute(
        "SELECT id FROM jobs WHERE status='running' AND updated_at < ?", (cutoff,),
    ).fetchall()
    return [r[0] for r in rows]


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
