"""Индекс серий озера — горячее состояние коллектора в SQLite.

Серия = «направление × день × прочие параметры» (как ключ бывшего ticket_cache).
Строка знает, когда серия получена, сколько страниц и билетов, в каком файле озера
и в какой row group лежат её билеты. По индексу работают кэш (свежая серия
отдаётся без источника), покрытие для фонового сборщика и ретеншн (файл удалён →
его серии выпадают из индекса). Таблица cities — известные города (для обхода)."""
import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

SCHEMA = """
CREATE TABLE IF NOT EXISTS series (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    origin       TEXT NOT NULL,
    destination  TEXT NOT NULL,
    search_date  TEXT NOT NULL,
    params_key   TEXT NOT NULL,
    fetched_at   TEXT NOT NULL,
    pages        INTEGER NOT NULL,
    exhausted    INTEGER NOT NULL,
    tickets      INTEGER NOT NULL,
    error        INTEGER NOT NULL DEFAULT 0,
    error_msg    TEXT,
    client       TEXT,
    file_key     TEXT,
    row_group    INTEGER,
    UNIQUE (origin, destination, search_date, params_key)
);
CREATE INDEX IF NOT EXISTS idx_series_file ON series (file_key);
CREATE INDEX IF NOT EXISTS idx_series_fetched ON series (fetched_at);

CREATE TABLE IF NOT EXISTS files (
    key         TEXT PRIMARY KEY,
    observed    TEXT NOT NULL,   -- (первый) день вылета серии; момент загрузки — created_at
    bytes       INTEGER NOT NULL,
    series      INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cities (
    code        TEXT PRIMARY KEY,
    first_seen  TEXT NOT NULL,
    last_seen   TEXT NOT NULL,
    tickets     INTEGER NOT NULL DEFAULT 0
);
"""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def parse_ts(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def series_key(origin: Optional[str], destination: Optional[str], day: str,
               params_key: str) -> Tuple[str, str, str, str]:
    """ANY-конец → пустая строка (как в старом ticket_cache)."""
    return ((origin or "").upper(), (destination or "").upper(), day, params_key)


class Index:
    """Потокобезопасная обёртка над SQLite (воркер + HTTP-потоки)."""

    def __init__(self, db_path: str = ":memory:"):
        if db_path != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        if db_path != ":memory:":
            self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(SCHEMA)
        cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(series)")}
        if "error_msg" not in cols:  # индекс до 02.10.2026: текст ошибки источника не хранился
            self._conn.execute("ALTER TABLE series ADD COLUMN error_msg TEXT")
            self._conn.commit()
        self._lock = threading.RLock()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------ series ---------------------------------

    def get(self, origin: Optional[str], destination: Optional[str], day: str,
            params_key: str) -> Optional[Dict[str, Any]]:
        o, d, day, k = series_key(origin, destination, day, params_key)
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM series WHERE origin=? AND destination=? AND search_date=? AND params_key=?",
                (o, d, day, k)).fetchone()
        return dict(row) if row else None

    def fresh(self, origin: Optional[str], destination: Optional[str], day: str, params_key: str,
              ttl_seconds: float, min_pages: Optional[int] = None,
              now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        """Серия годится как кэш: без ошибки, моложе TTL, и либо исчерпана, либо в ней
        не меньше страниц, чем просят сейчас."""
        row = self.get(origin, destination, day, params_key)
        if row is None or row["error"]:
            return None
        age = ((now or utcnow()) - parse_ts(row["fetched_at"])).total_seconds()
        if age > ttl_seconds:
            return None
        if not row["exhausted"] and min_pages is not None and row["pages"] < min_pages:
            return None
        return row

    def put(self, origin: Optional[str], destination: Optional[str], day: str, params_key: str, *,
            pages: int, exhausted: bool, tickets: int, error: bool = False,
            client: Optional[str] = None, fetched_at: Optional[datetime] = None) -> int:
        """Записывает/перезаписывает серию (file_key сбрасывается — назначит LakeWriter)."""
        o, d, day, k = series_key(origin, destination, day, params_key)
        with self._lock:
            self._conn.execute(
                "INSERT INTO series (origin, destination, search_date, params_key, fetched_at, pages, "
                "exhausted, tickets, error, error_msg, client, file_key, row_group) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, NULL) "
                "ON CONFLICT(origin, destination, search_date, params_key) DO UPDATE SET "
                "fetched_at=excluded.fetched_at, pages=excluded.pages, exhausted=excluded.exhausted, "
                "tickets=excluded.tickets, error=excluded.error, error_msg=excluded.error_msg, "
                "client=excluded.client, file_key=NULL, row_group=NULL",
                (o, d, day, k, _iso(fetched_at or utcnow()), int(pages), int(bool(exhausted)),
                 int(tickets), int(bool(error)), client))
            self._conn.commit()
            return self._conn.execute(
                "SELECT id FROM series WHERE origin=? AND destination=? AND search_date=? AND params_key=?",
                (o, d, day, k)).fetchone()[0]

    def put_days(self, origin: Optional[str], destination: Optional[str], params_key: str,
                 days: Sequence[Tuple[str, int, int]], *, exhausted: bool, error: bool = False,
                 client: Optional[str] = None, fetched_at: Optional[datetime] = None,
                 error_msg: Optional[str] = None) -> List[int]:
        """put для окна дат одной транзакцией: days — [(день, страниц, билетов)].
        error_msg — текст ошибки источника (для дашборда). Возвращает id серий в том же порядке."""
        o, d, _, k = series_key(origin, destination, "", params_key)
        ts = _iso(fetched_at or utcnow())
        with self._lock:
            self._conn.executemany(
                "INSERT INTO series (origin, destination, search_date, params_key, fetched_at, pages, "
                "exhausted, tickets, error, error_msg, client, file_key, row_group) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL) "
                "ON CONFLICT(origin, destination, search_date, params_key) DO UPDATE SET "
                "fetched_at=excluded.fetched_at, pages=excluded.pages, exhausted=excluded.exhausted, "
                "tickets=excluded.tickets, error=excluded.error, error_msg=excluded.error_msg, "
                "client=excluded.client, file_key=NULL, row_group=NULL",
                [(o, d, day, k, ts, int(pages), int(bool(exhausted)), int(tickets), int(bool(error)),
                  error_msg if error else None, client)
                 for day, pages, tickets in days])
            self._conn.commit()
            return [self._conn.execute(
                "SELECT id FROM series WHERE origin=? AND destination=? AND search_date=? AND params_key=?",
                (o, d, day, k)).fetchone()[0] for day, _, _ in days]

    def attach_file(self, key: str, day: str, size: int, placements: Sequence[Tuple[int, int]],
                    observed: Optional[datetime] = None) -> None:
        """Файл записан: серии (id → row_group) теперь читаются из него. observed —
        момент загрузки (порядок удаления при ретеншне: старейшие первыми)."""
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO files (key, observed, bytes, series, created_at) VALUES (?, ?, ?, ?, ?)",
                (key, day, int(size), len(placements), (observed or utcnow()).isoformat()))
            self._conn.executemany("UPDATE series SET file_key=?, row_group=? WHERE id=?",
                                   [(key, rg, sid) for sid, rg in placements])
            self._conn.commit()

    def count(self) -> int:
        with self._lock:
            return self._conn.execute("SELECT COUNT(*) FROM series").fetchone()[0]

    def coverage(self, params_key: str = "", destination: str = "") -> List[List[Any]]:
        """Компактные строки покрытия для сборщика:
        [origin, day, fetched_at, pages, tickets, exhausted, error]."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT origin, search_date, fetched_at, pages, tickets, exhausted, error FROM series "
                "WHERE params_key=? AND destination=? ORDER BY origin, search_date",
                (params_key, destination.upper())).fetchall()
        return [[r["origin"], r["search_date"], r["fetched_at"], r["pages"], r["tickets"],
                 bool(r["exhausted"]), bool(r["error"])] for r in rows]

    def age_stats(self, now: Optional[datetime] = None) -> Dict[str, Any]:
        """Число серий без ошибки (все: и прошедшие дни, и запросы приложения) и возраст
        данных сборщика в часах: медиана, p95 и максимум по парам «город × день» X→ANY
        на дни вылета от сегодня. Прошедшие дни и пары приложения не обновляются и
        лежат до ретеншна — по всем сериям максимум рос бы бесконечно."""
        now = now or utcnow()
        with self._lock:
            total = self._conn.execute("SELECT COUNT(*) FROM series WHERE error=0").fetchone()[0]
            rows = self._conn.execute(
                "SELECT fetched_at FROM series WHERE error=0 AND destination='' AND params_key='' "
                "AND search_date >= ?", (now.astimezone(timezone.utc).date().isoformat(),)).fetchall()
        if not rows:
            return {"series": total, "horizon_pairs": 0, "age_p50_h": None, "age_p95_h": None,
                    "age_max_h": None}
        ages = sorted((now - parse_ts(r["fetched_at"])).total_seconds() / 3600 for r in rows)
        return {"series": total, "horizon_pairs": len(ages), "age_p50_h": round(ages[len(ages) // 2], 2),
                "age_p95_h": round(ages[int(0.95 * (len(ages) - 1))], 2), "age_max_h": round(ages[-1], 2)}

    def coverage_errors(self) -> Dict[Tuple[str, str], str]:
        """Текст последней ошибки источника по парам X→ANY (origin, day) — для снимка покрытия."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT origin, search_date, error_msg FROM series "
                "WHERE error=1 AND destination='' AND params_key=''").fetchall()
        return {(r["origin"], r["search_date"]): r["error_msg"] or "" for r in rows}

    # ------------------------------- files ---------------------------------

    def files_oldest_first(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, observed, bytes, series, created_at FROM files "
                "ORDER BY created_at, key").fetchall()
        return [dict(r) for r in rows]

    def files_since(self, since: Optional[str] = None) -> List[Dict[str, Any]]:
        """Файлы озера для сверки склада билетов: ключ, момент загрузки (created_at), размер;
        since — ISO-время, отдаём файлы не раньше него (пусто — все)."""
        with self._lock:
            if since:
                rows = self._conn.execute(
                    "SELECT key, created_at, bytes FROM files WHERE created_at >= ? ORDER BY created_at, key",
                    (since,)).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT key, created_at, bytes FROM files ORDER BY created_at, key").fetchall()
        return [{"key": r["key"], "created_at": r["created_at"], "bytes": int(r["bytes"])} for r in rows]

    def files_bytes(self) -> int:
        with self._lock:
            return int(self._conn.execute("SELECT COALESCE(SUM(bytes), 0) FROM files").fetchone()[0])

    def rename_file(self, old: str, new: str) -> int:
        """Файл перенесён под новый ключ (копия уже в озере): файл и его серии —
        одной транзакцией. Возвращает число перенаправленных серий. Новый ключ уже в
        учёте (прерванный перенос: сверка при старте импортировала копию) — старая
        запись просто убирается."""
        with self._lock:
            if self._conn.execute("SELECT 1 FROM files WHERE key=?", (new,)).fetchone():
                self._conn.execute("DELETE FROM files WHERE key=?", (old,))
            else:
                self._conn.execute("UPDATE files SET key=? WHERE key=?", (new, old))
            n = self._conn.execute("UPDATE series SET file_key=? WHERE file_key=?", (new, old)).rowcount
            self._conn.commit()
        return n

    def delete_files(self, keys: Iterable[str]) -> int:
        """Файлы удалены из озера — их серии больше не читаются, убираем из индекса."""
        keys = list(keys)
        if not keys:
            return 0
        with self._lock:
            self._conn.executemany("DELETE FROM series WHERE file_key=?", [(k,) for k in keys])
            self._conn.executemany("DELETE FROM files WHERE key=?", [(k,) for k in keys])
            self._conn.commit()
        return len(keys)

    def import_files(self, objects: Iterable[Tuple[str, int]]) -> int:
        """Сверка с озером после потери индекса: файлы, которых нет в таблице,
        добавляются без серий (только для учёта объёма и ретеншна). Момент загрузки —
        из имени файла (collector.lake.parse_file_key); чужие файлы — в самый конец."""
        from collector.lake import parse_file_key
        added = 0
        with self._lock:
            for key, size in objects:
                meta = parse_file_key(key)
                day = meta["day"] if meta else "0000-00-00"
                created = meta["observed"].isoformat() if meta else "0000-00-00T00:00:00+00:00"
                cur = self._conn.execute(
                    "INSERT OR IGNORE INTO files (key, observed, bytes, series, created_at) VALUES (?, ?, ?, 0, ?)",
                    (key, day, int(size), created))
                added += cur.rowcount
            self._conn.commit()
        return added

    # ------------------------------- cities --------------------------------

    def touch_cities(self, counts: Dict[str, int], now: Optional[datetime] = None) -> None:
        """Пополняет список городов: код → сколько билетов встретилось в этой серии."""
        if not counts:
            return
        ts = _iso(now or utcnow())
        with self._lock:
            self._conn.executemany(
                "INSERT INTO cities (code, first_seen, last_seen, tickets) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(code) DO UPDATE SET last_seen=excluded.last_seen, "
                "tickets=cities.tickets+excluded.tickets",
                [(code.upper(), ts, ts, int(n)) for code, n in counts.items() if code])
            self._conn.commit()

    def cities(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT code, first_seen, last_seen, tickets FROM cities ORDER BY tickets DESC, code").fetchall()
        return [dict(r) for r in rows]
