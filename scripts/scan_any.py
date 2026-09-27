#!/usr/bin/env python3
"""CLI: сбор X → ANY обходом в ширину от MOW (см. core/anyscan.py).

Кэш под-запросов (fetch_cache, TTL 24ч), котировки (quotes) и озеро (data/lake)
— те же, что у фонового воркера api.worker. Долгий сбор можно прервать (Ctrl-C)
и продолжить: очередь и пройденные города лежат в файле состояния.

Примеры:
  poetry run python scripts/scan_any.py --dry-run                 # план без запросов
  poetry run python scripts/scan_any.py --max-cities 20           # первые 20 городов
  poetry run python scripts/scan_any.py                           # продолжить до конца очереди
  poetry run python scripts/scan_any.py --reset --from-date 2026-11-02 --months 2
"""
import argparse
import json
import os
import signal
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core import anyscan  # noqa: E402
from core.collector import require_token  # noqa: E402
from storage import hot, lake  # noqa: E402

DEFAULT_STATE = "data/anyscan_state.json"
DEFAULT_LEAD_DAYS = 14  # первое окно — через две недели от сегодня


def _default_from_date() -> date:
    """Ближайший понедельник не раньше чем через DEFAULT_LEAD_DAYS."""
    d = date.today() + timedelta(days=DEFAULT_LEAD_DAYS)
    return d + timedelta(days=(7 - d.weekday()) % 7)


def _load_state(path: Path, args) -> anyscan.ScanState:
    if path.exists() and not args.reset:
        state = anyscan.ScanState.from_dict(json.loads(path.read_text(encoding="utf-8")))
        print(f"[scan] продолжаю: {len(state.done)} городов собрано, "
              f"{len(state.queue)} в очереди, {state.requests} запросов сделано")
        return state
    from_date = date.fromisoformat(args.from_date) if args.from_date else _default_from_date()
    dates = anyscan.date_windows(from_date, months=args.months, days=args.days)
    return anyscan.ScanState.initial(dates, args.start)


def _save_state(path: Path, state: anyscan.ScanState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state.as_dict(), ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, path)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default=anyscan.DEFAULT_START_CITY, help="стартовый город BFS (IATA города)")
    ap.add_argument("--from-date", help="первый день первого окна, YYYY-MM-DD (по умолчанию — понедельник через ~2 недели)")
    ap.add_argument("--months", type=int, default=anyscan.DEFAULT_MONTHS, help="сколько месячных окон")
    ap.add_argument("--days", type=int, default=anyscan.DEFAULT_WINDOW_DAYS, help="дней в окне")
    ap.add_argument("--max-cities", type=int, help="сколько городов собрать за этот запуск")
    ap.add_argument("--max-requests", type=int, help="потолок суммарного числа запросов к API")
    ap.add_argument("--db", default=hot.DEFAULT_DB, help="SQLite с quotes/fetch_cache (FLIGHT_DB)")
    ap.add_argument("--lake-root", default=lake.DEFAULT_LAKE_ROOT, help="каталог Parquet-озера")
    ap.add_argument("--state", default=DEFAULT_STATE, help="файл состояния обхода")
    ap.add_argument("--reset", action="store_true", help="начать обход заново (игнорировать файл состояния)")
    ap.add_argument("--dry-run", action="store_true", help="показать план и выйти без запросов")
    args = ap.parse_args()

    state_path = Path(args.state)
    state = _load_state(state_path, args)
    per_city = anyscan.requests_per_city(state.dates)
    print(f"[scan] даты ({len(state.dates)}): {state.dates[0]} … {state.dates[-1]}; "
          f"{per_city} запросов на город; очередь: {', '.join(state.queue[:10])}"
          f"{' …' if len(state.queue) > 10 else ''}")
    if args.dry_run:
        return 0

    require_token()
    conn = hot.connect(args.db)
    hot.init_db(conn)
    from api.worker import _make_cached_fetch, _save_quotes  # noqa: E402  (нужен токен/.env)

    stop = {"flag": False}

    def on_sigint(*_):
        print("\n[scan] остановка после текущего города…")
        stop["flag"] = True
    signal.signal(signal.SIGINT, on_sigint)

    def on_city_done(city, flights):
        now = datetime.now().isoformat()
        part_id = f"anyscan-{city}-{now[:19].replace(':', '')}"
        _save_quotes(conn, flights, now, part_id, lake_root=args.lake_root)
        _save_state(state_path, state)
        print(f"[scan] {city}: {len(flights)} билетов сохранено; "
              f"собрано {len(state.done)} городов, в очереди {len(state.queue)}, "
              f"запросов {state.requests}")

    anyscan.scan_any(state, fetch_fn=_make_cached_fetch(conn), on_city_done=on_city_done,
                     max_cities=args.max_cities, max_requests=args.max_requests,
                     should_stop=lambda: stop["flag"])
    _save_state(state_path, state)
    print(f"[scan] готово: {len(state.done)} городов, в очереди {len(state.queue)}, "
          f"состояние — {state_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
