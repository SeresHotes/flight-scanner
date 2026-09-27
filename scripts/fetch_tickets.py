#!/usr/bin/env python3
"""Билеты на день через GraphQL Data API (core/graphql_api) — для проверки руками.

    poetry run python scripts/fetch_tickets.py MOW SEL 2026-10-15
    poetry run python scripts/fetch_tickets.py MOW - 2026-10-15 --min 20000 --max 40000   # MOW → ANY
    poetry run python scripts/fetch_tickets.py - SEL 2026-10-15 --pages 2 --json           # ANY → SEL

`-` вместо города — «любой». Серии кэшируются в SQLite (ticket_cache, TTL 24 ч),
как у планировщика; --no-cache — мимо кэша.
"""
import argparse
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from api import worker  # noqa: E402
from core import graphql_api  # noqa: E402
from storage import hot  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("origin", help="город/аэропорт вылета или '-' (любой)")
    ap.add_argument("destination", help="город/аэропорт прилёта или '-' (любой)")
    ap.add_argument("day", help="дата вылета YYYY-MM-DD")
    ap.add_argument("--min", type=int, default=None, help="value_min, ₽")
    ap.add_argument("--max", type=int, default=None, help="value_max, ₽")
    ap.add_argument("--direct", action="store_true", help="только прямые")
    ap.add_argument("--baggage", action="store_true", help="только с багажом")
    ap.add_argument("--pages", type=int, default=graphql_api.MAX_PAGES, help="потолок страниц по 400")
    ap.add_argument("--db", default=hot.DEFAULT_DB)
    ap.add_argument("--no-cache", action="store_true")
    ap.add_argument("--json", action="store_true", help="вывести нормализованные билеты JSON")
    ap.add_argument("--limit", type=int, default=30, help="сколько строк печатать")
    args = ap.parse_args()

    origin = None if args.origin == "-" else args.origin.upper()
    destination = None if args.destination == "-" else args.destination.upper()
    opts = dict(value_min=args.min, value_max=args.max, direct=True if args.direct else None,
                with_baggage=True if args.baggage else None, max_pages=args.pages)

    def progress(page, n):
        print(f"[fetch] страница {page}: {n} билетов", file=sys.stderr)

    if args.no_cache:
        series = graphql_api.fetch_series(origin, destination, args.day, progress_cb=progress, **opts)
    else:
        conn = hot.connect(args.db)
        hot.init_db(conn)
        series = worker.make_cached_ticket_fetch(conn)(origin, destination, args.day,
                                                       progress_cb=progress, **opts)
    tickets = series["tickets"]
    if args.json:
        json.dump(tickets, sys.stdout, ensure_ascii=False, indent=1)
        return

    print(f"{origin or 'ANY'} → {destination or 'ANY'} {args.day}: {len(tickets)} билетов, "
          f"страниц {series['pages']}, {'исчерпано' if series['exhausted'] else 'ОБРЕЗАНО'}"
          f"{', из кэша' if series.get('cached') else ''}{', ОШИБКА' if series.get('error') else ''}")
    if not tickets:
        return
    by_transfers = Counter(t["transfers"] for t in tickets)
    by_baggage = Counter("багаж" if t["baggage"]["included"] else
                         ("без багажа" if t["baggage"]["known"] else "?") for t in tickets)
    print(f"пересадки: {dict(sorted(by_transfers.items()))}; {dict(by_baggage)}")
    print(f"направлений: {len({(t['origin'], t['destination']) for t in tickets})}")
    print()
    for t in tickets[:args.limit]:
        bag = t["baggage"]
        bag_s = (f"{bag['pieces'] or 1}×{bag['kg'] or '?'}кг" if bag["included"]
                 else ("без багажа" if bag["known"] else "багаж ?"))
        stops = " ".join(f"{p['code']}({(p['minutes'] or 0) // 60}ч{'🌙' if p['night'] else ''})"
                         for p in t["transfer_points"])
        print(f"{t['price']:>8.0f} ₽  {t['departure_at'][:16]} → {t['arrival_at'][:16]}  "
              f"{'→'.join(t['chain'])}  {t['duration'] // 60 if t['duration'] else '?'}ч  "
              f"{t['airline']}  {bag_s}  {stops}")


if __name__ == "__main__":
    main()
