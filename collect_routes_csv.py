#!/usr/bin/env python3
"""
Collect direct and connecting itineraries for a fixed city pair (origin → destination)
via Travelpayouts prices_for_dates, and write a CSV for price/date comparison.

Per calendar day this runs two API queries (rates can “collapse” to a single offer if
you only pass direct=false): direct=true for nonstops, then direct=false and keep only
rows with transfers > 0 as connecting options.

Uses IATA city codes (e.g. MOW, EVN), same as collect_flights.py.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


CSV_COLUMNS = [
    "source_query",
    "route_type",
    "origin_city",
    "destination_city",
    "origin_airport",
    "destination_airport",
    "search_date",
    "departure_at",
    "duration_min",
    "transfers",
    "price",
    "currency",
    "airline",
    "flight_number",
    "gate",
    "link",
]


def flight_to_row(
    origin_city: str,
    destination_city: str,
    search_date: str,
    flight: Dict[str, Any],
    currency: str,
    source_query: str,
) -> Dict[str, Any]:
    transfers = flight.get("transfers")
    if transfers is None:
        transfers = 0
    route_type = "nonstop" if transfers == 0 else "connecting"
    return {
        "source_query": source_query,
        "route_type": route_type,
        "origin_city": origin_city,
        "destination_city": destination_city,
        "origin_airport": flight.get("origin_airport") or "",
        "destination_airport": flight.get("destination_airport") or "",
        "search_date": search_date,
        "departure_at": flight.get("departure_at") or "",
        "duration_min": flight.get("duration", ""),
        "transfers": transfers,
        "price": flight.get("price", ""),
        "currency": currency,
        "airline": flight.get("airline") or "",
        "flight_number": flight.get("flight_number") or "",
        "gate": flight.get("gate") or "",
        "link": flight.get("link") or "",
    }


def _transfer_count(flight: Dict[str, Any]) -> int:
    t = flight.get("transfers")
    if t is None:
        return 0
    try:
        return int(t)
    except (TypeError, ValueError):
        return 0


def collect_od_rows(
    fetch_flights_fn,
    origin: str,
    destination: str,
    dates: List[str],
    currency: str,
    limit: int,
    sleep_s: float,
    unique: bool,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    total = len(dates)
    for i, day in enumerate(dates, start=1):
        print(f"[{i}/{total}] {origin} → {destination} on {day}", flush=True)

        result_direct = fetch_flights_fn(
            origin=origin,
            destination=destination,
            departure_at=day,
            currency=currency,
            unique=unique,
            limit=limit,
            allow_indirect=False,
        )
        direct_data = result_direct.get("data") or []
        print(f"  direct (API direct=true): {len(direct_data)} offer(s)")
        for flight in direct_data:
            rows.append(
                flight_to_row(
                    origin, destination, day, flight, currency, "direct_only"
                )
            )
        time.sleep(sleep_s)

        result_indirect = fetch_flights_fn(
            origin=origin,
            destination=destination,
            departure_at=day,
            currency=currency,
            unique=unique,
            limit=limit,
            allow_indirect=True,
        )
        all_data = result_indirect.get("data") or []
        connecting = [f for f in all_data if _transfer_count(f) > 0]
        print(
            f"  connecting (API direct=false, transfers>0): "
            f"{len(connecting)} of {len(all_data)} offer(s)",
            flush=True,
        )
        for flight in connecting:
            rows.append(
                flight_to_row(
                    origin, destination, day, flight, currency, "all_itineraries"
                )
            )
        time.sleep(sleep_s)
    return rows


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Export nonstop and connecting routes to CSV: two API calls per day "
            "(direct=true, then direct=false keeping only transfers>0)."
        ),
    )
    parser.add_argument("origin", help="IATA city code of origin (e.g. MOW)")
    parser.add_argument("destination", help="IATA city code of destination (e.g. EVN)")
    parser.add_argument(
        "--dates",
        nargs=2,
        metavar=("START", "END"),
        required=True,
        help="Inclusive date range YYYY-MM-DD .. YYYY-MM-DD",
    )
    parser.add_argument("--currency", default="RUB", help="Price currency (default RUB)")
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Max offers per day from API (default 1000)",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="Seconds between API calls (default 0.5)",
    )
    parser.add_argument(
        "--no-unique",
        action="store_true",
        help="Do not pass unique=true to API (more rows, possible duplicates)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output CSV path (default data/routes_ORIGIN_DEST_TIMESTAMP.csv)",
    )
    args = parser.parse_args()

    from collect_flights import fetch_flights, get_date_range

    dates = get_date_range(args.dates[0], args.dates[1])
    if not dates:
        print("Empty date range.", file=sys.stderr)
        sys.exit(1)

    out = args.output
    if not out:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = str(Path("data") / f"routes_{args.origin}_{args.destination}_{ts}.csv")

    print(f"{args.origin} → {args.destination}, {len(dates)} day(s), currency={args.currency}")
    rows = collect_od_rows(
        fetch_flights,
        args.origin.upper(),
        args.destination.upper(),
        dates,
        args.currency,
        args.limit,
        args.sleep,
        unique=not args.no_unique,
    )
    write_csv(Path(out), rows)

    nonstop = sum(1 for r in rows if r["route_type"] == "nonstop")
    conn = sum(1 for r in rows if r["route_type"] == "connecting")
    print(f"Wrote {len(rows)} rows ({nonstop} nonstop, {conn} connecting) → {out}")


if __name__ == "__main__":
    main()
