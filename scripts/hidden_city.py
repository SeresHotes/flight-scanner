#!/usr/bin/env python3
"""CLI-запросы к графу пересадок (core.transfer_graph).

  # hidden-city: билеты из MOW через PEK (выходим в PEK) дешевле прямого MOW→PEK
  poetry run python scripts/hidden_city.py MOW PEK
  poetry run python scripts/hidden_city.py MOW BJS --same-day --limit 30

  # какие пересадки наблюдались по направлению MOW→HRB
  poetry run python scripts/hidden_city.py MOW HRB --transfers

Граф строится из quotes (--db) на лету или читается из JSON (--graph), собранного
scripts/build_transfer_graph.py. Коды — город (MOW) или аэропорт (SVO).
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core import aggregate as agg  # noqa: E402
from core.transfer_graph import TransferGraph, build_from_db  # noqa: E402
from storage import hot  # noqa: E402


def _load_graph(args) -> TransferGraph:
    network = agg.load_airport_network(args.network) if Path(args.network).exists() else {}
    if args.graph:
        data = json.loads(Path(args.graph).read_text(encoding="utf-8"))
        return TransferGraph.from_dict(data, network)
    return build_from_db(hot.connect(args.db), network)


def _bag(b) -> str:
    if not b["known"]:
        return "багаж ?"
    if not b["included"]:
        return "без багажа"
    return f"багаж {b['pieces'] or 1}×{b['kg'] or '?'}кг"


def _print_hidden(res, limit: int) -> None:
    print(f"{res['origin']} {res['origin_airports']} → через {res['via']} {res['via_airports']}: "
          f"прямой min {res['direct_min_price']} (дней с прямым: {res['direct_days']}), "
          f"кандидатов: {len(res['tickets'])}")
    for t in res["tickets"][:limit]:
        saving = f"−{t['saving']:.0f}" if t["saving"] is not None else "прямого нет"
        day = "" if t["direct_same_day"] else " (прямой — другой день)"
        dom = " [след. сегмент внутренний]" if t["next_leg_domestic"] else ""
        print(f"  {t['date']} {'→'.join(t['airports'])} {t['airline']} {t['price']:.0f} "
              f"vs прямой {t['direct_price']} {saving}{day}; {_bag(t['baggage'])}{dom}\n"
              f"      {t['url']}")


def _print_transfers(res, limit: int) -> None:
    print(f"{res['origin']} → {res['destination']}: прямой min {res['nonstop_min_price']} "
          f"({res['nonstop_tickets']} билетов); вариантов пересадок: {len(res['via'])}")
    for v in res["via"][:limit]:
        print(f"  через {'→'.join(v['via'])}: min {v['min_price']:.0f}, билетов {v['tickets']}, "
              f"{','.join(v['airlines'])}, {v['dates'][0]}…{v['dates'][1]}, "
              f"с багажом {v['baggage_included']}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("origin")
    ap.add_argument("target", help="H для hidden-city или X для --transfers")
    ap.add_argument("--transfers", action="store_true", help="режим «пересадки по направлению»")
    ap.add_argument("--same-day", action="store_true", help="сравнивать только с прямым на тот же день")
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--json", action="store_true", help="сырой JSON вместо текста")
    ap.add_argument("--db", default=hot.DEFAULT_DB)
    ap.add_argument("--graph", help="готовый data/transfer_graph.json вместо БД")
    ap.add_argument("--network", default="data/airport_network.json")
    args = ap.parse_args()

    graph = _load_graph(args)
    if args.transfers:
        res = graph.transfers_for(args.origin, args.target)
    else:
        res = graph.hidden_city(args.origin, args.target, same_day_only=args.same_day)
    if args.json:
        print(json.dumps(res, ensure_ascii=False, indent=1))
    elif args.transfers:
        _print_transfers(res, args.limit)
    else:
        _print_hidden(res, args.limit)
    return 0


if __name__ == "__main__":
    sys.exit(main())
