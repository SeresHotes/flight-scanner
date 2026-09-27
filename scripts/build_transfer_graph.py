#!/usr/bin/env python3
"""CLI: строит граф пересадок из quotes (SQLite) и пишет data/transfer_graph.json.

  poetry run python scripts/build_transfer_graph.py [--db data/flights.db] [--out data/transfer_graph.json]

Формат — core.transfer_graph.TransferGraph.to_dict(): nodes (аэропорты),
edges (прямые перелёты-сегменты), nonstop и transfers (наблюдённые билеты).
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core import aggregate as agg  # noqa: E402
from core.transfer_graph import build_from_db  # noqa: E402
from storage import hot  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=hot.DEFAULT_DB)
    ap.add_argument("--out", default="data/transfer_graph.json")
    ap.add_argument("--network", default="data/airport_network.json",
                    help="сеть аэропортов (страны для вершин); если нет — без стран")
    args = ap.parse_args()

    network = agg.load_airport_network(args.network) if Path(args.network).exists() else {}
    conn = hot.connect(args.db)
    graph = build_from_db(conn, network)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(graph.to_dict(), ensure_ascii=False), encoding="utf-8")
    print(f"[graph] {graph.stats()} → {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
