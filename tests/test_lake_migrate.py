"""Перенос озера из раскладки date=/origin= в fetched=/origin= (collector.migrate)."""
import io
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

from collector import lake
from collector.index import Index
from collector.migrate import migrate
from collector.store import LocalStore
from core import graphql_api as g
from tests.test_collector_engine import FIXTURE

OBS = datetime(2026, 9, 30, 7, 48, 26, tzinfo=timezone.utc)


def _legacy(store, index, key, day, series):
    """Файл старой раскладки: series — [(день, билеты)], row group на непустой день."""
    ids, sink = [], io.BytesIO()
    with pq.ParquetWriter(sink, lake.SCHEMA, compression="zstd") as w:
        rg, placements = 0, []
        for d, tickets in series:
            sid = index.put("MOW", None, d, "", pages=1, exhausted=True, tickets=len(tickets), fetched_at=OBS)
            ids.append(sid)
            if tickets:
                w.write_table(lake.series_table(tickets, sid, OBS.isoformat()), row_group_size=len(tickets))
                placements.append((sid, rg))
                rg += 1
            else:
                placements.append((sid, 0))
        if rg == 0:
            w.write_table(lake.series_table([], ids[0], OBS.isoformat()))
    store.put_bytes(key, sink.getvalue())
    index.attach_file(key, day, len(sink.getvalue()), placements, observed=OBS)
    return ids


def test_migrate_moves_files_and_index(tmp_path):
    store, index = LocalStore(str(tmp_path)), Index(":memory:")
    tickets = [t for t in (g.normalize_ticket(r, "MOW", None, "2026-10-15") for r in FIXTURE["mow_any"]) if t]
    one = "tickets/date=2026-10-15/origin=MOW/MOW-ANY__2026-09-30T07-48-26Z.parquet"
    win = "tickets/date=2026-10-16/origin=MOW/MOW-ANY__to=2026-10-17__2026-09-30T07-48-26Z.parquet"
    _legacy(store, index, one, "2026-10-15", [("2026-10-15", tickets)])
    _legacy(store, index, win, "2026-10-16", [("2026-10-16", []), ("2026-10-17", tickets[:2])])
    index.attach_file("tickets/date=2026-10-15/origin=X/chuzhoy.parquet", "2026-10-15", 1, [], observed=OBS)
    logs = []
    stats = migrate(store, index, workers=4, delete_grace=0, log=logs.append)
    assert stats == {"legacy": 3, "unparsed": 1, "moved": 2, "missing": 0, "deleted": 2}
    new_one = "tickets/fetched=2026-09-30/origin=MOW/MOW-ANY__2026-10-15__07-48-26Z.parquet"
    new_win = "tickets/fetched=2026-09-30/origin=MOW/MOW-ANY__2026-10-16..2026-10-17__07-48-26Z.parquet"
    assert (tmp_path / new_one).exists() and (tmp_path / new_win).exists()
    assert not (tmp_path / one).exists() and not (tmp_path / win).exists()
    writer = lake.LakeWriter(store, index)
    for day, n in (("2026-10-15", len(tickets)), ("2026-10-16", 0), ("2026-10-17", 2)):
        row = index.get("MOW", None, day, "")
        assert row["file_key"] in (new_one, new_win)
        got = writer.read(row["file_key"], row["row_group"]) if row["tickets"] else []
        assert len(got) == n
    keys = {f["key"] for f in index.files_oldest_first()}
    assert new_one in keys and new_win in keys and one not in keys
    # Повторный запуск — переносить нечего (кроме чужого файла).
    assert migrate(store, index, delete_grace=0, log=logs.append)["moved"] == 0
