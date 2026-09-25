"""Этапы сбора в jobs.stage_json: очередь → загрузка (переход i из N) → стыковка →
сохранение. По ним фронт рисует степпер «на каком этапе и сколько сделано»."""
import json

from api import worker
from core import collector
from storage import hot

STOPS = [
    {"kind": "cities", "codes": ["MOW"], "window": ["", ""]},
    {"kind": "cities", "codes": ["IST"], "window": ["2026-11-01", "2026-11-03"]},
    {"kind": "cities", "codes": ["DST"], "window": ["", ""]},
]


def _flight(origin, dest, dep, price):
    return {
        "origin": origin, "origin_airport": origin,
        "destination": dest, "destination_airport": dest,
        "departure_at": dep, "duration": 180, "price": price, "transfers": 0,
        "airline": "XX", "flight_number": "1", "link": "",
    }


def _fake_fetch(origin=None, destination=None, departure_at=None, **_):
    day = departure_at[:10]
    if origin == "MOW":
        return {"data": [_flight("MOW", "IST", f"{day}T10:00:00", 100)]}
    return {"data": [_flight("IST", "DST", f"{day}T20:00:00", 100)]}


def _run(tmp_path, monkeypatch):
    db = str(tmp_path / "jobs.db")
    conn = hot.connect(db)
    hot.init_db(conn)
    hot.create_job(conn, "j1", {"kind": "plan"}, total=6, stage=worker.initial_stage("plan"))

    monkeypatch.setattr(collector, "fetch_flights", _fake_fetch)
    monkeypatch.setattr(worker.agg, "load_airport_network", lambda *a, **k: {})
    snapshots = []
    orig_update = hot.update_job

    def spy(c, job_id, **fields):
        if "stage_json" in fields:
            snapshots.append(json.loads(fields["stage_json"]))
        orig_update(c, job_id, **fields)

    monkeypatch.setattr(hot, "update_job", spy)
    worker.run_plan_collection(db, "j1", STOPS)
    return conn, snapshots


def test_initial_stage_is_queued_with_all_stages(tmp_path):
    conn = hot.connect(str(tmp_path / "jobs.db"))
    hot.init_db(conn)
    hot.create_job(conn, "j0", {}, total=1, stage=worker.initial_stage("plan"))
    stage = json.loads(hot.get_job(conn, "j0")["stage_json"])
    assert stage["key"] == "queued"
    assert [s["key"] for s in stage["stages"]] == ["queued", "fetch", "build", "save"]


def test_plan_job_walks_stages_and_counts_steps(tmp_path, monkeypatch):
    conn, snaps = _run(tmp_path, monkeypatch)
    job = hot.get_job(conn, "j1")
    assert job["status"] == "done", job["error"]

    # Этапы идут по порядку без пропусков.
    keys = [s["key"] for s in snaps]
    assert [k for i, k in enumerate(keys) if i == 0 or keys[i - 1] != k] == ["fetch", "build", "save"]

    # Два перехода по 3 дня окна; на каждом счётчик доходит до total.
    fetch_steps = [s["step"] for s in snaps if s["key"] == "fetch" and s["step"]]
    last_per_leg = {st["index"]: st for st in fetch_steps}
    assert set(last_per_leg) == {0, 1}
    assert all(st["count"] == 2 for st in fetch_steps)
    assert [(st["done"], st["total"]) for st in last_per_leg.values()] == [(3, 3), (3, 3)]
    assert last_per_leg[0]["label"].endswith("(IST)") and "→" in last_per_leg[0]["label"]

    assert snaps[-1]["flights"] == 6
    assert job["progress"] == 6


def test_cache_hits_are_counted(tmp_path, monkeypatch):
    conn, _ = _run(tmp_path, monkeypatch)  # первый прогон наполняет fetch_cache
    hot.create_job(conn, "j2", {"kind": "plan"}, total=6, stage=worker.initial_stage("plan"))
    worker.run_plan_collection(str(tmp_path / "jobs.db"), "j2", STOPS)
    stage = json.loads(hot.get_job(conn, "j2")["stage_json"])
    assert stage["cached"] == 6


def test_init_db_migrates_old_jobs_table(tmp_path):
    conn = hot.connect(str(tmp_path / "old.db"))
    conn.execute("CREATE TABLE jobs (id TEXT PRIMARY KEY, params_json TEXT, status TEXT, "
                 "progress INTEGER, total INTEGER, result_json TEXT, error TEXT, "
                 "created_at TEXT, updated_at TEXT)")
    hot.init_db(conn)
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(jobs)")}
    assert "stage_json" in cols
