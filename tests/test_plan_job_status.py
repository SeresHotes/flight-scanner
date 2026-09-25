"""GET /api/plan/jobs/{id}: готовый результат вклеивается в ответ как есть.

Компактный результат на сотни тысяч цепочек весит десятки МБ — ручка не должна его
разбирать (json.loads + сериализация FastAPI держали бы в памяти лишние копии)."""
import json

from api import main, worker
from core import collector
from storage import hot
from tests.test_job_stages import STOPS, _fake_fetch


def _call(job_id):
    resp = main.plan_job_status(job_id)
    return json.loads(resp.body)


def test_done_job_returns_compact_result(tmp_path, monkeypatch):
    db = str(tmp_path / "jobs.db")
    conn = hot.connect(db)
    hot.init_db(conn)
    monkeypatch.setattr(main, "_conn", conn)
    monkeypatch.setattr(collector, "fetch_flights", _fake_fetch)
    monkeypatch.setattr(worker.agg, "load_airport_network", lambda *a, **k: {})
    hot.create_job(conn, "j1", {"kind": "plan"}, total=6, stage=worker.initial_stage("plan"))

    running = _call("j1")
    assert running["status"] == "pending" and "result" not in running

    worker.run_plan_collection(db, "j1", STOPS, max_results=10)
    done = _call("j1")
    assert done["status"] == "done"
    assert done["result"]["format"] == "compact-v1"
    assert done["result"]["count"] > 0
    assert len(done["result"]["chains"]) == done["result"]["count"] * done["result"]["legs"]
    assert done["result"]["graph"]["legs"]  # граф для режима «наборы городов» рядом с цепочками


def test_unknown_job(tmp_path, monkeypatch):
    conn = hot.connect(str(tmp_path / "jobs.db"))
    hot.init_db(conn)
    monkeypatch.setattr(main, "_conn", conn)
    assert _call("nope") == {"status": "not_found"}
