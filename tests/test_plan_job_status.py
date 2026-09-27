"""GET /api/plan/jobs/{id}: прогресс, по готовности — сводка; данные страницами."""
from api import main, worker
from core import graphql_api
from storage import hot
from tests.test_job_stages import STOPS, _fake_fetch


def _call(job_id):
    return main.plan_job_status(job_id)


def test_done_job_returns_compact_result(tmp_path, monkeypatch):
    db = str(tmp_path / "jobs.db")
    conn = hot.connect(db)
    hot.init_db(conn)
    monkeypatch.setattr(main, "_conn", conn)
    monkeypatch.setattr(graphql_api, "fetch_series", _fake_fetch)
    monkeypatch.setattr(worker.agg, "load_airport_network", lambda *a, **k: {})
    hot.create_job(conn, "j1", {"kind": "plan"}, total=6, stage=worker.initial_stage("plan"))

    main._plan_results.clear()
    running = _call("j1")
    assert running["status"] == "pending" and "summary" not in running

    worker.run_plan_collection(db, "j1", STOPS, max_results=10)
    done = _call("j1")
    assert done["status"] == "done"
    assert done["summary"]["count"] > 0 and done["summary"]["combos"] == 1
    assert done["summary"]["totalCount"] == done["summary"]["count"]


def test_unknown_job(tmp_path, monkeypatch):
    conn = hot.connect(str(tmp_path / "jobs.db"))
    hot.init_db(conn)
    monkeypatch.setattr(main, "_conn", conn)
    assert _call("nope") == {"status": "not_found"}
