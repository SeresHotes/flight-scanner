"""GET /api/plan/jobs/{id}: прогресс, по готовности — сводка; данные страницами."""
from api import main, worker
from core import graphql_api
from storage import hot
from tests.test_job_stages import STOPS, _fake_fetch


def _call(job_id, f=None):
    return main.plan_job_status(job_id, f)


class _Sync:
    def submit(self, fn, *args):
        fn(*args)


def test_done_job_returns_compact_result(tmp_path, monkeypatch):
    db = str(tmp_path / "jobs.db")
    conn = hot.connect(db)
    hot.init_db(conn)
    monkeypatch.setattr(main, "_conn", conn)
    monkeypatch.setattr(graphql_api, "fetch_series", _fake_fetch)
    monkeypatch.setattr(worker, "load_airport_network", lambda *a, **k: {})
    monkeypatch.setattr(hot, "DEFAULT_DB", db)
    monkeypatch.setattr(main, "_view_executor", _Sync())
    hot.create_job(conn, "j1", {"kind": "plan", "stops": STOPS, "maxResults": 10}, total=6,
                   stage=worker.initial_stage("plan"))

    main._views.clear()
    main._view_state.clear()
    running = _call("j1")
    assert running["status"] == "pending" and "summary" not in running

    # Воркер без on_view: вида в кэше нет — статус сам стыкует из сохранённых рейсов.
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


def test_view_error_is_reported_and_retried(tmp_path, monkeypatch):
    db = str(tmp_path / "jobs.db")
    conn = hot.connect(db)
    hot.init_db(conn)
    monkeypatch.setattr(main, "_conn", conn)
    monkeypatch.setattr(hot, "DEFAULT_DB", db)
    monkeypatch.setattr(main, "_view_executor", _Sync())
    main._views.clear()
    main._view_state.clear()
    hot.create_job(conn, "j9", {"kind": "plan", "stops": STOPS}, total=1, stage=worker.initial_stage("plan"))
    hot.update_job(conn, "j9", status="done")        # done, но рейсов в plan_flights нет
    first = _call("j9")
    assert first["status"] == "error" and "запустите поиск заново" in first["error"]
    assert main._view_state == {}                      # следующий запрос попробует заново
