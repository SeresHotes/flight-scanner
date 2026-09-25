"""Кнопка «Починить»: сброс зависших джоб, чтобы освободить однопоточный воркер.

Поток Python снаружи не убить, поэтому отмена кооперативная: /api/jobs/rescue
помечает молчащую running-джобу error и просит воркер её бросить — воркер видит
флаг на следующем запросе к API или шаге стыковки и выходит, не перетирая статус.
"""
import time
from datetime import datetime, timedelta

import pytest

from api import worker
from core import collector
from core.planner import SearchAborted, build_itineraries
from storage import hot
from tests.test_job_stages import STOPS, _fake_fetch
from tests.test_planner_max_cost import CITY_INFO, COLLECTED
from tests.test_planner_max_cost import STOPS as PLAN_STOPS


def _db(tmp_path):
    db = str(tmp_path / "jobs.db")
    conn = hot.connect(db)
    hot.init_db(conn)
    return db, conn


def _age(conn, job_id, seconds):
    old = (datetime.now() - timedelta(seconds=seconds)).isoformat()
    conn.execute("UPDATE jobs SET updated_at=? WHERE id=?", (old, job_id))
    conn.commit()


def test_find_hung_jobs_only_silent_running(tmp_path):
    _, conn = _db(tmp_path)
    for job_id, status in [("hung", "running"), ("live", "running"), ("queued", "pending")]:
        hot.create_job(conn, job_id, {})
        hot.update_job(conn, job_id, status=status)
    _age(conn, "hung", 120)
    _age(conn, "queued", 120)  # очередь молчит законно — её не трогаем

    assert hot.find_hung_jobs(conn, 60) == ["hung"]


def test_search_aborts_when_should_stop():
    with pytest.raises(SearchAborted):
        build_itineraries(PLAN_STOPS, COLLECTED, city_info=CITY_INFO,
                          max_results=10, should_stop=lambda: True)
    with pytest.raises(SearchAborted):
        build_itineraries(PLAN_STOPS, COLLECTED, city_info=CITY_INFO,
                          max_results=None, should_stop=lambda: True)


def test_cancelled_plan_job_keeps_rescued_status(tmp_path, monkeypatch):
    db, conn = _db(tmp_path)
    hot.create_job(conn, "j1", {"kind": "plan"}, total=6, stage=worker.initial_stage("plan"))
    hot.update_job(conn, "j1", status="error", error="сброшена")
    monkeypatch.setattr(collector, "fetch_flights", _fake_fetch)
    monkeypatch.setattr(worker.agg, "load_airport_network", lambda *a, **k: {})

    worker.request_cancel("j1")
    worker.run_plan_collection(db, "j1", STOPS, max_results=10)

    job = hot.get_job(conn, "j1")
    assert (job["status"], job["error"]) == ("error", "сброшена")
    assert not worker.is_cancel_requested("j1")  # флаг не течёт после выхода


def test_rescue_endpoint_fails_hung_and_requests_cancel(tmp_path, monkeypatch):
    from api import main

    _, conn = _db(tmp_path)
    monkeypatch.setattr(main, "_conn", conn)
    hot.create_job(conn, "hung", {})
    hot.update_job(conn, "hung", status="running")
    _age(conn, "hung", main.HUNG_JOB_SECONDS + 5)

    assert main.rescue_jobs() == {"status": "ok", "rescued": ["hung"]}
    job = hot.get_job(conn, "hung")
    assert (job["status"], job["error"]) == ("error", main.RESCUED_JOB_ERROR)
    assert worker.is_cancel_requested("hung")
    assert main.rescue_jobs()["rescued"] == []  # повторное нажатие — no-op


def test_rescue_unblocks_queue_behind_endless_build(tmp_path, monkeypatch):
    """Главный сценарий: воркер висит в стыковке, следующая джоба ждёт в очереди.
    После rescue висящая выходит (error), а очередь доходит до следующей (done)."""
    from concurrent.futures import ThreadPoolExecutor

    from api import main

    db, conn = _db(tmp_path)
    monkeypatch.setattr(main, "_conn", conn)
    monkeypatch.setattr(main, "HUNG_JOB_SECONDS", 0)
    monkeypatch.setattr(collector, "fetch_flights", _fake_fetch)
    monkeypatch.setattr(worker.agg, "load_airport_network", lambda *a, **k: {})
    real_build = worker.planner.build_itineraries

    def build(stops, collected, **kw):
        if kw.get("max_results") == 1:  # «зависающая» джоба: бесконечный перебор
            while True:
                if kw["should_stop"]():
                    raise SearchAborted()
        return real_build(stops, collected, **kw)

    monkeypatch.setattr(worker.planner, "build_itineraries", build)
    pool = ThreadPoolExecutor(max_workers=1)
    for job_id in ("stuck", "next"):
        hot.create_job(conn, job_id, {"kind": "plan"}, stage=worker.initial_stage("plan"))
    stuck = pool.submit(worker.run_plan_collection, db, "stuck", STOPS, 1)
    nxt = pool.submit(worker.run_plan_collection, db, "next", STOPS, 10)

    for _ in range(200):  # ждём, пока воркер дойдёт до стыковки
        job = hot.get_job(conn, "stuck")
        if job["status"] == "running" and '"key": "build"' in job["stage_json"]:
            break
        time.sleep(0.01)
    assert main.rescue_jobs()["rescued"] == ["stuck"]

    stuck.result(timeout=5)
    nxt.result(timeout=5)
    assert hot.get_job(conn, "stuck")["status"] == "error"
    assert hot.get_job(conn, "next")["status"] == "done"
