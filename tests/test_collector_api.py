"""HTTP-сервис коллектора и клиент планировщика поверх него (TestClient вместо сети):
задание → статус → результат, прогресс по страницам, кэш, batch, покрытие, города."""
import pytest
from fastapi.testclient import TestClient

from api import worker
from collector.config import Settings
from collector.engine import Engine
from collector.index import Index
from collector.main import create_app
from collector.store import LocalStore
from core.collector_client import CollectorClient, CollectorError
from tests.test_collector_engine import Source


@pytest.fixture
def client(tmp_path):
    settings = Settings(db_path=":memory:", lake_local_root=str(tmp_path / "lake"), s3_bucket=None,
                        rate_per_minute=100_000, flush_tickets=10_000, flush_seconds=300)
    source = Source({("MOW", "SEL"): [400, 12], ("MOW", ""): [7], ("LED", ""): [0]})
    engine = Engine(settings, LocalStore(settings.lake_local_root), Index(":memory:"), page_fn=source)
    with TestClient(create_app(engine)) as c:
        c.engine, c.source = engine, source
        yield c


def test_fetch_status_result_over_http(client):
    r = client.post("/v1/fetch", json={"origin": "MOW", "destination": "SEL", "day": "2026-10-15",
                                       "client": "app", "wait": 10})
    assert r.status_code == 200
    job = r.json()
    assert job["status"] == "done" and job["pages"] == 2 and job["exhausted"] and job["tickets"] == 412
    res = client.get(f"/v1/requests/{job['id']}/result").json()
    assert len(res["tickets"]) == 412 and res["pages"] == 2 and not res["cached"]
    assert res["tickets"][0]["origin"] == "MOW" and res["tickets"][0]["legs"][0]["origin"] == "SVO"
    # Повторный запрос — из кэша (буфер озера), без похода в источник.
    again = client.post("/v1/fetch", json={"origin": "MOW", "destination": "SEL", "day": "2026-10-15"}).json()
    assert again["cached"] and again["status"] == "done"
    assert client.get("/v1/series/exists", params={"origin": "MOW", "destination": "SEL",
                                                   "day": "2026-10-15"}).json()["exists"]
    assert not client.get("/v1/series/exists", params={"origin": "MOW", "day": "2026-10-15"}).json()["exists"]
    assert client.get("/v1/requests/nope").status_code == 404
    assert client.post("/v1/fetch", json={"day": "2026-10-15"}).status_code == 400
    health = client.get("/v1/health").json()
    assert health["status"] == "ok" and health["series"] == 1


def test_batch_coverage_cities_stats(client):
    r = client.post("/v1/batch", json={"items": [{"origin": "MOW", "day": "2026-10-15"},
                                                 {"origin": "LED", "day": "2026-10-15"}],
                                       "client": "crawl"})
    ids = r.json()["ids"]
    assert len(ids) == 2
    for jid in ids:
        st = client.get(f"/v1/requests/{jid}", params={"wait": 10}).json()
        assert st["status"] == "done" and st["priority"] == "crawl"
    cov = client.get("/v1/coverage").json()
    rows = {row[0]: row for row in cov["rows"]}
    assert rows["MOW"][4] == 7 and rows["LED"][4] == 0 and rows["MOW"][5] is True
    cities = {c["code"] for c in client.get("/v1/cities").json()["cities"]}
    assert {"MOW", "SEL"} <= cities
    client.post("/v1/stats/crawler", json={"wanted": 10, "fresh": 2})
    stats = client.get("/v1/stats").json()
    assert stats["counters"]["series_crawl"] == 2 and stats["crawler"] == {"wanted": 10, "fresh": 2}
    assert stats["queued_crawl"] == 0 and stats["cities"] >= 2


def test_planner_client_progress_and_cache_hit(client):
    cc = CollectorClient("http://testserver", http=client, poll_wait=5)
    ticks = []
    series = cc.fetch_series("MOW", "SEL", "2026-10-15", max_pages=12,
                             progress_cb=lambda p, n: ticks.append(p))
    assert (series["pages"], series["exhausted"], series["error"], series["cached"]) == (2, True, False, False)
    assert len(series["tickets"]) == 412 and ticks == [1, 2]
    assert cc.has_series("MOW", "SEL", "2026-10-15", "", pages=12)
    assert not cc.has_series("MOW", "SEL", "2026-10-15", "max=5000")
    # Обёртка воркера: cached → on_cache_hit, формат серии тот же, что у fetch_series.
    hits = []
    fetch = worker.make_collector_ticket_fetch(cc, on_cache_hit=lambda: hits.append(1))
    s2 = fetch("MOW", "SEL", "2026-10-15", max_pages=12, progress_cb=lambda p, n: None)
    assert s2["cached"] and hits == [1] and len(s2["tickets"]) == 412
    assert cc.health()["status"] == "ok"
    # Сравнение с прямой выборкой: коллектор отдал ровно те же нормализованные билеты.
    from core import graphql_api as g
    direct = g.fetch_series("MOW", "SEL", "2026-10-15", page_fn=client.source)
    assert direct["tickets"] == series["tickets"]


def test_client_reports_unreachable_collector():
    cc = CollectorClient("http://127.0.0.1:9", timeout=0.2)
    with pytest.raises(CollectorError):
        cc.health()
