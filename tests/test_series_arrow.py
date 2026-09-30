"""Серия Arrow-таблицей: разбор по колонкам == построчный from_row, и коллектор отдаёт
планировщику ту же серию Arrow IPC-потоком, что и JSON (свежая, из озера, пустая)."""
from collector.lake import from_row, series_table
from core.collector_client import CollectorClient
from core.series_arrow import ARROW_MEDIA_TYPE, ipc_to_table, table_to_ipc, tickets_from_table
from tests.test_collector_api import client  # noqa: F401  (фикстура)
from tests.test_planner_hidden_city import _ticket


def _tickets():
    full = _ticket("MOW", "HRB", ["SVO", "PKX", "HRB"], "2026-10-29T23:15:00+03:00",
                   "2026-10-30T14:00:00+08:00", 25000, number="2")
    bare = {**_ticket("MOW", "SEL", ["SVO", "ICN"], "2026-10-29T10:00:00+03:00",
                      "2026-10-30T01:00:00+09:00", 31000, baggage=False),
            "transfers": None, "duration": None, "chain": [], "legs": None,
            "baggage": {}, "link": None}
    return [full, bare]


def test_columnar_decode_matches_from_row():
    table = series_table(_tickets(), 7, "2026-09-30T00:00:00")
    assert tickets_from_table(table) == [from_row(r) for r in table.to_pylist()]
    assert tickets_from_table(ipc_to_table(table_to_ipc(table))) == tickets_from_table(table)
    assert tickets_from_table(series_table([], 0, "")) == []


def test_columnar_decode_tolerates_missing_columns():
    table = series_table(_tickets(), 7, "").drop_columns(["legs_json", "baggage_kg", "source"])
    rows = [from_row(r) for r in table.to_pylist()]
    assert tickets_from_table(table) == rows
    assert rows[0]["legs"] == [] and rows[0]["baggage"]["kg"] is None and rows[0]["source"] is None


def test_collector_serves_arrow_same_as_json(client):  # noqa: F811
    job = client.post("/v1/fetch", json={"origin": "MOW", "destination": "SEL", "day": "2026-10-15",
                                         "wait": 10}).json()
    fresh = client.get(f"/v1/requests/{job['id']}/result", params={"format": "arrow"})
    assert fresh.headers["content-type"].startswith(ARROW_MEDIA_TYPE)
    fresh_tickets = tickets_from_table(ipc_to_table(fresh.content))
    assert len(fresh_tickets) == 412

    cached = client.post("/v1/fetch", json={"origin": "MOW", "destination": "SEL", "day": "2026-10-15"}).json()
    as_json = client.get(f"/v1/requests/{cached['id']}/result").json()
    as_arrow = client.get(f"/v1/requests/{cached['id']}/result", params={"format": "arrow"})
    assert as_json["cached"] and as_json["tickets"] == fresh_tickets
    assert tickets_from_table(ipc_to_table(as_arrow.content)) == as_json["tickets"]

    cc = CollectorClient("http://testserver", http=client, poll_wait=5)
    series = cc.fetch_series("MOW", "SEL", "2026-10-15", max_pages=12)
    assert series["cached"] and series["tickets"] == as_json["tickets"]
    empty = cc.fetch_series("LED", None, "2026-10-15", max_pages=12)
    assert empty["tickets"] == [] and not empty["error"]


def test_client_falls_back_to_json_from_old_collector():
    class Resp:
        status_code = 200
        headers = {"content-type": "application/json"}

        def __init__(self, body):
            self.body = body

        def json(self):
            return self.body

    class OldCollector:
        def post(self, url, json=None, timeout=None):
            return Resp({"id": "j1", "status": "done", "pages": 1, "tickets": 1})

        def get(self, url, params=None, timeout=None):
            return Resp({"tickets": [{"origin": "MOW"}], "pages": 1, "exhausted": True,
                         "error": False, "cached": True})

    series = CollectorClient("http://old", http=OldCollector()).fetch_series("MOW", None, "2026-10-15")
    assert series["tickets"] == [{"origin": "MOW"}] and series["cached"]


def test_collector_client_imports_without_pyarrow():
    """Краулер грузит core.collector_client, а pyarrow в его образе нет."""
    import subprocess
    import sys
    code = "import sys; sys.modules['pyarrow'] = None; import core.collector_client, crawler.main"
    subprocess.run([sys.executable, "-c", code], check=True)
