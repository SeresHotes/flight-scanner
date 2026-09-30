"""Параллельный сбор серий (collect_plan workers) и чтение озера из S3 одним GET.

Серии плеча качаются пулом по единицам «день × город», разбор — в порядке обхода:
результат с workers > 1 обязан совпадать с последовательным, даже если серии
завершаются в другом порядке."""
import io
import random
import threading
import time

import pyarrow as pa
import pyarrow.parquet as pq

from collector.store import S3Store
from core.planner import Stop, collect_plan
from tests.test_planner_hidden_city import _series, _ticket

DAYS = ["2026-10-29", "2026-10-30", "2026-10-31"]


def _day_tickets(origin, dest, day, n):
    hub = {"BJS": "PKX", "SEL": "ICN", "TAS": "TAS"}.get(dest, dest)
    return [_ticket(origin, dest, ["SVO", "IST", hub][: 2 + (k % 2)] if k % 2 else ["SVO", hub],
                    f"{day}T0{k}:00:00+03:00", f"{day}T1{k}:00:00+08:00", 20000 + 1000 * k,
                    number=f"{origin}{dest}{day[-2:]}{k}")
            for k in range(n)]


def _fetch_factory(calls, lock):
    rnd = random.Random(7)

    def fetch(origin=None, destination=None, day=None, *, value_max=None, value_min=None,
              max_pages=None, progress_cb=None, **_):
        time.sleep(rnd.random() * 0.01)   # серии завершаются вразнобой
        with lock:
            calls.append((origin, destination, day, value_min, value_max))
        if progress_cb:
            progress_cb(1, 0)
        if origin and destination:
            return _series(_day_tickets(origin, destination, day, 3))
        if origin:   # A→ANY: сквозные рейсы через хабы остановок + прочие города
            return _series(_day_tickets(origin, "HRB", day, 2) + _day_tickets(origin, "BJS", day, 2))
        return _series(_day_tickets("DXB", destination, day, 2) + _day_tickets("IST", destination, day, 1))
    return fetch


def _collect(stops, workers):
    calls, ticks, lock = [], [], threading.Lock()
    tick_lock = threading.Lock()

    def tick():
        with tick_lock:
            ticks.append(1)
    collected = collect_plan(stops, progress_cb=tick, fetch_fn=_fetch_factory(calls, lock),
                             max_cost=90000, workers=workers)
    return collected, sorted(calls, key=repr), len(ticks)


def test_parallel_collect_matches_sequential_for_all_leg_kinds():
    window = [DAYS[0], DAYS[-1]]
    stops = [Stop("cities", ["MOW", "LED"], window), Stop("cities", ["BJS", "SEL"], window),
             Stop("any", [], window), Stop("cities", ["TAS"], window)]
    seq, seq_calls, seq_ticks = _collect(stops, 1)
    par, par_calls, par_ticks = _collect(stops, 4)
    assert seq and all(seq.values())
    assert par == seq                      # те же рейсы в том же порядке, включая hidden-city
    assert par_calls == seq_calls          # те же серии (коридоры hidden-city не поехали)
    assert par_ticks == seq_ticks


def test_parallel_collect_propagates_errors():
    stops = [Stop("cities", ["MOW"], [DAYS[0], DAYS[-1]]), Stop("cities", ["BJS"], [DAYS[0], DAYS[-1]])]

    def boom(*_a, **_k):
        raise RuntimeError("отмена")
    try:
        collect_plan(stops, fetch_fn=boom, workers=4)
    except RuntimeError as e:
        assert str(e) == "отмена"
    else:
        raise AssertionError("ошибка серии должна дойти до джобы")


class _FakeS3:
    def __init__(self, objects):
        self.objects, self.gets = objects, []

    def get_object(self, Bucket, Key):
        self.gets.append((Bucket, Key))
        return {"Body": io.BytesIO(self.objects[Key])}


def test_s3_store_reads_object_with_single_get():
    sink = io.BytesIO()
    pq.write_table(pa.table({"price": [1.0, 2.0]}), sink)
    store = S3Store.__new__(S3Store)
    store.bucket, store._s3 = "lake", _FakeS3({"a.parquet": sink.getvalue()})
    with store.open_input_file("a.parquet") as f:
        assert pq.ParquetFile(f).read_row_group(0).column("price").to_pylist() == [1.0, 2.0]
    assert store._s3.gets == [("lake", "a.parquet")]
