"""PlanQuery: разбор/ключ, фильтры плеча до перебора, фильтры городов внутри перебора,
длина поездки при выдаче, страница маршрутов и дедуп джоб по хэшу запроса."""
import json

from api import main, worker
from core import graphql_api, planner
from core.planner import Stop, build_itineraries_compact, materialize, routes_page
from core.planquery import LegFilter, PlanQuery
from storage import hot
from tests.test_job_stages import STOPS as JOB_STOPS, _fake_fetch

CITY = lambda code: {"city": code, "country": "", "flag": ""}  # noqa: E731


def _flight(origin, dest, dep, arr, price, transfers=0, points=None, baggage=True, hidden=None, number="1"):
    pts = [{"code": c, "minutes": m, "night": False, "visa": False} for c, m in (points or [])]
    return {
        "origin": origin, "origin_airport": origin, "destination": dest, "destination_airport": dest,
        "departure_at": dep, "arrival_at": arr, "duration": 600, "duration_to": 600 - sum(m for _, m in (points or [])),
        "price": price, "transfers": transfers, "airline": "XX", "flight_number": number, "link": "",
        "chain": [origin] + [c for c, _ in (points or [])] + [dest], "legs": [], "transfer_points": pts,
        "baggage": {"known": True, "included": baggage, "pieces": 1 if baggage else None, "kg": 23 if baggage else None},
        **({"hidden_city": hidden} if hidden else {}),
    }


# ------------------------------ разбор и ключ --------------------------------

def test_parse_defaults_and_key_is_stable():
    q = PlanQuery.from_dict({"stops": [{"kind": "cities", "codes": ["mow"], "window": ["", ""]},
                                       {"kind": "any", "codes": [], "window": ["2026-11-01", "2026-11-03"]},
                                       {"kind": "cities", "codes": ["SEL"], "window": ["", ""]}],
                             "maxResults": 100})
    assert q.stops[0]["codes"] == ["MOW"] and len(q.cities) == 3 and len(q.legs) == 2
    assert all(c.is_open() for c in q.cities) and q.trip_length == [0, None]
    assert q.legs[0].max_transfers == -1 and q.legs[0].baggage == "any" and q.legs[0].hidden_city
    assert q.mode() == "combos"
    same = PlanQuery.from_dict(json.loads(json.dumps(q.as_dict())))
    assert same.key() == q.key()
    other = PlanQuery.from_dict({**q.as_dict(), "legs": [{"baggage": "included"}, {}]})
    assert other.key() != q.key()
    assert PlanQuery.from_dict({"stops": q.stops[:1] + q.stops[2:]}).mode() == "routes"
    multi = {"kind": "cities", "codes": ["IST", "DXB"], "window": ["2026-11-01", "2026-11-03"]}
    assert PlanQuery.from_dict({"stops": [q.stops[0], multi, q.stops[2]]}).mode() == "routes"


# --------------------------- фильтр плеча (accepts) ---------------------------

def test_leg_filter_accepts():
    direct = _flight("MOW", "IST", "2026-11-01T10:00:00", "2026-11-01T14:00:00", 100)
    conn = _flight("MOW", "IST", "2026-11-01T10:00:00", "2026-11-01T20:00:00", 80, transfers=1,
                   points=[("SVX", 90)], baggage=False)
    virt = _flight("MOW", "IST", "2026-11-01T10:00:00", "2026-11-01T14:00:00", 70,
                   hidden={"final": "ANK"})
    assert LegFilter().accepts(conn) and LegFilter().accepts(virt)
    assert not LegFilter(max_transfers=0).accepts(conn) and LegFilter(max_transfers=0).accepts(direct)
    assert not LegFilter(min_layover_min=120).accepts(conn) and LegFilter(min_layover_min=60).accepts(conn)
    assert not LegFilter(travel_min=[0, 300]).accepts(conn) and LegFilter(travel_min=[0, 600]).accepts(direct)
    assert not LegFilter(baggage="included").accepts(conn) and LegFilter(baggage="included").accepts(direct)
    assert LegFilter(baggage="none").accepts(conn) and not LegFilter(baggage="none").accepts(direct)
    assert not LegFilter(hidden_city=False).accepts(virt)


# ------------------------ фильтры в построении цепочек ------------------------

STOPS3 = [Stop("cities", ["MOW"], ["", ""]), Stop("cities", ["IST"], ["2026-11-01", "2026-11-06"]),
          Stop("cities", ["SEL"], ["", ""])]


def _collected():
    """MOW→IST 1.11; IST→SEL 2.11 (1 день в IST), 4.11 (3 дня), 8.11 (7 дней, сб+вс 7–8.11)."""
    leg0 = [_flight("MOW", "IST", "2026-11-01T10:00:00", "2026-11-01T14:00:00", 100),
            _flight("MOW", "IST", "2026-11-01T09:00:00", "2026-11-01T20:00:00", 50, transfers=1,
                    points=[("SVX", 120)], baggage=False, number="2")]
    leg1 = [_flight("IST", "SEL", "2026-11-02T18:00:00", "2026-11-03T10:00:00", 300, number="a"),
            _flight("IST", "SEL", "2026-11-04T18:00:00", "2026-11-05T10:00:00", 200, number="b"),
            _flight("IST", "SEL", "2026-11-08T18:00:00", "2026-11-09T10:00:00", 250, number="c")]
    return {0: leg0, 1: leg1}


def _build(query_dict):
    q = PlanQuery.from_dict({"stops": [{"kind": s.kind, "codes": s.codes, "window": s.window} for s in STOPS3],
                             "maxResults": 100, **query_dict})
    res = build_itineraries_compact(STOPS3, _collected(), max_results=100, city_info=CITY, query=q)
    return [(materialize(res, n)["segments"][0]["flight_number"], materialize(res, n)["segments"][1]["flight_number"],
             materialize(res, n)["total_price"]) for n in range(res["count"])]


def test_no_filters_gives_all_chains_by_price():
    assert _build({}) == [("2", "b", 250), ("2", "c", 300), ("1", "b", 300), ("2", "a", 350),
                          ("1", "c", 350), ("1", "a", 400)]


def test_leg_filters_prune_flights_before_search():
    assert _build({"legs": [{"maxTransfers": 0}, {}]}) == [("1", "b", 300), ("1", "c", 350), ("1", "a", 400)]
    assert _build({"legs": [{"baggage": "included"}, {}]}) == [("1", "b", 300), ("1", "c", 350), ("1", "a", 400)]


def test_city_stay_filters_inside_search():
    assert _build({"cities": [{}, {"minStay": 2}, {}]}) == [("2", "b", 250), ("2", "c", 300),
                                                            ("1", "b", 300), ("1", "c", 350)]
    assert _build({"cities": [{}, {"minStay": 2, "maxStay": 3}, {}]}) == [("2", "b", 250), ("1", "b", 300)]
    assert _build({"cities": [{}, {"requireWeekend": True}, {}]}) == [("2", "c", 300), ("1", "c", 350)]
    assert _build({"cities": [{}, {"mustCover": ["2026-11-03", "2026-11-04"]}, {}]}) == [
        ("2", "b", 250), ("2", "c", 300), ("1", "b", 300), ("1", "c", 350)]


def test_trip_length_filter_at_emit():
    assert _build({"tripLength": [0, 4]}) == [("2", "b", 250), ("1", "b", 300), ("2", "a", 350), ("1", "a", 400)]
    assert _build({"tripLength": [8, None]}) == [("2", "c", 300), ("1", "c", 350)]


# --------------------------- страница маршрутов ------------------------------

def test_routes_page_and_combos():
    q = PlanQuery.from_dict({"stops": [{"kind": s.kind, "codes": s.codes, "window": s.window} for s in STOPS3],
                             "maxResults": 100})
    res = json.loads(planner.compact_to_json(
        build_itineraries_compact(STOPS3, _collected(), max_results=100, city_info=CITY, query=q)))
    page = routes_page(res, 0, 2)
    assert page["total"] == 6 and [it["total_price"] for it in page["items"]] == [250, 300]
    it = page["items"][0]
    assert [s["code"] for s in it["stops"]] == ["MOW", "IST", "SEL"] and it["stops"][1]["days"] == 2  # 1.11 20:00 → 4.11 18:00
    assert it["segments"][0]["baggage"]["included"] is False and it["id"] == 1
    assert routes_page(res, 4, 10)["items"][0]["total_price"] == 350
    assert routes_page(res, 0, 10, ["MOW-IST-SEL"])["total"] == 6
    assert routes_page(res, 0, 10, ["MOW-DXB-SEL"])["total"] == 0


# ------------------------ ручки: run (дедуп) и routes -------------------------

def _setup(tmp_path, monkeypatch):
    db = str(tmp_path / "jobs.db")
    conn = hot.connect(db)
    hot.init_db(conn)
    monkeypatch.setattr(main, "_conn", conn)
    monkeypatch.setattr(hot, "DEFAULT_DB", db)
    monkeypatch.setattr(graphql_api, "fetch_series", _fake_fetch)
    monkeypatch.setattr(worker, "load_airport_network", lambda *a, **k: {})
    main._plan_results.clear()

    class Sync:
        def submit(self, fn, *args):
            fn(*args)
    monkeypatch.setattr(main, "_executor", Sync())
    return conn


def test_run_dedupes_by_query_key_and_routes_page(tmp_path, monkeypatch):
    conn = _setup(tmp_path, monkeypatch)
    req = main.PlanQueryRequest(stops=[main.PlanStop(**s) for s in JOB_STOPS], maxResults=10,
                                legs=[{"baggage": "any"}, {"maxTransfers": 0}])
    first = main.plan_run(req)
    assert first["status"] == "collecting" and first["mode"] == "routes" and "reused" not in first
    job = hot.get_job(conn, first["job_id"])
    assert job["status"] == "done" and job["query_key"]
    assert json.loads(job["params_json"])["legs"][1]["maxTransfers"] == 0

    again = main.plan_run(req)
    assert again["job_id"] == first["job_id"] and again["reused"] is True and again["status"] == "done"
    changed = main.plan_run(main.PlanQueryRequest(stops=req.stops, maxResults=10, legs=[{}, {"maxTransfers": 1}]))
    assert changed["job_id"] != first["job_id"]

    page = main.plan_job_routes(first["job_id"], offset=0, limit=2)
    assert page["status"] == "ok" and page["count"] == page["total"] > 0
    assert [s["code"] for s in page["items"][0]["stops"]] == ["MOW", "IST", "DST"]
    assert main.plan_job_routes(first["job_id"], combos="MOW-IST-DST")["total"] == page["total"]
    assert main.plan_job_routes(first["job_id"], combos="MOW-LED-DST")["total"] == 0
    assert main.plan_job_routes("nope")["status"] == "not_ready"

    combos = main.plan_job_combos(first["job_id"])
    assert combos["status"] == "ok" and combos["total"] == 1 and combos["totalCount"] == page["total"]
    assert combos["items"][0]["codes"] == ["MOW", "IST", "DST"] and combos["items"][0]["count"] == page["total"]
    assert set(combos["cities"]) == {"MOW", "IST", "DST"}
    assert main.plan_job_combos(first["job_id"], sort="count")["items"] == combos["items"]
    assert main.plan_job_combos("nope")["status"] == "not_ready"


def test_gather_still_works_with_open_filters(tmp_path, monkeypatch):
    conn = _setup(tmp_path, monkeypatch)
    res = main.plan_gather(main.PlanRequest(stops=[main.PlanStop(**s) for s in JOB_STOPS], max_results=10))
    assert res["status"] == "collecting"
    assert hot.get_job(conn, res["job_id"])["status"] == "done"


def test_estimate_counts_cached_series_and_guard_uses_cold(tmp_path, monkeypatch):
    """Оценка: серии, уже лежащие в ticket_cache, — «в кэше», предохранитель — по холодным."""
    conn = _setup(tmp_path, monkeypatch)
    stops = [{"kind": "cities", "codes": ["MOW"], "window": ["", ""]},
             {"kind": "any", "codes": [], "window": ["2026-11-01", "2026-11-02"]},
             {"kind": "cities", "codes": ["SEL"], "window": ["", ""]}]
    req = main.PlanQueryRequest(stops=[main.PlanStop(**s) for s in stops], maxResults=10, maxCost=50000)
    est = main.plan_estimate(req)
    assert est["requests"] == 2 * planner.PAGES_ANY * 2 and est["cached"] == 0 and est["cold"] == est["requests"]

    # Кладём в кэш серию MOW→ANY на 1.11 с тем же коридором — она перестаёт быть холодной.
    hot.ticket_cache_put(conn, "MOW", None, "2026-11-01", "max=50000",
                         {"tickets": [], "pages": 3, "exhausted": True})
    est = main.plan_estimate(req)
    assert est["cached"] == planner.PAGES_ANY and est["cold"] == est["requests"] - planner.PAGES_ANY
    assert est["legs"][0]["cached"] == planner.PAGES_ANY and est["legs"][1]["cached"] == 0
    assert est["seconds"] == round(est["cold"] * planner.SECONDS_PER_REQUEST)

    monkeypatch.setattr(planner, "MAX_REQUESTS", est["cold"] - 1)
    assert main.plan_run(req)["status"] == "too_wide"
    monkeypatch.setattr(planner, "MAX_REQUESTS", est["cold"])
    assert main.plan_run(req)["status"] in ("collecting", "done")


def test_routes_for_selected_combos_built_on_demand(tmp_path, monkeypatch):
    """Маршруты выбранных наборов строятся из сохранённых рейсов джобы, а не берутся из
    общего топ-N: набор, которого нет среди самых дешёвых цепочек, всё равно получает
    свои маршруты."""
    conn = _setup(tmp_path, monkeypatch)
    stops = [{"kind": "cities", "codes": ["MOW"], "window": ["", ""]},
             {"kind": "any", "codes": [], "window": ["2026-11-01", "2026-11-01"]},
             {"kind": "cities", "codes": ["SEL"], "window": ["", ""]}]

    def fetch(origin=None, destination=None, day=None, **_):
        from tests.test_job_stages import _flight, _series
        if origin == "MOW" and destination is None:          # MOW → ANY: дешёвый IST, дорогой DXB
            return _series([_flight("MOW", "IST", f"{day}T10:00:00", 100),
                            _flight("MOW", "DXB", f"{day}T10:00:00", 900)])
        if destination == "SEL":                              # ANY → SEL
            return _series([_flight("IST", "SEL", f"{day}T20:00:00", 100),
                            _flight("DXB", "SEL", f"{day}T20:00:00", 100)])
        return _series([])
    monkeypatch.setattr(graphql_api, "fetch_series", fetch)

    res = main.plan_run(main.PlanQueryRequest(stops=[main.PlanStop(**s) for s in stops], maxResults=1))
    job = res["job_id"]
    assert hot.get_job(conn, job)["status"] == "done"
    assert hot.get_plan_flights(conn, job) is not None
    # Общий список ограничен одной самой дешёвой цепочкой (через IST)…
    top = main.plan_job_routes(job)
    assert top["total"] == 1 and [s["code"] for s in top["items"][0]["stops"]] == ["MOW", "IST", "SEL"]
    # …а маршруты набора через DXB строятся по требованию.
    dxb = main.plan_job_routes(job, combos="MOW-DXB-SEL")
    assert dxb["total"] == 1 and dxb["items"][0]["total_price"] == 1000 and dxb["items"][0]["combo"] == "MOW-DXB-SEL"
    both = main.plan_job_routes(job, combos="MOW-DXB-SEL,MOW-IST-SEL")
    assert [it["combo"] for it in both["items"]] == ["MOW-IST-SEL", "MOW-DXB-SEL"]   # по цене
    assert main.plan_job_routes(job, combos="MOW-LED-SEL")["total"] == 0


def test_run_without_max_results_uses_default(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    res = main.plan_run(main.PlanQueryRequest(stops=[main.PlanStop(**s) for s in JOB_STOPS]))
    assert res["status"] in ("collecting", "done")
    assert json.loads(hot.get_job(main._conn, res["job_id"])["params_json"])["maxResults"] == planner.DEFAULT_MAX_RESULTS
