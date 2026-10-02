"""Замер скорости прода через публичный API (только stdlib): здоровье склада билетов и
памяти планировщика, затем несколько типовых джоб с `fresh: true` (всегда новая джоба) —
время до done, этапы (`stage.timings`: сбор, запись, стыковка), число рейсов, пик памяти.

    python scripts/prod_bench.py [--base https://flights.sereshotes.dev] [--only health]

Запускается workflow `.github/workflows/prod-bench.yml` (руками или после деплоя).
"""
import argparse
import json
import sys
import time
import urllib.request
from datetime import date, timedelta


def call(base: str, path: str, body=None, timeout: float = 60.0):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, headers={"Content-Type": "application/json"} if data else {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def stop(kind, codes, a="", b=""):
    return {"kind": kind, "codes": codes, "window": [a, b], "radiusKm": 0}


def scenarios():
    d = lambda n: (date.today() + timedelta(days=n)).isoformat()
    open_ = lambda n: [{}] * n
    return [
        ("MOW → IST(7 дн) → MOW", [stop("cities", ["MOW"]), stop("cities", ["IST"], d(30), d(36)), stop("cities", ["MOW"])]),
        ("MOW → ANY(3 дн) → MOW", [stop("cities", ["MOW"]), stop("any", [], d(30), d(32)), stop("cities", ["MOW"])]),
        ("MOW → ANY(7 дн) → MOW", [stop("cities", ["MOW"]), stop("any", [], d(30), d(36)), stop("cities", ["MOW"])]),
        ("MOW → ANY → ANY → MOW (по 3 дн)", [stop("cities", ["MOW"]), stop("any", [], d(30), d(32)), stop("any", [], d(33), d(35)), stop("cities", ["MOW"])]),
        ("ANY → ANY(3 дн)", [stop("any", [], d(30), d(32)), stop("any", [])]),
        ("ANY → SEL(5 дн) → TAS(5 дн)", [stop("any", [], d(40), d(40)), stop("cities", ["SEL"], d(41), d(45)), stop("cities", ["TAS"], d(46), d(50))]),
    ], open_


def health(base: str) -> dict:
    t = time.time()
    for attempt in range(10):
        try:
            h = call(base, "/api/health", timeout=120)
            break
        except Exception as e:  # планировщик перезапускается или занят загрузкой склада
            print(f"health: {e!r}, повтор через 30 с")
            time.sleep(30)
    else:
        raise SystemExit("health недоступен")
    print(f"health за {time.time() - t:.2f} с")
    print(json.dumps({k: h.get(k) for k in ("status", "process", "tickets")}, ensure_ascii=False, indent=1))
    return h


def run_job(base: str, label: str, stops, timeout: float = 1800.0) -> dict:
    q = {"stops": stops, "cities": [{}] * len(stops), "legs": [{}] * (len(stops) - 1), "tripLength": [0, None], "fresh": True}
    t0 = time.time()
    r = call(base, "/api/plan/run", q)
    if not r.get("job_id"):
        print(f"{label}: не запущена — {r.get('status')}: {r.get('message')}")
        return {"label": label, "status": r.get("status"), "message": r.get("message")}
    job = r["job_id"]
    st = {}
    while time.time() - t0 < timeout:
        st = call(base, f"/api/plan/jobs/{job}")
        if st.get("status") in ("done", "error"):
            break
        time.sleep(0.5)
    wall = time.time() - t0
    stage = st.get("stage") or {}
    row = {"label": label, "job": job, "status": st.get("status"), "wall_s": round(wall, 2), "flights": stage.get("flights"),
           "timings": stage.get("timings"), "routes": (st.get("summary") or {}).get("count"), "error": st.get("error")}
    if st.get("status") == "done":
        t = time.time()
        call(base, f"/api/plan/jobs/{job}/routes?limit=20")
        row["routes_page_s"] = round(time.time() - t, 2)
    print(json.dumps(row, ensure_ascii=False))
    return row


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="https://flights.sereshotes.dev")
    ap.add_argument("--only", default="")
    args = ap.parse_args()
    h = health(args.base)
    if args.only == "health":
        return 0
    if (h.get("tickets") or {}).get("status") != "ok":
        print("склад билетов ещё не готов — джобы не запускаю")
        return 0
    rows = [run_job(args.base, label, stops) for label, stops in scenarios()[0]]
    print("\n| сценарий | статус | рейсов | маршрутов | всего, с | сбор | запись | стыковка | страница маршрутов, с |")
    print("|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        tm = r.get("timings") or {}
        print(f"| {r['label']} | {r.get('status')} | {r.get('flights')} | {r.get('routes')} | {r.get('wall_s')} | {tm.get('collect')} | {tm.get('save')} | {tm.get('build')} | {r.get('routes_page_s')} |")
    health(args.base)
    return 0


if __name__ == "__main__":
    sys.exit(main())
