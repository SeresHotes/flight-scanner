"""Замер страницы «Динамика цены» на проде через публичный API (только stdlib):
`/api/dynamics` для нескольких направлений и ширины дней вылета — время до результата
(фон + опрос), сколько файлов прочитано, снимков, рейсов, объём ответа; затем повтор
(должен прийти из кэша мгновенно).

    python scripts/dynamics_bench.py [--base https://flights.sereshotes.dev]

Запускается workflow `.github/workflows/dynamics-bench.yml` (руками или через API).
"""
import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from datetime import date, timedelta


def get(base: str, params: dict, timeout: float = 120.0):
    url = f"{base}/api/dynamics?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=timeout) as r:
        raw = r.read()
    return json.loads(raw), len(raw)


def run_case(base: str, name: str, params: dict, limit: float = 900.0) -> bool:
    t0 = time.time()
    last = None
    while True:
        v, size = get(base, params)
        if not v.get("pending"):
            break
        prog = (v.get("done"), v.get("total"))
        if prog != last:
            print(f"    … {prog[0]}/{prog[1]} файлов за {time.time() - t0:.1f} с", flush=True)
            last = prog
        if time.time() - t0 > limit:
            print(f"  ✗ {name}: дольше {limit:.0f} с")
            return False
        time.sleep(1.0)
    took = time.time() - t0
    if v.get("error"):
        print(f"  ✗ {name}: {v['error']} ({took:.1f} с)")
        return False
    t1 = time.time()
    get(base, params)
    again = time.time() - t1
    print(
        f"  ✓ {name}: {took:.1f} с (бэк {v.get('seconds')} с), снимков {v.get('files')}, рейсов {len(v.get('flights', []))}, "
        f"цен {len(v.get('obs', []))}, ответ {size / 1024:.0f} КБ, ошибок файлов {v.get('files_failed')}; повтор {again:.2f} с",
        flush=True,
    )
    return True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="https://flights.sereshotes.dev")
    a = ap.parse_args()
    d = lambda n: (date.today() + timedelta(days=n)).isoformat()
    cases = [
        ("MOW→EVN, 1 день, 60 дн. истории", {"origin": "MOW", "destination": "EVN", "from": d(29), "to": d(29), "history": 60}),
        ("MOW→EVN, неделя", {"origin": "MOW", "destination": "EVN", "from": d(29), "to": d(35), "history": 60}),
        ("MOW→EVN, месяц", {"origin": "MOW", "destination": "EVN", "from": d(29), "to": d(59), "history": 60}),
        ("MOW→IST, месяц, 180 дн. истории", {"origin": "MOW", "destination": "IST", "from": d(10), "to": d(40), "history": 180}),
        ("LED→TBS, неделя", {"origin": "LED", "destination": "TBS", "from": d(20), "to": d(26), "history": 60}),
        ("SVO→AER (аэропорт), 1 день", {"origin": "SVO", "destination": "AER", "from": d(14), "to": d(14), "history": 60}),
    ]
    print(f"База: {a.base}")
    ok = all([run_case(a.base, n, p) for n, p in cases])
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
