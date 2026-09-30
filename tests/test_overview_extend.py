"""overview._extend (отрезки по отсортированным прилётам) == _extend_dense (полная
матрица D × P × F) на случайных рейсах: все фильтры пребывания, переезд к соседу,
равные цены (выбор предшественника — как у argmin)."""
import itertools
import random

import numpy as np

from core.overview import _DAY, _extend, _extend_dense, _weekend_deadlines
from core.planquery import CityFilter


class _FakeLeg:
    def __init__(self, rnd: random.Random, n: int):
        base = 20400 * _DAY                                    # ~11.2025, наивные секунды
        dep = np.array([base + rnd.randrange(0, 12 * 86400, 900) for _ in range(n)], dtype=float)
        arr = dep + np.array([rnd.randrange(60, 30 * 60, 15) * 60 for _ in range(n)], dtype=float)
        self.dep_ts, self.arr_ts = dep, arr
        self.dep_ord = (dep // _DAY).astype(np.int64) + 719163
        self.arr_ord = (arr // _DAY).astype(np.int64) + 719163
        self.weekend_ok_from = _weekend_deadlines(self.arr_ord)
        self.price = np.array([rnd.choice([100.0, 150.0, 200.0, 250.0]) for _ in range(n)])   # много равных
        self.transfers = np.array([rnd.randrange(0, 3) for _ in range(n)], dtype=np.int32)


def _state(rnd: random.Random, days: int, n: int):
    cnt = np.array([[float(rnd.choice([0, 0, 1, 2, 5])) for _ in range(n)] for _ in range(days)])
    minp = np.where(cnt > 0, np.array([[rnd.choice([100.0, 200.0, 300.0]) for _ in range(n)]
                                       for _ in range(days)]), np.inf)
    tr_at = np.array([[rnd.randrange(0, 3) for _ in range(n)] for _ in range(days)], dtype=np.int32)
    mintr = np.where(cnt > 0, tr_at, np.int32(1 << 20))
    return cnt, minp, tr_at, mintr


FILTERS = [None, CityFilter(), CityFilter(min_stay=2), CityFilter(max_stay=3), CityFilter(min_stay=1, max_stay=4),
           CityFilter(require_weekend=True), CityFilter(min_stay=1, max_stay=6, require_weekend=True),
           CityFilter(must_cover=["2025-11-08", "2025-11-09"]),
           CityFilter(min_stay=0, max_stay=0)]


def _same(a, b):
    cnt_a, minp_a, tr_a, mintr_a = a
    cnt_b, minp_b, tr_b, mintr_b = b
    assert np.array_equal(cnt_a, cnt_b)
    assert np.array_equal(minp_a, minp_b)
    finite = np.isfinite(minp_a)                     # при INF предшественник не наблюдаем
    assert np.array_equal(tr_a[finite], tr_b[finite])
    assert np.array_equal(mintr_a, mintr_b)


def test_fast_extend_matches_dense():
    rnd = random.Random(11)
    for trial, (cf, use_hop) in enumerate(itertools.product(FILTERS, [False, True])):
        for _ in range(6):
            n_p, n_f, days = rnd.randrange(1, 40), rnd.randrange(1, 40), rnd.randrange(1, 5)
            prev, nxt = _FakeLeg(rnd, n_p + 5), _FakeLeg(rnd, n_f + 5)
            p_idx = np.array(sorted(rnd.sample(range(n_p + 5), n_p)), dtype=np.int64)
            f_idx = np.array(sorted(rnd.sample(range(n_f + 5), n_f)), dtype=np.int64)
            hop = np.array([rnd.random() < 0.4 for _ in range(n_f)]) if use_hop else None
            state = _state(rnd, days, n_p)
            _same(_extend(state, prev, p_idx, nxt, f_idx, cf, hop),
                  _extend_dense(state, prev, p_idx, nxt, f_idx, cf, hop))
