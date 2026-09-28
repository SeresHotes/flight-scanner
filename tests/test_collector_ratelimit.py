"""Лимитер ручки: ровный темп, пауза по Retry-After, растущий бэкофф, замедление после 429."""
from collector import ratelimit
from collector.ratelimit import RateLimiter


class Clock:
    def __init__(self):
        self.t = 0.0
        self.slept = []

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.slept.append(round(s, 3))
        self.t += s


def test_even_pace_per_minute():
    c = Clock()
    rl = RateLimiter(60, clock=c, sleep=c.sleep)
    assert rl.wait() == 0.0            # первый запрос сразу
    assert rl.wait() == 1.0 and c.t == 1.0
    c.t += 0.4
    assert round(rl.wait(), 3) == 0.6 and c.t == 2.0
    c.t += 5
    assert rl.wait() == 0.0            # долго не ходили — ждать не надо


def test_429_retry_after_backoff_and_slowdown():
    c = Clock()
    rl = RateLimiter(60, clock=c, sleep=c.sleep)
    rl.wait()
    assert rl.on_429(retry_after=7) == 7 and rl.total_429 == 1
    assert rl.wait() == 7.0            # пауза Retry-After
    rl.on_success()
    assert round(rl.wait(), 3) == 1.5  # после 429 темп в полтора раза медленнее
    c.t += ratelimit.SLOWDOWN_SECONDS + 10
    rl.wait()
    assert rl.wait() == 1.0            # замедление прошло
    # Без Retry-After: бэкофф 5, 10, 20 … не больше 60 с; успех сбрасывает ряд.
    assert [rl.on_429() for _ in range(5)] == [5, 10, 20, 40, 60]
    rl.on_success()
    assert rl.on_429() == 5
