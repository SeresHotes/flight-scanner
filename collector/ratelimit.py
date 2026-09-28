"""Лимит ручки GraphQL: ровный темп N запросов в минуту и реакция на 429.

На 429 источник просят подождать (Retry-After) либо ждём растущий бэкофф; после
этого на пару минут замедляемся в полтора раза и возвращаемся к норме, когда
запросы снова проходят. Часы и sleep подменяются в тестах."""
import time
from typing import Callable, Optional

MAX_BACKOFF_SECONDS = 60.0
SLOWDOWN_FACTOR = 1.5
SLOWDOWN_SECONDS = 120.0


class RateLimiter:
    def __init__(self, per_minute: float, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep):
        self.interval = 60.0 / per_minute if per_minute > 0 else 0.0
        self._clock = clock
        self._sleep = sleep
        self._last = -1e9
        self._penalty_until = 0.0
        self._slow_until = 0.0
        self._consecutive_429 = 0
        self.total_429 = 0

    @property
    def per_minute(self) -> float:
        return 60.0 / self.interval if self.interval else 0.0

    def wait(self) -> float:
        """Блокирует до момента, когда можно делать следующий запрос. Возвращает,
        сколько ждали (для метрик)."""
        now = self._clock()
        factor = SLOWDOWN_FACTOR if now < self._slow_until else 1.0
        due = max(self._last + self.interval * factor, self._penalty_until)
        wait = due - now
        if wait > 0:
            self._sleep(wait)
            now = self._clock()
        self._last = now
        return max(wait, 0.0)

    def on_429(self, retry_after: Optional[float] = None) -> float:
        """Источник ответил 429: пауза Retry-After или бэкофф 5, 10, 20 … 60 с, плюс
        замедление на SLOWDOWN_SECONDS. Возвращает длину паузы."""
        self._consecutive_429 += 1
        self.total_429 += 1
        backoff = min(MAX_BACKOFF_SECONDS, 5.0 * (2 ** (self._consecutive_429 - 1)))
        pause = float(retry_after) if retry_after and retry_after > 0 else backoff
        now = self._clock()
        self._penalty_until = now + pause
        self._slow_until = now + pause + SLOWDOWN_SECONDS
        return pause

    def on_success(self) -> None:
        self._consecutive_429 = 0
