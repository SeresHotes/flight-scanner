"""Сбор X → ANY для всех городов обходом в ширину от стартового города.

Для каждого города X делаем то же, что collect_leg_data(origin=X, destination=None):
по одному запросу на день в двух режимах (с пересадками и только прямые — см.
planner.FETCH_MODES: с direct=false API отдаёт по направлению один самый дешёвый
билет, и прямой за стыковочным теряется). Даты — «неделя через месяц»: окно из
N дней, повторённое раз в месяц (date_windows).

Города-соседи берём из поля `destination` собранных билетов и ставим в очередь
BFS; порядок обхода детерминирован (соседи — по убыванию числа билетов, хабы
первыми, затем по алфавиту). Состояние
обхода (очередь, пройденные) сериализуется, чтобы долгий сбор можно было
прервать и продолжить (scripts/scan_any.py).

Модуль чистый: обращение к API — через fetch_fn (кэширующая обёртка воркера),
сохранение — через on_city_done.
"""
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Callable, Dict, List, Optional

from core.collector import collect_leg_data
from core.planner import FETCH_MODES, _flight_key

DEFAULT_START_CITY = "MOW"
DEFAULT_WINDOW_DAYS = 7
DEFAULT_MONTHS = 3


def _add_months(d: date, months: int) -> date:
    """Та же дата через `months` месяцев; 31-е в коротком месяце ужимается к концу."""
    month0 = d.month - 1 + months
    year = d.year + month0 // 12
    month = month0 % 12 + 1
    last_day = (date(year + (month == 12), month % 12 + 1, 1) - timedelta(days=1)).day
    return date(year, month, min(d.day, last_day))


def date_windows(start: date, months: int = DEFAULT_MONTHS,
                 days: int = DEFAULT_WINDOW_DAYS) -> List[str]:
    """«Неделя через месяц»: `days` подряд начиная со start, и так `months` раз с
    шагом в месяц. Например (2026-10-05, 3, 7) → 05–11 окт, 05–11 ноя, 05–11 дек."""
    dates: List[str] = []
    for m in range(months):
        first = _add_months(start, m)
        for i in range(days):
            dates.append((first + timedelta(days=i)).isoformat())
    return dates


def requests_per_city(dates: List[str]) -> int:
    return len(dates) * len(FETCH_MODES)


@dataclass
class ScanState:
    """Состояние BFS: done — уже собранные города (в порядке обхода), queue — ещё
    не собранные (в порядке постановки), requests — сколько запросов сделано."""
    dates: List[str]
    queue: List[str] = field(default_factory=list)
    done: List[str] = field(default_factory=list)
    requests: int = 0

    def as_dict(self) -> Dict[str, Any]:
        return {"dates": self.dates, "queue": self.queue, "done": self.done,
                "requests": self.requests}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ScanState":
        return cls(dates=list(d["dates"]), queue=list(d.get("queue") or []),
                   done=list(d.get("done") or []), requests=int(d.get("requests") or 0))

    @classmethod
    def initial(cls, dates: List[str], start_city: str = DEFAULT_START_CITY) -> "ScanState":
        return cls(dates=dates, queue=[start_city.upper()])

    def seen(self, city: str) -> bool:
        return city in self.done or city in self.queue


def collect_city_any(city: str, dates: List[str], fetch_fn=None,
                     progress_cb: Callable[[], None] = None) -> List[Dict[str, Any]]:
    """X → ANY за все даты в обоих режимах, без дублей (см. planner._flight_key)."""
    flights: List[Dict[str, Any]] = []
    seen = set()
    for allow_indirect in FETCH_MODES:
        batch = collect_leg_data(city, None, dates, leg_name=f"any:{city}",
                                 allow_indirect=allow_indirect,
                                 progress_cb=progress_cb, fetch_fn=fetch_fn)
        for f in batch:
            key = _flight_key(f)
            if key in seen:
                continue
            seen.add(key)
            flights.append(f)
    return flights


def neighbours(flights: List[Dict[str, Any]]) -> List[str]:
    """Города назначения из билетов — кандидаты в очередь BFS.

    Порядок внутри уровня: сначала города с бо́льшим числом билетов (хабы — у них
    больше рейсов и пересадок, они ценнее для hidden-city), при равенстве — по
    алфавиту. MOW→ANY даёт 800+ соседей, алфавит начинал бы с крошечных AAP/AAT."""
    counts: Dict[str, int] = {}
    for f in flights:
        city = (f.get("destination") or "").upper()
        if city:
            counts[city] = counts.get(city, 0) + 1
    return sorted(counts, key=lambda c: (-counts[c], c))


def scan_any(state: ScanState, fetch_fn=None,
             on_city_done: Callable[[str, List[Dict[str, Any]]], None] = None,
             max_cities: Optional[int] = None, max_requests: Optional[int] = None,
             should_stop: Callable[[], bool] = None,
             progress_cb: Callable[[], None] = None) -> ScanState:
    """Продолжает обход с текущего состояния, пока очередь не опустеет или не
    сработает ограничитель (max_cities — городов за этот вызов, max_requests —
    суммарный счётчик state.requests, should_stop — внешняя отмена).

    on_city_done(city, flights) — вызывается после каждого города (сохранение
    котировок); состояние обновляется до вызова, чтобы прерывание между городами
    не теряло уже сохранённое."""
    cities_this_run = 0
    per_city = requests_per_city(state.dates)
    while state.queue:
        if should_stop and should_stop():
            break
        if max_cities is not None and cities_this_run >= max_cities:
            break
        if max_requests is not None and state.requests + per_city > max_requests:
            break
        city = state.queue.pop(0)
        flights = collect_city_any(city, state.dates, fetch_fn=fetch_fn, progress_cb=progress_cb)
        state.requests += per_city
        state.done.append(city)
        for nb in neighbours(flights):
            if not state.seen(nb):
                state.queue.append(nb)
        cities_this_run += 1
        if on_city_done:
            on_city_done(city, flights)
    return state
