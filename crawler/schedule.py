"""Планирование сбора: проход по календарю дат вылета, по кругу.

Курсор прохода идёт по дням вылета от сегодня до +horizon_days. На каждом дне в очередь
встают запросы всех городов, у которых этот день ещё не обработан в текущем проходе;
запрос начинается с этого дня и захватывает столько следующих дней, сколько влезает в
бюджет билетов (`window_tickets` / плотность города, p90 билетов в день по его покрытию):
у Москвы — день, у маленького города — месяц и больше. Когда курсор доходит до дней,
уже покрытых таким окном, запроса не нужно. Дошли до конца горизонта — проход окончен,
следующий начинается снова с сегодняшнего дня (не раньше `min_pass_hours` после начала
предыдущего: источник сам кэширует цены ~сутки).

Проход задаётся моментом начала `pass_started` (хранит коллектор, `/v1/crawler/state`):
пара «город × день» обработана в этом проходе, если её серия получена не раньше
`pass_started` (в том числе с ошибкой источника — повтор в следующем проходе). Курсор
не хранится — это первый день вылета, где есть необработанная пара (`sweep_day`); новый
город, появившийся посреди прохода, догоняется с начала горизонта.

Город, у которого одни ошибки (неизвестный источнику код), — в карантине: за проход
проверяется одна дата (сегодня), остальные не тратят запросы.

Скорость упирается в страницы по 400 билетов: крупные окна (~25 страниц) теряют на
неполной последней странице ~2 %, окно маленького города на месяц — одна страница
вместо тридцати.
"""
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

QUARANTINE_ERRORS = 3
PAGE_TICKETS = 400


def p90(tickets: Optional[List[int]]) -> Optional[int]:
    """Плотность города: p90 билетов в день по его покрытию (None — данных нет)."""
    if not tickets:
        return None
    ranked = sorted(tickets)
    return ranked[int(0.9 * (len(ranked) - 1))]


def mean(tickets: Optional[List[int]]) -> Optional[int]:
    """Средняя плотность города (билетов в день) — для оценки страниц: p90 режет окно с
    запасом, но как оценка работы завышает её в разы (дальние даты реже)."""
    if not tickets:
        return None
    return -(-sum(tickets) // len(tickets))


def densities(coverage: Iterable[Sequence[Any]]) -> Dict[str, int]:
    """Город → средняя плотность (билетов в день) по строкам /v1/coverage без ошибок."""
    by_city: Dict[str, List[int]] = {}
    for row in coverage:
        if not row[6]:
            by_city.setdefault(row[0], []).append(int(row[4] or 0))
    return {c: mean(t) for c, t in by_city.items()}


def estimate_pages(density: Optional[int], days: int) -> int:
    """Оценка страниц серии: билетов в окне / 400, не меньше одной."""
    return max(1, -(-int(density or 0) * days // PAGE_TICKETS))


@dataclass
class Item:
    origin: str
    day: str
    offset: int
    reason: str  # missing (данных не было) | update (данные с прошлого прохода) | quarantine
    day_to: Optional[str] = None  # окно дат: последний день включительно
    days: int = 1
    est_pages: int = 1            # оценка страниц (плотность × дни / 400) — для глубины очереди
    rank: int = 0                 # порядок города (хабы и крупные раньше) — внутри одного дня

    def request(self, max_pages: int) -> Dict[str, Any]:
        out = {"origin": self.origin, "destination": None, "day": self.day, "max_pages": max_pages}
        if self.day_to and self.day_to != self.day:
            out["day_to"] = self.day_to
        return out


def _parse_ts(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def merge_cities(seeds: Sequence[str], known: Iterable[Dict[str, Any]]) -> List[str]:
    """Стартовые хабы + города из ответов, по убыванию числа билетов (крупные раньше)."""
    ranked = sorted(((c.get("tickets") or 0, c["code"].upper()) for c in known if c.get("code")),
                    reverse=True)
    out: List[str] = []
    seen: Set[str] = set()
    for code in [s.upper() for s in seeds] + [code for _, code in ranked]:
        if len(code) == 3 and code.isalpha() and code != "ANY" and code not in seen:
            seen.add(code)
            out.append(code)
    return out


def _window_days(density: Optional[int], budget: Optional[int], unknown_days: int,
                 horizon_days: int) -> int:
    """Длина окна: бюджет билетов / плотность (p90 билетов в день по покрытию города);
    город без данных — unknown_days; budget пуст — по одному дню."""
    if not budget:
        return 1
    if density is None:
        return max(1, unknown_days)
    return max(1, min(horizon_days + 1, budget // max(density, 1)))


def sweep(cities: Sequence[str], coverage: Iterable[Sequence[Any]], *, today: date,
          horizon_days: int, pass_started: datetime, exclude: Iterable[Tuple[str, str]] = (),
          window_tickets: Optional[int] = 10_000, unknown_window_days: int = 30
          ) -> Tuple[List[Item], Dict[str, Any]]:
    """coverage — строки /v1/coverage: [origin, day, fetched_at, pages, tickets, exhausted, error].
    exclude — пары (origin, day), уже стоящие в очереди коллектора.
    Возвращает (окна к подаче по календарю: по дню начала, на одном дне — города по
    порядку cities; сводка прохода для метрик)."""
    cov: Dict[Tuple[str, str], Tuple[datetime, bool]] = {}
    errors_by_city: Dict[str, int] = {}
    ok_by_city: Dict[str, int] = {}
    tickets_by_city: Dict[str, List[int]] = {}
    for row in coverage:
        origin, day, fetched_at, _pages, tickets, _exhausted, error = row[:7]
        cov[(origin, day)] = (_parse_ts(fetched_at), bool(error))
        if error:
            errors_by_city[origin] = errors_by_city.get(origin, 0) + 1
        else:
            ok_by_city[origin] = ok_by_city.get(origin, 0) + 1
            tickets_by_city.setdefault(origin, []).append(int(tickets or 0))

    excluded = set(exclude)
    days = [(today + timedelta(days=o)).isoformat() for o in range(horizon_days + 1)]
    summary = {"cities": len(cities), "pairs": 0, "done": 0, "update": 0, "missing": 0,
               "errors": 0, "quarantined_cities": 0, "queued_excluded": 0}
    pending_by_offset = [0] * len(days)
    items: List[Item] = []
    for rank, city in enumerate(cities):
        quarantined = errors_by_city.get(city, 0) >= QUARANTINE_ERRORS and not ok_by_city.get(city)
        if quarantined:
            summary["quarantined_cities"] += 1
        known = tickets_by_city.get(city)
        max_days = 1 if quarantined else _window_days(p90(known), window_tickets, unknown_window_days,
                                                      horizon_days)
        run: List[Tuple[int, str]] = []

        def flush() -> None:
            if run:
                items.append(_item(city, run, days, mean(known), rank))
                run.clear()

        for offset, day in enumerate(days):
            if quarantined and offset != 0:
                continue
            summary["pairs"] += 1
            entry = cov.get((city, day))
            if entry is not None and entry[1]:
                summary["errors"] += 1
            if entry is not None and entry[0] >= pass_started:
                summary["done"] += 1
                flush()
                continue
            pending_by_offset[offset] += 1
            summary["missing" if entry is None else "update"] += 1
            if (city, day) in excluded:
                summary["queued_excluded"] += 1
                flush()
                continue
            if len(run) >= max_days:
                flush()
            run.append((offset, "quarantine" if quarantined else ("missing" if entry is None else "update")))
        flush()

    items.sort(key=lambda it: (it.offset, it.rank))
    cursor = next((o for o, n in enumerate(pending_by_offset) if n), None)
    summary["sweep_offset"] = len(days) if cursor is None else cursor
    summary["sweep_day"] = None if cursor is None else days[cursor]
    summary["pass_progress"] = round(summary["done"] / summary["pairs"], 4) if summary["pairs"] else 1.0
    summary["pass_done"] = cursor is None
    summary["due"] = sum(it.days for it in items)
    summary["windows"] = len(items)
    return items, summary


def _item(city: str, run: List[Tuple[int, str]], days: List[str], density: Optional[int],
          rank: int) -> Item:
    first, last = run[0][0], run[-1][0]
    reason = "missing" if all(r == "missing" for _, r in run) else run[0][1]
    return Item(city, days[first], first, reason, day_to=days[last] if last != first else None,
                days=len(run), est_pages=estimate_pages(density, len(run)), rank=rank)
