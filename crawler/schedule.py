"""Планирование покрытия: какие серии X→ANY подать в очередь следующими.

Пара «город × день вылета» на горизонте имеет целевую свежесть по дальности даты
(до двух месяцев — трое суток, дальше — неделя; не чаще раза в трое суток).
Срочность пары:
- есть серия: возраст / цель (≥ 1 — устарела);
- серии нет: 2 + (горизонт − дальность) / горизонт, то есть 2…3 — так первый
  проход идёт от ближних дат к дальним, но устаревшая ближняя дата (возраст ≥ 2×
  цели) обгоняет отсутствующие дальние;
- серия с ошибкой источника: возраст / интервал повтора ошибок.
- обрезанная серия (упёрлась в предохранитель страниц) — как ошибка: возраст / интервал
  повтора, следующий запрос возьмёт её целиком.
Город, у которого одни ошибки (неизвестный источнику код), — в карантине: проверяется
одна дата раз в интервал повтора, остальные не тратят запросы.

Сбор не останавливается, когда весь горизонт свежий: свежие пары старше `refresh_floor_hours`
(24 ч — младше коллектор всё равно отдаст из кэша) идут вторым эшелоном («refresh») по
убыванию срочности, то есть самые старые относительно своей цели первыми; у ближних дат
цель меньше, поэтому они обновляются чаще. Очередь коллектора всегда полна, лимит ручки
расходуется целиком (запросы приложения — вне очереди, с приоритетом).

Окна дат: источник отдаёт диапазон дат одним запросом (те же билеты, что посуточно),
а платим минимум страницу за запрос — поэтому подряд идущие «пора» дни города
склеиваются в окно. Длина окна = бюджет билетов / плотность города (p90 билетов в день
по его покрытию; популярность хранить отдельно не нужно — она в индексе), для города
без данных — `unknown_window_days`. Окно не пересекает смену целевой свежести.
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
class Targets:
    near_days: int = 14
    near_hours: float = 72
    mid_days: int = 60
    mid_hours: float = 72
    far_hours: float = 168
    error_retry_hours: float = 24

    def hours(self, offset: int) -> float:
        if offset <= self.near_days:
            return self.near_hours
        if offset <= self.mid_days:
            return self.mid_hours
        return self.far_hours


@dataclass
class Item:
    origin: str
    day: str
    score: float
    offset: int
    reason: str  # missing | stale | error | truncated
    day_to: Optional[str] = None  # окно дат: последний день включительно
    days: int = 1
    est_pages: int = 1            # оценка страниц (плотность × дни / 400) — для глубины очереди

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


def plan(cities: Sequence[str], coverage: Iterable[Sequence[Any]], *, today: date,
         horizon_days: int, targets: Targets, now: Optional[datetime] = None,
         exclude: Iterable[Tuple[str, str]] = (), limit: Optional[int] = None,
         window_tickets: Optional[int] = None, unknown_window_days: int = 30,
         refresh_floor_hours: Optional[float] = 24.0
         ) -> Tuple[List[Item], Dict[str, Any]]:
    """coverage — строки /v1/coverage: [origin, day, fetched_at, pages, tickets, exhausted, error].
    exclude — пары (origin, day), уже стоящие в очереди коллектора.
    window_tickets — бюджет билетов на окно дат (None — по одному дню на серию).
    refresh_floor_hours — свежие пары старше этого возраста идут вторым эшелоном
    («refresh», срочность < 1), чтобы сбор не останавливался; None — свежие пропускаются.
    Возвращает (серии по убыванию срочности, сводка покрытия для метрик)."""
    now = now or datetime.now(timezone.utc)
    cov: Dict[Tuple[str, str], Tuple[float, bool, bool]] = {}
    errors_by_city: Dict[str, int] = {}
    ok_by_city: Dict[str, int] = {}
    tickets_by_city: Dict[str, List[int]] = {}
    for row in coverage:
        origin, day, fetched_at, _pages, tickets, exhausted, error = row[:7]
        age_h = max(0.0, (now - _parse_ts(fetched_at)).total_seconds() / 3600)
        cov[(origin, day)] = (age_h, bool(error), bool(exhausted))
        if error:
            errors_by_city[origin] = errors_by_city.get(origin, 0) + 1
        else:
            ok_by_city[origin] = ok_by_city.get(origin, 0) + 1
            tickets_by_city.setdefault(origin, []).append(int(tickets or 0))

    excluded = set(exclude)
    items: List[Item] = []
    summary = {"cities": len(cities), "pairs": 0, "fresh": 0, "stale": 0, "missing": 0,
               "errors": 0, "truncated": 0, "refresh": 0, "quarantined_cities": 0, "oldest_h": 0.0,
               "queued_excluded": 0}
    for city in cities:
        quarantined = errors_by_city.get(city, 0) >= QUARANTINE_ERRORS and not ok_by_city.get(city)
        if quarantined:
            summary["quarantined_cities"] += 1
        due: List[Item] = []
        for offset in range(horizon_days + 1):
            if quarantined and offset != 1:
                continue
            day = (today + timedelta(days=offset)).isoformat()
            summary["pairs"] += 1
            entry = cov.get((city, day))
            if entry is None:
                score, reason = 2.0 + (horizon_days - offset) / max(horizon_days, 1), "missing"
                summary["missing"] += 1
            else:
                age_h, error, exhausted = entry
                summary["oldest_h"] = max(summary["oldest_h"], age_h)
                if error:
                    score, reason = age_h / targets.error_retry_hours, "error"
                    summary["errors"] += 1
                else:
                    score, reason = age_h / targets.hours(offset), "stale"
                    if not exhausted and age_h / targets.error_retry_hours > score:
                        score, reason = age_h / targets.error_retry_hours, "truncated"
                    if score < 1:
                        summary["fresh"] += 1
                        if refresh_floor_hours is not None and age_h >= refresh_floor_hours:
                            reason = "refresh"  # второй эшелон: обновляем, самые старые первыми
                            summary["refresh"] += 1
                    else:
                        summary[reason] += 1
                if score < 1 and reason != "refresh":
                    continue
            if (city, day) in excluded:
                summary["queued_excluded"] += 1
                continue
            due.append(Item(city, day, score, offset, reason))
        known = tickets_by_city.get(city)
        if window_tickets is None or quarantined:
            for it in due:
                it.est_pages = estimate_pages(mean(known), 1)
            items.extend(due)
        else:
            items.extend(_windows(due, targets, _window_days(
                p90(known), window_tickets, unknown_window_days, horizon_days), mean(known)))
    rank = {c: i for i, c in enumerate(cities)}
    # «пора» (срочность ≥ 1) всегда раньше второго эшелона (< 1); внутри — по убыванию срочности
    items.sort(key=lambda it: (-it.score, it.offset, rank[it.origin]))
    summary["oldest_h"] = round(summary["oldest_h"], 1)
    summary["pass_progress"] = round(1 - summary["missing"] / summary["pairs"], 4) if summary["pairs"] else 1.0
    summary["due"] = sum(it.days for it in items)
    summary["windows"] = len(items)
    return (items[:limit] if limit is not None else items), summary


def _window_days(density: Optional[int], budget: int, unknown_days: int, horizon_days: int) -> int:
    """Длина окна: бюджет билетов / плотность (p90 билетов в день по покрытию города)."""
    if density is None:
        return max(1, unknown_days)
    return max(1, min(horizon_days + 1, budget // max(density, 1)))


def _windows(due: List[Item], targets: Targets, max_days: int,
             density: Optional[int] = None) -> List[Item]:
    """Подряд идущие дни (по возрастанию дальности) → окна до max_days дней; окно не
    пересекает пропуск и смену целевой свежести. Срочность окна — самого срочного дня."""
    out: List[Item] = []
    cur: List[Item] = []

    def flush() -> None:
        if cur:
            top = max(cur, key=lambda it: it.score)
            out.append(Item(cur[0].origin, cur[0].day, top.score, cur[0].offset, top.reason,
                            day_to=cur[-1].day, days=len(cur),
                            est_pages=estimate_pages(density, len(cur))))
            cur.clear()

    # дни «пора» и дни второго эшелона не смешиваем в одном окне: иначе обновление свежих
    # дней шло бы с приоритетом устаревших соседей
    for it in sorted(due, key=lambda it: it.offset):
        if cur and (it.offset != cur[-1].offset + 1 or len(cur) >= max_days
                    or targets.hours(it.offset) != targets.hours(cur[0].offset)
                    or (it.score >= 1) != (cur[0].score >= 1)):
            flush()
        cur.append(it)
    flush()
    return out
