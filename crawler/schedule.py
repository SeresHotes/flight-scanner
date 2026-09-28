"""Планирование покрытия: какие серии X→ANY подать в очередь следующими.

Пара «город × день вылета» на горизонте имеет целевую свежесть по дальности даты
(ближние две недели — сутки, до двух месяцев — трое суток, дальше — неделя).
Срочность пары:
- есть серия: возраст / цель (≥ 1 — устарела);
- серии нет: 2 + (горизонт − дальность) / горизонт, то есть 2…3 — так первый
  проход идёт от ближних дат к дальним, но устаревшая ближняя дата (возраст ≥ 2×
  цели) обгоняет отсутствующие дальние;
- серия с ошибкой источника: возраст / интервал повтора ошибок.
Город, у которого одни ошибки (неизвестный источнику код), — в карантине: проверяется
одна дата раз в интервал повтора, остальные не тратят запросы.
"""
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

QUARANTINE_ERRORS = 3


@dataclass
class Targets:
    near_days: int = 14
    near_hours: float = 24
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
    reason: str  # missing | stale | error

    def request(self, max_pages: int) -> Dict[str, Any]:
        return {"origin": self.origin, "destination": None, "day": self.day, "max_pages": max_pages}


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
         exclude: Iterable[Tuple[str, str]] = (), limit: Optional[int] = None
         ) -> Tuple[List[Item], Dict[str, Any]]:
    """coverage — строки /v1/coverage: [origin, day, fetched_at, pages, tickets, exhausted, error].
    exclude — пары (origin, day), уже стоящие в очереди коллектора.
    Возвращает (серии по убыванию срочности, сводка покрытия для метрик)."""
    now = now or datetime.now(timezone.utc)
    cov: Dict[Tuple[str, str], Tuple[float, bool]] = {}
    errors_by_city: Dict[str, int] = {}
    ok_by_city: Dict[str, int] = {}
    for row in coverage:
        origin, day, fetched_at, _pages, _tickets, _exhausted, error = row[:7]
        age_h = max(0.0, (now - _parse_ts(fetched_at)).total_seconds() / 3600)
        cov[(origin, day)] = (age_h, bool(error))
        if error:
            errors_by_city[origin] = errors_by_city.get(origin, 0) + 1
        else:
            ok_by_city[origin] = ok_by_city.get(origin, 0) + 1

    excluded = set(exclude)
    items: List[Item] = []
    summary = {"cities": len(cities), "pairs": 0, "fresh": 0, "stale": 0, "missing": 0,
               "errors": 0, "quarantined_cities": 0, "oldest_h": 0.0, "queued_excluded": 0}
    for city in cities:
        quarantined = errors_by_city.get(city, 0) >= QUARANTINE_ERRORS and not ok_by_city.get(city)
        if quarantined:
            summary["quarantined_cities"] += 1
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
                age_h, error = entry
                summary["oldest_h"] = max(summary["oldest_h"], age_h)
                if error:
                    score, reason = age_h / targets.error_retry_hours, "error"
                    summary["errors"] += 1
                else:
                    score, reason = age_h / targets.hours(offset), "stale"
                    if score >= 1:
                        summary["stale"] += 1
                    else:
                        summary["fresh"] += 1
                if score < 1:
                    continue
            if (city, day) in excluded:
                summary["queued_excluded"] += 1
                continue
            items.append(Item(city, day, score, offset, reason))
    rank = {c: i for i, c in enumerate(cities)}
    items.sort(key=lambda it: (-it.score, it.offset, rank[it.origin]))
    summary["oldest_h"] = round(summary["oldest_h"], 1)
    summary["pass_progress"] = round(1 - summary["missing"] / summary["pairs"], 4) if summary["pairs"] else 1.0
    summary["due"] = len(items)
    return (items[:limit] if limit is not None else items), summary
