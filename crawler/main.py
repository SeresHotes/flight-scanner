"""Цикл фонового сборщика: раз в CRAWL_TICK_SECONDS берёт у коллектора города,
покрытие, очередь и курсор прохода (`/v1/crawler/state`), строит окна по календарю
(crawler.schedule.sweep) и досыпает в очередь `crawl`, пока там не наберётся
CRAWL_QUEUE_PAGES оценочных страниц (~30 мин работы ручки; не больше CRAWL_QUEUE_TARGET окон).
Проход окончен (все пары горизонта обработаны) — сразу начинается следующий (не раньше
CRAWL_MIN_PASS_HOURS после начала предыдущего, по умолчанию 0).
Сводку прохода отправляет коллектору (`/v1/stats/crawler`) — она уходит в метрики.

Запуск: python -m crawler.main
"""
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from core.collector_client import CollectorClient, CollectorError
from crawler.config import Settings
from crawler.schedule import densities, estimate_pages, merge_cities, sweep


def _days(day: str, day_to: Optional[str]) -> List[str]:
    """Дни серии в очереди (окно дат — все дни окна)."""
    start, end = date.fromisoformat(day), date.fromisoformat(day_to or day)
    return [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]


def _load_state(client: CollectorClient, now: datetime) -> Dict[str, Any]:
    state = client.crawler_state() or {}
    if not state.get("pass_started"):
        state = {"pass": 1, "pass_started": now.isoformat(timespec="seconds")}
        client.save_crawler_state(state)
    return state


def tick(client: CollectorClient, settings: Settings, *, today: Optional[date] = None,
         now: Optional[datetime] = None) -> Dict[str, Any]:
    """Один шаг: курсор прохода → подача окон по календарю → сводка. Возвращает сводку."""
    now = now or datetime.now(timezone.utc)
    today = today or now.date()
    health = client.health()
    queued = int(health.get("queued_crawl") or 0)
    cities = merge_cities(settings.seeds, client.cities())
    coverage = client.coverage()  # X→ANY без коридора
    density = densities(coverage)
    ours = [q for q in client.queue()
            if q.get("origin") and not q.get("destination") and not q.get("params_key")]
    queued_keys = {(q["origin"], day) for q in ours for day in _days(q["day"], q.get("day_to"))}
    # Глубина очереди — в оценочных страницах, а не в штуках: окно маленького города —
    # одна страница, крупного — десятки.
    queued_pages = sum(estimate_pages(density.get(q["origin"]), len(_days(q["day"], q.get("day_to"))))
                       for q in ours if q.get("priority") != "app")
    state = _load_state(client, now)

    def plan_pass():
        started = datetime.fromisoformat(state["pass_started"])
        return sweep(cities, coverage, today=today, horizon_days=settings.horizon_days,
                     pass_started=started, exclude=queued_keys,
                     window_tickets=settings.window_tickets or None,
                     unknown_window_days=settings.unknown_window_days)

    items, summary = plan_pass()
    started = datetime.fromisoformat(state["pass_started"])
    if summary["pass_done"] and now - started >= timedelta(hours=settings.min_pass_hours):
        state = {"pass": int(state.get("pass") or 1) + 1, "pass_started": now.isoformat(timespec="seconds"),
                 "previous_pass_hours": round((now - started).total_seconds() / 3600, 1)}
        client.save_crawler_state(state)
        items, summary = plan_pass()
        started = now
    batch, pages = [], queued_pages
    for it in items[:max(0, settings.queue_target - queued)]:
        if pages >= settings.queue_pages:
            break
        batch.append(it)
        pages += it.est_pages
    submitted = 0
    if batch:
        # всё, что получено после начала прохода, уже обработано — подаём только то, что старше
        ttl = max(1.0, (now - started).total_seconds())
        ids = client.submit_batch([it.request(settings.max_pages) for it in batch], client="crawl",
                                  ttl_seconds=ttl)
        submitted = len(ids)
    summary.update({"queued_crawl": queued, "queued_pages_est": queued_pages,
                    "submitted_pages_est": pages - queued_pages, "submitted": submitted,
                    "queued_app": int(health.get("queued_app") or 0),
                    "pass": int(state.get("pass") or 1), "pass_started": state["pass_started"],
                    "previous_pass_hours": state.get("previous_pass_hours"),
                    "horizon_days": settings.horizon_days, "tick_at": now.isoformat(timespec="seconds")})
    try:
        client.report_crawler_stats(summary)
    except CollectorError as e:
        print(f"[crawler] сводка не отправлена: {e}")
    return summary


def run(settings: Optional[Settings] = None) -> None:
    settings = settings or Settings()
    client = CollectorClient(settings.collector_url)
    print(f"[crawler] старт: коллектор {settings.collector_url}, горизонт {settings.horizon_days} дн., "
          f"очередь {settings.queue_pages} стр. (≤ {settings.queue_target} окон), хабов {len(settings.seeds)}")
    while True:
        started = time.monotonic()
        try:
            s = tick(client, settings)
            print(f"[crawler] проход {s['pass']} (с {s['pass_started']}): дошёл до {s['sweep_day'] or 'конца'} "
                  f"(+{s['sweep_offset']} дн.), обработано {s['done']} из {s['pairs']} пар "
                  f"({s['pass_progress'] * 100:.1f} %), с прошлого прохода {s['update']}, нет данных {s['missing']}; "
                  f"городов {s['cities']}, ошибок {s['errors']}; "
                  f"в очереди {s['queued_crawl']} (~{s['queued_pages_est']} стр.), подано {s['submitted']} "
                  f"(~{s['submitted_pages_est']} стр.)")
        except CollectorError as e:
            print(f"[crawler] коллектор недоступен: {e}")
        except Exception as e:  # цикл не должен умирать
            print(f"[crawler] ошибка тика: {e!r}")
        time.sleep(max(1.0, settings.tick_seconds - (time.monotonic() - started)))


if __name__ == "__main__":
    run()
