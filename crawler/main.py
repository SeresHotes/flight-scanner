"""Цикл фонового сборщика: раз в CRAWL_TICK_SECONDS берёт у коллектора города,
покрытие и очередь, считает срочность пар «город × день» (crawler.schedule) и
досыпает в очередь `crawl` окна, пока там не наберётся CRAWL_QUEUE_PAGES оценочных
страниц (не больше CRAWL_QUEUE_TARGET окон).
Сводку покрытия отправляет коллектору (`/v1/stats/crawler`) — она уходит в метрики.

Запуск: python -m crawler.main
"""
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from core.collector_client import CollectorClient, CollectorError
from crawler.config import Settings
from crawler.schedule import Targets, densities, estimate_pages, merge_cities, plan


def _days(day: str, day_to: Optional[str]) -> List[str]:
    """Дни серии в очереди (окно дат — все дни окна)."""
    start, end = date.fromisoformat(day), date.fromisoformat(day_to or day)
    return [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]


def tick(client: CollectorClient, settings: Settings, *, today: Optional[date] = None,
         now: Optional[datetime] = None) -> Dict[str, Any]:
    """Один шаг: план → подача недостающих серий → сводка. Возвращает сводку."""
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
    # одна страница, и 50 таких выбирались за минуту, а тик — раз в две (простой ручки).
    queued_pages = sum(estimate_pages(density.get(q["origin"]), len(_days(q["day"], q.get("day_to"))))
                       for q in ours if q.get("priority") != "app")
    targets = Targets(settings.near_days, settings.near_hours, settings.mid_days,
                      settings.mid_hours, settings.far_hours, settings.error_retry_hours)
    items, summary = plan(cities, coverage, today=today, horizon_days=settings.horizon_days,
                          targets=targets, now=now, exclude=queued_keys,
                          limit=max(0, settings.queue_target - queued),
                          window_tickets=settings.window_tickets or None,
                          unknown_window_days=settings.unknown_window_days)
    batch, pages = [], queued_pages
    for it in items:
        if pages >= settings.queue_pages:
            break
        batch.append(it)
        pages += it.est_pages
    submitted = 0
    if batch:
        ids = client.submit_batch([it.request(settings.max_pages) for it in batch], client="crawl")
        submitted = len(ids)
    summary.update({"queued_crawl": queued, "queued_pages_est": queued_pages,
                    "submitted_pages_est": pages - queued_pages, "submitted": submitted,
                    "queued_app": int(health.get("queued_app") or 0),
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
            print(f"[crawler] городов {s['cities']}, пар {s['pairs']}: свежих {s['fresh']}, "
                  f"устарело {s['stale']}, нет {s['missing']}, ошибок {s['errors']}; "
                  f"в очереди {s['queued_crawl']} (~{s['queued_pages_est']} стр.), подано {s['submitted']} (~{s['submitted_pages_est']} стр.), "
                  f"окон {s['windows']}, проход {s['pass_progress'] * 100:.1f} %")
        except CollectorError as e:
            print(f"[crawler] коллектор недоступен: {e}")
        except Exception as e:  # цикл не должен умирать
            print(f"[crawler] ошибка тика: {e!r}")
        time.sleep(max(1.0, settings.tick_seconds - (time.monotonic() - started)))


if __name__ == "__main__":
    run()
