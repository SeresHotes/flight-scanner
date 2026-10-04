"""Настройки фонового сборщика из окружения."""
import os
from dataclasses import dataclass, field
from typing import List

from dotenv import load_dotenv

load_dotenv()

# Стартовые хабы: с них начинается обход, дальше города открываются из ответов
# (destination каждого билета попадает в /v1/cities коллектора).
DEFAULT_SEEDS = "MOW,LED,SVX,OVB,KZN,AER,KRR,UFA,IST,AYT,DXB,SEL,BJS,SHA,BKK,TAS,ALA,EVN,TBS,BER,PAR,LON,ROM,MIL"


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw not in (None, "") else default


def _float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw not in (None, "") else default


@dataclass
class Settings:
    collector_url: str = field(default_factory=lambda: os.getenv("COLLECTOR_URL", "http://collector:8001"))
    seeds: List[str] = field(default_factory=lambda: [
        c.strip().upper() for c in os.getenv("CRAWL_SEED_CITIES", DEFAULT_SEEDS).split(",") if c.strip()])
    # Горизонт: дни вылета от сегодня (включительно) вперёд.
    horizon_days: int = field(default_factory=lambda: _int("CRAWL_HORIZON_DAYS", 180))
    # Как часто пересчитывать план и досыпать очередь; сколько серий держать в очереди crawl.
    tick_seconds: float = field(default_factory=lambda: _float("CRAWL_TICK_SECONDS", 120))
    # Глубина очереди — в оценочных страницах (средняя плотность × дни / 400): ~30 минут
    # работы ручки на 60/мин — переживает паузу сборщика. Окон — не больше queue_target
    # (предохранитель: при неверной оценке очередь не раздувается).
    queue_pages: int = field(default_factory=lambda: _int("CRAWL_QUEUE_PAGES", 1800))
    queue_target: int = field(default_factory=lambda: _int("CRAWL_QUEUE_TARGET", 3000))
    max_pages: int = field(default_factory=lambda: _int("CRAWL_MAX_PAGES", 100))
    # Окна дат: бюджет билетов на запрос (≈ 25 страниц; окно = бюджет / плотность города —
    # у Москвы день, у маленького города месяц) и длина окна для города без покрытия.
    # CRAWL_WINDOW_TICKETS=0 — по одному дню.
    window_tickets: int = field(default_factory=lambda: _int("CRAWL_WINDOW_TICKETS", 10_000))
    unknown_window_days: int = field(default_factory=lambda: _int("CRAWL_UNKNOWN_WINDOW_DAYS", 30))
    # Новый проход по горизонту — не раньше чем через столько часов после начала предыдущего.
    # По умолчанию 0: дошли до конца — сразу снова с сегодняшнего дня (с 04.10.2026; при 24
    # проход за ~полсуток оставлял ручку простаивать до конца суток, данные не обновлялись).
    min_pass_hours: float = field(default_factory=lambda: _float("CRAWL_MIN_PASS_HOURS", 0))
