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
    queue_target: int = field(default_factory=lambda: _int("CRAWL_QUEUE_TARGET", 200))
    max_pages: int = field(default_factory=lambda: _int("CRAWL_MAX_PAGES", 37))
    # Целевая свежесть по дальности даты вылета (часы): ближние — чаще.
    near_days: int = field(default_factory=lambda: _int("CRAWL_FRESH_NEAR_DAYS", 14))
    near_hours: float = field(default_factory=lambda: _float("CRAWL_FRESH_NEAR_HOURS", 24))
    mid_days: int = field(default_factory=lambda: _int("CRAWL_FRESH_MID_DAYS", 60))
    mid_hours: float = field(default_factory=lambda: _float("CRAWL_FRESH_MID_HOURS", 72))
    far_hours: float = field(default_factory=lambda: _float("CRAWL_FRESH_FAR_HOURS", 168))
    # Серия с ошибкой источника повторяется не раньше чем через столько часов.
    error_retry_hours: float = field(default_factory=lambda: _float("CRAWL_ERROR_RETRY_HOURS", 24))
