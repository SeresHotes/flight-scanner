"""Настройки коллектора из окружения (env_file /opt/flights/.env на VM, .env локально)."""
import os
from dataclasses import dataclass, field
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw not in (None, "") else default


def _float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw not in (None, "") else default


@dataclass
class Settings:
    # Горячее состояние коллектора: индекс серий и файлов озера, города.
    db_path: str = field(default_factory=lambda: os.getenv("COLLECTOR_DB", "data/collector.db"))
    # Озеро: S3 (все S3_* заданы) или локальный каталог (dev/тесты).
    s3_endpoint: Optional[str] = field(default_factory=lambda: os.getenv("S3_ENDPOINT"))
    s3_region: Optional[str] = field(default_factory=lambda: os.getenv("S3_REGION"))
    s3_bucket: Optional[str] = field(default_factory=lambda: os.getenv("S3_BUCKET"))
    s3_access_key: Optional[str] = field(default_factory=lambda: os.getenv("S3_ACCESS_KEY"))
    s3_secret_key: Optional[str] = field(default_factory=lambda: os.getenv("S3_SECRET_KEY"))
    lake_local_root: str = field(default_factory=lambda: os.getenv("LAKE_LOCAL_ROOT", "data/lake"))
    # Лимит ручки GraphQL: запросов в минуту (источник даёт 60).
    rate_per_minute: float = field(default_factory=lambda: _float("RATE_PER_MINUTE", 60.0))
    # Свежесть серии по умолчанию: источник сам кэширует цены ~сутки.
    ttl_seconds: int = field(default_factory=lambda: _int("SERIES_TTL_SECONDS", 24 * 3600))
    # Предохранитель страниц на серию: offset > 14 800 источник не отдаёт (37 × 400).
    max_pages: int = field(default_factory=lambda: _int("COLLECTOR_MAX_PAGES", 37))
    # Сброс буфера серий в файл озера: по числу билетов (оценка объёма) или по времени.
    flush_tickets: int = field(default_factory=lambda: _int("LAKE_FLUSH_TICKETS", 25_000))
    flush_seconds: int = field(default_factory=lambda: _int("LAKE_FLUSH_SECONDS", 300))
    # Ретеншн озера: потолок объёма tickets/ (ниже жёсткого max_size бакета 200 ГБ).
    lake_max_gb: float = field(default_factory=lambda: _float("LAKE_MAX_GB", 180.0))
    retention_interval_seconds: int = field(default_factory=lambda: _int("LAKE_RETENTION_INTERVAL", 1800))
    # Сколько держать завершённые задания в памяти (результат забирают один раз).
    job_keep_seconds: int = field(default_factory=lambda: _int("COLLECTOR_JOB_KEEP_SECONDS", 3600))

    @property
    def s3_configured(self) -> bool:
        return bool(self.s3_bucket and self.s3_access_key and self.s3_secret_key)
