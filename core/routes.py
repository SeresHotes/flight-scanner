"""Реестр маршрутов, для которых уже собраны данные и есть конфиг сборки.

Сейчас доступен единственный маршрут MOW→ICN (событие MCR). Конфиг восстановлен
так, что build_from_config воспроизводит текущий web/data.json байт-в-байт.
В Фазе 2 реестр наполняется автоматически по мере сбора коллектором произвольных A/B.
"""
from typing import Dict, Optional, Tuple

from core.trip_builder import Config

# ключ — (origin, destination) в верхнем регистре
ROUTES: Dict[Tuple[str, str], Config] = {
    ("MOW", "ICN"): Config(min_stay=7, stopover_days=(2, 14), max_stay=34),
}


def get_route_config(origin: str, destination: str) -> Optional[Config]:
    return ROUTES.get((origin.upper(), destination.upper()))


def get_route_keys() -> list:
    """Список доступных пар (origin, destination)."""
    return list(ROUTES.keys())


def make_route_config(origin: str, destination: str,
                      stop_days=(2, 7), min_stay: int = 1, max_stay: int = 30) -> Config:
    """Конфиг сборки для произвольного маршрута (без события, имена — из справочника)."""
    from core.airports import get_airport

    o = get_airport(origin)
    d = get_airport(destination)
    return Config(
        origin=origin, origin_name=(o["label"] if o else origin),
        hub=destination, hub_name=(d["label"] if d else destination),
        region_label=(d["city"] if d else destination),
        region_flag=(d["flag"] if d else ""),
        min_stay=min_stay, max_stay=max_stay, stopover_days=tuple(stop_days),
        arrive_by=None, event_name="", event_date="", event_venue="",
    )
