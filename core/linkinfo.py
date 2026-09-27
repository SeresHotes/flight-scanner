"""Разбор поля `link` билета Travelpayouts (prices_for_dates).

Ссылка вида
`/search/MOW2910BJS1?t=MU17932941001793339400000455SVOPKX_<hash>_36584&static_fare_key=TY%7CP1%7CH1%7CL0%7CCH1%7CR0%7CTBC0&...`
несёт то, чего нет в остальных полях ответа:

- `t=` — маршрут билета: `<авиакомпания 2><вылет unix 10><прилёт unix 10>
  <длительность, мин 6><цепочка аэропортов по 3 буквы>_<hash>_<цена>`.
  Цепочка `SVOPKXHRB` = SVO→PKX→HRB: видно, ГДЕ пересадка (для hidden-city).
  Вылет — честный unix UTC (проверено: MU SVO→PKX 17:15 MSK). Прилётный
  таймстемп сдвинут на пояс прилёта (разница с вылетом ≠ длительность при смене
  пояса) — на него не опираемся, длительность берём из своего поля.
- `static_fare_key=TY|P1|H1|L1_1_23|CH1|R0|TBC1` — условия тарифа:
  `L0` — без багажа, `L1_<мест>_<кг>` — багаж включён; `H1` — ручная кладь,
  `CH`/`R` — обмен/возврат, `P`/`TBC`/`TY` — не расшифрованы. У старых ссылок
  параметра нет вовсе (багаж неизвестен).
"""
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, unquote, urlsplit

AVIASALES_BASE = "https://www.aviasales.ru"

_T_RE = re.compile(r"^([A-Z0-9]{2})(\d{10})(\d{10})(\d{6})([A-Z]{6,})_")
_BAGGAGE_RE = re.compile(r"^L(\d)(?:_(\d+)_(\d+))?$")


@dataclass
class Baggage:
    known: bool            # False — в ссылке нет static_fare_key
    included: bool = False
    pieces: Optional[int] = None
    kg: Optional[int] = None

    def as_dict(self) -> Dict[str, Any]:
        return {"known": self.known, "included": self.included,
                "pieces": self.pieces, "kg": self.kg}


@dataclass
class LinkInfo:
    airline: Optional[str] = None
    depart_ts: Optional[int] = None
    arrive_ts: Optional[int] = None
    duration_min: Optional[int] = None
    airports: List[str] = field(default_factory=list)   # SVO, PKX, HRB
    price: Optional[int] = None
    baggage: Baggage = field(default_factory=lambda: Baggage(known=False))
    fare: Dict[str, str] = field(default_factory=dict)  # сырые поля static_fare_key

    @property
    def transfers(self) -> List[str]:
        """Аэропорты пересадок — всё, кроме первого и последнего."""
        return self.airports[1:-1]


def parse_baggage(fare_key: Optional[str]) -> Baggage:
    """`TY|P1|H1|L1_1_23|CH1|R0|TBC1` → Baggage(included=True, pieces=1, kg=23)."""
    if not fare_key:
        return Baggage(known=False)
    for token in unquote(fare_key).split("|"):
        m = _BAGGAGE_RE.match(token)
        if not m:
            continue
        included = m.group(1) != "0"
        pieces = int(m.group(2)) if m.group(2) else None
        kg = int(m.group(3)) if m.group(3) else None
        return Baggage(known=True, included=included, pieces=pieces, kg=kg)
    return Baggage(known=False)


def parse_fare_key(fare_key: Optional[str]) -> Dict[str, str]:
    """Сырые поля тарифа: {'TY': '', 'P': '1', 'H': '1', 'L': '1_1_23', 'CH': '1', ...}."""
    fare: Dict[str, str] = {}
    if not fare_key:
        return fare
    for token in unquote(fare_key).split("|"):
        m = re.match(r"^([A-Z]+)(.*)$", token)
        if m:
            fare[m.group(1)] = m.group(2)
    return fare


def parse_t(t: Optional[str]) -> LinkInfo:
    """Разбирает параметр `t=`; при неизвестном формате — пустой LinkInfo."""
    info = LinkInfo()
    if not t:
        return info
    m = _T_RE.match(t)
    if not m:
        return info
    info.airline = m.group(1)
    info.depart_ts = int(m.group(2))
    info.arrive_ts = int(m.group(3))
    info.duration_min = int(m.group(4))
    chain = m.group(5)
    if len(chain) % 3 == 0:
        info.airports = [chain[i:i + 3] for i in range(0, len(chain), 3)]
    tail = t.rsplit("_", 1)[-1]
    if tail.isdigit():
        info.price = int(tail)
    return info


def parse_link(link: Optional[str]) -> LinkInfo:
    """Полный разбор `link`: маршрут из `t=` + багаж/тариф из `static_fare_key`."""
    if not link:
        return LinkInfo()
    query = parse_qs(urlsplit(link).query)
    info = parse_t((query.get("t") or [None])[0])
    fare_key = (query.get("static_fare_key") or [None])[0]
    info.baggage = parse_baggage(fare_key)
    info.fare = parse_fare_key(fare_key)
    return info


def booking_url(link: Optional[str]) -> Optional[str]:
    """Относительный `link` из API → абсолютная ссылка на Aviasales."""
    if not link:
        return None
    return link if link.startswith("http") else AVIASALES_BASE + link
