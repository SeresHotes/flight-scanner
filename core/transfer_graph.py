"""Граф пересадок по наблюдённым билетам.

Вершины — аэропорты, рёбра — прямые перелёты (сегменты из цепочки `t=` в link,
см. core.linkinfo). Отдельно хранятся наблюдённые пересадки: билеты с двумя и
более сегментами целиком (цена, дата, багаж) — по ним отвечаем на запросы
«какие пересадки видели по направлению A→X» (transfers_for) и главный продуктовый
— hidden-city: «билеты через H дешевле прямого A→H» (hidden_city). Идея: покупаем
A→H→X, выходим в H; работает только в одну сторону/на последнем перелёте и без
багажа, сдаваемого до X.

Источник строк — таблица quotes (storage.hot) или любой список словарей с полями
origin, destination, origin_airport, dest_airport, departure_at, price, airline,
transfers, link, observed_at. Коды в запросах — город (MOW) или аэропорт (SVO).
"""
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from core.linkinfo import Baggage, LinkInfo, booking_url, parse_link


@dataclass
class Ticket:
    airports: List[str]            # SVO, PKX, HRB — цепочка билета
    price: float
    date: str                      # день вылета YYYY-MM-DD
    airline: Optional[str]
    origin_city: Optional[str]
    dest_city: Optional[str]
    baggage: Baggage
    link: Optional[str]
    observed_at: Optional[str]
    duration_min: Optional[int]

    @property
    def transfers(self) -> List[str]:
        return self.airports[1:-1]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "airports": self.airports, "price": self.price, "date": self.date,
            "airline": self.airline, "origin_city": self.origin_city,
            "dest_city": self.dest_city, "baggage": self.baggage.as_dict(),
            "link": self.link, "observed_at": self.observed_at,
            "duration_min": self.duration_min,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Ticket":
        b = d.get("baggage") or {}
        return cls(airports=list(d["airports"]), price=d["price"], date=d["date"],
                   airline=d.get("airline"), origin_city=d.get("origin_city"),
                   dest_city=d.get("dest_city"),
                   baggage=Baggage(known=bool(b.get("known")), included=bool(b.get("included")),
                                   pieces=b.get("pieces"), kg=b.get("kg")),
                   link=d.get("link"), observed_at=d.get("observed_at"),
                   duration_min=d.get("duration_min"))


def _chain(row: Dict[str, Any], info: LinkInfo) -> List[str]:
    """Цепочка аэропортов билета: из link; для прямого без link — из полей строки."""
    if info.airports:
        return info.airports
    if int(row.get("transfers") or 0) == 0 and row.get("origin_airport") and row.get("dest_airport"):
        return [row["origin_airport"], row["dest_airport"]]
    return []


def ticket_from_row(row: Dict[str, Any]) -> Optional[Ticket]:
    """Строка quotes → Ticket; None, если цепочку аэропортов восстановить нельзя."""
    info = parse_link(row.get("link"))
    airports = _chain(row, info)
    if len(airports) < 2 or row.get("price") is None or not row.get("departure_at"):
        return None
    return Ticket(
        airports=airports, price=float(row["price"]), date=str(row["departure_at"])[:10],
        airline=row.get("airline") or info.airline,
        origin_city=row.get("origin"), dest_city=row.get("destination"),
        baggage=info.baggage, link=row.get("link"), observed_at=row.get("observed_at"),
        duration_min=info.duration_min if info.duration_min is not None else row.get("duration"),
    )


class TransferGraph:
    def __init__(self, network: Optional[Dict[str, Dict[str, Any]]] = None):
        self.network = network or {}
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.edges: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self.nonstop: List[Ticket] = []
        self.transfers: List[Ticket] = []
        self.city_airports: Dict[str, Set[str]] = defaultdict(set)
        self.skipped = 0  # строк без восстановимой цепочки

    # ------------------------------- построение ------------------------------

    def _node(self, code: str) -> Dict[str, Any]:
        node = self.nodes.get(code)
        if node is None:
            info = self.network.get(code, {})
            node = {"city": None, "country": info.get("country") or info.get("iso_country"),
                    "name": info.get("municipality") or info.get("name")}
            self.nodes[code] = node
        return node

    def _edge(self, a: str, b: str) -> Dict[str, Any]:
        edge = self.edges.get((a, b))
        if edge is None:
            edge = {"tickets": 0, "airlines": set(), "nonstop_tickets": 0, "nonstop_min_price": None}
            self.edges[(a, b)] = edge
        return edge

    def add(self, ticket: Ticket) -> None:
        first, last = ticket.airports[0], ticket.airports[-1]
        if ticket.origin_city:
            self._node(first)["city"] = ticket.origin_city
            self.city_airports[ticket.origin_city].add(first)
        if ticket.dest_city:
            self._node(last)["city"] = ticket.dest_city
            self.city_airports[ticket.dest_city].add(last)
        for a, b in zip(ticket.airports, ticket.airports[1:]):
            self._node(a)
            self._node(b)
            edge = self._edge(a, b)
            edge["tickets"] += 1
            if ticket.airline:
                edge["airlines"].add(ticket.airline)
        if len(ticket.airports) == 2:
            edge = self._edge(first, last)
            edge["nonstop_tickets"] += 1
            if edge["nonstop_min_price"] is None or ticket.price < edge["nonstop_min_price"]:
                edge["nonstop_min_price"] = ticket.price
            self.nonstop.append(ticket)
        else:
            self.transfers.append(ticket)

    def add_rows(self, rows: Iterable[Dict[str, Any]]) -> None:
        for row in rows:
            t = ticket_from_row(dict(row))
            if t is None:
                self.skipped += 1
                continue
            self.add(t)

    # ------------------------------ сериализация -----------------------------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "format": "transfer-graph-v1",
            "nodes": self.nodes,
            "edges": [{"from": a, "to": b, **e, "airlines": sorted(e["airlines"])}
                      for (a, b), e in sorted(self.edges.items())],
            "nonstop": [t.as_dict() for t in self.nonstop],
            "transfers": [t.as_dict() for t in self.transfers],
            "stats": self.stats(),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any], network=None) -> "TransferGraph":
        g = cls(network)
        for t in d.get("nonstop", []) + d.get("transfers", []):
            g.add(Ticket.from_dict(t))
        return g

    def stats(self) -> Dict[str, int]:
        return {"nodes": len(self.nodes), "edges": len(self.edges),
                "nonstop_tickets": len(self.nonstop), "transfer_tickets": len(self.transfers),
                "skipped_rows": self.skipped}

    # --------------------------------- запросы -------------------------------

    def resolve(self, code: str) -> Set[str]:
        """Код города → его аэропорты по наблюдениям; код аэропорта → он сам."""
        code = code.upper()
        airports = set(self.city_airports.get(code, ()))
        if code in self.nodes:
            airports.add(code)
        return airports

    def country(self, airport: str) -> Optional[str]:
        return (self.nodes.get(airport) or {}).get("country")

    def _from_to(self, origins: Set[str], dests: Set[str], tickets: List[Ticket]) -> List[Ticket]:
        return [t for t in tickets if t.airports[0] in origins and t.airports[-1] in dests]

    def transfers_for(self, origin: str, destination: str) -> Dict[str, Any]:
        """Наблюдённые пересадки по направлению A→X: группы по цепочке пересадок с
        числом билетов, минимальной ценой, авиакомпаниями и датами; плюс лучший прямой."""
        origins, dests = self.resolve(origin), self.resolve(destination)
        nonstop = self._from_to(origins, dests, self.nonstop)
        groups: Dict[Tuple[str, ...], List[Ticket]] = defaultdict(list)
        for t in self._from_to(origins, dests, self.transfers):
            groups[tuple(t.transfers)].append(t)
        via = []
        for chain, items in groups.items():
            cheapest = min(items, key=lambda t: t.price)
            via.append({
                "via": list(chain), "tickets": len(items), "min_price": cheapest.price,
                "airlines": sorted({t.airline for t in items if t.airline}),
                "dates": [min(t.date for t in items), max(t.date for t in items)],
                "baggage_included": sum(1 for t in items if t.baggage.included),
                "cheapest": _ticket_view(cheapest),
            })
        via.sort(key=lambda v: v["min_price"])
        return {
            "origin": origin.upper(), "destination": destination.upper(),
            "origin_airports": sorted(origins), "destination_airports": sorted(dests),
            "nonstop_min_price": min((t.price for t in nonstop), default=None),
            "nonstop_tickets": len(nonstop),
            "via": via,
        }

    def hidden_city(self, origin: str, via: str, same_day_only: bool = False) -> Dict[str, Any]:
        """Hidden-city: билеты A→…→H→…→X дешевле прямого A→H (выходим в H).

        Прямой для сравнения берём на тот же день вылета; если в тот день прямых не
        видели — самый дешёвый прямой за всё время (direct_same_day=False). При
        same_day_only без прямого в тот же день билет отбрасывается. Если прямых
        A→H не наблюдалось вовсе, отдаём всех кандидатов с saving=None."""
        origins, hubs = self.resolve(origin), self.resolve(via)
        nonstop = self._from_to(origins, hubs, self.nonstop)
        direct_by_day: Dict[str, float] = {}
        for t in nonstop:
            if t.date not in direct_by_day or t.price < direct_by_day[t.date]:
                direct_by_day[t.date] = t.price
        direct_any = min(direct_by_day.values(), default=None)

        tickets = []
        for t in self.transfers:
            if t.airports[0] not in origins:
                continue
            hub_idx = next((i for i, a in enumerate(t.transfers, 1) if a in hubs), None)
            if hub_idx is None:
                continue
            same_day = t.date in direct_by_day
            direct_price = direct_by_day.get(t.date, direct_any)
            if same_day_only and not same_day:
                continue
            if direct_price is not None and t.price >= direct_price:
                continue
            hub = t.airports[hub_idx]
            nxt = t.airports[hub_idx + 1]
            view = _ticket_view(t)
            view.update({
                "hub": hub, "hub_index": hub_idx,
                "dropped": t.airports[hub_idx + 1:],
                "direct_price": direct_price, "direct_same_day": same_day,
                "saving": (direct_price - t.price) if direct_price is not None else None,
                # Багаж на международном→внутреннем стыке обычно получают в первом
                # аэропорту въезда (то есть в H) — иначе он улетит в X.
                "next_leg_domestic": (self.country(hub) is not None
                                      and self.country(hub) == self.country(nxt)),
            })
            tickets.append(view)
        tickets.sort(key=lambda v: (-(v["saving"] or 0), v["price"]))
        return {
            "origin": origin.upper(), "via": via.upper(),
            "origin_airports": sorted(origins), "via_airports": sorted(hubs),
            "direct_min_price": direct_any, "direct_days": len(direct_by_day),
            "tickets": tickets,
        }


def _ticket_view(t: Ticket) -> Dict[str, Any]:
    return {
        "airports": t.airports, "price": t.price, "date": t.date, "airline": t.airline,
        "destination": t.dest_city or t.airports[-1], "baggage": t.baggage.as_dict(),
        "duration_min": t.duration_min, "url": booking_url(t.link), "observed_at": t.observed_at,
    }


def load_quote_rows(conn) -> List[Dict[str, Any]]:
    """Все котировки из SQLite (storage.hot.quotes) для построения графа."""
    cur = conn.execute(
        "SELECT origin, destination, origin_airport, dest_airport, departure_at, price, "
        "airline, transfers, duration, link, observed_at FROM quotes")
    return [dict(r) for r in cur.fetchall()]


def build_from_db(conn, network=None) -> TransferGraph:
    g = TransferGraph(network)
    g.add_rows(load_quote_rows(conn))
    return g
