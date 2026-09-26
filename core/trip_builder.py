#!/usr/bin/env python3
"""
Универсальный сборщик данных для веб-планировщика поездки «туда-обратно с остановкой».

Собирает полные маршруты из датасетов collect_flights.py:
  --plain  : прямые (одним билетом) плечи ORIGIN <-> HUB
  --there  : ORIGIN -> [любой город] -> HUB   (остановка ПО ПУТИ ТУДА)
  --back   : HUB -> [любой город] -> ORIGIN   (остановка НА ОБРАТНОМ ПУТИ)

Формирует три категории:
  - direct         : ORIGIN -> HUB -> ORIGIN (без длительной остановки)
  - stopover_there : ORIGIN -> [город N дн] -> HUB -> ORIGIN
  - stopover_back  : ORIGIN -> HUB -> [город N дн] -> ORIGIN

Гарантирует >= --min-stay дней в пункте назначения (HUB-регионе). Если задано --event-date,
маршрут строится так, чтобы событие попадало в окно пребывания.

Ничего в скрипте не захардкожено под конкретную поездку — всё задаётся аргументами.
Значения по умолчанию соответствуют примеру «Москва → Корея на MCR», чтобы работало из коробки.

Результат: <out>/data.json и <out>/data.js
"""

import argparse
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from core import aggregate as agg


# ------------------------------- утилиты -------------------------------------

def flag_emoji(iso2: str) -> str:
    if not iso2 or len(iso2) != 2 or not iso2.isalpha():
        return ""
    return "".join(chr(0x1F1E6 + ord(c.upper()) - ord("A")) for c in iso2)


def make_city_lookup(network: dict):
    # Метро-коды агломераций (BJS, LON, TYO…) физических аэропортов в сети не имеют,
    # поэтому их имена берём из курируемого справочника — иначе в выдаче остаётся
    # голый код («BJS» вместо «Beijing»). Импорт ленивый: рвём цикл airports↔trip_builder.
    from core.airports import _CITY_BY_CODE

    def city_info(code: str) -> dict:
        info = network.get(code, {})
        name = info.get("municipality") or info.get("name")
        # build_airport_network кладёт ISO2 страны в "country" (iso_country — поле
        # исходного CSV); раньше читали только его, и у городов из сети не было флага.
        country = info.get("country") or info.get("iso_country") or ""
        if not name:
            entry = _CITY_BY_CODE.get(code)
            if entry:
                name, country = entry[2], entry[3]  # английское имя, ISO2 страны
        return {"city": name or code, "country": country, "flag": flag_emoji(country)}
    return city_info


def ddmm(iso_dt: str) -> str:
    d = agg.parse_datetime(iso_dt)
    return f"{d.day:02d}{d.month:02d}"


def booking_link(origin: str, dest: str, depart_at: str) -> str:
    return f"https://www.aviasales.ru/search/{origin}{ddmm(depart_at)}{dest}1"


def arrival_of(flight: dict) -> str:
    if flight.get("arrival_at"):
        return flight["arrival_at"]
    if flight.get("departure_at") and flight.get("duration"):
        return agg.calculate_arrival(flight["departure_at"], flight["duration"])
    return flight.get("departure_at")


def date_only(iso_dt: str) -> str:
    return agg.parse_datetime(iso_dt).strftime("%Y-%m-%d")


def stay_between(arrive_iso: str, depart_iso: str) -> int:
    return agg.calculate_stay_duration(arrive_iso, depart_iso)


def load(path: str) -> dict:
    return json.load(open(path, encoding="utf-8"))


def load_merged(paths) -> dict:
    """Объединяет leg1/leg2 из нескольких датасетов (существующие файлы)."""
    leg1, leg2 = [], []
    for p in paths:
        if not p or not Path(p).exists():
            continue
        d = load(p)
        leg1 += d.get("leg1_flights", [])
        leg2 += d.get("leg2_flights", [])
    return {"leg1_flights": leg1, "leg2_flights": leg2}


# --------------------------- построение сегментов ----------------------------

class Builder:
    def __init__(self, cfg, city_info):
        self.cfg = cfg
        self.city_info = city_info

    def make_segment(self, flight: dict) -> dict:
        origin = flight.get("origin") or flight.get("search_origin")
        dest = flight.get("destination") or flight.get("search_destination")
        dep = flight.get("departure_at")
        arr = arrival_of(flight)
        transfers = int(flight.get("transfers") or 0)
        return {
            "origin": origin,
            "destination": dest,
            "origin_airport": flight.get("origin_airport") or origin,
            "destination_airport": flight.get("destination_airport") or dest,
            "origin_city": self.city_info(origin)["city"],
            "destination_city": self.city_info(dest)["city"],
            "departure_at": dep,
            "arrival_at": arr,
            "duration": flight.get("duration"),
            "transfers": transfers,
            "direct": transfers == 0,
            "transfer_points": self._transfer_points(flight.get("link"), transfers),
            "airline": flight.get("airline"),
            "flight_number": flight.get("flight_number"),
            "price": flight.get("price") or flight.get("value", 0),
            "link": booking_link(origin, dest, dep) if dep else None,
        }

    def _transfer_points(self, link: str, transfers: int) -> list:
        """Города пересадок внутри билета — парсятся из токена ссылки Aviasales
        (цепочка аэропортов вида SVO+TJM+NYA). Возвращает промежуточные точки."""
        if not link or not transfers:
            return []
        m = re.search(r'[?&]t=([^&]+)', link)
        if not m:
            return []
        m2 = re.search(r'([A-Z]{6,})_', m.group(1))
        if not m2:
            return []
        chain = m2.group(1)
        codes = [chain[i:i + 3] for i in range(0, len(chain), 3)]
        if len(codes) != transfers + 2:   # цепочка не бьётся с числом пересадок
            return []
        return [{"code": c, "city": self.city_info(c)["city"]} for c in codes[1:-1]]

    def _find(self, data):
        """find_combinations с учётом переходов между близкими аэропортами."""
        return agg.find_combinations(
            data, min_stay=self.cfg.stop_min, max_stay=self.cfg.stop_max,
            airport_network=self.cfg.airport_network,
            max_airport_distance=self.cfg.airport_distance,
            same_country_only=self.cfg.same_country)

    def _stopover_obj(self, c):
        """Объект остановки; добавляет info о переходе между аэропортами, если он есть."""
        city = c["intermediate_city"]
        ci = self.city_info(city)
        obj = {"code": city, "city": ci["city"], "country": ci["country"],
               "flag": ci["flag"], "days": c["stay_days"]}
        at = c.get("airport_transfer")
        if at:
            obj["transfer"] = {
                "from": at["from_airport"], "to": at["to_airport"],
                "from_city": at.get("from_city") or self.city_info(at["from_airport"])["city"],
                "to_city": at.get("to_city") or self.city_info(at["to_airport"])["city"],
                "distance_km": at.get("distance_km"),
            }
        return obj

    def segment_from_combo_leg(self, leg: dict) -> dict:
        return self.make_segment({
            "origin": leg.get("origin"),
            "destination": leg.get("destination"),
            "departure_at": leg.get("departure_at"),
            "arrival_at": leg.get("arrival_at"),
            "duration": leg.get("duration"),
            "transfers": leg.get("transfers", 0),
            "airline": leg.get("airline"),
            "flight_number": leg.get("flight_number"),
            "price": leg.get("price", 0),
            "link": leg.get("link"),
        })

    # --- ограничения пребывания/события ---
    def event_ok(self, arrive_iso: str, depart_iso: str) -> bool:
        ev = self.cfg.event_date
        if not ev:
            return True
        return date_only(arrive_iso) <= ev <= date_only(depart_iso)

    def arrival_ok(self, arrive_iso: str) -> bool:
        if not self.cfg.arrive_by:
            return True
        return date_only(arrive_iso) <= self.cfg.arrive_by

    def stay_ok(self, days: int) -> bool:
        return self.cfg.min_stay <= days <= self.cfg.max_stay

    # --- категории ---
    def build_direct(self, plain_out, plain_ret):
        trips = []
        for out in plain_out:
            out_arr = arrival_of(out)
            if not self.arrival_ok(out_arr):
                continue
            for ret in plain_ret:
                days = stay_between(out_arr, ret["departure_at"])
                if not self.stay_ok(days) or not self.event_ok(out_arr, ret["departure_at"]):
                    continue
                segs = [self.make_segment(out), self.make_segment(ret)]
                trips.append({
                    "type": "direct", "stopover": None,
                    "total_price": segs[0]["price"] + segs[1]["price"],
                    "stay_days": days, "segments": segs,
                })
        return trips

    def build_there(self, there_data, plain_ret):
        combos = self._find(there_data)
        trips = []
        for c in combos:
            city = c["intermediate_city"]
            if city in self.cfg.exclude:
                continue
            hub_arr = c["leg2"]["arrival_at"]
            if not self.arrival_ok(hub_arr):
                continue
            best = self._cheapest(plain_ret, lambda r: self.stay_ok(stay_between(hub_arr, r["departure_at"]))
                                  and self.event_ok(hub_arr, r["departure_at"]))
            if not best:
                continue
            seg1 = self.segment_from_combo_leg(c["leg1"])
            seg2 = self.segment_from_combo_leg(c["leg2"])
            seg3 = self.make_segment(best)
            trips.append({
                "type": "stopover_there",
                "total_price": seg1["price"] + seg2["price"] + seg3["price"],
                "stay_days": stay_between(hub_arr, best["departure_at"]),
                "stopover": self._stopover_obj(c),
                "segments": [seg1, seg2, seg3],
            })
        return trips

    def build_back(self, back_data, plain_out):
        combos = self._find(back_data)
        trips = []
        for c in combos:
            city = c["intermediate_city"]
            if city in self.cfg.exclude:
                continue
            hub_dep = c["leg1"]["departure_at"]
            best = self._cheapest(plain_out, lambda o: self.arrival_ok(arrival_of(o))
                                  and self.stay_ok(stay_between(arrival_of(o), hub_dep))
                                  and self.event_ok(arrival_of(o), hub_dep))
            if not best:
                continue
            seg1 = self.make_segment(best)
            seg2 = self.segment_from_combo_leg(c["leg1"])
            seg3 = self.segment_from_combo_leg(c["leg2"])
            trips.append({
                "type": "stopover_back",
                "total_price": seg1["price"] + seg2["price"] + seg3["price"],
                "stay_days": stay_between(arrival_of(best), hub_dep),
                "stopover": self._stopover_obj(c),
                "segments": [seg1, seg2, seg3],
            })
        return trips

    def build_both(self, there_data, back_data):
        """ORIGIN -> [город N дн] -> HUB -> [тот же город M дн] -> ORIGIN, все сегменты прямые.

        Оба плеча в/из HUB идут через один и тот же промежуточный город отдельными
        прямыми билетами, поэтому пересадок внутри билетов нет (max_transfers == 0).
        """
        there = agg.find_combinations(there_data, min_stay=self.cfg.stop_min, max_stay=self.cfg.stop_max)
        back = agg.find_combinations(back_data, min_stay=self.cfg.stop_min, max_stay=self.cfg.stop_max)

        there_by, back_by = {}, {}
        for c in there:
            city = c["intermediate_city"]
            if city in self.cfg.exclude or not self.arrival_ok(c["leg2"]["arrival_at"]):
                continue
            there_by.setdefault(city, []).append(c)
        for c in back:
            city = c["intermediate_city"]
            if city in self.cfg.exclude:
                continue
            back_by.setdefault(city, []).append(c)

        trips = []
        for city in set(there_by) & set(back_by):
            best = None
            for a in there_by[city]:
                korea_arr = a["leg2"]["arrival_at"]
                for bk in back_by[city]:
                    korea_dep = bk["leg1"]["departure_at"]
                    kd = stay_between(korea_arr, korea_dep)
                    if not self.stay_ok(kd) or not self.event_ok(korea_arr, korea_dep):
                        continue
                    price = a["total_price"] + bk["total_price"]
                    if best is None or price < best["price"]:
                        best = {"a": a, "b": bk, "kd": kd, "price": price}
            if not best:
                continue
            a, bk = best["a"], best["b"]
            seg1 = self.segment_from_combo_leg(a["leg1"])   # ORIGIN -> город
            seg2 = self.segment_from_combo_leg(a["leg2"])   # город -> HUB
            seg3 = self.segment_from_combo_leg(bk["leg1"])  # HUB -> город
            seg4 = self.segment_from_combo_leg(bk["leg2"])  # город -> ORIGIN
            ci = self.city_info(city)
            trips.append({
                "type": "stopover_both",
                "total_price": sum(s["price"] for s in (seg1, seg2, seg3, seg4)),
                "stay_days": best["kd"],
                "stopover": {"code": city, "city": ci["city"], "country": ci["country"],
                              "flag": ci["flag"], "days": a["stay_days"] + bk["stay_days"],
                              "days_there": a["stay_days"], "days_back": bk["stay_days"]},
                "segments": [seg1, seg2, seg3, seg4],
            })
        return trips

    # --- односторонние варианты (только туда / только обратно) ---
    def oneway_there_plain(self, plain_out):
        opts = []
        for out in plain_out:
            arr = arrival_of(out)
            if not self.arrival_ok(arr):
                continue
            seg = self.make_segment(out)
            opts.append({"direction": "there", "type": "direct", "has_stopover": False,
                         "total_price": seg["price"], "segments": [seg], "stopover": None,
                         "note_date": date_only(arr)})
        return opts

    def oneway_there_stop(self, there_data):
        combos = self._find(there_data)
        opts = []
        for c in combos:
            city = c["intermediate_city"]
            if city in self.cfg.exclude or not self.arrival_ok(c["leg2"]["arrival_at"]):
                continue
            seg1 = self.segment_from_combo_leg(c["leg1"])
            seg2 = self.segment_from_combo_leg(c["leg2"])
            opts.append({"direction": "there", "type": "stopover_there", "has_stopover": True,
                         "total_price": seg1["price"] + seg2["price"], "segments": [seg1, seg2],
                         "stopover": self._stopover_obj(c),
                         "note_date": date_only(c["leg2"]["arrival_at"])})
        return opts

    def oneway_back_plain(self, plain_ret):
        opts = []
        for ret in plain_ret:
            dep = ret.get("departure_at")
            if self.cfg.return_from and date_only(dep) < self.cfg.return_from:
                continue
            seg = self.make_segment(ret)
            opts.append({"direction": "back", "type": "direct", "has_stopover": False,
                         "total_price": seg["price"], "segments": [seg], "stopover": None,
                         "note_date": date_only(dep)})
        return opts

    def oneway_back_stop(self, back_data):
        combos = self._find(back_data)
        opts = []
        for c in combos:
            city = c["intermediate_city"]
            if city in self.cfg.exclude:
                continue
            dep = c["leg1"]["departure_at"]
            if self.cfg.return_from and date_only(dep) < self.cfg.return_from:
                continue
            seg1 = self.segment_from_combo_leg(c["leg1"])
            seg2 = self.segment_from_combo_leg(c["leg2"])
            opts.append({"direction": "back", "type": "stopover_back", "has_stopover": True,
                         "total_price": seg1["price"] + seg2["price"], "segments": [seg1, seg2],
                         "stopover": self._stopover_obj(c),
                         "note_date": date_only(dep)})
        return opts

    def combine_two(self, there_opts, back_opts, per_there=1, cap=700):
        """Round-trip с остановками в РАЗНЫХ городах:
        ORIGIN → X (2-6 дн) → HUB (>=min_stay) → Y (2-6 дн) → ORIGIN, где X != Y.
        Делается в два прохода: обычный (самый дешёвый на город, обычно с пересадками)
        и беспосадочный (для варианта «без пересадок») — иначе беспосадочных 2-городных не будет."""
        def nonstop_opt(o):
            return all(int(s.get("transfers") or 0) == 0 for s in o["segments"])

        def best_by_city(opts, nonstop):
            # дешёвейший на каждую пару (город, длительность остановки) — чтобы сохранить
            # разные длительности остановки и в двухгородных маршрутах
            best = {}
            for o in opts:
                if nonstop and not nonstop_opt(o):
                    continue
                key = (o["stopover"]["code"], o["stopover"]["days"])
                if key not in best or o["total_price"] < best[key]["total_price"]:
                    best[key] = o
            return best

        trips = []
        for nonstop in (False, True):
            there_by = best_by_city(there_opts, nonstop)
            back_by = list(best_by_city(back_opts, nonstop).values())
            for (xcode, _xdays), xo in there_by.items():
                x_arr = xo["segments"][-1]["arrival_at"]        # прилёт в HUB
                cand = []
                for yo in back_by:
                    if yo["stopover"]["code"] == xcode:          # нужен ДРУГОЙ город
                        continue
                    y_dep = yo["segments"][0]["departure_at"]    # вылет из HUB
                    kd = stay_between(x_arr, y_dep)
                    if not self.stay_ok(kd) or not self.event_ok(x_arr, y_dep):
                        continue
                    cand.append((xo["total_price"] + yo["total_price"], kd, yo))
                cand.sort(key=lambda z: z[0])
                for price, kd, yo in cand[:per_there]:
                    trips.append({
                        "type": "stopover_two", "direction": "round", "has_stopover": True,
                        "total_price": price, "stay_days": kd, "stopover": None,
                        "stop_there": xo["stopover"], "stop_back": yo["stopover"],
                        "segments": xo["segments"] + yo["segments"],
                    })
        trips.sort(key=lambda x: x["total_price"])
        return trips[:cap]

    @staticmethod
    def _cheapest(flights, predicate):
        best = None
        for f in flights:
            if not predicate(f):
                continue
            price = f.get("price") or f.get("value", 0)
            if best is None or price < (best.get("price") or best.get("value", 0)):
                best = f
        return best


def keep_variety(opts):
    """Оставляет дешёвейший вариант на каждую пару (город, длительность остановки) И на
    каждую пару (город, дата касания хаба). Так сохраняются и разные длительности остановки,
    и разные даты прилёта/вылета из региона (нужны для фильтра «дней на месте»)."""
    best_days, best_date = {}, {}
    for o in opts:
        c = o["city"]
        kd = (c, o["sdays"])
        if kd not in best_days or o["total_price"] < best_days[kd]["total_price"]:
            best_days[kd] = o
        kt = (c, o["hub_date"])
        if kt not in best_date or o["total_price"] < best_date[kt]["total_price"]:
            best_date[kt] = o
    seen, out = set(), []
    for o in list(best_days.values()) + list(best_date.values()):
        if id(o) in seen:
            continue
        seen.add(id(o))
        out.append(o)
    return out


def keep_cheapest_per_city(trips, per_city: int):
    """Оставляет несколько самых дешёвых на каждую пару (город, длительность остановки)
    + гарантирует беспосадочный вариант, если он есть. Группировка по (город, дни) нужна,
    чтобы сохранить разные длительности остановки (иначе на город остаётся лишь одна)."""
    def all_nonstop(t):
        return all(int(s.get("transfers") or 0) == 0 for s in t["segments"])
    groups = {}
    for t in trips:
        key = (t["stopover"]["code"], t["stopover"]["days"])
        groups.setdefault(key, []).append(t)
    out = []
    for lst in groups.values():
        lst.sort(key=lambda x: x["total_price"])
        chosen = lst[:per_city]
        if not any(all_nonstop(t) for t in chosen):
            ns = next((t for t in lst if all_nonstop(t)), None)
            if ns:
                chosen.append(ns)
        out.extend(chosen)
    return out


# ----------------------------------- CLI -------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Сборка данных для веб-планировщика поездки")
    p.add_argument("--origin", default="MOW", help="IATA код города вылета")
    p.add_argument("--origin-name", default="Москва (MOW)", help="Отображаемое имя города вылета")
    p.add_argument("--hub", default="ICN", help="IATA код пункта назначения (куда летим)")
    p.add_argument("--hub-name", default="Сеул / Инчхон (ICN)", help="Отображаемое имя пункта назначения")
    p.add_argument("--region-label", default=None, help="Как называть регион пребывания (по умолчанию — город хаба)")
    p.add_argument("--region-flag", default=None, help="Эмодзи-флаг региона (по умолчанию — по стране хаба)")

    p.add_argument("--plain", default="data/mcr_plain.json", help="Датасет прямых плеч ORIGIN<->HUB")
    p.add_argument("--there", default="data/mcr_stopover_there.json", help="Датасет остановки ПО ПУТИ ТУДА, беспосадочные плечи (или '' чтобы пропустить)")
    p.add_argument("--back", default="data/mcr_stopover_back.json", help="Датасет остановки ОБРАТНО, беспосадочные плечи (или '' чтобы пропустить)")
    p.add_argument("--there-indirect", default="data/mcr_stopover_there_indirect.json",
                   help="Доп. датасет остановки ТУДА с пересадками в плечах (объединяется с --there)")
    p.add_argument("--back-indirect", default="data/mcr_stopover_back_indirect.json",
                   help="Доп. датасет остановки ОБРАТНО с пересадками в плечах (объединяется с --back)")

    p.add_argument("--min-stay", type=int, default=7, help="Минимум дней в пункте назначения")
    p.add_argument("--max-stay", type=int, default=21, help="Максимум дней в пункте назначения")
    p.add_argument("--stopover-days", type=int, nargs=2, default=[2, 6], metavar=("MIN", "MAX"),
                   help="Диапазон длительности остановки в промежуточном городе")
    p.add_argument("--arrive-by", default=None, help="Прибыть в пункт назначения не позже (YYYY-MM-DD)")

    p.add_argument("--event-name", default="My Chemical Romance — The Black Parade 2026")
    p.add_argument("--event-date", default="2026-11-07", help="Дата события (YYYY-MM-DD) или '' если события нет")
    p.add_argument("--event-venue", default="Paradise City, Инчхон")

    p.add_argument("--no-airport-transfers", action="store_true",
                   help="Отключить переходы между близкими аэропортами в городе остановки (по умолчанию включены)")
    p.add_argument("--airport-distance", type=float, default=100,
                   help="Макс. расстояние между аэропортами для перехода, км (по умолчанию 100)")
    p.add_argument("--cross-country-transfers", action="store_true",
                   help="Разрешить переходы между аэропортами разных стран (по умолчанию только внутри страны)")
    p.add_argument("--exclude", nargs="*", default=None,
                   help="IATA коды, которые НЕ могут быть городом остановки (по умолчанию: город вылета и хаб + типовые)")
    p.add_argument("--currency", default="RUB")
    p.add_argument("--title", default=None, help="Заголовок сайта (по умолчанию собирается автоматически)")
    p.add_argument("--per-city", type=int, default=1, help="Сколько дешёвых вариантов оставлять на каждую пару (город остановки, длительность)")
    p.add_argument("--top-direct", type=int, default=12, help="Сколько вариантов без остановки оставлять")
    p.add_argument("--network", default="data/airport_network.json")
    p.add_argument("--out", default="web", help="Каталог для data.json / data.js")
    return p.parse_args()


DEFAULT_EXCLUDE = {"MOW", "SVO", "DME", "VKO", "ZIA", "LED",
                   "ICN", "SEL", "GMP", "PUS", "CJU"}


@dataclass
class Config:
    """Параметры сборки. Дефолты совпадают с CLI (событие MCR, MOW→ICN)."""
    origin: str = "MOW"
    origin_name: str = "Москва (MOW)"
    hub: str = "ICN"
    hub_name: str = "Сеул / Инчхон (ICN)"
    region_label: Optional[str] = None
    region_flag: Optional[str] = None
    min_stay: int = 7
    max_stay: int = 21
    stopover_days: tuple = (2, 6)
    arrive_by: Optional[str] = None
    event_name: str = "My Chemical Romance — The Black Parade 2026"
    event_date: str = "2026-11-07"
    event_venue: str = "Paradise City, Инчхон"
    no_airport_transfers: bool = False
    airport_distance: float = 100
    cross_country_transfers: bool = False
    exclude: Optional[set] = None
    currency: str = "RUB"
    title: Optional[str] = None
    network_path: str = "data/airport_network.json"
    # пути к датасетам-плечам (raw-выгрузки коллектора)
    plain: str = "data/mcr_plain.json"
    there: str = "data/mcr_stopover_there.json"
    back: str = "data/mcr_stopover_back.json"
    there_indirect: str = "data/mcr_stopover_there_indirect.json"
    back_indirect: str = "data/mcr_stopover_back_indirect.json"


def enrich(o):
    """Обогащает односторонее плечо агрегатами (перенос из main без изменений)."""
    segs = o["segments"]
    trs = [int(s.get("transfers") or 0) for s in segs]
    o["total_transfers"] = sum(trs)
    o["max_transfers"] = max(trs, default=0)
    o["tr"] = o["max_transfers"]
    o["travel_minutes"] = sum(int(s.get("duration") or 0) for s in segs)
    o["travel"] = o["travel_minutes"]
    o["stop"] = bool(o.get("has_stopover"))
    so = o.get("stopover")
    o["sdays"] = so["days"] if so else 0
    o["city"] = so["city"] if so else None
    o["dep_date"] = date_only(segs[0]["departure_at"])
    # «касание» хаба: прилёт (туда) или вылет (обратно) — по нему считаются дни в регионе
    hub_iso = segs[-1]["arrival_at"] if o["direction"] == "there" else segs[0]["departure_at"]
    o["hub_date"] = date_only(hub_iso)
    o["hub_ord"] = datetime.strptime(o["hub_date"], "%Y-%m-%d").toordinal()


def dedup(opts):
    seen, out = set(), []
    for o in opts:
        sig = tuple((s["origin"], s["destination"], s["departure_at"], s["price"]) for s in o["segments"])
        if sig in seen:
            continue
        seen.add(sig)
        out.append(o)
    return out


def _make_internal_cfg(cfg: Config, network):
    """Собирает служебный конфиг, который читают методы Builder (те же поля, что раньше в main)."""
    arrive_by = cfg.arrive_by
    if arrive_by is None and cfg.event_date:
        arrive_by = (datetime.strptime(cfg.event_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

    exclude = set(cfg.exclude) if cfg.exclude is not None else set(DEFAULT_EXCLUDE)
    exclude |= {cfg.origin, cfg.hub}

    class Cfg:
        pass

    ic = Cfg()
    ic.exclude = exclude
    ic.min_stay, ic.max_stay = cfg.min_stay, cfg.max_stay
    ic.stop_min, ic.stop_max = cfg.stopover_days
    ic.arrive_by = arrive_by
    ic.event_date = cfg.event_date or None
    ic.airport_network = None if cfg.no_airport_transfers else network
    ic.airport_distance = cfg.airport_distance
    ic.same_country = not cfg.cross_country_transfers
    ic.return_from = None
    if arrive_by:
        ic.return_from = (datetime.strptime(arrive_by, "%Y-%m-%d")
                          + timedelta(days=cfg.min_stay)).strftime("%Y-%m-%d")
    return ic


def build_payload(cfg: Config, network, plain: dict,
                  there_merged: Optional[dict], back_merged: Optional[dict]) -> Dict[str, Any]:
    """Чистая сборка контракта {meta, trips} из загруженных датасетов.

    Тот же результат, что писал main() в web/data.json. Используется и CLI, и API.
    """
    city_info = make_city_lookup(network)
    hub_ci = city_info(cfg.hub)
    region_label = cfg.region_label or hub_ci["city"]
    region_flag = cfg.region_flag or hub_ci["flag"]

    ic = _make_internal_cfg(cfg, network)
    b = Builder(ic, city_info)

    plain_out = plain["leg1_flights"]   # ORIGIN -> HUB
    plain_ret = plain["leg2_flights"]   # HUB -> ORIGIN

    # --- Собираем только ОДНОСТОРОННИЕ плечи. Round-trip собирается на лету в браузере ---
    there = b.oneway_there_plain(plain_out)
    if there_merged:
        there += b.oneway_there_stop(there_merged)
    back = b.oneway_back_plain(plain_ret)
    if back_merged:
        back += b.oneway_back_stop(back_merged)

    for o in there + back:
        enrich(o)
    there = keep_variety(dedup(there))
    back = keep_variety(dedup(back))
    there.sort(key=lambda x: x["total_price"])
    back.sort(key=lambda x: x["total_price"])
    trips = there + back
    for i, o in enumerate(trips, 1):
        o["id"] = i

    there_cities = sorted({o["city"] for o in there if o["city"]})
    back_cities = sorted({o["city"] for o in back if o["city"]})

    def prange(opts):
        ps = [o["total_price"] for o in opts]
        return [min(ps, default=0), max(ps, default=0)]

    max_korea = cfg.max_stay
    if there and back:
        max_korea = max(o["hub_ord"] for o in back) - min(o["hub_ord"] for o in there)

    title = cfg.title
    if not title:
        title = f"{cfg.origin_name.split(' (')[0]} → {region_label} ⇄ обратно"
        if cfg.event_name:
            title += f" · {cfg.event_name.split(' —')[0]}"

    event = None
    if cfg.event_date:
        event = {"name": cfg.event_name, "date": cfg.event_date, "venue": cfg.event_venue}

    return {
        "meta": {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "title": title,
            "currency": cfg.currency,
            "origin": cfg.origin_name, "origin_code": cfg.origin,
            "destination": cfg.hub_name, "destination_code": cfg.hub,
            "region_label": region_label, "region_flag": region_flag,
            "event": event,
            "min_stay": cfg.min_stay,
            "max_korea_days": max(cfg.min_stay, max_korea),
            "stopover_days_range": list(cfg.stopover_days),
            "max_stop_days": cfg.stopover_days[1],
            "counts": {"there": len(there), "back": len(back), "total": len(trips)},
            "there_cities": there_cities,
            "back_cities": back_cities,
            "there_price_range": prange(there),
            "back_price_range": prange(back),
            "max_leg_travel_minutes": max((o["travel"] for o in trips), default=0),
            "dep_there_range": [
                min((o["dep_date"] for o in there), default=""),
                max((o["dep_date"] for o in there), default="")],
            "dep_back_range": [
                min((o["dep_date"] for o in back), default=""),
                max((o["dep_date"] for o in back), default="")],
        },
        "trips": trips,
    }


def load_datasets(cfg: Config):
    """Загружает сеть аэропортов и датасеты-плечи по путям из конфига."""
    network = agg.load_airport_network(cfg.network_path)
    plain = load(cfg.plain)
    there_merged = load_merged([cfg.there, cfg.there_indirect]) if cfg.there else None
    back_merged = load_merged([cfg.back, cfg.back_indirect]) if cfg.back else None
    return network, plain, there_merged, back_merged


def build_from_config(cfg: Config) -> Dict[str, Any]:
    """Удобная обёртка: загрузить датасеты по путям конфига и собрать контракт."""
    network, plain, there_merged, back_merged = load_datasets(cfg)
    return build_payload(cfg, network, plain, there_merged, back_merged)


def _config_from_args(a) -> Config:
    return Config(
        origin=a.origin, origin_name=a.origin_name, hub=a.hub, hub_name=a.hub_name,
        region_label=a.region_label, region_flag=a.region_flag,
        min_stay=a.min_stay, max_stay=a.max_stay, stopover_days=tuple(a.stopover_days),
        arrive_by=a.arrive_by, event_name=a.event_name, event_date=a.event_date,
        event_venue=a.event_venue, no_airport_transfers=a.no_airport_transfers,
        airport_distance=a.airport_distance, cross_country_transfers=a.cross_country_transfers,
        exclude=set(a.exclude) if a.exclude is not None else None,
        currency=a.currency, title=a.title, network_path=a.network,
        plain=a.plain, there=a.there, back=a.back,
        there_indirect=a.there_indirect, back_indirect=a.back_indirect,
    )


def main():
    a = parse_args()
    cfg = _config_from_args(a)
    out = build_from_config(cfg)

    out_dir = Path(a.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / "data.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    with open(out_dir / "data.js", "w", encoding="utf-8") as f:
        f.write("window.TRIP_DATA = ")
        json.dump(out, f, ensure_ascii=False)
        f.write(";\n")

    c = out["meta"]["counts"]
    print(f"✓ {a.out}/data.json: {c['total']} односторонних плеч "
          f"(туда={c['there']}, обратно={c['back']}); round-trip собирается на лету")
    print(f"  Городов: туда {len(out['meta']['there_cities'])} / обратно {len(out['meta']['back_cities'])} | "
          f"цена плеча туда {out['meta']['there_price_range']}, обратно {out['meta']['back_price_range']}")


if __name__ == "__main__":
    main()
