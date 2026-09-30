"""Рейсы джобы колонками (FlightCols): все плечи одной таблицей, поля стыковки уже
числами. Из неё строят свои структуры перебор A* (core.planner) и наборы городов
(core.overview) — векторно, без обхода 135 тыс. словарей рейсов при каждой смене
фильтра. Для сегмента результата (segments.Builder.make_segment) рейс лежит
JSON-колонкой seg — только поля сегмента (SEG_KEYS; ссылка — лишь там, где она
нужна) — и разбирается только для рейсов из результата (flight(i)).

Хранение — Parquet-файл на джобу (storage.hot.put_plan_flights): строковые колонки
читаются словарём (уникальных значений — сотни–десятки тысяч на 135 тыс. строк),
seg — лениво при первом flight(i), сжатие lz4 (читается в ~3 раза быстрее zstd).
Файлы прежнего формата с полным рейсом в колонке raw читаются так же.

Семантика полей — ровно как у словарных помощников планировщика:
  city/airport концов — planner._side_codes (город или search_*, аэропорт; UPPER),
  arr_iso — segments.arrival_of (у рейса без вылета — None),
  *_day — дата локального времени (planner.date_only), *_ts/*_ord — overview,
  price — planner._price_of, transfers/duration — int(... or 0) как в LegFilter,
  pts_min/layover — входы фильтра ожидания на пересадке (NaN — неизвестно)."""
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from core.dates import parse_datetime
from core.segments import arrival_of

_EPOCH = datetime(1970, 1, 1)
_NAN = float("nan")

_STR = ["orig_city", "orig_airport", "dest", "dest_airport", "dep_iso", "arr_iso"]
_NUM = {"leg": np.int16, "price": np.float64, "transfers": np.int32, "duration": np.int64,
        "hidden": np.bool_, "bag_incl": np.bool_, "pts_min": np.float64, "layover": np.float64,
        "dep_ts": np.float64, "arr_ts": np.float64, "dep_ord": np.int64, "arr_ord": np.int64}
# Поля рейса, которые читает segments.Builder.make_segment (и arrival_of). legs/chain
# и прочее сегменту не нужны — в файл не пишем. link нужен только hidden-city (ссылка
# на реальный билет) и рейсам без transfer_points (пересадки — из токена ссылки).
SEG_KEYS = ["origin", "destination", "search_origin", "search_destination", "origin_airport",
            "destination_airport", "departure_at", "arrival_at", "duration", "duration_to", "transfers",
            "transfer_points", "airline", "flight_number", "price", "value", "baggage", "hidden_city"]


def segment_source(f: Dict[str, Any]) -> Dict[str, Any]:
    """Рейс, урезанный до полей сегмента: make_segment даёт тот же сегмент."""
    out = {k: f[k] for k in SEG_KEYS if k in f}
    if f.get("link") and (f.get("hidden_city") or f.get("transfer_points") is None):
        out["link"] = f["link"]
    return out


_ARROW = {np.int16: pa.int16(), np.float64: pa.float64(), np.int32: pa.int32(), np.int64: pa.int64(),
          np.bool_: pa.bool_()}


def _upper(v: Any) -> str:
    return v.upper() if v else ""


def _pts_min(f: Dict[str, Any]) -> float:
    points = f.get("transfer_points") or []
    minutes = [p.get("minutes") for p in points if isinstance(p, dict)]
    if minutes and all(m is not None for m in minutes):
        return float(min(minutes))
    return _NAN


class FlightCols:
    """Колонки: списки строк (_STR) и numpy-массивы (_NUM) одной длины n."""

    def __init__(self, cols: Dict[str, Any], raw=None, path: Optional[str] = None):
        for k in _STR:
            setattr(self, k, cols[k])
        for k, dt in _NUM.items():
            setattr(self, k, np.asarray(cols[k], dtype=dt))
        self.n = len(self.leg)
        # Поля, по которым A* ходит поштучно, — обычными списками (быстрее numpy-скаляров).
        self.price_list: List[float] = self.price.tolist()
        self.dep_day = [s[:10] if s else None for s in self.dep_iso]
        self.arr_day = [s[:10] if s else None for s in self.arr_iso]
        self._raw = raw          # список словарей (из сбора) или pa-массив JSON-строк
        self._path = path        # Parquet, откуда raw дочитать лениво
        self._parsed: Dict[int, Dict[str, Any]] = {}

    def __len__(self) -> int:
        return self.n

    # ------------------------------ построение -------------------------------

    @classmethod
    def from_collected(cls, collected: Dict[int, List[Dict[str, Any]]]) -> "FlightCols":
        cols: Dict[str, list] = {k: [] for k in _STR + list(_NUM)}
        raw: List[Dict[str, Any]] = []
        for leg in sorted(collected):
            for f in collected[leg]:
                dep = f.get("departure_at")
                arr = arrival_of(f) if dep else None
                dep_dt = parse_datetime(dep) if dep else None
                arr_dt = parse_datetime(arr) if arr else None
                price = f.get("price")
                bag = f.get("baggage") or {}
                lay = f.get("layover_minutes")
                cols["leg"].append(leg)
                cols["orig_city"].append(_upper(f.get("origin") or f.get("search_origin")))
                cols["orig_airport"].append(_upper(f.get("origin_airport")))
                cols["dest"].append(_upper(f.get("destination") or f.get("search_destination")))
                cols["dest_airport"].append(_upper(f.get("destination_airport")))
                cols["dep_iso"].append(dep)
                cols["arr_iso"].append(arr)
                cols["price"].append(price if price is not None else f.get("value", 0))
                cols["transfers"].append(int(f.get("transfers") or 0))
                cols["duration"].append(int(f.get("duration") or 0))
                cols["hidden"].append(bool(f.get("hidden_city")))
                cols["bag_incl"].append(bool(bag.get("included")))
                cols["pts_min"].append(_pts_min(f))
                cols["layover"].append(float(lay) if lay is not None else _NAN)
                cols["dep_ts"].append((dep_dt - _EPOCH).total_seconds() if dep_dt else _NAN)
                cols["arr_ts"].append((arr_dt - _EPOCH).total_seconds() if arr_dt else _NAN)
                cols["dep_ord"].append(dep_dt.date().toordinal() if dep_dt else -1)
                cols["arr_ord"].append(arr_dt.date().toordinal() if arr_dt else -1)
                raw.append(f)
        return cls(cols, raw=raw)

    # ------------------------------ хранение ---------------------------------

    def to_table(self) -> pa.Table:
        arrays = {k: pa.array(getattr(self, k), type=pa.string()) for k in _STR}
        arrays.update({k: pa.array(getattr(self, k), type=_ARROW[dt]) for k, dt in _NUM.items()})
        arrays["seg"] = pa.array([json.dumps(segment_source(self.flight(i)), ensure_ascii=False)
                                  for i in range(self.n)], type=pa.string())
        return pa.table(arrays)

    def write(self, path: str) -> None:
        table = self.to_table()
        pq.write_table(table, path, compression={c: ("lz4" if c == "seg" else "zstd") for c in table.column_names})

    @classmethod
    def read(cls, path: str) -> "FlightCols":
        table = pq.read_table(path, columns=_STR + list(_NUM), read_dictionary=_STR)
        cols: Dict[str, Any] = {k: _dict_to_list(table.column(k)) for k in _STR}
        cols.update({k: table.column(k).to_numpy() for k in _NUM})
        return cls(cols, path=path)

    # ------------------------------ рейсы ------------------------------------

    def flight(self, i: int) -> Dict[str, Any]:
        """Полный рейс строки i (словарь, как в сборе) — для сегментов результата."""
        if self._raw is None:
            names = pq.ParquetFile(self._path).schema_arrow.names
            col = "seg" if "seg" in names else "raw"      # raw — файлы прежнего формата
            self._raw = pq.read_table(self._path, columns=[col]).column(col).combine_chunks()
        if isinstance(self._raw, list):
            return self._raw[i]
        got = self._parsed.get(i)
        if got is None:
            got = self._parsed[i] = json.loads(self._raw[i].as_py())
        return got

    def rows(self, leg: int) -> np.ndarray:
        return np.flatnonzero(self.leg == leg)

    def origin_codes(self, r: int) -> tuple:
        return self.orig_city[r], self.orig_airport[r]

    def dest_in(self, r: int, allow: Optional[set]) -> bool:
        """Город или аэропорт прилёта строки r в allow (None — любой)."""
        if allow is None:
            return True
        return (self.dest[r] in allow) or (self.dest_airport[r] in allow)

    def by_origin(self, rows: np.ndarray) -> Dict[str, np.ndarray]:
        """Строки плеча по коду вылета — и городу, и аэропорту (как planner._index_leg)."""
        out: Dict[str, List[int]] = {}
        oc, oa = self.orig_city, self.orig_airport
        for r in rows.tolist():
            c, a = oc[r], oa[r]
            if c:
                out.setdefault(c, []).append(r)
            if a and a != c:
                out.setdefault(a, []).append(r)
        return {k: np.array(v, dtype=np.int64) for k, v in out.items()}


def _dict_to_list(column: pa.ChunkedArray) -> List[Optional[str]]:
    """Словарная строковая колонка → список строк: объект строки на каждое
    уникальное значение, а не на каждую из 135 тыс. строк (to_pylist — ~0.9 с на проде)."""
    out: List[Optional[str]] = []
    for chunk in column.chunks:
        vocab = np.array(chunk.dictionary.to_pylist() + [None], dtype=object)
        idx = chunk.indices.fill_null(len(vocab) - 1).to_numpy()
        out.extend(vocab[idx].tolist())
    return out


def as_cols(collected) -> FlightCols:
    """FlightCols как есть, словарь {плечо: [рейсы]} — в колонки."""
    return collected if isinstance(collected, FlightCols) else FlightCols.from_collected(collected)
