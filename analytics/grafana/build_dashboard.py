"""Генерирует analytics/grafana/dashboards/flights-ops.json — дашборд «Flights · Коллектор»
для Grafana (источник данных ClickHouse `flights-clickhouse`, база `flights`).

    python analytics/grafana/build_dashboard.py

JSON держим в репозитории (его заливает analytics/setup.sh), генератор — чтобы
панели правились в одном месте без ручного редактирования JSON."""
import json
from pathlib import Path

DS = {"type": "grafana-clickhouse-datasource", "uid": "flights-clickhouse"}
OUT = Path(__file__).parent / "dashboards" / "flights-ops.json"

_id = [0]


def _next_id() -> int:
    _id[0] += 1
    return _id[0]


def target(sql: str, kind: str = "timeseries", ref: str = "A") -> dict:
    return {"datasource": DS, "editorType": "sql", "rawSql": sql, "queryType": kind, "refId": ref}


def panel(title: str, sql: str, x: int, y: int, w: int = 12, h: int = 8, *, ptype: str = "timeseries",
          unit: str = None, stack: bool = False, kind: str = "timeseries", description: str = None,
          extra: dict = None) -> dict:
    custom = {"fillOpacity": 12, "lineWidth": 2, "showPoints": "never"}
    if stack:
        custom["stacking"] = {"mode": "normal", "group": "A"}
    defaults = {"custom": custom}
    if unit:
        defaults["unit"] = unit
    p = {"id": _next_id(), "title": title, "type": ptype, "gridPos": {"x": x, "y": y, "w": w, "h": h},
         "datasource": DS, "targets": [target(sql, kind)],
         "fieldConfig": {"defaults": defaults, "overrides": []}, "options": {}}
    if description:
        p["description"] = description
    if extra:
        p.update(extra)
    return p


def row(title: str, y: int) -> dict:
    return {"id": _next_id(), "type": "row", "title": title, "collapsed": False,
            "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []}


def stat(title: str, sql: str, x: int, y: int, w: int = 4, h: int = 4, unit: str = None,
         thresholds: list = None) -> dict:
    defaults = {"unit": unit} if unit else {}
    defaults["thresholds"] = {"mode": "absolute", "steps": thresholds or [{"color": "blue", "value": None}]}
    return {"id": _next_id(), "title": title, "type": "stat", "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "datasource": DS, "targets": [target(sql, "table")],
            "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "colorMode": "value", "graphMode": "none", "textMode": "value"}}


OPS = "flights.ops_metrics"
LAST = f"SELECT * FROM {OPS} ORDER BY ts DESC LIMIT 1"


def build() -> dict:
    y = 0
    panels = [row("Данные и свежесть", y)]
    y += 1
    panels += [
        stat("Серий в озере", f"SELECT series_total FROM ({LAST})", 0, y),
        stat("Городов", f"SELECT cities FROM ({LAST})", 4, y),
        stat("Проход сборщика", f"SELECT crawler_pass_progress FROM ({LAST})", 8, y, unit="percentunit",
             thresholds=[{"color": "red", "value": None}, {"color": "orange", "value": 0.5},
                         {"color": "green", "value": 0.95}]),
        stat("Устарело пар", f"SELECT crawler_stale FROM ({LAST})", 12, y,
             thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 500},
                         {"color": "red", "value": 5000}]),
        stat("Медианный возраст, ч", f"SELECT series_age_p50_h FROM ({LAST})", 16, y, unit="h",
             thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 48},
                         {"color": "red", "value": 120}]),
        stat("Озеро, ГБ", f"SELECT lake_bytes/1073741824 FROM ({LAST})", 20, y, unit="decgbytes",
             thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 150},
                         {"color": "red", "value": 180}]),
    ]
    y += 4
    panels += [
        panel("Покрытие сборщика: пары «город × день»",
              f"SELECT ts AS time, crawler_fresh AS \"свежие\", crawler_stale AS \"устарели\", "
              f"crawler_missing AS \"нет данных\", crawler_errors AS \"ошибки источника\" "
              f"FROM {OPS} WHERE $__timeFilter(ts) ORDER BY ts", 0, y, stack=True,
              description="Цель свежести: до 14 дней вперёд — 24 ч, до 60 — 72 ч, дальше — неделя."),
        panel("Возраст серий в индексе, ч",
              f"SELECT ts AS time, series_age_p50_h AS \"медиана\", series_age_max_h AS \"максимум\" "
              f"FROM {OPS} WHERE $__timeFilter(ts) ORDER BY ts", 12, y, unit="h"),
    ]
    y += 8
    panels += [
        panel("Свежесть по дальности даты вылета (недели от сегодня)",
              "SELECT concat('нед. ', toString(intDiv(dateDiff('day', today(), day), 7))) AS week, "
              "round(avg(age_h), 1) AS \"средний возраст, ч\", round(max(age_h), 1) AS \"максимум, ч\" "
              "FROM flights.coverage WHERE day >= today() AND NOT error "
              "GROUP BY intDiv(dateDiff('day', today(), day), 7) ORDER BY intDiv(dateDiff('day', today(), day), 7)",
              0, y, ptype="barchart", kind="table", unit="h",
              extra={"options": {"orientation": "vertical", "xField": "week", "showValue": "never"}}),
        panel("Самые старые данные по городам (топ 20)",
              "SELECT origin AS \"город\", round(max(age_h), 1) AS \"макс. возраст, ч\", count() AS \"дней\", "
              "sum(tickets) AS \"билетов\" FROM flights.coverage WHERE day >= today() AND NOT error "
              "GROUP BY origin ORDER BY max(age_h) DESC LIMIT 20", 12, y, ptype="table", kind="table"),
    ]
    y += 8
    panels.append(row("Ручка GraphQL и очередь", y))
    y += 1
    panels += [
        panel("Страниц в минуту по клиентам",
              f"SELECT ts AS time, pages_app_1m AS \"приложение\", pages_crawl_1m AS \"фон\" "
              f"FROM {OPS} WHERE $__timeFilter(ts) ORDER BY ts", 0, y, stack=True,
              description="Лимит источника — 60 запросов в минуту на токен."),
        panel("Очередь коллектора",
              f"SELECT ts AS time, queued_app AS \"приложение\", queued_crawl AS \"фон\" "
              f"FROM {OPS} WHERE $__timeFilter(ts) ORDER BY ts", 12, y),
    ]
    y += 8
    panels += [
        panel("429 и ошибки в минуту",
              f"SELECT ts AS time, http_429_1m AS \"429\", source_errors_1m AS \"ошибки источника\", "
              f"network_errors_1m AS \"сеть\", lake_write_errors_1m AS \"запись в озеро\" "
              f"FROM {OPS} WHERE $__timeFilter(ts) ORDER BY ts", 0, y),
        panel("Приложение: серии, кэш, задержка",
              f"SELECT ts AS time, series_app_1m AS \"серий из источника\", cache_hit_app_1m AS \"из кэша\", "
              f"app_latency_avg_s AS \"задержка, с\" FROM {OPS} WHERE $__timeFilter(ts) ORDER BY ts", 12, y,
              extra={"fieldConfig": {"defaults": {"custom": {"fillOpacity": 12, "lineWidth": 2, "showPoints": "never"}},
                                     "overrides": [{"matcher": {"id": "byName", "options": "задержка, с"},
                                                    "properties": [{"id": "custom.axisPlacement", "value": "right"},
                                                                   {"id": "unit", "value": "s"}]}]}}),
    ]
    y += 8
    panels.append(row("Машина и хранилище", y))
    y += 1
    panels += [
        panel("Нагрузка VM",
              f"SELECT ts AS time, cpu_pct AS CPU, mem_pct AS RAM, disk_used_pct AS \"диск\" "
              f"FROM {OPS} WHERE $__timeFilter(ts) ORDER BY ts", 0, y, unit="percent"),
        panel("Озеро и бакет, ГБ",
              f"SELECT ts AS time, lake_bytes/1073741824 AS \"озеро (tickets)\", "
              f"bucket_bytes/1073741824 AS \"весь бакет\", lake_max_bytes/1073741824 AS \"порог ретеншна\" "
              f"FROM {OPS} WHERE $__timeFilter(ts) ORDER BY ts", 12, y, unit="decgbytes",
              description="Жёсткий потолок бакета — 200 ГБ (terraform), ретеншн удаляет старые файлы выше порога."),
    ]
    y += 8
    panels += [
        panel("Серий и билетов в минуту",
              f"SELECT ts AS time, series_app_1m + series_crawl_1m AS \"серий\", tickets_1m AS \"билетов\" "
              f"FROM {OPS} WHERE $__timeFilter(ts) ORDER BY ts", 0, y,
              extra={"fieldConfig": {"defaults": {"custom": {"fillOpacity": 12, "lineWidth": 2, "showPoints": "never"}},
                                     "overrides": [{"matcher": {"id": "byName", "options": "билетов"},
                                                    "properties": [{"id": "custom.axisPlacement", "value": "right"}]}]}}),
        panel("Файлы озера и удаления ретеншном",
              f"SELECT ts AS time, lake_files AS \"файлов\", files_deleted_1m AS \"удалено за минуту\" "
              f"FROM {OPS} WHERE $__timeFilter(ts) ORDER BY ts", 12, y,
              extra={"fieldConfig": {"defaults": {"custom": {"fillOpacity": 12, "lineWidth": 2, "showPoints": "never"}},
                                     "overrides": [{"matcher": {"id": "byName", "options": "удалено за минуту"},
                                                    "properties": [{"id": "custom.axisPlacement", "value": "right"}]}]}}),
    ]
    return {
        "uid": "flights-ops", "title": "Flights · Коллектор", "tags": ["flights"], "timezone": "browser",
        "schemaVersion": 39, "refresh": "1m", "time": {"from": "now-24h", "to": "now"},
        "editable": True, "graphTooltip": 1, "panels": panels,
    }


if __name__ == "__main__":
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(build(), ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"{OUT}: {len(build()['panels'])} панелей")
