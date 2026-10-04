"""Генерирует analytics/grafana/dashboards/flights-ops.json — дашборд «Flights · Коллектор»
для Grafana (источник данных ClickHouse `flights-clickhouse`, база `flights`).

    python analytics/grafana/build_dashboard.py

JSON держим в репозитории (его провижнит observatory с main), генератор — чтобы
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


def stat(title: str, field: str, x: int, y: int, w: int = 4, h: int = 4, unit: str = None,
         thresholds: list = None, description: str = None, decimals: int = None) -> dict:
    """Последнее значение поля общего запроса OPS (без своего похода в S3)."""
    defaults = {"unit": unit} if unit else {}
    if decimals is not None:
        defaults["decimals"] = decimals
    defaults["thresholds"] = {"mode": "absolute", "steps": thresholds or [{"color": "blue", "value": None}]}
    p = {"id": _next_id(), "title": title, "type": "stat", "gridPos": {"x": x, "y": y, "w": w, "h": h},
         "datasource": SHARED, "targets": [shared_target()], "transformations": [only(field)],
         "fieldConfig": {"defaults": defaults, "overrides": []},
         "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                     "colorMode": "value", "graphMode": "none", "textMode": "value"}}
    if description:
        p["description"] = description
    return p


OPS = "flights.ops_metrics"
GIB = 1073741824

# Один запрос к ops_metrics на весь дашборд: остальные панели берут его результат через
# источник «-- Dashboard --» и оставляют свои поля (filterFieldsByName). Раньше каждая
# панель и каждая плитка сама читала все файлы ops_metrics из S3 — загрузка 15–20 с.
# Имена полей уникальны на весь дашборд: по ним панели и выбирают свои ряды.
# Доли пар горизонта по возрасту — считаем в SQL: сырые счётчики в процентном стеке Grafana
# выглядели верно на оси, но в подсказке показывались как проценты (сотни тысяч %).
AGE_SHARES = [("age_le_6h", "до 6 ч"), ("age_6_24h", "6–24 ч"), ("age_24_48h", "1–2 суток"),
              ("age_gt_48h", "старше 2 суток")]
AGE_TOTAL = " + ".join(col for col, _ in AGE_SHARES)
OPS_FIELDS = [
    ("crawler_fresh", "обработаны в этом проходе"), ("crawler_pairs", "всего пар"),
    ("crawler_stale", "ждут (данные с прошлого прохода)"), ("crawler_missing", "нет данных"),
    ("crawler_errors", "ошибка источника"),
    ("crawler_pass", "номер прохода"),
    ("crawler_pass_progress", "по парам"), ("crawler_pass_progress_pages", "по объёму работы"),
    ("crawler_sweep_offset", "дошёл до дня вылета (дней от сегодня)"), ("cities", "городов"),
    ("series_age_p50_h", "медиана"),
] + [(f"{col} / nullIf({AGE_TOTAL}, 0)", name) for col, name in AGE_SHARES] + [
    ("pages_app_1m + pages_crawl_1m", "всего запросов"), ("pages_app_1m", "запросы приложения"),
    ("pages_crawl_1m", "запросы фона"), ("rate_per_minute", "лимит в минуту"),
    ("queued_crawl", "фон, окон дат"), ("queued_app", "приложение, заданий"),
    ("crawler_queued_pages_est", "фон, оценка страниц"),
    ("http_429_1m", "429"), ("source_errors_1m", "ошибка в ответе GraphQL"),
    ("network_errors_1m", "сеть / HTTP 5xx"), ("lake_write_errors_1m", "запись в озеро"),
    ("series_app_1m + series_crawl_1m", "выборок"), ("tickets_1m", "билетов"),
    ("cpu_pct", "CPU"), ("mem_pct", "RAM"), ("disk_used_pct", "диск"),
    (f"lake_bytes / {GIB}", "озеро (tickets/)"), (f"bucket_bytes / {GIB}", "весь бакет"),
    (f"lake_max_bytes / {GIB}", "порог ретеншна"),
    ("lake_files", "файлов"), ("files_deleted_1m", "удалено за минуту"),
]
OPS_SQL = ("SELECT ts AS time, " + ", ".join(f'{expr} AS "{name}"' for expr, name in OPS_FIELDS)
           + f" FROM {OPS} WHERE $__timeFilter(ts) ORDER BY ts")
SHARED = {"type": "datasource", "uid": "-- Dashboard --"}
SOURCE_ID = 100  # id панели с общим запросом (покрытие сборщика)


def shared_target() -> dict:
    return {"datasource": SHARED, "panelId": SOURCE_ID, "refId": "A"}


def only(*names: str) -> dict:
    return {"id": "filterFieldsByName", "options": {"include": {"names": ["time", *names]}}}


def ops_panel(title: str, fields: list, x: int, y: int, w: int = 12, h: int = 8, *, unit: str = None,
              description: str = None, custom: dict = None, defaults: dict = None,
              overrides: list = None, source: bool = False) -> dict:
    """Временной ряд из общего запроса OPS: только поля fields. source=True — эта панель
    и выполняет общий запрос (остальные ссылаются на неё)."""
    c = {"fillOpacity": 0, "lineWidth": 2, "showPoints": "never", "axisSoftMin": 0}
    c.update(custom or {})
    d = {"custom": c}
    if unit:
        d["unit"] = unit
    d.update(defaults or {})
    p = {"id": SOURCE_ID if source else _next_id(), "title": title, "type": "timeseries",
         "gridPos": {"x": x, "y": y, "w": w, "h": h},
         "datasource": DS if source else SHARED,
         "targets": [target(OPS_SQL)] if source else [shared_target()],
         "transformations": [only(*fields)],
         "fieldConfig": {"defaults": d, "overrides": overrides or []},
         "options": {"tooltip": {"mode": "multi"},
                     "legend": {"showLegend": True, "displayMode": "list", "placement": "bottom"}}}
    if description:
        p["description"] = description
    return p


def by_name(name: str, *props: tuple) -> dict:
    return {"matcher": {"id": "byName", "options": name},
            "properties": [{"id": k, "value": v} for k, v in props]}


RIGHT = ("custom.axisPlacement", "right")
DASHED = ("custom.lineStyle", {"fill": "dash", "dash": [6, 4]})


def color(c: str) -> tuple:
    return ("color", {"mode": "fixed", "fixedColor": c})


def threshold_lines(*steps: tuple) -> dict:
    """Пунктирные горизонтали порогов (значение, цвет) — видно, где предел."""
    return {"thresholds": {"mode": "absolute", "steps": [{"color": "transparent", "value": None}]
                           + [{"color": col, "value": v} for v, col in steps]}}


# Дальность даты вылета от сегодня в днях (для снимка покрытия).
AGE_CAP_H = 48  # верх шкалы возраста по дням вылета: «нет данных или старше»
OFFSET = "dateDiff('day', today(), day)"
# Сетка «город × день вылета» горизонта сборщика (сегодня … +180): все города с хоть одной
# успешной серией. Пары без данных — в LEFT JOIN с join_use_nulls, а не выпадают.
GRID = ("FROM (SELECT o.origin AS origin, today() + n.number AS day "
        "FROM (SELECT DISTINCT origin FROM flights.coverage WHERE NOT error) AS o "
        "CROSS JOIN numbers(181) AS n) AS g")


def build() -> dict:
    y = 0
    panels = [row("Данные и свежесть", y)]
    y += 1
    panels += [
        stat("Проход №", "номер прохода", 0, y, decimals=0,
             description="Сборщик идёт по датам вылета от сегодня до +180 дней и начинает сначала; "
                         "номер растёт с каждым кругом."),
        stat("Все города дошли до дня, +дней", "дошёл до дня вылета (дней от сегодня)", 4, y, decimals=0,
             description="Первый день вылета (от сегодня), где хоть у одного города пара ещё не "
                         "обработана в текущем проходе. Почти весь проход стоит на 0: у каждого города "
                         "первое окно начинается с сегодня, а окна маленьких городов (месяц и больше "
                         "одним запросом) идут в очереди после сегодняшних дней крупных. Двигается "
                         "только в конце прохода, когда остаются крупные города (окно — день). Ход "
                         "прохода — «Обработано в проходе» и «Проход по календарю»."),
        stat("Проход: сделано работы", "по объёму работы", 8, y, unit="percentunit",
             description="Доля работы прохода в оценочных страницах (пара «город × день» стоит "
                         "среднюю плотность города / 400: у Москвы ~25 страниц, у маленького города "
                         "сотые доли). Ручка работает с постоянной скоростью, поэтому доля растёт "
                         "почти линейно и по ней видно, сколько осталось. Доля по парам — на графике "
                         "«Проход сборщика».",
             thresholds=[{"color": "blue", "value": None}]),
        stat("Городов", "городов", 12, y),
        stat("Возраст данных: медиана, ч", "медиана", 16, y, unit="h", decimals=1,
             description="Половина пар «город × день» на горизонте получена не раньше стольких часов назад.",
             thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 48},
                         {"color": "red", "value": 120}]),
        stat("Озеро, ГБ", "озеро (tickets/)", 20, y, unit="decgbytes",
             thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 150},
                         {"color": "red", "value": 180}]),
    ]
    y += 4
    panels += [
        ops_panel("Проход сборщика: пары «город × день вылета»",
                  ["всего пар", "обработаны в этом проходе", "ждут (данные с прошлого прохода)",
                   "нет данных", "ошибка источника", "по парам", "по объёму работы"], 0, y, source=True,
                  description="Каждая линия — отдельное число, без стека. За проход сборщик скачивает "
                              "каждую пару горизонта один раз (как идёт по дням — «Проход по календарю») "
                              "и начинает сначала: «обработаны» растут от нуля до «всего пар», «ждут» — "
                              "убывают. Правая ось (пунктир): пар без данных вообще и пар, где последняя "
                              "попытка — ошибка источника (тексты — в таблице «Ошибки источника»); "
                              "доля прохода по парам и по объёму работы (оценочные страницы). По "
                              "парам кривая S-образная: сначала сегодняшние дни крупных городов "
                              "(десятки страниц на пару), потом маленькие города окнами на полгода "
                              "(сотни пар за страницу), в конце — дальние дни крупных. По объёму "
                              "работы — почти прямая.",
                  custom={"axisLabel": "пар"},
                  overrides=[by_name("всего пар", color("#8f8f8f"), DASHED),
                             by_name("обработаны в этом проходе", color("green"), ("custom.fillOpacity", 10)),
                             by_name("ждут (данные с прошлого прохода)", color("#3d7fd9")),
                             by_name("нет данных", color("purple"), RIGHT, DASHED),
                             by_name("ошибка источника", color("red"), RIGHT, DASHED),
                             by_name("по парам", color("#8f8f8f"), RIGHT, ("unit", "percentunit"),
                                     ("max", 1), ("custom.axisLabel", "доля прохода")),
                             by_name("по объёму работы", color("orange"), RIGHT, ("unit", "percentunit"),
                                     ("max", 1), ("custom.lineWidth", 3))]),
        ops_panel("Возраст данных сборщика: доля пар горизонта",
                  ["до 6 ч", "6–24 ч", "1–2 суток", "старше 2 суток"], 12, y, unit="percentunit",
                  description="Какая доля пар «город × день вылета» на горизонте (сегодня … +180 дней) "
                              "получена не позже 6 ч назад, 6–24 ч, 1–2 суток и раньше. Стек до 100 %. "
                              "Проход короче суток — почти всё зелёное и голубое; оранжевое и красное "
                              "растут — сборщик не успевает (или стоит). Прошедшие дни вылета и пары "
                              "приложения (A→B) не учитываются.",
                  custom={"stacking": {"mode": "normal", "group": "A"}, "fillOpacity": 70,
                          "lineWidth": 0},
                  defaults={"min": 0, "max": 1},
                  overrides=[by_name("до 6 ч", color("green")), by_name("6–24 ч", color("#3d7fd9")),
                             by_name("1–2 суток", color("orange")),
                             by_name("старше 2 суток", color("red"))]),
    ]
    y += 8
    panels += [
        panel("Проход по календарю: города по дням вылета (сейчас)",
              f"WITH (SELECT max(pass_started) FROM flights.coverage) AS ps "
              f"SELECT formatDateTime(g.day, '%d.%m') AS \"день\", "
              f"countIf(cv.error = 0 AND cv.fetched_at >= ps) AS \"обработаны в этом проходе\", "
              f"countIf(cv.error = 0 AND NOT ifNull(cv.fetched_at >= ps, 0)) AS \"ждут (с прошлого прохода)\", "
              f"countIf(cv.error = 1) AS \"ошибка источника\", "
              f"countIf(cv.origin IS NULL) AS \"нет данных\" "
              f"{GRID} "
              f"LEFT JOIN (SELECT origin, day, fetched_at, error FROM flights.coverage WHERE day >= today()) AS cv "
              f"ON cv.origin = g.origin AND cv.day = g.day "
              f"GROUP BY g.day ORDER BY g.day SETTINGS join_use_nulls = 1",
              0, y, ptype="barchart", kind="table", stack=True,
              description="Снимок покрытия (раз в 10 мин): на каждый день вылета от сегодня до +180 — "
                          "сколько городов уже скачаны в текущем проходе (зелёное), сколько ждут "
                          "(данные с прошлого прохода), у скольких последняя попытка — ошибка "
                          "источника и у скольких данных нет вообще. Проход идёт не одной линией по "
                          "календарю: маленькие города берутся окнами на месяц и больше (их дни "
                          "зеленеют сразу по всему горизонту), крупные — по дню, от ближних дат к "
                          "дальним (зелёное наползает слева). Проход окончен — всё зелёное, новый "
                          "начинается — всё синее.",
              extra={"fieldConfig": {"defaults": {"custom": {"fillOpacity": 90, "lineWidth": 0,
                                                             "stacking": {"mode": "normal", "group": "A"}},
                                                  "decimals": 0},
                                     "overrides": [by_name("обработаны в этом проходе", color("green")),
                                                   by_name("ждут (с прошлого прохода)", color("#3d7fd9")),
                                                   by_name("ошибка источника", color("red")),
                                                   by_name("нет данных", color("purple"))]},
                     "options": {"orientation": "vertical", "xField": "день", "showValue": "never",
                                 "stacking": "normal", "xTickLabelSpacing": 100, "barWidth": 0.9,
                                 "tooltip": {"mode": "multi"},
                                 "legend": {"showLegend": True, "displayMode": "list",
                                            "placement": "bottom"}}}),
        panel("Когда получены данные: билетов по часу получения",
              f"SELECT toStartOfHour(fetched_at) AS time, "
              f"sumIf(tickets, {OFFSET} <= 14) AS \"вылет через 0–14 дн.\", "
              f"sumIf(tickets, {OFFSET} BETWEEN 15 AND 60) AS \"15–60 дн.\", "
              f"sumIf(tickets, {OFFSET} > 60) AS \"61–180 дн.\" "
              f"FROM flights.coverage WHERE day >= today() AND NOT error AND $__timeFilter(fetched_at) "
              f"GROUP BY time ORDER BY time", 12, y, stack=True,
              description="Сколько билетов из текущих данных скачано в каждый час (у каждой пары "
                          "«город × день» — последний раз, когда её скачали; цвет — дальность даты "
                          "вылета). Считаем билеты, а не пары: билеты пропорциональны работе ручки "
                          "(400 на запрос), а пары — нет: маленький город закрывает 181 пару одним "
                          "запросом, и час, когда шли маленькие города, был гигантским столбиком на "
                          "фоне остальных. Час без столбика — данные того часа уже перезаписаны "
                          "следующим проходом. Снимок покрытия — раз в 10 мин.",
              extra={"fieldConfig": {"defaults": {"custom": {
                  "drawStyle": "bars", "fillOpacity": 80, "lineWidth": 0, "showPoints": "never",
                  "stacking": {"mode": "normal", "group": "A"}}}, "overrides": []},
                  "options": {"tooltip": {"mode": "multi"}}}),
        panel("Сейчас обновляется: города за последний час",
              f"SELECT origin AS \"город\", count() AS \"пар обновлено\", "
              f"min({OFFSET}) AS \"дни вылета: с +\", max({OFFSET}) AS \"по +\", "
              f"sum(tickets) AS \"билетов\", "
              f"formatDateTime(max(fetched_at), '%H:%i', 'Europe/Moscow') AS \"последнее (МСК)\" "
              f"FROM flights.coverage WHERE day >= today() AND NOT error "
              f"AND fetched_at >= snapshot_at - INTERVAL 1 HOUR "
              f"GROUP BY origin ORDER BY max(fetched_at) DESC LIMIT 50",
              0, y + 8, ptype="table", kind="table",
              description="Какие города и какие дни вылета (от сегодня) сборщик скачал за час до "
                          "последнего снимка покрытия (раз в 10 мин). Проход идёт по календарю: у "
                          "крупных городов окно — день-два около курсора, у маленьких — месяц вперёд."),
    ]
    y += 8
    panels += [
        panel("Ошибки источника: что именно отвечает GraphQL",
              # GraphQL отдаёт errors JSON-массивом — показываем message первой ошибки
              "SELECT substring(multiIf(error_msg IS NULL, '(текст не сохранён — ошибка до 02.10.2026)', "
              "startsWith(error_msg, '[') AND JSONExtractString(error_msg, 1, 'message') != '', "
              "JSONExtractString(error_msg, 1, 'message'), error_msg), 1, 200) "
              "AS \"ошибка\", uniqExact(origin) AS \"городов\", count() AS \"пар\", "
              "arrayStringConcat(arraySlice(groupUniqArray(origin), 1, 8), ', ') AS \"города (до 8)\", "
              "formatDateTime(max(fetched_at), '%d.%m %H:%i', 'Europe/Moscow') AS \"последняя (МСК)\" "
              "FROM flights.coverage WHERE error AND day >= today() "
              "GROUP BY 1 ORDER BY count() DESC LIMIT 30",
              12, y, ptype="table", kind="table",
              description="Пары «город × день», где последняя попытка закончилась ошибкой: "
                          "текст ответа источника (обычно «city … not found» — код, которого "
                          "Aviasales не знает) или «network: …» — сеть и HTTP 5xx после повторов. "
                          "Повтор — в следующем проходе; город, у которого одни ошибки, — "
                          "в карантине (за проход проверяется одна дата)."),
    ]
    y += 8
    panels += [
        panel("Самые старые данные по городам (топ 20)",
              "SELECT origin AS \"город\", round(max(age_h), 1) AS \"макс. возраст, ч\", "
              "round(avg(age_h), 1) AS \"средний, ч\", count() AS \"дней\", "
              "sum(tickets) AS \"билетов\" FROM flights.coverage WHERE day >= today() AND NOT error "
              "GROUP BY origin ORDER BY max(age_h) DESC LIMIT 20", 0, y, w=24, h=7,
              ptype="table", kind="table",
              description="Возраст — на момент снимка покрытия (раз в 10 мин). Каждая пара "
                          "обновляется раз за проход, поэтому максимум ≈ длина прохода."),
    ]
    y += 8
    # Возраст по дню вылета, ч: перцентили по городам, у которых на этот день есть данные, —
    # p50, p95, максимум (худший город); прижато сверху к AGE_CAP_H. Города без данных или с
    # ошибкой источника на день — отдельным рядом «без данных» (в подсказке): раньше они
    # считались «самыми старыми», и максимум на каждом дне стоял в потолке (~10–60 таких
    # городов на день есть всегда). До этого был «запас свежести» 7 − возраст в сутках:
    # при проходе короче суток все столбики стояли на ~6.
    hidden = {"id": "custom.hideFrom", "value": {"viz": True, "legend": True, "tooltip": False}}
    shades = {"p50": "#9ec5f4", "p95": "#3d7fd9", "максимум": "#1a3f7a"}
    age = "least(round({agg}(cv.age_h), 1), " + str(AGE_CAP_H) + ")"
    panels += [
        panel("Возраст данных по дням вылета, ч: p50 / p95 / худший город",
              "SELECT formatDateTime(g.day, '%d.%m') AS \"день\", "
              f"{age.format(agg='quantile(0.5)')} AS \"p50\", "
              f"{age.format(agg='quantile(0.95)')} AS \"p95\", "
              f"{age.format(agg='max')} AS \"максимум\", "
              "countIf(cv.age_h IS NOT NULL) AS \"городов с данными\", "
              "countIf(cv.age_h IS NULL) AS \"без данных или с ошибкой\" "
              f"{GRID} "
              "LEFT JOIN (SELECT origin, day, age_h FROM flights.coverage WHERE day >= today() AND NOT error) AS cv "
              "ON cv.origin = g.origin AND cv.day = g.day "
              "GROUP BY g.day ORDER BY g.day SETTINGS join_use_nulls = 1",
              0, y, w=24, ptype="barchart", kind="table", unit="h",
              description="Сколько часов назад получены данные на каждый день вылета (снимок покрытия, "
                          "раз в 10 мин), по городам дня: p50 — половина городов свежее, p95 — все, "
                          "кроме 5 % отстающих, максимум — худший город (только города с данными на "
                          f"этот день; без данных или с ошибкой — число в подсказке). Ниже — свежее, "
                          f"шкала до {AGE_CAP_H} ч. "
                          "Пунктир — сутки. Пока идёт проход, дни, где крупные города уже обновлены, "
                          "ниже; проход окончен — всё ниже длины прохода. Города, у которых только "
                          "ошибки источника, не учитываются.",
              extra={"fieldConfig": {"defaults": {"unit": "h",
                                                  "custom": {"fillOpacity": 90, "lineWidth": 0,
                                                             "thresholdsStyle": {"mode": "dashed"}},
                                                  "min": 0, "max": AGE_CAP_H, "decimals": 1,
                                                  "thresholds": {"mode": "absolute", "steps": [
                                                      {"color": "transparent", "value": None},
                                                      {"color": "orange", "value": 24}]}},
                                     "overrides": [{"matcher": {"id": "byName", "options": name},
                                                    "properties": [{"id": "color", "value": {
                                                        "mode": "fixed", "fixedColor": c}}]}
                                                   for name, c in shades.items()]
                                                  + [{"matcher": {"id": "byName", "options": name},
                                                      "properties": [hidden, {"id": "decimals", "value": 0},
                                                                     {"id": "unit", "value": "none"}]}
                                                     for name in ("городов с данными",
                                                                  "без данных или с ошибкой")]},
                     "options": {"orientation": "vertical", "xField": "день", "showValue": "never",
                                 "xTickLabelSpacing": 100, "barWidth": 0.9, "groupWidth": 0.8,
                                 "tooltip": {"mode": "multi"},
                                 "legend": {"showLegend": True, "displayMode": "list",
                                            "placement": "bottom"}}}),
    ]
    y += 8
    panels.append(row("Ручка GraphQL и очередь", y))
    y += 1
    panels += [
        ops_panel("Запросы к GraphQL в минуту (1 запрос = страница до 400 билетов)",
                  ["всего запросов", "запросы фона", "запросы приложения", "лимит в минуту"], 0, y,
                  description="Сколько запросов к ручке prices_one_way ушло за минуту: фон — сборщик, "
                              "приложение — планировщик (он берёт рейсы из склада в памяти и ходит в "
                              "источник только за дырами, поэтому обычно около нуля). Пунктир — лимит "
                              "коллектора (60 в минуту на токен; после 429 временно снижается). "
                              "Держаться у лимита — норма: ручка выбирается целиком.",
                  overrides=[by_name("всего запросов", color("#8f8f8f")),
                             by_name("лимит в минуту", color("red"), DASHED)]),
        ops_panel("Очередь коллектора",
                  ["фон, окон дат", "приложение, заданий", "фон, оценка страниц"], 12, y,
                  description="Буфер, а не отставание. Сборщик раз в 2 мин досыпает очередь до ~1800 "
                              "оценочных страниц (~30 мин работы ручки при 60 запросах в минуту), не "
                              "больше 3000 окон; очередь живёт в памяти коллектора — после его рестарта "
                              "(деплой) падает в ноль и за пару минут набирается снова. Окно дат = «город × несколько дней вылета» одним "
                              "запросом: у маленького города это одна страница (секунда), у крупного — "
                              "десятки, поэтому число окон скачет пилой и «падает мгновенно», пока "
                              "оценка страниц (правая ось) держит уровень. Очередь пуста надолго — "
                              "сборщику нечего обновлять или он не работает.",
                  overrides=[by_name("фон, оценка страниц", RIGHT, DASHED)]),
    ]
    y += 8
    panels += [
        ops_panel("429 и ошибки в минуту",
                  ["429", "ошибка в ответе GraphQL", "сеть / HTTP 5xx", "запись в озеро"], 0, y,
                  description="429 — превышен лимит ручки (коллектор ждёт и снижает темп; запрос "
                              "повторяется, данные не теряются). Ошибка в ответе GraphQL — источник "
                              "ответил полем errors (обычно неизвестный код города); тексты — в "
                              "таблице «Ошибки источника». Сеть / HTTP 5xx — таймауты, обрывы и 5xx, "
                              "до 3 повторов. Запись в озеро — не удалось положить файл в S3."),
        ops_panel("Готовые выборки и билеты в минуту",
                  ["выборок", "билетов"], 12, y,
                  description="Выборка — результат одного задания коллектора: «город → куда угодно» "
                              "на окно дней вылета (или пара A→B на день от приложения), все страницы. "
                              "Билеты — строки в ней (конкретные варианты перелёта с ценой).",
                  overrides=[by_name("билетов", RIGHT)]),
    ]
    y += 8
    panels.append(row("Машина и хранилище", y))
    y += 1
    panels += [
        ops_panel("Нагрузка VM, % от предела", ["CPU", "RAM", "диск"], 0, y, unit="percent",
                  description="Проценты от возможностей VM (CPU — все ядра, RAM — вся память машины, "
                              "диск — том данных). Пунктир: 80 % — внимание, 95 % — предел.",
                  custom={"thresholdsStyle": {"mode": "dashed"}},
                  defaults={"min": 0, "max": 100, **threshold_lines((80, "orange"), (95, "red"))}),
        ops_panel("Озеро и бакет, ГБ", ["озеро (tickets/)", "весь бакет", "порог ретеншна"], 12, y,
                  unit="decgbytes",
                  description="Озеро — файлы билетов tickets/ (по учёту коллектора). Весь бакет — "
                              "озеро плюс служебное: ops_metrics/ (эти графики), coverage/ (снимок "
                              "покрытия) и прочее (листинг S3 раз в 30 мин). Порог ретеншна — выше "
                              "него удаляются самые старые файлы озера; жёсткий потолок бакета — 200 ГБ.",
                  overrides=[by_name("порог ретеншна", color("red"), DASHED)]),
    ]
    y += 8
    panels += [
        ops_panel("Файлы озера и удаления ретеншном", ["файлов", "удалено за минуту"], 0, y, w=24,
                  overrides=[by_name("удалено за минуту", RIGHT)]),
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
