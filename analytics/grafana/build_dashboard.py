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
OPS_FIELDS = [
    ("crawler_fresh", "свежие"), ("crawler_pairs", "всего пар"),
    ("crawler_refresh", "из них старше суток (обновляются)"),
    ("crawler_stale", "устарели"), ("crawler_missing", "нет данных"), ("crawler_errors", "ошибка источника"),
    ("crawler_pass_progress", "проход"), ("cities", "городов"),
    ("series_age_p50_h", "медиана"), ("series_age_p95_h", "p95"), ("series_age_max_h", "максимум"),
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
OFFSET = "dateDiff('day', today(), day)"


def build() -> dict:
    y = 0
    panels = [row("Данные и свежесть", y)]
    y += 1
    panels += [
        stat("Пар «город × день» на горизонте", "всего пар", 0, y,
             description="Сколько пар «город вылета × день вылета» сборщик держит свежими: "
                         "города × 181 день от сегодня."),
        stat("Городов", "городов", 4, y),
        stat("Первый проход", "проход", 8, y, unit="percentunit",
             description="Доля пар, по которым есть хоть какие-то данные.",
             thresholds=[{"color": "red", "value": None}, {"color": "orange", "value": 0.5},
                         {"color": "green", "value": 0.95}]),
        stat("Устарело пар", "устарели", 12, y,
             description="Пары старше цели свежести: до 60 дней вперёд — 72 ч, дальше — неделя.",
             thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 500},
                         {"color": "red", "value": 5000}]),
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
        ops_panel("Покрытие сборщика: пары «город × день вылета»",
                  ["всего пар", "свежие", "из них старше суток (обновляются)", "устарели", "нет данных",
                   "ошибка источника"], 0, y, source=True,
                  description="Каждая линия — отдельное число, без стека (раньше ряды складывались, "
                              "и «устарели»/«нет данных» рисовались поверх «свежих», повторяя их). "
                              "Левая ось: все пары на горизонте, свежие и те из свежих, что старше "
                              "суток — сборщик обновляет их вторым эшелоном, самые старые первыми. "
                              "Правая ось (пунктир): проблемные пары — устарели (старше цели: 72 ч "
                              "до 60 дней вперёд, неделя дальше), нет данных, последняя попытка — "
                              "ошибка источника (тексты — в таблице «Ошибки источника»).",
                  custom={"axisLabel": "пар"},
                  overrides=[by_name("всего пар", color("#8f8f8f"), DASHED),
                             by_name("свежие", color("green"), ("custom.fillOpacity", 10)),
                             by_name("из них старше суток (обновляются)", color("#3d7fd9")),
                             by_name("устарели", color("orange"), RIGHT, DASHED),
                             by_name("нет данных", color("purple"), RIGHT, DASHED),
                             by_name("ошибка источника", color("red"), RIGHT, DASHED)]),
        ops_panel("Возраст данных сборщика, ч (пары X→ANY на дни вылета от сегодня)",
                  ["медиана", "p95", "максимум"], 12, y, unit="h",
                  description="Сколько часов назад получены данные пары «город × день вылета» — "
                              "по всем парам на горизонте (сегодня … +180 дней): медиана, p95 и самая "
                              "старая. Прошедшие дни вылета и пары, запрошенные приложением (A→B), "
                              "не обновляются и лежат до ретеншна — их здесь нет (до 02.10.2026 "
                              "считалось по ним, и максимум рос бесконечно). Пунктир: 24 ч — с этого "
                              "возраста пара встаёт на обновление (кэш коллектора — сутки), 72 ч — цель "
                              "свежести ближних дат.",
                  custom={"thresholdsStyle": {"mode": "dashed"}},
                  defaults=threshold_lines((24, "#3d7fd9"), (72, "orange"))),
    ]
    y += 8
    panels += [
        panel("Когда получены данные: пар «город × день» по часу получения",
              f"SELECT toStartOfHour(fetched_at) AS time, "
              f"countIf({OFFSET} <= 14) AS \"вылет через 0–14 дн.\", "
              f"countIf({OFFSET} BETWEEN 15 AND 60) AS \"15–60 дн.\", "
              f"countIf({OFFSET} > 60) AS \"61–180 дн.\" "
              f"FROM flights.coverage WHERE day >= today() AND NOT error AND $__timeFilter(fetched_at) "
              f"GROUP BY time ORDER BY time", 0, y, stack=True,
              description="Распределение текущих данных по времени получения: у каждой пары "
                          "«город × день» на горизонте берётся последний раз, когда её скачали, и "
                          "считается, сколько пар пришлось на каждый час (цвет — дальность даты "
                          "вылета). Правый край — то, что сборщик обновляет прямо сейчас; провал — "
                          "часы, данные которых уже обновлены заново. Снимок покрытия — раз в 10 мин.",
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
              12, y, ptype="table", kind="table",
              description="Какие города (и какие дни вылета от сегодня) сборщик скачал за час до "
                          "последнего снимка покрытия (раз в 10 мин). Почему именно они — в панели "
                          "покрытия: сначала «нет данных» и «устарели», затем второй эшелон — свежие "
                          "старше суток, самые старые первыми."),
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
              0, y, ptype="table", kind="table",
              description="Пары «город × день», где последняя попытка закончилась ошибкой: "
                          "текст ответа источника (обычно «city … not found» — код, которого "
                          "Aviasales не знает) или «network: …» — сеть и HTTP 5xx после повторов. "
                          "Повтор — не раньше чем через сутки; город, у которого одни ошибки, — "
                          "в карантине (проверяется одна дата)."),
        panel("Самые старые данные по городам (топ 20)",
              "SELECT origin AS \"город\", round(max(age_h), 1) AS \"макс. возраст, ч\", "
              "round(avg(age_h), 1) AS \"средний, ч\", count() AS \"дней\", "
              "sum(tickets) AS \"билетов\" FROM flights.coverage WHERE day >= today() AND NOT error "
              "GROUP BY origin ORDER BY max(age_h) DESC LIMIT 20", 12, y, ptype="table", kind="table",
              description="Возраст — на момент снимка покрытия (раз в 10 мин). Около суток — "
                          "норма: со 24 ч пара встаёт на обновление вторым эшелоном."),
    ]
    y += 8
    # Запас свежести по дню вылета: 7 − возраст в сутках. Только что собранный день = 7,
    # дальше убывает на 1 в сутки; ниже 0 — старше недели. Три ряда — перцентили возраста
    # по городам дня: p50 (половина городов свежее), p95 (все, кроме 5 % отстающих) и
    # p100 (худший город). Город без данных на день считается самым старым (а не
    # выпадает): иначе день, где собрано 5 % городов, выглядел бы свежим. Сетка —
    # все города с хоть одной успешной серией × 180 дней; снизу значение прижато к −1
    # («нет данных или старше 8 суток»). Цвет — ряд (оттенки одного синего от p50 к
    # p100), пороги целей — пунктиром. Городов с данными — только в подсказке.
    hidden = {"id": "custom.hideFrom", "value": {"viz": True, "legend": True, "tooltip": False}}
    shades = {"p50": "#9ec5f4", "p95": "#3d7fd9", "p100": "#1a3f7a"}
    pct = "greatest(round(7 - {agg}(ifNull(cv.age_h, 1e6)) / 24, 2), -1)"
    panels += [
        panel("Запас свежести по дням вылета на 180 дней: p50 / p95 / p100 городов (7 − возраст в сутках)",
              "SELECT formatDateTime(g.day, '%d.%m') AS \"день\", "
              f"{pct.format(agg='quantile(0.5)')} AS \"p50\", "
              f"{pct.format(agg='quantile(0.95)')} AS \"p95\", "
              f"{pct.format(agg='max')} AS \"p100\", "
              "countIf(cv.age_h IS NOT NULL) AS \"городов с данными\", count() AS \"всего городов\" "
              "FROM (SELECT o.origin AS origin, today() + n.number AS day "
              "FROM (SELECT DISTINCT origin FROM flights.coverage WHERE NOT error) AS o "
              "CROSS JOIN numbers(180) AS n) AS g "
              "LEFT JOIN (SELECT origin, day, age_h FROM flights.coverage WHERE day >= today() AND NOT error) AS cv "
              "ON cv.origin = g.origin AND cv.day = g.day "
              "GROUP BY g.day ORDER BY g.day SETTINGS join_use_nulls = 1",
              0, y, w=24, ptype="barchart", kind="table",
              description="Перцентили возраста данных по городам дня вылета: p50 — половина городов "
                          "свежее, p95 — все, кроме 5 % самых отстающих, p100 — худший город. Город без "
                          "данных на этот день считается самым старым: если не собрано больше 5 % городов, "
                          "p95 на дне (−1), больше половины — и p50. "
                          "Горизонт сборщика — 180 дней от сегодня. 7 — только что обновлено, минус 1 за "
                          "каждые сутки, −1 — нет данных или старше 8 суток. Пунктир: 4 — 72 ч (цель до "
                          "60 дней вперёд), 0 — неделя (цель для дальних дат). Пока сборщик успевает "
                          "обновлять всё за сутки, столбики ровные (~6) — это норма. Города, у которых "
                          "только ошибки источника, не учитываются.",
              extra={"fieldConfig": {"defaults": {"custom": {"fillOpacity": 90, "lineWidth": 0,
                                                             "thresholdsStyle": {"mode": "dashed"}},
                                                  "min": -1, "max": 7, "decimals": 1,
                                                  "thresholds": {"mode": "absolute", "steps": [
                                                      {"color": "transparent", "value": None},
                                                      {"color": "orange", "value": 0},
                                                      {"color": "green", "value": 4}]}},
                                     "overrides": [{"matcher": {"id": "byName", "options": name},
                                                    "properties": [{"id": "color", "value": {
                                                        "mode": "fixed", "fixedColor": color}}]}
                                                   for name, color in shades.items()]
                                                  + [{"matcher": {"id": "byName", "options": name},
                                                      "properties": [hidden, {"id": "decimals", "value": 0}]}
                                                     for name in ("городов с данными", "всего городов")]},
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
                  description="Буфер, а не отставание. Сборщик раз в 2 мин досыпает очередь до ~600 "
                              "оценочных страниц (~10 мин работы ручки при 60 запросах в минуту), не "
                              "больше 500 окон. Окно дат = «город × несколько дней вылета» одним "
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
