# Аналитика: дашборд коллектора в общей Grafana

Данные flights показываются в [observatory](https://github.com/SeresHotes/observatory) —
общей VM с ClickHouse (читает Parquet прямо из Object Storage) и Grafana на
https://grafana.sereshotes.dev. VM раз в 2 минуты берёт с `main` этого репо:

- `observatory.yaml` — база `flights`, named collections бакета (`tickets/`,
  `ops_metrics/`, `coverage/`; креды и пользователей ClickHouse подставляет observatory),
  SQL и каталог дашбордов, datasource `flights-clickhouse`;
- `clickhouse/init-flights.sql` — вьюхи `tickets`, `ops_metrics`, `coverage`
  (применяется от `flights_admin` при изменении);
- `grafana/build_dashboard.py` → `grafana/dashboards/flights-ops.json` — дашборд
  «Flights · Коллектор»: свежесть данных и покрытие сборщика, ручка GraphQL и очередь,
  нагрузка VM, озеро и бакет.

```sh
python analytics/grafana/build_dashboard.py                  # после правки панелей
python3 ../observatory/vm/observatory.py check analytics     # та же проверка, что на VM
```

Коммит, не прошедший проверку, VM не возьмёт — останется на прошлом рабочем. В UI
дашборд не сохраняется: правка — в генераторе, затем PR.

Что пишет коллектор (`collector/metrics.py`): раз в минуту строка в файл часа
`ops_metrics/date=YYYY-MM-DD/<HH>-00-00.parquet` (перезапись каждые 2 мин; прошедшие дни
склеиваются в `day.parquet` — мало файлов, быстрый дашборд) — нагрузка машины,
объём озера и бакета, очередь, страницы/серии/429/ошибки за минуту по клиентам,
задержка ответа приложению, возраст данных сборщика, сводка сборщика; раз в 10 минут —
`coverage/latest.parquet` (город × день вылета: когда получено, возраст, билетов, текст ошибки).

Дашборд делает один запрос к `ops_metrics` (панель «Покрытие сборщика»); остальные
временные ряды и плитки берут его результат через источник «-- Dashboard --» и
оставляют свои поля — имена полей в `OPS_FIELDS` генератора уникальны.
