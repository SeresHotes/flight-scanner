# Аналитика: дашборд коллектора в общей Grafana

Данные flights показываются на аналитической VM проекта market-data-fetcher
(ClickHouse читает Parquet прямо из Object Storage, Grafana на `:3000`). Сама VM
и её провижининг живут в том репозитории (до фазы 4 — отдельного репо аналитики);
здесь только то, что принадлежит flights:

- `clickhouse/named_collections_flights.xml.tmpl` — креды и URL бакета flights
  (`tickets/`, `ops_metrics/`, `coverage/`), рендерится из terraform output.
- `clickhouse/users_flights.xml.tmpl` — пользователь ClickHouse `flights_grafana`
  для источника данных Grafana (только `flights.*`).
- `clickhouse/init-flights.sql` — база `flights`: вьюхи `tickets`, `ops_metrics`, `coverage`.
- `grafana/build_dashboard.py` → `grafana/dashboards/flights-ops.json` — дашборд
  «Flights · Коллектор»: свежесть данных и покрытие сборщика, ручка GraphQL и очередь,
  нагрузка VM, озеро и бакет.
- `setup.sh` — кладёт конфиги на VM, применяет SQL, создаёт источник данных
  «ClickHouse Flights», папку «Flights» и заливает дашборды. Идемпотентно.

```sh
python analytics/grafana/build_dashboard.py     # после правки панелей
analytics/setup.sh ubuntu@89.169.140.225        # SSH_OPTS, GRAFANA_ADMIN_PASSWORD — см. шапку скрипта
```

Файлы Market Data на VM (`named_collections.xml`, `users.d/grafana.xml`, папка
«Market Data») не трогаются: у flights свои файлы в тех же каталогах, свой
пользователь и свой источник данных.

Что пишет коллектор (`collector/metrics.py`): раз в минуту строка
`ops_metrics/date=YYYY-MM-DD/<HH-MM-SS>.parquet` (10 строк на файл) — нагрузка машины,
объём озера и бакета, очередь, страницы/серии/429/ошибки за минуту по клиентам,
задержка ответа приложению, возраст серий, сводка сборщика; раз в 10 минут —
`coverage/latest.parquet` (город × день вылета: когда получено, возраст, билетов).
