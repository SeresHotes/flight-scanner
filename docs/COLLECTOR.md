# Коллектор и Parquet-озеро

Билеты больше не живут в SQLite: их собирает отдельный сервис **коллектор** и
складывает Parquet-файлами в Object Storage. В SQLite остаётся только горячее
состояние (джобы планировщика, котировки, индекс серий коллектора).

```
web (Caddy) ──/api/*──> planner (api/)  ──POST /v1/fetch──> collector (collector/) ──> GraphQL Data API
                                                             │  очередь app > crawl, лимит 60/мин
crawler (фаза 2) ──POST /v1/batch (crawl)────────────────────┘  │
                                                                └──> S3: tickets/observed=YYYY-MM-DD/part-*.parquet
                                                                     индекс серий: data/collector.db (SQLite)
```

Три Python-образа из трёх Dockerfile (`deploy/Dockerfile.planner`, `.collector`,
`.crawler` — в фазе 2) плюс `flights-web`. Токен Travelpayouts и ключи S3 нужны
только коллектору (на VM общий `.env`, планировщик их не использует).

## Фазы

1. **Коллектор и переход на S3** (этот документ): сервис, очередь с приоритетами,
   лимит ручки, озеро, индекс, ретеншн; планировщик ходит за сериями в коллектор;
   переименование `api` → `planner`; потолок бакета 200 ГБ.
2. **Фоновый сборщик** (`crawler/`): покрытие «город × день» на 180 дней вперёд,
   целевая свежесть по дальности даты, открытие новых городов из ответов, подача
   заданий в очередь `crawl`. Собираем только `X→ANY` (обратная сторона дублирует).
3. **Дашборды**: ops-метрики и снимок покрытия из коллектора в бакет, вьюхи
   ClickHouse и дашборд Grafana (папка «Flights») на аналитической VM Market Data.
4. **Отдельный репозиторий для аналитической VM** (terraform, compose, провижининг);
   проекты оставляют у себя только дашборды и SQL.
5. **Планировщик на данных озера**: без запросов к источнику из планировщика.
   Запуск — по решению, когда данных накопится достаточно.

## Коллектор (`collector/`)

- **Единица очереди — серия**: направление × день × прочие параметры (коридор цен,
  direct, багаж) — тот же ключ, что был у `ticket_cache`. **Единица работы — одна
  страница GraphQL** (400 билетов). После каждой страницы серия возвращается в
  очередь, и воркер берёт самое приоритетное задание: серия приложения (`app`)
  вытесняет фоновую (`crawl`) на границе страницы, та продолжает с того же offset.
- Одинаковые серии от разных клиентов **склеиваются** в одно задание (приоритет
  поднимается до `app`, глубина — до максимальной из запрошенных).
- **Кэш**: свежая серия из индекса (моложе `SERIES_TTL_SECONDS`, по умолчанию сутки;
  для обрезанной серии — не меньше страниц, чем просят) отдаётся без источника —
  из буфера или range-чтением своей row group из Parquet.
- **Лимит ручки** (`collector/ratelimit.py`): ровный темп `RATE_PER_MINUTE` (60).
  На 429 — пауза `Retry-After` либо бэкофф 5 → 10 → … → 60 с, затем две минуты
  темп в полтора раза ниже. Сетевые ошибки — три повтора страницы, ошибка источника
  (400: неизвестный город и т.п.) — серия помечается `error` и кэшем не считается.
- **Озеро** (`collector/lake.py`): буфер серий сбрасывается файлом
  `tickets/observed=YYYY-MM-DD/part-<время>.parquet` по `LAKE_FLUSH_TICKETS`
  (25 000 билетов) или `LAKE_FLUSH_SECONDS` (5 мин); одна серия = одна row group.
  Колонки плоские (origin, destination, departure_at, price, airline, …) плюс
  `legs_json`, `transfer_points_json`, `chain_json`; `from_row` восстанавливает
  словарь `normalize_ticket` один в один.
- **Индекс** (`collector/index.py`, SQLite `COLLECTOR_DB`): серии (когда получена,
  страниц, билетов, файл + row group, ошибка), файлы (объём для ретеншна), города
  (все origin/destination из билетов — список для сборщика). Потерян индекс →
  при старте файлы озера импортируются в учёт объёма (без серий).
- **Ретеншн**: раз в `LAKE_RETENTION_INTERVAL` (30 мин) объём `tickets/` по индексу
  сравнивается с `LAKE_MAX_GB` (180); выше — удаляются самые старые файлы до 95 %
  порога, их серии выпадают из индекса. Lifecycle Object Storage умеет только по
  возрасту, поэтому по размеру чистим сами; `max_size` бакета 200 ГБ — аварийный потолок.

### HTTP API (порт 8001, только внутри compose-сети)

| Ручка | Что |
|---|---|
| `POST /v1/fetch` | одна серия `{origin, destination, day, value_min, value_max, direct, with_baggage, max_pages, client, ttl_seconds, wait}` → задание `{id, status, pages, tickets, cached, error}` |
| `GET /v1/requests/{id}?wait=` | статус (long-poll до `wait` с) |
| `GET /v1/requests/{id}/result` | `{tickets, pages, exhausted, error, cached}` — билеты отдаются один раз |
| `POST /v1/batch` | список серий, `client: crawl` |
| `GET /v1/series/exists` | свежая ли серия (оценка объёма в планировщике) |
| `GET /v1/coverage?params_key=&destination=` | `[origin, day, fetched_at, pages, tickets, exhausted, error]` |
| `GET /v1/cities` | известные города по числу билетов |
| `GET /v1/stats`, `POST /v1/stats/crawler` | счётчики очереди/страниц/429/кэша, сводка сборщика |

## Планировщик

`api/worker.make_ticket_fetch`: при `COLLECTOR_URL` серии берутся через
`core/collector_client.CollectorClient.fetch_series` (тот же контракт, что у
`graphql_api.fetch_series`: прогресс на каждую страницу, `cached` → «из кэша» в
степпере). Без `COLLECTOR_URL` — прежний прямой GraphQL с `ticket_cache` (локальный
запуск, тесты). С коллектором таблица `ticket_cache` при старте удаляется.

## Деплой

- Образы: `flights-planner`, `flights-collector`, `flights-web` (GitHub Actions
  `.github/workflows/deploy.yml`).
- Прод-compose `deploy/compose.prod.yml` едет внутри образа planner; `run.sh` на VM
  при каждом обновлении копирует его в `/opt/flights/docker-compose.yml`, так что
  состав контейнеров меняется обычным деплоем (cloud-init выполняется один раз).
- Terraform: `bucket_max_size_gb` 200, в `.env` VM — `PLANNER_IMAGE`, `COLLECTOR_IMAGE`,
  `LAKE_MAX_GB`, `RATE_PER_MINUTE`. `terraform apply` на существующей VM меняет только
  бакет и metadata (VM не пересоздаётся).
- **Одноразово на уже созданной VM**: `deploy/vm-migrate.sh ubuntu@<ip>` — дополняет
  `.env`, заменяет `run.sh`, останавливает старый стек, сжимает `flights.db` без
  `ticket_cache` (`scripts/shrink_planner_db.py`, VACUUM INTO — обычному VACUUM не
  хватило бы диска) и поднимает новый.

Проверка после выкатки: `https://flights.sereshotes.dev/api/health` содержит блок
`collector` (`status: ok`, очередь, серии, объём озера); джоба планировщика идёт
через коллектор; в бакете появляются `tickets/observed=…/part-*.parquet`.
