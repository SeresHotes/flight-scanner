# Склад билетов (`tickets/`, контейнер `flights-tickets`)

Текущее состояние всех билетов из серий **X→ANY** озера коллектора: одна строка на билет
в Postgres, новая серия «город вылета × день» целиком заменяет старые записи этой серии.
Планировщик берёт рейсы отсюда за один запрос на плечо вместо сотен серий из S3 через
коллектор; озеро остаётся как есть (история, аналитика, источник истины для склада).

```
collector ──запись файла в S3──┬──> POST /v1/files (те же байты Parquet)  ──> tickets ──> Postgres (tickets-db)
                               │                                              ▲  ▲
                               └──> индекс files ── GET /v1/lake/files, /v1/lake/file ──┘  │ сверка (заливка на старте, догон)
planner ── GET /v1/tickets?origin=|destination=|via=&from=&to= (Arrow IPC в схеме озера) ──┘
```

## Данные

- `tickets` — колонки схемы озера (`collector/lake.SCHEMA`, 28 полей: концы на уровне
  города и аэропорта, времена, длительности, `legs_json`, `transfer_points_json`,
  `chain_json`, багаж, ссылка, `search_*`) плюс `dep_day` (день вылета = `search_date`) и
  `via text[]` (коды пересадок из `transfer_points` — для hidden-city). Индексы:
  `(search_origin, dep_day)` — замена серии; `(origin, dep_day)`, `(destination, dep_day)`,
  `(dep_day)`; GIN по `via`.
- `series (origin, dep_day, fetched_at, tickets, file_key)` — покрытие склада: какие
  «город × день» есть и насколько свежие; пустые дни окна тоже серии (0 билетов — день
  покрыт, планировщику не нужно идти в GraphQL).
- `lake_files (key, created_at, applied_at, rows)` — журнал применённых файлов озера:
  сверка не перебирает их снова. Файлы ANY→Y, A→B и с ценовыми коридорами (запросы
  приложения) в склад не идут (склад = X→ANY краулера), но отмечаются применёнными.
- `airport_city (airport, city)` — карта аэропорт → город из концов билетов: `via=<город>`
  расширяется до его аэропортов.

Объём на 01.10.2026: 511 тыс. серий, 18,7 млн билетов, ~1,3 КБ на строку → 26 ГБ с индексами;
память VM 8 ГБ, диск 100 ГБ (Terraform). Postgres 16 в compose (`tickets-db`, `shared_buffers`
1 ГБ) на **отдельном SSD** `/opt/flights/pg` (network-ssd-nonreplicated 93 ГБ,
`yandex_compute_disk.pg`, перенос — `deploy/vm-pg-ssd.sh`): таблица не влезает в память, выборка
— тысячи случайных чтений; на network-hdd холодный запрос шёл 25–86 с, на SSD с предвыборкой
(`effective_io_concurrency=200`) — 0,4–2,5 с холодный (ANY→MOW за неделю, 38 МБ — 2,4 с),
< 0,7 с тёплый. Без реплик: склад целиком восстанавливается из озера сверкой.

## Как серии попадают в склад

1. **Пуш** (`collector/push.py`): сразу после записи файла в озеро коллектор кладёт те же
   байты в очередь и фоновым потоком шлёт `POST /v1/files?key=&created_at=`. Очередь
   ограничена, недоступный склад записи не мешает — пуш только ускоритель.
2. **Сверка** (`tickets/src/sync.rs`): на старте склад берёт у коллектора весь список файлов
   индекса (`GET /v1/lake/files`), сравнивает с журналом и забирает недостающие
   (`GET /v1/lake/file?key=`), в несколько параллельных загрузок. На пустом складе это
   **разовая заливка всего озера** (123 тыс. файлов, ~40 мин при 4 загрузках). Дальше — раз в
   `TICKETS_SYNC_SECONDS` (300) по файлам новее последнего применённого минус 6 ч.
3. **Применение файла** (`Store::apply_file`) — одна транзакция: по каждому дню окна файла —
   если в складе серия не новее (`fetched_at` из имени файла), `DELETE` билетов дня,
   `COPY … BINARY` новых, upsert строки `series`; затем журнал `lake_files`. Повтор того же
   файла — no-op по содержимому; более старый файл на уже обновлённый день пропускается;
   рестарт любого контейнера ничего не теряет (сверка догоняет) и не дублирует (замена
   серии атомарна).
4. **Ретеншн**: раз в час `DELETE` дней вылета раньше вчера (UTC, запас на часовые пояса).

## HTTP API (порт 8002, только внутри compose-сети)

| Ручка | Что |
|---|---|
| `GET /v1/tickets?from=&to=[&origin=A,B][&destination=C,D][&via=E][&limit=][&format=json]` | билеты с днём вылета в `[from, to]`: из города (X→ANY), в город (ANY→Y), пара, списки с обеих сторон (ANY→ANY между соседними плечами), через город (hidden-city). Нужен хотя бы один из `origin/destination/via`. Ответ — Arrow IPC в схеме озера (как у коллектора), `X-Tickets-Count`; `format=json` — словари как у `normalize_ticket`. Потолок строк — `TICKETS_MAX_ROWS` (500 000) |
| `GET /v1/coverage?from=&to=[&origin=A,B]` | серии склада `{origin, day, fetched_at, tickets}` — планировщик решает, за чем идти в GraphQL |
| `GET /v1/coverage/days?from=&to=` | по дням: сколько серий (городов) и билетов |
| `POST /v1/files?key=&created_at=` | файл озера (Parquet в теле) → серии склада |
| `POST /v1/sync` | внеочередная сверка |
| `GET /v1/health` | серии, билеты, города, дни, журнал файлов, размер БД |

Коллектор для склада: `GET /v1/lake/files?since=` и `GET /v1/lake/file?key=`.

## Планировщик на складе (`planner/src/tickets.rs`, `collect.rs`)

- `TICKETS_URL` у планировщика → `TicketsClient`; на джобу один раз считается покрытие
  (`/v1/coverage` по городам вылета плеч и `/v1/coverage/days` по дням окон).
- Порядок сбора: сначала плечи с конкретным городом — город→город и город→ANY одним
  запросом X→ANY по городам вылета за окно (прямые A→B и hidden-city из тех же билетов),
  ANY→город — срез по городу прилёта; затем «любой → любой»: вылеты — города прилёта
  предыдущего плеча, прилёты — города вылета следующего (если уже собрано).
- В GraphQL через коллектор — только за отсутствующим: X→ANY — (город, день) без серии в
  складе; ANY→Y — дни, по которым в складе нет ни одной серии. ANY→ANY без склада — ошибка.
- Оценка (`POST /api/plan/estimate`): `cached` включает единицы из склада, `source: tickets`;
  в степпере джобы они идут в «из кэша».

## Запуск и проверка

- Переменные: `TICKETS_PG_URL`, `COLLECTOR_URL`, `PORT` (8002), `TICKETS_SYNC_SECONDS`,
  `TICKETS_SYNC_WORKERS` (4), `TICKETS_MAX_ROWS`, `TICKETS_RETENTION_HOURS`; у коллектора —
  `TICKETS_URL`. В `.env` VM — `TICKETS_IMAGE`, `TICKETS_PG_PASSWORD` (terraform
  `random_password.tickets_pg`; на уже созданной VM — `deploy/vm-tickets-env.sh`).
- Тесты: `cd tickets && cargo test` (юнит) и с живым Postgres —
  `TICKETS_TEST_PG_URL=postgres://tickets@localhost:54329/tickets cargo test --test store_pg`
  (файл окна → серии, повтор без дублей, свежая серия заменяет, старая пропускается,
  выборки, покрытие, ретеншн, HTTP).
- Проверка на проде: `docker compose exec tickets wget -qO- localhost:8002/v1/health`,
  число серий сходится с индексом коллектора (`series` в `/v1/health` коллектора минус
  серии с ошибками и не-X→ANY); выборка ANY→Y за неделю —
  `time wget -qO /dev/null 'http://tickets:8002/v1/tickets?destination=MOW&from=…&to=…'`.
