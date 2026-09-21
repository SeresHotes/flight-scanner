# План развития Flight Scanner: бек/фронт-сервис + периодический сбор

Статус: согласовано, к реализации. Последнее обновление: 2026-09-21.

Документ фиксирует целевую архитектуру и план работ. Все ключевые развилки
уже приняты (см. раздел «Принятые решения» — там же обоснования).

## Цель

Две фичи поверх текущего пайплайна CLI-скриптов:

1. **Полноценный бек/фронт.** Запрос с фронта на любые остановки A/B заставляет
   бэк собрать недостающие данные и вернуть результат.
2. **Периодический сбор данных Aviasales** по разным направлениям с кэшированием.
   Собираем как можно больше данных для дальнейшей обработки.

## Принятые решения

| Решение | Выбор | Почему |
|---|---|---|
| Backend | **FastAPI** + фоновый asyncio-воркер | async-джобы из коробки, тот же Python, что и ядро |
| UX долгого сбора | **Джоба + прогресс** (polling / SSE) | сбор по диапазону дат идёт минуты; синхронный HTTP отвалится по таймауту |
| Поисковая логика | **Сохраняем stopover-логику** (there/back/both, переходы между аэропортами) | это вся ценность проекта |
| Горячее хранилище | **SQLite (WAL)** | serving-нагрузка скромная; ноль администрирования и стоимости сверх VM |
| Архив данных | **Parquet в Object Storage** (append-only, партиции по дате/маршруту) | ценность = временной ряд наблюдений цены; колоночно, дёшево, готово к обработке |
| Аналитика | **DuckDB** поверх Parquet-озера | читает Parquet прямо из S3, ноль ETL; вырастет объём — тот же Parquet зальётся в ClickHouse |
| Frontend | **Vite + React + TypeScript + TanStack Query** | появились форма поиска, автокомплит A/B, polling джоб; контракт данных не меняется |
| Стили | **Переносим текущий CSS 1:1** | тёмная тема уже выверена; Tailwind/UI-кит дали бы другой вид и лишнюю работу |
| Инфраструктура | **Yandex Cloud: одна burstable VM + Docker Compose** | грант 15k₽ ограничен; managed-сервисы 24/7 (Managed PG и т.п.) съели бы его за недели |
| Прочее YC | **Object Storage** (озеро + бэкапы), **Container Registry**, **Caddy** (статика + `/api` reverse-proxy + авто-TLS) | один origin (без CORS), минимум managed-сервисов |

**Явно отвергнуто:** Managed PostgreSQL и вообще Postgres как основа (дорого 24/7,
хуже под историю цен и колоночную аналитику, чем Parquet); Managed K8s (overkill);
YMQ/Redis как очередь джоб (избыточно на этом масштабе — хватает таблицы `jobs`).

## Целевая архитектура

```
React (Vite/TS)  ──POST /api/search──>  FastAPI  ──> SQLite (quotes + jobs)
  TanStack Query  <──{trips, job_id?}──     │  ▲            ▲
  polling /jobs/{id}                        │  │ upsert     │ прогресс
        ▲                                   ▼  │            │
     Caddy (TLS, /api → FastAPI,       Worker (asyncio + APScheduler)
            / → web/dist)                 │  • collector.collect(недостающее)
                                          │  • rate-limit к Travelpayouts
                                          │  • append Parquet → Object Storage
                                          ▼
                            Object Storage: quotes/dt=YYYY-MM-DD/route=A-B/*.parquet
                                          ▼  DuckDB (обработка/выгрузки)
```

## Структура репозитория (целевая)

```
core/        aggregate.py · trip_builder.py · collector.py · airports.py   # из текущих скриптов
storage/     hot.py (SQLite)  · lake.py (Parquet→S3)  · analytics.py (DuckDB)
api/         main.py · search.py · meta.py · worker.py
frontend/    Vite+React+TS: SearchForm · AirportCombobox · JobProgress ·
             ResultsView(Stats/FilterBar/Controls/TripCard)
deploy/      Dockerfile · docker-compose.yml · Caddyfile · config.yaml       # config = направления для крона
```

Ядро — рефактор существующих скриптов без смены поведения (CLI остаётся рабочим):
- `core/aggregate.py`  ← `aggregate_flights.py` (`find_combinations`, переходы между аэропортами)
- `core/trip_builder.py` ← `build_web_data.py` (`Builder`, категории there/back/both)
- `core/collector.py` ← `collect_flights.py`, но как функции `collect(origin, dest, dates)`
- `core/airports.py` ← справочник + `airport_network` + city lookup

## Модель данных

**SQLite (горячее):**
- `quotes(origin, destination, origin_airport, dest_airport, departure_at, price,
  airline, flight_number, transfers, duration, link, direct, observed_at,
  search_origin, search_destination, search_date)`;
  индексы `(origin, destination, departure_at)`, `observed_at`;
  upsert «свежайшее по маршруту+дате+рейсу».
- `jobs(id, params_json, status, progress, total, result_json, error,
  created_at, updated_at)`.
- Свежесть: `observed_at` в пределах TTL (конфиг). «Недостающее» = маршрут/дата
  без свежей котировки.

**Parquet-озеро (архив, append-only):** каждый запуск сбора добавляет файл
`quotes/dt=YYYY-MM-DD/route=A-B/part-<jobid>.parquet`. Никогда не перезаписываем —
сохраняем полную историю цен. Ретеншн чистит только горячий SQLite.

## API (черновой контракт)

- `POST /api/search` — тело: `{origin, destination, leg1_dates:[start,end],
  leg2_dates:[start,end], min_stay, max_stay, stopover_days, options...}`.
  Считает покрытие в SQLite: есть свежее → сразу `trip_builder`, отдаёт `{meta, trips}`;
  есть пробелы → заводит job, отдаёт `{meta, trips (что уже считается), job_id}`.
- `GET /api/jobs/{id}` — `{status, progress, total, trips?}`.
- `GET /api/airports?q=` — автокомплит A/B из справочника.
- `GET /api/health`.

**Контракт `trips`/`meta` совпадает с нынешним `web/data.json`** — рендер карточек
и фильтров переносится в React почти 1:1.

## Frontend

Дерево компонентов (маппинг на текущий `web/index.html`):

```
App
├─ Header (hero + событие)
├─ SearchForm                 # A/B, диапазоны дат, min_stay, дни остановки → POST /search
│   ├─ AirportCombobox ×2     # автокомплит из GET /airports?q=
│   └─ DateRangePicker ×2     # leg1 / leg2
├─ JobProgress                # polling GET /jobs/{id}: прогресс-бар, «догружаем X из Y»
└─ ResultsView
    ├─ Stats                  # ← .stats
    ├─ FilterBar              # ← сегменты направление × остановка × пересадки (счётчики)
    ├─ Controls               # ← город / цена / дни / время в пути / сортировка
    ├─ TripList → TripCard    # ← cardHTML: legRow, cityBar, regionBar, transferBadge, buy
    └─ EmptyState
```

- Фильтрация/сортировка — клиентские, над массивом `trips`
  (логика `matchDir/matchStop/matchTr` и компараторы `cmp` переезжают в хелперы).
- Polling джоб — через `useQuery` с `refetchInterval`, пока `status !== 'done'`.
- Стейт фильтров — `useState` (опционально — в URL, чтобы делиться выборкой).

## План работ по фазам

Разбивка укрупнённая (совпадает с видением владельца); в скобках — подшаги.

### Фаза 1. React + бек-архитектура (логику не меняем)
- (0) Рефактор скриптов в пакет `core/` без смены поведения; CLI продолжает работать.
- (1) `storage/hot.py`: SQLite (`quotes` + `jobs`) + импорт существующих `data/*.json`.
      `storage/lake.py`: Parquet-writer (в деве — MinIO/локальный S3).
- (2) FastAPI: `/search` (пока только из кэша, синхронно) + `/airports`.
      React-каркас (Vite/TS) с формой и портом карточек/фильтров.

> «Логику не меняем» относится к бизнес-логике (`aggregate`/`trip_builder`) — она
> нетронута. Storage-слой (SQLite) вводится уже здесь: без него React не сможет
> запрашивать произвольные A/B. Запись/сбор добавляются в Фазе 2.

### Фаза 2. Джобы с фронта (+ периодический сбор)
- Воркер (asyncio) + таблица `jobs` + прогресс; `collector.collect` пишет в SQLite
  и дозаписывает Parquet в озеро.
- `/search` детектит пробелы покрытия → заводит job → фронт (TanStack Query)
  показывает прогресс и догружает результаты.
- **Периодический сбор:** APScheduler-крон обходит направления из `deploy/config.yaml`
  и наполняет озеро/кэш. Механически — тот же джоб, запускаемый по расписанию,
  поэтому цепляется сюда же (фича №2, а не «доработка»).

### Фаза 3. В облако
- Деплой в Yandex Cloud: burstable VM + Docker Compose (api + worker + Caddy),
  Object Storage-бакет, Container Registry, Caddy (авто-TLS, статика + `/api`).
- Потребуется: Service Account + ключ, бакет, реестр, VM (2 vCPU / 4 GB), домен (или IP).

### Фаза 4. Доработки
- Ретеншн горячего SQLite; бэкап SQLite (`.dump`) в Object Storage по крону.
- DuckDB-аналитика и выгрузки из Parquet-озера.
- По необходимости — прерываемая (preemptible) VM под отдельный сборщик для экономии.

## Заметки по эксплуатации / бюджету

- 24/7 работает только VM; SQLite и озеро не добавляют постоянной платы.
- Грант 15k₽ при этой схеме растягивается на месяцы, а не недели.
- Rate-limit к Travelpayouts централизован в `collector` (сейчас `sleep(0.5)` на запрос).
- Object Storage — durable-архив; SQLite можно потерять/пересобрать из озера.
