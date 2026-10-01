# Flight Scanner — заметки для агентов

## Деплой и проверка изменений

**Пока у нас нет dev-окружения — изменения проверяются только на проде, поэтому
готовый PR всегда мержим в `main`.**

- «dev-окружение» здесь = отдельный **доступный из интернета** стенд (staging),
  куда можно выкатить и посмотреть. Локальный запуск на компьютере **не считается**
  dev-окружением.
- Такого стенда сейчас нет. Единственный способ увидеть изменения «вживую» —
  выкатить их на прод.
- Поэтому рабочий цикл: ветка → PR → **мерж в `main`**. Мерж в `main` запускает
  автодеплой (GitHub Actions `.github/workflows/deploy.yml` собирает образы →
  Container Registry → VM подхватывает таймером `flights-update`, ~2 мин) и
  изменения появляются на `https://flights.sereshotes.dev`.
- Не оставляй проверенный PR висеть черновиком «на потом»: без dev-стенда это
  единственный путь довести изменение до проверки. Мержим (после того как
  typecheck/сборка/локальная логика зелёные).

Когда появится настоящий dev-стенд (задеплоенный staging), это правило пересмотрим:
проверять на staging, а в `main` мержить уже после ревью.

## Полезное

- Архитектура и фазы — `docs/PLAN.md`; локальный запуск — `docs/RUN.md`.
- Коллектор и Parquet-озеро (очередь к GraphQL с приоритетами app > crawl, лимит
  ручки, серии в S3, индекс, ретеншн, фазы 1–5) — `docs/COLLECTOR.md`. На VM контейнеры:
  `planner` (**Rust**, крейт `planner/`, axum `/api/*`), `collector` (Python, порт 8001,
  единственный с токеном и S3), `crawler` (Python, фоновый обход «город × день» на 180
  дней, только HTTP к коллектору), `tickets` (**Rust**, крейт `tickets/`, порт 8002 —
  склад билетов: текущее состояние серий X→ANY в Postgres `tickets-db`, выборки для
  планировщика, `docs/TICKETS.md`) + `web`. VM: 2 vCPU, 8 ГБ, диск 100 ГБ, статический IP.
  Прод-compose `deploy/compose.prod.yml` едет в образе planner; на уже созданной
  VM один раз запускается `deploy/vm-migrate.sh`.
- Дашборд «Flights · Коллектор» — в общей Grafana аналитической VM Market Data
  (`http://89.169.140.225:3000`, папка «Flights»); конфиги ClickHouse, генератор
  дашборда и скрипт заливки — `analytics/` (`analytics/README.md`).
- Все ручки Travelpayouts/Aviasales (GraphQL-схема, REST Data API, Search API, справочники, лимиты) — `docs/travelpayouts/README.md`.
- Инфраструктура прод-деплоя (Terraform + cloud-init + registry) — `infra/terraform/`.
- Фронтенд: Vite + React + TS (`frontend/`), только планировщик: `pages/PlanPage`
  (`/`, запрос) → `pages/CombosPage` (`/combos/:job`) → `pages/RoutesPage`
  (`/routes/:job`); общее — `frontend/src/planner/` (запрос `query.ts`, API `api.ts`).

## Планировщик v2 (docs/PLANNER_V2.md)

Единый запрос `PlanQuery` (`planner/src/planquery.rs`; скелет + фильтры городов/плеч/длины
поездки, `maxResults`; бюджет `maxCost` удалён 01.10.2026): `POST /api/plan/run` → джоба = рейсы по
остановкам (ключ `collect_key` — виды, города, окна, радиус; TTL сутки). Сбор (`collect.rs`):
рейсы — из склада билетов `tickets` (`TICKETS_URL`, один запрос `/v1/tickets` на плечо:
X→ANY, ANY→Y, пара, ANY→ANY — после плеч с городом, сужен городами соседних плеч), серии
«направление × день» через коллектор/GraphQL — только за городами и днями без покрытия
в складе (`/v1/coverage`); все фильтры — при чтении: параметр `f` (JSON фильтров) у `GET /api/plan/jobs/{id}`,
`…/combos`, `…/routes` — вид под фильтры стыкуется из сохранённых рейсов в фоне
(этап «Стыковка»), кэш в памяти по `(джоба, view_key)`. Рейсы джобы — колонками
(`core/flightcols.FlightCols`, Parquet `plan_flights/<job>.parquet` рядом с БД): A* и
наборы строят таблицы из numpy-колонок, полный рейс (JSON) — только для результата. `…/routes?offset&limit&combos=`
— страница маршрутов (полные сегменты: багаж, пересадки, hidden-city). Фильтры
плеча применяются до перебора, городов — внутри A*, длина поездки — при выдаче.
Старый `POST /api/plan/gather` = тот же запуск с открытыми фильтрами. Наборы
городов считает бэк (`planner/src/overview.rs`, этап «combos» джобы) — только
`COMBOS_TOP` = 1000 самых дешёвых по minPrice, ветки дороже K-го отсекаются по той же
нижней оценке хвоста, что у A* (`truncated` в ответе, если наборов больше) —
`…/combos?sort&offset&limit` (сортировки count/transfers — внутри этой тысячи). Фронт ничего не считает: запрос в URL, страницы
результата поллят `/api/plan/jobs/{id}` (прогресс + сводка) и листают
`/combos` и `/routes` с бэка.

Переезд в соседний город (`core/nearby.py`): у остановки `stops[].radiusKm` —
прилетели в a, улетаем из d, если d == a или ≤ радиуса (центры городов, `core/geo.json`
из справочников Travelpayouts, пересобрать `scripts/build_geo.py`), хотя бы один из
них — город остановки, запас на переезд `HOP_MIN_GAP_MIN` (4 ч). Соседи добавляются
в сбор (`planner.collect_view`) — оценка растёт; на карточке `stops[].departFrom`.
Ключ набора городов — города прилёта (первый — реальный город вылета).

## GraphQL Data API (`core/graphql_api.py`)

`prices_one_way` — все билеты на дату с сегментами, пересадками и багажом
(REST `prices_for_dates` отдаёт один самый дешёвый билет на направление).
Работают `город → ANY` и `ANY → город`, страна→город, город→страна; ANY→страна,
страна→страна и запрос без обоих концов — ошибка 400 (хотя бы один конец — город).
Лимит 400 на страницу, 60 запросов/мин, `offset` ≲ 14 800. `grouping` по умолчанию
`DATES` (один самый дешёвый билет на дату!) — для всех билетов нужен `NONE`; любая
группировка = минимум на группу (docs/travelpayouts/FINDINGS.md).
Без ценового коридора первые страницы ANY-запроса — дешёвая ближняя Россия/СНГ,
поэтому `value_min/value_max` обязательны для разумного объёма. `trip_duration`
приходит 0 — длительность считаем по сегментам. На проде серии получает коллектор
(`COLLECTOR_URL`, свежесть сутки, озеро в S3); без него планировщик ходит в GraphQL
сам и кэширует серии в `ticket_cache` (`planner/src/graphql.rs`). Проверить руками:

```sh
poetry run python scripts/fetch_tickets.py MOW SEL 2026-10-15
poetry run python scripts/fetch_tickets.py MOW - 2026-10-15 --min 20000 --max 40000   # MOW → ANY
```

Тесты: планировщик — `cd planner && cargo test` (юнит-тесты модулей + сквозной
`tests/e2e.rs` с моком коллектора); коллектор/краулер — `PYTHONPATH=. poetry run pytest -q`
(venv worktree может быть пустым — тогда python из venv основного checkout). План
перехода планировщика на GraphQL — `docs/PLANNER_V2.md`.

## Планировщик на Rust (`planner/`, 2026-09-30)

Сервис `flights-planner` (axum + tokio, rusqlite, arrow/parquet, reqwest) повторяет
контракт и семантику Python-планировщика один в один: те же `/api/*`, та же SQLite
`data/flights.db` (jobs, quotes, ticket_cache), те же Parquet-файлы `plan_flights/<job>.parquet`
(Python-планировщик `api/` + `storage/` + планировочные модули `core/` удалены 30.09.2026;
в `core/` остались только общие с коллектором `graphql_api`, `collector_client`,
`series_arrow`). Модули: `stops` (остановки, окна, оценка), `collect`
(сбор серий, hidden-city), `search` (ленивый A*, компактный результат, маршруты
наборов), `overview` (наборы городов, динамика по префиксам), `planquery` (фильтры,
хэши `collect_key`/`view_key` — те же sha1, что у Python), `collector` (клиент коллектора,
Arrow IPC), `graphql` (прямой режим), `worker` (джоба, этапы), `api` (HTTP, кэш видов).
Переменные: `FLIGHT_DB`, `COLLECTOR_URL`, `PORT`, `GEO_PATH`, `CITY_NAMES_PATH`,
`AIRPORT_NETWORK_PATH`, `TRAVELPAYOUTS_TOKEN` (без коллектора). Образ —
`deploy/Dockerfile.planner` (multi-stage, `rust:1-slim-bookworm` → `debian:bookworm-slim`).

## Что удалено 2026-09-27 (шаг 6 планировщика v2)

Классический поиск «туда-обратно» (`core/aggregate`, `core/trip_builder`,
`core/collector` REST, `/api/search`, `/api/routes`), граф пересадок и сбор
X→ANY (`core/transfer_graph`, `core/anyscan`, `scripts/scan_any.py`,
`/api/graph/*`), CLI-скрипты в корне, `web/`. Их заменяют GraphQL-сбор и
планировщик v2. Контейнер `flights-scan` на VM (если ещё крутится) работает
из смонтированной копии старого кода — его можно остановить:
`ssh ubuntu@93.77.186.45 'sudo docker rm -f flights-scan'`. Накопленные `quotes`
остаются: из них берётся карта аэропорт→город (`hot.airport_city_map`).
Разбор ссылки Aviasales (`t=`, `static_fare_key`) — `core/linkinfo.py`.
