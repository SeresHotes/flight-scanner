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
- Инфраструктура прод-деплоя (Terraform + cloud-init + registry) — `infra/terraform/`.
- Фронтенд: Vite + React + TS (`frontend/`), только планировщик: `pages/PlanPage`
  (`/`, запрос) → `pages/CombosPage` (`/combos/:job`) → `pages/RoutesPage`
  (`/routes/:job`); общее — `frontend/src/planner/` (запрос `query.ts`, API `api.ts`).

## Планировщик v2 (docs/PLANNER_V2.md)

Единый запрос `core/planquery.PlanQuery` (скелет + фильтры городов/плеч/длины
поездки, `maxCost`, `maxResults`): `POST /api/plan/run` → джоба (дедуп по хэшу
запроса, TTL сутки), `GET /api/plan/jobs/{id}` — прогресс, `…/routes?offset&limit&combos=`
— страница маршрутов (полные сегменты: багаж, пересадки, hidden-city). Фильтры
плеча применяются до перебора, городов — внутри A*, длина поездки — при выдаче.
Старый `POST /api/plan/gather` = тот же запуск с открытыми фильтрами. Наборы
городов считает бэк (`core/overview.py`, numpy, этап «combos» джобы) —
`…/combos?sort&offset&limit`. Фронт ничего не считает: запрос в URL, страницы
результата поллят `/api/plan/jobs/{id}` (прогресс + сводка) и листают
`/combos` и `/routes` с бэка.

## GraphQL Data API (`core/graphql_api.py`)

`prices_one_way` — все билеты на дату с сегментами, пересадками и багажом
(REST `prices_for_dates` отдаёт один самый дешёвый билет на направление).
Работают `город → ANY` и `ANY → город`, страна→город; страна→страна и запрос без
обоих концов — ошибка. Лимит 400 на страницу, 60 запросов/мин, `offset` ≲ 14 800.
Без ценового коридора первые страницы ANY-запроса — дешёвая ближняя Россия/СНГ,
поэтому `value_min/value_max` обязательны для разумного объёма. `trip_duration`
приходит 0 — длительность считаем по сегментам. Серии кэшируются в `ticket_cache`
(TTL 24 ч, `api.worker.make_cached_ticket_fetch`). Проверить руками:

```sh
poetry run python scripts/fetch_tickets.py MOW SEL 2026-10-15
poetry run python scripts/fetch_tickets.py MOW - 2026-10-15 --min 20000 --max 40000   # MOW → ANY
```

Тесты: `PYTHONPATH=. poetry run pytest -q` (venv worktree может быть пустым —
тогда python из venv основного checkout). План перехода планировщика на GraphQL —
`docs/PLANNER_V2.md`.

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
