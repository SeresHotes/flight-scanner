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
- Фронтенд: Vite + React + TS (`frontend/`). Новая страница-планировщик — `frontend/src/planner/`.

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

## ANY → ANY, граф пересадок и hidden-city

Главная продуктовая задача — **hidden-city**: билет A→H→X дешевле прямого A→H,
выходим в H. Для этого собираем X → ANY по всем городам и строим граф пересадок.

**Сбор** (`core/anyscan.py`, CLI `scripts/scan_any.py`): обход в ширину от MOW,
для каждого города — `X → ANY` по дням в двух режимах (с пересадками и только
прямые), даты «неделя через месяц» (7 дней × 3 месяца по умолчанию, 42 запроса
на город, ≈30 с). Кэш под-запросов `fetch_cache` (TTL 24 ч), котировки `quotes`
и озеро `data/lake` — те же, что у воркера `api/worker.py`. Состояние обхода —
`data/anyscan_state.json`, сбор можно прервать (Ctrl-C) и продолжить.

```sh
poetry run python scripts/scan_any.py --dry-run            # план
poetry run python scripts/scan_any.py --max-cities 30      # порция; повторный запуск продолжает
poetry run python scripts/scan_any.py --reset --from-date 2026-11-02 --months 2
```

Запускать из основного checkout (данные — в его `data/`); из worktree —
`--db <main>/data/flights.db --lake-root <main>/data/lake --state <main>/data/anyscan_state.json`.

**Сбор на VM** (не зависит от локального соединения): контейнер `flights-scan`
из того же образа api, тот же том данных и `.env` с токеном, перезапуск при
сбое; результат сразу виден проду через `/api/graph/*` (кэш графа 5 мин).
Код берётся не из образа, а из `/opt/flights/scan-src` (rsync ветки, без CI):

```sh
rsync -az --delete --exclude __pycache__ core scripts api storage ubuntu@93.77.186.45:/opt/flights/scan-src/
ssh ubuntu@93.77.186.45 'sudo chown -R 1000:1000 /opt/flights/scan-src; sudo bash -c "
  set -a; source /opt/flights/.env; set +a
  docker rm -f flights-scan 2>/dev/null
  docker run -d --name flights-scan --restart on-failure --env-file /opt/flights/.env \
    -v /opt/flights/data:/app/data -v /opt/flights/scan-src:/app/src:ro -w /app/src \$API_IMAGE \
    python -u scripts/scan_any.py --lake-root /app/data/lake --state /app/data/anyscan_state.json"'
ssh ubuntu@93.77.186.45 'sudo docker logs flights-scan 2>&1 | grep "\[scan\]" | tail'   # прогресс
ssh ubuntu@93.77.186.45 'sudo docker rm -f flights-scan'                                 # стоп; повторный запуск продолжит очередь
```
`--max-cities N` — порция вместо всей очереди. Контейнер не входит в compose и
переживает деплои api. Когда `scripts/` попадёт в образ (`COPY scripts` в
Dockerfile), mount `scan-src` можно не делать.

**Hidden-city в планировщике** (`core/planner.hidden_city_flights`): в список
плеча A→B попадают виртуальные рейсы — билеты A→C с первой пересадкой в B, если
они дешевле обычного A→B того же дня. Цена — всего билета, `transfers=0`,
`destination=B`, прилёт в B — оценка (по прямому A→B из выборки, иначе по
расстоянию), ссылка — на реальный билет A→C, метка `segment.hidden_city`
(финал, цепочка, багаж); на карточке — бейдж «🎯 hidden-city → C» и «≈» у
времени прилёта. Источник — тот же ответ A→ANY: при якорении по A он уже есть,
при якорении по B добавляется запрос A→ANY с пересадками на каждый день и город A
(оценка запросов учитывает, `estimate.ts` синхронизирован). Ограничение REST:
A→ANY отдаёт один самый дешёвый билет на каждый X в день, так что видны только
те C, у которых дешёвый билет идёт через B; полнее — GraphQL (A→страна B).

**Граф** (`core/transfer_graph.py`): вершины — аэропорты, рёбра — сегменты
прямых перелётов из цепочки `link`, отдельно — наблюдённые билеты с пересадками
(цена, дата, багаж). Строится из `quotes` на лету или сохраняется:
`scripts/build_transfer_graph.py` → `data/transfer_graph.json` (~18 МБ на 27k котировок).

**Запросы**: CLI `scripts/hidden_city.py MOW BJS` (через H дешевле прямого A→H;
`--same-day` — сравнивать только с прямым того же дня), `scripts/hidden_city.py MOW HRB --transfers`
(какие пересадки наблюдались по направлению). То же в API:
`GET /api/graph/hidden-city?origin=MOW&via=BJS`, `GET /api/graph/transfers?origin=MOW&destination=HRB`,
`GET /api/graph/stats` (граф кэшируется в памяти 5 мин).

**Формат `link`** (`core/linkinfo.py`):
`/search/MOW0305CAN1?t=CZ17778429001777983900002050SVOWUHCAN_<hash>_25387&static_fare_key=TY%7CP1%7CH1%7CL1_1_23%7CCH1%7CR1%7CTBC1&…`
- `t=` = `<авиакомпания 2><вылет unix 10><прилёт 10><длительность мин 6><цепочка аэропортов по 3><_hash_цена>`;
  `SVOWUHCAN` = SVO→WUH→CAN. Вылет — честный UTC; прилётный таймстемп сдвинут на
  пояс прилёта — не опираемся, длительность берём из своего поля.
- `static_fare_key` (может отсутствовать у старых ссылок): `L0` — без багажа,
  `L1_<мест>_<кг>` — включён; `H1` — ручная кладь; `CH`/`R` — обмен/возврат (не проверено).
- Полная ссылка: `https://www.aviasales.ru` + `link`.
