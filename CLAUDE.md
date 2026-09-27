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

**Сбор на VM** (не зависит от локального соединения): отдельный контейнер из
того же образа api, тот же том данных и `.env` с токеном, перезапуск при сбое;
результат сразу виден проду через `/api/graph/*` (кэш графа 5 мин).

```sh
ssh ubuntu@93.77.186.45
set -a; source /opt/flights/.env; set +a
sudo docker run -d --name flights-scan --restart on-failure   --env-file /opt/flights/.env -v /opt/flights/data:/app/data "$API_IMAGE"   python -u scripts/scan_any.py                    # весь обход; --max-cities N — порция
sudo docker logs -f flights-scan | grep '\[scan\]'  # прогресс; состояние — /opt/flights/data/anyscan_state.json
sudo docker rm -f flights-scan                     # остановить (повторный запуск продолжит очередь)
```
Контейнер `flights-scan` не входит в compose и переживает деплои api; после
обновления образа перезапустить его вручную, чтобы подхватить новый код.

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
