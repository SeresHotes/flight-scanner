# Локальный запуск

```
React (Vite, :5173) ──POST /api/plan/run──> planner, Rust/axum (:8000) ──> search (A*) / overview
   /combos, /routes  ──GET  …/combos|routes─>        │  └──> SQLite data/flights.db (джобы, котировки)
   (Vite проксирует /api → :8000;                    └──COLLECTOR_URL──> collector (:8001) ──> GraphQL
    в проде один origin через Caddy)                    очередь app>crawl, Parquet-озеро (S3 или data/lake)
```

## Бэкенд: планировщик (Rust) и коллектор (Python)

Планировщик — крейт `planner/` (`cargo`, Rust ≥ 1.85). Коллектор и краулер — Python,
зависимости в `pyproject.toml` (poetry). Токен Travelpayouts — `TRAVELPAYOUTS_TOKEN`
в `.env` (нужен коллектору или планировщику в прямом режиме; страницы готовых
джоб отдаются и без него). Планировщик читает `.env` из текущего каталога, поэтому
запускать его удобно из корня репозитория: `core/geo.json`, `core/city_names.json`
и `data/airport_network.json` берутся по относительным путям (переопределяются
`GEO_PATH`, `CITY_NAMES_PATH`, `AIRPORT_NETWORK_PATH`; БД — `FLIGHT_DB`, порт — `PORT`).

```sh
poetry install
# Как в проде: коллектор отдельно, планировщик ходит в него за сериями (docs/COLLECTOR.md).
poetry run uvicorn collector.main:app --port 8001            # озеро без S3_* — в data/lake
COLLECTOR_URL=http://localhost:8001 cargo run --release --manifest-path planner/Cargo.toml
# Фоновый сборщик (по желанию; ест квоту источника): CRAWL_* — crawler/config.py
COLLECTOR_URL=http://localhost:8001 CRAWL_HORIZON_DAYS=3 poetry run python -m crawler.main
# Без коллектора: планировщик сам ходит в GraphQL и кэширует серии в SQLite (ticket_cache).
cargo run --release --manifest-path planner/Cargo.toml
# Тесты: планировщик и коллектор/краулер
(cd planner && cargo test)
PYTHONPATH=. poetry run pytest -q
```

Эндпоинты:
- `GET  /api/health` — статус, число котировок, блок `collector` (очередь, серии, объём озера).
- `GET  /api/airports?q=&limit=` — автокомплит городов/аэропортов.
- `POST /api/plan/estimate` — оценка объёма сбора (страниц GraphQL).
- `POST /api/plan/run` — запуск по PlanQuery (`planner/src/planquery.rs`), дедуп по хэшу остановок → `{job_id, mode}`.
- `GET  /api/plan/jobs/{id}` — прогресс (этапы fetch → build → combos), по готовности `summary`.
- `GET  /api/plan/jobs/{id}/combos?sort&offset&limit` — наборы городов.
- `GET  /api/plan/jobs/{id}/routes?offset&limit&combos=` — страница маршрутов с полными сегментами.
- `POST /api/jobs/rescue` — сброс зависших джоб.

Справочник аэропортов `data/airport_network.json` строится `build_airport_network.py`
(нужен `datasets`, офлайн). Проверить источник руками: `scripts/fetch_tickets.py MOW SEL 2026-10-15`.

## Фронтенд (React)

```sh
cd frontend
npm install
npm run dev        # http://localhost:5173, /api проксируется на :8000
npm run build      # tsc + vite
```

Страницы: `/` (запрос) → `/combos/:job` → `/routes/:job`; запрос целиком в URL.
