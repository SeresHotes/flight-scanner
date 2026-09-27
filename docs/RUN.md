# Локальный запуск

```
React (Vite, :5173) ──POST /api/plan/run──> FastAPI (:8000) ──> core.planner / core.overview
   /combos, /routes  ──GET  …/combos|routes─>        └──> core.graphql_api (Travelpayouts)
   (Vite проксирует /api → :8000; в проде один origin через Caddy)   └──> storage.hot (SQLite)
```

## Бэкенд (FastAPI)

Зависимости — `pyproject.toml` (poetry). Токен Travelpayouts — `TRAVELPAYOUTS_TOKEN`
в `.env` (нужен для сбора; страницы готовых джоб отдаются и без него).

```sh
poetry install
poetry run uvicorn api.main:app --port 8000 --reload
PYTHONPATH=. poetry run pytest -q
```

Эндпоинты:
- `GET  /api/health` — статус, число котировок и серий в кэше.
- `GET  /api/airports?q=&limit=` — автокомплит городов/аэропортов.
- `POST /api/plan/estimate` — оценка объёма сбора (страниц GraphQL).
- `POST /api/plan/run` — запуск по PlanQuery (`core/planquery`), дедуп по хэшу запроса → `{job_id, mode}`.
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
