# Локальный запуск: бэк + фронт (Фаза 1)

Архитектура после Фазы 1:

```
React (Vite, :5173) ──POST /api/search──> FastAPI (:8000) ──> core.trip_builder
   AirportCombobox  ──GET  /api/airports──>              └──> storage.hot (SQLite)
   (Vite проксирует /api → :8000; в проде один origin через Caddy)
```

## Бэкенд (FastAPI)

Зависимости — в `pyproject.toml` (poetry). Токен Travelpayouts — в `.env`
(нужен только для сбора; для serving из кэша не требуется).

```sh
poetry install                       # или: poetry run pip install fastapi "uvicorn[standard]" pyarrow
poetry run uvicorn api.main:app --port 8000 --reload
```

Эндпоинты:
- `GET  /api/health` — статус + число котировок в SQLite.
- `GET  /api/routes` — что уже доступно: собранные маршруты (`available`) + агрегат
  хранилища (`stored`: всего котировок, число направлений, топ плеч). Питает страницу выбора.
- `GET  /api/airports?q=<строка>&limit=10` — автокомплит A/B из `airport_network.json`.
- `POST /api/search` — **только оценивает, ничего не собирает сам**. Тело `{origin, destination, leg1_dates, leg2_dates}`.
  - Собранный маршрут → `{status:"ok", data:{meta, trips}}`.
  - Не собран, есть даты → `{status:"needs_collection", estimate:{requests, seconds}}` (ждёт подтверждения).
  - Идёт сбор → `{status:"collecting", job_id}`.
  - Нет дат / слишком широко → `{status:"needs_backend", message}`.
- `POST /api/gather` — **явный запуск сбора** (после подтверждения на фронте). Тело как у `/search`.
  Заводит job → `{status:"collecting", job_id}` (**реальный Travelpayouts**).
  (Путь без слова «collect» — иначе блокировщики рекламы режут его как трекер: `ERR_BLOCKED_BY_CLIENT`.)
- `GET  /api/jobs/{id}` — прогресс сбора `{status, progress, total, error}`.

На старте API импортирует существующие выгрузки коллектора из `data/` в SQLite
(`quotes`), а собранный контракт MOW→ICN кэширует в памяти.

## Фронтенд (React)

```sh
cd frontend
npm install
npm run dev        # http://localhost:5173, /api проксируется на :8000
```

## CLI (без изменений)

Скрипты остались рабочими — теперь это тонкие обёртки над `core/`:

```sh
poetry run python collect_flights.py MOW ICN --leg1-dates 2026-10-25 2026-11-05 --leg2-dates 2026-11-13 2026-11-30
poetry run python build_web_data.py --stopover-days 2 14 --max-stay 34   # → web/data.json
```

`core.trip_builder.build_from_config` воспроизводит текущий `web/data.json`
байт-в-байт (проверено) — та же логика, что отдаёт `/api/search`.
