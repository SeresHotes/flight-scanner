# Планировщик маршрута

С 2026-09-27 приложение — только планировщик (v2): единый запрос с фильтрами,
все вычисления на бэке, GraphQL Data API, наборы городов с мультивыбором,
hidden-city. Актуальное описание — `docs/PLANNER_V2.md`; журнал изменений —
`CHANGELOG.md`; локальный запуск — `docs/RUN.md`.

Страницы: `/` (запрос) → `/combos/:job` (наборы городов) → `/routes/:job`
(маршруты). API: `POST /api/plan/run`, `GET /api/plan/jobs/{id}`,
`…/combos`, `…/routes`, `POST /api/plan/estimate`.

История v1 (моки → REST → GraphQL) — в CHANGELOG и закрытых PR #4–#69.
