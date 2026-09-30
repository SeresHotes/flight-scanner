# Flight Scanner ✈️

Планировщик маршрута из нескольких городов: `A → B → C → …` с окнами дат, «любым»
городом в середине, фильтрами на каждое плечо (пересадки, багаж, hidden-city,
длительность) и по пребыванию в городах. Данные — Aviasales Data API (GraphQL,
Travelpayouts), все вычисления на бэке. Прод: <https://flights.sereshotes.dev>.

- Описание и архитектура — `docs/PLANNER_V2.md`, история — `CHANGELOG.md`.
- Локальный запуск — `docs/RUN.md`; заметки для агентов — `CLAUDE.md`.
- Инфраструктура (Yandex Cloud, Terraform, деплой) — `infra/terraform/`, `deploy/`.

```sh
cp .env.example .env                         # TRAVELPAYOUTS_TOKEN
(cd planner && cargo run --release)          # планировщик (Rust), http://localhost:8000
cd frontend && npm install && npm run dev   # http://localhost:5173
```
