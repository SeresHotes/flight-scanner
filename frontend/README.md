# Flight Scanner — Frontend (React)

Vite + React + TypeScript + TanStack Query. Реализация **Фазы 1** из [`../docs/PLAN.md`](../docs/PLAN.md):
перевод текущего `web/index.html` на React с универсальным интерфейсом поиска
(точки A/B + диапазоны дат плеч) и заделом под подгрузку данных с бэкенда.

## Что уже есть

- **Порт UI 1:1** из `web/index.html`: тёмная тема (CSS перенесён без изменений),
  карточки маршрутов (oneway / round-trip), фильтры по плечам, сортировки, статистика.
- **SearchForm** — форма «что смотрим и за какие даты»: комбобоксы A/B и диапазоны
  дат вылета для каждого плеча. Структурно универсальна.
- **Слой данных через реальный бэк** (`src/data/searchClient.ts`): `POST /api/search`
  возвращает `{status:'ok', data:{meta,trips}}` для собранного маршрута `MOW → ICN`
  и `{status:'needs_backend'}` для прочих A/B. Всё обёрнуто в TanStack Query
  (`src/hooks/useSearch.ts`). Vite проксирует `/api` → FastAPI (`:8000`).
- **A/B разлочены** — `AirportCombobox` с автокомплитом из `GET /api/airports?q=`.

## Текущие ограничения (осознанно)

- Данные собраны только для `MOW → ICN`; прочие маршруты вернут `needs_backend`
  (плашка). Реальный сбор недостающего с прогресс-джобой — Фаза 2 (см. план).
- Диапазоны дат плеч и `min_stay` сейчас применяются на клиенте (как в web-версии);
  server-side сбор именно этих дат — Фаза 2.

## Две страницы (react-router)

- **`/` — SearchPage:** выбор маршрута (A/B + даты плеч) и обзор того, какие данные
  уже собраны (`StoredData` ← `GET /api/routes`: готовые маршруты + агрегат хранилища).
  Сабмит/клик по маршруту → навигация на `/results?...` (параметры в URL, ссылка шарится).
- **`/results` — ResultsPage:** результаты по параметрам из URL — статистика, фильтры,
  карточки (или `BackendNote`, если маршрут ещё не собран). Ссылка «← Изменить поиск».

## Структура

```
src/
  main.tsx            точка входа + QueryClientProvider
  App.tsx             BrowserRouter + маршруты (/ и /results)
  pages/              SearchPage · ResultsPage
  types.ts            контракт данных (== web/data.json == /api/search)
  styles.css          CSS, перенесённый из web/index.html 1:1 (+ блок SearchForm)
  data/
    airports.ts       справочник A/B (пока залочен)
    searchClient.ts   SEAM: локальный data.json ↔ будущий POST /api/search
  hooks/useSearch.ts  обёртка TanStack Query
  lib/
    format.ts         money / fmtDT / durFmt / plural (порт хелперов)
    filters.ts        matchOpt, компараторы, сборка round-trip на лету
  components/
    Header, SearchForm, AirportCombobox, Stats, FilterBar, Controls,
    SegButtons, Legend, TripCard, ResultsView, BackendNote
```

## Команды

```sh
npm install
npm run dev        # http://localhost:5173 (проксирует /api → :8000 под будущий FastAPI)
npm run build      # tsc -b && vite build → dist/
npm run preview    # предпросмотр собранного dist/
npm run typecheck  # tsc без эмита
```

## Обновление датасета

Пока данные статичны — после перегенерации основного пайплайна скопируйте свежий
файл:

```sh
cp ../web/data.json public/data.json
```

В Фазе 2 это заменится ответом бэкенда.
