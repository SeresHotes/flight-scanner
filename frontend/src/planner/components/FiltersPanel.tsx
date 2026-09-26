import { useEffect, useMemo, useState } from 'react'
import { plural } from '../../lib/format'
import type { CityFilter, PlanGraph, PlannerFilters, PlannerStop, TransitionFilter } from '../types'
import type { ItinerarySet } from '../compact'
import { pointLabel } from '../validation'
import { applyFilters, computeBounds } from '../filtering'
import { CityFilterCard, type CityOption } from './CityFilterCard'
import { TransitionFilterCard } from './TransitionFilterCard'
import { TripLengthFilter } from './TripLengthFilter'
import { ItineraryCard } from './ItineraryCard'
import { CityCombos } from './CityCombos'
import { graphCitiesByStop } from '../overview'

type View = 'routes' | 'combos'

function cityName(s: PlannerStop): string {
  if (s.kind === 'any') return 'любой город'
  if (s.airports.length === 0) return 'город'
  if (s.airports.length === 1) return s.airports[0].city
  return s.airports.map((a) => a.city).join(' / ')
}

// Зона 3: детальные фильтры (город → переход → город → …), общая длина, результаты.
export function FiltersPanel({
  stops,
  set,
  graph,
  filters,
  onChange,
  limit,
}: {
  stops: PlannerStop[]
  set: ItinerarySet
  graph: PlanGraph | null // весь собранный граф — для режима «наборы городов»
  filters: PlannerFilters
  onChange: (f: PlannerFilters) => void
  limit: number // хард-лимит числа маршрутов (задаётся возле кнопки загрузки)
}) {
  const [view, setView] = useState<View>('routes')
  const bounds = useMemo(() => computeBounds(set, graph), [set, graph])
  const visible = useMemo(() => applyFilters(set, filters), [set, filters])

  // Хард-лимит (задан выше, возле кнопки загрузки) отсекает сколько подходящих
  // цепочек берём в рассмотрение (visible отсортирован по цене). Внутри лимита
  // листаем страницами по PAGE_SIZE.
  const PAGE_SIZE = 100
  const [page, setPage] = useState(0)

  const limited = useMemo(() => visible.subarray(0, Math.max(1, limit)), [visible, limit])
  const pageCount = Math.max(1, Math.ceil(limited.length / PAGE_SIZE))
  const curPage = Math.min(page, pageCount - 1)
  // Объекты Itinerary собираем только для показанной страницы.
  const pageItems = useMemo(
    () => Array.from(limited.subarray(curPage * PAGE_SIZE, curPage * PAGE_SIZE + PAGE_SIZE), (n) => set.materialize(n)),
    [set, limited, curPage],
  )
  useEffect(() => setPage(0), [visible, limit]) // сброс на первую страницу при смене выборки

  // Города, реально встретившиеся на каждой остановке — из них и выбираем в фильтре.
  // Граф шире топ-N цепочек списка, поэтому его города тоже в выборе.
  const cityOptionsByStop = useMemo<CityOption[][]>(() => {
    const graphCities = graph ? graphCitiesByStop(graph) : []
    return stops.map((_, i) => {
      const seen = new Map<string, CityOption>()
      for (let n = 0; i < set.stopCount && n < set.count; n++) {
        const code = set.stopCode(n, i)
        if (!seen.has(code)) seen.set(code, { code, ...set.cityOf(code) })
      }
      for (const code of graphCities[i] ?? []) {
        const ci = graph?.cities[code]
        if (!seen.has(code)) seen.set(code, { code, city: ci?.city ?? code, flag: ci?.flag })
      }
      return Array.from(seen.values())
    })
  }, [stops, set, graph])

  // «Показать маршруты» у набора: фиксируем города на остановках и уходим в список.
  const showComboRoutes = (codes: string[]) => {
    const cities = filters.cities.map((c, i) => ({ ...c, allowedCodes: codes[i] ? [codes[i]] : c.allowedCodes }))
    onChange({ ...filters, cities })
    setView('routes')
  }

  const patchCity = (i: number, patch: Partial<CityFilter>) => {
    const cities = filters.cities.map((c, idx) => (idx === i ? { ...c, ...patch } : c))
    onChange({ ...filters, cities })
  }
  const patchTransition = (i: number, patch: Partial<TransitionFilter>) => {
    const transitions = filters.transitions.map((t, idx) => (idx === i ? { ...t, ...patch } : t))
    onChange({ ...filters, transitions })
  }

  return (
    <div className="pl-results">
      <div className="pl-filters">
        {stops.map((s, i) => (
          <div key={s.id} style={{ display: 'contents' }}>
            <CityFilterCard
              point={pointLabel(i)}
              name={cityName(s)}
              filter={filters.cities[i]}
              cityOptions={cityOptionsByStop[i]}
              stayBounds={bounds.stayDays}
              onChange={(patch) => patchCity(i, patch)}
            />
            {i < stops.length - 1 && (
              <TransitionFilterCard
                title={`${pointLabel(i)} → ${pointLabel(i + 1)}`}
                filter={filters.transitions[i]}
                maxBound={bounds.maxTravelMinutes}
                onChange={(patch) => patchTransition(i, patch)}
              />
            )}
          </div>
        ))}
        <TripLengthFilter
          value={filters.tripLength}
          bounds={bounds.tripLength}
          onChange={(tripLength) => onChange({ ...filters, tripLength })}
        />
      </div>

      <div className="pl-viewtabs" role="tablist">
        <button
          type="button"
          role="tab"
          aria-selected={view === 'routes'}
          className={view === 'routes' ? 'on' : ''}
          onClick={() => setView('routes')}
        >
          ✈ Маршруты
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={view === 'combos'}
          className={view === 'combos' ? 'on' : ''}
          onClick={() => setView('combos')}
          title="Все варианты наборов городов — без лимита маршрутов и цены"
        >
          🗺 Наборы городов
        </button>
      </div>

      {view === 'combos' ? (
        graph ? (
          <CityCombos graph={graph} filters={filters} onShowRoutes={showComboRoutes} />
        ) : (
          <div className="empty">
            Эти данные собраны до появления обзора наборов городов — нажмите «↻ Загрузить свежие данные».
          </div>
        )
      ) : (
        <>
          <div className="count">
            Подходит <b>{visible.length}</b> из {set.count}{' '}
            {plural(set.count, 'маршрута', 'маршрутов', 'маршрутов')}
            {limited.length < visible.length && <> · лимит {limited.length}</>}
          </div>

          {visible.length ? (
            <>
              <div className="cards">
                {pageItems.map((it) => (
                  <ItineraryCard key={it.id} it={it} />
                ))}
              </div>
              {pageCount > 1 && (
                <div className="pl-pager">
                  <button type="button" disabled={curPage === 0} onClick={() => setPage(curPage - 1)}>
                    ← Назад
                  </button>
                  <span>
                    Страница {curPage + 1} из {pageCount}
                  </span>
                  <button type="button" disabled={curPage >= pageCount - 1} onClick={() => setPage(curPage + 1)}>
                    Вперёд →
                  </button>
                </div>
              )}
            </>
          ) : (
            <div className="empty">Под текущие фильтры маршрутов нет — ослабьте условия.</div>
          )}
        </>
      )}
    </div>
  )
}
