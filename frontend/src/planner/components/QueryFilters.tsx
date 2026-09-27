import { plural } from '../../lib/format'
import type { PlannerStop } from '../types'
import type { CityQuery, LegQuery, PlanQuery } from '../query'
import { DEFAULT_MAX_RESULTS, MAX_RESULTS } from '../query'
import { pointLabel } from '../validation'
import { CityFilterCard } from './CityFilterCard'
import { TransitionFilterCard } from './TransitionFilterCard'
import { TripLengthFilter } from './TripLengthFilter'

function cityName(s: PlannerStop): string {
  if (s.kind === 'any') return 'любой город'
  if (s.airports.length === 0) return 'город'
  return s.airports.map((a) => a.city || a.code).join(' / ')
}

// Единая панель фильтров запроса: город → переход → город → …, длина поездки,
// потолок цены и число маршрутов. Задаётся до загрузки; на страницах результата
// правка запускает новый сбор (данные — из кэша, считает бэк).
export function QueryFilters({ query, onChange }: { query: PlanQuery; onChange: (q: PlanQuery) => void }) {
  const { stops } = query
  const patchCity = (i: number, patch: Partial<CityQuery>) =>
    onChange({ ...query, cities: query.cities.map((c, k) => (k === i ? { ...c, ...patch } : c)) })
  const patchLeg = (i: number, patch: Partial<LegQuery>) =>
    onChange({ ...query, legs: query.legs.map((l, k) => (k === i ? { ...l, ...patch } : l)) })

  return (
    <div className="pl-filters">
      {stops.map((s, i) => (
        <div key={s.id} style={{ display: 'contents' }}>
          <CityFilterCard
            point={pointLabel(i)}
            name={cityName(s)}
            filter={query.cities[i]}
            endpoint={i === 0 || i === stops.length - 1}
            onChange={(patch) => patchCity(i, patch)}
          />
          {i < stops.length - 1 && (
            <TransitionFilterCard
              title={`${pointLabel(i)} → ${pointLabel(i + 1)}`}
              filter={query.legs[i]}
              onChange={(patch) => patchLeg(i, patch)}
            />
          )}
        </div>
      ))}
      <TripLengthFilter value={query.tripLength} onChange={(tripLength) => onChange({ ...query, tripLength })} />

      <div className="pl-frow pl-bounds">
        <div className="pl-ficon">⚙</div>
        <div className="pl-fname">Границы</div>
        <div className="pl-fcell">
          <label className="pl-limit-row" title="Верхняя граница суммарной цены маршрута. Отсекает дорогие направления ещё при сборе и сужает загрузку «любых» городов.">
            Максимум цены, ₽:{' '}
            <input
              type="number"
              min={0}
              step={5000}
              value={query.maxCost ?? ''}
              placeholder="без лимита"
              onChange={(e) => onChange({ ...query, maxCost: e.target.value === '' ? null : Math.max(0, Number(e.target.value) || 0) })}
            />
          </label>
        </div>
        <div className="pl-fcell">
          <label className="pl-limit-row" title="Сколько самых дешёвых маршрутов строить. Наборы городов считаются без этого лимита.">
            Максимум маршрутов:{' '}
            <input
              type="number"
              min={1}
              max={MAX_RESULTS}
              step={100}
              value={query.maxResults}
              onChange={(e) =>
                onChange({ ...query, maxResults: Math.max(1, Math.min(MAX_RESULTS, Number(e.target.value) || DEFAULT_MAX_RESULTS)) })
              }
            />
            <span className="pl-flabel"> {plural(query.maxResults, 'маршрут', 'маршрута', 'маршрутов')}</span>
          </label>
        </div>
      </div>
    </div>
  )
}
