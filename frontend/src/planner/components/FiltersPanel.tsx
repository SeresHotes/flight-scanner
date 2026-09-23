import { useMemo } from 'react'
import { plural } from '../../lib/format'
import type { CityFilter, Itinerary, PlannerFilters, PlannerStop, TransitionFilter } from '../types'
import { pointLabel } from '../validation'
import { applyFilters, computeBounds } from '../filtering'
import { CityFilterCard } from './CityFilterCard'
import { TransitionFilterCard } from './TransitionFilterCard'
import { TripLengthFilter } from './TripLengthFilter'
import { ItineraryCard } from './ItineraryCard'

function cityName(s: PlannerStop): string {
  if (s.kind === 'any') return 'любой город'
  if (s.airports.length === 0) return 'город'
  if (s.airports.length === 1) return s.airports[0].city
  return s.airports.map((a) => a.city).join(' / ')
}

// Зона 3: детальные фильтры (город → переход → город → …), общая длина, результаты.
export function FiltersPanel({
  stops,
  itineraries,
  filters,
  onChange,
}: {
  stops: PlannerStop[]
  itineraries: Itinerary[]
  filters: PlannerFilters
  onChange: (f: PlannerFilters) => void
}) {
  const bounds = useMemo(() => computeBounds(itineraries), [itineraries])
  const visible = useMemo(() => applyFilters(itineraries, filters), [itineraries, filters])

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
              title={`${pointLabel(i)} · ${cityName(s)}`}
              filter={filters.cities[i]}
              onChange={(patch) => patchCity(i, patch)}
            />
            {i < stops.length - 1 && (
              <TransitionFilterCard
                title={`Переход ${pointLabel(i)} → ${pointLabel(i + 1)}`}
                filter={filters.transitions[i]}
                maxBound={bounds.maxTravelMinutes}
                onChange={(patch) => patchTransition(i, patch)}
              />
            )}
          </div>
        ))}
      </div>

      <TripLengthFilter
        value={filters.tripLength}
        bounds={bounds.tripLength}
        onChange={(tripLength) => onChange({ ...filters, tripLength })}
      />

      <div className="count">
        Подходит <b>{visible.length}</b> из {itineraries.length}{' '}
        {plural(itineraries.length, 'маршрута', 'маршрутов', 'маршрутов')}
      </div>

      {visible.length ? (
        <div className="cards">
          {visible.map((it) => (
            <ItineraryCard key={it.id} it={it} />
          ))}
        </div>
      ) : (
        <div className="empty">Под текущие фильтры маршрутов нет — ослабьте условия.</div>
      )}
    </div>
  )
}
