import { useEffect, useMemo, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import { type AirportOption } from '../data/airports'
import type { CollectState, Itinerary, PlannerFilters, PlannerStop } from '../planner/types'
import { estimatePlan, runMockCollection } from '../planner/mock'
import { validatePlan } from '../planner/validation'
import { defaultFilters } from '../planner/filtering'
import { RouteSkeleton } from '../planner/components/RouteSkeleton'
import { PlanEstimateBar } from '../planner/components/PlanEstimateBar'
import { CollectProgress } from '../planner/components/CollectProgress'
import { FiltersPanel } from '../planner/components/FiltersPanel'
import { RecentSearches } from '../planner/components/RecentSearches'
import { addRecent, loadRecent, removeRecent, type RecentSearch } from '../planner/recentSearches'

let stopSeq = 0
const nid = () => `s${stopSeq++}`

// Демо-города (названия — на английском, как в справочнике аэропортов/автокомплите).
const MOW: AirportOption = { code: 'MOW', city: 'Moscow', flag: '🇷🇺', label: 'Moscow (MOW)' }
const IST: AirportOption = { code: 'IST', city: 'Istanbul', flag: '🇹🇷', label: 'Istanbul (IST)' }
const DXB: AirportOption = { code: 'DXB', city: 'Dubai', flag: '🇦🇪', label: 'Dubai (DXB)' }
const ICN: AirportOption = { code: 'ICN', city: 'Seoul', flag: '🇰🇷', label: 'Seoul (ICN)' }

function initialStops(): PlannerStop[] {
  return [
    // Концы — конкретные города, без окна дат (выводится из соседних).
    { id: nid(), kind: 'cities', airports: [MOW], window: ['', ''] },
    // Промежуточная — несколько городов на выбор + диапазон дат.
    { id: nid(), kind: 'cities', airports: [IST, DXB], window: ['2026-11-06', '2026-11-10'] },
    { id: nid(), kind: 'cities', airports: [ICN], window: ['', ''] },
  ]
}

// Альтернативная страница-планировщик: цепочка остановок с окнами дат,
// оценка объёма + загрузка, затем детальные фильтры и карточки маршрутов.
export function PlannerPage() {
  const [stops, setStops] = useState<PlannerStop[]>(initialStops)
  const [collect, setCollect] = useState<CollectState>({ status: 'idle' })
  const [filters, setFilters] = useState<PlannerFilters | null>(null)
  const [recent, setRecent] = useState<RecentSearch[]>(() => loadRecent())
  const cancelRef = useRef<(() => void) | null>(null)

  const estimate = useMemo(() => estimatePlan(stops), [stops])
  const validation = useMemo(() => validatePlan(stops), [stops])

  // Любая правка маршрута сбрасывает собранное — данные надо перезагрузить.
  function resetCollected() {
    cancelRef.current?.()
    cancelRef.current = null
    setCollect({ status: 'idle' })
    setFilters(null)
  }

  function updateStop(index: number, patch: Partial<PlannerStop>) {
    setStops((prev) => prev.map((s, i) => (i === index ? { ...s, ...patch } : s)))
    resetCollected()
  }
  function addStop() {
    setStops((prev) => {
      const insertAt = Math.max(1, prev.length - 1) // новую — перед последней (концы фиксированы)
      const next = [...prev]
      next.splice(insertAt, 0, { id: nid(), kind: 'cities', airports: [], window: ['', ''] })
      return next
    })
    resetCollected()
  }
  function removeStop(index: number) {
    setStops((prev) => (prev.length <= 2 ? prev : prev.filter((_, i) => i !== index)))
    resetCollected()
  }

  // Восстановить маршрут из истории: id остановок регенерим (они не переносятся).
  function restoreRecent(saved: PlannerStop[]) {
    cancelRef.current?.()
    cancelRef.current = null
    setStops(saved.map((s) => ({ ...s, id: nid() })))
    setCollect({ status: 'idle' })
    setFilters(null)
  }
  function dropRecent(key: string) {
    setRecent(removeRecent(key))
  }

  function load() {
    if (!validation.ok) return
    cancelRef.current?.()
    setFilters(null)
    // Валидный запрос уходит в сбор — фиксируем его в истории.
    setRecent(addRecent(stops, Date.now()))
    setCollect({ status: 'collecting', progress: 0, total: estimate.requests })
    cancelRef.current = runMockCollection(
      stops,
      (progress, total) => setCollect({ status: 'collecting', progress, total }),
      (itineraries: Itinerary[]) => {
        cancelRef.current = null
        setCollect({ status: 'ready', itineraries })
        setFilters(defaultFilters(itineraries, stops.length))
      },
    )
  }

  useEffect(() => () => cancelRef.current?.(), [])

  return (
    <>
      <header className="hero">
        <h1>🧭 Планировщик маршрута</h1>
        <div className="sub">
          Соберите цепочку городов с окнами дат, загрузите данные и отфильтруйте варианты.
        </div>
      </header>

      <div className="backlink">
        <Link to="/">← классический поиск «туда-обратно»</Link>
      </div>

      <div className="searchform">
        <RouteSkeleton
          stops={stops}
          valid={validation.stopValid}
          onUpdate={updateStop}
          onAdd={addStop}
          onRemove={removeStop}
        />
        <PlanEstimateBar
          estimate={estimate}
          validation={validation}
          disabled={!validation.ok || collect.status === 'collecting'}
          onLoad={load}
        />
      </div>

      <RecentSearches items={recent} onRestore={restoreRecent} onRemove={dropRecent} />

      {collect.status === 'collecting' && (
        <CollectProgress progress={collect.progress} total={collect.total} />
      )}

      {collect.status === 'error' && (
        <div className="backend-note">
          <div className="bn-title">Ошибка сбора</div>
          <div>{collect.message}</div>
        </div>
      )}

      {collect.status === 'ready' && filters && (
        <FiltersPanel stops={stops} itineraries={collect.itineraries} filters={filters} onChange={setFilters} />
      )}
    </>
  )
}
