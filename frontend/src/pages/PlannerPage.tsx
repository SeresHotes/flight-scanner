import { useEffect, useMemo, useRef, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { type AirportOption } from '../data/airports'
import type { CollectState, Itinerary, PlannerFilters, PlannerStop } from '../planner/types'
import { estimatePlan, runMockCollection } from '../planner/mock'
import { validatePlan } from '../planner/validation'
import { defaultFilters } from '../planner/filtering'
import { buildPlannerQuery, parsePlannerQuery } from '../planner/urlState'
import { RouteSkeleton } from '../planner/components/RouteSkeleton'
import { PlanEstimateBar } from '../planner/components/PlanEstimateBar'
import { CollectProgress } from '../planner/components/CollectProgress'
import { FiltersPanel } from '../planner/components/FiltersPanel'

let stopSeq = 0
const nid = () => `s${stopSeq++}`

// Фильтры из URL применимы, только если их форма совпадает с текущим маршрутом.
function filtersFitStops(f: PlannerFilters, stopCount: number): boolean {
  return f.cities.length === stopCount && f.transitions.length === Math.max(0, stopCount - 1)
}

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
  const [searchParams, setSearchParams] = useSearchParams()

  // Разбираем URL один раз при монтировании: маршрут → начальное состояние,
  // фильтры → отложенно применяем после первого сбора (см. load).
  const initial = useMemo(() => parsePlannerQuery(searchParams, nid), [])
  const [stops, setStops] = useState<PlannerStop[]>(() => initial.stops ?? initialStops())
  const [collect, setCollect] = useState<CollectState>({ status: 'idle' })
  const [filters, setFilters] = useState<PlannerFilters | null>(null)
  const cancelRef = useRef<(() => void) | null>(null)
  // Фильтры из ссылки ждут своего сбора; правка маршрута их аннулирует.
  const pendingFilters = useRef<PlannerFilters | null>(initial.filters)

  const estimate = useMemo(() => estimatePlan(stops), [stops])
  const validation = useMemo(() => validatePlan(stops), [stops])

  // URL всегда отражает то, что на экране: маршрут + активные фильтры.
  useEffect(() => {
    const activeFilters = collect.status === 'ready' ? filters : null
    setSearchParams(buildPlannerQuery(stops, activeFilters), { replace: true })
  }, [stops, filters, collect.status, setSearchParams])

  // Любая правка маршрута сбрасывает собранное — данные надо перезагрузить.
  function resetCollected() {
    cancelRef.current?.()
    cancelRef.current = null
    pendingFilters.current = null
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

  function load() {
    cancelRef.current?.()
    setFilters(null)
    setCollect({ status: 'collecting', progress: 0, total: estimate.requests })
    cancelRef.current = runMockCollection(
      stops,
      (progress, total) => setCollect({ status: 'collecting', progress, total }),
      (itineraries: Itinerary[]) => {
        cancelRef.current = null
        setCollect({ status: 'ready', itineraries })
        // Если пришли по ссылке с фильтрами и они подходят под маршрут — берём их,
        // иначе — широкие значения по умолчанию. Фильтры из URL одноразовые.
        const fromUrl = pendingFilters.current
        pendingFilters.current = null
        setFilters(
          fromUrl && filtersFitStops(fromUrl, stops.length)
            ? fromUrl
            : defaultFilters(itineraries, stops.length),
        )
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
