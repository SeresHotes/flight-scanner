import { useEffect, useMemo, useRef, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { type AirportOption } from '../data/airports'
import type { CollectState, Itinerary, PlannerFilters, PlannerStop } from '../planner/types'
import { estimatePlan, runMockCollection } from '../planner/mock'
import { validatePlan } from '../planner/validation'
import { defaultFilters } from '../planner/filtering'
import { buildPlannerQuery, parsePlannerQuery } from '../planner/urlState'
import { getCached, putCached } from '../planner/cache'
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

function formatCollectedAt(iso: string): string {
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return ''
  return d.toLocaleString('ru-RU', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' })
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
  // фильтры → отложенно применяем при гидрации из кэша или после сбора.
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

  // Гидрация из кэша: при заходе по ссылке или смене маршрута показываем уже
  // собранные данные вместо повторного сбора. Свежий сбор запускается кнопкой.
  useEffect(() => {
    const cached = getCached(stops)
    if (!cached) return // нет данных под маршрут — оставляем как есть (idle → приглашение собрать)
    const fromUrl = pendingFilters.current
    pendingFilters.current = null
    setCollect({ status: 'ready', itineraries: cached.itineraries, collectedAt: cached.collectedAt })
    setFilters(
      fromUrl && filtersFitStops(fromUrl, stops.length)
        ? fromUrl
        : defaultFilters(cached.itineraries, stops.length),
    )
  }, [stops])

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

  // Собрать данные под текущий маршрут (первичная загрузка и «свежие данные» —
  // это одно и то же действие). Результат кэшируется с отметкой времени.
  function collectFresh() {
    cancelRef.current?.()
    const prevFilters = filters // при пересборе сохраняем уже настроенные фильтры
    setCollect({ status: 'collecting', progress: 0, total: estimate.requests })
    cancelRef.current = runMockCollection(
      stops,
      (progress, total) => setCollect({ status: 'collecting', progress, total }),
      (itineraries: Itinerary[]) => {
        cancelRef.current = null
        const collectedAt = new Date().toISOString()
        putCached(stops, itineraries, collectedAt)
        setCollect({ status: 'ready', itineraries, collectedAt })
        // Приоритет фильтров: из ссылки (одноразово) → уже настроенные → дефолтные.
        const fromUrl = pendingFilters.current
        pendingFilters.current = null
        const reuse =
          (fromUrl && filtersFitStops(fromUrl, stops.length) && fromUrl) ||
          (prevFilters && filtersFitStops(prevFilters, stops.length) && prevFilters) ||
          defaultFilters(itineraries, stops.length)
        setFilters(reuse)
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
        {/* Пока данных нет — оценка объёма и кнопка первичной загрузки. */}
        {collect.status !== 'ready' && (
          <PlanEstimateBar
            estimate={estimate}
            validation={validation}
            disabled={!validation.ok || collect.status === 'collecting'}
            onLoad={collectFresh}
          />
        )}

        {/* Данные уже собраны — показываем их возраст и даём перезагрузить свежие. */}
        {collect.status === 'ready' && (
          <div className="pl-dataline">
            <span className="pl-data-age">
              📦 Данные собраны <b>{formatCollectedAt(collect.collectedAt)}</b>
            </span>
            <button
              type="button"
              className="btn-ghost"
              disabled={!validation.ok}
              onClick={collectFresh}
              title="Собрать данные заново на текущий момент"
            >
              ↻ Загрузить свежие данные
            </button>
          </div>
        )}
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
