import { useEffect, useMemo, useRef, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { resolveAirports, type AirportOption } from '../data/airports'
import type { CollectState, Itinerary, PlannerBounds, PlannerFilters, PlannerStop } from '../planner/types'
import { estimatePlan } from '../planner/estimate'
import { runCollection } from '../planner/api'
import { validatePlan } from '../planner/validation'
import { defaultFilters } from '../planner/filtering'
import { buildPlannerQuery, parsePlannerQuery } from '../planner/urlState'
import { getCached, putCached } from '../planner/cache'
import { RouteSkeleton } from '../planner/components/RouteSkeleton'
import { PlanEstimateBar } from '../planner/components/PlanEstimateBar'
import { CollectProgress } from '../planner/components/CollectProgress'
import { FiltersPanel } from '../planner/components/FiltersPanel'
import { RecentSearches } from '../planner/components/RecentSearches'
import { addRecent, loadRecent, removeRecent, type RecentSearch } from '../planner/recentSearches'

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
  // Движковые границы: сколько цепочек строить (самые дешёвые) и потолок цены.
  // Уходят на бэк при сборе; их смена делает собранные данные несвежими.
  const [limit, setLimit] = useState(initial.bounds?.maxResults ?? 100)
  const [maxCost, setMaxCost] = useState<number | null>(initial.bounds?.maxCost ?? null)
  const [recent, setRecent] = useState<RecentSearch[]>(() => loadRecent())
  const cancelRef = useRef<(() => void) | null>(null)
  // Фильтры из ссылки ждут своего сбора; правка маршрута их аннулирует.
  const pendingFilters = useRef<PlannerFilters | null>(initial.filters)

  const estimate = useMemo(() => estimatePlan(stops), [stops])
  const validation = useMemo(() => validatePlan(stops), [stops])
  const bounds = useMemo<PlannerBounds>(() => ({ maxResults: limit, maxCost }), [limit, maxCost])

  // URL всегда отражает то, что на экране: маршрут + активные фильтры.
  useEffect(() => {
    const activeFilters = collect.status === 'ready' ? filters : null
    setSearchParams(buildPlannerQuery(stops, activeFilters, bounds), { replace: true })
  }, [stops, filters, collect.status, bounds, setSearchParams])

  // Дорезолв названий/флагов: из ссылки приходят только коды городов (city пустой).
  // Коды в URL/кэше не меняются, поэтому подстановка карточек не трогает ни URL,
  // ни ключ кэша.
  useEffect(() => {
    const codes = stops
      .filter((s) => s.kind === 'cities')
      .flatMap((s) => s.airports)
      .filter((a) => a.code && !a.city)
      .map((a) => a.code)
    if (codes.length === 0) return
    let cancelled = false
    resolveAirports(codes).then((map) => {
      if (cancelled || map.size === 0) return
      setStops((prev) =>
        prev.map((s) =>
          s.kind !== 'cities'
            ? s
            : { ...s, airports: s.airports.map((a) => (!a.city && map.has(a.code) ? map.get(a.code)! : a)) },
        ),
      )
    })
    return () => {
      cancelled = true
    }
  }, [stops])

  // Гидрация из кэша: при заходе по ссылке или смене маршрута показываем уже
  // собранные данные вместо повторного сбора. Свежий сбор запускается кнопкой.
  useEffect(() => {
    const cached = getCached(stops, bounds)
    if (!cached) return // нет данных под маршрут+границы — оставляем как есть (idle → приглашение собрать)
    const fromUrl = pendingFilters.current
    pendingFilters.current = null
    setCollect({ status: 'ready', itineraries: cached.itineraries, collectedAt: cached.collectedAt })
    setFilters(
      fromUrl && filtersFitStops(fromUrl, stops.length)
        ? fromUrl
        : defaultFilters(cached.itineraries, stops.length),
    )
  }, [stops, bounds])

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

  // Восстановить маршрут из истории: id остановок регенерим (они не переносятся).
  function restoreRecent(saved: PlannerStop[]) {
    cancelRef.current?.()
    cancelRef.current = null
    pendingFilters.current = null
    setStops(saved.map((s) => ({ ...s, id: nid() })))
    setCollect({ status: 'idle' })
    setFilters(null)
  }
  function dropRecent(key: string) {
    setRecent(removeRecent(key))
  }

  // Собрать данные под текущий маршрут (первичная загрузка и «свежие данные» —
  // это одно и то же действие). Результат кэшируется с отметкой времени.
  function collectFresh() {
    if (!validation.ok) return
    cancelRef.current?.()
    const prevFilters = filters // при пересборе сохраняем уже настроенные фильтры
    // Валидный запрос уходит в сбор — фиксируем его в истории.
    setRecent(addRecent(stops, Date.now()))
    setCollect({ status: 'collecting', progress: 0, total: estimate.requests })
    cancelRef.current = runCollection(
      stops,
      bounds,
      (progress, total) => setCollect({ status: 'collecting', progress, total }),
      (itineraries: Itinerary[]) => {
        cancelRef.current = null
        const collectedAt = new Date().toISOString()
        putCached(stops, bounds, itineraries, collectedAt)
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
      (message: string) => {
        cancelRef.current = null
        setCollect({ status: 'error', message })
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

        {/* Движковые границы — видны всегда, рядом с загрузкой данных. Их смена
            делает собранные данные несвежими (нужен пересбор), т.к. результат
            строится на бэке под эти границы. */}
        <label className="pl-limit-row" title="Сколько самых дешёвых маршрутов строить (движковый потолок)">
          Максимум маршрутов:{' '}
          <input
            type="number"
            min={1}
            max={100000}
            step={100}
            value={limit}
            onChange={(e) => {
              // Всегда конечное число: безлимит (None) бэк отклоняет — движок не
              // должен уходить в неограниченный перебор.
              setLimit(Math.max(1, Math.min(100000, Number(e.target.value) || 1)))
              resetCollected()
            }}
          />
        </label>
        <label className="pl-limit-row" title="Верхняя граница суммарной цены маршрута (пусто — без ограничения). Отсекает бесперспективные направления ещё при сборе.">
          Максимум цены:{' '}
          <input
            type="number"
            min={0}
            step={5000}
            value={maxCost ?? ''}
            placeholder="без лимита"
            onChange={(e) => {
              const raw = e.target.value
              setMaxCost(raw === '' ? null : Math.max(0, Number(raw) || 0))
              resetCollected()
            }}
          />
        </label>
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
        <FiltersPanel stops={stops} itineraries={collect.itineraries} filters={filters} onChange={setFilters} limit={limit} />
      )}
    </>
  )
}
