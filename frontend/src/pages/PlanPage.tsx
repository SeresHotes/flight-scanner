import { useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import type { AirportOption } from '../data/airports'
import type { PlannerStop } from '../planner/types'
import { estimatePlan } from '../planner/estimate'
import { runPlan } from '../planner/api'
import { validatePlan } from '../planner/validation'
import { DEFAULT_MAX_RESULTS, encodeQuery, fitFilters, queryMode, type PlanQuery } from '../planner/query'
import { nextStopId, useQueryFromUrl } from '../planner/useQueryState'
import { RouteSkeleton } from '../planner/components/RouteSkeleton'
import { PlanEstimateBar } from '../planner/components/PlanEstimateBar'
import { QueryFilters } from '../planner/components/QueryFilters'
import { RecentSearches } from '../planner/components/RecentSearches'
import { addRecent, loadRecent, removeRecent, type RecentSearch } from '../planner/recentSearches'

// Демо-города (названия — на английском, как в справочнике аэропортов/автокомплите).
const MOW: AirportOption = { code: 'MOW', city: 'Moscow', flag: '🇷🇺', label: 'Moscow (MOW)' }
const IST: AirportOption = { code: 'IST', city: 'Istanbul', flag: '🇹🇷', label: 'Istanbul (IST)' }
const DXB: AirportOption = { code: 'DXB', city: 'Dubai', flag: '🇦🇪', label: 'Dubai (DXB)' }
const ICN: AirportOption = { code: 'ICN', city: 'Seoul', flag: '🇰🇷', label: 'Seoul (ICN)' }

function initialQuery(): PlanQuery {
  return fitFilters({
    stops: [
      { id: nextStopId(), kind: 'cities', airports: [MOW], window: ['', ''] },
      { id: nextStopId(), kind: 'cities', airports: [IST, DXB], window: ['2026-11-06', '2026-11-10'] },
      { id: nextStopId(), kind: 'cities', airports: [ICN], window: ['', ''] },
    ],
    cities: [],
    legs: [],
    tripLength: [0, null],
    maxCost: null,
    maxResults: DEFAULT_MAX_RESULTS,
  })
}

// Главная страница: скелет маршрута + единые фильтры + оценка + «Найти».
// Всё считает бэк: по кнопке уходим на страницу наборов городов (если где-то
// «любой» или несколько городов) или сразу на страницу маршрутов.
export function PlanPage() {
  const navigate = useNavigate()
  const [fromUrl, setFromUrl] = useQueryFromUrl()
  const [local, setLocal] = useState<PlanQuery | null>(null)
  const query = local ?? fromUrl ?? initialQuery()
  const setQuery = (q: PlanQuery) => {
    setLocal(q)
    setFromUrl(q)
  }
  const [recent, setRecent] = useState<RecentSearch[]>(() => loadRecent())
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const estimate = useMemo(() => estimatePlan(query.stops), [query.stops])
  const validation = useMemo(() => validatePlan(query.stops), [query.stops])

  const setStops = (stops: PlannerStop[]) => setQuery(fitFilters({ ...query, stops }))
  const updateStop = (i: number, patch: Partial<PlannerStop>) =>
    setStops(query.stops.map((s, k) => (k === i ? { ...s, ...patch } : s)))
  const addStop = () => {
    const next = [...query.stops]
    next.splice(Math.max(1, next.length - 1), 0, { id: nextStopId(), kind: 'cities', airports: [], window: ['', ''] })
    setStops(next)
  }
  const removeStop = (i: number) => {
    if (query.stops.length > 2) setStops(query.stops.filter((_, k) => k !== i))
  }

  async function run() {
    if (!validation.ok || busy) return
    setBusy(true)
    setError(null)
    try {
      setRecent(addRecent(query.stops, Date.now()))
      const res = await runPlan(query)
      if (!res.job_id) {
        setError(res.message || 'Не удалось запустить сбор.')
        return
      }
      const mode = res.mode ?? queryMode(query)
      navigate(`/${mode}/${res.job_id}?${encodeQuery(query)}`)
    } catch {
      setError('Не удалось связаться с сервером.')
    } finally {
      setBusy(false)
    }
  }

  return (
    <>
      <header className="hero">
        <h1>🧭 Планировщик маршрута</h1>
        <div className="sub">Города и даты, фильтры на каждое плечо — бэк соберёт билеты и покажет варианты.</div>
      </header>

      <div className="searchform">
        <RouteSkeleton stops={query.stops} valid={validation.stopValid} onUpdate={updateStop} onAdd={addStop} onRemove={removeStop} />
        <QueryFilters query={query} onChange={setQuery} />
        <PlanEstimateBar
          estimate={estimate}
          validation={validation}
          disabled={!validation.ok || busy}
          label={busy ? 'Запускаем…' : queryMode(query) === 'combos' ? 'Найти наборы городов →' : 'Найти маршруты →'}
          onLoad={run}
        />
        {error && (
          <div className="backend-note">
            <div className="bn-title">Не удалось запустить</div>
            <div>{error}</div>
          </div>
        )}
      </div>

      <RecentSearches
        items={recent}
        onRestore={(stops) => setStops(stops.map((s) => ({ ...s, id: nextStopId() })))}
        onRemove={(key) => setRecent(removeRecent(key))}
      />
    </>
  )
}
