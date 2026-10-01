import { useEffect, useMemo, useState } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router-dom'
import type { AirportOption } from '../data/airports'
import { estimatePlan } from '../planner/estimate'
import { fetchEstimate, runPlan } from '../planner/api'
import { validatePlan } from '../planner/validation'
import { encodeQuery, fitFilters, queryMode, type PlanQuery } from '../planner/query'
import type { PlannerEstimate } from '../planner/types'
import { nextStopId, useQueryFromUrl } from '../planner/useQueryState'
import { PlanEstimateBar } from '../planner/components/PlanEstimateBar'
import { QueryEditor } from '../planner/components/QueryEditor'
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
  })
}

// Главная страница: скелет маршрута + единые фильтры + оценка + «Найти».
// Всё считает бэк: по кнопке уходим на страницу наборов городов (если где-то
// «любой» или несколько городов) или сразу на страницу маршрутов.
export function PlanPage() {
  const navigate = useNavigate()
  const [, setSearchParams] = useSearchParams()
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

  // Запрос целиком живёт в URL: любая правка остановок, дат и условий сразу
  // отражается в адресной строке (replace — без засорения истории), так что
  // ссылку можно сохранить/переслать и открыть с теми же фильтрами.
  const encoded = encodeQuery(query).toString()
  useEffect(() => {
    setSearchParams(new URLSearchParams(encoded), { replace: true })
  }, [encoded, setSearchParams])

  const validation = useMemo(() => validatePlan(query.stops), [query.stops])
  // Оценка: мгновенно клиентская (без кэша), затем уточнённая с бэка — сколько
  // страниц уже в кэше серий и сколько реально пойдёт в источник.
  const clientEstimate = useMemo(() => estimatePlan(query.stops), [query.stops])
  const [serverEstimate, setServerEstimate] = useState<PlannerEstimate | null>(null)
  useEffect(() => {
    setServerEstimate(null)
    if (!validation.ok) return
    let cancelled = false
    const timer = setTimeout(() => {
      fetchEstimate(query)
        .then((est) => {
          if (!cancelled && !est.status) setServerEstimate(est)
        })
        .catch(() => undefined)
    }, 400)
    return () => {
      cancelled = true
      clearTimeout(timer)
    }
  }, [encoded, validation.ok]) // eslint-disable-line react-hooks/exhaustive-deps
  const estimate = serverEstimate ?? clientEstimate


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
        <div className="sub">
          Города, даты и условия одним списком — бэк соберёт билеты и покажет варианты.{' '}
          <Link to="/dynamics" className="dyn-back">📈 Динамика цены →</Link>
        </div>
      </header>

      <div className="searchform">
        <QueryEditor query={query} validation={validation} onChange={setQuery} />
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
        onRestore={(stops) => setQuery(fitFilters({ ...query, stops: stops.map((s) => ({ ...s, id: nextStopId() })) }))}
        onRemove={(key) => setRecent(removeRecent(key))}
      />
    </>
  )
}
