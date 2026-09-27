import { useEffect, useMemo, useState } from 'react'
import { Link, Navigate, useParams, useSearchParams } from 'react-router-dom'
import { plural } from '../lib/format'
import { fetchRoutes, type RoutesPage as RoutesPayload } from '../planner/api'
import { encodeQuery } from '../planner/query'
import { usePlanJob } from '../planner/usePlanJob'
import { useQueryFromUrl } from '../planner/useQueryState'
import { JobShell } from '../planner/components/JobShell'
import { ItineraryCard } from '../planner/components/ItineraryCard'

const PAGE = 50

// Режим маршрутов: цепочки по возрастанию цены, страницами с бэка. При переходе
// из наборов (combos=) — только выбранные наборы, вместе.
export function RoutesPage() {
  const { jobId } = useParams()
  const [sp] = useSearchParams()
  const [query] = useQueryFromUrl()
  const job = usePlanJob(jobId)
  const combos = useMemo(() => (sp.get('combos') || '').split(',').filter(Boolean), [sp])
  if (!jobId || !query) return <Navigate to="/" replace />
  const queryString = encodeQuery(query).toString()
  return (
    <JobShell jobId={jobId} query={query} job={job} title="✈ Маршруты">
      {combos.length > 0 && (
        <div className="pl-combos-bar">
          <span>
            Наборы: {combos.map((c) => (
              <span className="mchip" key={c}>
                {c.split('-').join(' → ')}
              </span>
            ))}
          </span>
          <Link className="btn-ghost" to={`/combos/${jobId}?${queryString}`}>
            ← к наборам
          </Link>
        </div>
      )}
      <RoutesList jobId={jobId} combos={combos} />
    </JobShell>
  )
}

function RoutesList({ jobId, combos }: { jobId: string; combos: string[] }) {
  const [pages, setPages] = useState<RoutesPayload[]>([])
  const [loading, setLoading] = useState(false)
  const key = combos.join(',')

  const load = async (offset: number, reset: boolean) => {
    setLoading(true)
    try {
      const page = await fetchRoutes(jobId, offset, PAGE, combos)
      setPages((prev) => (reset ? [page] : [...prev, page]))
    } finally {
      setLoading(false)
    }
  }
  useEffect(() => {
    void load(0, true)
  }, [jobId, key]) // eslint-disable-line react-hooks/exhaustive-deps

  const first = pages[0]
  const items = pages.flatMap((p) => p.items)
  if (!first) return <div className="empty">{loading ? 'Загружаем маршруты…' : 'Нет данных.'}</div>
  if (first.status !== 'ok' || first.total === 0) {
    return <div className="empty">Маршрутов нет — ослабьте фильтры, поднимите потолок цены или лимит маршрутов.</div>
  }
  return (
    <div className="pl-results">
      <div className="count">
        <b>{first.total.toLocaleString('ru-RU')}</b> {plural(first.total, 'маршрут', 'маршрута', 'маршрутов')}
        {first.total < first.count && <> из {first.count.toLocaleString('ru-RU')} собранных</>} · по возрастанию цены
      </div>
      <div className="cards">
        {items.map((it) => (
          <ItineraryCard key={it.id} it={it} />
        ))}
      </div>
      {items.length < first.total && (
        <div className="pl-pager">
          <button type="button" disabled={loading} onClick={() => load(items.length, false)}>
            {loading ? 'Загружаем…' : `Показать ещё ${Math.min(PAGE, first.total - items.length)} из ${first.total - items.length}`}
          </button>
        </div>
      )}
    </div>
  )
}
