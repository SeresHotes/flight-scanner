import { useState, type ReactNode } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { runPlan } from '../api'
import { encodeQuery, fitFilters, queryMode, routeSummary, type PlanQuery } from '../query'
import type { PlanJobState } from '../usePlanJob'
import { CollectProgress } from './CollectProgress'
import { QueryFilters } from './QueryFilters'

// Обвязка страниц результата: подпись запроса, «изменить», сворачиваемые фильтры
// (правка → новый сбор → переход на новую джобу), прогресс, ошибка, затем контент.
export function JobShell({
  jobId,
  query,
  job,
  title,
  children,
}: {
  jobId: string
  query: PlanQuery
  job: PlanJobState
  title: ReactNode
  children: ReactNode
}) {
  const navigate = useNavigate()
  const [open, setOpen] = useState(false)
  const [draft, setDraft] = useState<PlanQuery>(query)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const changed = encodeQuery(draft).toString() !== encodeQuery(query).toString()

  async function apply() {
    if (busy) return
    setBusy(true)
    setError(null)
    try {
      const q = fitFilters(draft)
      const res = await runPlan(q)
      if (!res.job_id) {
        setError(res.message || 'Не удалось запустить сбор.')
        return
      }
      const mode = res.mode ?? queryMode(q)
      navigate(`/${mode}/${res.job_id}?${encodeQuery(q)}`, { replace: true })
      setOpen(false)
    } catch {
      setError('Не удалось связаться с сервером.')
    } finally {
      setBusy(false)
    }
  }

  return (
    <>
      <header className="hero">
        <h1>{title}</h1>
        <div className="sub">{routeSummary(query.stops)}</div>
      </header>

      <div className="pl-query-head">
        <Link className="btn-ghost" to={`/?${encodeQuery(query)}`}>
          ← изменить маршрут
        </Link>
        <button type="button" className="btn-ghost" onClick={() => setOpen(!open)}>
          {open ? 'Скрыть фильтры' : '⚙ Фильтры'}
        </button>
        {open && (
          <button type="button" className="btn-primary" disabled={!changed || busy} onClick={apply}>
            {busy ? 'Запускаем…' : 'Применить →'}
          </button>
        )}
      </div>
      {open && (
        <div className="searchform">
          <QueryFilters query={draft} onChange={setDraft} />
        </div>
      )}
      {error && (
        <div className="backend-note">
          <div className="bn-title">Не удалось запустить</div>
          <div>{error}</div>
        </div>
      )}

      {job.status === 'error' ? (
        <div className="backend-note">
          <div className="bn-title">Ошибка сбора</div>
          <div>{job.error}</div>
          <div className="pl-query-head">
            <button type="button" className="btn-ghost" onClick={apply}>
              ↻ Повторить
            </button>
          </div>
        </div>
      ) : job.status !== 'done' ? (
        <CollectProgress progress={job.progress} total={job.total} stage={job.stage} />
      ) : (
        children
      )}
      <div className="count" key={jobId} />
    </>
  )
}
