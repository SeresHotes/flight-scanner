import { useEffect, useState } from 'react'
import { Link, Navigate, useLocation } from 'react-router-dom'
import { useSearch } from '../hooks/useSearch'
import { startCollection } from '../data/searchClient'
import { queryToParams } from '../lib/urlParams'
import { Header } from '../components/Header'
import { ResultsView } from '../components/ResultsView'
import { BackendNote } from '../components/BackendNote'
import { CollectPrompt } from '../components/CollectPrompt'
import { JobProgress } from '../components/JobProgress'
import { FreshnessBar } from '../components/FreshnessBar'

// Страница 2 — результаты по выбранному маршруту (параметры берём из URL).
export function ResultsPage() {
  const location = useLocation()
  const params = queryToParams(location.search)
  const query = useSearch(params)
  const [jobId, setJobId] = useState<string | null>(null)
  const [starting, setStarting] = useState(false)
  const [collectError, setCollectError] = useState<string | null>(null)

  const result = query.data
  const meta = result?.status === 'ok' ? result.data.meta : undefined

  useEffect(() => {
    if (meta) document.title = meta.title + ' ✈️'
  }, [meta])

  // Смена маршрута/дат — сбрасываем локально запущенную джобу.
  useEffect(() => {
    setJobId(null)
  }, [location.search])

  if (!params) return <Navigate to="/" replace />

  const activeJob = jobId ?? (result?.status === 'collecting' ? result.job_id : null)

  async function onCollect(force = false) {
    if (!params) return
    setStarting(true)
    setCollectError(null)
    try {
      const r = await startCollection(params, force)
      if (r.status === 'collecting') setJobId(r.job_id)
      else if (r.status === 'needs_backend') setCollectError(r.message ?? 'Сбор недоступен для этого запроса.')
      else await query.refetch() // ok — данные уже собраны
    } catch (e) {
      // Чаще всего — бэкенд не запущен / недоступен. Раньше ошибка молча терялась.
      setCollectError(
        `Не удалось запустить сбор: ${(e as Error)?.message ?? 'нет связи с бэкендом'}. ` +
          'Проверьте, что backend запущен (uvicorn на :8000).',
      )
    } finally {
      setStarting(false)
    }
  }

  return (
    <>
      <div className="backlink">
        <Link to="/">← Изменить поиск</Link>
      </div>

      <Header meta={meta} />

      {query.isLoading && <div className="loading-note">Проверяем, что уже собрано…</div>}

      {query.isError && (
        <div className="backend-note">
          <div className="bn-title">Не удалось загрузить данные</div>
          <div>{(query.error as Error)?.message ?? 'Неизвестная ошибка'}</div>
        </div>
      )}

      {activeJob && (
        <JobProgress
          jobId={activeJob}
          onDone={() => {
            setJobId(null)
            query.refetch()
          }}
        />
      )}

      {!activeJob && result?.status === 'needs_collection' && (
        <CollectPrompt
          origin={result.origin}
          destination={result.destination}
          estimate={result.estimate}
          starting={starting}
          error={collectError}
          onCollect={onCollect}
        />
      )}

      {!activeJob && result?.status === 'needs_backend' && (
        <BackendNote
          origin={result.origin}
          destination={result.destination}
          message={result.message}
        />
      )}

      {result?.status === 'ok' && (
        <>
          <FreshnessBar
            collectedAt={result.data.meta.collected_at}
            estimate={result.refresh_estimate}
            starting={starting || !!activeJob}
            error={collectError}
            onRefresh={() => onCollect(true)}
          />
          <ResultsView key={location.search} data={result.data} params={params} />
        </>
      )}
    </>
  )
}
