import { useNavigate } from 'react-router-dom'
import type { SearchParams } from '../data/searchClient'
import { useRoutes } from '../hooks/useRoutes'
import { paramsToQuery } from '../lib/urlParams'
import { Header } from '../components/Header'
import { SearchForm } from '../components/SearchForm'
import { StoredData } from '../components/StoredData'

// Страница 1 — выбор маршрута + обзор того, какие данные уже собраны.
export function SearchPage() {
  const navigate = useNavigate()
  const { data, isLoading, isError, error } = useRoutes()

  function goToResults(params: SearchParams) {
    navigate(`/results?${paramsToQuery(params)}`)
  }

  // «Загрузить данные» — на страницу результатов с флагом авто-сбора свежих данных.
  function goToCollect(params: SearchParams) {
    navigate(`/results?${paramsToQuery(params)}&collect=1`)
  }

  function pickRoute(origin: string, destination: string) {
    const r = data?.available.find((a) => a.origin === origin && a.destination === destination)
    goToResults({
      origin,
      destination,
      leg1_dates: r ? r.dep_there_range : ['', ''],
      leg2_dates: r ? r.dep_back_range : ['', ''],
      min_stay: r ? r.min_stay : 7,
    })
  }

  return (
    <>
      <Header />

      <SearchForm availableRoutes={data?.available} onShow={goToResults} onCollect={goToCollect} />

      {isLoading && <div className="loading-note">Загружаем список собранных данных…</div>}
      {isError && (
        <div className="backend-note">
          <div className="bn-title">Не удалось загрузить список данных</div>
          <div>{(error as Error)?.message}</div>
        </div>
      )}
      {data && <StoredData available={data.available} stored={data.stored} onPick={pickRoute} />}
    </>
  )
}
