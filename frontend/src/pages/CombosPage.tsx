import { useEffect, useState } from 'react'
import { Navigate, useNavigate, useParams } from 'react-router-dom'
import { makeMoney, plural } from '../lib/format'
import { fetchCombos, type CityCombo, type CombosPage as CombosPayload } from '../planner/api'
import { encodeQuery } from '../planner/query'
import { usePlanJob } from '../planner/usePlanJob'
import { useQueryFromUrl } from '../planner/useQueryState'
import { JobShell } from '../planner/components/JobShell'

const money = makeMoney('RUB')
const PAGE = 100
type SortKey = 'price' | 'count' | 'transfers'
const comboKey = (c: CityCombo) => c.codes.join('-')

// Режим городов: все наборы под запрос (считает бэк). Отмечаем несколько и
// переходим к маршрутам выбранных наборов вместе.
export function CombosPage() {
  const { jobId } = useParams()
  const [query] = useQueryFromUrl()
  const job = usePlanJob(jobId)
  if (!jobId || !query) return <Navigate to="/" replace />
  return (
    <JobShell jobId={jobId} query={query} job={job} title="🗺 Наборы городов">
      <CombosList jobId={jobId} queryString={encodeQuery(query).toString()} />
    </JobShell>
  )
}

function CombosList({ jobId, queryString }: { jobId: string; queryString: string }) {
  const navigate = useNavigate()
  const [sort, setSort] = useState<SortKey>('price')
  const [pages, setPages] = useState<CombosPayload[]>([])
  const [loading, setLoading] = useState(false)
  const [selected, setSelected] = useState<Set<string>>(new Set())

  const load = async (offset: number, reset: boolean) => {
    setLoading(true)
    try {
      const page = await fetchCombos(jobId, sort, offset, PAGE)
      setPages((prev) => (reset ? [page] : [...prev, page]))
    } finally {
      setLoading(false)
    }
  }
  useEffect(() => {
    void load(0, true)
  }, [jobId, sort]) // eslint-disable-line react-hooks/exhaustive-deps

  const first = pages[0]
  const items = pages.flatMap((p) => p.items)
  const cities = Object.assign({}, ...pages.map((p) => p.cities)) as Record<string, [string, string]>
  const cityLabel = (code: string) => {
    const c = cities[code]
    return c ? `${c[1] ? c[1] + ' ' : ''}${c[0] || code}` : code
  }
  const toggle = (key: string) =>
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  const showRoutes = (keys: string[]) => navigate(`/routes/${jobId}?${queryString}&combos=${encodeURIComponent(keys.join(','))}`)

  if (!first) return <div className="empty">{loading ? 'Загружаем наборы…' : 'Нет данных.'}</div>
  if (first.status !== 'ok' || first.total === 0) {
    return <div className="empty">Под этот запрос наборов нет — ослабьте фильтры или расширьте окна дат.</div>
  }

  return (
    <div className="pl-combos">
      <div className="count">
        <b>{first.total.toLocaleString('ru-RU')}</b> {plural(first.total, 'набор', 'набора', 'наборов')} городов ·{' '}
        <b>{first.totalCount.toLocaleString('ru-RU')}</b> {plural(first.totalCount, 'маршрут', 'маршрута', 'маршрутов')} под
        фильтры (без лимита маршрутов и цены)
      </div>

      <div className="pl-combo-sort">
        Сортировать:
        {(
          [
            ['price', 'по цене'],
            ['count', 'по числу маршрутов'],
            ['transfers', 'по пересадкам'],
          ] as [SortKey, string][]
        ).map(([key, label]) => (
          <button key={key} type="button" className={sort === key ? 'on' : ''} onClick={() => setSort(key)}>
            {label}
          </button>
        ))}
      </div>

      <div className="pl-combos-bar">
        <span>
          Отмечено: <b>{selected.size}</b>
        </span>
        <button type="button" className="btn-primary" disabled={selected.size === 0} onClick={() => showRoutes(Array.from(selected))}>
          Показать маршруты выбранных →
        </button>
        {selected.size > 0 && (
          <button type="button" className="btn-ghost" onClick={() => setSelected(new Set())}>
            Снять выбор
          </button>
        )}
      </div>

      <div className="pl-combo-list">
        {items.map((c) => {
          const key = comboKey(c)
          const on = selected.has(key)
          return (
            <div key={key} className={`pl-combo ${on ? 'on' : ''}`}>
              <label className="pl-combo-check">
                <input type="checkbox" checked={on} onChange={() => toggle(key)} />
                <span className="pl-combo-route">{c.codes.map(cityLabel).join(' → ')}</span>
              </label>
              <div className="pl-combo-stats">
                <span className="p">от {money(c.minPrice)}</span>
                <span title="Пересадок суммарно у самой дешёвой цепочки (в скобках — минимум по набору)">
                  ✈ {c.transfersAtMin} {plural(c.transfersAtMin, 'пересадка', 'пересадки', 'пересадок')}
                  {c.minTransfers < c.transfersAtMin && <> (мин. {c.minTransfers})</>}
                </span>
                <span>
                  🧾 {c.count.toLocaleString('ru-RU')} {plural(c.count, 'маршрут', 'маршрута', 'маршрутов')}
                </span>
                <button type="button" className="btn-ghost" onClick={() => showRoutes([key])}>
                  Маршруты →
                </button>
              </div>
            </div>
          )
        })}
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
