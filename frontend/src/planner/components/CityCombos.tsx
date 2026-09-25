import { useDeferredValue, useMemo, useState } from 'react'
import { makeMoney, plural } from '../../lib/format'
import type { PlanGraph, PlannerFilters } from '../types'
import { computeOverview, type CityCombo } from '../overview'

const money = makeMoney('RUB')
const PAGE_SIZE = 100

type SortKey = 'price' | 'count' | 'transfers'

const SORTS: Record<SortKey, (a: CityCombo, b: CityCombo) => number> = {
  price: (a, b) => a.minPrice - b.minPrice,
  count: (a, b) => b.count - a.count || a.minPrice - b.minPrice,
  transfers: (a, b) => a.minTransfers - b.minTransfers || a.minPrice - b.minPrice,
}

// Режим «наборы городов»: все последовательности городов под текущие фильтры —
// минимальная цена, пересадки и число цепочек (без движковых лимитов списка).
export function CityCombos({
  graph,
  filters,
  onShowRoutes,
}: {
  graph: PlanGraph
  filters: PlannerFilters
  onShowRoutes: (codes: string[]) => void
}) {
  // Пересчёт по всему графу — отложенный, чтобы слайдеры фильтров не подтормаживали.
  const deferredFilters = useDeferredValue(filters)
  const overview = useMemo(() => computeOverview(graph, deferredFilters), [graph, deferredFilters])
  const [sort, setSort] = useState<SortKey>('price')
  const [shown, setShown] = useState(PAGE_SIZE)

  const sorted = useMemo(() => [...overview.combos].sort(SORTS[sort]), [overview, sort])
  const stale = deferredFilters !== filters

  const cityLabel = (code: string) => {
    const ci = graph.cities[code]
    return `${ci?.flag ? ci.flag + ' ' : ''}${ci?.city || code}`
  }

  return (
    <div className="pl-combos" style={{ opacity: stale ? 0.6 : 1 }}>
      <div className="count">
        <b>{overview.combos.length}</b> {plural(overview.combos.length, 'набор', 'набора', 'наборов')} городов ·{' '}
        <b>{overview.totalCount.toLocaleString('ru-RU')}</b>{' '}
        {plural(overview.totalCount, 'маршрут', 'маршрута', 'маршрутов')} под фильтры — все варианты, без лимита
        маршрутов и цены
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

      {sorted.length ? (
        <>
          <div className="pl-combo-list">
            {sorted.slice(0, shown).map((c) => (
              <div key={c.key} className="pl-combo">
                <div className="pl-combo-route">{c.codes.map(cityLabel).join(' → ')}</div>
                <div className="pl-combo-stats">
                  <span className="p">от {money(c.minPrice)}</span>
                  <span title="Пересадок суммарно у самой дешёвой цепочки (в скобках — минимум по набору)">
                    ✈ {c.transfersAtMin} {plural(c.transfersAtMin, 'пересадка', 'пересадки', 'пересадок')}
                    {c.minTransfers < c.transfersAtMin && <> (мин. {c.minTransfers})</>}
                  </span>
                  <span>
                    🧾 {c.count.toLocaleString('ru-RU')} {plural(c.count, 'маршрут', 'маршрута', 'маршрутов')}
                  </span>
                  <button
                    type="button"
                    className="btn-ghost"
                    onClick={() => onShowRoutes(c.codes)}
                    title="Зафиксировать эти города в фильтрах и открыть список. В списке — только собранные с учётом лимита маршрутов и цены."
                  >
                    Маршруты →
                  </button>
                </div>
              </div>
            ))}
          </div>
          {shown < sorted.length && (
            <div className="pl-pager">
              <button type="button" onClick={() => setShown(shown + PAGE_SIZE)}>
                Показать ещё {Math.min(PAGE_SIZE, sorted.length - shown)} из {sorted.length - shown}
              </button>
            </div>
          )}
        </>
      ) : (
        <div className="empty">Под текущие фильтры наборов нет — ослабьте условия.</div>
      )}
    </div>
  )
}
