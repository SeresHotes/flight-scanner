import { useMemo, useState } from 'react'
import type { FlightData } from '../types'
import type { SearchParams } from '../data/searchClient'
import { makeMoney, fmtDate } from '../lib/format'
import { buildItems, durationBounds, type FilterState, type LegFilter, type DirFilter } from '../lib/filters'
import { Stats } from './Stats'
import { FilterBar } from './FilterBar'
import { Controls } from './Controls'
import { Legend } from './Legend'
import { TripCard, type Ctx } from './TripCard'

const CAP = 250

function priceBoundsFor(data: FlightData): Record<DirFilter, [number, number]> {
  const m = data.meta
  return {
    round: [m.there_price_range[0] + m.back_price_range[0], m.there_price_range[1] + m.back_price_range[1]],
    there: m.there_price_range,
    back: m.back_price_range,
  }
}

export function ResultsView({ data, params }: { data: FlightData; params: SearchParams }) {
  const meta = data.meta
  const money = useMemo(() => makeMoney(meta.currency), [meta.currency])
  const region = (meta.region_flag ? meta.region_flag + ' ' : '') + (meta.region_label || meta.destination_code)
  const ctx: Ctx = { meta, money, region }

  const THERE = useMemo(() => data.trips.filter((t) => t.direction === 'there'), [data])
  const BACK = useMemo(() => data.trips.filter((t) => t.direction === 'back'), [data])
  const concertDate = meta.event?.date ?? null

  const stayCap = Math.max(meta.min_stay, meta.max_korea_days)
  const legTravMax = Math.ceil((meta.max_leg_travel_minutes || 0) / 60) || 1
  const maxStopDays = meta.max_stop_days || 6
  const priceBounds = useMemo(() => priceBoundsFor(data), [data])
  const durBounds = useMemo(() => durationBounds(THERE, BACK), [THERE, BACK])

  const [state, setState] = useState<FilterState>(() => {
    const initMinStay = Math.max(meta.min_stay, params.min_stay || meta.min_stay)
    const legDefaults = (dates: [string, string]): LegFilter => ({
      stop: 'all',
      tr: 'all',
      maxTravel: legTravMax,
      sMin: 0,
      sMax: maxStopDays,
      city: 'all',
      depFrom: dates[0],
      depTo: dates[1],
    })
    return {
      dir: 'round',
      there: legDefaults(params.leg1_dates),
      back: legDefaults(params.leg2_dates),
      maxPrice: priceBounds.round[1],
      minStay: initMinStay,
      maxStay: stayCap,
      sort: 'price',
      // По умолчанию точные фильтры дат не ограничивают (полный диапазон / пустые окна).
      durMin: durBounds[0],
      durMax: durBounds[1],
      beFrom: '',
      beTo: '',
      stopFrom: '',
      stopTo: '',
    }
  })

  function onDir(v: DirFilter) {
    setState((s) => ({ ...s, dir: v, maxPrice: priceBounds[v][1] }))
  }
  function onLegPatch(leg: 'there' | 'back', patch: Partial<LegFilter>) {
    setState((s) => ({ ...s, [leg]: { ...s[leg], ...patch } }))
  }
  function onPatch(patch: Partial<FilterState>) {
    setState((s) => ({ ...s, ...patch }))
  }

  const { items, total } = useMemo(
    () => buildItems(state, THERE, BACK, concertDate),
    [state, THERE, BACK, concertDate],
  )
  const shown = items.slice(0, CAP)

  return (
    <>
      <Stats meta={meta} money={money} />

      <FilterBar
        state={state}
        meta={meta}
        there={THERE}
        back={BACK}
        bounds={{ legTravMax, maxStopDays }}
        onDir={onDir}
        onLegPatch={onLegPatch}
      />

      <Controls
        state={state}
        meta={meta}
        stayCap={stayCap}
        priceBounds={priceBounds[state.dir]}
        durBounds={durBounds}
        money={money}
        onPatch={onPatch}
      />

      <Legend />

      <div className="count">
        {items.length > CAP ? (
          <>
            Показаны первые <b>{CAP}</b> из <b>{items.length}</b> подходящих (уточните фильтры)
          </>
        ) : (
          <>
            Найдено <b>{items.length}</b> маршрутов
          </>
        )}
        <span style={{ marginLeft: 8, opacity: 0.6 }}>· всего комбинаций: {total.toLocaleString('ru-RU')}</span>
      </div>

      <div className="cards">
        {shown.length ? (
          shown.map((it, i) => <TripCard key={i} item={it} ctx={ctx} />)
        ) : (
          <div className="empty">Ничего не найдено под эти фильтры 🤷</div>
        )}
      </div>

      <div className="footer">
        Данные Travelpayouts/Aviasales
        {meta.collected_at ? ` · собраны ${fmtDate(meta.collected_at)}` : ''} · сгенерировано{' '}
        {meta.generated_at.replace('T', ' ')} · цены и наличие мест могут меняться
      </div>
    </>
  )
}
