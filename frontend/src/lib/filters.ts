import type { Trip } from '../types'

// Логика фильтрации/сортировки — перенос из web/index.html без смены поведения.

export type StopFilter = 'all' | 'no' | 'yes'
export type TrFilter = 'all' | 'no' | 'yes'
export type DirFilter = 'round' | 'there' | 'back'
export type SortKey = 'price' | 'stay_desc' | 'stopdays_desc' | 'transfers' | 'travel'

export interface LegFilter {
  stop: StopFilter
  tr: TrFilter
  maxTravel: number // часы
  sMin: number
  sMax: number
  city: string // 'all' | конкретный город
  depFrom: string // из SearchForm: диапазон дат вылета плеча
  depTo: string
}

export interface FilterState {
  dir: DirFilter
  there: LegFilter
  back: LegFilter
  maxPrice: number
  minStay: number
  maxStay: number
  sort: SortKey
}

export const DIR_OPTS: { v: DirFilter; l: string }[] = [
  { v: 'round', l: 'Туда-обратно' },
  { v: 'there', l: 'Только туда' },
  { v: 'back', l: 'Только обратно' },
]
export const STOP_OPTS: { v: StopFilter; l: string }[] = [
  { v: 'all', l: 'Все' },
  { v: 'no', l: 'Без остановки' },
  { v: 'yes', l: 'С остановкой' },
]
export const TR_OPTS: { v: TrFilter; l: string }[] = [
  { v: 'all', l: 'Все' },
  { v: 'no', l: 'Без пересадок' },
  { v: 'yes', l: 'С пересадками' },
]

// Фильтр одного плеча (опции) — matchOpt из index.html.
export function matchOpt(o: Trip, f: LegFilter): boolean {
  if (f.stop === 'no' && o.stop) return false
  if (f.stop === 'yes' && !o.stop) return false
  if (f.tr === 'no' && o.tr > 0) return false
  if (f.tr === 'yes' && o.tr === 0) return false
  if (o.travel > f.maxTravel * 60) return false
  if (o.sdays > 0 && (o.sdays < f.sMin || o.sdays > f.sMax)) return false
  if (f.city && f.city !== 'all' && o.city !== f.city) return false
  if (f.depFrom && o.dep_date < f.depFrom) return false
  if (f.depTo && o.dep_date > f.depTo) return false
  return true
}

export interface OnewayItem {
  kind: 'oneway'
  trip: Trip
  price: number
  stay: number
  sdays: number
  tr: number
  travel: number
}
export interface RoundItem {
  kind: 'round'
  th: Trip
  bk: Trip
  korea: number
  price: number
  stay: number
  sdays: number
  tr: number
  travel: number
}
export type RenderItem = OnewayItem | RoundItem

const COMPARATORS: Record<SortKey, (a: RenderItem, b: RenderItem) => number> = {
  price: (a, b) => a.price - b.price,
  stay_desc: (a, b) => b.stay - a.stay || a.price - b.price,
  stopdays_desc: (a, b) => b.sdays - a.sdays || a.price - b.price,
  transfers: (a, b) => a.tr - b.tr || a.price - b.price,
  travel: (a, b) => a.travel - b.travel || a.price - b.price,
}

export interface BuildResult {
  items: RenderItem[]
  total: number
}

// Сборка отображаемого списка: односторонние плечи или round-trip на лету.
export function buildItems(
  state: FilterState,
  there: Trip[],
  back: Trip[],
  concertDate: string | null,
): BuildResult {
  const fThere = there.filter((o) => matchOpt(o, state.there))
  const fBack = back.filter((o) => matchOpt(o, state.back))
  let items: RenderItem[] = []

  if (state.dir === 'there') {
    items = fThere
      .filter((o) => o.total_price <= state.maxPrice)
      .map((o) => oneway(o))
  } else if (state.dir === 'back') {
    items = fBack
      .filter((o) => o.total_price <= state.maxPrice)
      .map((o) => oneway(o))
  } else {
    // round: собираем на лету, ограничивая произведение
    let ths = fThere
    let bks = fBack
    const LIM = 250000
    if (ths.length * bks.length > LIM) {
      const capT = Math.max(400, Math.floor(LIM / Math.max(1, bks.length)))
      const capB = Math.max(400, Math.floor(LIM / Math.max(1, ths.length)))
      ths = fThere.slice(0, capT)
      bks = fBack.slice(0, capB)
    }
    for (const th of ths) {
      for (const bk of bks) {
        const korea = bk.hub_ord - th.hub_ord
        if (korea < state.minStay || korea > state.maxStay) continue
        if (concertDate && !(th.hub_date <= concertDate && concertDate <= bk.hub_date)) continue
        const price = th.total_price + bk.total_price
        if (price > state.maxPrice) continue
        items.push({
          kind: 'round',
          th,
          bk,
          korea,
          price,
          stay: korea,
          sdays: Math.max(th.sdays, bk.sdays),
          tr: th.total_transfers + bk.total_transfers,
          travel: th.travel_minutes + bk.travel_minutes,
        })
      }
    }
  }

  items.sort(COMPARATORS[state.sort])

  const total =
    state.dir === 'there'
      ? there.length
      : state.dir === 'back'
        ? back.length
        : there.length * back.length

  return { items, total }
}

function oneway(o: Trip): OnewayItem {
  return {
    kind: 'oneway',
    trip: o,
    price: o.total_price,
    stay: 0,
    sdays: o.sdays,
    tr: o.total_transfers,
    travel: o.travel_minutes,
  }
}
