import type { Trip } from '../types'
import { dateOnly, daysBetweenISO } from './dates'

// Логика фильтрации/сортировки — перенос из web/index.html без смены поведения.

// --- Даты присутствия по точкам маршрута (для точных фильтров дат) ---

// Первый вылет (из A) и последний прилёт (обратно в A / в B для плеча) — по сегментам.
export function tripStart(t: Trip): string {
  return dateOnly(t.segments[0]?.departure_at)
}
export function tripEnd(t: Trip): string {
  return dateOnly(t.segments[t.segments.length - 1]?.arrival_at)
}

// Интервал присутствия в городе-остановке = самый длинный «разрыв» между соседними
// сегментами (там и живём). null — если остановки нет. Надёжнее матчинга по названию.
export function stopoverInterval(t: Trip): [string, string] | null {
  if (!t.stop || t.segments.length < 2) return null
  let best: [string, string] | null = null
  let bestGap = -1
  for (let i = 0; i < t.segments.length - 1; i++) {
    const a = dateOnly(t.segments[i].arrival_at)
    const b = dateOnly(t.segments[i + 1].departure_at)
    const gap = daysBetweenISO(a, b)
    if (gap > bestGap) {
      bestGap = gap
      best = [a, b]
    }
  }
  return best
}

// Интервал [start, end] полностью покрывает окно [from, to]. Пустые границы окна не ограничивают.
function covers(start: string, end: string, from: string, to: string): boolean {
  if (from && start > from) return false
  if (to && end < to) return false
  return true
}

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
  // Точные фильтры дат (клиентские, поверх собранного):
  durMin: number // длительность всей поездки, дней (A→B→A для round, A→B для плеча)
  durMax: number
  beFrom: string // окно, которое поездка должна ПОКРЫТЬ присутствием в точке назначения B
  beTo: string
  stopFrom: string // окно присутствия в городе-остановке (если есть)
  stopTo: string
}

// Длительность отображаемого варианта, дней. Для round — от вылета из A до прилёта
// обратно в A; для плеча — его собственный размах (включая остановку).
export function itemDuration(it: RenderItem): number {
  if (it.kind === 'round') return Math.max(0, daysBetweenISO(tripStart(it.th), tripEnd(it.bk)))
  return Math.max(0, daysBetweenISO(tripStart(it.trip), tripEnd(it.trip)))
}

// Верхняя граница слайдера длительности — по самому размашистому возможному варианту.
export function durationBounds(there: Trip[], back: Trip[]): [number, number] {
  const starts = there.map(tripStart).filter(Boolean)
  const ends = back.map(tripEnd).filter(Boolean)
  let max = 1
  if (starts.length && ends.length) {
    max = Math.max(max, daysBetweenISO(starts.reduce((a, b) => (a < b ? a : b)),
                                       ends.reduce((a, b) => (a > b ? a : b))))
  }
  for (const t of [...there, ...back]) max = Math.max(max, daysBetweenISO(tripStart(t), tripEnd(t)))
  return [1, Math.max(2, max)]
}

// Проходит ли вариант точные фильтры дат (длительность + окна присутствия по точкам).
function matchDates(it: RenderItem, s: FilterState): boolean {
  const dur = itemDuration(it)
  if (dur < s.durMin || dur > s.durMax) return false

  // Присутствие в точке назначения B.
  if (s.beFrom || s.beTo) {
    if (it.kind === 'round') {
      // Живём в B с прилёта туда (th.hub_date) до вылета обратно (bk.hub_date).
      if (!covers(it.th.hub_date, it.bk.hub_date, s.beFrom, s.beTo)) return false
    } else {
      // Плечо касается B один раз (hub_date) — должно попадать в окно.
      const d = it.trip.hub_date
      if (s.beFrom && d < s.beFrom) return false
      if (s.beTo && d > s.beTo) return false
    }
  }

  // Присутствие в городе-остановке (если задано окно — вариант без остановки не подходит).
  if (s.stopFrom || s.stopTo) {
    const legs = it.kind === 'round' ? [it.th, it.bk] : [it.trip]
    const ok = legs.some((t) => {
      const iv = stopoverInterval(t)
      return iv ? covers(iv[0], iv[1], s.stopFrom, s.stopTo) : false
    })
    if (!ok) return false
  }
  return true
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

  // Точные фильтры дат (длительность + окна присутствия) — поверх уже собранного списка.
  items = items.filter((it) => matchDates(it, state))

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
