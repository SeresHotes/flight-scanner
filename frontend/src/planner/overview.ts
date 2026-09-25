// Режим «наборы городов»: по каждой последовательности городов (MOW → IST → ICN, …)
// — минимальная цена, пересадки и число цепочек. В отличие от списка маршрутов,
// здесь нет движковых границ (maxResults/maxCost): оцениваются ВСЕ варианты из
// собранного графа рёбер (PlanGraph).
//
// Цепочки не перечисляются (их экспоненциально много). Все фильтры локальны:
// пребывание в городе зависит только от пары соседних рейсов, длина поездки — от
// последнего. Поэтому для фиксированной последовательности городов хватает динамики
// по времени прилёта: состояние — «прилёт в текущий город в момент T» → агрегат
// (min цена, число цепочек). Семантика стыковки повторяет core/planner.py
// (_onward_candidates/_assemble), фильтры — filtering.ts/itineraryMatches.

import type { GraphEdge, PlanGraph, PlannerFilters } from './types'
import { addDays, coversWindow, dateOnly, hasBothWeekendDays } from './dates'

const DAY_MS = 24 * 3600 * 1000

export interface CityCombo {
  key: string
  codes: string[]
  minPrice: number
  transfersAtMin: number // пересадки у самой дешёвой цепочки
  minTransfers: number // минимум пересадок среди всех цепочек набора
  count: number // точное число цепочек
}

export interface OverviewResult {
  combos: CityCombo[] // по возрастанию минимальной цены
  totalCount: number // всего цепочек во всех наборах
}

interface Agg {
  price: number
  transfersAtMin: number
  minTransfers: number
  count: number
}

// Время без TZ → мс (как parse_datetime на бэке: таймзону игнорируем).
function naiveMs(iso: string): number {
  return Date.parse(iso.length === 10 ? `${iso}T00:00:00Z` : `${iso.slice(0, 19)}Z`)
}

// Дни пребывания — как timedelta.days в calculate_stay_duration (floor).
function stayDays(arriveIso: string, departIso: string): number {
  return Math.floor((naiveMs(departIso) - naiveMs(arriveIso)) / DAY_MS)
}

function finalDepart(g: PlanGraph, arriveIso: string): string {
  return `${addDays(dateOnly(arriveIso), g.final_stay_days)}T00:00:00`
}

// Проверка фильтра города k для пребывания [arrive..depart] (кроме allowedCodes —
// он проверяется на уровне последовательности).
function stopFits(f: PlannerFilters, k: number, arrive: string, depart: string, isLast: boolean): boolean {
  const cf = f.cities[k]
  if (!cf) return true
  let days = stayDays(arrive, depart)
  if (isLast) days = Math.max(1, days)
  days = Math.max(0, days)
  if (days < cf.minStay || days > cf.maxStay) return false
  if (cf.mustCover && !coversWindow(arrive, depart, cf.mustCover)) return false
  if (cf.requireWeekend && !hasBothWeekendDays(arrive, depart)) return false
  return true
}

function merge(a: Agg | undefined, b: Agg): Agg {
  if (!a) return b
  const cheaper = b.price < a.price
  return {
    price: cheaper ? b.price : a.price,
    transfersAtMin: cheaper ? b.transfersAtMin : a.transfersAtMin,
    minTransfers: Math.min(a.minTransfers, b.minTransfers),
    count: a.count + b.count,
  }
}

type LegIndex = Map<string, Map<string, GraphEdge[]>> // from → to → рёбра

// Рёбра перехода, прошедшие фильтр перехода (пересадки, длительность), по from/to.
function indexLeg(edges: GraphEdge[], f: PlannerFilters, i: number): LegIndex {
  const tf = f.transitions[i]
  const idx: LegIndex = new Map()
  for (const e of edges) {
    if (tf && tf.maxTransfers >= 0 && e.transfers > tf.maxTransfers) continue
    if (tf && (e.duration || 0) > tf.maxTravelMinutes) continue
    let byTo = idx.get(e.from)
    if (!byTo) idx.set(e.from, (byTo = new Map()))
    const list = byTo.get(e.to)
    if (list) list.push(e)
    else byTo.set(e.to, [e])
  }
  return idx
}

function isAllowed(f: PlannerFilters, k: number, code: string): boolean {
  const allowed = f.cities[k]?.allowedCodes
  return !allowed || allowed.includes(code)
}

// reach[i] — города, из которых с остановки i в принципе (без учёта времени)
// можно добраться до финала. Отсекает тупиковые ветки перебора заранее.
function reachability(legs: LegIndex[], f: PlannerFilters): Set<string>[] {
  const last = legs.length
  const reach: Set<string>[] = Array.from({ length: last + 1 }, () => new Set<string>())
  for (const byTo of legs[last - 1]?.values() ?? []) for (const to of byTo.keys()) reach[last].add(to)
  for (let i = last - 1; i >= 0; i--) {
    for (const [from, byTo] of legs[i]) {
      for (const to of byTo.keys()) {
        if (to !== from && reach[i + 1].has(to) && isAllowed(f, i + 1, to)) {
          reach[i].add(from)
          break
        }
      }
    }
  }
  return reach
}

// Переход i: из состояний «прилёт в город в момент a» по рёбрам в следующий город.
// Для каждого момента вылета совместимые прилёты сворачиваются один раз.
function extend(state: Map<string, Agg>, edges: GraphEdge[], f: PlannerFilters, i: number): Map<string, Agg> {
  const byDep = new Map<string, Agg | null>()
  const next = new Map<string, Agg>()
  for (const e of edges) {
    let acc = byDep.get(e.dep)
    if (acc === undefined) {
      acc = null
      const depDay = dateOnly(e.dep)
      for (const [arrive, agg] of state) {
        if (depDay < dateOnly(arrive)) continue // нельзя вылететь раньше прилёта
        if (!stopFits(f, i, arrive, e.dep, false)) continue
        acc = merge(acc ?? undefined, agg)
      }
      byDep.set(e.dep, acc)
    }
    if (!acc) continue
    const withEdge: Agg = {
      price: acc.price + e.price,
      transfersAtMin: acc.transfersAtMin + e.transfers,
      minTransfers: acc.minTransfers + e.transfers,
      count: acc.count,
    }
    next.set(e.arr, merge(next.get(e.arr), withEdge))
  }
  return next
}

// Закрытие цепочки в финальном городе: фильтр финала и общей длины поездки.
function finish(g: PlanGraph, f: PlannerFilters, state: Map<string, Agg>, last: number): Agg | undefined {
  const start = `${g.chain_start}T00:00:00`
  let total: Agg | undefined
  for (const [arrive, agg] of state) {
    const depart = finalDepart(g, arrive)
    if (!stopFits(f, last, arrive, depart, true)) continue
    const tripDays = Math.max(1, stayDays(start, depart))
    if (tripDays < f.tripLength[0] || tripDays > f.tripLength[1]) continue
    total = merge(total, agg)
  }
  return total
}

export function computeOverview(g: PlanGraph, f: PlannerFilters): OverviewResult {
  const last = g.legs.length
  const combos: CityCombo[] = []
  if (last === 0) return { combos, totalCount: 0 }
  const legs = g.legs.map((edges, i) => indexLeg(edges, f, i))
  const reach = reachability(legs, f)
  const startState = new Map<string, Agg>([
    [`${g.chain_start}T00:00:00`, { price: 0, transfersAtMin: 0, minTransfers: 0, count: 1 }],
  ])

  const dfs = (i: number, city: string, state: Map<string, Agg>, codes: string[], visited: Set<string>) => {
    if (i === last) {
      const agg = finish(g, f, state, last)
      if (agg) combos.push({ key: codes.join('>'), codes, ...aggFields(agg) })
      return
    }
    for (const [to, edges] of legs[i].get(city) ?? []) {
      if (to === city) continue // без петель
      if (g.any[i + 1] && visited.has(to)) continue // «любой» не ведём в уже посещённый
      if (!isAllowed(f, i + 1, to) || !reach[i + 1].has(to)) continue
      const next = extend(state, edges, f, i)
      if (next.size === 0) continue
      dfs(i + 1, to, next, [...codes, to], new Set(visited).add(to))
    }
  }

  for (const start of legs[0].keys()) {
    if (!isAllowed(f, 0, start) || !reach[0].has(start)) continue
    dfs(0, start, startState, [start], new Set([start]))
  }

  combos.sort((a, b) => a.minPrice - b.minPrice)
  let totalCount = 0
  for (const c of combos) totalCount += c.count
  return { combos, totalCount }
}

function aggFields(a: Agg) {
  return { minPrice: a.price, transfersAtMin: a.transfersAtMin, minTransfers: a.minTransfers, count: a.count }
}

// Границы фильтров по всему графу (а не только по топ-N цепочкам списка) — чтобы
// дефолтные фильтры не срезали варианты, которых в списке нет.
export interface GraphBounds {
  maxTravel: number
  tripMin: number
  tripMax: number
  stayMax: number
}

export function graphBounds(g: PlanGraph): GraphBounds | null {
  const lastLeg = g.legs[g.legs.length - 1]
  if (!lastLeg || lastLeg.length === 0) return null
  const start = `${g.chain_start}T00:00:00`
  let maxTravel = 0
  let latest = start
  for (const leg of g.legs) {
    for (const e of leg) {
      if ((e.duration || 0) > maxTravel) maxTravel = e.duration || 0
      if (e.dep > latest) latest = e.dep
    }
  }
  let tripMin = Infinity
  let tripMax = -Infinity
  for (const e of lastLeg) {
    const days = Math.max(1, stayDays(start, finalDepart(g, e.arr)))
    if (days < tripMin) tripMin = days
    if (days > tripMax) tripMax = days
  }
  // Дольше, чем от старта до самого позднего вылета (или финального пребывания), не пробыть.
  const stayMax = Math.max(stayDays(start, latest), g.final_stay_days, 0)
  return { maxTravel, tripMin, tripMax, stayMax }
}

// Города, реально встречающиеся на каждой остановке графа (для фильтра городов).
export function graphCitiesByStop(g: PlanGraph): string[][] {
  const out: string[][] = [Array.from(new Set(g.legs[0]?.map((e) => e.from) ?? []))]
  for (const leg of g.legs) out.push(Array.from(new Set(leg.map((e) => e.to))))
  return out
}
