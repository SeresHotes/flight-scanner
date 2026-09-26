// Режим «наборы городов»: по каждой последовательности городов (MOW → IST → ICN, …)
// — минимальная цена, пересадки и число цепочек. В отличие от списка маршрутов,
// здесь нет движковых границ (maxResults/maxCost): оцениваются ВСЕ варианты из
// собранного графа рёбер (PlanGraph).
//
// Цепочки не перечисляются (их экспоненциально много). Все фильтры локальны:
// пребывание в городе зависит только от пары соседних рейсов, длина поездки — от
// первого вылета и последнего прилёта. Поэтому для фиксированной последовательности
// городов хватает динамики: состояние — «первый вылет в день D, прилёт в текущий
// город в момент T» → агрегат (min цена, число цепочек). Семантика стыковки повторяет core/planner.py
// (_onward_candidates/_assemble), фильтры — filtering.ts/itineraryMatches.

import type { GraphEdge, PlanGraph, PlannerFilters } from './types'
import { coversWindow, dateOnly, hasBothWeekendDays } from './dates'

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

// Длина поездки — от даты первого вылета до даты последнего прилёта (как _trip_days на бэке).
function tripDays(firstDepDay: string, lastArriveIso: string): number {
  return Math.max(1, stayDays(firstDepDay, dateOnly(lastArriveIso)))
}

// Проверка фильтра ПРОМЕЖУТОЧНОГО города k для пребывания [arrive..depart] (кроме
// allowedCodes — он проверяется на уровне последовательности). У концов маршрута
// фильтров пребывания нет (см. filtering.ts/isEndpoint).
function stopFits(f: PlannerFilters, k: number, arrive: string, depart: string): boolean {
  const cf = f.cities[k]
  if (!cf) return true
  const days = Math.max(0, stayDays(arrive, depart))
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
type Arrivals = Map<string, Agg> // момент прилёта → агрегат
type State = Map<string, Arrivals> // день первого вылета → прилёты

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

// Первый переход: вылет из стартового города (не раньше начала окна, как на бэке).
function departFirst(g: PlanGraph, edges: GraphEdge[]): State {
  const next: State = new Map()
  for (const e of edges) {
    const depDay = dateOnly(e.dep)
    if (depDay < g.chain_start) continue
    let arrivals = next.get(depDay)
    if (!arrivals) next.set(depDay, (arrivals = new Map()))
    const agg: Agg = { price: e.price, transfersAtMin: e.transfers, minTransfers: e.transfers, count: 1 }
    arrivals.set(e.arr, merge(arrivals.get(e.arr), agg))
  }
  return next
}

// Переход i ≥ 1 — отдельно для каждого дня первого вылета.
function extendState(state: State, edges: GraphEdge[], f: PlannerFilters, i: number): State {
  const next: State = new Map()
  for (const [firstDep, arrivals] of state) {
    const ext = extend(arrivals, edges, f, i)
    if (ext.size) next.set(firstDep, ext)
  }
  return next
}

// Переход i: из состояний «прилёт в город в момент a» по рёбрам в следующий город.
// Для каждого момента вылета совместимые прилёты сворачиваются один раз.
function extend(state: Arrivals, edges: GraphEdge[], f: PlannerFilters, i: number): Arrivals {
  const byDep = new Map<string, Agg | null>()
  const next: Arrivals = new Map()
  for (const e of edges) {
    let acc = byDep.get(e.dep)
    if (acc === undefined) {
      acc = null
      const depDay = dateOnly(e.dep)
      for (const [arrive, agg] of state) {
        if (depDay < dateOnly(arrive)) continue // нельзя вылететь раньше прилёта
        if (!stopFits(f, i, arrive, e.dep)) continue
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

// Закрытие цепочки в финальном городе: фильтр общей длины поездки.
function finish(f: PlannerFilters, state: State): Agg | undefined {
  let total: Agg | undefined
  for (const [firstDep, arrivals] of state) {
    for (const [arrive, agg] of arrivals) {
      const days = tripDays(firstDep, arrive)
      if (days < f.tripLength[0] || days > f.tripLength[1]) continue
      total = merge(total, agg)
    }
  }
  return total
}

export function computeOverview(g: PlanGraph, f: PlannerFilters): OverviewResult {
  const last = g.legs.length
  const combos: CityCombo[] = []
  if (last === 0) return { combos, totalCount: 0 }
  const legs = g.legs.map((edges, i) => indexLeg(edges, f, i))
  const reach = reachability(legs, f)
  const dfs = (i: number, city: string, state: State, codes: string[], visited: Set<string>) => {
    if (i === last) {
      const agg = finish(f, state)
      if (agg) combos.push({ key: codes.join('>'), codes, ...aggFields(agg) })
      return
    }
    for (const [to, edges] of legs[i].get(city) ?? []) {
      if (to === city) continue // без петель
      if (g.any[i + 1] && visited.has(to)) continue // «любой» не ведём в уже посещённый
      if (!isAllowed(f, i + 1, to) || !reach[i + 1].has(to)) continue
      const next = i === 0 ? departFirst(g, edges) : extendState(state, edges, f, i)
      if (next.size === 0) continue
      dfs(i + 1, to, next, [...codes, to], new Set(visited).add(to))
    }
  }

  for (const start of legs[0].keys()) {
    if (!isAllowed(f, 0, start) || !reach[0].has(start)) continue
    dfs(0, start, new Map(), [start], new Set([start]))
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
  // Длина поездки: охватывающий диапазон по крайним первым вылетам и последним прилётам.
  const firstDeps = g.legs[0].map((e) => dateOnly(e.dep)).sort()
  const lastArrs = lastLeg.map((e) => e.arr).sort()
  const tripMin = tripDays(firstDeps[firstDeps.length - 1] ?? g.chain_start, lastArrs[0])
  const tripMax = tripDays(firstDeps[0] ?? g.chain_start, lastArrs[lastArrs.length - 1])
  // Дольше, чем от старта до самого позднего вылета, в промежуточном городе не пробыть.
  const stayMax = Math.max(stayDays(start, latest), 0)
  return { maxTravel, tripMin, tripMax, stayMax }
}

// Города, реально встречающиеся на каждой остановке графа (для фильтра городов).
export function graphCitiesByStop(g: PlanGraph): string[][] {
  const out: string[][] = [Array.from(new Set(g.legs[0]?.map((e) => e.from) ?? []))]
  for (const leg of g.legs) out.push(Array.from(new Set(leg.map((e) => e.to))))
  return out
}
