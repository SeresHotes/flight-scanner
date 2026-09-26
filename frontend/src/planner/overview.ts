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
//
// Скорость (на широком запросе — сотни миллионов цепочек):
// - время рейсов разбирается один раз (prepareGraph), в цикле — только числа;
// - подходящие к вылету прилёты — непрерывный отрезок отсортированных прилётов
//   (все условия пребывания монотонны по моменту прилёта), поэтому стыковка —
//   скользящее окно с монотонными очередями, а не «каждый вылет × каждый прилёт»;
// - в конце каждой последовательности состояние сворачивается в гистограмму по
//   длине поездки: смена только этого фильтра не пересчитывает динамику.
// OverviewEngine.steps — генератор: воркер (overview.worker.ts) между шагами
// отдаёт управление и бросает устаревший расчёт, если фильтры уже поменялись.

import type { PlanGraph, PlannerFilters } from './types'
import { dateOnly } from './dates'

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

// Номер дня (дни от эпохи) — календарная дата наивного времени.
function dayOf(ms: number): number {
  return Math.floor(ms / DAY_MS)
}

// Дни пребывания — как timedelta.days в calculate_stay_duration (floor).
function stayDays(arriveIso: string, departIso: string): number {
  return Math.floor((naiveMs(departIso) - naiveMs(arriveIso)) / DAY_MS)
}

// Длина поездки — от даты первого вылета до даты последнего прилёта (как _trip_days на бэке).
function tripDays(firstDepDay: string, lastArriveIso: string): number {
  return Math.max(1, stayDays(firstDepDay, dateOnly(lastArriveIso)))
}

// Оба выходных (сб и вс) среди дней [from..to] — как dates.hasBothWeekendDays.
function hasBothWeekendDays(from: number, to: number): boolean {
  if (to < from) return false
  if (to - from >= 6) return true
  let sat = false
  let sun = false
  for (let d = from; d <= to; d++) {
    const wd = (d + 4) % 7 // день 0 (1970-01-01) — четверг; 0 — вс, 6 — сб
    if (wd === 6) sat = true
    if (wd === 0) sun = true
  }
  return sat && sun
}

// --- Подготовленный граф: время уже числами, рёбра по возрастанию вылета ---

interface Edge {
  from: string
  to: string
  dep: number // мс наивного времени
  arr: number
  depDay: number
  arrDay: number
  price: number
  transfers: number
  duration: number
}

export interface PreparedGraph {
  chainStartDay: number
  any: boolean[]
  legs: Edge[][]
}

export function prepareGraph(g: PlanGraph): PreparedGraph {
  const parsed = new Map<string, number>()
  const parse = (iso: string) => {
    let ms = parsed.get(iso)
    if (ms === undefined) parsed.set(iso, (ms = naiveMs(iso)))
    return ms
  }
  const legs = g.legs.map((edges) =>
    edges
      .map((e) => {
        const dep = parse(e.dep)
        const arr = parse(e.arr)
        return {
          from: e.from,
          to: e.to,
          dep,
          arr,
          depDay: dayOf(dep),
          arrDay: dayOf(arr),
          price: e.price,
          transfers: e.transfers,
          duration: e.duration || 0,
        }
      })
      .sort((a, b) => a.dep - b.dep),
  )
  return { chainStartDay: dayOf(naiveMs(g.chain_start)), any: g.any, legs }
}

// --- Фильтр пребывания ПРОМЕЖУТОЧНОГО города в числах ---
// allowedCodes проверяется на уровне последовательности. У концов маршрута
// фильтров пребывания нет (см. filtering.ts/isEndpoint).

interface StayRule {
  minStay: number
  maxStay: number
  coverFrom: number | null // день, не позже которого нужно прилететь
  coverTo: number // день, не раньше которого нужно улететь
  requireWeekend: boolean
}

function stayRule(f: PlannerFilters, k: number): StayRule | null {
  const cf = f.cities[k]
  if (!cf) return null
  const cover = cf.mustCover && cf.mustCover[0] && cf.mustCover[1] ? cf.mustCover : null
  return {
    minStay: cf.minStay,
    maxStay: cf.maxStay,
    coverFrom: cover ? dayOf(naiveMs(cover[0])) : null,
    coverTo: cover ? dayOf(naiveMs(cover[1])) : 0,
    requireWeekend: cf.requireWeekend,
  }
}

// Прилёт не слишком поздний для вылета e: успеваем и пребывание проходит все
// условия, кроме maxStay. Чем раньше прилёт, тем легче условие — монотонно.
function isEarlyEnough(arr: number, e: Edge, rule: StayRule | null): boolean {
  const arrDay = dayOf(arr)
  if (e.depDay < arrDay) return false // нельзя вылететь раньше прилёта
  if (!rule) return true
  const days = Math.max(0, Math.floor((e.dep - arr) / DAY_MS))
  if (days < rule.minStay) return false
  if (rule.coverFrom !== null && (arrDay > rule.coverFrom || e.depDay < rule.coverTo)) return false
  if (rule.requireWeekend && !hasBothWeekendDays(arrDay, e.depDay)) return false
  return true
}

// Прилёт слишком ранний: пребывание длиннее maxStay. Тоже монотонно.
function isTooEarly(arr: number, e: Edge, rule: StayRule | null): boolean {
  if (!rule) return false
  return Math.max(0, Math.floor((e.dep - arr) / DAY_MS)) > rule.maxStay
}

// При равной цене — меньше пересадок (детерминированно, без зависимости от порядка).
function isCheaper(b: Agg, a: Agg): boolean {
  return b.price < a.price || (b.price === a.price && b.transfersAtMin < a.transfersAtMin)
}

function merge(a: Agg | undefined, b: Agg): Agg {
  if (!a) return b
  const cheaper = isCheaper(b, a)
  return {
    price: cheaper ? b.price : a.price,
    transfersAtMin: cheaper ? b.transfersAtMin : a.transfersAtMin,
    minTransfers: Math.min(a.minTransfers, b.minTransfers),
    count: a.count + b.count,
  }
}

type LegIndex = Map<string, Map<string, Edge[]>> // from → to → рёбра (по возрастанию вылета)
type Arrivals = Map<number, Agg> // момент прилёта → агрегат
type State = Map<number, Arrivals> // день первого вылета → прилёты

// Рёбра перехода, прошедшие фильтр перехода (пересадки, длительность), по from/to.
function indexLeg(edges: Edge[], f: PlannerFilters, i: number): LegIndex {
  const tf = f.transitions[i]
  const idx: LegIndex = new Map()
  for (const e of edges) {
    if (tf && tf.maxTransfers >= 0 && e.transfers > tf.maxTransfers) continue
    if (tf && e.duration > tf.maxTravelMinutes) continue
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
function departFirst(g: PreparedGraph, edges: Edge[]): State {
  const next: State = new Map()
  for (const e of edges) {
    if (e.depDay < g.chainStartDay) continue
    let arrivals = next.get(e.depDay)
    if (!arrivals) next.set(e.depDay, (arrivals = new Map()))
    const agg: Agg = { price: e.price, transfersAtMin: e.transfers, minTransfers: e.transfers, count: 1 }
    arrivals.set(e.arr, merge(arrivals.get(e.arr), agg))
  }
  return next
}

// Переход i ≥ 1 — отдельно для каждого дня первого вылета.
function extendState(state: State, edges: Edge[], rule: StayRule | null): State {
  const next: State = new Map()
  for (const [firstDep, arrivals] of state) {
    const ext = extend(arrivals, edges, rule)
    if (ext.size) next.set(firstDep, ext)
  }
  return next
}

// Скользящее окно по отсортированным прилётам: [lo, hi) — прилёты, совместимые с
// текущим вылетом. Вылеты идут по возрастанию, границы окна только растут.
// Минимумы окна — монотонные очереди, число цепочек — префиксные суммы.
class ArrivalWindow {
  private readonly rule: StayRule | null
  private readonly times: number[]
  private readonly aggs: Agg[]
  private readonly counts: Float64Array
  private readonly byPrice: Int32Array
  private readonly byTransfers: Int32Array
  private priceHead = 0
  private priceTail = 0
  private trHead = 0
  private trTail = 0
  private lo = 0
  private hi = 0

  constructor(state: Arrivals, rule: StayRule | null) {
    this.rule = rule
    this.times = Array.from(state.keys()).sort((a, b) => a - b)
    this.aggs = this.times.map((t) => state.get(t) as Agg)
    const n = this.times.length
    this.counts = new Float64Array(n + 1)
    for (let k = 0; k < n; k++) this.counts[k + 1] = this.counts[k] + this.aggs[k].count
    this.byPrice = new Int32Array(n)
    this.byTransfers = new Int32Array(n)
  }

  // Агрегат всех прилётов, с которых можно улететь рейсом e (null — таких нет).
  fold(e: Edge): Agg | null {
    while (this.hi < this.times.length && isEarlyEnough(this.times[this.hi], e, this.rule)) this.push(this.hi++)
    while (this.lo < this.hi && isTooEarly(this.times[this.lo], e, this.rule)) this.lo++
    while (this.priceHead < this.priceTail && this.byPrice[this.priceHead] < this.lo) this.priceHead++
    while (this.trHead < this.trTail && this.byTransfers[this.trHead] < this.lo) this.trHead++
    if (this.lo >= this.hi) return null
    const cheapest = this.aggs[this.byPrice[this.priceHead]]
    return {
      price: cheapest.price,
      transfersAtMin: cheapest.transfersAtMin,
      minTransfers: this.aggs[this.byTransfers[this.trHead]].minTransfers,
      count: this.counts[this.hi] - this.counts[this.lo],
    }
  }

  private push(k: number) {
    const a = this.aggs[k]
    while (this.priceTail > this.priceHead && !isCheaper(this.aggs[this.byPrice[this.priceTail - 1]], a))
      this.priceTail--
    this.byPrice[this.priceTail++] = k
    while (this.trTail > this.trHead && this.aggs[this.byTransfers[this.trTail - 1]].minTransfers >= a.minTransfers)
      this.trTail--
    this.byTransfers[this.trTail++] = k
  }
}

// Переход i: из состояний «прилёт в город в момент a» по рёбрам в следующий город.
function extend(state: Arrivals, edges: Edge[], rule: StayRule | null): Arrivals {
  const next: Arrivals = new Map()
  extendInto(state, edges, rule, next, (e) => e.arr)
  return next
}

// То же, но результат сливается в into по ключу keyOf(ребро): так последний
// переход пишет сразу в гистограмму длины поездки, минуя карту прилётов.
function extendInto(
  state: Arrivals,
  edges: Edge[],
  rule: StayRule | null,
  into: Map<number, Agg>,
  keyOf: (e: Edge) => number,
) {
  const window = new ArrivalWindow(state, rule)
  let dep = NaN
  let acc: Agg | null = null
  for (const e of edges) {
    if (e.dep !== dep) {
      dep = e.dep
      acc = window.fold(e)
    }
    if (!acc) continue
    const withEdge: Agg = {
      price: acc.price + e.price,
      transfersAtMin: acc.transfersAtMin + e.transfers,
      minTransfers: acc.minTransfers + e.transfers,
      count: acc.count,
    }
    const key = keyOf(e)
    into.set(key, merge(into.get(key), withEdge))
  }
}

// --- Конец последовательности: гистограмма по длине поездки ---

type TripHist = Map<number, Agg> // длина поездки → агрегат цепочек этой длины

// Длина поездки — от дня первого вылета до дня прилёта в финал (как _trip_days на бэке).
function tripDaysOf(firstDep: number, arriveDay: number): number {
  return Math.max(1, arriveDay - firstDep)
}

// Последний переход: сразу в гистограмму по длине поездки.
function finalHist(state: State, edges: Edge[], rule: StayRule | null): TripHist {
  const hist: TripHist = new Map()
  for (const [firstDep, arrivals] of state) {
    extendInto(arrivals, edges, rule, hist, (e) => tripDaysOf(firstDep, e.arrDay))
  }
  return hist
}

// Маршрут из одного перехода: состояние после departFirst → гистограмма.
function stateHist(state: State): TripHist {
  const hist: TripHist = new Map()
  for (const [firstDep, arrivals] of state) {
    for (const [arrive, agg] of arrivals) {
      const days = tripDaysOf(firstDep, dayOf(arrive))
      hist.set(days, merge(hist.get(days), agg))
    }
  }
  return hist
}

// Гистограммы всех последовательностей плоскими колонками: записей — миллионы,
// объект на запись занял бы в несколько раз больше памяти.
interface LeafTable {
  codes: string[][]
  start: Int32Array // записи последовательности k — [start[k], start[k + 1])
  days: Int16Array // длина поездки, по возрастанию внутри последовательности
  price: Float64Array
  transfersAtMin: Int16Array
  minTransfers: Int16Array
  count: Float64Array
}

class LeafTableBuilder {
  private readonly codes: string[][] = []
  private readonly start: number[] = [0]
  private readonly days: number[] = []
  private readonly price: number[] = []
  private readonly transfersAtMin: number[] = []
  private readonly minTransfers: number[] = []
  private readonly count: number[] = []

  add(codes: string[], hist: TripHist) {
    if (hist.size === 0) return
    for (const d of Array.from(hist.keys()).sort((a, b) => a - b)) {
      const agg = hist.get(d) as Agg
      this.days.push(d)
      this.price.push(agg.price)
      this.transfersAtMin.push(agg.transfersAtMin)
      this.minTransfers.push(agg.minTransfers)
      this.count.push(agg.count)
    }
    this.codes.push(codes)
    this.start.push(this.days.length)
  }

  build(): LeafTable {
    return {
      codes: this.codes,
      start: Int32Array.from(this.start),
      days: Int16Array.from(this.days),
      price: Float64Array.from(this.price),
      transfersAtMin: Int16Array.from(this.transfersAtMin),
      minTransfers: Int16Array.from(this.minTransfers),
      count: Float64Array.from(this.count),
    }
  }
}

// Закрытие цепочки в финальном городе: фильтр общей длины поездки.
function finish(t: LeafTable, k: number, [minDays, maxDays]: [number, number]): Agg | undefined {
  let total: Agg | undefined
  for (let r = t.start[k]; r < t.start[k + 1]; r++) {
    const d = t.days[r]
    if (d > maxDays) break
    if (d < minDays) continue
    const agg: Agg = {
      price: t.price[r],
      transfersAtMin: t.transfersAtMin[r],
      minTransfers: t.minTransfers[r],
      count: t.count[r],
    }
    total = merge(total, agg)
  }
  return total
}

function summarize(t: LeafTable, tripLength: [number, number]): OverviewResult {
  const combos: CityCombo[] = []
  let totalCount = 0
  for (let k = 0; k < t.codes.length; k++) {
    const agg = finish(t, k, tripLength)
    if (!agg) continue
    combos.push({
      key: t.codes[k].join('>'),
      codes: t.codes[k],
      minPrice: agg.price,
      transfersAtMin: agg.transfersAtMin,
      minTransfers: agg.minTransfers,
      count: agg.count,
    })
    totalCount += agg.count
  }
  combos.sort((a, b) => a.minPrice - b.minPrice)
  return { combos, totalCount }
}

// Всё, кроме длины поездки, влияет на динамику; длина — только на summarize.
function dynamicsKey(f: PlannerFilters): string {
  return JSON.stringify([f.cities, f.transitions])
}

// Расчёт обзора по одному графу. Помнит гистограммы последнего полного расчёта:
// если поменялась только длина поездки, динамика не пересчитывается.
export class OverviewEngine {
  private readonly g: PreparedGraph
  private cache: { key: string; leaves: LeafTable } | null = null

  constructor(g: PreparedGraph) {
    this.g = g
  }

  // Генератор: yield — точка, где расчёт можно приостановить или бросить.
  // Кэш обновляется только по завершении, брошенный расчёт его не портит.
  *steps(f: PlannerFilters): Generator<void, OverviewResult> {
    const key = dynamicsKey(f)
    if (this.cache?.key !== key) {
      const leaves = new LeafTableBuilder()
      yield* this.collectLeaves(f, leaves)
      this.cache = { key, leaves: leaves.build() }
    }
    return summarize(this.cache.leaves, f.tripLength)
  }

  private *collectLeaves(f: PlannerFilters, out: LeafTableBuilder): Generator<void> {
    const g = this.g
    const last = g.legs.length
    if (last === 0) return
    const legs = g.legs.map((edges, i) => indexLeg(edges, f, i))
    const rules = Array.from({ length: last }, (_, i) => stayRule(f, i))
    const reach = reachability(legs, f)
    // Переход i ≥ 1 (первый — в цикле ниже, по шагу генератора на ветку).
    const dfs = (i: number, city: string, state: State, codes: string[], visited: Set<string>) => {
      if (i === last) return out.add(codes, stateHist(state)) // маршрут из одного перехода
      for (const [to, edges] of legs[i].get(city) ?? []) {
        if (to === city) continue // без петель
        if (g.any[i + 1] && visited.has(to)) continue // «любой» не ведём в уже посещённый
        if (!isAllowed(f, i + 1, to) || !reach[i + 1].has(to)) continue
        const nextCodes = [...codes, to]
        if (i === last - 1) {
          out.add(nextCodes, finalHist(state, edges, rules[i]))
          continue
        }
        const next = extendState(state, edges, rules[i])
        if (next.size) dfs(i + 1, to, next, nextCodes, new Set(visited).add(to))
      }
    }

    for (const start of legs[0].keys()) {
      if (!isAllowed(f, 0, start) || !reach[0].has(start)) continue
      // Шаг генератора — одна ветка первого перехода (MOW → X → …).
      for (const [to, edges] of legs[0].get(start) ?? []) {
        if (to === start || !isAllowed(f, 1, to) || !reach[1].has(to)) continue // как в dfs
        const next = departFirst(g, edges)
        if (next.size) dfs(1, to, next, [start, to], new Set([start, to]))
        yield
      }
    }
  }
}

// Синхронный расчёт целиком (без воркера).
export function computeOverview(g: PlanGraph, f: PlannerFilters): OverviewResult {
  const steps = new OverviewEngine(prepareGraph(g)).steps(f)
  for (;;) {
    const r = steps.next()
    if (r.done) return r.value
  }
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
