// Компактный результат сбора (контракт — core/planner.py: _pack_compact).
//
// Цепочек бывает до миллиона, поэтому в памяти они — плоские typed-массивы индексов,
// а не объекты Itinerary: сегменты лежат один раз, цепочка ссылается на них. Объект
// Itinerary собирается только для показанной страницы (materialize).

import type { Segment } from '../types'
import type { Itinerary, ItineraryStop } from './types'

// Как приходит с бэка (и хранится в кэше).
export interface CompactResult {
  format: 'compact-v1'
  count: number
  legs: number
  chain_start: string // ISO прилёта в первый город (начало окна, T00:00:00)
  final_stay_days: number
  any_stops: boolean[]
  cities: Record<string, [string, string]> // код → [город, флаг]
  segments: Segment[]
  chains: number[] // count × legs индексов сегментов, цепочки по возрастанию цены
  days: number[] // count × (legs + 1) дней в городах
  weekend: number[] // count битовых масок «оба выходных» по остановкам
  total_days: number[]
}

export function isCompactResult(x: unknown): x is CompactResult {
  return !!x && (x as CompactResult).format === 'compact-v1'
}

export class ItinerarySet {
  readonly count: number
  readonly legs: number
  readonly stopCount: number
  readonly segments: Segment[]
  private readonly chains: Int32Array
  private readonly days: Int16Array
  private readonly weekend: Int32Array
  private readonly totalDays: Int16Array
  private readonly chainStart: string
  private readonly finalStayDays: number
  private readonly anyStops: boolean[]
  private readonly cities: Record<string, [string, string]>

  constructor(r: CompactResult) {
    this.count = r.count
    this.legs = r.legs
    this.stopCount = r.legs + 1
    this.segments = r.segments
    this.chains = Int32Array.from(r.chains)
    this.days = Int16Array.from(r.days)
    this.weekend = Int32Array.from(r.weekend)
    this.totalDays = Int16Array.from(r.total_days)
    this.chainStart = r.chain_start
    this.finalStayDays = r.final_stay_days
    this.anyStops = r.any_stops
    this.cities = r.cities
  }

  segment(n: number, k: number): Segment {
    return this.segments[this.chains[n * this.legs + k]]
  }

  stopCode(n: number, k: number): string {
    return k === 0 ? this.segment(n, 0).origin : this.segment(n, k - 1).destination
  }

  stopDays(n: number, k: number): number {
    return this.days[n * this.stopCount + k]
  }

  weekendCovered(n: number, k: number): boolean {
    return ((this.weekend[n] >> k) & 1) === 1
  }

  totalDaysOf(n: number): number {
    return this.totalDays[n]
  }

  arrive(n: number, k: number): string {
    return k === 0 ? this.chainStart : this.segment(n, k - 1).arrival_at
  }

  depart(n: number, k: number): string {
    if (k < this.legs) return this.segment(n, k).departure_at
    // Финал: у последнего города конца окна нет — дефолтное пребывание (как на бэке).
    return `${shiftDate(this.arrive(n, k).slice(0, 10), this.finalStayDays)}T00:00:00`
  }

  cityOf(code: string): { city: string; flag: string } {
    const c = this.cities[code]
    return { city: c?.[0] ?? code, flag: c?.[1] ?? '' }
  }

  // Полный объект Itinerary — только для отрисовки карточки.
  materialize(n: number): Itinerary {
    const segments = Array.from({ length: this.legs }, (_, k) => this.segment(n, k))
    const stops: ItineraryStop[] = Array.from({ length: this.stopCount }, (_, k) => {
      const code = this.stopCode(n, k)
      const { city, flag } = this.cityOf(code)
      return {
        code,
        city,
        flag,
        arrive: this.arrive(n, k),
        depart: this.depart(n, k),
        days: this.stopDays(n, k),
        weekendCovered: this.weekendCovered(n, k),
        resolvedFromAny: this.anyStops[k] ?? false,
      }
    })
    let price = 0
    let transfers = 0
    let minutes = 0
    for (const s of segments) {
      price += s.price || 0
      transfers += s.transfers || 0
      minutes += s.duration || 0
    }
    return {
      id: n + 1,
      stops,
      segments,
      total_price: price,
      total_days: this.totalDaysOf(n),
      total_transfers: transfers,
      travel_minutes: minutes,
    }
  }
}

function shiftDate(ymd: string, days: number): string {
  const d = new Date(`${ymd}T00:00:00Z`)
  d.setUTCDate(d.getUTCDate() + days)
  return d.toISOString().slice(0, 10)
}
