// Единый запрос планировщика (зеркало core/planquery.PlanQuery): скелет остановок
// + все фильтры. Пользователь задаёт его целиком ДО загрузки; бэк собирает данные
// и строит наборы/маршруты под него. Кладём в URL, чтобы страницы результатов
// могли показать и поправить запрос, а ссылку — расшарить.

import type { AirportOption } from '../data/airports'
import type { PlannerStop, StopKind } from './types'

export type BaggageMode = 'any' | 'included' | 'none'

export interface CityQuery {
  minStay: number
  maxStay: number | null // null — без потолка
  mustCover: [string, string] | null
  requireWeekend: boolean
}

export interface LegQuery {
  maxTransfers: number // -1 — любые
  minLayoverMin: number // минимум ожидания на каждой пересадке
  travelMin: [number, number | null] // суммарная длительность перелёта, [lo, hi|∞]
  baggage: BaggageMode
  hiddenCity: boolean
}

export interface PlanQuery {
  stops: PlannerStop[]
  cities: CityQuery[] // == stops
  legs: LegQuery[] // == stops - 1
  tripLength: [number, number | null]
  maxCost: number | null // бюджет поездки: фильтр и коридор загрузки «любых» городов
}

// Границы слайдеров (запрос задаётся до данных, поэтому границы фиксированные).
// Верхняя граница слайдера = «без ограничения» (null в запросе).
export const STAY_MAX = 30 // дней в городе
export const TRAVEL_MAX_MIN = 48 * 60 // длительность перелёта, минут
export const TRIP_MAX = 60 // длина поездки, дней

export const openCity = (): CityQuery => ({ minStay: 0, maxStay: null, mustCover: null, requireWeekend: false })
export const openLeg = (): LegQuery => ({
  maxTransfers: -1,
  minLayoverMin: 0,
  travelMin: [0, null],
  baggage: 'any',
  hiddenCity: true,
})

// Подгоняет длины массивов фильтров под число остановок (новые — открытые).
export function fitFilters(q: PlanQuery): PlanQuery {
  const n = q.stops.length
  const cities = Array.from({ length: n }, (_, i) => q.cities[i] ?? openCity())
  const legs = Array.from({ length: Math.max(0, n - 1) }, (_, i) => q.legs[i] ?? openLeg())
  return { ...q, cities, legs }
}

// Режим показа (как planquery.PlanQuery.mode): наборы городов, если где-то
// «любой» (набор городов заранее не известен), иначе сразу маршруты.
export function queryMode(q: PlanQuery): 'combos' | 'routes' {
  return q.stops.some((s) => s.kind === 'any') ? 'combos' : 'routes'
}

// Тело POST /api/plan/run.
export function toApi(q: PlanQuery) {
  return {
    stops: q.stops.map((s) => ({
      kind: s.kind,
      codes: s.airports.map((a) => a.code),
      window: s.window,
      radiusKm: s.radiusKm || 0,
    })),
    cities: q.cities.map((c) => ({
      minStay: c.minStay,
      maxStay: c.maxStay,
      mustCover: c.mustCover && c.mustCover[0] && c.mustCover[1] ? c.mustCover : null,
      requireWeekend: c.requireWeekend,
    })),
    legs: q.legs.map((l) => ({
      maxTransfers: l.maxTransfers,
      minLayoverMin: l.minLayoverMin,
      travelMin: l.travelMin,
      baggage: l.baggage,
      hiddenCity: l.hiddenCity,
    })),
    tripLength: q.tripLength,
    maxCost: q.maxCost,
  }
}

// --- URL ---
// Компактно, без спецсимволов ('.' и '-' URLSearchParams не кодирует).
//   st = kind.codes(-).winA.winB.radius   — по остановке (radius — км переезда, нет — 0)
//   cf = minStay.maxStay.coverA.coverB.wk — по остановке ('' = ∞ / нет)
//   lf = maxTransfers.minLayover.travelLo.travelHi.baggage(a|i|n).hidden(1|0) — по переходу
//   tl = lo.hi   mc = бюджет поездки

const F = '.'
const L = '-'
const BAG: Record<BaggageMode, string> = { any: 'a', included: 'i', none: 'n' }
const BAG_BACK: Record<string, BaggageMode> = { a: 'any', i: 'included', n: 'none' }

const numOrNull = (s: string | undefined): number | null => (s === undefined || s === '' ? null : Number(s))

export function encodeQuery(q: PlanQuery): URLSearchParams {
  const sp = new URLSearchParams()
  for (const s of q.stops) {
    const parts = [s.kind === 'any' ? 'any' : 'c', s.airports.map((a) => a.code).join(L), s.window[0], s.window[1]]
    if (s.radiusKm) parts.push(String(s.radiusKm))
    sp.append('st', parts.join(F))
  }
  for (const c of q.cities) {
    const cover = c.mustCover ?? ['', '']
    sp.append('cf', [c.minStay, c.maxStay ?? '', cover[0], cover[1], c.requireWeekend ? 1 : 0].join(F))
  }
  for (const l of q.legs) {
    sp.append('lf', [l.maxTransfers, l.minLayoverMin, l.travelMin[0], l.travelMin[1] ?? '', BAG[l.baggage], l.hiddenCity ? 1 : 0].join(F))
  }
  sp.set('tl', `${q.tripLength[0]}${F}${q.tripLength[1] ?? ''}`)
  if (q.maxCost !== null) sp.set('mc', String(q.maxCost))
  return sp
}

// Города приходят «заготовками» (только код); имена дорезолвит страница.
export function decodeQuery(sp: URLSearchParams, nextId: () => string): PlanQuery | null {
  const stops: PlannerStop[] = []
  for (const raw of sp.getAll('st')) {
    const [kind, codesRaw, winA, winB, radius] = raw.split(F)
    if (!kind) continue
    const k: StopKind = kind === 'any' ? 'any' : 'cities'
    const airports: AirportOption[] =
      k === 'cities' && codesRaw ? codesRaw.split(L).filter(Boolean).map((code) => ({ code, city: '', label: code })) : []
    stops.push({ id: nextId(), kind: k, airports, window: [winA ?? '', winB ?? ''], radiusKm: Number(radius) || 0 })
  }
  if (!stops.length) return null
  const cities = sp.getAll('cf').map((raw): CityQuery => {
    const [minStay, maxStay, coverA, coverB, wk] = raw.split(F)
    return {
      minStay: Number(minStay) || 0,
      maxStay: numOrNull(maxStay),
      mustCover: coverA && coverB ? [coverA, coverB] : null,
      requireWeekend: wk === '1',
    }
  })
  const legs = sp.getAll('lf').map((raw): LegQuery => {
    const [mt, lay, lo, hi, bag, hid] = raw.split(F)
    return {
      maxTransfers: mt === undefined ? -1 : Number(mt),
      minLayoverMin: Number(lay) || 0,
      travelMin: [Number(lo) || 0, numOrNull(hi)],
      baggage: BAG_BACK[bag ?? 'a'] ?? 'any',
      hiddenCity: hid !== '0',
    }
  })
  const tl = sp.get('tl')?.split(F)
  return fitFilters({
    stops,
    cities,
    legs,
    tripLength: tl ? [Number(tl[0]) || 0, numOrNull(tl[1])] : [0, null],
    maxCost: numOrNull(sp.get('mc') ?? undefined),
  })
}

// Краткая подпись запроса: коды по остановкам, «любой» → 🌍.
export function routeSummary(stops: PlannerStop[]): string {
  return stops
    .map((s) => (s.kind === 'any' ? '🌍 любой' : s.airports.map((a) => a.city || a.code).join(' / ') || '—'))
    .join(' → ')
}
