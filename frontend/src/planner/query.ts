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
  bypass: LegQuery | null // условия перелёта в обход остановки (если её пропускаем)
  flights: LegQuery | null // блок любых городов: условия всех его перелётов (в блок, внутри, из блока)
}

export interface LegQuery {
  maxTransfers: number // -1 — любые
  minLayoverMin: number // минимум ожидания на каждой пересадке
  travelMin: [number, number | null] // суммарная длительность перелёта, [lo, hi|∞]
  depTime: [number, number] // время суток вылета (местное), минуты [0, DAY_MIN]
  arrTime: [number, number] // время суток прилёта (местное), минуты [0, DAY_MIN]
  baggage: BaggageMode
  hiddenCity: boolean
}

export interface PlanQuery {
  stops: PlannerStop[]
  cities: CityQuery[] // == stops
  legs: LegQuery[] // == stops - 1
  tripLength: [number, number | null]
}

// Границы слайдеров (запрос задаётся до данных, поэтому границы фиксированные).
// Верхняя граница слайдера = «без ограничения» (null в запросе).
export const STAY_MAX = 30 // дней в городе
export const TRAVEL_MAX_MIN = 48 * 60 // длительность перелёта, минут
export const TRIP_MAX = 60 // длина поездки, дней
export const DAY_MIN = 24 * 60 // окно времени суток [0, DAY_MIN] = любое

export const openCity = (): CityQuery => ({ minStay: 0, maxStay: null, mustCover: null, requireWeekend: false, bypass: null, flights: null })
export const openLeg = (): LegQuery => ({
  maxTransfers: -1,
  minLayoverMin: 0,
  travelMin: [0, null],
  depTime: [0, DAY_MIN],
  arrTime: [0, DAY_MIN],
  baggage: 'any',
  hiddenCity: true,
})

// Условия перелёта в обход остановки i по умолчанию — из двух заменяемых плеч: пересадки
// любые (обход обычно с пересадкой), длительность — до суммы потолков, багаж и hidden-city —
// строже из двух, вылет — как у плеча в остановку, прилёт — как у плеча из неё.
export function defaultBypass(q: PlanQuery, i: number): LegQuery {
  const a = q.legs[i - 1] ?? openLeg()
  const b = q.legs[i] ?? openLeg()
  const hi = a.travelMin[1] !== null && b.travelMin[1] !== null ? Math.min(TRAVEL_MAX_MIN, a.travelMin[1] + b.travelMin[1]) : null
  return {
    maxTransfers: -1,
    minLayoverMin: Math.max(a.minLayoverMin, b.minLayoverMin),
    travelMin: [0, hi],
    depTime: a.depTime,
    arrTime: b.arrTime,
    baggage: a.baggage !== 'any' ? a.baggage : b.baggage,
    hiddenCity: a.hiddenCity && b.hiddenCity,
  }
}

// Блок любых городов «от–до» (planner/src/stops.rs is_block).
export const isBlock = (s: { kind: string; count?: [number, number] }): boolean =>
  s.kind === 'any' && !!s.count && (s.count[0] !== 1 || s.count[1] !== 1)

// Название остановки для подписей: города через «/», «любой» или буква точки.
export function stopName(q: PlanQuery, i: number): string {
  const s = q.stops[i]
  if (!s) return '?'
  if (s.kind === 'any') return 'любой город'
  return s.airports.map((a) => a.city || a.code).join(' / ') || String.fromCharCode(65 + i)
}

// Подгоняет длины массивов фильтров под число остановок (новые — открытые).
// Пропуск — только у промежуточных остановок: у концов флаг снимается.
export function fitFilters(q: PlanQuery): PlanQuery {
  const n = q.stops.length
  // пропуск и блок «от–до» — только у промежуточных; у концов снимаются
  const stops = q.stops.map((s, i) =>
    (i === 0 || i === n - 1) && (s.skip || s.count) ? { ...s, skip: false, count: undefined } : s,
  )
  const cities = Array.from({ length: n }, (_, i) => q.cities[i] ?? openCity())
  const legs = Array.from({ length: Math.max(0, n - 1) }, (_, i) => q.legs[i] ?? openLeg())
  return { ...q, stops, cities, legs }
}

// Режим показа (как planquery.PlanQuery.mode): наборы городов, если где-то
// «любой» или несколько городов, иначе сразу маршруты.
export function queryMode(q: PlanQuery): 'combos' | 'routes' {
  return q.stops.some((s) => s.kind === 'any' || s.airports.length > 1) ? 'combos' : 'routes'
}

// Параметр f страниц результата (статус, наборы, маршруты): всё, кроме остановок.
// Джоба = рейсы по остановкам, фильтры и бюджет применяются при чтении.
export function filtersParam(q: PlanQuery): string {
  const { stops: _stops, ...filters } = toApi(q) // eslint-disable-line @typescript-eslint/no-unused-vars
  return JSON.stringify(filters)
}

// Тело POST /api/plan/run.
export function toApi(q: PlanQuery) {
  return {
    stops: q.stops.map((s) => ({
      kind: s.kind,
      codes: s.airports.map((a) => a.code),
      window: s.window,
      radiusKm: s.radiusKm || 0,
      ...(s.skip ? { skip: true } : {}),
      ...(isBlock(s) ? { count: s.count } : {}),
    })),
    cities: q.cities.map((c, i) => ({
      minStay: c.minStay,
      maxStay: c.maxStay,
      mustCover: c.mustCover && c.mustCover[0] && c.mustCover[1] ? c.mustCover : null,
      requireWeekend: c.requireWeekend,
      ...(q.stops[i]?.skip && c.bypass ? { bypass: legApi(c.bypass) } : {}),
      ...(q.stops[i] && isBlock(q.stops[i]) && c.flights ? { flights: legApi(c.flights) } : {}),
    })),
    legs: q.legs.map(legApi),
    tripLength: q.tripLength,
  }
}

function legApi(l: LegQuery) {
  return {
    maxTransfers: l.maxTransfers,
    minLayoverMin: l.minLayoverMin,
    travelMin: l.travelMin,
    depTime: l.depTime,
    arrTime: l.arrTime,
    baggage: l.baggage,
    hiddenCity: l.hiddenCity,
  }
}

// --- URL ---
// Компактно, без спецсимволов ('.' и '-' URLSearchParams не кодирует).
//   st = kind.codes(-).winA.winB.radius[.s]   — по остановке (radius — км переезда, нет — 0;
//        s — можно пропустить)
//   bf = stop.<поля lf>   — условия перелёта в обход пропускаемой остановки stop
//   ff = stop.<поля lf>   — условия перелётов блока любых городов stop
//   в st после radius — флаги: s (можно пропустить), nОТ-ДО (блок любых городов)
//   cf = minStay.maxStay.coverA.coverB.wk — по остановке ('' = ∞ / нет)
//   lf = maxTransfers.minLayover.travelLo.travelHi.baggage(a|i|n).hidden(1|0)[.depLo.depHi.arrLo.arrHi]
//        — по переходу; окна времени суток — только если заданы ('' = край суток)
//   tl = lo.hi   mc = бюджет поездки

const F = '.'
const L = '-'
const BAG: Record<BaggageMode, string> = { any: 'a', included: 'i', none: 'n' }
const BAG_BACK: Record<string, BaggageMode> = { a: 'any', i: 'included', n: 'none' }

export const isOpenDay = (w: [number, number]) => w[0] <= 0 && w[1] >= DAY_MIN
const dayEnc = (w: [number, number]) => [w[0] > 0 ? w[0] : '', w[1] < DAY_MIN ? w[1] : '']
const dayDec = (lo: string | undefined, hi: string | undefined): [number, number] => [
  Math.max(0, Number(lo) || 0),
  hi ? Math.min(DAY_MIN, Number(hi)) : DAY_MIN,
]

const numOrNull = (s: string | undefined): number | null => (s === undefined || s === '' ? null : Number(s))

export function encodeQuery(q: PlanQuery): URLSearchParams {
  const sp = new URLSearchParams()
  for (const s of q.stops) {
    const parts = [s.kind === 'any' ? 'any' : 'c', s.airports.map((a) => a.code).join(L), s.window[0], s.window[1]]
    const block = isBlock(s) && s.count
    if (s.radiusKm || s.skip || block) parts.push(String(s.radiusKm || 0))
    if (s.skip) parts.push('s')
    if (block) parts.push(`n${block[0]}${L}${block[1]}`)
    sp.append('st', parts.join(F))
  }
  for (const c of q.cities) {
    const cover = c.mustCover ?? ['', '']
    sp.append('cf', [c.minStay, c.maxStay ?? '', cover[0], cover[1], c.requireWeekend ? 1 : 0].join(F))
  }
  for (const l of q.legs) sp.append('lf', legEnc(l))
  q.cities.forEach((c, i) => {
    if (q.stops[i]?.skip && c.bypass) sp.append('bf', `${i}${F}${legEnc(c.bypass)}`)
    if (q.stops[i] && isBlock(q.stops[i]) && c.flights) sp.append('ff', `${i}${F}${legEnc(c.flights)}`)
  })
  sp.set('tl', `${q.tripLength[0]}${F}${q.tripLength[1] ?? ''}`)
  return sp
}

function legEnc(l: LegQuery): string {
  const parts: (string | number)[] = [l.maxTransfers, l.minLayoverMin, l.travelMin[0], l.travelMin[1] ?? '', BAG[l.baggage], l.hiddenCity ? 1 : 0]
  if (!isOpenDay(l.depTime) || !isOpenDay(l.arrTime)) parts.push(...dayEnc(l.depTime), ...dayEnc(l.arrTime))
  return parts.join(F)
}

function legDec(raw: string): LegQuery {
  const [mt, lay, lo, hi, bag, hid, dLo, dHi, aLo, aHi] = raw.split(F)
  return {
    maxTransfers: mt === undefined || mt === '' ? -1 : Number(mt),
    minLayoverMin: Number(lay) || 0,
    travelMin: [Number(lo) || 0, numOrNull(hi)],
    depTime: dayDec(dLo, dHi),
    arrTime: dayDec(aLo, aHi),
    baggage: BAG_BACK[bag ?? 'a'] ?? 'any',
    hiddenCity: hid !== '0',
  }
}

// Города приходят «заготовками» (только код); имена дорезолвит страница.
export function decodeQuery(sp: URLSearchParams, nextId: () => string): PlanQuery | null {
  const stops: PlannerStop[] = []
  for (const raw of sp.getAll('st')) {
    const [kind, codesRaw, winA, winB, radius, ...flags] = raw.split(F)
    const n = flags.find((f) => f.startsWith('n'))?.slice(1).split(L).map(Number)
    const count: [number, number] | undefined = n && n.length === 2 && n.every((x) => Number.isFinite(x)) ? [n[0], n[1]] : undefined
    if (!kind) continue
    const k: StopKind = kind === 'any' ? 'any' : 'cities'
    const airports: AirportOption[] =
      k === 'cities' && codesRaw ? codesRaw.split(L).filter(Boolean).map((code) => ({ code, city: '', label: code })) : []
    stops.push({ id: nextId(), kind: k, airports, window: [winA ?? '', winB ?? ''], radiusKm: Number(radius) || 0, ...(flags.includes('s') ? { skip: true } : {}), ...(count && k === 'any' ? { count } : {}) })
  }
  if (!stops.length) return null
  const cities = sp.getAll('cf').map((raw): CityQuery => {
    const [minStay, maxStay, coverA, coverB, wk] = raw.split(F)
    return {
      minStay: Number(minStay) || 0,
      maxStay: numOrNull(maxStay),
      mustCover: coverA && coverB ? [coverA, coverB] : null,
      requireWeekend: wk === '1',
      bypass: null,
      flights: null,
    }
  })
  const legs = sp.getAll('lf').map(legDec)
  const tl = sp.get('tl')?.split(F)
  const q = fitFilters({
    stops,
    cities,
    legs,
    tripLength: tl ? [Number(tl[0]) || 0, numOrNull(tl[1])] : [0, null], // параметр mc (бюджет) удалён — игнорируется
  })
  // условия обхода — после fitFilters: cf в URL может не быть, а cities уже по числу остановок
  for (const [param, field] of [['bf', 'bypass'], ['ff', 'flights']] as const) {
    for (const raw of sp.getAll(param)) {
      const dot = raw.indexOf(F)
      const i = Number(raw.slice(0, dot))
      if (dot > 0 && q.cities[i]) q.cities[i] = { ...q.cities[i], [field]: legDec(raw.slice(dot + 1)) }
    }
  }
  return q
}

// Краткая подпись запроса: коды по остановкам, «любой» → 🌍.
export function routeSummary(stops: PlannerStop[]): string {
  return stops
    .map((s) => (s.kind === 'any' ? '🌍 любой' : s.airports.map((a) => a.city || a.code).join(' / ') || '—'))
    .join(' → ')
}
