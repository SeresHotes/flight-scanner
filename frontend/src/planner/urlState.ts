// Состояние планировщика ↔ query-строка: маршрут (какие данные собираем) и
// фильтры (как отбираем) кладём в URL, чтобы результат можно было шарить ссылкой
// и восстанавливать из закладки. Аналог lib/urlParams.ts, но под цепочку остановок.

import type { AirportOption } from '../data/airports'
import type { CityFilter, PlannerFilters, PlannerStop, StopKind, TransitionFilter } from './types'

// Разделители. Ни один не встречается в IATA-кодах, названиях городов, метках
// аэропортов и датах, поэтому разбор однозначен:
const F = '~' //  поля внутри одной остановки/фильтра
const A = '|' //  между городами-кандидатами одной остановки
const AF = '^' // поля одного аэропорта

// --- Маршрут (stops) ---

function encodeAirport(a: AirportOption): string {
  // code ^ city ^ flag ^ label
  return [a.code, a.city, a.flag ?? '', a.label].join(AF)
}

function decodeAirport(raw: string): AirportOption | null {
  const [code, city, flag, label] = raw.split(AF)
  if (!code) return null
  return { code, city: city ?? '', flag: flag || undefined, label: label || `${city} (${code})` }
}

function encodeStop(s: PlannerStop): string {
  // kind ~ winStart ~ winEnd ~ airport|airport|…
  const airports = s.airports.map(encodeAirport).join(A)
  return [s.kind, s.window[0] ?? '', s.window[1] ?? '', airports].join(F)
}

function decodeStop(raw: string, id: string): PlannerStop | null {
  const parts = raw.split(F)
  if (parts.length < 4) return null
  const [kind, winA, winB, airportsRaw] = parts
  const k: StopKind = kind === 'any' ? 'any' : 'cities'
  const airports =
    k === 'cities' && airportsRaw
      ? airportsRaw.split(A).map(decodeAirport).filter((a): a is AirportOption => a !== null)
      : []
  return { id, kind: k, airports, window: [winA ?? '', winB ?? ''] }
}

// --- Фильтры ---

// allowedCodes: null — любые города → маркер '*'; массив — коды через запятую
// (пустой массив «никакие» кодируется пустой строкой).
const ANY_CITIES = '*'

function encodeCity(c: CityFilter): string {
  const cover = c.mustCover ?? ['', '']
  const allowed = c.allowedCodes === null ? ANY_CITIES : c.allowedCodes.join(',')
  // minStay ~ maxStay ~ coverStart ~ coverEnd ~ requireWeekend(1/0) ~ allowedCodes
  return [c.minStay, c.maxStay, cover[0], cover[1], c.requireWeekend ? '1' : '0', allowed].join(F)
}

function decodeCity(raw: string): CityFilter | null {
  const parts = raw.split(F)
  if (parts.length < 5) return null
  const [minStay, maxStay, coverA, coverB, weekend, allowedRaw] = parts
  const mustCover: [string, string] | null = coverA && coverB ? [coverA, coverB] : null
  // Старые ссылки (без 6-го поля) → allowedCodes null (любые города).
  const allowedCodes =
    allowedRaw === undefined || allowedRaw === ANY_CITIES
      ? null
      : allowedRaw === ''
        ? []
        : allowedRaw.split(',')
  return {
    minStay: Number(minStay),
    maxStay: Number(maxStay),
    mustCover,
    requireWeekend: weekend === '1',
    allowedCodes,
  }
}

function encodeTransition(t: TransitionFilter): string {
  return [t.maxTransfers, t.maxTravelMinutes].join(F)
}

function decodeTransition(raw: string): TransitionFilter | null {
  const parts = raw.split(F)
  if (parts.length < 2) return null
  return { maxTransfers: Number(parts[0]), maxTravelMinutes: Number(parts[1]) }
}

// --- Сборка / разбор query ---

// Собирает query из текущего состояния. Фильтры пишем только если они есть
// (появляются после сбора) — URL всегда отражает то, что видно на экране.
export function buildPlannerQuery(stops: PlannerStop[], filters: PlannerFilters | null): URLSearchParams {
  const sp = new URLSearchParams()
  for (const s of stops) sp.append('st', encodeStop(s))
  if (filters) {
    for (const c of filters.cities) sp.append('cf', encodeCity(c))
    for (const t of filters.transitions) sp.append('tf', encodeTransition(t))
    sp.set('tl', `${filters.tripLength[0]}${F}${filters.tripLength[1]}`)
  }
  return sp
}

export interface ParsedPlannerQuery {
  stops: PlannerStop[] | null
  filters: PlannerFilters | null
}

// Разбирает query. stops === null означает «в URL ничего нет» → страница берёт
// значения по умолчанию. Фильтры возвращаем, только если они консистентны с
// числом остановок (иначе применять их не к чему).
export function parsePlannerQuery(sp: URLSearchParams, nextId: () => string): ParsedPlannerQuery {
  const rawStops = sp.getAll('st')
  const stops: PlannerStop[] = []
  for (const raw of rawStops) {
    const s = decodeStop(raw, nextId())
    if (s) stops.push(s)
  }

  let filters: PlannerFilters | null = null
  const cities = sp.getAll('cf').map(decodeCity)
  const transitions = sp.getAll('tf').map(decodeTransition)
  const tl = sp.get('tl')?.split(F)

  const filtersComplete =
    stops.length > 0 &&
    cities.length === stops.length &&
    transitions.length === Math.max(0, stops.length - 1) &&
    tl?.length === 2 &&
    cities.every(Boolean) &&
    transitions.every(Boolean)

  if (filtersComplete && tl) {
    filters = {
      cities: cities as CityFilter[],
      transitions: transitions as TransitionFilter[],
      tripLength: [Number(tl[0]), Number(tl[1])],
    }
  }

  return { stops: stops.length ? stops : null, filters }
}
