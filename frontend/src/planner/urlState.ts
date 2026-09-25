// Состояние планировщика ↔ query-строка: маршрут (какие данные собираем) и
// фильтры (как отбираем) кладём в URL, чтобы результат можно было шарить ссылкой
// и восстанавливать из закладки. Аналог lib/urlParams.ts, но под цепочку остановок.
//
// Кодируем компактно и без спецсимволов: разделители '.' и '-' входят в
// «безопасный» набор application/x-www-form-urlencoded, поэтому URLSearchParams
// НЕ превращает их в %2E/%2D. По городам храним только IATA-код — названия и
// флаги подтягиваются из /api/airports при загрузке (см. data/airports.resolveAirports).

import type { AirportOption } from '../data/airports'
import type { CityFilter, PlannerBounds, PlannerFilters, PlannerStop, StopKind, TransitionFilter } from './types'

const F = '.' // разделитель полей (не встречается в кодах, датах и числах)
const L = '-' // разделитель списков: города-кандидаты, allowedCodes (в датах '-' — часть значения, но это отдельное поле)
const ANY_CITIES = '*' // маркер allowedCodes === null (любые города)

// --- Маршрут (stops) ---

// kind . codes . winStart . winEnd   (codes — коды через '-'; окно у концов пустое)
function encodeStop(s: PlannerStop): string {
  const kind = s.kind === 'any' ? 'any' : 'c'
  const codes = s.airports.map((a) => a.code).join(L)
  return [kind, codes, s.window[0] ?? '', s.window[1] ?? ''].join(F)
}

// Города восстанавливаем как «заготовки» (только код); имя/флаг дорезолвит страница.
function decodeStop(raw: string, id: string): PlannerStop | null {
  const [kind, codesRaw, winA, winB] = raw.split(F)
  if (!kind) return null
  const k: StopKind = kind === 'any' ? 'any' : 'cities'
  const airports: AirportOption[] =
    k === 'cities' && codesRaw
      ? codesRaw
          .split(L)
          .filter(Boolean)
          .map((code) => ({ code, city: '', label: code }))
      : []
  return { id, kind: k, airports, window: [winA ?? '', winB ?? ''] }
}

// --- Фильтры ---

// minStay . maxStay . coverStart . coverEnd . requireWeekend(1/0) . allowedCodes
function encodeCity(c: CityFilter): string {
  const cover = c.mustCover ?? ['', '']
  const allowed = c.allowedCodes === null ? ANY_CITIES : c.allowedCodes.join(L)
  return [c.minStay, c.maxStay, cover[0], cover[1], c.requireWeekend ? '1' : '0', allowed].join(F)
}

function decodeCity(raw: string): CityFilter | null {
  const parts = raw.split(F)
  if (parts.length < 5) return null
  const [minStay, maxStay, coverA, coverB, weekend, allowedRaw] = parts
  const mustCover: [string, string] | null = coverA && coverB ? [coverA, coverB] : null
  // Отсутствие поля / '*' → null (любые города); '' → пустой список; иначе коды через '-'.
  const allowedCodes =
    allowedRaw === undefined || allowedRaw === ANY_CITIES
      ? null
      : allowedRaw === ''
        ? []
        : allowedRaw.split(L)
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
// Границы (mr/mc) входят в ключ данных: от них зависит результат сбора, поэтому
// они же участвуют в ключе кэша (см. cache.routeKey).
export function buildPlannerQuery(
  stops: PlannerStop[],
  filters: PlannerFilters | null,
  bounds?: PlannerBounds | null,
): URLSearchParams {
  const sp = new URLSearchParams()
  for (const s of stops) sp.append('st', encodeStop(s))
  if (bounds) {
    sp.set('mr', String(bounds.maxResults))
    if (bounds.maxCost !== null) sp.set('mc', String(bounds.maxCost))
  }
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
  bounds: PlannerBounds | null
}

// Разбирает query. stops === null означает «в URL ничего нет» → страница берёт
// значения по умолчанию. Фильтры возвращаем, только если они консистентны с
// числом остановок (иначе применять их не к чему).
export function parsePlannerQuery(sp: URLSearchParams, nextId: () => string): ParsedPlannerQuery {
  const stops: PlannerStop[] = []
  for (const raw of sp.getAll('st')) {
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

  // Границы: maxResults обязателен (движковый потолок), maxCost — опционально.
  const mrRaw = sp.get('mr')
  const bounds: PlannerBounds | null = mrRaw
    ? { maxResults: Number(mrRaw), maxCost: sp.get('mc') !== null ? Number(sp.get('mc')) : null }
    : null

  return { stops: stops.length ? stops : null, filters, bounds }
}
