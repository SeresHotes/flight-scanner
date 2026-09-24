// История последних запросов планировщика — хранится в localStorage браузера.
// Запрос дедупим по ПОЛНОМУ содержимому (города + окна дат): повтор того же
// запроса не плодит запись, а поднимает существующую наверх и обновляет время.

import type { AirportOption } from '../data/airports'
import type { PlannerStop } from './types'

const STORAGE_KEY = 'planner.recentSearches.v1'
const MAX_RECENT = 10

export interface RecentSearch {
  key: string
  stops: PlannerStop[]
  savedAt: number // epoch ms
}

// Ключ дедупа: kind + коды городов + окно по каждой остановке. id остановки в
// ключ не входит — он генерится на лету и к смыслу запроса не относится.
export function requestKey(stops: PlannerStop[]): string {
  const shape = stops.map((s) => ({
    k: s.kind,
    a: s.airports.map((x) => x.code),
    w: s.window,
  }))
  return JSON.stringify(shape)
}

function isAirport(x: unknown): x is AirportOption {
  return (
    typeof x === 'object' && x !== null &&
    typeof (x as AirportOption).code === 'string' &&
    typeof (x as AirportOption).label === 'string'
  )
}

function isStop(x: unknown): x is PlannerStop {
  if (typeof x !== 'object' || x === null) return false
  const s = x as PlannerStop
  return (
    typeof s.id === 'string' &&
    (s.kind === 'cities' || s.kind === 'any') &&
    Array.isArray(s.airports) && s.airports.every(isAirport) &&
    Array.isArray(s.window) && s.window.length === 2 &&
    typeof s.window[0] === 'string' && typeof s.window[1] === 'string'
  )
}

function isRecentSearch(x: unknown): x is RecentSearch {
  if (typeof x !== 'object' || x === null) return false
  const r = x as RecentSearch
  return (
    typeof r.key === 'string' &&
    typeof r.savedAt === 'number' &&
    Array.isArray(r.stops) && r.stops.length >= 2 && r.stops.every(isStop)
  )
}

export function loadRecent(): RecentSearch[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (!raw) return []
    const parsed: unknown = JSON.parse(raw)
    if (!Array.isArray(parsed)) return []
    return parsed.filter(isRecentSearch).slice(0, MAX_RECENT)
  } catch {
    return [] // битый JSON / недоступный storage — начинаем с чистой истории
  }
}

function persist(list: RecentSearch[]): RecentSearch[] {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(list))
  } catch {
    // приватный режим / переполнение квоты — молча живём без персиста
  }
  return list
}

// Добавить запрос в историю: свежий — первым, дубль по ключу схлопываем,
// режем до MAX_RECENT. now передаётся снаружи ради тестируемости.
export function addRecent(stops: PlannerStop[], now: number): RecentSearch[] {
  const key = requestKey(stops)
  const entry: RecentSearch = { key, stops, savedAt: now }
  const rest = loadRecent().filter((r) => r.key !== key)
  return persist([entry, ...rest].slice(0, MAX_RECENT))
}

export function removeRecent(key: string): RecentSearch[] {
  return persist(loadRecent().filter((r) => r.key !== key))
}
