// Кэш собранных маршрутов в localStorage: чтобы переход по ссылке (или перезагрузка)
// показывал уже собранные данные, а не запускал сбор заново. Ключ — маршрут (набор
// остановок), значение — цепочки + момент сбора. Пока это мок; когда появится
// backend, кэш заменится реальным «что уже собрано и когда» (см. types.ts контракт).

import type { Itinerary, PlannerStop } from './types'
import { buildPlannerQuery } from './urlState'

const KEY = 'planner:collected:v1'
const MAX_ENTRIES = 8 // держим только несколько последних маршрутов

export interface CachedCollection {
  itineraries: Itinerary[]
  collectedAt: string // ISO
}

type Store = Record<string, CachedCollection>

// Ключ маршрута — та же кодировка остановок, что и в URL (без фильтров): данные
// зависят только от набора остановок и их дат.
function routeKey(stops: PlannerStop[]): string {
  return buildPlannerQuery(stops, null).toString()
}

function readStore(): Store {
  try {
    const raw = localStorage.getItem(KEY)
    return raw ? (JSON.parse(raw) as Store) : {}
  } catch {
    return {} // localStorage недоступен/повреждён — работаем без кэша
  }
}

function writeStore(store: Store): void {
  try {
    localStorage.setItem(KEY, JSON.stringify(store))
  } catch {
    // Переполнение квоты или приватный режим — молча пропускаем.
  }
}

export function getCached(stops: PlannerStop[]): CachedCollection | null {
  return readStore()[routeKey(stops)] ?? null
}

export function putCached(stops: PlannerStop[], itineraries: Itinerary[], collectedAt: string): void {
  const store = readStore()
  store[routeKey(stops)] = { itineraries, collectedAt }

  // Ограничиваем размер: выкидываем самые старые по времени сбора.
  const keys = Object.keys(store)
  if (keys.length > MAX_ENTRIES) {
    keys.sort((a, b) => store[a].collectedAt.localeCompare(store[b].collectedAt))
    for (const k of keys.slice(0, keys.length - MAX_ENTRIES)) delete store[k]
  }
  writeStore(store)
}
