// Кэш собранных маршрутов в localStorage: чтобы переход по ссылке (или перезагрузка)
// показывал уже собранные данные, а не запускал сбор заново. Ключ — маршрут (набор
// остановок), значение — цепочки + момент сбора. Пока это мок; когда появится
// backend, кэш заменится реальным «что уже собрано и когда» (см. types.ts контракт).

import type { PlannerBounds, PlannerStop } from './types'
import { isCompactResult, type CompactResult } from './compact'
import { buildPlannerQuery } from './urlState'

const KEY = 'planner:collected:v2' // v2 — компактный результат (compact.ts)
const MAX_ENTRIES = 8 // держим только несколько последних маршрутов

export interface CachedCollection {
  result: CompactResult
  collectedAt: string // ISO
}

type Store = Record<string, CachedCollection>

// Ключ маршрута — та же кодировка остановок и границ, что и в URL (без фильтров):
// данные зависят от набора остановок, их дат И движковых границ (maxResults/maxCost).
function routeKey(stops: PlannerStop[], bounds: PlannerBounds): string {
  return buildPlannerQuery(stops, null, bounds).toString()
}

function readStore(): Store {
  try {
    const raw = localStorage.getItem(KEY)
    return raw ? (JSON.parse(raw) as Store) : {}
  } catch {
    return {} // localStorage недоступен/повреждён — работаем без кэша
  }
}

function writeStore(store: Store): boolean {
  try {
    localStorage.setItem(KEY, JSON.stringify(store))
    return true
  } catch {
    return false // переполнение квоты или приватный режим
  }
}

// Старый формат (полные Itinerary) только занимает квоту — убираем.
try {
  localStorage.removeItem('planner:collected:v1')
} catch {
  // localStorage недоступен
}

export function getCached(stops: PlannerStop[], bounds: PlannerBounds): CachedCollection | null {
  const hit = readStore()[routeKey(stops, bounds)]
  return hit && isCompactResult(hit.result) ? hit : null
}

export function putCached(
  stops: PlannerStop[],
  bounds: PlannerBounds,
  result: CompactResult,
  collectedAt: string,
): void {
  const store = readStore()
  store[routeKey(stops, bounds)] = { result, collectedAt }

  // Ограничиваем размер: выкидываем самые старые по времени сбора. Большой результат
  // (сотни тысяч цепочек — мегабайты) может не влезть в квоту localStorage (~5 МБ) —
  // тогда вытесняем старые маршруты по одному; не влез и один — просто не кэшируем.
  const keys = Object.keys(store)
  keys.sort((a, b) => store[a].collectedAt.localeCompare(store[b].collectedAt))
  while (keys.length > MAX_ENTRIES) delete store[keys.shift() as string]
  while (!writeStore(store) && keys.length > 0) delete store[keys.shift() as string]
}
