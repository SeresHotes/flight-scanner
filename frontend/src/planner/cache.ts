// Кэш собранных маршрутов в localStorage: чтобы переход по ссылке (или перезагрузка)
// показывал уже собранные данные, а не запускал сбор заново. Ключ — маршрут (набор
// остановок), значение — компактный результат (compact.ts) + момент сбора.

import type { PlannerBounds, PlannerStop } from './types'
import { isCompactResult, type CompactResult } from './compact'
import { buildPlannerQuery } from './urlState'

const KEY = 'planner:collected:v2' // v2 — компактный результат (compact.ts)
const MAX_ENTRIES = 8 // держим только несколько последних маршрутов

export interface CachedCollection {
  result: CompactResult // result.graph нет у записей, сохранённых без графа (квота)
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
  const key = routeKey(stops, bounds)
  store[key] = { result, collectedAt }

  // Ограничиваем размер: выкидываем самые старые по времени сбора. Большой результат
  // (сотни тысяч цепочек, широкий граф — мегабайты) может не влезть в квоту
  // localStorage (~5 МБ): вытесняем старые маршруты по одному, затем граф текущего
  // (обзор попросит пересбор); не влезло и так — просто не кэшируем.
  const older = Object.keys(store).filter((k) => k !== key)
  older.sort((a, b) => store[a].collectedAt.localeCompare(store[b].collectedAt))
  while (older.length >= MAX_ENTRIES) delete store[older.shift() as string]
  while (!writeStore(store)) {
    if (older.length) delete store[older.shift() as string]
    else if (store[key]?.result.graph) store[key] = { result: { ...result, graph: null }, collectedAt }
    else {
      delete store[key]
      writeStore(store)
      return
    }
  }
}
