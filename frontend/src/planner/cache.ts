// Кэш собранных маршрутов в localStorage: чтобы переход по ссылке (или перезагрузка)
// показывал уже собранные данные, а не запускал сбор заново. Ключ — маршрут (набор
// остановок), значение — цепочки + момент сбора. Пока это мок; когда появится
// backend, кэш заменится реальным «что уже собрано и когда» (см. types.ts контракт).

import type { Itinerary, PlanGraph, PlannerBounds, PlannerStop } from './types'
import { buildPlannerQuery } from './urlState'

const KEY = 'planner:collected:v1'
const MAX_ENTRIES = 8 // держим только несколько последних маршрутов

export interface CachedCollection {
  itineraries: Itinerary[]
  graph?: PlanGraph | null // нет у записей, сохранённых до режима «наборы городов»
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

export function getCached(stops: PlannerStop[], bounds: PlannerBounds): CachedCollection | null {
  return readStore()[routeKey(stops, bounds)] ?? null
}

export function putCached(
  stops: PlannerStop[],
  bounds: PlannerBounds,
  itineraries: Itinerary[],
  graph: PlanGraph | null,
  collectedAt: string,
): void {
  const store = readStore()
  store[routeKey(stops, bounds)] = { itineraries, graph, collectedAt }

  // Ограничиваем размер: выкидываем самые старые по времени сбора.
  const keys = Object.keys(store)
  if (keys.length > MAX_ENTRIES) {
    keys.sort((a, b) => store[a].collectedAt.localeCompare(store[b].collectedAt))
    for (const k of keys.slice(0, keys.length - MAX_ENTRIES)) delete store[k]
  }
  if (writeStore(store)) return
  // Граф на широких маршрутах крупный — не влез в квоту. Храним графы только у
  // текущей записи; если и так не влезло — без графов вовсе (обзор попросит пересбор).
  for (const k of Object.keys(store)) if (k !== routeKey(stops, bounds)) delete store[k].graph
  if (writeStore(store)) return
  for (const k of Object.keys(store)) delete store[k].graph
  writeStore(store)
}
