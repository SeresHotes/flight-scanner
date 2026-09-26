// Кэш собранных маршрутов в IndexedDB: чтобы переход по ссылке (или перезагрузка)
// показывал уже собранные данные, а не запускал сбор заново. Ключ — маршрут (набор
// остановок), значение — компактный результат (compact.ts) + момент сбора.
//
// Раньше кэш жил в localStorage, но его квота (~5 МБ на весь сайт) вмещала один
// крупный результат — остальные маршруты вытеснялись, и фактически хранился только
// последний. У IndexedDB квота — сотни МБ, так что держим несколько наборов данных.

import type { PlannerBounds, PlannerStop } from './types'
import { isCompactResult, type CompactResult } from './compact'
import { buildPlannerQuery } from './urlState'

const DB_NAME = 'planner'
const STORE = 'collected'
const MAX_ENTRIES = 20 // держим несколько последних маршрутов
const LEGACY_KEYS = ['planner:collected:v1', 'planner:collected:v2']

export interface CachedCollection {
  result: CompactResult // result.graph нет у записей, сохранённых без графа (квота)
  collectedAt: string // ISO
}

// Ключ маршрута — та же кодировка остановок и границ, что и в URL (без фильтров):
// данные зависят от набора остановок, их дат И движковых границ (maxResults/maxCost).
function routeKey(stops: PlannerStop[], bounds: PlannerBounds): string {
  return buildPlannerQuery(stops, null, bounds).toString()
}

function promisify<T>(req: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result)
    req.onerror = () => reject(req.error)
  })
}

function txDone(tx: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve()
    tx.onerror = () => reject(tx.error)
    tx.onabort = () => reject(tx.error)
  })
}

let dbPromise: Promise<IDBDatabase> | null = null

function openDb(): Promise<IDBDatabase> {
  if (!dbPromise) {
    const req = indexedDB.open(DB_NAME, 1)
    req.onupgradeneeded = () => req.result.createObjectStore(STORE)
    dbPromise = promisify(req).then(async (db) => {
      await migrateFromLocalStorage(db)
      return db
    })
    dbPromise.catch(() => {
      dbPromise = null // IndexedDB недоступен — следующий вызов попробует снова
    })
  }
  return dbPromise
}

// Переносим записи старого localStorage-кэша, чтобы не терять уже собранное, и
// освобождаем квоту localStorage (там ещё история запросов).
async function migrateFromLocalStorage(db: IDBDatabase): Promise<void> {
  let legacy: Record<string, CachedCollection> = {}
  try {
    const raw = localStorage.getItem('planner:collected:v2')
    legacy = raw ? JSON.parse(raw) : {}
  } catch {
    // localStorage недоступен/повреждён — переносить нечего
  }
  const entries = Object.entries(legacy).filter(([, v]) => isCompactResult(v?.result))
  if (entries.length) {
    const tx = db.transaction(STORE, 'readwrite')
    const store = tx.objectStore(STORE)
    for (const [key, value] of entries) store.put(value, key)
    await txDone(tx).catch(() => undefined)
  }
  try {
    for (const key of LEGACY_KEYS) localStorage.removeItem(key)
  } catch {
    // localStorage недоступен
  }
}

export async function getCached(
  stops: PlannerStop[],
  bounds: PlannerBounds,
): Promise<CachedCollection | null> {
  try {
    const db = await openDb()
    const store = db.transaction(STORE, 'readonly').objectStore(STORE)
    const hit = (await promisify(store.get(routeKey(stops, bounds)))) as CachedCollection | undefined
    return hit && isCompactResult(hit.result) ? hit : null
  } catch {
    return null // IndexedDB недоступен (приватный режим и т.п.) — работаем без кэша
  }
}

async function writeEntry(db: IDBDatabase, key: string, value: CachedCollection): Promise<boolean> {
  try {
    const tx = db.transaction(STORE, 'readwrite')
    tx.objectStore(STORE).put(value, key)
    await txDone(tx)
    return true
  } catch {
    return false // переполнение квоты
  }
}

async function deleteEntries(db: IDBDatabase, keys: string[]): Promise<void> {
  if (!keys.length) return
  const tx = db.transaction(STORE, 'readwrite')
  const store = tx.objectStore(STORE)
  for (const key of keys) store.delete(key)
  await txDone(tx)
}

// Прочие ключи — от самого старого сбора к самому свежему. Читаем записи целиком:
// индекса по collectedAt нет, а записей всего несколько десятков.
async function olderKeys(db: IDBDatabase, except: string): Promise<string[]> {
  const store = db.transaction(STORE, 'readonly').objectStore(STORE)
  const [keys, values] = await Promise.all([
    promisify(store.getAllKeys()),
    promisify(store.getAll()) as Promise<CachedCollection[]>,
  ])
  const dated = keys
    .map((key, i) => ({ key: String(key), collectedAt: values[i]?.collectedAt ?? '' }))
    .filter((e) => e.key !== except)
  dated.sort((a, b) => a.collectedAt.localeCompare(b.collectedAt))
  return dated.map((e) => e.key)
}

export async function putCached(
  stops: PlannerStop[],
  bounds: PlannerBounds,
  result: CompactResult,
  collectedAt: string,
): Promise<void> {
  try {
    const db = await openDb()
    const key = routeKey(stops, bounds)
    const older = await olderKeys(db, key)
    await deleteEntries(db, older.splice(0, Math.max(0, older.length - (MAX_ENTRIES - 1))))

    // Не влезло в квоту — вытесняем старые маршруты по одному, затем граф текущего
    // (обзор попросит пересбор); не влезло и так — просто не кэшируем.
    let value: CachedCollection = { result, collectedAt }
    while (!(await writeEntry(db, key, value))) {
      if (older.length) await deleteEntries(db, [older.shift() as string])
      else if (value.result.graph) value = { result: { ...result, graph: null }, collectedAt }
      else return
    }
  } catch {
    // IndexedDB недоступен — работаем без кэша
  }
}
