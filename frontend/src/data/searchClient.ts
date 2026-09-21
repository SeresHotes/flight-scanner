import type { FlightData } from '../types'

// Параметры запроса «что мы хотим посмотреть/скачать»: A/B и диапазоны дат плеч.
// Ровно этот объект уходит телом в POST /api/search.
export interface SearchParams {
  origin: string
  destination: string
  leg1_dates: [string, string] // плечо ТУДА: [start, end] вылета
  leg2_dates: [string, string] // плечо ОБРАТНО
  min_stay: number
}

export interface Estimate {
  requests: number
  seconds: number
}

export type SearchResult =
  | { status: 'ok'; data: FlightData }
  // Идёт фоновый сбор недостающих данных — фронт следит за прогрессом job.
  | { status: 'collecting'; job_id: string }
  // Данных нет, но собрать можно — ждём подтверждения пользователя (с оценкой объёма).
  | { status: 'needs_collection'; origin: string; destination: string; estimate: Estimate }
  // Собрать нельзя (нет дат / слишком широкий диапазон).
  | { status: 'needs_backend'; origin: string; destination: string; message?: string; estimate?: Estimate }

const API_BASE = import.meta.env.VITE_API_BASE ?? '/api'

// Реальный вызов бэкенда. Контракт ответа совпадает с SearchResult:
// {status:'ok', data:{meta,trips}} | {status:'needs_backend', origin, destination}.
export async function search(params: SearchParams): Promise<SearchResult> {
  const res = await fetch(`${API_BASE}/search`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params),
  })
  if (!res.ok) {
    throw new Error(`Ошибка поиска: ${res.status}`)
  }
  return (await res.json()) as SearchResult
}

// Явный запуск сбора (после подтверждения). Возвращает collecting/ok/needs_backend.
// Путь /gather (не /collect) — блокировщики рекламы режут "collect" как трекер.
export async function startCollection(params: SearchParams): Promise<SearchResult> {
  const res = await fetch(`${API_BASE}/gather`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params),
  })
  if (!res.ok) {
    throw new Error(`Ошибка запуска сбора: ${res.status}`)
  }
  return (await res.json()) as SearchResult
}
