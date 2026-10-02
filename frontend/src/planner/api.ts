// Клиент API планировщика v2: запуск джобы по единому запросу, статус, страницы
// наборов городов и маршрутов. Вся тяжёлая работа — на бэке (docs/PLANNER_V2.md).

import type { JobStatus } from '../data/jobsApi'
import type { PlanQuery } from './query'
import { filtersParam, toApi } from './query'
import type { Itinerary, PlannerEstimate } from './types'

const API_BASE = import.meta.env.VITE_API_BASE ?? '/api'

export interface RunResponse {
  status: 'collecting' | 'done' | 'invalid' | 'too_wide'
  job_id?: string
  total?: number
  mode?: 'combos' | 'routes'
  reused?: boolean
  message?: string
}

export interface CityCombo {
  codes: string[]
  minPrice: number
  transfersAtMin: number
  minTransfers: number
  count: number
  skipped?: number[] // пропущенные остановки (номера в запросе); codes — только города варианта
  key?: string // ключ набора для …/routes?combos= (коды + метка варианта: ~s1 пропуски, ~n2 города блока)
}

export interface CombosPage {
  status: 'ok' | 'not_ready'
  total: number // наборов (не больше 1000 самых дешёвых)
  totalCount: number // цепочек в выданных наборах
  truncated?: boolean // наборов больше — показаны 1000 самых дешёвых
  incomplete?: boolean // обход наборов остановлен по лимиту шагов — список неполный
  offset: number
  limit: number
  items: CityCombo[]
  cities: Record<string, [string, string]> // код → [город, флаг]
}

export interface RoutesPage {
  status: 'ok' | 'not_ready'
  count: number // всего цепочек в джобе
  total: number // подходящих под combos
  offset: number
  limit: number
  items: Itinerary[]
}

async function getJson<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init)
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return (await res.json()) as T
}

// Оценка с учётом кэша серий на бэке (клиентская estimate.ts — мгновенный черновик без кэша).
export function fetchEstimate(q: PlanQuery): Promise<PlannerEstimate & { status?: string; message?: string }> {
  return getJson(`${API_BASE}/plan/estimate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(toApi(q)),
  })
}

export function runPlan(q: PlanQuery): Promise<RunResponse> {
  return getJson<RunResponse>(`${API_BASE}/plan/run`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(toApi(q)),
  })
}

// Джоба = рейсы по остановкам запроса; фильтры и бюджет (f) — при каждом чтении:
// другой f у той же джобы — стыковка на бэке без повторного сбора.
const f = (q: PlanQuery) => `f=${encodeURIComponent(filtersParam(q))}`

export function fetchPlanJob(
  jobId: string,
  q: PlanQuery,
): Promise<JobStatus & { summary?: { count: number; combos: number; totalCount: number } }> {
  return getJson(`${API_BASE}/plan/jobs/${jobId}?${f(q)}`)
}

export function fetchCombos(jobId: string, q: PlanQuery, sort: 'price' | 'count' | 'transfers', offset: number, limit: number) {
  return getJson<CombosPage>(`${API_BASE}/plan/jobs/${jobId}/combos?sort=${sort}&offset=${offset}&limit=${limit}&${f(q)}`)
}

export function fetchRoutes(jobId: string, q: PlanQuery, offset: number, limit: number, combos: string[] | null) {
  const c = combos && combos.length ? `&combos=${encodeURIComponent(combos.join(','))}` : ''
  return getJson<RoutesPage>(`${API_BASE}/plan/jobs/${jobId}/routes?offset=${offset}&limit=${limit}${c}&${f(q)}`)
}

// «Починить»: сбрасывает зависшие на сервере сборы (running без обновлений > минуты).
export async function rescueJobs(): Promise<string[]> {
  const data = await getJson<{ rescued?: string[] }>(`${API_BASE}/jobs/rescue`, { method: 'POST' })
  return data.rescued ?? []
}
