// Что уже доступно/собрано — из GET /api/routes.

export interface AvailableRoute {
  origin: string
  destination: string
  title: string
  trips: number
  region_label?: string
  region_flag?: string
  there_price_range: [number, number]
  back_price_range: [number, number]
  dep_there_range: [string, string]
  dep_back_range: [string, string]
  min_stay: number
  stopover_days_range: [number, number]
  collected_at?: string | null
}

export interface Collection {
  origin: string | null // null = «любой» (запрос по всем направлениям)
  destination: string | null
  direct: boolean
  dep_from: string | null
  dep_to: string | null
  collected_at: string | null
  runs: number
  flights: number
}

export interface StoredCoverage {
  total_quotes: number
  distinct_routes: number
  collections: Collection[]
}

export interface RoutesResponse {
  available: AvailableRoute[]
  stored: StoredCoverage
}

const API_BASE = import.meta.env.VITE_API_BASE ?? '/api'

export async function fetchRoutes(): Promise<RoutesResponse> {
  const res = await fetch(`${API_BASE}/routes`)
  if (!res.ok) throw new Error(`Не удалось загрузить список маршрутов: ${res.status}`)
  return (await res.json()) as RoutesResponse
}
