// Контракт данных совпадает с текущим web/data.json (см. build_web_data.py).
// Тот же контракт позже отдаст FastAPI /api/search — типы не меняются.

export interface EventInfo {
  name: string
  date?: string | null
  venue?: string | null
}

export interface Meta {
  generated_at: string
  title: string
  currency: string
  origin: string
  origin_code: string
  destination: string
  destination_code: string
  region_label?: string
  region_flag?: string
  event?: EventInfo | null
  min_stay: number
  max_korea_days: number
  stopover_days_range: [number, number]
  max_stop_days: number
  counts: { there: number; back: number; total: number }
  there_cities: string[]
  back_cities: string[]
  there_price_range: [number, number]
  back_price_range: [number, number]
  max_leg_travel_minutes: number
  dep_there_range?: [string, string]
  dep_back_range?: [string, string]
  collected_at?: string | null
}

export interface TransferPoint {
  code: string
  city: string
}

export interface Segment {
  origin: string
  destination: string
  origin_airport?: string
  destination_airport?: string
  origin_city?: string
  destination_city?: string
  departure_at: string
  arrival_at: string
  duration: number
  transfers: number
  direct: boolean
  transfer_points?: TransferPoint[]
  layover_minutes?: number | null // суммарно на земле на всех пересадках (источник не разбивает)
  airline?: string
  flight_number?: string
  price: number
  link?: string
}

export interface StopoverTransfer {
  from: string
  to: string
  from_city: string
  to_city: string
  distance_km: number
}

export interface Stopover {
  code: string
  city: string
  country?: string
  flag?: string
  days: number
  transfer?: StopoverTransfer | null
}

export type Direction = 'there' | 'back'

export interface Trip {
  direction: Direction
  type: string
  has_stopover: boolean
  total_price: number
  segments: Segment[]
  stopover: Stopover | null
  note_date?: string
  total_transfers: number
  max_transfers: number
  tr: number
  travel_minutes: number
  travel: number
  stop: boolean
  sdays: number
  city: string | null
  dep_date: string
  hub_date: string
  hub_ord: number
  id: number
}

export interface FlightData {
  meta: Meta
  trips: Trip[]
}
