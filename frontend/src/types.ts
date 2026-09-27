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
  minutes?: number | null // ожидание на этой пересадке (GraphQL); нет у старых данных
  night?: boolean
  visa?: boolean
  country?: string | null
}

export interface Baggage {
  known: boolean
  included: boolean
  pieces: number | null
  kg: number | null
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
  layover_minutes?: number | null // суммарно на земле на всех пересадках
  baggage?: Baggage // GraphQL: багаж по билету; нет у данных из REST
  airline?: string
  flight_number?: string
  price: number
  link?: string
  // Виртуальный рейс hidden-city (core/planner.hidden_city_flights): на самом деле
  // это билет origin→final с первой пересадкой в destination — выходим там. Цена —
  // всего билета, link — на него, arrival_at — оценка (в источнике нет времени пересадки).
  hidden_city?: HiddenCity
}

export interface HiddenCity {
  final: string // код города конечного пункта билета
  final_city?: string
  final_airport?: string
  chain: string[] // цепочка аэропортов билета, напр. SVO, PKX, HRB
  full_duration: number | null
  full_transfers: number
  baggage?: Baggage
  arrival_estimated: boolean // с GraphQL всегда false: прилёт в хаб — реальный
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
