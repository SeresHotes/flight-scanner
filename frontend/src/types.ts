// Общие типы карточки маршрута: сегмент перелёта (контракт core/segments.make_segment).

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
