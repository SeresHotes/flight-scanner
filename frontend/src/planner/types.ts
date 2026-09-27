// Контракт планировщика v2 (docs/PLANNER_V2.md). Запрос — planner/query.ts,
// ответы бэка — planner/api.ts.

import type { AirportOption } from '../data/airports'
import type { Segment } from '../types'

export type StopKind = 'cities' | 'any'

// Одна остановка маршрута: набор городов-кандидатов (kind==='cities', ≥1) или
// «любой город» (kind==='any', только в середине). window — «в какие даты нам ОК
// быть в этом городе»; у первого и последнего города дат нет (выводятся из соседних).
export interface PlannerStop {
  id: string
  kind: StopKind
  airports: AirportOption[]
  window: [string, string]
}

// Оценка объёма сбора: страниц GraphQL по переходам (planner/estimate.ts).
export interface EstimateLeg {
  fromLabel: string
  toLabel: string
  days: number
  requests: number
  anyLeg: boolean
}

export interface PlannerEstimate {
  requests: number
  seconds: number
  legs: EstimateLeg[]
}

// --- Результат: построенная цепочка (GET /api/plan/jobs/{id}/routes) ---

export interface ItineraryStop {
  code: string
  city: string
  flag?: string
  arrive: string
  depart: string
  days: number
  weekendCovered: boolean
  resolvedFromAny: boolean
}

export interface Itinerary {
  id: number
  stops: ItineraryStop[]
  segments: Segment[]
  total_price: number
  total_days: number
  total_transfers: number
  travel_minutes: number
}
