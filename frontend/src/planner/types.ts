// Контракт нового планировщика цепочек A → B → C → … (пока на моках).
// Backend реализует ровно эти формы позже: POST /api/plan/estimate, /api/plan/gather.

import type { AirportOption } from '../data/airports'
import type { Segment } from '../types'

export type StopKind = 'city' | 'any'

// Одна остановка маршрута: конкретный город или «любой», плюс широкое окно
// «примерно хочу быть в этом городе тогда-то».
export interface PlannerStop {
  id: string
  kind: StopKind
  airport: AirportOption | null // задан для kind==='city'
  window: [string, string] // [start, end] дат пребывания (YYYY-MM-DD)
}

export interface PlannerRequest {
  stops: PlannerStop[]
}

// Оценка объёма сбора: одно «плечо» на каждый переход между остановками.
// Плечо с wildcard-концом собирается как «все направления» (тоже 1 запрос/дата).
export interface EstimateLeg {
  fromLabel: string
  toLabel: string
  days: number
  anyLeg: boolean
}

export interface PlannerEstimate {
  requests: number
  seconds: number
  legs: EstimateLeg[]
}

// --- Фильтры (появляются после сбора) ---

export interface CityFilter {
  minStay: number
  maxStay: number
  mustCover: [string, string] | null // окно, которое город обязан покрыть целиком
  requireWeekend: boolean // должны быть оба выходных (сб + вс)
}

export interface TransitionFilter {
  maxTransfers: number // 0 — только прямые, N — до N пересадок, -1 — любое
  maxTravelMinutes: number // верхняя граница суммарной длительности перелёта
}

export interface PlannerFilters {
  cities: CityFilter[] // длина == числу остановок
  transitions: TransitionFilter[] // длина == числу переходов (stops - 1)
  tripLength: [number, number] // общая длина поездки, дни
}

// --- Результат: построенная цепочка ---

export interface ItineraryStop {
  code: string
  city: string
  flag?: string
  arrive: string // ISO прилёта в город
  depart: string // ISO вылета из города дальше
  days: number // сколько дней в городе
  weekendCovered: boolean // попадают ли оба выходных в пребывание
  resolvedFromAny: boolean // город подобран под wildcard-остановку
}

export interface Itinerary {
  id: number
  stops: ItineraryStop[]
  segments: Segment[] // stops.length - 1 сегментов (переиспользуем общий контракт)
  total_price: number
  total_days: number
  total_transfers: number
  travel_minutes: number
}

// Состояние сбора данных под текущий маршрут.
export type CollectState =
  | { status: 'idle' }
  | { status: 'collecting'; progress: number; total: number }
  | { status: 'ready'; itineraries: Itinerary[] }
  | { status: 'error'; message: string }
