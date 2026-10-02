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
  // Можно улететь дальше из соседнего города в этом радиусе, км (0/нет — только свой).
  radiusKm?: number
  // Остановку можно пропустить (только промежуточную): сбор добавит перелёт в обход,
  // его условия — cities[i].bypass запроса.
  skip?: boolean
  // У «любой» в середине: сколько городов подряд [от, до] — «блок любых городов» с общим
  // окном, пребыванием в каждом и одними условиями на все перелёты (cities[i].flights).
  // Нет или [1, 1] — обычная остановка.
  count?: [number, number]
}

// Оценка объёма сбора: страниц GraphQL по переходам (planner/estimate.ts).
export interface EstimateLeg {
  fromLabel: string
  toLabel: string
  days: number
  requests: number
  cached?: number // страниц уже в кэше серий (знает только бэк)
  anyLeg: boolean
}

export interface PlannerEstimate {
  requests: number // всего страниц по потолку серий
  cached?: number // из них уже в складе билетов или в кэше серий (POST /api/plan/estimate)
  cold?: number // пойдут в источник
  seconds: number // по холодным
  legs: EstimateLeg[]
  source?: 'tickets' | 'collector' // откуда рейсы: склад билетов (секунды) или серии через коллектор
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
  // Дальше летим из соседнего города (переезд внутри остановки), а не из города прилёта.
  departFrom?: { code: string; city: string; flag?: string; km: number | null }
}

export interface Itinerary {
  id: number
  skipped?: number[] // пропущенные остановки (номера в запросе)
  stops: ItineraryStop[]
  segments: Segment[]
  total_price: number
  total_days: number
  total_transfers: number
  travel_minutes: number
}
