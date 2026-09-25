// Клиентская фильтрация цепочек по фильтрам городов/переходов/общей длины.

import type { Itinerary, PlanGraph, PlannerFilters } from './types'
import { coversWindow } from './dates'
import { graphBounds } from './overview'

export function itineraryMatches(it: Itinerary, f: PlannerFilters): boolean {
  // Города: длительность, обязательное окно, выходные.
  for (let i = 0; i < it.stops.length; i++) {
    const cf = f.cities[i]
    if (!cf) continue
    const s = it.stops[i]
    if (cf.allowedCodes && !cf.allowedCodes.includes(s.code)) return false
    if (s.days < cf.minStay || s.days > cf.maxStay) return false
    if (cf.mustCover && !coversWindow(s.arrive, s.depart, cf.mustCover)) return false
    if (cf.requireWeekend && !s.weekendCovered) return false
  }
  // Переходы: пересадки и суммарная длительность перелёта.
  for (let i = 0; i < it.segments.length; i++) {
    const tf = f.transitions[i]
    if (!tf) continue
    const seg = it.segments[i]
    if (tf.maxTransfers >= 0 && seg.transfers > tf.maxTransfers) return false
    if ((seg.duration || 0) > tf.maxTravelMinutes) return false
  }
  // Общая длина поездки.
  if (it.total_days < f.tripLength[0] || it.total_days > f.tripLength[1]) return false
  return true
}

export function applyFilters(itineraries: Itinerary[], f: PlannerFilters): Itinerary[] {
  return itineraries.filter((it) => itineraryMatches(it, f))
}

// Границы набора одним проходом. Важно: НЕ используем Math.max(...arr) со spread —
// маршрутов могут быть десятки тысяч (кэпы сборки сняты), а spread такого массива
// в Math.max/min вешает вкладку / бросает "Maximum call stack size exceeded".
interface SetBounds {
  maxTravel: number // максимум суммарной длительности перелёта сегмента
  tripMin: number
  tripMax: number
  stayMin: number // мин дней в городе по всем остановкам
  stayMax: number // макс дней в городе (не ниже 30 — чтобы слайдер имел запас)
}

// graph — весь собранный граф (режим «наборы городов»): границы расширяются под него,
// иначе дефолтные фильтры, выведенные из топ-N цепочек, срезали бы остальные варианты.
function setBounds(itineraries: Itinerary[], graph: PlanGraph | null = null): SetBounds {
  let maxTravel = 60
  let tripMin = Infinity
  let tripMax = -Infinity
  let stayMin = Infinity
  let stayMax = 30
  for (const it of itineraries) {
    for (const s of it.segments) {
      const d = s.duration || 0
      if (d > maxTravel) maxTravel = d
    }
    if (it.total_days < tripMin) tripMin = it.total_days
    if (it.total_days > tripMax) tripMax = it.total_days
    for (const s of it.stops) {
      if (s.days < stayMin) stayMin = s.days
      if (s.days > stayMax) stayMax = s.days
    }
  }
  const gb = graph ? graphBounds(graph) : null
  if (gb) {
    maxTravel = Math.max(maxTravel, gb.maxTravel)
    tripMin = Math.min(tripMin, gb.tripMin)
    tripMax = Math.max(tripMax, gb.tripMax)
    stayMin = 0 // в графе пересадка «день в день» возможна всегда
    stayMax = Math.max(stayMax, gb.stayMax)
  } else if (!itineraries.length) {
    tripMin = 1
    tripMax = 60
    stayMin = 1
  }
  return { maxTravel, tripMin, tripMax, stayMin, stayMax }
}

// Дефолтные (максимально широкие) фильтры под собранный набор цепочек.
export function defaultFilters(
  itineraries: Itinerary[],
  stopCount: number,
  graph: PlanGraph | null = null,
): PlannerFilters {
  const transitionCount = Math.max(0, stopCount - 1)
  const b = setBounds(itineraries, graph)

  return {
    cities: Array.from({ length: stopCount }, () => ({
      minStay: b.stayMin,
      maxStay: b.stayMax,
      mustCover: null,
      requireWeekend: false,
      allowedCodes: null,
    })),
    transitions: Array.from({ length: transitionCount }, () => ({
      maxTransfers: -1,
      maxTravelMinutes: b.maxTravel,
    })),
    tripLength: [b.tripMin, b.tripMax],
  }
}

// Диапазоны для настройки контролов (границы слайдеров).
export interface FilterBounds {
  maxTravelMinutes: number
  tripLength: [number, number]
  stayDays: [number, number] // мин/макс дней в городе среди собранных цепочек
}

export function computeBounds(itineraries: Itinerary[], graph: PlanGraph | null = null): FilterBounds {
  const b = setBounds(itineraries, graph)
  return { maxTravelMinutes: b.maxTravel, tripLength: [b.tripMin, b.tripMax], stayDays: [b.stayMin, b.stayMax] }
}
