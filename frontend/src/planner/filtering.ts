// Клиентская фильтрация цепочек по фильтрам городов/переходов/общей длины.

import type { Itinerary, PlannerFilters } from './types'
import { coversWindow } from './dates'

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

// Диапазон дней пребывания в городе по всем остановкам всех цепочек — чтобы
// дефолт «дней в городе» никого не резал, а слайдер покрывал реальные значения.
function stayRange(itineraries: Itinerary[]): [number, number] {
  const stayDays = itineraries.flatMap((it) => it.stops.map((s) => s.days))
  const min = stayDays.length ? Math.min(...stayDays) : 1
  const max = stayDays.length ? Math.max(30, ...stayDays) : 30
  return [min, max]
}

// Дефолтные (максимально широкие) фильтры под собранный набор цепочек.
export function defaultFilters(itineraries: Itinerary[], stopCount: number): PlannerFilters {
  const transitionCount = Math.max(0, stopCount - 1)
  const maxTravel = Math.max(60, ...itineraries.map((it) => Math.max(0, ...it.segments.map((s) => s.duration || 0))))
  const days = itineraries.map((it) => it.total_days)
  const tripMin = days.length ? Math.min(...days) : 1
  const tripMax = days.length ? Math.max(...days) : 60
  const [stayMin, stayMax] = stayRange(itineraries)

  return {
    cities: Array.from({ length: stopCount }, () => ({
      minStay: stayMin,
      maxStay: stayMax,
      mustCover: null,
      requireWeekend: false,
      allowedCodes: null,
    })),
    transitions: Array.from({ length: transitionCount }, () => ({
      maxTransfers: -1,
      maxTravelMinutes: maxTravel,
    })),
    tripLength: [tripMin, tripMax],
  }
}

// Диапазоны для настройки контролов (границы слайдеров).
export interface FilterBounds {
  maxTravelMinutes: number
  tripLength: [number, number]
  stayDays: [number, number] // мин/макс дней в городе среди собранных цепочек
}

export function computeBounds(itineraries: Itinerary[]): FilterBounds {
  const maxTravel = Math.max(60, ...itineraries.map((it) => Math.max(0, ...it.segments.map((s) => s.duration || 0))))
  const days = itineraries.map((it) => it.total_days)
  const tripMin = days.length ? Math.min(...days) : 1
  const tripMax = days.length ? Math.max(...days) : 60
  return { maxTravelMinutes: maxTravel, tripLength: [tripMin, tripMax], stayDays: stayRange(itineraries) }
}
