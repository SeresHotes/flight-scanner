// Клиентская фильтрация цепочек по фильтрам городов/переходов/общей длины.
// Работает по компактному набору (ItinerarySet) и отдаёт индексы подходящих
// цепочек: объектов Itinerary на миллион цепочек браузер бы не удержал.

import type { PlannerFilters } from './types'
import type { ItinerarySet } from './compact'
import { coversWindow } from './dates'

export function itineraryMatches(set: ItinerarySet, n: number, f: PlannerFilters): boolean {
  // Города: длительность, обязательное окно, выходные.
  for (let k = 0; k < set.stopCount; k++) {
    const cf = f.cities[k]
    if (!cf) continue
    if (cf.allowedCodes && !cf.allowedCodes.includes(set.stopCode(n, k))) return false
    const days = set.stopDays(n, k)
    if (days < cf.minStay || days > cf.maxStay) return false
    if (cf.mustCover && !coversWindow(set.arrive(n, k), set.depart(n, k), cf.mustCover)) return false
    if (cf.requireWeekend && !set.weekendCovered(n, k)) return false
  }
  // Переходы: пересадки и суммарная длительность перелёта.
  for (let k = 0; k < set.legs; k++) {
    const tf = f.transitions[k]
    if (!tf) continue
    const seg = set.segment(n, k)
    if (tf.maxTransfers >= 0 && seg.transfers > tf.maxTransfers) return false
    if ((seg.duration || 0) > tf.maxTravelMinutes) return false
  }
  // Общая длина поездки.
  const total = set.totalDaysOf(n)
  if (total < f.tripLength[0] || total > f.tripLength[1]) return false
  return true
}

// Индексы подходящих цепочек (в порядке цены — как в наборе).
export function applyFilters(set: ItinerarySet, f: PlannerFilters): Int32Array {
  const out = new Int32Array(set.count)
  let m = 0
  for (let n = 0; n < set.count; n++) {
    if (itineraryMatches(set, n, f)) out[m++] = n
  }
  return out.subarray(0, m)
}

// Границы набора одним проходом (без Math.max(...arr): на больших наборах spread
// вешает вкладку / бросает "Maximum call stack size exceeded").
interface SetBounds {
  maxTravel: number // максимум суммарной длительности перелёта сегмента
  tripMin: number
  tripMax: number
  stayMin: number // мин дней в городе по всем остановкам
  stayMax: number // макс дней в городе (не ниже 30 — чтобы слайдер имел запас)
}

function setBounds(set: ItinerarySet): SetBounds {
  let maxTravel = 60
  let tripMin = Infinity
  let tripMax = -Infinity
  let stayMin = Infinity
  let stayMax = 30
  for (const s of set.segments) {
    const d = s.duration || 0
    if (d > maxTravel) maxTravel = d
  }
  for (let n = 0; n < set.count; n++) {
    const total = set.totalDaysOf(n)
    if (total < tripMin) tripMin = total
    if (total > tripMax) tripMax = total
    for (let k = 0; k < set.stopCount; k++) {
      const days = set.stopDays(n, k)
      if (days < stayMin) stayMin = days
      if (days > stayMax) stayMax = days
    }
  }
  if (!set.count) {
    tripMin = 1
    tripMax = 60
    stayMin = 1
  }
  return { maxTravel, tripMin, tripMax, stayMin, stayMax }
}

// Дефолтные (максимально широкие) фильтры под собранный набор цепочек.
export function defaultFilters(set: ItinerarySet, stopCount: number): PlannerFilters {
  const transitionCount = Math.max(0, stopCount - 1)
  const b = setBounds(set)

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

export function computeBounds(set: ItinerarySet): FilterBounds {
  const b = setBounds(set)
  return { maxTravelMinutes: b.maxTravel, tripLength: [b.tripMin, b.tripMax], stayDays: [b.stayMin, b.stayMax] }
}
