// Правила корректности скелета маршрута (v1):
//  • минимум 2 остановки;
//  • любая остановка, включая концы, — города (≥1) или «любой» (откуда / куда угодно);
//  • хотя бы одна остановка — конкретный город (иначе выборка — весь склад);
//  • несколько «любых» подряд разрешены (рейсы берутся из склада билетов), но суммарная
//    ширина окон дат всех плеч ограничена MAX_TOTAL_WINDOW_DAYS — иначе выборка непомерна;
//  • один и тот же единственный город не может идти дважды подряд;
//  • у ПРОМЕЖУТОЧНЫХ остановок задан диапазон дат (start <= end); у концов даты
//    выводятся из соседей — поле не показывается и не требуется.

import { daysInWindow } from './dates'
import type { PlannerStop } from './types'

// Совпадает с planner/src/stops.rs MAX_TOTAL_WINDOW_DAYS и DEFAULT_LEG_DAYS.
export const MAX_TOTAL_WINDOW_DAYS = 60
const DEFAULT_LEG_DAYS = 7

// Окно плеча i (stop i → i+1): заданное окно того конца, у кого оно есть; без окон — дефолт.
export function legDays(stops: PlannerStop[], i: number): number {
  const wi = stops[i].window
  if (wi[0] && wi[1]) return daysInWindow(wi)
  const wj = stops[i + 1].window
  if (wj[0] && wj[1]) return daysInWindow(wj)
  return DEFAULT_LEG_DAYS
}

// Суммарная ширина окон всех плеч (в днях).
export function totalWindowDays(stops: PlannerStop[]): number {
  let total = 0
  for (let i = 0; i < stops.length - 1; i++) total += legDays(stops, i)
  return total
}

export interface StopIssue {
  index: number
  message: string
}

export interface Validation {
  ok: boolean
  stopValid: boolean[] // по остановке — есть ли проблема именно с ней
  issues: StopIssue[]
  general: string[]
}

export const pointLabel = (i: number): string => String.fromCharCode(65 + i) // A, B, C…

export function validatePlan(stops: PlannerStop[]): Validation {
  const issues: StopIssue[] = []
  const general: string[] = []
  const stopValid = stops.map(() => true)

  const flag = (index: number, message: string) => {
    issues.push({ index, message })
    if (index >= 0 && index < stopValid.length) stopValid[index] = false
  }

  if (stops.length < 2) {
    general.push('Нужно минимум две остановки: откуда и куда.')
  }

  stops.forEach((s, i) => {
    const isEndpoint = i === 0 || i === stops.length - 1

    if (s.kind === 'cities' && s.airports.length === 0) {
      flag(i, `Точка ${pointLabel(i)}: добавьте хотя бы один город.`)
    }

    // Диапазон дат — только у промежуточных остановок (у концов выводится из соседей).
    if (!isEndpoint) {
      const [from, to] = s.window
      if (!from || !to) {
        flag(i, `Точка ${pointLabel(i)}: задайте диапазон дат «когда ОК быть здесь».`)
      } else if (from > to) {
        flag(i, `Точка ${pointLabel(i)}: начало диапазона позже конца.`)
      }
    }
  })

  if (stops.length >= 2 && stops.every((s) => s.kind === 'any')) {
    general.push('Хотя бы одна точка должна быть конкретным городом.')
  }

  // Суммарная ширина окон: объём выборки растёт с числом «любых» и шириной окон.
  if (stops.length >= 2) {
    const total = totalWindowDays(stops)
    if (total > MAX_TOTAL_WINDOW_DAYS) {
      general.push(`Суммарная ширина окон дат — ${total} дн., максимум ${MAX_TOTAL_WINDOW_DAYS}. Сузьте диапазоны.`)
    }
  }

  // Один и тот же единственный город дважды подряд.
  for (let i = 0; i < stops.length - 1; i++) {
    const a = stops[i]
    const b = stops[i + 1]
    if (
      a.kind === 'cities' && b.kind === 'cities' &&
      a.airports.length === 1 && b.airports.length === 1 &&
      a.airports[0].code && a.airports[0].code === b.airports[0].code
    ) {
      flag(i + 1, `Точки ${pointLabel(i)} и ${pointLabel(i + 1)}: один город дважды подряд.`)
    }
  }

  return { ok: issues.length === 0 && general.length === 0, stopValid, issues, general }
}
