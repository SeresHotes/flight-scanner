// Правила корректности скелета маршрута (v1):
//  • минимум 2 остановки;
//  • любая остановка, включая концы, — города (≥1) или «любой» (откуда / куда угодно);
//  • несколько «любых» подряд разрешены (рейсы берутся из склада билетов), но суммарная
//    ширина окон дат всех плеч ограничена MAX_TOTAL_WINDOW_DAYS — иначе выборка непомерна;
//  • один и тот же единственный город не может идти дважды подряд;
//  • у ПРОМЕЖУТОЧНЫХ остановок задан диапазон дат (start <= end); у концов даты
//    выводятся из соседей — поле не показывается и не требуется;
//  • пропускать можно промежуточные остановки, не две подряд, не больше MAX_SKIPS и не
//    между двумя «любыми» (planner/src/stops.rs skip_error).

export const MAX_SKIPS = 3

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

// Окно плеча i (даты сбора) или null — окон нет.
function legWindow(stops: PlannerStop[], i: number): [string, string] | null {
  const wi = stops[i].window
  if (wi[0] && wi[1]) return wi
  const wj = stops[i + 1].window
  if (wj[0] && wj[1]) return wj
  return null
}

// Дни перелёта в обход остановки i: от начала плеча в неё до конца плеча из неё.
export function bypassDays(stops: PlannerStop[], i: number): number {
  const a = legWindow(stops, i - 1)
  const b = legWindow(stops, i)
  if (!a && !b) return DEFAULT_LEG_DAYS
  if (!a || !b) return daysInWindow((a ?? b)!)
  return daysInWindow([a[0] < b[0] ? a[0] : b[0], a[1] > b[1] ? a[1] : b[1]])
}

// Суммарная ширина окон всех плеч, включая перелёты в обход (в днях).
export function totalWindowDays(stops: PlannerStop[]): number {
  let total = 0
  for (let i = 0; i < stops.length - 1; i++) total += legDays(stops, i)
  stops.forEach((s, i) => {
    if (s.skip && i > 0 && i < stops.length - 1) total += bypassDays(stops, i)
  })
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

  // Суммарная ширина окон: объём выборки растёт с числом «любых» и шириной окон.
  if (stops.length >= 2) {
    const total = totalWindowDays(stops)
    if (total > MAX_TOTAL_WINDOW_DAYS) {
      general.push(`Суммарная ширина окон дат — ${total} дн., максимум ${MAX_TOTAL_WINDOW_DAYS}. Сузьте диапазоны.`)
    }
  }

  // Пропуски: не две подряд, не между двумя «любыми», не больше MAX_SKIPS.
  const skips = stops.map((s, i) => (s.skip && i > 0 && i < stops.length - 1 ? i : -1)).filter((i) => i >= 0)
  for (const i of skips) {
    if (skips.includes(i + 1)) flag(i + 1, `Точки ${pointLabel(i)} и ${pointLabel(i + 1)}: нельзя разрешить пропуск двух остановок подряд.`)
    if (stops[i - 1].kind === 'any' && stops[i + 1].kind === 'any') {
      flag(i, `Точка ${pointLabel(i)}: пропуск между двумя «любыми» не поддерживается — задайте город у соседней.`)
    }
  }
  if (skips.length > MAX_SKIPS) general.push(`Пропускаемых остановок — не больше ${MAX_SKIPS}.`)

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
