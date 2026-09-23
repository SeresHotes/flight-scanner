// Правила корректности скелета маршрута (v1):
//  • минимум 2 остановки;
//  • концы (первая и последняя) — конкретные города (не «любой»), ≥1 город;
//  • у каждой остановки с городами выбран хотя бы один город;
//  • два «любых» подряд запрещены;
//  • один и тот же единственный город не может идти дважды подряд;
//  • у ПРОМЕЖУТОЧНЫХ остановок задан диапазон дат (start <= end); у концов даты
//    выводятся из соседей — поле не показывается и не требуется.

import type { PlannerStop } from './types'

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

    if (isEndpoint && s.kind === 'any') {
      flag(i, `Точка ${pointLabel(i)}: концы маршрута должны быть конкретным городом.`)
    }
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

  // Два «любых» подряд.
  for (let i = 0; i < stops.length - 1; i++) {
    if (stops[i].kind === 'any' && stops[i + 1].kind === 'any') {
      flag(i + 1, `Точки ${pointLabel(i)} и ${pointLabel(i + 1)}: два «любых» города подряд запрещены.`)
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
