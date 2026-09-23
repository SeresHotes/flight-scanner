// Правила корректности скелета маршрута (v1):
//  • минимум 2 остановки;
//  • концы (первая и последняя) — конкретные города, не «любой»;
//  • два «любых» подряд запрещены;
//  • один и тот же город не может идти дважды подряд;
//  • у каждой остановки задано окно дат, start <= end.

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
    if (s.kind === 'city' && !s.airport?.code) {
      flag(i, `Точка ${pointLabel(i)}: выберите город.`)
    }

    const [from, to] = s.window
    if (!from || !to) {
      flag(i, `Точка ${pointLabel(i)}: задайте окно дат пребывания.`)
    } else if (from > to) {
      flag(i, `Точка ${pointLabel(i)}: начало окна позже конца.`)
    }
  })

  // Два «любых» подряд.
  for (let i = 0; i < stops.length - 1; i++) {
    if (stops[i].kind === 'any' && stops[i + 1].kind === 'any') {
      flag(i + 1, `Точки ${pointLabel(i)} и ${pointLabel(i + 1)}: два «любых» города подряд запрещены.`)
    }
  }

  // Один и тот же город дважды подряд.
  for (let i = 0; i < stops.length - 1; i++) {
    const a = stops[i]
    const b = stops[i + 1]
    if (a.kind === 'city' && b.kind === 'city' && a.airport?.code && a.airport.code === b.airport?.code) {
      flag(i + 1, `Точки ${pointLabel(i)} и ${pointLabel(i + 1)}: один город дважды подряд.`)
    }
  }

  return { ok: issues.length === 0 && general.length === 0, stopValid, issues, general }
}
