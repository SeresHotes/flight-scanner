// Правила корректности скелета маршрута (v1):
//  • минимум 2 остановки;
//  • любая остановка, включая концы, — города (≥1) или «любой» (откуда / куда угодно);
//  • несколько «любых» подряд разрешены (рейсы берутся из склада билетов);
//  • один и тот же единственный город не может идти дважды подряд;
//  • у ПРОМЕЖУТОЧНЫХ остановок задан диапазон дат (start <= end); у концов даты
//    выводятся из соседей — поле не показывается и не требуется;
//  • пропускать можно промежуточные остановки, не две подряд, не больше MAX_SKIPS и не
//    между двумя «любыми»;
//  • блок любых городов «от–до» — в середине, между остановками с городами, до
//    MAX_BLOCK_CITIES городов; блок «от 0» считается пропускаемой остановкой;
//  • вариантов маршрута (пропуски × число городов блоков) не больше MAX_VARIANTS
//    (planner/src/stops.rs plan_error).

export const MAX_SKIPS = 3
export const MAX_BLOCK_CITIES = 3
export const MAX_VARIANTS = 16

const block = (s: PlannerStop): [number, number] | null =>
  s.kind === 'any' && s.count && (s.count[0] !== 1 || s.count[1] !== 1) ? s.count : null
const removable = (s: PlannerStop): boolean => !!s.skip || block(s)?.[0] === 0

import type { PlannerStop } from './types'

// Вариантов маршрута: 2 на пропускаемую остановку × (до − от + 1) на блок.
export function variantCount(stops: PlannerStop[]): number {
  return stops.reduce((n, s) => {
    const b = block(s)
    return n * (s.skip ? 2 : b ? Math.max(1, b[1] - b[0] + 1) : 1)
  }, 1)
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

  // Блоки любых городов: между городами, от 0 до MAX_BLOCK_CITIES.
  stops.forEach((s, i) => {
    const b = block(s)
    if (!b || i === 0 || i === stops.length - 1) return
    if (b[0] > b[1] || b[1] < 1 || b[1] > MAX_BLOCK_CITIES) {
      flag(i, `Точка ${pointLabel(i)}: любых городов подряд — от 0 до ${MAX_BLOCK_CITIES}, «от» не больше «до».`)
    }
    if (stops[i - 1].kind !== 'cities' || stops[i + 1].kind !== 'cities') {
      flag(i, `Точка ${pointLabel(i)}: рядом с несколькими любыми городами должны быть остановки с городами.`)
    }
  })

  // Пропуски: не две подряд, не между двумя «любыми», не больше MAX_SKIPS.
  const skips = stops.map((s, i) => (s.skip && i > 0 && i < stops.length - 1 ? i : -1)).filter((i) => i >= 0)
  const gone = stops.map((s, i) => (removable(s) && i > 0 && i < stops.length - 1 ? i : -1)).filter((i) => i >= 0)
  for (const i of gone) {
    if (gone.includes(i + 1)) flag(i + 1, `Точки ${pointLabel(i)} и ${pointLabel(i + 1)}: нельзя пропускать две остановки подряд (блок «от 0» тоже пропускается).`)
  }
  for (const i of skips) {
    if (stops[i - 1].kind === 'any' && stops[i + 1].kind === 'any') {
      flag(i, `Точка ${pointLabel(i)}: пропуск между двумя «любыми» не поддерживается — задайте город у соседней.`)
    }
  }
  if (skips.length > MAX_SKIPS) general.push(`Пропускаемых остановок — не больше ${MAX_SKIPS}.`)
  const variants = variantCount(stops)
  if (variants > MAX_VARIANTS) {
    general.push(`Слишком много вариантов маршрута (${variants}, максимум ${MAX_VARIANTS}): уменьшите пропуски или разброс числа городов.`)
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
