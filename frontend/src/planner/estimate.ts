// Клиентская оценка объёма сбора цепочки — чистая арифметика по окнам дат,
// считается мгновенно на каждый ввод (без round-trip к беку). Формула совпадает
// с core/planner.estimate_plan: якорим сторону с МЕНЬШИМ числом городов и
// запрашиваем «все направления» через неё (1 запрос/день на якорный город).

import type { PlannerEstimate, PlannerStop } from './types'
import { daysInWindow } from './dates'

const SECONDS_PER_REQUEST = 0.65 // совпадает с core/planner.SECONDS_PER_REQUEST
const DEFAULT_LEG_DAYS = 7 // ширина окна плеча, если оба конца без окна

export function stopLabel(s: PlannerStop): string {
  if (s.kind === 'any') return 'Любой город'
  if (s.airports.length === 0) return 'Город не выбран'
  if (s.airports.length === 1) return `${s.airports[0].city} (${s.airports[0].code})`
  return s.airports.map((a) => a.code).join('/')
}

// Окно плеча i (stop i → i+1): заданное окно того конца, у кого оно есть
// (у концов маршрута окна нет — выводятся из соседей), иначе пусто.
function legWindow(stops: PlannerStop[], i: number): [string, string] {
  const wi = stops[i].window
  if (wi[0] && wi[1]) return wi
  const wj = stops[i + 1].window
  if (wj[0] && wj[1]) return wj
  return ['', '']
}

// Число городов на конце; «любой» = бесконечность (нельзя «заякорить»).
const cardinality = (s: PlannerStop): number =>
  s.kind === 'cities' ? Math.max(1, s.airports.length) : Infinity

export function estimatePlan(stops: PlannerStop[]): PlannerEstimate {
  const legs = []
  let requests = 0
  for (let i = 0; i < stops.length - 1; i++) {
    const from = stops[i]
    const to = stops[i + 1]
    const win = legWindow(stops, i)
    const days = daysInWindow(win) || DEFAULT_LEG_DAYS
    const anyLeg = from.kind === 'any' || to.kind === 'any'
    const anchor = Math.min(cardinality(from), cardinality(to))
    const reqs = anchor * days
    legs.push({ fromLabel: stopLabel(from), toLabel: stopLabel(to), days, requests: reqs, anyLeg })
    requests += reqs
  }
  return { requests, seconds: Math.round(requests * SECONDS_PER_REQUEST), legs }
}
