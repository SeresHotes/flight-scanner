// Клиентская оценка объёма сбора цепочки — чистая арифметика по окнам дат,
// считается мгновенно на каждый ввод (без round-trip к беку). Формула совпадает
// с core/planner._leg_requests: «запрос» = страница GraphQL по 400 билетов.
//   город → город:  пары A×B по PAGES_CITY + hidden-city A→ANY по PAGES_HIDDEN на город A;
//   с «любым» концом: PAGES_ANY на каждый конкретный город другого конца.
// Всё × дней окна плеча. Оценка пессимистичная (потолок страниц серии), прогресс
// сбора доходит ровно до неё.

import type { PlannerEstimate, PlannerStop } from './types'
import { daysInWindow } from './dates'

const SECONDS_PER_REQUEST = 1.0 // совпадает с core/planner.SECONDS_PER_REQUEST (60 запросов/мин)
const DEFAULT_LEG_DAYS = 7 // ширина окна плеча, если оба конца без окна
const PAGES_CITY = 1 // core/planner.PAGES_CITY
const PAGES_ANY = 12 // core/planner.PAGES_ANY
const PAGES_HIDDEN = 4 // core/planner.PAGES_HIDDEN

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

const cities = (s: PlannerStop): number => Math.max(1, s.airports.length)

export function estimatePlan(stops: PlannerStop[]): PlannerEstimate {
  const legs = []
  let requests = 0
  for (let i = 0; i < stops.length - 1; i++) {
    const from = stops[i]
    const to = stops[i + 1]
    const win = legWindow(stops, i)
    const days = daysInWindow(win) || DEFAULT_LEG_DAYS
    const anyLeg = from.kind === 'any' || to.kind === 'any'
    let perDay: number
    if (from.kind === 'cities' && to.kind === 'cities') {
      perDay = cities(from) * cities(to) * PAGES_CITY + cities(from) * PAGES_HIDDEN
    } else {
      perDay = cities(from.kind === 'cities' ? from : to) * PAGES_ANY
    }
    const reqs = days * perDay
    legs.push({ fromLabel: stopLabel(from), toLabel: stopLabel(to), days, requests: reqs, anyLeg })
    requests += reqs
  }
  return { requests, seconds: Math.round(requests * SECONDS_PER_REQUEST), legs }
}
