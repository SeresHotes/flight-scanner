// Мок-«бэкенд» планировщика: оценка объёма сбора и генерация цепочечных
// маршрутов. Никаких сетевых запросов — фейковый прогресс + правдоподобные данные,
// чтобы отладить UX. Реальный сбор/движок появятся отдельно (см. docs/PLAN.md).

import type { AirportOption } from '../data/airports'
import type { Segment } from '../types'
import type { Itinerary, ItineraryStop, PlannerEstimate, PlannerStop } from './types'
import { addDays, dateOnly, dayCountBetween, daysInWindow, hasBothWeekendDays } from './dates'

const SECONDS_PER_REQUEST = 0.65 // совпадает с api/worker.py
const DEFAULT_START = '2026-11-01' // якорь старта цепочки, если окон нигде нет
const DEFAULT_LEG_DAYS = 7 // ширина окна плеча, если оба конца без окна (2-остановочный)

// Пул городов, которыми мок «раскрывает» wildcard-остановки (any).
const ANY_POOL: AirportOption[] = [
  { code: 'IST', city: 'Istanbul', flag: '🇹🇷', label: 'Istanbul (IST)' },
  { code: 'DXB', city: 'Dubai', flag: '🇦🇪', label: 'Dubai (DXB)' },
  { code: 'EVN', city: 'Yerevan', flag: '🇦🇲', label: 'Yerevan (EVN)' },
  { code: 'TBS', city: 'Tbilisi', flag: '🇬🇪', label: 'Tbilisi (TBS)' },
  { code: 'BEG', city: 'Belgrade', flag: '🇷🇸', label: 'Belgrade (BEG)' },
  { code: 'ALA', city: 'Almaty', flag: '🇰🇿', label: 'Almaty (ALA)' },
  { code: 'DOH', city: 'Doha', flag: '🇶🇦', label: 'Doha (DOH)' },
  { code: 'GYD', city: 'Baku', flag: '🇦🇿', label: 'Baku (GYD)' },
]

const AIRLINES = ['Aeroflot', 'Turkish Airlines', 'Emirates', 'Qatar Airways', 'S7', 'flydubai']

export function stopLabel(s: PlannerStop): string {
  if (s.kind === 'any') return 'Любой город'
  if (s.airports.length === 0) return 'Город не выбран'
  if (s.airports.length === 1) return `${s.airports[0].city} (${s.airports[0].code})`
  return s.airports.map((a) => a.code).join('/')
}

// Ширина окна плеча i (stop i → i+1): берём заданное окно того конца, у кого оно
// есть (у концов маршрута окна нет — они выводятся из соседей), иначе дефолт.
function legWindowDays(stops: PlannerStop[], i: number): number {
  const wi = stops[i].window
  if (wi[0] && wi[1]) return daysInWindow(wi)
  const wj = stops[i + 1].window
  if (wj[0] && wj[1]) return daysInWindow(wj)
  return DEFAULT_LEG_DAYS
}

// Оценка: одно плечо на каждый переход, число запросов = дни окна плеча.
export function estimatePlan(stops: PlannerStop[]): PlannerEstimate {
  const legs = []
  let requests = 0
  for (let i = 0; i < stops.length - 1; i++) {
    const days = legWindowDays(stops, i)
    const anyLeg = stops[i].kind === 'any' || stops[i + 1].kind === 'any'
    legs.push({ fromLabel: stopLabel(stops[i]), toLabel: stopLabel(stops[i + 1]), days, anyLeg })
    requests += days
  }
  return { requests, seconds: Math.round(requests * SECONDS_PER_REQUEST), legs }
}

// --- Детерминированный ГПСЧ, чтобы список не «прыгал» между рендерами ---
function mulberry32(seed: number): () => number {
  let a = seed >>> 0
  return () => {
    a |= 0
    a = (a + 0x6d2b79f5) | 0
    let t = Math.imul(a ^ (a >>> 15), 1 | a)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

const pick = <T,>(rng: () => number, arr: T[]): T => arr[Math.floor(rng() * arr.length)]
const between = (rng: () => number, lo: number, hi: number): number => Math.floor(lo + rng() * (hi - lo + 1))

function ddmm(dateStr: string): string {
  const [, m, d] = dateStr.slice(0, 10).split('-')
  return `${d}${m}`
}

function bookingLink(origin: string, dest: string, departDate: string): string {
  return `https://www.aviasales.ru/search/${origin}${ddmm(departDate)}${dest}1`
}

// Однозначный код соседа (если у него ровно один город) — чтобы не ставить его
// дважды подряд при раскрытии наборов/«любого».
const neighborCode = (s: PlannerStop): string | null =>
  s.kind === 'cities' && s.airports.length === 1 ? s.airports[0].code : null

// Кандидаты под остановку: набор выбранных городов; «любой» → до 3 из пула,
// исключая однозначных соседей (чтобы не нарушать «не дважды подряд»).
function candidates(
  stop: PlannerStop,
  prev: string | null,
  next: string | null,
  rng: () => number,
): AirportOption[] {
  if (stop.kind === 'cities') {
    const list = stop.airports.filter((a) => a.code !== prev && a.code !== next)
    return list.length ? list : stop.airports
  }
  const pool = ANY_POOL.filter((a) => a.code !== prev && a.code !== next)
  const shuffled = [...pool].sort(() => rng() - 0.5)
  return shuffled.slice(0, 3)
}

// Декартово произведение кандидатов по остановкам, с ограничением сверху.
function cityCombos(perStop: AirportOption[][], cap: number): AirportOption[][] {
  let combos: AirportOption[][] = [[]]
  for (const options of perStop) {
    const next: AirportOption[][] = []
    for (const combo of combos) {
      for (const opt of options) {
        // Не даём одинаковый город подряд даже после раскрытия wildcard.
        if (combo.length && combo[combo.length - 1].code === opt.code) continue
        next.push([...combo, opt])
        if (next.length >= cap) break
      }
      if (next.length >= cap) break
    }
    combos = next
  }
  return combos
}

// Дата старта цепочки: начало первого заданного окна (у концов окна нет), иначе дефолт.
function firstWindowStart(stops: PlannerStop[]): string {
  for (const s of stops) if (s.window[0]) return dateOnly(s.window[0])
  return DEFAULT_START
}

function buildItinerary(
  id: number,
  cities: AirportOption[],
  stops: PlannerStop[],
  seed: number,
): Itinerary {
  const rng = mulberry32(seed)
  const segments: Segment[] = []
  const itinStops: ItineraryStop[] = []

  let arriveDate = firstWindowStart(stops) // старт цепочки — по первому заданному окну
  let arriveHour = 13

  for (let i = 0; i < cities.length; i++) {
    const c = cities[i]
    // Длительность пребывания: тяготеет к длине окна (у концов окна нет — берём дефолт).
    const win = stops[i].window
    const winDays = win[0] && win[1] ? Math.max(1, daysInWindow(win) - 1) : 5
    const stay = Math.max(2, Math.min(winDays, between(rng, 2, 6)))
    const arriveIso = `${arriveDate}T${String(arriveHour).padStart(2, '0')}:00`
    const departDate = addDays(arriveDate, stay)
    const departHour = between(rng, 8, 20)
    const departIso = `${departDate}T${String(departHour).padStart(2, '0')}:00`

    itinStops.push({
      code: c.code,
      city: c.city,
      flag: c.flag,
      arrive: arriveIso,
      depart: departIso,
      days: stay,
      weekendCovered: hasBothWeekendDays(arriveIso, departIso),
      resolvedFromAny: stops[i].kind === 'any',
    })

    // Сегмент до следующего города.
    if (i < cities.length - 1) {
      const next = cities[i + 1]
      const transfers = pick(rng, [0, 0, 1, 1, 2])
      const duration = transfers === 0 ? between(rng, 180, 360) : transfers === 1 ? between(rng, 420, 720) : between(rng, 720, 1140)
      const price = between(rng, 9, 20 + transfers * 12) * 1000 + between(rng, 0, 999)
      const arrMs = new Date(`${departIso}:00Z`).getTime() + duration * 60 * 1000
      const arrNext = new Date(arrMs).toISOString()
      segments.push({
        origin: c.code,
        destination: next.code,
        origin_airport: c.code,
        destination_airport: next.code,
        origin_city: c.city,
        destination_city: next.city,
        departure_at: departIso,
        arrival_at: arrNext.slice(0, 16),
        duration,
        transfers,
        direct: transfers === 0,
        transfer_points: [],
        airline: pick(rng, AIRLINES),
        price,
        link: bookingLink(c.code, next.code, departDate),
      })
      // Следующий город: прилетаем в день прилёта сегмента (или +1).
      arriveDate = dateOnly(arrNext)
      arriveHour = Number(arrNext.slice(11, 13))
    }
  }

  const total_price = segments.reduce((s, x) => s + x.price, 0)
  const total_transfers = segments.reduce((s, x) => s + x.transfers, 0)
  const travel_minutes = segments.reduce((s, x) => s + (x.duration || 0), 0)
  const first = itinStops[0]
  const last = itinStops[itinStops.length - 1]
  const total_days = Math.max(1, dayCountBetween(dateOnly(first.arrive), dateOnly(last.depart)))

  return { id, stops: itinStops, segments, total_price, total_transfers, travel_minutes, total_days }
}

// Генерирует набор цепочек под маршрут (детерминированно по составу остановок).
export function generateItineraries(stops: PlannerStop[]): Itinerary[] {
  const seed0 = stops.reduce(
    (h, s, i) => h + ((s.airports[0]?.code.charCodeAt(0) ?? 42) * (i + 7)) + (s.window[0]?.length ?? 0),
    17,
  )
  const rng = mulberry32(seed0)

  const perStop = stops.map((s, i) =>
    candidates(
      s,
      i > 0 ? neighborCode(stops[i - 1]) : null,
      i < stops.length - 1 ? neighborCode(stops[i + 1]) : null,
      rng,
    ),
  )
  const combos = cityCombos(perStop, 16)

  const itineraries: Itinerary[] = []
  let id = 1
  for (const combo of combos) {
    const variants = 3 // несколько вариантов по цене/пересадкам на одну цепочку городов
    for (let v = 0; v < variants; v++) {
      itineraries.push(buildItinerary(id, combo, stops, seed0 + id * 101 + v * 13))
      id++
      if (itineraries.length >= 30) break
    }
    if (itineraries.length >= 30) break
  }
  itineraries.sort((a, b) => a.total_price - b.total_price)
  return itineraries
}

// Симулирует сбор: тикает прогресс до total, затем отдаёт цепочки.
// Возвращает функцию отмены (на случай размонтирования/смены маршрута).
export function runMockCollection(
  stops: PlannerStop[],
  onProgress: (progress: number, total: number) => void,
  onDone: (itineraries: Itinerary[]) => void,
): () => void {
  const total = Math.max(1, estimatePlan(stops).requests)
  const ticks = Math.min(total, 40) // не плодим таймеры на широких окнах
  const step = total / ticks
  let done = 0
  let cancelled = false

  onProgress(0, total)
  const timer = setInterval(() => {
    if (cancelled) return
    done += 1
    const progress = Math.min(total, Math.round(done * step))
    onProgress(progress, total)
    if (done >= ticks) {
      clearInterval(timer)
      onProgress(total, total)
      onDone(generateItineraries(stops))
    }
  }, 45)

  return () => {
    cancelled = true
    clearInterval(timer)
  }
}
