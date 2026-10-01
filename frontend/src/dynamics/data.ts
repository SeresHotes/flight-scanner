// Динамика цен (GET /api/dynamics): ответ бэка и расчёт линий графика под фильтры.
// Бэк отдаёт историю как есть — снимки озера (моменты загрузки), рейсы и тройки
// «рейс × снимок → цена»; фильтры дёшевы и мгновенны, поэтому считаются здесь.

const API_BASE = import.meta.env.VITE_API_BASE ?? '/api'

export interface DynFlight {
  day: string
  departure_at: string
  arrival_at: string | null
  duration: number | null
  transfers: number
  airline: string | null
  flights: string[]
  chain: string[]
  via: string[]
  origin_airport: string | null
  destination_airport: string | null
  link: string | null
}

export interface DynSnapshot {
  at: string
  days: string[]
}

export interface DynResponse {
  error?: string
  origin: string
  origin_city: string
  destination: string
  origin_name: string
  destination_name: string
  from: string
  to: string
  history_days: number
  files: number
  files_failed: number
  seconds?: number
  snapshots: DynSnapshot[]
  flights: DynFlight[]
  // [рейс, снимок, цена, багаж: 1 включён, 0 нет, -1 неизвестно]
  obs: [number, number, number, number][]
}

export async function fetchDynamics(p: { origin: string; destination: string; from: string; to: string; history: number }): Promise<DynResponse> {
  const qs = new URLSearchParams({ origin: p.origin, destination: p.destination, from: p.from, to: p.to, history: String(p.history) })
  const res = await fetch(`${API_BASE}/dynamics?${qs}`)
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return (await res.json()) as DynResponse
}

// ---------------------------------------------------------------- фильтры

export interface DynFilters {
  dep: [number, number] // часы вылета (местное время), 0..24
  arr: [number, number] // часы прилёта
  maxTransfers: number // -1 — любые
  baggage: boolean // только тарифы с багажом
  maxDuration: number // часы в пути, 0 — без ограничения
  airlines: string[] // пусто — все
}

export const DEFAULT_FILTERS: DynFilters = { dep: [0, 24], arr: [0, 24], maxTransfers: -1, baggage: false, maxDuration: 0, airlines: [] }

// Часы местного времени из ISO с поясом ('2026-10-30T10:15:00+03:00' → 10.25).
export function hourOf(iso: string | null | undefined): number | null {
  const m = iso?.match(/T(\d{2}):(\d{2})/)
  return m ? Number(m[1]) + Number(m[2]) / 60 : null
}

function inHours(h: number | null, [lo, hi]: [number, number]): boolean {
  if (lo <= 0 && hi >= 24) return true
  return h !== null && h >= lo && h <= hi
}

export function passes(f: DynFlight, flt: DynFilters): boolean {
  if (!inHours(hourOf(f.departure_at), flt.dep)) return false
  if (!inHours(hourOf(f.arrival_at), flt.arr)) return false
  if (flt.maxTransfers >= 0 && f.transfers > flt.maxTransfers) return false
  if (flt.maxDuration > 0 && (f.duration ?? 0) > flt.maxDuration * 60) return false
  if (flt.airlines.length && !flt.airlines.includes(f.airline ?? '')) return false
  return true
}

// Цена рейса по снимкам под фильтр багажа: prices[рейс] = Map<снимок, цена>.
export function flightPrices(data: DynResponse, baggage: boolean): Map<number, number>[] {
  const out: Map<number, number>[] = data.flights.map(() => new Map())
  for (const [fi, si, price, bag] of data.obs) {
    if (baggage && bag !== 1) continue
    const m = out[fi]
    const cur = m.get(si)
    if (cur === undefined || price < cur) m.set(si, price)
  }
  return out
}

// ---------------------------------------------------------------- линии графика

export interface Point {
  t: number // момент снимка, мс
  v: number | null // null — снимок видел день, но подходящих билетов не было
  si: number
  fi?: number // какой рейс дал минимум
}

export interface Series {
  id: string
  label: string
  color: string
  points: Point[]
}

// Категориальная палитра (тёмная тема), порядок фиксирован — цвет следует за сущностью.
export const PALETTE = ['#3987e5', '#d95926', '#199e70', '#c98500', '#d55181', '#9085e9', '#e66767', '#008300']

export const snapTime = (s: DynSnapshot) => Date.parse(s.at)

// Минимум по подходящим рейсам дня на каждом снимке, покрывшем этот день.
export function minSeries(data: DynResponse, ok: boolean[], prices: Map<number, number>[], days: string[]): Series[] {
  return days.map((day, di) => {
    const flights = data.flights.map((_, i) => i).filter((i) => ok[i] && data.flights[i].day === day)
    const points: Point[] = []
    data.snapshots.forEach((s, si) => {
      if (!s.days.includes(day)) return
      let best: number | null = null
      let bestF: number | undefined
      for (const fi of flights) {
        const p = prices[fi].get(si)
        if (p !== undefined && (best === null || p < best)) {
          best = p
          bestF = fi
        }
      }
      points.push({ t: snapTime(s), v: best, si, fi: bestF })
    })
    return { id: day, label: dayLabel(day), color: PALETTE[di % PALETTE.length], points }
  })
}

// Цена выбранного рейса по снимкам, видевшим его день (нет в снимке — разрыв линии).
export function flightSeries(data: DynResponse, picked: number[], prices: Map<number, number>[], colorOf: (fi: number) => string): Series[] {
  return picked.map((fi) => {
    const f = data.flights[fi]
    const points: Point[] = []
    data.snapshots.forEach((s, si) => {
      if (!s.days.includes(f.day)) return
      points.push({ t: snapTime(s), v: prices[fi].get(si) ?? null, si, fi })
    })
    const label = data.from === data.to ? flightTitle(f) : `${dayLabel(f.day)} · ${flightTitle(f)}`
    return { id: String(fi), label, color: colorOf(fi), points }
  })
}

// ---------------------------------------------------------------- подписи

const WD = ['вс', 'пн', 'вт', 'ср', 'чт', 'пт', 'сб']
const MON = ['янв', 'фев', 'мар', 'апр', 'мая', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']

export function dayLabel(iso: string): string {
  const [y, m, d] = iso.slice(0, 10).split('-').map(Number)
  const dt = new Date(y, m - 1, d)
  return `${d} ${MON[m - 1]}, ${WD[dt.getDay()]}`
}

export function clock(iso: string | null | undefined): string {
  const m = iso?.match(/T(\d{2}:\d{2})/)
  return m ? m[1] : '—'
}

// +1, если прилёт на следующий день (по местным датам).
export function dayShift(dep: string, arr: string | null): number {
  if (!arr) return 0
  const a = Date.UTC(+arr.slice(0, 4), +arr.slice(5, 7) - 1, +arr.slice(8, 10))
  const d = Date.UTC(+dep.slice(0, 4), +dep.slice(5, 7) - 1, +dep.slice(8, 10))
  return Math.round((a - d) / 86400000)
}

export function flightTitle(f: DynFlight): string {
  const shift = dayShift(f.departure_at, f.arrival_at)
  return `${clock(f.departure_at)}→${clock(f.arrival_at)}${shift > 0 ? ` +${shift}` : ''} · ${f.flights.join(' + ')}`
}

// Момент снимка: '30 сен 14:05' (местное время браузера).
export function snapLabel(t: number, withTime = true): string {
  const d = new Date(t)
  const base = `${d.getDate()} ${MON[d.getMonth()]}`
  return withTime ? `${base} ${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}` : base
}

export const money = (n: number) => Math.round(n).toLocaleString('ru-RU') + ' ₽'

export function hoursLabel([lo, hi]: [number, number]): string {
  const h = (x: number) => `${String(Math.floor(x)).padStart(2, '0')}:${x % 1 ? '30' : '00'}`
  return lo <= 0 && hi >= 24 ? 'любое' : `${h(lo)}–${hi >= 24 ? '24:00' : h(hi)}`
}
