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
  // бэк ещё читает озеро: done/total файлов
  pending?: boolean
  done?: number
  total?: number
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
  t: number // момент по оси X, мс: снимок (или день вылета — в профиле)
  v: number | null // null — снимок видел день, но подходящих билетов не было
  si: number
  fi?: number // какой рейс дал минимум
  hi?: number // верх коридора: медиана цен подходящих рейсов в снимке
  n?: number // сколько подходящих рейсов в снимке
}

export interface Series {
  id: string
  label: string
  color: string
  points: Point[]
}

// Категориальная палитра (тёмная тема), порядок фиксирован — цвет следует за сущностью.
export const PALETTE = ['#3987e5', '#d95926', '#199e70', '#c98500', '#d55181', '#9085e9', '#e66767', '#008300']

// Цвет i-й из n упорядоченных сущностей (дни вылета, даты «на момент»): до 8 —
// категориальная палитра, больше — одна шкала синего от тёмного к светлому.
export function orderedColor(i: number, n: number): string {
  if (n <= PALETTE.length) return PALETTE[i]
  return mix('#2f62a8', '#b9dcff', n > 1 ? i / (n - 1) : 0)
}

export function mix(a: string, b: string, t: number): string {
  const p = (h: string) => [1, 3, 5].map((i) => parseInt(h.slice(i, i + 2), 16))
  const [x, y] = [p(a), p(b)]
  return '#' + x.map((v, i) => Math.round(v + (y[i] - v) * Math.min(1, Math.max(0, t))).toString(16).padStart(2, '0')).join('')
}

// Шкала «выгодности» для календаря и тепловой карты: чем дешевле, тем ярче.
export function heatT(price: number, lo: number, hi: number): number {
  return hi > lo ? (hi - price) / (hi - lo) : 0.5
}

export function heatColor(price: number, lo: number, hi: number): string {
  const t = heatT(price, lo, hi)
  // одна шкала зелёного: тёмная (дорого) → яркая (дёшево), средняя точка — для разлёта
  return t < 0.5 ? mix('#1c2430', '#17644d', t * 2) : mix('#17644d', '#46e3ae', (t - 0.5) * 2)
}

// Текст поверх клетки тепловой шкалы: тёмный на ярком фоне.
export const heatInk = (price: number, lo: number, hi: number) => (heatT(price, lo, hi) > 0.72 ? '#0d1a14' : '#e8ecf3')

export const snapTime = (s: DynSnapshot) => Date.parse(s.at)

export function dayRange(from: string, to: string): string[] {
  const out: string[] = []
  for (let d = from; d <= to; d = addDays(d, 1)) out.push(d)
  return out
}

export function addDays(iso: string, n: number): string {
  const [y, m, d] = iso.split('-').map(Number)
  const dt = new Date(Date.UTC(y, m - 1, d + n))
  return dt.toISOString().slice(0, 10)
}

// Полночь дня вылета (UTC) — ось X профиля «по дням вылета».
export const dayTime = (iso: string) => Date.parse(iso + 'T00:00:00Z')

function median(xs: number[]): number {
  const s = [...xs].sort((a, b) => a - b)
  const m = s.length >> 1
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2
}

// Подходящие рейсы по дням вылета.
export function flightsByDay(data: DynResponse, ok: boolean[]): Map<string, number[]> {
  const out = new Map<string, number[]>()
  data.flights.forEach((f, i) => {
    if (!ok[i]) return
    const l = out.get(f.day)
    if (l) l.push(i)
    else out.set(f.day, [i])
  })
  return out
}

// Минимум (и медиана — верх коридора) по подходящим рейсам дня на каждом снимке,
// покрывшем этот день.
export function minSeries(data: DynResponse, ok: boolean[], prices: Map<number, number>[], days: string[]): Series[] {
  const byDay = flightsByDay(data, ok)
  return days.map((day, di) => {
    const flights = byDay.get(day) ?? []
    const points: Point[] = []
    data.snapshots.forEach((s, si) => {
      if (!s.days.includes(day)) return
      let best: number | null = null
      let bestF: number | undefined
      const all: number[] = []
      for (const fi of flights) {
        const p = prices[fi].get(si)
        if (p === undefined) continue
        all.push(p)
        if (best === null || p < best) {
          best = p
          bestF = fi
        }
      }
      points.push({ t: snapTime(s), v: best, si, fi: bestF, hi: all.length ? median(all) : undefined, n: all.length })
    })
    return { id: day, label: dayLabel(day), color: orderedColor(di, days.length), points }
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

// Последняя известная цена серии на момент t (снимки не позже t).
export function asOf(s: Series, t: number): Point | null {
  let best: Point | null = null
  for (const p of s.points) if (p.t <= t && (!best || p.t >= best.t)) best = p
  return best
}

// Профиль «по дням вылета»: X — день вылета, линия — как выглядели цены на дату
// (последний снимок каждого дня не позже этой даты). Даты — сегодня и назад.
export function profileSeries(daySeries: Series[], snapshots: DynSnapshot[]): Series[] {
  if (!snapshots.length) return []
  const last = snapTime(snapshots[snapshots.length - 1])
  const first = snapTime(snapshots[0])
  const steps = [0, 3, 7, 14, 30, 60, 90, 180].filter((d) => last - d * 86400e3 >= first - 86400e3)
  return steps
    .map((back, i) => {
      const t = last - back * 86400e3
      const points: Point[] = daySeries.map((s, di) => {
        const p = asOf(s, t)
        return { t: dayTime(s.id), v: p ? p.v : null, si: p ? p.si : -1 - di, fi: p?.fi }
      })
      const label = back === 0 ? 'последний снимок' : `${back} ${plural(back, 'день', 'дня', 'дней')} назад`
      return { id: `asof-${back}`, label, color: back === 0 ? PALETTE[0] : mix('#56607a', '#9aa6c0', 1 - i / Math.max(1, steps.length - 1)), points }
    })
    .reverse()
}

// В процентах от первой известной цены серии.
export function toPct(series: Series[]): Series[] {
  return series.map((s) => {
    const base = s.points.find((p) => p.v !== null)?.v
    if (!base) return { ...s, points: s.points.map((p) => ({ ...p, v: null, hi: undefined })) }
    const f = (v: number) => (v / base - 1) * 100
    return { ...s, points: s.points.map((p) => ({ ...p, v: p.v === null ? null : f(p.v), hi: p.hi === undefined ? undefined : f(p.hi) })) }
  })
}

// Тепловая карта: строки — дни вылета, колонки — дни наблюдения (UTC); в клетке —
// последняя цена на конец дня наблюдения; `seen` — был ли снимок в этот день.
export interface HeatCell {
  v: number | null
  seen: boolean
}

export function heatMatrix(daySeries: Series[], snapshots: DynSnapshot[]): { cols: string[]; rows: HeatCell[][] } {
  if (!snapshots.length) return { cols: [], rows: [] }
  const first = snapshots[0].at.slice(0, 10)
  const last = snapshots[snapshots.length - 1].at.slice(0, 10)
  const cols = dayRange(first, last)
  const rows = daySeries.map((s) =>
    cols.map((c) => {
      const end = dayTime(c) + 86400e3 - 1
      const p = asOf(s, end)
      const seen = s.points.some((q) => q.t >= dayTime(c) && q.t <= end)
      return { v: p ? p.v : null, seen }
    }),
  )
  return { cols, rows }
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

export const pct = (n: number) => `${n > 0 ? '+' : n < 0 ? '−' : ''}${Math.abs(n).toFixed(Math.abs(n) < 10 ? 1 : 0)}%`

export function plural(n: number, one: string, few: string, many: string): string {
  const m10 = n % 10
  const m100 = n % 100
  if (m10 === 1 && m100 !== 11) return one
  if (m10 >= 2 && m10 <= 4 && (m100 < 10 || m100 >= 20)) return few
  return many
}

export function hoursLabel([lo, hi]: [number, number]): string {
  const h = (x: number) => `${String(Math.floor(x)).padStart(2, '0')}:${x % 1 ? '30' : '00'}`
  return lo <= 0 && hi >= 24 ? 'любое' : `${h(lo)}–${hi >= 24 ? '24:00' : h(hi)}`
}
