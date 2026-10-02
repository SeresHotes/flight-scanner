import { useEffect, useMemo, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { AirportCombobox } from '../components/AirportCombobox'
import { DateRangePicker } from '../components/DateRangePicker'
import { resolveAirport, type AirportOption } from '../data/airports'
import { addDaysISO, daysBetweenISO, todayISO } from '../lib/dates'
import { durFmt, plural } from '../lib/format'
import { RangeSlider } from '../planner/components/RangeSlider'
import { FORMATS, PriceChart, type ChartFormat, type ChartUnit } from '../dynamics/PriceChart'
import { CalendarView, GridView, HeatmapView, ProfileView, SingleView, TableView, WeekView } from '../dynamics/Views'
import {
  DEFAULT_FILTERS,
  PALETTE,
  clock,
  dayLabel,
  dayRange,
  dayShift,
  fetchDynamics,
  flightPrices,
  flightSeries,
  hoursLabel,
  minSeries,
  money,
  orderedColor,
  passes,
  snapLabel,
  snapTime,
  toPct,
  type DynFilters,
  type DynResponse,
  type Series,
} from '../dynamics/data'

// Отдельная страница «Динамика цены»: два города, дни вылета (до месяца) и фильтры — как
// менялась цена по снимкам озера (каждая повторная выборка коллектора — снимок).
// Что считаем: самая низкая цена под фильтры (линия на день вылета) или конкретные рейсы.
// Вид: один график, рядом, неделя, календарь, тепловая карта, по дням вылета, один день
// или все сразу; формат графика и шкала (₽ / %) — отдельно. Состояние — в URL.

const MAX_DAYS = 31
// рейсов на графике разом: до 8 — свои цвета, больше — одна шкала по времени вылета
const MAX_PICK = 60
type FlightSort = 'now' | 'min' | 'dep' | 'drop' | 'rise'
const FLIGHT_SORTS: [FlightSort, string][] = [
  ['now', 'дешевле сейчас'],
  ['min', 'дешевле за всю историю'],
  ['dep', 'по времени вылета'],
  ['drop', 'сильнее подешевели'],
  ['rise', 'сильнее подорожали'],
]
const HISTORY = [14, 30, 60, 90, 180]
const SPANS: [number, string][] = [
  [1, '1 день'],
  [7, 'неделя'],
  [14, '2 недели'],
  [31, 'месяц'],
]
const placeholder = (code: string): AirportOption => ({ code, city: code, label: code })

type Mode = 'min' | 'flight'
type View = 'overlay' | 'grid' | 'week' | 'calendar' | 'heatmap' | 'table' | 'profile' | 'single' | 'all'

const VIEWS: [View, string, string][] = [
  ['overlay', 'Один график', 'все дни вылета на одном графике'],
  ['grid', 'Рядом', 'график на каждый день, одна шкала'],
  ['week', 'Неделя', 'дни одной недели и их график'],
  ['calendar', 'Календарь', 'месяц: в клетке цена и её линия'],
  ['heatmap', 'Тепловая карта', 'день вылета × день наблюдения'],
  ['table', 'Таблица', 'цены по дням наблюдения и их изменение'],
  ['profile', 'По дням вылета', 'как сдвигался весь профиль цен'],
  ['single', 'Один день', 'крупно один день вылета'],
  ['all', 'Все сразу', 'все виды друг под другом'],
]
const FLIGHT_VIEWS: View[] = ['overlay', 'grid', 'table']
const isView = (v: string | null): v is View => VIEWS.some(([k]) => k === v)
const isFormat = (v: string | null): v is ChartFormat => FORMATS.some(([k]) => k === v)

function filtersFromUrl(sp: URLSearchParams): DynFilters {
  const range = (k: string): [number, number] => {
    const v = sp.get(k)?.split('-').map(Number)
    return v && v.length === 2 && v.every((x) => Number.isFinite(x)) ? [v[0], v[1]] : [0, 24]
  }
  return {
    dep: range('dep'),
    arr: range('arr'),
    maxTransfers: sp.has('tr') ? Number(sp.get('tr')) : -1,
    baggage: sp.get('bag') === '1',
    maxDuration: Number(sp.get('dur') ?? 0) || 0,
    airlines: sp.get('al')?.split(',').filter(Boolean) ?? [],
  }
}

function filtersToUrl(f: DynFilters, sp: URLSearchParams) {
  const set = (k: string, v: string | null) => (v === null ? sp.delete(k) : sp.set(k, v))
  set('dep', f.dep[0] > 0 || f.dep[1] < 24 ? `${f.dep[0]}-${f.dep[1]}` : null)
  set('arr', f.arr[0] > 0 || f.arr[1] < 24 ? `${f.arr[0]}-${f.arr[1]}` : null)
  set('tr', f.maxTransfers >= 0 ? String(f.maxTransfers) : null)
  set('bag', f.baggage ? '1' : null)
  set('dur', f.maxDuration > 0 ? String(f.maxDuration) : null)
  set('al', f.airlines.length ? f.airlines.join(',') : null)
}

export function DynamicsPage() {
  const [sp, setSp] = useSearchParams()
  const [origin, setOrigin] = useState<AirportOption>(() => placeholder(sp.get('o') ?? 'MOW'))
  const [dest, setDest] = useState<AirportOption>(() => placeholder(sp.get('d') ?? 'EVN'))
  const [from, setFrom] = useState(sp.get('from') ?? addDaysISO(todayISO(), 30))
  const [to, setTo] = useState(sp.get('to') ?? '')
  const [history, setHistory] = useState(Number(sp.get('h')) || 60)
  const [mode, setMode] = useState<Mode>(sp.get('m') === 'flight' ? 'flight' : 'min')
  const [view, setView] = useState<View>(() => (isView(sp.get('v')) ? (sp.get('v') as View) : 'overlay'))
  const [format, setFormat] = useState<ChartFormat>(() => (isFormat(sp.get('fmt')) ? (sp.get('fmt') as ChartFormat) : 'line'))
  const [unit, setUnit] = useState<ChartUnit>(sp.get('u') === 'pct' ? 'pct' : 'rub')
  const [progress, setProgress] = useState<{ done: number; total: number } | null>(null)
  const [filters, setFilters] = useState<DynFilters>(() => filtersFromUrl(sp))
  const [data, setData] = useState<DynResponse | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  // выбранные рейсы → слот палитры (цвет следует за рейсом, а не за порядком)
  const [picked, setPicked] = useState<Map<number, number>>(new Map())
  const [showAll, setShowAll] = useState(false)

  // Имена городов из кодов URL.
  useEffect(() => {
    for (const [opt, set] of [[origin, setOrigin], [dest, setDest]] as const) {
      if (opt.label === opt.code) resolveAirport(opt.code).then((a) => a && set(a))
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // Состояние формы и фильтров — в URL.
  useEffect(() => {
    const p = new URLSearchParams()
    p.set('o', origin.code)
    p.set('d', dest.code)
    p.set('from', from)
    if (to && to !== from) p.set('to', to)
    p.set('h', String(history))
    if (mode === 'flight') p.set('m', 'flight')
    if (view !== 'overlay') p.set('v', view)
    if (format !== 'line') p.set('fmt', format)
    if (unit === 'pct') p.set('u', 'pct')
    filtersToUrl(filters, p)
    setSp(p, { replace: true })
  }, [origin.code, dest.code, from, to, history, mode, view, format, unit, filters, setSp])

  const toEff = to || from
  const span = from ? daysBetweenISO(from, toEff) + 1 : 0
  const formError = !from
    ? 'Выберите дату вылета.'
    : origin.code === dest.code
      ? 'Откуда и куда — разные города.'
      : span > MAX_DAYS
        ? `Не больше ${MAX_DAYS} дней вылета подряд.`
        : null

  async function load() {
    if (formError || busy) return
    setBusy(true)
    setError(null)
    setProgress(null)
    try {
      // бэк считает в фоне: опрашиваем теми же параметрами, пока не придёт результат
      let res = await fetchDynamics({ origin: origin.code, destination: dest.code, from, to: toEff, history })
      while (res.pending) {
        setProgress({ done: res.done ?? 0, total: res.total ?? 0 })
        await new Promise((r) => setTimeout(r, 700))
        res = await fetchDynamics({ origin: origin.code, destination: dest.code, from, to: toEff, history })
      }
      if (res.error) {
        setError(res.error)
        setData(null)
      } else {
        setData(res)
        setPicked(new Map())
        setShowAll(false)
      }
    } catch {
      setError('Не удалось связаться с сервером.')
    } finally {
      setBusy(false)
      setProgress(null)
    }
  }

  // Открыли ссылку с параметрами — сразу показываем.
  useEffect(() => {
    if (sp.has('o') && sp.has('d') && sp.has('from')) load()
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const setF = (patch: Partial<DynFilters>) => setFilters({ ...filters, ...patch })

  return (
    <>
      <header className="hero">
        <h1>📈 Динамика цены</h1>
        <div className="sub">
          Как менялась цена билета, если смотреть её в разные дни: по всем снимкам, что коллектор сохранил в озере.{' '}
          <Link to="/" className="dyn-back">← Планировщик</Link>
        </div>
      </header>

      <div className="searchform">
        <div className="dyn-form">
          <AirportCombobox label="Откуда" value={origin} onChange={setOrigin} />
          <button
            type="button"
            className="dyn-swap"
            title="Поменять местами"
            onClick={() => {
              setOrigin(dest)
              setDest(origin)
            }}
          >
            ⇄
          </button>
          <AirportCombobox label="Куда" value={dest} onChange={setDest} />
          <div className="field">
            <label>Даты вылета (до {MAX_DAYS} дней подряд)</label>
            <DateRangePicker
              label="Вылет"
              from={from}
              to={to}
              min={addDaysISO(todayISO(), -HISTORY[HISTORY.length - 1])}
              onChange={(f, t) => {
                setFrom(f)
                setTo(t)
              }}
            />
            <div className="dyn-spans">
              {SPANS.map(([n, l]) => (
                <button
                  key={n}
                  type="button"
                  className={span === n ? 'active' : ''}
                  onClick={() => from && setTo(n === 1 ? '' : addDaysISO(from, n - 1))}
                >
                  {l}
                </button>
              ))}
            </div>
          </div>
          <div className="field">
            <label>Глубина истории</label>
            <select value={history} onChange={(e) => setHistory(Number(e.target.value))}>
              {HISTORY.map((h) => (
                <option key={h} value={h}>
                  {h} {plural(h, 'день', 'дня', 'дней')}
                </option>
              ))}
            </select>
          </div>
          <button type="button" className="btn-primary dyn-go" disabled={!!formError || busy} onClick={load}>
            {busy ? 'Читаем озеро…' : 'Показать →'}
          </button>
        </div>
        {formError && <div className="dyn-hint">{formError}</div>}
        {busy && (
          <div className="dyn-hint">
            Читаем снимки направления из озера
            {progress && progress.total > 0 ? `: ${progress.done} из ${progress.total} файлов` : '…'}
            {progress && progress.total > 0 && (
              <div className="progressbar progressbar-small dyn-progress">
                <div className="progressbar-fill" style={{ width: `${Math.round((progress.done / progress.total) * 100)}%` }} />
              </div>
            )}
          </div>
        )}
        {error && (
          <div className="backend-note">
            <div className="bn-title">Не получилось</div>
            <div>{error}</div>
          </div>
        )}
      </div>

      {data && (
        <Result
          data={data}
          mode={mode}
          setMode={setMode}
          view={view}
          setView={setView}
          format={format}
          setFormat={setFormat}
          unit={unit}
          setUnit={setUnit}
          filters={filters}
          setF={setF}
          resetFilters={() => setFilters(DEFAULT_FILTERS)}
          picked={picked}
          setPicked={setPicked}
          showAll={showAll}
          setShowAll={setShowAll}
        />
      )}
    </>
  )
}

function Result({
  data,
  mode,
  setMode,
  view,
  setView,
  format,
  setFormat,
  unit,
  setUnit,
  filters,
  setF,
  resetFilters,
  picked,
  setPicked,
  showAll,
  setShowAll,
}: {
  data: DynResponse
  mode: Mode
  setMode: (m: Mode) => void
  view: View
  setView: (v: View) => void
  format: ChartFormat
  setFormat: (f: ChartFormat) => void
  unit: ChartUnit
  setUnit: (u: ChartUnit) => void
  filters: DynFilters
  setF: (p: Partial<DynFilters>) => void
  resetFilters: () => void
  picked: Map<number, number>
  setPicked: (m: Map<number, number>) => void
  showAll: boolean
  setShowAll: (v: boolean) => void
}) {
  const days = useMemo(() => dayRange(data.from, data.to), [data])
  const [focusDay, setFocusDay] = useState<string | null>(null)
  const prices = useMemo(() => flightPrices(data, filters.baggage), [data, filters.baggage])
  const ok = useMemo(() => data.flights.map((f) => passes(f, filters)), [data, filters])
  const airlines = useMemo(() => {
    const cnt = new Map<string, number>()
    for (const f of data.flights) if (f.airline) cnt.set(f.airline, (cnt.get(f.airline) ?? 0) + 1)
    return [...cnt.entries()].sort((a, b) => b[1] - a[1]).map(([a]) => a)
  }, [data])

  // Последняя известная цена рейса и первая — для списка и сортировки.
  const flightStats = useMemo(
    () =>
      data.flights.map((_, fi) => {
        const entries = [...prices[fi].entries()].sort((a, b) => a[0] - b[0])
        if (!entries.length) return null
        const lastSnap = data.snapshots.length - 1
        return {
          first: entries[0][1],
          last: entries[entries.length - 1][1],
          lastSi: entries[entries.length - 1][0],
          // рейс есть в самом свежем снимке своего дня — ещё продаётся
          live: entries[entries.length - 1][0] === lastSnapFor(data, data.flights[fi].day, lastSnap),
          min: Math.min(...entries.map((e) => e[1])),
          max: Math.max(...entries.map((e) => e[1])),
          n: entries.length,
        }
      }),
    [data, prices],
  )

  // список рейсов можно сузить до одного дня вылета и отсортировать
  const [listDay, setListDay] = useState('')
  const [sort, setSort] = useState<FlightSort>('now')
  const list = useMemo(() => {
    const st = (fi: number) => flightStats[fi]!
    const cmp: Record<FlightSort, (a: number, b: number) => number> = {
      // продающиеся сейчас — выше, среди них дешевле
      now: (a, b) => Number(st(b).live) - Number(st(a).live) || st(a).last - st(b).last,
      min: (a, b) => st(a).min - st(b).min,
      dep: (a, b) => data.flights[a].departure_at.localeCompare(data.flights[b].departure_at) || st(a).last - st(b).last,
      drop: (a, b) => (st(a).last - st(a).first) / st(a).first - (st(b).last - st(b).first) / st(b).first,
      rise: (a, b) => (st(b).last - st(b).first) / st(b).first - (st(a).last - st(a).first) / st(a).first,
    }
    return data.flights
      .map((_, fi) => fi)
      .filter((fi) => ok[fi] && flightStats[fi] && (!listDay || data.flights[fi].day === listDay))
      .sort(cmp[sort])
  }, [data, ok, flightStats, listDay, sort])
  const cheapestLive = useMemo(() => {
    const live = data.flights.map((_, fi) => fi).filter((fi) => ok[fi] && flightStats[fi]?.live && (!listDay || data.flights[fi].day === listDay))
    return live.sort((a, b) => flightStats[a]!.last - flightStats[b]!.last)[0] ?? list[0]
  }, [data, ok, flightStats, listDay, list])

  // В режиме рейсов без выбора — самый дешёвый из продающихся.
  const effectivePicked = useMemo(() => {
    const m = new Map([...picked].filter(([fi]) => ok[fi] && flightStats[fi]))
    if (!m.size && cheapestLive !== undefined) m.set(cheapestLive, 0)
    return m
  }, [picked, ok, flightStats, cheapestLive])
  // цвет рейса: до 8 выбранных — свой цвет по слоту; больше — шкала по времени вылета
  const colorOf = useMemo(() => {
    const keys = [...effectivePicked.keys()]
    if (keys.length <= PALETTE.length) return (fi: number) => PALETTE[effectivePicked.get(fi)! % PALETTE.length]
    const byDep = [...keys].sort((a, b) => data.flights[a].departure_at.localeCompare(data.flights[b].departure_at))
    return (fi: number) => orderedColor(byDep.indexOf(fi), byDep.length)
  }, [effectivePicked, data])

  const daySeries: Series[] = useMemo(() => minSeries(data, ok, prices, days), [data, ok, prices, days])
  const series: Series[] = useMemo(
    () =>
      mode === 'min'
        ? daySeries
        : flightSeries(data, [...effectivePicked.keys()], prices, colorOf),
    [mode, data, daySeries, prices, effectivePicked, colorOf],
  )
  const effView: View = mode === 'flight' && !FLIGHT_VIEWS.includes(view) ? 'overlay' : view
  // виды без графика: формат и подпись про точки не нужны
  const chartless = effView === 'table' || effView === 'heatmap'

  function toggle(fi: number) {
    const m = new Map(effectivePicked)
    if (m.has(fi)) m.delete(fi)
    else {
      if (m.size >= MAX_PICK) return
      const used = new Set(m.values())
      let slot = 0
      while (used.has(slot)) slot++
      m.set(fi, slot)
    }
    setPicked(m)
  }

  // выбрать все рейсы списка (с учётом дня и фильтров) — до MAX_PICK
  function pickAll() {
    const m = new Map<number, number>()
    list.slice(0, MAX_PICK).forEach((fi, i) => m.set(fi, i))
    setPicked(m)
  }

  const nOk = ok.filter(Boolean).length
  const filtersActive = JSON.stringify(filters) !== JSON.stringify(DEFAULT_FILTERS)
  const snaps = data.snapshots
  const visible = showAll ? list : list.slice(0, 40)

  return (
    <>
      <div className="freshbar">
        <div className="fb-info">
          <b>{data.origin_name || data.origin} → {data.destination_name || data.destination}</b>, вылет{' '}
          {days.length > 1 ? `${dayLabel(data.from)} – ${dayLabel(data.to)}` : dayLabel(data.from)} ·{' '}
          <b>{snaps.length}</b> {plural(snaps.length, 'снимок', 'снимка', 'снимков')}
          {snaps.length > 0 && <> с {snapLabel(snapTime(snaps[0]), false)} по {snapLabel(snapTime(snaps[snaps.length - 1]))}</>} ·{' '}
          <b>{data.flights.length}</b> {plural(data.flights.length, 'рейс', 'рейса', 'рейсов')}
          {data.seconds !== undefined && <> · {data.seconds} с</>}
        </div>
        {data.files_failed > 0 && <div className="fb-error">Не прочитано файлов: {data.files_failed}</div>}
      </div>

      {snaps.length === 0 ? (
        <div className="empty">
          В озере нет снимков {data.origin_city}→ANY на эти дни за последние {data.history_days} дн. Коллектор собирает
          направления по городу вылета — возможно, город ещё не обходили.
        </div>
      ) : (
        <>
          <div className="controls dyn-filters">
            <div className="ctl">
              <label>
                Вылет: <span className="rangeval">{hoursLabel(filters.dep)}</span>
              </label>
              <RangeSlider min={0} max={24} step={0.5} value={filters.dep} onChange={(v) => setF({ dep: v })} />
            </div>
            <div className="ctl">
              <label>
                Прилёт: <span className="rangeval">{hoursLabel(filters.arr)}</span>
              </label>
              <RangeSlider min={0} max={24} step={0.5} value={filters.arr} onChange={(v) => setF({ arr: v })} />
            </div>
            <div className="ctl">
              <label>Пересадки</label>
              <div className="segbtns">
                {[
                  [-1, 'Любые'],
                  [0, 'Прямой'],
                  [1, '≤ 1'],
                  [2, '≤ 2'],
                ].map(([v, l]) => (
                  <button key={v} className={filters.maxTransfers === v ? 'active' : ''} onClick={() => setF({ maxTransfers: v as number })}>
                    {l}
                  </button>
                ))}
              </div>
            </div>
            <div className="ctl">
              <label>Багаж</label>
              <div className="segbtns">
                <button className={!filters.baggage ? 'active' : ''} onClick={() => setF({ baggage: false })}>
                  Любой
                </button>
                <button className={filters.baggage ? 'active' : ''} onClick={() => setF({ baggage: true })}>
                  С багажом
                </button>
              </div>
            </div>
            <div className="ctl">
              <label>В пути</label>
              <select value={filters.maxDuration} onChange={(e) => setF({ maxDuration: Number(e.target.value) })}>
                {[0, 4, 6, 8, 12, 18, 24].map((h) => (
                  <option key={h} value={h}>
                    {h ? `до ${h} ч` : 'сколько угодно'}
                  </option>
                ))}
              </select>
            </div>
            {airlines.length > 1 && (
              <div className="ctl ctl-windows">
                <label>
                  Авиакомпании{' '}
                  {filters.airlines.length > 0 && (
                    <button className="ctl-clear" onClick={() => setF({ airlines: [] })}>
                      все
                    </button>
                  )}
                </label>
                <div className="dyn-chips">
                  {airlines.slice(0, 24).map((a) => (
                    <button
                      key={a}
                      className={`dyn-chip ${filters.airlines.includes(a) ? 'active' : ''}`}
                      onClick={() =>
                        setF({ airlines: filters.airlines.includes(a) ? filters.airlines.filter((x) => x !== a) : [...filters.airlines, a] })
                      }
                    >
                      {a}
                    </button>
                  ))}
                </div>
              </div>
            )}
            <div className="ctl-sub dyn-filter-sum">
              Под фильтры: <b>{nOk}</b> из {data.flights.length} {plural(data.flights.length, 'рейса', 'рейсов', 'рейсов')}
              {filtersActive && (
                <button className="ctl-clear" onClick={resetFilters}>
                  сбросить фильтры
                </button>
              )}
            </div>
          </div>

          <div className="dyn-viewbar">
            <div className="dyn-vb-group">
              <span className="dyn-vb-l">Что считаем</span>
              <div className="segbtns">
                <button className={mode === 'min' ? 'active' : ''} onClick={() => setMode('min')}>
                  Самая низкая цена
                </button>
                <button className={mode === 'flight' ? 'active' : ''} onClick={() => setMode('flight')}>
                  Конкретный рейс
                </button>
              </div>
            </div>
            <div className="dyn-vb-group">
              <span className="dyn-vb-l">Шкала</span>
              <div className="segbtns">
                <button className={unit === 'rub' ? 'active' : ''} onClick={() => setUnit('rub')}>₽</button>
                <button className={unit === 'pct' ? 'active' : ''} onClick={() => setUnit('pct')} title="В процентах от первой цены каждой линии">
                  % от первой
                </button>
              </div>
            </div>
            <div className="dyn-vb-group wide">
              <span className="dyn-vb-l">Вид</span>
              <div className="dyn-views">
                {VIEWS.map(([k, l, hint]) => {
                  const off = mode === 'flight' && !FLIGHT_VIEWS.includes(k)
                  return (
                    <button key={k} className={`${effView === k ? 'active' : ''} ${off ? 'off' : ''}`} disabled={off} title={off ? 'Только для «самой низкой цены»' : hint} onClick={() => setView(k)}>
                      {l}
                    </button>
                  )
                })}
              </div>
            </div>
            {!chartless && (
            <div className="dyn-vb-group wide">
              <span className="dyn-vb-l">Формат графика</span>
              <div className="dyn-views">
                {FORMATS.map(([k, l]) => (
                  <button key={k} className={format === k ? 'active' : ''} onClick={() => setFormat(k)} title={k === 'band' ? 'От самой низкой цены до медианы подходящих рейсов' : undefined}>
                    {l}
                  </button>
                ))}
              </div>
            </div>
            )}
          </div>

          <Stats series={series} />

          {mode === 'flight' ? (
            effView === 'grid' ? (
              <GridView series={series} format={format} unit={unit} />
            ) : effView === 'table' ? (
              <TableView series={series} snapshots={data.snapshots} />
            ) : (
              <PriceChart series={unit === 'pct' ? toPct(series) : series} format={format} unit={unit} emptyText="Выберите рейс в списке ниже." />
            )
          ) : (
            <MinViews
              view={effView}
              series={daySeries}
              snapshots={data.snapshots}
              format={format}
              unit={unit}
              focusDay={focusDay}
              setFocusDay={setFocusDay}
              flightLabel={(fi) => `${clock(data.flights[fi].departure_at)} · ${data.flights[fi].flights.join(' + ')}`}
            />
          )}
          {!chartless && <div className="dyn-note">
            Точка — снимок: когда коллектор смотрел цены. Разрыв линии — в этом снимке подходящих билетов не было (распроданы
            или не продавались). {filters.baggage ? 'Цена — тарифы с багажом.' : 'Цена — самый дешёвый тариф (багаж любой).'}
            {format === 'band' && mode === 'min' && ' Коридор — от самой низкой цены до медианы подходящих рейсов в снимке.'}
            {format === 'step' && ' Ступеньки: цена держится до следующего снимка.'}
          </div>}

          {mode === 'flight' && (
            <div className="dyn-flights">
              <div className="count dyn-flights-h">
                <span>
                  Рейсы под фильтры: <b>{list.length}</b> · на графике <b>{effectivePicked.size}</b> — кликните рейс, чтобы
                  добавить или убрать
                </span>
                <span className="dyn-flights-tools">
                  <button className="btn-ghost" onClick={pickAll} disabled={!list.length}>
                    {list.length > MAX_PICK ? `Выбрать первые ${MAX_PICK}` : 'Выбрать все'}
                  </button>
                  <button className="btn-ghost" onClick={() => setPicked(new Map())} disabled={effectivePicked.size <= 1}>
                    Сбросить
                  </button>
                  <select value={sort} onChange={(e) => setSort(e.target.value as FlightSort)} title="Сортировка">
                    {FLIGHT_SORTS.map(([k, l]) => (
                      <option key={k} value={k}>
                        {l}
                      </option>
                    ))}
                  </select>
                {days.length > 1 && (
                  <select value={listDay} onChange={(e) => setListDay(e.target.value)}>
                    <option value="">все дни вылета</option>
                    {days.map((d) => (
                      <option key={d} value={d}>
                        {dayLabel(d)}
                      </option>
                    ))}
                  </select>
                )}
                </span>
              </div>
              {visible.map((fi) => {
                const f = data.flights[fi]
                const st = flightStats[fi]!
                const on = effectivePicked.has(fi)
                const col = on ? colorOf(fi) : undefined
                const diff = st.last - st.first
                const shift = dayShift(f.departure_at, f.arrival_at)
                return (
                  <div
                    key={fi}
                    className={`dyn-flight ${on ? 'on' : ''} ${st.live ? '' : 'gone'}`}
                    onClick={() => toggle(fi)}
                  >
                    <span className="dyn-swatch" style={col ? { background: col, borderColor: col } : undefined} />
                    <div className="dyn-f-main">
                      <div className="dyn-f-time">
                        {days.length > 1 && <span className="dyn-f-day">{dayLabel(f.day)}</span>}
                        <b>{clock(f.departure_at)}</b> → <b>{clock(f.arrival_at)}</b>
                        {shift > 0 && <sup>+{shift}</sup>}
                        <span className="dyn-f-dur">{durFmt(f.duration ?? 0)}</span>
                      </div>
                      <div className="dyn-f-sub">
                        {f.flights.join(' + ')} · {f.chain.join(' → ')} ·{' '}
                        {f.transfers === 0 ? 'прямой' : `${f.transfers} ${plural(f.transfers, 'пересадка', 'пересадки', 'пересадок')}`}
                        {!st.live && ' · в последнем снимке нет'}
                      </div>
                    </div>
                    <div className="dyn-f-price">
                      <b>{money(st.last)}</b>
                      <span className={diff > 0 ? 'up' : diff < 0 ? 'down' : ''}>
                        {diff === 0 ? 'без изменений' : `${diff > 0 ? '▲' : '▼'} ${money(Math.abs(diff))}`}
                      </span>
                      <small>
                        мин {money(st.min)} · макс {money(st.max)} · {st.n} {plural(st.n, 'снимок', 'снимка', 'снимков')}
                      </small>
                    </div>
                    {f.link && (
                      <a className="dyn-f-buy" href={f.link} target="_blank" rel="noreferrer" onClick={(e) => e.stopPropagation()}>
                        купить
                      </a>
                    )}
                  </div>
                )
              })}
              {!showAll && list.length > visible.length && (
                <button className="btn-ghost dyn-more" onClick={() => setShowAll(true)}>
                  Показать все {list.length}
                </button>
              )}
            </div>
          )}
        </>
      )}
    </>
  )
}

// Виды «самой низкой цены» по дням вылета.
function MinViews({
  view,
  series,
  snapshots,
  format,
  unit,
  focusDay,
  setFocusDay,
  flightLabel,
}: {
  view: View
  series: Series[]
  snapshots: DynResponse['snapshots']
  flightLabel: (fi: number) => string
  format: ChartFormat
  unit: ChartUnit
  focusDay: string | null
  setFocusDay: (d: string) => void
}) {
  const one = (v: View) => {
    switch (v) {
      case 'overlay':
        return <PriceChart series={unit === 'pct' ? toPct(series) : series} format={format} unit={unit} emptyText="Под фильтры нет ни одного билета — ослабьте фильтры." />
      case 'grid':
        return <GridView series={series} format={format} unit={unit} onPick={setFocusDay} />
      case 'week':
        return <WeekView series={series} format={format} unit={unit} />
      case 'calendar':
        return (
          <>
            <CalendarView series={series} onPick={setFocusDay} picked={focusDay} />
            {focusDay && <SingleView series={series} format={format} unit={unit} day={focusDay} setDay={setFocusDay} />}
          </>
        )
      case 'heatmap':
        return <HeatmapView series={series} snapshots={snapshots} />
      case 'table':
        return <TableView series={series} snapshots={snapshots} flightLabel={flightLabel} />
      case 'profile':
        return <ProfileView series={series} snapshots={snapshots} format={format} unit={unit} />
      case 'single':
        return <SingleView series={series} format={format} unit={unit} day={focusDay} setDay={setFocusDay} />
      default:
        return null
    }
  }
  if (view !== 'all') return one(view)
  return (
    <div className="dyn-all">
      {VIEWS.filter(([k]) => k !== 'all').map(([k, l, hint]) => (
        <section key={k} className="dyn-all-sec">
          <h3>
            {l} <span>{hint}</span>
          </h3>
          {one(k)}
        </section>
      ))}
    </div>
  )
}

// Индекс последнего снимка, покрывшего день.
function lastSnapFor(data: DynResponse, day: string, fallback: number): number {
  for (let i = data.snapshots.length - 1; i >= 0; i--) if (data.snapshots[i].days.includes(day)) return i
  return fallback
}

// Плитки: сейчас (дешевле всего среди линий по их последнему снимку), минимум и максимум
// за историю, изменение с первого снимка (так же — по первым снимкам линий).
function Stats({ series }: { series: Series[] }) {
  const pts = series.flatMap((s) => s.points.filter((p) => p.v !== null).map((p) => ({ ...p, s })))
  if (!pts.length) return null
  const ends = series
    .map((s) => {
      const vals = s.points.filter((p) => p.v !== null)
      const last = s.points[s.points.length - 1]
      return { s, last: last && last.v !== null ? last : null, first: vals[0] ?? null }
    })
  const lastOk = ends.filter((e) => e.last)
  const nowE = lastOk.length ? lastOk.reduce((a, b) => (b.last!.v! < a.last!.v! ? b : a)) : null
  const firstOk = ends.filter((e) => e.first)
  const firstE = firstOk.reduce((a, b) => (b.first!.v! < a.first!.v! ? b : a))
  const lo = pts.reduce((a, b) => (b.v! < a.v! ? b : a))
  const hi = pts.reduce((a, b) => (b.v! > a.v! ? b : a))
  const many = series.length > 1
  const now = nowE ? nowE.last!.v! : null
  const first = firstE.first!.v!
  const diff = now !== null ? now - first : null
  return (
    <div className="stats dyn-stats">
      <div className="stat">
        <div className="n">{now !== null ? money(now) : '—'}</div>
        <div className="l">
          {many ? 'дешевле всего сейчас' : 'последний снимок'}
          {nowE && <>, {many ? nowE.s.label : snapLabel(nowE.last!.t)}</>}
        </div>
      </div>
      <div className="stat">
        <div className="n dyn-good">{money(lo.v!)}</div>
        <div className="l">минимум — {snapLabel(lo.t)}{many ? `, ${lo.s.label}` : ''}</div>
      </div>
      <div className="stat">
        <div className="n">{money(hi.v!)}</div>
        <div className="l">максимум — {snapLabel(hi.t)}{many ? `, ${hi.s.label}` : ''}</div>
      </div>
      <div className="stat">
        <div className={`n ${diff && diff > 0 ? 'dyn-bad' : diff && diff < 0 ? 'dyn-good' : ''}`}>
          {diff === null ? '—' : diff === 0 ? '0 ₽' : `${diff > 0 ? '+' : '−'}${money(Math.abs(diff))}`}
        </div>
        <div className="l">
          {many ? 'дешевле всего сейчас vs в первых снимках' : `с первого снимка (${snapLabel(firstE.first!.t, false)})`}
          {diff !== null && first ? `, ${diff > 0 ? '+' : ''}${Math.round((diff / first) * 100)}%` : ''}
        </div>
      </div>
    </div>
  )
}
