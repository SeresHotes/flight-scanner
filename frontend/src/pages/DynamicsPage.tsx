import { useEffect, useMemo, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { AirportCombobox } from '../components/AirportCombobox'
import { DateRangePicker } from '../components/DateRangePicker'
import { resolveAirport, type AirportOption } from '../data/airports'
import { addDaysISO, daysBetweenISO, todayISO } from '../lib/dates'
import { durFmt, plural } from '../lib/format'
import { RangeSlider } from '../planner/components/RangeSlider'
import { PriceChart } from '../dynamics/PriceChart'
import {
  DEFAULT_FILTERS,
  PALETTE,
  clock,
  dayLabel,
  dayShift,
  fetchDynamics,
  flightPrices,
  flightSeries,
  hoursLabel,
  minSeries,
  money,
  passes,
  snapLabel,
  snapTime,
  type DynFilters,
  type DynResponse,
  type Series,
} from '../dynamics/data'

// Отдельная страница «Динамика цены»: два города, день(и) вылета и фильтры — как менялась
// цена по снимкам озера (каждая повторная выборка коллектора — снимок). Два режима:
// самая низкая цена под фильтры (линия на каждый день вылета) и конкретные рейсы.
// Состояние — в URL (ссылку можно переслать), с планировщиком ничего не делит.

const MAX_DAYS = 7
const MAX_PICK = 8
const HISTORY = [14, 30, 60, 90, 180]
const placeholder = (code: string): AirportOption => ({ code, city: code, label: code })

type Mode = 'min' | 'flight'

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
    filtersToUrl(filters, p)
    setSp(p, { replace: true })
  }, [origin.code, dest.code, from, to, history, mode, filters, setSp])

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
    try {
      const res = await fetchDynamics({ origin: origin.code, destination: dest.code, from, to: toEff, history })
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
            <label>Дата вылета (можно до {MAX_DAYS} дней подряд)</label>
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
        {busy && <div className="dyn-hint">Читаем все снимки направления из озера — обычно несколько секунд.</div>}
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
  filters: DynFilters
  setF: (p: Partial<DynFilters>) => void
  resetFilters: () => void
  picked: Map<number, number>
  setPicked: (m: Map<number, number>) => void
  showAll: boolean
  setShowAll: (v: boolean) => void
}) {
  const days = useMemo(() => {
    const out: string[] = []
    for (let d = data.from; d <= data.to; d = addDaysISO(d, 1)) out.push(d)
    return out
  }, [data])
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

  const list = useMemo(
    () =>
      data.flights
        .map((_, fi) => fi)
        .filter((fi) => ok[fi] && flightStats[fi])
        .sort((a, b) => Number(flightStats[b]!.live) - Number(flightStats[a]!.live) || flightStats[a]!.last - flightStats[b]!.last),
    [data, ok, flightStats],
  )

  // В режиме рейсов без выбора — самый дешёвый из продающихся.
  const effectivePicked = useMemo(() => {
    const m = new Map([...picked].filter(([fi]) => ok[fi] && flightStats[fi]))
    if (!m.size && list.length) m.set(list[0], 0)
    return m
  }, [picked, ok, flightStats, list])

  const series: Series[] = useMemo(
    () =>
      mode === 'min'
        ? minSeries(data, ok, prices, days)
        : flightSeries(data, [...effectivePicked.keys()], prices, (fi) => PALETTE[effectivePicked.get(fi)! % PALETTE.length]),
    [mode, data, ok, prices, days, effectivePicked],
  )

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

          <div className="segbtns big dyn-modes">
            <button className={mode === 'min' ? 'active' : ''} onClick={() => setMode('min')}>
              Самая низкая цена
            </button>
            <button className={mode === 'flight' ? 'active' : ''} onClick={() => setMode('flight')}>
              Конкретный рейс
            </button>
          </div>

          <Stats series={series} />

          <PriceChart
            series={series}
            emptyText={mode === 'flight' ? 'Выберите рейс в списке ниже.' : 'Под фильтры нет ни одного билета — ослабьте фильтры.'}
          />
          <div className="dyn-note">
            Точка — снимок: когда коллектор смотрел цены. Разрыв линии — в этом снимке подходящих билетов не было (распроданы
            или не продавались). {filters.baggage ? 'Цена — тарифы с багажом.' : 'Цена — самый дешёвый тариф (багаж любой).'}
          </div>

          {mode === 'min' ? (
            <MinTable data={data} series={series} />
          ) : (
            <div className="dyn-flights">
              <div className="count">
                Рейсы под фильтры: <b>{list.length}</b> · отметьте до {MAX_PICK}, чтобы сравнить на графике
              </div>
              {visible.map((fi) => {
                const f = data.flights[fi]
                const st = flightStats[fi]!
                const slot = effectivePicked.get(fi)
                const diff = st.last - st.first
                const shift = dayShift(f.departure_at, f.arrival_at)
                return (
                  <div
                    key={fi}
                    className={`dyn-flight ${slot !== undefined ? 'on' : ''} ${st.live ? '' : 'gone'}`}
                    onClick={() => toggle(fi)}
                  >
                    <span className="dyn-swatch" style={slot !== undefined ? { background: PALETTE[slot % PALETTE.length], borderColor: PALETTE[slot % PALETTE.length] } : undefined} />
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
                        {money(st.min)} – {money(st.max)} · {st.n} {plural(st.n, 'снимок', 'снимка', 'снимков')}
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

// Индекс последнего снимка, покрывшего день.
function lastSnapFor(data: DynResponse, day: string, fallback: number): number {
  for (let i = data.snapshots.length - 1; i >= 0; i--) if (data.snapshots[i].days.includes(day)) return i
  return fallback
}

// Плитки: сейчас, минимум и максимум за историю, изменение с первого снимка.
function Stats({ series }: { series: Series[] }) {
  const pts = series.flatMap((s) => s.points.filter((p) => p.v !== null).map((p) => ({ ...p, s })))
  if (!pts.length) return null
  const byT = [...pts].sort((a, b) => a.t - b.t)
  const lastT = byT[byT.length - 1].t
  const firstT = byT[0].t
  const at = (t: number) => Math.min(...pts.filter((p) => p.t === t).map((p) => p.v!))
  const now = at(lastT)
  const first = at(firstT)
  const lo = pts.reduce((a, b) => (b.v! < a.v! ? b : a))
  const hi = pts.reduce((a, b) => (b.v! > a.v! ? b : a))
  const diff = now - first
  return (
    <div className="stats dyn-stats">
      <div className="stat">
        <div className="n">{money(now)}</div>
        <div className="l">последний снимок, {snapLabel(lastT)}</div>
      </div>
      <div className="stat">
        <div className="n dyn-good">{money(lo.v!)}</div>
        <div className="l">минимум — {snapLabel(lo.t)}{series.length > 1 ? `, ${lo.s.label}` : ''}</div>
      </div>
      <div className="stat">
        <div className="n">{money(hi.v!)}</div>
        <div className="l">максимум — {snapLabel(hi.t)}{series.length > 1 ? `, ${hi.s.label}` : ''}</div>
      </div>
      <div className="stat">
        <div className={`n ${diff > 0 ? 'dyn-bad' : diff < 0 ? 'dyn-good' : ''}`}>
          {diff === 0 ? '0 ₽' : `${diff > 0 ? '+' : '−'}${money(Math.abs(diff))}`}
        </div>
        <div className="l">с первого снимка ({snapLabel(firstT, false)}){first ? `, ${diff > 0 ? '+' : ''}${Math.round((diff / first) * 100)}%` : ''}</div>
      </div>
    </div>
  )
}

// Таблица режима «самая низкая цена»: снимок × день вылета, какой рейс дал минимум.
function MinTable({ data, series }: { data: DynResponse; series: Series[] }) {
  const rows = [...data.snapshots.keys()].reverse()
  return (
    <details className="dyn-table" open={series.length === 1}>
      <summary>Таблица по снимкам</summary>
      <div className="coll-table-wrap">
        <table className="coll-table">
          <thead>
            <tr>
              <th>Смотрели</th>
              {series.map((s) => (
                <th key={s.id}>{s.label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((si) => (
              <tr key={si}>
                <td>{snapLabel(snapTime(data.snapshots[si]))}</td>
                {series.map((s) => {
                  const p = s.points.find((p) => p.si === si)
                  const f = p?.fi !== undefined ? data.flights[p.fi] : null
                  return (
                    <td key={s.id} className="num">
                      {!p ? '—' : p.v === null ? 'нет билетов' : (
                        <>
                          <b>{money(p.v)}</b>
                          {f && <div className="dyn-cell-sub">{clock(f.departure_at)} · {f.flights.join(' + ')}</div>}
                        </>
                      )}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </details>
  )
}
