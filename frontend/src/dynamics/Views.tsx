import { useContext, useMemo, useState } from 'react'
import { PriceChart, Sparkline, YFitContext, domains, type ChartFormat, type ChartUnit } from './PriceChart'
import {
  addDays,
  dayLabel,
  PALETTE,
  heatColor,
  heatInk,
  heatMatrix,
  money,
  pct,
  plural,
  profileSeries,
  snapLabel,
  toPct,
  type DynSnapshot,
  type Series,
} from './data'

// Виды динамики по нескольким дням вылета. Все получают линии «самая низкая цена по
// дню вылета» (Series на день, id = 'YYYY-MM-DD') и рисуют их по-разному.

type ViewProps = { series: Series[]; format: ChartFormat; unit: ChartUnit }

const MON_FULL = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']
const WD = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']

const scaled = (series: Series[], unit: ChartUnit) => (unit === 'pct' ? toPct(series) : series)

// Последняя цена серии, первая и изменение.
export function summary(s: Series): { last: number | null; first: number | null; diff: number | null } {
  const vals = s.points.filter((p) => p.v !== null)
  const lastP = s.points[s.points.length - 1]
  const last = lastP && lastP.v !== null ? lastP.v : null
  const first = vals.length ? vals[0].v : null
  return { last, first, diff: last !== null && first !== null ? last - first : null }
}

function Delta({ diff, first }: { diff: number | null; first: number | null }) {
  if (diff === null || first === null) return null
  if (diff === 0) return <span className="dyn-delta">=</span>
  return (
    <span className={`dyn-delta ${diff > 0 ? 'up' : 'down'}`}>
      {diff > 0 ? '▲' : '▼'} {pct((diff / first) * 100)}
    </span>
  )
}

// ---------------------------------------------------------------- рядом (small multiples)

export function GridView({ series, format, unit, onPick }: ViewProps & { onPick?: (id: string) => void }) {
  // в «рядом» каждая карточка подписана — цвет один на всех
  const sc = useMemo(() => scaled(series, unit).map((s) => (series.length > PALETTE.length ? { ...s, color: PALETTE[0] } : s)), [series, unit])
  const fit = useContext(YFitContext)
  const dom = useMemo(() => domains(sc, format, fit), [sc, format, fit])
  return (
    <div className="dyn-grid-view">
      {sc.map((s, i) => {
        const sm = summary(series[i])
        return (
          <div key={s.id} className="dyn-mini-card">
            <div className="dyn-mini-h" onClick={() => onPick?.(s.id)}>
              <i style={{ background: s.color }} />
              <span className="dyn-mini-t">{s.label}</span>
              <b>{sm.last !== null ? money(sm.last) : '—'}</b>
              <Delta diff={sm.diff} first={sm.first} />
            </div>
            <PriceChart series={[s]} format={format} unit={unit} mini height={130} xDomain={dom?.x} yDomain={dom?.y} emptyText="нет билетов" />
          </div>
        )
      })}
    </div>
  )
}

// ---------------------------------------------------------------- неделя

function weekStart(iso: string): string {
  const [y, m, d] = iso.split('-').map(Number)
  const wd = (new Date(Date.UTC(y, m - 1, d)).getUTCDay() + 6) % 7
  return addDays(iso, -wd)
}

export function WeekView({ series, format, unit }: ViewProps) {
  const weeks = useMemo(() => [...new Set(series.map((s) => weekStart(s.id)))], [series])
  const [week, setWeek] = useState(0)
  const [hidden, setHidden] = useState<Set<string>>(new Set())
  const w = Math.min(week, weeks.length - 1)
  const start = weeks[w]
  const days = Array.from({ length: 7 }, (_, i) => addDays(start, i))
  // дни недели — категориальная палитра по дню недели (цвет следует за днём)
  const inWeek = series.filter((s) => days.includes(s.id)).map((s) => ({ ...s, color: PALETTE[days.indexOf(s.id)] }))
  const shown = inWeek.filter((s) => !hidden.has(s.id))
  const cheapest = Math.min(...inWeek.map((s) => summary(s).last ?? Infinity))
  return (
    <div className="dyn-week">
      {weeks.length > 1 && (
        <div className="dyn-week-nav">
          <button className="btn-ghost" disabled={w === 0} onClick={() => setWeek(w - 1)}>‹</button>
          <span>
            Неделя {dayLabel(start).split(',')[0]} – {dayLabel(addDays(start, 6)).split(',')[0]}
          </span>
          <button className="btn-ghost" disabled={w >= weeks.length - 1} onClick={() => setWeek(w + 1)}>›</button>
        </div>
      )}
      <div className="dyn-week-strip">
        {days.map((d, i) => {
          const s = inWeek.find((x) => x.id === d)
          if (!s) return <div key={d} className="dyn-week-day off"><span className="wd">{WD[i]}</span><span className="dd">{+d.slice(8)}</span></div>
          const sm = summary(s)
          const off = hidden.has(d)
          return (
            <button
              key={d}
              className={`dyn-week-day ${off ? 'muted' : ''} ${sm.last === cheapest ? 'best' : ''}`}
              onClick={() => {
                const h = new Set(hidden)
                if (h.has(d)) h.delete(d)
                else h.add(d)
                setHidden(h)
              }}
              title={off ? 'Показать на графике' : 'Скрыть с графика'}
            >
              <span className="wd"><i style={{ background: off ? 'transparent' : s.color, borderColor: s.color }} />{WD[i]}</span>
              <span className="dd">{+d.slice(8)}</span>
              <b>{sm.last !== null ? money(sm.last) : '—'}</b>
              <Delta diff={sm.diff} first={sm.first} />
            </button>
          )
        })}
      </div>
      <PriceChart series={scaled(shown, unit)} format={format} unit={unit} emptyText="Все дни скрыты или без билетов." />
    </div>
  )
}

// ---------------------------------------------------------------- календарь

export function CalendarView({ series, onPick, picked }: { series: Series[]; onPick?: (id: string) => void; picked?: string | null }) {
  const byDay = useMemo(() => new Map(series.map((s) => [s.id, s])), [series])
  const lasts = series.map((s) => summary(s).last).filter((v): v is number => v !== null)
  const [lo, hi] = [Math.min(...lasts), Math.max(...lasts)]
  const months = [...new Set(series.map((s) => s.id.slice(0, 7)))]
  return (
    <div className="dyn-cal">
      <div className="dyn-cal-legend">
        Цвет — цена в последнем снимке: <span className="sw" style={{ background: heatColor(hi, lo, hi) }} /> дороже
        <span className="sw" style={{ background: heatColor((lo + hi) / 2, lo, hi) }} />
        <span className="sw" style={{ background: heatColor(lo, lo, hi) }} /> дешевле · линия — как менялась цена дня
      </div>
      {months.map((ym) => {
        const [y, m] = ym.split('-').map(Number)
        // только недели, задевающие выбранные дни этого месяца
        const inMonth = series.filter((s) => s.id.startsWith(ym)).map((s) => s.id)
        const start = weekStart(inMonth[0])
        const end = addDays(weekStart(inMonth[inMonth.length - 1]), 6)
        const cells: (string | null)[] = []
        for (let d = start; d <= end; d = addDays(d, 1)) cells.push(d.startsWith(ym) ? d : null)
        return (
          <div key={ym} className="dyn-cal-month">
            <div className="dyn-cal-title">{MON_FULL[m - 1]} {y}</div>
            <div className="dyn-cal-grid">
              {WD.map((w) => <div key={w} className="dyn-cal-wd">{w}</div>)}
              {cells.map((d, i) => {
                if (!d) return <div key={i} className="dyn-cal-cell empty" />
                const s = byDay.get(d)
                if (!s) return <div key={i} className="dyn-cal-cell out"><span className="dn">{+d.slice(8)}</span></div>
                const sm = summary(s)
                const bg = sm.last !== null ? heatColor(sm.last, lo, hi) : undefined
                return (
                  <button key={i} className={`dyn-cal-cell ${picked === d ? 'picked' : ''} ${sm.last === lo ? 'best' : ''}`} style={{ background: bg, color: sm.last !== null ? heatInk(sm.last, lo, hi) : undefined }} onClick={() => onPick?.(d)}>
                    <span className="dn">{+d.slice(8)}</span>
                    {sm.last === lo && <span className="star" title="Дешевле всего">★</span>}
                    <b className="full">{sm.last !== null ? money(sm.last) : '—'}</b>
                    <b className="short">{sm.last !== null ? short(sm.last) : '—'}</b>
                    <Sparkline series={{ ...s, color: sm.last !== null ? heatInk(sm.last, lo, hi) : '#e8ecf3' }} />
                    <Delta diff={sm.diff} first={sm.first} />
                  </button>
                )
              })}
            </div>
          </div>
        )
      })}
    </div>
  )
}

// ---------------------------------------------------------------- тепловая карта

const short = (v: number) => (v >= 100000 ? `${Math.round(v / 1000)}к` : `${(v / 1000).toFixed(1)}к`)

export function HeatmapView({ series, snapshots }: { series: Series[]; snapshots: DynSnapshot[] }) {
  const { cols, rows } = useMemo(() => heatMatrix(series, snapshots), [series, snapshots])
  const [perRow, setPerRow] = useState(false)
  const vals = rows.flat().map((c) => c.v).filter((v): v is number => v !== null)
  if (!vals.length) return <div className="dyn-chart-empty">Нет данных под фильтры.</div>
  const [lo, hi] = [Math.min(...vals), Math.max(...vals)]
  // шкала: общая на всю таблицу или своя у каждой строки (видно движение внутри дня)
  const rowRange = rows.map((r) => {
    const v = r.map((c) => c.v).filter((x): x is number => x !== null)
    return perRow && v.length ? [Math.min(...v), Math.max(...v)] : [lo, hi]
  })
  return (
    <div className="dyn-heat">
      <div className="dyn-cal-legend">
        Строка — день вылета, столбец — день, когда смотрели. Ярче — дешевле
        {!perRow && (
          <>
            {' '}(<span className="sw" style={{ background: heatColor(lo, lo, hi) }} /> {money(lo)} …{' '}
            <span className="sw" style={{ background: heatColor(hi, lo, hi) }} /> {money(hi)})
          </>
        )}
        ; бледные — снимка в этот день не было, показана последняя известная цена.
        <span className="segbtns dyn-heat-mode">
          <button className={!perRow ? 'active' : ''} onClick={() => setPerRow(false)}>общая шкала</button>
          <button className={perRow ? 'active' : ''} onClick={() => setPerRow(true)}>своя у каждого дня</button>
        </span>
      </div>
      <div className="dyn-heat-wrap">
        <table className="dyn-heat-table">
          <thead>
            <tr>
              <th className="corner">вылет \ смотрели</th>
              {cols.map((c, i) => (
                <th key={c} className="col">{i === 0 || c.endsWith('-01') || i % 3 === 0 ? `${+c.slice(8)}.${c.slice(5, 7)}` : ''}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, ri) => (
              <tr key={series[ri].id}>
                <th className="rowh">{series[ri].label}</th>
                {r.map((c, ci) => (
                  <td
                    key={ci}
                    className={`${c.seen ? 'seen' : 'carry'} ${c.v === null ? 'none' : ''}`}
                    style={c.v !== null ? { background: heatColor(c.v, rowRange[ri][0], rowRange[ri][1]), color: heatInk(c.v, rowRange[ri][0], rowRange[ri][1]) } : undefined}
                    title={`Вылет ${series[ri].label}, смотрели ${cols[ci]}: ${c.v === null ? 'нет данных' : money(c.v)}${c.seen ? '' : ' (последняя известная)'}`}
                  >
                    {c.seen && c.v !== null && cols.length <= 45 ? short(c.v) : ''}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------- профиль по дням вылета

export function ProfileView({ series, snapshots, format, unit }: ViewProps & { snapshots: DynSnapshot[] }) {
  const [step, setStep] = useState(1)
  const prof = useMemo(() => profileSeries(series, snapshots, step), [series, snapshots, step])
  if (series.length < 2) return <div className="dyn-chart-empty">Профиль нужен для нескольких дней вылета — выберите диапазон дат.</div>
  return (
    <>
      <div className="dyn-cal-legend">
        Линия на каждый день наблюдения: старые — тусклые, свежие — светлее, <i className="sw" style={{ background: PALETTE[0] }} /> последний.
        <span className="segbtns dyn-heat-mode">
          {[1, 3, 7].map((n) => (
            <button key={n} className={step === n ? 'active' : ''} onClick={() => setStep(n)}>
              {n === 1 ? 'каждый день' : `раз в ${n} дня`.replace('раз в 7 дня', 'раз в неделю')}
            </button>
          ))}
        </span>
      </div>
      <PriceChart series={unit === 'pct' ? toPct(prof) : prof} format={format === 'band' ? 'line' : format} unit={unit} xKind="day" tipNewestFirst alwaysDots />
      <div className="dyn-note">
        Ось X — день вылета. Каждая линия — цены, увиденные в этот день наблюдения: точка есть только у дней вылета,
        которые в этот день действительно смотрели (коллектор обновляет каждый день вылета раз в 1–3 дня), линия
        соединяет только эти точки. Видно, какие дни подорожали и как сдвигался весь «профиль» цен. {prof.length}{' '}
        {plural(prof.length, 'линия', 'линии', 'линий')}.
      </div>
    </>
  )
}

// ---------------------------------------------------------------- один день

export function SingleView({ series, format, unit, day, setDay }: ViewProps & { day: string | null; setDay: (d: string) => void }) {
  const cur = series.find((s) => s.id === day) ?? series[0]
  if (!cur) return null
  const sm = summary(cur)
  return (
    <div>
      {series.length > 1 && (
        <div className="dyn-chips dyn-day-chips">
          {series.map((s) => (
            <button key={s.id} className={`dyn-chip ${s.id === cur.id ? 'active' : ''}`} onClick={() => setDay(s.id)}>
              {s.label}
            </button>
          ))}
        </div>
      )}
      <div className="dyn-single-h">
        Вылет <b>{cur.label}</b> · сейчас <b>{sm.last !== null ? money(sm.last) : '—'}</b> <Delta diff={sm.diff} first={sm.first} />
        {cur.points.length > 0 && <> · {cur.points.length} снимков, последний {snapLabel(cur.points[cur.points.length - 1].t)}</>}
      </div>
      <PriceChart series={scaled([{ ...cur, color: PALETTE[0] }], unit)} format={format} unit={unit} height={360} />
    </div>
  )
}

// ---------------------------------------------------------------- таблица

// Таблица «день наблюдения × день вылета»: на конец каждого дня — последняя известная
// цена (бледная — в этот день снимка не было, перенесена) и изменение к прошлой строке.
// Сверху вниз — от старых наблюдений к новым, как на графике слева направо.
export function TableView({ series, snapshots, flightLabel }: { series: Series[]; snapshots: DynSnapshot[]; flightLabel?: (fi: number) => string }) {
  const { cols, rows } = useMemo(() => heatMatrix(series, snapshots), [series, snapshots])
  const [newestFirst, setNewestFirst] = useState(false)
  if (!cols.length) return <div className="dyn-chart-empty">Нет данных под фильтры.</div>
  // строки — только дни, когда хоть одна линия что-то видела
  const idx = cols.map((_, ci) => ci).filter((ci) => rows.some((r) => r[ci].seen))
  const order = newestFirst ? [...idx].reverse() : idx
  const prevOf = (ci: number) => {
    const k = idx.indexOf(ci)
    return k > 0 ? idx[k - 1] : null
  }
  const one = series.length === 1
  return (
    <div className="dyn-table-view">
      <div className="dyn-cal-legend">
        Строка — день, когда смотрели цену; в клетке — последняя цена на конец этого дня и изменение к предыдущей строке.
        Бледная — в этот день снимка дня вылета не было, цена с прошлого снимка.
        <span className="segbtns dyn-heat-mode">
          <button className={!newestFirst ? 'active' : ''} onClick={() => setNewestFirst(false)}>сначала старые</button>
          <button className={newestFirst ? 'active' : ''} onClick={() => setNewestFirst(true)}>сначала новые</button>
        </span>
      </div>
      <div className="dyn-heat-wrap">
        <table className="dyn-ptable">
          <thead>
            <tr>
              <th className="corner">смотрели</th>
              {series.map((s) => (
                <th key={s.id}>{one ? 'цена' : s.label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {order.map((ci) => {
              const prev = prevOf(ci)
              return (
                <tr key={ci}>
                  <th className="rowh">{dayLabel(cols[ci])}</th>
                  {rows.map((r, si) => {
                    const c = r[ci]
                    const pv = prev === null ? null : r[prev].v
                    const d = c.v !== null && pv !== null && pv !== undefined ? c.v - pv : null
                    return (
                      <td key={si} className={`${c.seen ? '' : 'carry'}`}>
                        {c.v === null ? (
                          <span className="dyn-muted">—</span>
                        ) : (
                          <>
                            <b>{money(c.v)}</b>
                            {d !== null && d !== 0 && (
                              <span className={`dyn-delta ${d > 0 ? 'up' : 'down'}`}>
                                {d > 0 ? '▲' : '▼'} {money(Math.abs(d))}
                              </span>
                            )}
                            {one && c.fi !== undefined && flightLabel && <div className="dyn-cell-sub">{flightLabel(c.fi)}</div>}
                          </>
                        )}
                      </td>
                    )
                  })}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
