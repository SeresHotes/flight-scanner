import { createContext, useContext, useEffect, useId, useMemo, useRef, useState } from 'react'
import { dayLabel, money, pct, snapLabel, type Series } from './data'

// График «цена во времени». Ось X — когда смотрели цену (или день вылета — в профиле),
// ось Y — цена (или % от первой цены). Разрыв линии — снимок видел день, но подходящих
// билетов не было. Наведение — вертикаль на ближайшую точку и подсказка со значениями.
// Форматы: линия, ступеньки (цена держится до следующего снимка), область, точки,
// столбики, коридор (от минимума до медианы подходящих рейсов).
// Ось Y: «всё» — от минимума до максимума, «без выбросов» — редкие всплески за краем
// (линия уходит за край, на краю — стрелка, в подсказке настоящая цена). На большом
// графике диапазон можно задать руками: протянуть мышью по оси Y, двойной клик — сброс.

export type ChartFormat = 'line' | 'step' | 'area' | 'dots' | 'bars' | 'band'
export type ChartUnit = 'rub' | 'pct'
export type YFit = 'all' | 'robust'

// Режим оси Y на всю страницу (виды не протаскивают его через пропсы).
export const YFitContext = createContext<YFit>('all')

export const FORMATS: [ChartFormat, string][] = [
  ['line', 'Линия'],
  ['step', 'Ступеньки'],
  ['area', 'Область'],
  ['dots', 'Точки'],
  ['bars', 'Столбики'],
  ['band', 'Коридор'],
]

const PAD = { l: 64, r: 16, t: 12, b: 28 }
const PAD_MINI = { l: 46, r: 8, t: 8, b: 20 }
const TIP_ROWS = 10

function niceStep(span: number, count: number): number {
  const raw = span / Math.max(1, count)
  const pow = Math.pow(10, Math.floor(Math.log10(raw)))
  const m = raw / pow
  return (m <= 1 ? 1 : m <= 2 ? 2 : m <= 2.5 ? 2.5 : m <= 5 ? 5 : 10) * pow
}

// Границы без выбросов: всё, что дальше 3 межквартильных размахов от квартилей
// (но не ближе 15% медианы — ровная цена с парой скачков на 5% выбросом не считается).
function robustRange(vals: number[]): [number, number] {
  const v = [...vals].sort((a, b) => a - b)
  const q = (f: number) => v[Math.min(v.length - 1, Math.max(0, Math.round(f * (v.length - 1))))]
  const [q1, med, q3] = [q(0.25), q(0.5), q(0.75)]
  const reach = Math.max(3 * (q3 - q1), Math.abs(med) * 0.15, 1)
  const lo = v.find((x) => x >= q1 - reach) ?? v[0]
  const hi = [...v].reverse().find((x) => x <= q3 + reach) ?? v[v.length - 1]
  return [lo, hi]
}

// Общие границы осей для набора графиков (мини-графики на одной шкале).
export function domains(series: Series[], format: ChartFormat, fit: YFit = 'all'): { x: [number, number]; y: [number, number] } | null {
  const ts = series.flatMap((s) => s.points.map((p) => p.t))
  const vals = series.flatMap((s) => s.points.flatMap((p) => (p.v === null ? [] : format === 'band' && p.hi !== undefined ? [p.v, p.hi] : [p.v])))
  if (!ts.length || !vals.length) return null
  // медиану коридора в оценку выбросов не берём — по ней не видно цену
  const fitVals = fit === 'robust' ? series.flatMap((s) => s.points.flatMap((p) => (p.v === null ? [] : [p.v]))) : vals
  return { x: [Math.min(...ts), Math.max(...ts)], y: fit === 'robust' ? robustRange(fitVals) : [Math.min(...vals), Math.max(...vals)] }
}

export function PriceChart({
  series,
  emptyText = 'Нет данных под фильтры.',
  format = 'line',
  unit = 'rub',
  height = 320,
  mini = false,
  xKind = 'time',
  xDomain,
  yDomain,
  legend = true,
  tipNewestFirst = false,
  alwaysDots = false,
}: {
  series: Series[]
  emptyText?: string
  format?: ChartFormat
  unit?: ChartUnit
  height?: number
  mini?: boolean
  xKind?: 'time' | 'day'
  xDomain?: [number, number]
  yDomain?: [number, number]
  legend?: boolean
  // подсказка: строки в порядке линий от последней к первой (а не по цене)
  tipNewestFirst?: boolean
  // точки на всех значениях (линия соединяет только реальные наблюдения)
  alwaysDots?: boolean
}) {
  const boxRef = useRef<HTMLDivElement>(null)
  const [w, setW] = useState(mini ? 300 : 800)
  const [hover, setHover] = useState<number | null>(null)
  // ручной диапазон оси Y (протянули по оси) и протяжка в процессе — в пикселях
  const [zoom, setZoom] = useState<[number, number] | null>(null)
  const [drag, setDrag] = useState<[number, number] | null>(null)
  const fit = useContext(YFitContext)
  const clipId = `dyn-clip-${useId().replace(/[^a-zA-Z0-9]/g, '')}`
  const H = height
  const pad = mini ? PAD_MINI : PAD

  // ₽ и % — разные шкалы, ручной диапазон одной к другой не подходит
  useEffect(() => setZoom(null), [unit])

  useEffect(() => {
    const el = boxRef.current
    if (!el) return
    const ro = new ResizeObserver(() => setW(el.clientWidth))
    ro.observe(el)
    setW(el.clientWidth)
    return () => ro.disconnect()
  }, [])

  const geo = useMemo(() => {
    const ts = [...new Set(series.flatMap((s) => s.points.map((p) => p.t)))].sort((a, b) => a - b)
    const d = domains(series, format, fit)
    if (!ts.length || !d) return null
    let [t0, t1] = xDomain ?? d.x
    const slack = xKind === 'day' ? 12 * 3600e3 : 0
    if (t1 - t0 < 3600e3) {
      t0 -= 12 * 3600e3
      t1 += 12 * 3600e3
    } else {
      t0 -= slack
      t1 += slack
    }
    let [v0, v1] = zoom ?? yDomain ?? d.y
    let step: number
    if (zoom) {
      // ручной диапазон — как задали, только подписи по круглым значениям
      step = niceStep(v1 - v0, mini ? 3 : 5)
    } else {
      if (unit === 'pct') {
        v0 = Math.min(v0, 0)
        v1 = Math.max(v1, 0)
      }
      const span = Math.max(v1 - v0, unit === 'pct' ? 2 : Math.max(v1 * 0.03, 100))
      const padV = span * 0.08
      v0 = unit === 'rub' ? Math.max(0, v0 - padV) : v0 - padV
      v1 = v1 + padV
      step = niceStep(v1 - v0, mini ? 3 : 5)
      v0 = Math.floor(v0 / step) * step
      v1 = Math.ceil(v1 / step) * step
      if (format === 'bars' && unit === 'rub') v0 = 0
    }
    const yTicks: number[] = []
    for (let v = Math.ceil(v0 / step - 1e-9) * step; v <= v1 + step * 1e-6; v += step) yTicks.push(v)
    const iw = Math.max(40, w - pad.l - pad.r)
    const ih = H - pad.t - pad.b
    const x = (t: number) => pad.l + ((t - t0) / (t1 - t0)) * iw
    const y = (v: number) => pad.t + (1 - (v - v0) / (v1 - v0)) * ih
    const yInv = (py: number) => v0 + (1 - (py - pad.t) / ih) * (v1 - v0)
    // подписи оси X — по дням, не теснее ~80px (мини — ~70px)
    const dayMs = 86400e3
    const days = Math.max(1, (t1 - t0) / dayMs)
    const every = Math.max(1, Math.ceil(days / Math.max(1, iw / (mini ? 70 : 84))))
    const xTicks: number[] = []
    const start = new Date(t0)
    if (xKind === 'day') start.setUTCHours(0, 0, 0, 0)
    else start.setHours(0, 0, 0, 0)
    for (let t = start.getTime() + dayMs; t <= t1; t += dayMs * every) xTicks.push(t)
    // ширина столбика: по самому тесному шагу между точками
    let gap = iw
    for (let i = 1; i < ts.length; i++) gap = Math.min(gap, x(ts[i]) - x(ts[i - 1]))
    const nS = Math.max(1, series.length)
    const barW = Math.max(1.5, Math.min(18, (gap * 0.8) / nS))
    // точки рисуем, только если они не сливаются (иначе — только под курсором)
    const perSeries = Math.max(...series.map((s) => s.points.length))
    const sparse = series.length <= 8 && iw / Math.max(1, perSeries) >= 9
    return { ts, x, y, yInv, yTicks, xTicks, v0, v1, barW, nS, sparse }
  }, [series, w, format, unit, xDomain, yDomain, xKind, mini, H, pad, fit, zoom])

  if (!geo) {
    return (
      <div className={`dyn-chart ${mini ? 'mini' : ''}`} ref={boxRef}>
        <div className="dyn-chart-empty" style={{ height: H }}>{emptyText}</div>
      </div>
    )
  }
  const { ts, x, y, yInv, yTicks, xTicks, v0, v1, barW, nS, sparse } = geo
  const plotTop = pad.t
  const plotBot = H - pad.b
  const clampY = (py: number) => Math.min(plotBot, Math.max(plotTop, py))

  // протяжка по оси Y: mousedown на оси, дальше слушаем окно (можно уйти за график)
  function onAxisDown(e: React.MouseEvent<SVGRectElement>) {
    if (e.button !== 0) return
    e.preventDefault()
    const svgTop = (e.currentTarget.ownerSVGElement as SVGSVGElement).getBoundingClientRect().top
    const a = clampY(e.clientY - svgTop)
    setDrag([a, a])
    const move = (ev: MouseEvent) => setDrag([a, clampY(ev.clientY - svgTop)])
    const up = (ev: MouseEvent) => {
      window.removeEventListener('mousemove', move)
      window.removeEventListener('mouseup', up)
      setDrag(null)
      const b = clampY(ev.clientY - svgTop)
      if (Math.abs(b - a) < 8) return
      const lo = yInv(Math.max(a, b))
      const hi = yInv(Math.min(a, b))
      setZoom([unit === 'rub' ? Math.max(0, lo) : lo, hi])
    }
    window.addEventListener('mousemove', move)
    window.addEventListener('mouseup', up)
  }
  // точки за краем оси — стрелка на краю
  const outside = (v: number) => (v > v1 ? 'up' : v < v0 ? 'down' : null)

  const hoverT = hover !== null ? ts[hover] : null
  const fmtV = (v: number) => (unit === 'pct' ? pct(v) : money(v))
  const fmtTick = (v: number) => (unit === 'pct' ? `${Math.round(v)}%` : mini && v >= 10000 ? `${Math.round(v / 1000)}к` : Math.round(v).toLocaleString('ru-RU'))
  const fmtX = (t: number) => (xKind === 'day' ? dayLabel(new Date(t).toISOString().slice(0, 10)).split(',')[0] : snapLabel(t, false))

  function onMove(e: React.MouseEvent<SVGRectElement>) {
    const rect = (e.currentTarget.ownerSVGElement as SVGSVGElement).getBoundingClientRect()
    const mx = e.clientX - rect.left
    let best = 0
    ts.forEach((t, i) => {
      if (Math.abs(x(t) - mx) < Math.abs(x(ts[best]) - mx)) best = i
    })
    setHover(best)
  }

  const rows = hoverT === null ? [] : series
    .map((s) => ({ s, p: s.points.find((p) => p.t === hoverT) }))
    .filter((r) => r.p)
    .sort((a, b) => (tipNewestFirst ? 0 : (a.p!.v ?? Infinity) - (b.p!.v ?? Infinity)))
  if (tipNewestFirst) rows.reverse()
  const tipLeft = hoverT !== null ? x(hoverT) : 0
  const base = y(Math.max(v0, unit === 'pct' ? Math.min(0, yTicks[yTicks.length - 1]) : v0))

  // отрезки без разрывов
  const runs = (s: Series) => {
    const out: { t: number; v: number; hi?: number }[][] = []
    let cur: { t: number; v: number; hi?: number }[] = []
    for (const p of s.points) {
      if (p.v === null) {
        if (cur.length) out.push(cur)
        cur = []
      } else cur.push({ t: p.t, v: p.v, hi: p.hi })
    }
    if (cur.length) out.push(cur)
    return out
  }
  const linePath = (pts: { t: number; v: number }[], step: boolean) =>
    pts
      .map((p, i) => {
        const X = x(p.t).toFixed(1)
        const Y = y(p.v).toFixed(1)
        if (i === 0) return `M${X},${Y}`
        return step ? `H${X}V${Y}` : `L${X},${Y}`
      })
      .join('')

  return (
    <div className={`dyn-chart ${mini ? 'mini' : ''}`} ref={boxRef}>
      {legend && !mini && series.length > 1 && series.length <= 12 && (
        <div className="dyn-legend">
          {series.map((s) => (
            <span key={s.id} className="dyn-legend-item">
              <i style={{ background: s.color }} />
              {s.label}
            </span>
          ))}
        </div>
      )}
      <div className="dyn-plot">
        <svg width={w} height={H} role="img" aria-label="График цены">
          <defs>
            <clipPath id={clipId}>
              <rect x={0} y={plotTop - 6} width={w} height={plotBot - plotTop + 12} />
            </clipPath>
          </defs>
          {yTicks.map((v) => (
            <g key={v}>
              <line x1={pad.l} x2={w - pad.r} y1={y(v)} y2={y(v)} className={unit === 'pct' && v === 0 ? 'dyn-grid zero' : 'dyn-grid'} />
              <text x={pad.l - 6} y={y(v) + 4} textAnchor="end" className="dyn-axis">
                {fmtTick(v)}
              </text>
            </g>
          ))}
          {xTicks.map((t) => (
            <text key={t} x={x(t)} y={H - 7} textAnchor="middle" className="dyn-axis">
              {fmtX(t)}
            </text>
          ))}
          {hoverT !== null && <line x1={x(hoverT)} x2={x(hoverT)} y1={pad.t} y2={H - pad.b} className="dyn-cross" />}
          <g clipPath={`url(#${clipId})`}>
          {series.map((s, si) => {
            const rs = runs(s)
            const dotR = (t: number) => (t === hoverT ? 5 : format === 'dots' ? 4 : mini || (alwaysDots && series.length > 8) ? 2.5 : 3.5)
            return (
              <g key={s.id}>
                {format === 'band' &&
                  rs.map((r, i) => {
                    const withHi = r.filter((p) => p.hi !== undefined)
                    if (withHi.length < 2) return null
                    const top = withHi.map((p, j) => `${j ? 'L' : 'M'}${x(p.t).toFixed(1)},${y(p.hi!).toFixed(1)}`).join('')
                    const bottom = [...withHi].reverse().map((p) => `L${x(p.t).toFixed(1)},${y(p.v).toFixed(1)}`).join('')
                    return <path key={`b${i}`} d={`${top}${bottom}Z`} fill={s.color} opacity={series.length > 3 ? 0.1 : 0.2} />
                  })}
                {format === 'area' &&
                  rs.map((r, i) => (
                    <path
                      key={`a${i}`}
                      d={`${linePath(r, false)}L${x(r[r.length - 1].t).toFixed(1)},${base}L${x(r[0].t).toFixed(1)},${base}Z`}
                      fill={s.color}
                      opacity={series.length > 3 ? 0.08 : 0.18}
                    />
                  ))}
                {format === 'bars' &&
                  s.points.map((p) =>
                    p.v === null ? null : (
                      <rect
                        key={p.si}
                        x={x(p.t) - (nS * barW) / 2 + si * barW}
                        y={Math.min(y(p.v), base)}
                        width={Math.max(1, barW - (nS > 1 ? 1 : 0))}
                        height={Math.max(1, Math.abs(base - y(p.v)))}
                        rx={Math.min(3, barW / 3)}
                        fill={s.color}
                        opacity={hoverT === null || p.t === hoverT ? 1 : 0.75}
                      />
                    ),
                  )}
                {format !== 'dots' && format !== 'bars' &&
                  rs.map((r, i) => (
                    <path key={`l${i}`} d={linePath(r, format === 'step')} fill="none" stroke={s.color} strokeWidth={mini || series.length > 8 ? 1.5 : 2} strokeLinejoin="round" />
                  ))}
                {format !== 'bars' &&
                  s.points.map((p) =>
                    p.v === null ? null : format !== 'dots' && !alwaysDots && p.t !== hoverT && (!sparse || mini) ? null : (
                      <circle key={`${p.t}-${p.si}`} cx={x(p.t)} cy={y(p.v)} r={dotR(p.t)} fill={s.color} className="dyn-dot" />
                    ),
                  )}
                {s.points.map((p) => {
                  const side = p.v === null ? null : outside(p.v)
                  if (!side) return null
                  const cx = x(p.t)
                  const d = side === 'up' ? `M${cx - 5},${plotTop + 7}L${cx},${plotTop + 1}L${cx + 5},${plotTop + 7}Z` : `M${cx - 5},${plotBot - 7}L${cx},${plotBot - 1}L${cx + 5},${plotBot - 7}Z`
                  return <path key={`o${p.t}-${p.si}`} d={d} fill={s.color} className="dyn-out"><title>{`за краем оси: ${fmtV(p.v!)}`}</title></path>
                })}
              </g>
            )
          })}
          </g>
          <rect
            x={pad.l}
            y={pad.t}
            width={Math.max(0, w - pad.l - pad.r)}
            height={H - pad.t - pad.b}
            fill="transparent"
            onMouseMove={onMove}
            onMouseLeave={() => setHover(null)}
          />
          {!mini && (
            <rect
              x={0}
              y={plotTop}
              width={pad.l}
              height={plotBot - plotTop}
              fill="transparent"
              className="dyn-yaxis-hit"
              onMouseDown={onAxisDown}
              onDoubleClick={() => setZoom(null)}
            >
              <title>Протяните по оси, чтобы задать диапазон цен; двойной клик — сброс</title>
            </rect>
          )}
          {drag && Math.abs(drag[1] - drag[0]) >= 2 && (
            <rect x={pad.l} y={Math.min(...drag)} width={Math.max(0, w - pad.l - pad.r)} height={Math.abs(drag[1] - drag[0])} className="dyn-yzoom-sel" />
          )}
        </svg>
        {zoom && (
          <button className="dyn-yzoom-reset" onClick={() => setZoom(null)} title="Вернуть ось Y как была">
            {fmtTick(zoom[0])} – {fmtTick(zoom[1])} ✕
          </button>
        )}
        {hoverT !== null && rows.length > 0 && (
          <div className={`dyn-tip ${tipLeft > w * 0.55 ? 'left' : ''}`} style={{ left: tipLeft }}>
            <div className="dyn-tip-h">{xKind === 'day' ? `Вылет ${dayLabel(new Date(hoverT).toISOString().slice(0, 10))}` : `Смотрели ${snapLabel(hoverT)}`}</div>
            {rows.slice(0, TIP_ROWS).map(({ s, p }) => (
              <div key={s.id} className="dyn-tip-row">
                <i style={{ background: s.color }} />
                <span className="dyn-tip-l">{s.label}</span>
                <b>{p!.v === null ? 'нет билетов' : fmtV(p!.v)}</b>
                {format === 'band' && p!.hi !== undefined && p!.v !== null && <span className="dyn-tip-m">медиана {fmtV(p!.hi)}</span>}
              </div>
            ))}
            {rows.length > TIP_ROWS && <div className="dyn-tip-more">и ещё {rows.length - TIP_ROWS}</div>}
          </div>
        )}
      </div>
    </div>
  )
}

// Спарклайн для клетки календаря: линия без осей, точка на последнем значении.
export function Sparkline({ series, yDomain, height = 34 }: { series: Series; yDomain?: [number, number]; height?: number }) {
  const pts = series.points.filter((p) => p.v !== null) as { t: number; v: number }[]
  if (pts.length === 0) return <div className="dyn-spark-empty" style={{ height }} />
  const W = 100
  const ts = pts.map((p) => p.t)
  const vs = pts.map((p) => p.v)
  const [t0, t1] = [Math.min(...ts), Math.max(...ts)]
  const [v0, v1] = yDomain ?? [Math.min(...vs), Math.max(...vs)]
  const X = (t: number) => (t1 > t0 ? ((t - t0) / (t1 - t0)) * (W - 4) + 2 : W / 2)
  const Y = (v: number) => (v1 > v0 ? (1 - (v - v0) / (v1 - v0)) * (height - 6) + 3 : height / 2)
  const d = pts.map((p, i) => `${i ? 'L' : 'M'}${X(p.t).toFixed(1)},${Y(p.v).toFixed(1)}`).join('')
  const last = pts[pts.length - 1]
  return (
    <svg className="dyn-spark" viewBox={`0 0 ${W} ${height}`} preserveAspectRatio="none" height={height}>
      <path d={d} fill="none" stroke={series.color} strokeWidth={1.6} vectorEffect="non-scaling-stroke" />
      <circle cx={X(last.t)} cy={Y(last.v)} r={2.2} fill={series.color} />
    </svg>
  )
}
