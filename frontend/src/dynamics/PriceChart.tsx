import { useEffect, useMemo, useRef, useState } from 'react'
import { money, snapLabel, type Series } from './data'

// Линейный график «цена по моментам снимков»: ось X — когда смотрели цену, ось Y — цена.
// Разрыв линии — снимок видел день, но подходящих билетов не было. Наведение — вертикаль
// на ближайший снимок и подсказка со значениями всех линий.

const H = 320
const PAD = { l: 64, r: 16, t: 14, b: 30 }

function niceStep(span: number, count: number): number {
  const raw = span / Math.max(1, count)
  const pow = Math.pow(10, Math.floor(Math.log10(raw)))
  const m = raw / pow
  return (m <= 1 ? 1 : m <= 2 ? 2 : m <= 2.5 ? 2.5 : m <= 5 ? 5 : 10) * pow
}

export function PriceChart({ series, emptyText }: { series: Series[]; emptyText: string }) {
  const boxRef = useRef<HTMLDivElement>(null)
  const [w, setW] = useState(800)
  const [hover, setHover] = useState<number | null>(null)

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
    const vals = series.flatMap((s) => s.points.map((p) => p.v)).filter((v): v is number => v !== null)
    if (!ts.length || !vals.length) return null
    let [t0, t1] = [ts[0], ts[ts.length - 1]]
    if (t1 - t0 < 3600e3) {
      t0 -= 12 * 3600e3
      t1 += 12 * 3600e3
    }
    let [v0, v1] = [Math.min(...vals), Math.max(...vals)]
    const padV = Math.max((v1 - v0) * 0.08, v1 * 0.03, 100)
    v0 = Math.max(0, v0 - padV)
    v1 = v1 + padV
    const step = niceStep(v1 - v0, 5)
    v0 = Math.floor(v0 / step) * step
    v1 = Math.ceil(v1 / step) * step
    const yTicks: number[] = []
    for (let v = v0; v <= v1 + step / 2; v += step) yTicks.push(v)
    const iw = Math.max(50, w - PAD.l - PAD.r)
    const ih = H - PAD.t - PAD.b
    const x = (t: number) => PAD.l + ((t - t0) / (t1 - t0)) * iw
    const y = (v: number) => PAD.t + (1 - (v - v0) / (v1 - v0)) * ih
    // подписи оси X — по дням, не чаще чем раз в ~90px
    const dayMs = 86400e3
    const days = Math.max(1, (t1 - t0) / dayMs)
    const every = Math.max(1, Math.ceil(days / Math.max(1, iw / 90)))
    const xTicks: number[] = []
    const start = new Date(t0)
    start.setHours(0, 0, 0, 0)
    for (let t = start.getTime() + dayMs; t <= t1; t += dayMs * every) xTicks.push(t)
    return { ts, x, y, yTicks, xTicks, iw, ih }
  }, [series, w])

  if (!geo) {
    return (
      <div className="dyn-chart" ref={boxRef}>
        <div className="dyn-chart-empty">{emptyText}</div>
      </div>
    )
  }
  const { ts, x, y, yTicks, xTicks } = geo
  const hoverT = hover !== null ? ts[hover] : null

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
    .sort((a, b) => (a.p!.v ?? Infinity) - (b.p!.v ?? Infinity))
  const tipLeft = hoverT !== null ? x(hoverT) : 0

  return (
    <div className="dyn-chart" ref={boxRef}>
      {series.length > 1 && (
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
        <svg width={w} height={H} role="img" aria-label="График цены по дням наблюдения">
          {yTicks.map((v) => (
            <g key={v}>
              <line x1={PAD.l} x2={w - PAD.r} y1={y(v)} y2={y(v)} className="dyn-grid" />
              <text x={PAD.l - 8} y={y(v) + 4} textAnchor="end" className="dyn-axis">
                {Math.round(v).toLocaleString('ru-RU')}
              </text>
            </g>
          ))}
          {xTicks.map((t) => (
            <text key={t} x={x(t)} y={H - 8} textAnchor="middle" className="dyn-axis">
              {snapLabel(t, false)}
            </text>
          ))}
          {hoverT !== null && <line x1={x(hoverT)} x2={x(hoverT)} y1={PAD.t} y2={H - PAD.b} className="dyn-cross" />}
          {series.map((s) => {
            // линия рвётся на снимках без подходящих билетов
            const parts: string[] = []
            let cur = ''
            for (const p of s.points) {
              if (p.v === null) {
                if (cur) parts.push(cur)
                cur = ''
                continue
              }
              cur += `${cur ? 'L' : 'M'}${x(p.t).toFixed(1)},${y(p.v).toFixed(1)}`
            }
            if (cur) parts.push(cur)
            return (
              <g key={s.id}>
                {parts.map((d, i) => (
                  <path key={i} d={d} fill="none" stroke={s.color} strokeWidth={2} strokeLinejoin="round" />
                ))}
                {s.points.map((p) =>
                  p.v === null ? null : (
                    <circle
                      key={p.si}
                      cx={x(p.t)}
                      cy={y(p.v)}
                      r={p.t === hoverT ? 5 : 3.5}
                      fill={s.color}
                      className="dyn-dot"
                    />
                  ),
                )}
              </g>
            )
          })}
          <rect
            x={PAD.l}
            y={PAD.t}
            width={Math.max(0, w - PAD.l - PAD.r)}
            height={H - PAD.t - PAD.b}
            fill="transparent"
            onMouseMove={onMove}
            onMouseLeave={() => setHover(null)}
          />
        </svg>
        {hoverT !== null && rows.length > 0 && (
          <div className={`dyn-tip ${tipLeft > w * 0.6 ? 'left' : ''}`} style={{ left: tipLeft }}>
            <div className="dyn-tip-h">Смотрели {snapLabel(hoverT)}</div>
            {rows.map(({ s, p }) => (
              <div key={s.id} className="dyn-tip-row">
                <i style={{ background: s.color }} />
                <span className="dyn-tip-l">{s.label}</span>
                <b>{p!.v === null ? 'нет билетов' : money(p!.v)}</b>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
