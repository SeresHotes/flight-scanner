// Слайдер на одной дорожке: один ползунок (value — число) или диапазон из двух
// (value — [lo, hi]). Два <input type=range> наложены друг на друга; клики
// ловят только ползунки, а заливка между ними рисуется отдельным слоем.
export function RangeSlider<V extends number | [number, number]>({
  min,
  max,
  step = 1,
  value,
  onChange,
}: {
  min: number
  max: number
  step?: number
  value: V
  onChange: (value: V) => void
}) {
  const range = Array.isArray(value)
  const [lo, hi] = range ? (value as [number, number]) : [min, value as number]
  const span = Math.max(1, max - min)
  const pct = (v: number) => ((Math.min(Math.max(v, min), max) - min) / span) * 100

  const setLo = (v: number) => onChange([Math.min(v, hi), hi] as V)
  const setHi = (v: number) => onChange((range ? [lo, Math.max(v, lo)] : v) as V)
  // Ползунки в одной точке у правого края: сверху нижний, иначе его не сдвинуть.
  const loOnTop = lo > min + span / 2

  return (
    <div className="pl-range">
      <div className="pl-range-track" />
      <div className="pl-range-fill" style={{ left: `${pct(lo)}%`, right: `${100 - pct(hi)}%` }} />
      {range && (
        <input
          type="range"
          min={min}
          max={max}
          step={step}
          value={lo}
          style={{ zIndex: loOnTop ? 3 : 2 }}
          onChange={(e) => setLo(Number(e.target.value))}
        />
      )}
      <input type="range" min={min} max={max} step={step} value={hi} onChange={(e) => setHi(Number(e.target.value))} />
    </div>
  )
}
