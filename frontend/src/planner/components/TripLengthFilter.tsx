import { dayW } from '../../lib/format'

// Общий фильтр: длина всей поездки, дни (диапазон).
export function TripLengthFilter({
  value,
  bounds,
  onChange,
}: {
  value: [number, number]
  bounds: [number, number]
  onChange: (value: [number, number]) => void
}) {
  const [lo, hi] = value
  const [minB, maxB] = bounds
  return (
    <div className="filterbar pl-triplen">
      <div className="fgroup">
        <div className="flabel">🧳 Длина всей поездки</div>
        <div className="fsub">
          <span>
            <span className="rangeval">{lo}–{hi} {dayW(hi)}</span>
          </span>
          <div className="dualrange">
            <input
              type="range"
              min={minB}
              max={maxB}
              value={lo}
              onChange={(e) => onChange([Math.min(Number(e.target.value), hi), hi])}
            />
            <input
              type="range"
              min={minB}
              max={maxB}
              value={hi}
              onChange={(e) => onChange([lo, Math.max(Number(e.target.value), lo)])}
            />
          </div>
        </div>
      </div>
    </div>
  )
}
