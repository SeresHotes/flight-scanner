import { dayW } from '../../lib/format'
import { RangeSlider } from './RangeSlider'

// Общий фильтр одной строкой: длина всей поездки, дни (диапазон).
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
    <div className="pl-frow pl-triplen">
      <div className="pl-ficon">🧳</div>
      <div className="pl-fname">Вся поездка</div>
      <div className="pl-fcell" />
      <div className="pl-fcell">
        <div className="pl-flabel">
          Длина: <span className="rangeval">{lo}–{hi} {dayW(hi)}</span>
        </div>
        <RangeSlider min={minB} max={maxB} value={value} onChange={onChange} />
      </div>
    </div>
  )
}
