import { dayW } from '../../lib/format'
import { TRIP_MAX } from '../query'
import { RangeSlider } from './RangeSlider'

// Общий фильтр одной строкой: длина всей поездки, дни (диапазон; верх = без потолка).
export function TripLengthFilter({
  value,
  onChange,
}: {
  value: [number, number | null]
  onChange: (value: [number, number | null]) => void
}) {
  const [lo, hi] = value
  return (
    <div className="pl-frow pl-triplen">
      <div className="pl-ficon">🧳</div>
      <div className="pl-fname">Вся поездка</div>
      <div className="pl-fcell" />
      <div className="pl-fcell">
        <div className="pl-flabel">
          Длина:{' '}
          <span className="rangeval">
            {lo}–{hi === null ? '∞' : `${hi} ${dayW(hi)}`}
          </span>
        </div>
        <RangeSlider
          min={0}
          max={TRIP_MAX}
          value={[lo, hi ?? TRIP_MAX]}
          onChange={([l, h]) => onChange([l, h >= TRIP_MAX ? null : h])}
        />
      </div>
    </div>
  )
}
