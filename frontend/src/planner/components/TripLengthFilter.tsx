import { dayW } from '../../lib/format'
import { TRIP_MAX } from '../query'
import { RangeSlider } from './RangeSlider'

// Общие условия одной строкой: бюджет поездки и длина (диапазон; верх = без потолка).
export function TripLengthFilter({
  value,
  maxCost,
  onChange,
  onMaxCost,
}: {
  value: [number, number | null]
  maxCost: number | null
  onChange: (value: [number, number | null]) => void
  onMaxCost: (maxCost: number | null) => void
}) {
  const [lo, hi] = value
  return (
    <div className="pl-frow pl-triplen">
      <div className="pl-ficon">🧳</div>
      <div className="pl-fname">Вся поездка</div>
      <div className="pl-fcell">
        <label
          className="pl-limit-row"
          title="Верхняя граница суммарной цены маршрута. Это фильтр: рейсы не перезагружаются, меняется только стыковка."
        >
          Бюджет, ₽:{' '}
          <input
            type="number"
            min={0}
            step={5000}
            value={maxCost ?? ''}
            placeholder="без лимита"
            onChange={(e) => onMaxCost(e.target.value === '' ? null : Math.max(0, Number(e.target.value) || 0))}
          />
        </label>
      </div>
      <div className="pl-fcell">
        <div className="pl-flabel">
          Длина:{' '}
          <span className="rangeval">
            {lo}–{hi === null ? '∞' : `${hi} ${dayW(hi)}`}
          </span>
        </div>
        <RangeSlider min={0} max={TRIP_MAX} value={[lo, hi ?? TRIP_MAX]} onChange={([l, h]) => onChange([l, h >= TRIP_MAX ? null : h])} />
      </div>
    </div>
  )
}
