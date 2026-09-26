import { durFmt } from '../../lib/format'
import type { TransitionFilter } from '../types'
import { RangeSlider } from './RangeSlider'

const TRANSFER_OPTS: { v: number; l: string }[] = [
  { v: -1, l: 'Пересадки: любые' },
  { v: 2, l: 'До 2 пересадок' },
  { v: 1, l: 'До 1 пересадки' },
  { v: 0, l: 'Только прямые' },
]

// Фильтры перехода между городами одной строкой: пересадки и длительность перелёта.
export function TransitionFilterCard({
  title,
  filter,
  maxBound,
  onChange,
}: {
  title: string
  filter: TransitionFilter
  maxBound: number
  onChange: (patch: Partial<TransitionFilter>) => void
}) {
  return (
    <div className="pl-frow pl-transition">
      <div className="pl-ficon">✈</div>
      <div className="pl-fname">{title}</div>

      <div className="pl-fcell">
        <select
          aria-label="Пересадки"
          value={filter.maxTransfers}
          onChange={(e) => onChange({ maxTransfers: Number(e.target.value) })}
        >
          {TRANSFER_OPTS.map((o) => (
            <option key={o.v} value={o.v}>
              {o.l}
            </option>
          ))}
        </select>
      </div>

      <div className="pl-fcell">
        <div className="pl-flabel">
          Перелёт: до <span className="rangeval">{durFmt(filter.maxTravelMinutes)}</span>
        </div>
        <RangeSlider
          min={60}
          max={maxBound}
          step={30}
          value={filter.maxTravelMinutes}
          onChange={(maxTravelMinutes) => onChange({ maxTravelMinutes })}
        />
      </div>
    </div>
  )
}
