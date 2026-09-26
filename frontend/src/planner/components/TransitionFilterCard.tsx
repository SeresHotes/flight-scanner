import { durFmt } from '../../lib/format'
import type { TransitionFilter } from '../types'
import { RangeSlider } from './RangeSlider'

const TRANSFER_OPTS: { v: number; l: string }[] = [
  { v: -1, l: 'Любые' },
  { v: 2, l: 'до 2' },
  { v: 1, l: 'до 1' },
  { v: 0, l: 'прямой' },
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
        <div className="segbtns">
          {TRANSFER_OPTS.map((o) => (
            <button
              key={o.v}
              type="button"
              className={filter.maxTransfers === o.v ? 'active' : ''}
              onClick={() => onChange({ maxTransfers: o.v })}
            >
              {o.l}
            </button>
          ))}
        </div>
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
