import { durFmt } from '../../lib/format'
import type { TransitionFilter } from '../types'

const TRANSFER_OPTS: { v: number; l: string }[] = [
  { v: -1, l: 'Любые' },
  { v: 2, l: 'до 2' },
  { v: 1, l: 'до 1' },
  { v: 0, l: 'прямой' },
]

// Фильтры перехода между городами: пересадки и суммарная длительность перелёта.
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
    <div className="legpanel pl-transition">
      <div className="legtitle">✈ {title}</div>

      <div className="fsub">
        <span>Пересадки</span>
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

      <div className="fsub">
        <span>
          Длительность перелёта: до <span className="rangeval">{durFmt(filter.maxTravelMinutes)}</span>
        </span>
        <div className="dualrange">
          <input
            type="range"
            min={60}
            max={maxBound}
            step={30}
            value={filter.maxTravelMinutes}
            onChange={(e) => onChange({ maxTravelMinutes: Number(e.target.value) })}
          />
        </div>
      </div>
    </div>
  )
}
