import type { Meta } from '../types'
import type { FilterState, SortKey } from '../lib/filters'

const SORT_OPTS: { v: SortKey; l: string }[] = [
  { v: 'price', l: 'Сначала дешёвые' },
  { v: 'stay_desc', l: 'Дольше на месте' },
  { v: 'stopdays_desc', l: 'Дольше остановка' },
  { v: 'transfers', l: 'Меньше пересадок' },
  { v: 'travel', l: 'Быстрее в пути' },
]

// Порт .controls из index.html: цена, дни на месте, сортировка.
export function Controls({
  state,
  meta,
  stayCap,
  priceBounds,
  money,
  onPatch,
}: {
  state: FilterState
  meta: Meta
  stayCap: number
  priceBounds: [number, number]
  money: (n: number) => string
  onPatch: (patch: Partial<FilterState>) => void
}) {
  const stayLocked = state.dir !== 'round'
  const stayVal =
    state.minStay <= meta.min_stay && state.maxStay >= stayCap
      ? `${meta.min_stay}+ дн.`
      : `${state.minStay}–${state.maxStay} дн.`

  return (
    <div className="controls">
      <div className="ctl">
        <label>
          Макс. цена: <span className="rangeval">{money(state.maxPrice)}</span>
        </label>
        <input
          type="range"
          min={priceBounds[0]}
          max={priceBounds[1]}
          step={500}
          value={state.maxPrice}
          onChange={(e) => onPatch({ maxPrice: Number(e.target.value) })}
        />
      </div>

      <div className="ctl" style={{ opacity: stayLocked ? 0.4 : 1 }}>
        <label>
          Дней на месте: <span className="rangeval">{stayVal}</span>
        </label>
        <div className="dualrange">
          <input
            type="range"
            min={meta.min_stay}
            max={stayCap}
            step={1}
            value={state.minStay}
            disabled={stayLocked}
            onChange={(e) => {
              const v = Number(e.target.value)
              onPatch({ minStay: v, maxStay: Math.max(v, state.maxStay) })
            }}
          />
          <input
            type="range"
            min={meta.min_stay}
            max={stayCap}
            step={1}
            value={state.maxStay}
            disabled={stayLocked}
            onChange={(e) => {
              const v = Number(e.target.value)
              onPatch({ maxStay: v, minStay: Math.min(v, state.minStay) })
            }}
          />
        </div>
      </div>

      <div className="ctl">
        <label>Сортировка</label>
        <select
          value={state.sort}
          onChange={(e) => onPatch({ sort: e.target.value as SortKey })}
        >
          {SORT_OPTS.map((o) => (
            <option key={o.v} value={o.v}>
              {o.l}
            </option>
          ))}
        </select>
      </div>
    </div>
  )
}
