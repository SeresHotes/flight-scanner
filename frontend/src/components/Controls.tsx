import type { Meta } from '../types'
import type { FilterState, SortKey } from '../lib/filters'

const SORT_OPTS: { v: SortKey; l: string }[] = [
  { v: 'price', l: 'Сначала дешёвые' },
  { v: 'stay_desc', l: 'Дольше на месте' },
  { v: 'stopdays_desc', l: 'Дольше остановка' },
  { v: 'transfers', l: 'Меньше пересадок' },
  { v: 'travel', l: 'Быстрее в пути' },
]

// Порт .controls из index.html: цена, дни на месте, сортировка + точные фильтры дат.
export function Controls({
  state,
  meta,
  stayCap,
  priceBounds,
  durBounds,
  money,
  onPatch,
}: {
  state: FilterState
  meta: Meta
  stayCap: number
  priceBounds: [number, number]
  durBounds: [number, number]
  money: (n: number) => string
  onPatch: (patch: Partial<FilterState>) => void
}) {
  const stayLocked = state.dir !== 'round'
  const stayVal =
    state.minStay <= meta.min_stay && state.maxStay >= stayCap
      ? `${meta.min_stay}+ дн.`
      : `${state.minStay}–${state.maxStay} дн.`

  const destName = meta.region_label || meta.destination || meta.destination_code
  const durVal =
    state.durMin <= durBounds[0] && state.durMax >= durBounds[1]
      ? 'любая'
      : `${state.durMin}–${state.durMax} дн.`
  const windowsActive = !!(state.beFrom || state.beTo || state.stopFrom || state.stopTo)

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
        <label>
          Длительность поездки: <span className="rangeval">{durVal}</span>
        </label>
        <div className="dualrange">
          <input
            type="range"
            min={durBounds[0]}
            max={durBounds[1]}
            step={1}
            value={state.durMin}
            onChange={(e) => {
              const v = Number(e.target.value)
              onPatch({ durMin: v, durMax: Math.max(v, state.durMax) })
            }}
          />
          <input
            type="range"
            min={durBounds[0]}
            max={durBounds[1]}
            step={1}
            value={state.durMax}
            onChange={(e) => {
              const v = Number(e.target.value)
              onPatch({ durMax: v, durMin: Math.min(v, state.durMin) })
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

      <div className="ctl ctl-windows">
        <label>
          Быть в точке назначения ({destName}){' '}
          {windowsActive && (
            <button
              type="button"
              className="ctl-clear"
              onClick={() => onPatch({ beFrom: '', beTo: '', stopFrom: '', stopTo: '' })}
            >
              сбросить
            </button>
          )}
        </label>
        <div className="daterow">
          <input
            type="date"
            value={state.beFrom}
            onChange={(e) => onPatch({ beFrom: e.target.value })}
          />
          <span>–</span>
          <input
            type="date"
            value={state.beTo}
            onChange={(e) => onPatch({ beTo: e.target.value })}
          />
        </div>
        <div className="ctl-sub">Оставить только поездки, которые целиком покрывают эти даты в {destName}.</div>
        <label style={{ marginTop: 10 }}>Быть в городе-остановке</label>
        <div className="daterow">
          <input
            type="date"
            value={state.stopFrom}
            onChange={(e) => onPatch({ stopFrom: e.target.value })}
          />
          <span>–</span>
          <input
            type="date"
            value={state.stopTo}
            onChange={(e) => onPatch({ stopTo: e.target.value })}
          />
        </div>
        <div className="ctl-sub">Если задано — покажем только варианты с остановкой, покрывающей эти даты.</div>
      </div>
    </div>
  )
}
