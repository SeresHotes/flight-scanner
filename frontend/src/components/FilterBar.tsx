import type { Meta, Trip } from '../types'
import {
  DIR_OPTS,
  STOP_OPTS,
  TR_OPTS,
  matchOpt,
  type FilterState,
  type LegFilter,
  type DirFilter,
} from '../lib/filters'
import { SegButtons } from './SegButtons'

interface Bounds {
  legTravMax: number
  maxStopDays: number
}

// Порт .filterbar из index.html: направление + два плечевых блока фильтров.
export function FilterBar({
  state,
  meta,
  there,
  back,
  bounds,
  onDir,
  onLegPatch,
}: {
  state: FilterState
  meta: Meta
  there: Trip[]
  back: Trip[]
  bounds: Bounds
  onDir: (v: DirFilter) => void
  onLegPatch: (leg: 'there' | 'back', patch: Partial<LegFilter>) => void
}) {
  // Названия плеч — из фактического маршрута, а не захардкоженные «Москва → Корея».
  const originName = meta.origin.split(' (')[0]
  const regionName =
    (meta.region_flag ? meta.region_flag + ' ' : '') + (meta.region_label || meta.destination_code)

  return (
    <div className="filterbar">
      <div className="fgroup">
        <div className="flabel">Направление (что смотрим)</div>
        <div className="segbtns big">
          {DIR_OPTS.map((o) => (
            <button
              key={o.v}
              className={state.dir === o.v ? 'active' : ''}
              onClick={() => onDir(o.v)}
            >
              {o.l}
            </button>
          ))}
        </div>
      </div>
      <div className="legpanels">
        <LegPanel
          leg="there"
          title={`🛫 Плечо ТУДА — ${originName} → ${regionName}`}
          pool={there}
          cities={meta.there_cities}
          f={state.there}
          disabled={state.dir === 'back'}
          bounds={bounds}
          onPatch={(p) => onLegPatch('there', p)}
        />
        <LegPanel
          leg="back"
          title={`🛬 Плечо ОБРАТНО — ${regionName} → ${originName}`}
          pool={back}
          cities={meta.back_cities}
          f={state.back}
          disabled={state.dir === 'there'}
          bounds={bounds}
          onPatch={(p) => onLegPatch('back', p)}
        />
      </div>
    </div>
  )
}

function LegPanel({
  title,
  pool,
  cities,
  f,
  disabled,
  bounds,
  onPatch,
}: {
  leg: 'there' | 'back'
  title: string
  pool: Trip[]
  cities: string[]
  f: LegFilter
  disabled: boolean
  bounds: Bounds
  onPatch: (patch: Partial<LegFilter>) => void
}) {
  const { legTravMax, maxStopDays } = bounds
  const travVal = f.maxTravel >= legTravMax ? 'любое' : '≤ ' + f.maxTravel + ' ч'
  const sdaysVal = f.sMin <= 0 && f.sMax >= maxStopDays ? 'любое' : `${f.sMin}–${f.sMax} дн.`

  return (
    <div className={`legpanel${disabled ? ' disabled' : ''}`}>
      <div className="legtitle">{title}</div>

      <div className="fsub">
        <span>Остановка</span>
        <SegButtons
          big
          options={STOP_OPTS}
          current={f.stop}
          countFn={(v) => pool.filter((o) => matchOpt(o, { ...f, stop: v })).length}
          onPick={(v) => onPatch({ stop: v })}
        />
      </div>

      <div className="fsub">
        <span>Город остановки</span>
        <select value={f.city} onChange={(e) => onPatch({ city: e.target.value })}>
          <option value="all">Любой</option>
          {cities.map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
        </select>
      </div>

      <div className="fsub">
        <span>Пересадки</span>
        <SegButtons
          big
          options={TR_OPTS}
          current={f.tr}
          countFn={(v) => pool.filter((o) => matchOpt(o, { ...f, tr: v })).length}
          onPick={(v) => onPatch({ tr: v })}
        />
      </div>

      <div className="fsub">
        <span>
          Время в пути: <span className="rangeval">{travVal}</span>
        </span>
        <input
          type="range"
          min={0}
          max={legTravMax}
          step={1}
          value={f.maxTravel}
          onChange={(e) => onPatch({ maxTravel: Number(e.target.value) })}
        />
      </div>

      <div className="fsub">
        <span>
          Дней в городе остановки: <span className="rangeval">{sdaysVal}</span>
        </span>
        <div className="dualrange">
          <input
            type="range"
            min={0}
            max={maxStopDays}
            step={1}
            value={f.sMin}
            onChange={(e) => {
              const v = Number(e.target.value)
              onPatch({ sMin: v, sMax: Math.max(v, f.sMax) })
            }}
          />
          <input
            type="range"
            min={0}
            max={maxStopDays}
            step={1}
            value={f.sMax}
            onChange={(e) => {
              const v = Number(e.target.value)
              onPatch({ sMax: v, sMin: Math.min(v, f.sMin) })
            }}
          />
        </div>
      </div>
    </div>
  )
}
