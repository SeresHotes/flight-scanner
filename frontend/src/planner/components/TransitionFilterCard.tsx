import { durFmt } from '../../lib/format'
import type { BaggageMode, LegQuery } from '../query'
import { DAY_MIN, TRAVEL_MAX_MIN } from '../query'
import { RangeSlider } from './RangeSlider'

const TRANSFER_OPTS: { v: number; l: string }[] = [
  { v: -1, l: 'Пересадки: любые' },
  { v: 2, l: 'До 2 пересадок' },
  { v: 1, l: 'До 1 пересадки' },
  { v: 0, l: 'Только прямые' },
]

const BAGGAGE_OPTS: { v: BaggageMode; l: string }[] = [
  { v: 'any', l: 'Багаж: не важно' },
  { v: 'included', l: 'С багажом' },
  { v: 'none', l: 'Без багажа (дешевле)' },
]

const LAYOVER_OPTS = [0, 60, 90, 120, 180, 240]

const clock = (m: number) => `${String(Math.floor(m / 60)).padStart(2, '0')}:${String(m % 60).padStart(2, '0')}`
const dayLabel = ([lo, hi]: [number, number]) => (lo <= 0 && hi >= DAY_MIN ? 'любое' : `${clock(lo)} – ${clock(Math.min(hi, DAY_MIN - 1))}`)

// Окно времени суток (местное): слайдер 00:00–24:00 с шагом 30 мин.
function DayWindow({ label, value, onChange }: { label: string; value: [number, number]; onChange: (v: [number, number]) => void }) {
  return (
    <div>
      <div className="pl-flabel">
        {label}: <span className="rangeval">{dayLabel(value)}</span>
      </div>
      <RangeSlider min={0} max={DAY_MIN} step={30} value={value} onChange={([lo, hi]) => onChange([lo, hi])} />
    </div>
  )
}

// Фильтры перехода между городами: пересадки, минимальное ожидание, багаж,
// hidden-city, длительность перелёта и время суток вылета/прилёта (местное).
export function TransitionFilterCard({
  title,
  filter,
  onChange,
}: {
  title: string
  filter: LegQuery
  onChange: (patch: Partial<LegQuery>) => void
}) {
  const hi = filter.travelMin[1] ?? TRAVEL_MAX_MIN
  return (
    <div className="pl-frow pl-transition">
      <div className="pl-ficon">✈</div>
      <div className="pl-fname">{title}</div>

      <div className="pl-fcell pl-fselects">
        <select aria-label="Пересадки" value={filter.maxTransfers} onChange={(e) => onChange({ maxTransfers: Number(e.target.value) })}>
          {TRANSFER_OPTS.map((o) => (
            <option key={o.v} value={o.v}>
              {o.l}
            </option>
          ))}
        </select>
        <select aria-label="Багаж" value={filter.baggage} onChange={(e) => onChange({ baggage: e.target.value as BaggageMode })}>
          {BAGGAGE_OPTS.map((o) => (
            <option key={o.v} value={o.v}>
              {o.l}
            </option>
          ))}
        </select>
        <select
          aria-label="Минимальная пересадка"
          value={filter.minLayoverMin}
          disabled={filter.maxTransfers === 0}
          onChange={(e) => onChange({ minLayoverMin: Number(e.target.value) })}
        >
          {LAYOVER_OPTS.map((m) => (
            <option key={m} value={m}>
              {m ? `Пересадка от ${durFmt(m)}` : 'Пересадка: любой длины'}
            </option>
          ))}
        </select>
        <label className="pl-check" title="Билет A→B→X дешевле A→B: выходим в B. Только ручная кладь, билет в одну сторону.">
          <input type="checkbox" checked={filter.hiddenCity} onChange={(e) => onChange({ hiddenCity: e.target.checked })} />
          🎯 hidden-city
        </label>
      </div>

      <div className="pl-fcell">
        <div className="pl-flabel">
          Перелёт:{' '}
          <span className="rangeval">
            {filter.travelMin[0] ? `${durFmt(filter.travelMin[0])} – ` : 'до '}
            {filter.travelMin[1] === null ? '∞' : durFmt(filter.travelMin[1])}
          </span>
        </div>
        <RangeSlider
          min={0}
          max={TRAVEL_MAX_MIN}
          step={30}
          value={[filter.travelMin[0], hi]}
          onChange={([lo, h]) => onChange({ travelMin: [lo, h >= TRAVEL_MAX_MIN ? null : h] })}
        />
      </div>

      <div className="pl-fcell pl-fchecks">
        <DayWindow label="Вылет" value={filter.depTime} onChange={(depTime) => onChange({ depTime })} />
        <DayWindow label="Прилёт" value={filter.arrTime} onChange={(arrTime) => onChange({ arrTime })} />
      </div>
    </div>
  )
}
