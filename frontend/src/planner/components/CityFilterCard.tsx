import { DateRangePicker } from '../../components/DateRangePicker'
import { dayW } from '../../lib/format'
import type { CityFilter } from '../types'
import { RangeSlider } from './RangeSlider'

// Города, встретившиеся на этой остановке среди собранных цепочек.
export interface CityOption {
  code: string
  city: string
  flag?: string
}

// Фильтры одного города одной строкой: список городов, дни в городе,
// оба выходных и обязательное окно дат.
export function CityFilterCard({
  point,
  name,
  filter,
  cityOptions,
  stayBounds,
  onChange,
}: {
  point: string
  name: string
  filter: CityFilter
  cityOptions: CityOption[]
  stayBounds: [number, number] // [min, max] дней в городе — границы слайдера из данных
  onChange: (patch: Partial<CityFilter>) => void
}) {
  const [stayMin, stayMax] = stayBounds
  const coverOn = filter.mustCover !== null
  const cover = filter.mustCover ?? ['', '']

  // Город включён, если фильтр не задан (null == любой) либо код в списке.
  const isOn = (code: string) => filter.allowedCodes === null || filter.allowedCodes.includes(code)

  const toggleCity = (code: string) => {
    const allCodes = cityOptions.map((c) => c.code)
    const current = filter.allowedCodes ?? allCodes
    const next = current.includes(code) ? current.filter((c) => c !== code) : [...current, code]
    // Все выбраны обратно → снова «любой» (null), иначе — явный список.
    onChange({ allowedCodes: next.length === allCodes.length ? null : next })
  }

  return (
    <div className="pl-frow">
      <div className="pl-point">{point}</div>
      <div className="pl-fname">{name}</div>

      <div className="pl-fcell">
        {cityOptions.length > 1 && (
          <div className="citychips">
            {cityOptions.map((c) => (
              <button
                key={c.code}
                type="button"
                className={isOn(c.code) ? 'active' : ''}
                onClick={() => toggleCity(c.code)}
              >
                {c.flag ? `${c.flag} ` : ''}{c.city}
              </button>
            ))}
          </div>
        )}
      </div>

      <div className="pl-fcell">
        <div className="pl-flabel">
          Дней в городе: <span className="rangeval">{filter.minStay}–{filter.maxStay} {dayW(filter.maxStay)}</span>
        </div>
        <RangeSlider
          min={stayMin}
          max={stayMax}
          value={[filter.minStay, filter.maxStay]}
          onChange={([minStay, maxStay]) => onChange({ minStay, maxStay })}
        />
      </div>

      <div className="pl-fcell pl-fchecks">
        <label className="pl-check">
          <input
            type="checkbox"
            checked={filter.requireWeekend}
            onChange={(e) => onChange({ requireWeekend: e.target.checked })}
          />
          Оба выходных (сб + вс)
        </label>
        <label className="pl-check">
          <input
            type="checkbox"
            checked={coverOn}
            onChange={(e) => onChange({ mustCover: e.target.checked ? ['', ''] : null })}
          />
          Покрыть окно дат
        </label>
        {coverOn && (
          <DateRangePicker
            label="покрыть даты"
            from={cover[0]}
            to={cover[1]}
            onChange={(from, to) => onChange({ mustCover: [from, to] })}
          />
        )}
      </div>
    </div>
  )
}
