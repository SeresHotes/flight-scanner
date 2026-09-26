import { DateRangePicker } from '../../components/DateRangePicker'
import { dayW } from '../../lib/format'
import type { CityFilter } from '../types'
import { CityPicker } from './CityPicker'
import { RangeSlider } from './RangeSlider'

// Города, встретившиеся на этой остановке среди собранных цепочек.
export interface CityOption {
  code: string
  city: string
  flag?: string
}

// Фильтры одного города одной строкой: выбор городов/стран, дни в городе,
// оба выходных и обязательное окно дат.
// У концов маршрута (endpoint) — только выбор городов: пребывание там не учитывается.
export function CityFilterCard({
  point,
  name,
  filter,
  cityOptions,
  stayBounds,
  endpoint,
  onChange,
}: {
  point: string
  name: string
  filter: CityFilter
  cityOptions: CityOption[]
  stayBounds: [number, number] // [min, max] дней в городе — границы слайдера из данных
  endpoint: boolean
  onChange: (patch: Partial<CityFilter>) => void
}) {
  return (
    <div className="pl-frow">
      <div className="pl-point">{point}</div>
      <div className="pl-fname">{name}</div>

      <div className="pl-fcell">
        {cityOptions.length > 1 && (
          <CityPicker
            options={cityOptions}
            allowed={filter.allowedCodes}
            onChange={(allowedCodes) => onChange({ allowedCodes })}
          />
        )}
      </div>

      {endpoint ? (
        <div className="pl-fcell pl-flabel">Конец маршрута — дни здесь не учитываются</div>
      ) : (
        <StayFilters filter={filter} stayBounds={stayBounds} onChange={onChange} />
      )}
    </div>
  )
}

// Пребывание в промежуточном городе: дни (слайдер) и галочки — две ячейки строки.
function StayFilters({
  filter,
  stayBounds,
  onChange,
}: {
  filter: CityFilter
  stayBounds: [number, number]
  onChange: (patch: Partial<CityFilter>) => void
}) {
  const [stayMin, stayMax] = stayBounds
  const coverOn = filter.mustCover !== null
  const cover = filter.mustCover ?? ['', '']

  return (
    <>
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
    </>
  )
}
