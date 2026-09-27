import { DateRangePicker } from '../../components/DateRangePicker'
import { dayW } from '../../lib/format'
import type { CityQuery } from '../query'
import { STAY_MAX } from '../query'
import { RangeSlider } from './RangeSlider'

// Фильтры одного города одной строкой: дни в городе, оба выходных, обязательное
// окно дат. У концов маршрута (endpoint) пребывание не учитывается.
export function CityFilterCard({
  point,
  name,
  filter,
  endpoint,
  onChange,
}: {
  point: string
  name: string
  filter: CityQuery
  endpoint: boolean
  onChange: (patch: Partial<CityQuery>) => void
}) {
  return (
    <div className="pl-frow">
      <div className="pl-point">{point}</div>
      <div className="pl-fname">{name}</div>
      {endpoint ? (
        <>
          <div className="pl-fcell pl-flabel">Конец маршрута — дни здесь не учитываются</div>
          <div className="pl-fcell" />
        </>
      ) : (
        <StayFilters filter={filter} onChange={onChange} />
      )}
    </div>
  )
}

function StayFilters({ filter, onChange }: { filter: CityQuery; onChange: (patch: Partial<CityQuery>) => void }) {
  const coverOn = filter.mustCover !== null
  const cover = filter.mustCover ?? ['', '']
  const hi = filter.maxStay ?? STAY_MAX
  return (
    <>
      <div className="pl-fcell">
        <div className="pl-flabel">
          Дней в городе:{' '}
          <span className="rangeval">
            {filter.minStay}–{filter.maxStay === null ? '∞' : `${filter.maxStay} ${dayW(filter.maxStay)}`}
          </span>
        </div>
        <RangeSlider
          min={0}
          max={STAY_MAX}
          value={[filter.minStay, hi]}
          onChange={([minStay, maxStay]) => onChange({ minStay, maxStay: maxStay >= STAY_MAX ? null : maxStay })}
        />
      </div>

      <div className="pl-fcell pl-fchecks">
        <label className="pl-check">
          <input type="checkbox" checked={filter.requireWeekend} onChange={(e) => onChange({ requireWeekend: e.target.checked })} />
          Оба выходных (сб + вс)
        </label>
        <label className="pl-check">
          <input type="checkbox" checked={coverOn} onChange={(e) => onChange({ mustCover: e.target.checked ? ['', ''] : null })} />
          Покрыть окно дат
        </label>
        {coverOn && (
          <DateRangePicker label="покрыть даты" from={cover[0]} to={cover[1]} onChange={(from, to) => onChange({ mustCover: [from, to] })} />
        )}
      </div>
    </>
  )
}
