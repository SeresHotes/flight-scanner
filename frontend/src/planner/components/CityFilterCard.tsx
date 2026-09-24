import { DateRangePicker } from '../../components/DateRangePicker'
import { dayW } from '../../lib/format'
import type { CityFilter } from '../types'

// Города, встретившиеся на этой остановке среди собранных цепочек.
export interface CityOption {
  code: string
  city: string
  flag?: string
}

// Фильтры одного города: список городов, мин/макс дней, обязательное окно, оба выходных.
export function CityFilterCard({
  title,
  filter,
  cityOptions,
  stayBounds,
  onChange,
}: {
  title: string
  filter: CityFilter
  cityOptions: CityOption[]
  stayBounds: [number, number] // [min, max] дней в городе — границы слайдеров из данных
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
    <div className="legpanel">
      <div className="legtitle">🏙 {title}</div>

      {cityOptions.length > 1 && (
        <div className="fsub">
          <span>Города на этой остановке</span>
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
        </div>
      )}

      <div className="fsub">
        <span>
          Дней в городе: <span className="rangeval">{filter.minStay}–{filter.maxStay} {dayW(filter.maxStay)}</span>
        </span>
        <div className="dualrange">
          <input
            type="range"
            min={stayMin}
            max={stayMax}
            value={filter.minStay}
            onChange={(e) => onChange({ minStay: Math.min(Number(e.target.value), filter.maxStay) })}
          />
          <input
            type="range"
            min={stayMin}
            max={stayMax}
            value={filter.maxStay}
            onChange={(e) => onChange({ maxStay: Math.max(Number(e.target.value), filter.minStay) })}
          />
        </div>
      </div>

      <div className="fsub">
        <label className="pl-check">
          <input
            type="checkbox"
            checked={coverOn}
            onChange={(e) => onChange({ mustCover: e.target.checked ? ['', ''] : null })}
          />
          Обязательно покрыть окно дат
        </label>
        {coverOn && (
          <div style={{ marginTop: 8 }}>
            <DateRangePicker
              label="покрыть даты"
              from={cover[0]}
              to={cover[1]}
              onChange={(from, to) => onChange({ mustCover: [from, to] })}
            />
          </div>
        )}
      </div>

      <label className="pl-check">
        <input
          type="checkbox"
          checked={filter.requireWeekend}
          onChange={(e) => onChange({ requireWeekend: e.target.checked })}
        />
        Должны быть оба выходных (сб + вс)
      </label>
    </div>
  )
}
