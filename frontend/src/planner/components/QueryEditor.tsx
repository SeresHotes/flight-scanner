import { Fragment } from 'react'
import { AirportCombobox } from '../../components/AirportCombobox'
import { DateRangePicker } from '../../components/DateRangePicker'
import type { AirportOption } from '../../data/airports'
import { dayW, plural } from '../../lib/format'
import type { PlannerStop } from '../types'
import type { CityQuery, LegQuery, PlanQuery } from '../query'
import { DEFAULT_MAX_RESULTS, MAX_RESULTS, STAY_MAX, fitFilters } from '../query'
import { pointLabel, type Validation } from '../validation'
import { RangeSlider } from './RangeSlider'
import { TransitionFilterCard } from './TransitionFilterCard'
import { TripLengthFilter } from './TripLengthFilter'

const BLANK: AirportOption = { code: '', city: '', label: '' }
let stopSeq = 1000
const nid = () => `q${stopSeq++}`

// Единый редактор запроса: карточка остановки (города или «любой», окно дат и —
// у промежуточных — фильтры пребывания), между остановками фильтры плеча, в конце
// длина поездки и границы. Один список сверху вниз, без отдельной панели фильтров.
export function QueryEditor({
  query,
  validation,
  onChange,
}: {
  query: PlanQuery
  validation: Validation
  onChange: (q: PlanQuery) => void
}) {
  const { stops } = query
  const setStops = (next: PlannerStop[]) => onChange(fitFilters({ ...query, stops: next }))
  const updateStop = (i: number, patch: Partial<PlannerStop>) =>
    setStops(stops.map((s, k) => (k === i ? { ...s, ...patch } : s)))
  const addStop = () => setStops([...stops, { id: nid(), kind: 'cities', airports: [], window: ['', ''] }])
  const removeStop = (i: number) => {
    if (stops.length > 2) setStops(stops.filter((_, k) => k !== i))
  }
  const patchCity = (i: number, patch: Partial<CityQuery>) =>
    onChange({ ...query, cities: query.cities.map((c, k) => (k === i ? { ...c, ...patch } : c)) })
  const patchLeg = (i: number, patch: Partial<LegQuery>) =>
    onChange({ ...query, legs: query.legs.map((l, k) => (k === i ? { ...l, ...patch } : l)) })

  return (
    <div className="pl-editor">
      <div className="pl-sk-head">
        <div className="sftitle">🧭 Маршрут и условия</div>
        <div className="sfhint">
          В каждом пункте — несколько городов на выбор или «любой» (в середине), диапазон дат —{' '}
          <b>в какие даты вам ОК быть в этом городе</b> — и условия пребывания. Между пунктами — условия перелёта.
        </div>
      </div>

      {stops.map((s, i) => {
        const endpoint = i === 0 || i === stops.length - 1
        return (
          <Fragment key={s.id}>
            <div className={`pl-stopcard ${validation.stopValid[i] ? '' : 'invalid'}`}>
              <StopRow stop={s} index={i} endpoint={endpoint} canRemove={stops.length > 2} onUpdate={updateStop} onRemove={removeStop} />
              {!endpoint && <StayRow filter={query.cities[i]} onChange={(patch) => patchCity(i, patch)} />}
            </div>
            {i < stops.length - 1 && (
              <TransitionFilterCard
                title={`${pointLabel(i)} → ${pointLabel(i + 1)}`}
                filter={query.legs[i]}
                onChange={(patch) => patchLeg(i, patch)}
              />
            )}
          </Fragment>
        )
      })}

      <button type="button" className="pl-add" onClick={addStop}>
        ＋ добавить остановку
      </button>

      <TripLengthFilter value={query.tripLength} onChange={(tripLength) => onChange({ ...query, tripLength })} />

      <div className="pl-frow pl-bounds">
        <div className="pl-ficon">⚙</div>
        <div className="pl-fname">Границы</div>
        <div className="pl-fcell">
          <label className="pl-limit-row" title="Верхняя граница суммарной цены маршрута. Отсекает дорогие направления ещё при сборе и сужает загрузку «любых» городов.">
            Максимум цены, ₽:{' '}
            <input
              type="number"
              min={0}
              step={5000}
              value={query.maxCost ?? ''}
              placeholder="без лимита"
              onChange={(e) => onChange({ ...query, maxCost: e.target.value === '' ? null : Math.max(0, Number(e.target.value) || 0) })}
            />
          </label>
        </div>
        <div className="pl-fcell">
          <label className="pl-limit-row" title="Сколько самых дешёвых маршрутов строить. Наборы городов считаются без этого лимита.">
            Максимум маршрутов:{' '}
            <input
              type="number"
              min={1}
              max={MAX_RESULTS}
              step={100}
              value={query.maxResults}
              onChange={(e) =>
                onChange({ ...query, maxResults: Math.max(1, Math.min(MAX_RESULTS, Number(e.target.value) || DEFAULT_MAX_RESULTS)) })
              }
            />
            <span className="pl-flabel"> {plural(query.maxResults, 'маршрут', 'маршрута', 'маршрутов')}</span>
          </label>
        </div>
      </div>
    </div>
  )
}

// Строка остановки: точка, «Города / Любой», города-кандидаты, окно дат, удалить.
function StopRow({
  stop: s,
  index: i,
  endpoint,
  canRemove,
  onUpdate,
  onRemove,
}: {
  stop: PlannerStop
  index: number
  endpoint: boolean
  canRemove: boolean
  onUpdate: (index: number, patch: Partial<PlannerStop>) => void
  onRemove: (index: number) => void
}) {
  const addAirport = (opt: AirportOption) => {
    if (!opt.code || s.airports.some((a) => a.code === opt.code)) return
    onUpdate(i, { airports: [...s.airports, opt] })
  }
  const removeAirport = (code: string) => onUpdate(i, { airports: s.airports.filter((a) => a.code !== code) })

  return (
    <div className="pl-stoprow">
      <div className="pl-point">{pointLabel(i)}</div>

      <div className="pl-kind">
        <div className="segbtns">
          <button type="button" className={s.kind === 'cities' ? 'active' : ''} onClick={() => onUpdate(i, { kind: 'cities' })}>
            Города
          </button>
          <button
            type="button"
            className={s.kind === 'any' ? 'active' : ''}
            disabled={endpoint}
            title={endpoint ? 'Конец маршрута — только конкретные города' : ''}
            onClick={() => onUpdate(i, { kind: 'any' })}
          >
            Любой
          </button>
        </div>
      </div>

      <div className="pl-city">
        {s.kind === 'cities' ? (
          <div className="pl-cities">
            {s.airports.length > 0 && (
              <div className="pl-chips">
                {s.airports.map((a) => (
                  <span className="pl-chip" key={a.code}>
                    {a.flag ? `${a.flag} ` : ''}
                    {a.city || a.code} <span className="pl-chip-code">{a.code}</span>
                    <button type="button" className="pl-chip-x" title="Убрать город" onClick={() => removeAirport(a.code)}>
                      ✕
                    </button>
                  </span>
                ))}
              </div>
            )}
            {/* key меняется после добавления → комбобокс очищается */}
            <AirportCombobox key={`add-${s.id}-${s.airports.length}`} label="" value={BLANK} onChange={addAirport} />
          </div>
        ) : (
          <div className="pl-any">🌍 Любой город (подберём при сборе)</div>
        )}
      </div>

      <div className="pl-window">
        {endpoint ? (
          <div className="pl-window-derived">даты — из соседних городов</div>
        ) : (
          <DateRangePicker label="ОК быть здесь" from={s.window[0]} to={s.window[1]} onChange={(from, to) => onUpdate(i, { window: [from, to] })} />
        )}
      </div>

      <button type="button" className="pl-remove" disabled={!canRemove} title="Убрать остановку" onClick={() => onRemove(i)}>
        ✕
      </button>
    </div>
  )
}

// Условия пребывания в промежуточном городе — вторая строка карточки остановки.
function StayRow({ filter, onChange }: { filter: CityQuery; onChange: (patch: Partial<CityQuery>) => void }) {
  const coverOn = filter.mustCover !== null
  const cover = filter.mustCover ?? ['', '']
  const hi = filter.maxStay ?? STAY_MAX
  return (
    <div className="pl-stayrow">
      <div className="pl-ficon">🏙</div>
      <div className="pl-fname">Пребывание</div>
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
    </div>
  )
}
