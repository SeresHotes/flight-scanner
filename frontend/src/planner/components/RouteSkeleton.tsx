import { AirportCombobox } from '../../components/AirportCombobox'
import { DateRangePicker } from '../../components/DateRangePicker'
import type { AirportOption } from '../../data/airports'
import type { PlannerStop } from '../types'
import { pointLabel } from '../validation'

const BLANK: AirportOption = { code: '', city: '', label: '' }

// Зона 1: столбец остановок. На каждой — набор городов (или «любой») и, для
// промежуточных, диапазон дат «когда ОК быть здесь». У концов дат нет — они
// выводятся из соседних городов.
export function RouteSkeleton({
  stops,
  valid,
  onUpdate,
  onAdd,
  onRemove,
}: {
  stops: PlannerStop[]
  valid: boolean[]
  onUpdate: (index: number, patch: Partial<PlannerStop>) => void
  onAdd: () => void
  onRemove: (index: number) => void
}) {
  function addAirport(i: number, opt: AirportOption) {
    if (!opt.code) return
    const cur = stops[i].airports
    if (cur.some((a) => a.code === opt.code)) return // без дублей
    onUpdate(i, { airports: [...cur, opt] })
  }
  function removeAirport(i: number, code: string) {
    onUpdate(i, { airports: stops[i].airports.filter((a) => a.code !== code) })
  }

  return (
    <div className="pl-skeleton">
      <div className="pl-sk-head">
        <div className="sftitle">🧭 Маршрут: города и когда где быть</div>
        <div className="sfhint">
          В каждом пункте можно выбрать несколько городов на выбор, либо «любой» (в середине).
          Диапазон дат — <b>в какие даты вам ОК быть в этом городе</b>. У первого и последнего
          города дат нет — они выводятся из соседних.
        </div>
      </div>

      <div className="pl-stops">
        {stops.map((s, i) => {
          const endpoint = i === 0 || i === stops.length - 1
          return (
            <div key={s.id} className={`pl-stoprow ${valid[i] ? '' : 'invalid'}`}>
              <div className="pl-point">{pointLabel(i)}</div>

              <div className="pl-kind">
                <div className="segbtns">
                  <button
                    type="button"
                    className={s.kind === 'cities' ? 'active' : ''}
                    onClick={() => onUpdate(i, { kind: 'cities' })}
                  >
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
                            <button
                              type="button"
                              className="pl-chip-x"
                              title="Убрать город"
                              onClick={() => removeAirport(i, a.code)}
                            >
                              ✕
                            </button>
                          </span>
                        ))}
                      </div>
                    )}
                    {/* key меняется после добавления → комбобокс очищается */}
                    <AirportCombobox
                      key={`add-${s.id}-${s.airports.length}`}
                      label=""
                      value={BLANK}
                      onChange={(opt) => addAirport(i, opt)}
                    />
                  </div>
                ) : (
                  <div className="pl-any">🌍 Любой город (подберём при сборе)</div>
                )}
              </div>

              <div className="pl-window">
                {endpoint ? (
                  <div className="pl-window-derived">даты — из соседних городов</div>
                ) : (
                  <DateRangePicker
                    label="ОК быть здесь"
                    from={s.window[0]}
                    to={s.window[1]}
                    onChange={(from, to) => onUpdate(i, { window: [from, to] })}
                  />
                )}
              </div>

              <button
                type="button"
                className="pl-remove"
                disabled={stops.length <= 2}
                title="Убрать остановку"
                onClick={() => onRemove(i)}
              >
                ✕
              </button>
            </div>
          )
        })}
      </div>

      <button type="button" className="pl-add" onClick={onAdd}>
        ＋ добавить остановку
      </button>
    </div>
  )
}
