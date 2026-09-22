import { AirportCombobox } from '../../components/AirportCombobox'
import type { AirportOption } from '../../data/airports'
import type { PlannerStop } from '../types'
import { pointLabel } from '../validation'

const BLANK: AirportOption = { code: '', city: '', label: '' }

// Зона 1: столбец остановок, напротив каждой — тип (город/любой) и окно дат.
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
  return (
    <div className="pl-skeleton">
      <div className="pl-sk-head">
        <div className="sftitle">🧭 Маршрут: остановки и когда где быть</div>
        <div className="sfhint">Концы — конкретные города. В середине можно поставить «любой».</div>
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
                    className={s.kind === 'city' ? 'active' : ''}
                    onClick={() => onUpdate(i, { kind: 'city', airport: s.airport ?? BLANK })}
                  >
                    Город
                  </button>
                  <button
                    type="button"
                    className={s.kind === 'any' ? 'active' : ''}
                    disabled={endpoint}
                    title={endpoint ? 'Конец маршрута — только конкретный город' : ''}
                    onClick={() => onUpdate(i, { kind: 'any' })}
                  >
                    Любой
                  </button>
                </div>
              </div>

              <div className="pl-city">
                {s.kind === 'city' ? (
                  <AirportCombobox
                    label=""
                    value={s.airport ?? BLANK}
                    onChange={(opt) => onUpdate(i, { airport: opt })}
                  />
                ) : (
                  <div className="pl-any">🌍 Любой город (подберём при сборе)</div>
                )}
              </div>

              <div className="pl-window">
                <div className="daterow">
                  <input
                    type="date"
                    value={s.window[0]}
                    onChange={(e) => onUpdate(i, { window: [e.target.value, s.window[1]] })}
                  />
                  <span>–</span>
                  <input
                    type="date"
                    value={s.window[1]}
                    onChange={(e) => onUpdate(i, { window: [s.window[0], e.target.value] })}
                  />
                </div>
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
