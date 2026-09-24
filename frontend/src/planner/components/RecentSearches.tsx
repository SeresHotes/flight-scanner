import type { PlannerStop } from '../types'
import type { RecentSearch } from '../recentSearches'

// Краткое описание маршрута: коды городов по остановкам, «любой» → 🌍.
function routeSummary(stops: PlannerStop[]): string {
  return stops
    .map((s) => (s.kind === 'any' ? '🌍' : s.airports.map((a) => a.code).join('/') || '—'))
    .join(' → ')
}

// Окно поездки: самая ранняя дата начала и самая поздняя дата конца среди
// заданных окон (у концов маршрута окон нет).
function dateSummary(stops: PlannerStop[]): string {
  const windows = stops.map((s) => s.window).filter(([from, to]) => from && to)
  if (windows.length === 0) return ''
  const start = windows.map((w) => w[0]).sort()[0]
  const end = windows.map((w) => w[1]).sort().slice(-1)[0]
  return start === end ? start : `${start} … ${end}`
}

// Блок «Недавние запросы»: клик по строке восстанавливает маршрут в форму,
// крестик — убирает запись из истории.
export function RecentSearches({
  items,
  onRestore,
  onRemove,
}: {
  items: RecentSearch[]
  onRestore: (stops: PlannerStop[]) => void
  onRemove: (key: string) => void
}) {
  if (items.length === 0) return null

  return (
    <div className="pl-recent">
      <div className="pl-recent-title">🕘 Недавние запросы</div>
      <div className="pl-recent-list">
        {items.map((r) => {
          const dates = dateSummary(r.stops)
          return (
            <div className="pl-recent-item" key={r.key}>
              <button
                type="button"
                className="pl-recent-load"
                title="Восстановить маршрут"
                onClick={() => onRestore(r.stops)}
              >
                <span className="pl-recent-route">{routeSummary(r.stops)}</span>
                {dates && <span className="pl-recent-dates">{dates}</span>}
              </button>
              <button
                type="button"
                className="pl-recent-x"
                title="Убрать из истории"
                onClick={() => onRemove(r.key)}
              >
                ✕
              </button>
            </div>
          )
        })}
      </div>
    </div>
  )
}
