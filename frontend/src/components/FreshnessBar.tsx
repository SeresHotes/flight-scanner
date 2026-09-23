import type { Estimate } from '../data/searchClient'
import { fmtDate } from '../lib/format'

// Панель над результатами: когда собраны данные + кнопка пересобрать свежие.
// Показывается, когда данные уже есть (status 'ok').
export function FreshnessBar({
  collectedAt,
  estimate,
  starting,
  error,
  onRefresh,
}: {
  collectedAt?: string | null
  estimate?: Estimate | null
  starting: boolean
  error?: string | null
  onRefresh: () => void
}) {
  return (
    <div className="freshbar">
      <div className="fb-info">
        {collectedAt ? (
          <>🗓 Данные собраны <b>{fmtDate(collectedAt)}</b></>
        ) : (
          <>Данные из кэша</>
        )}
      </div>
      <div className="fb-actions">
        <button
          className="btn-ghost"
          onClick={onRefresh}
          disabled={starting || !estimate}
          title={estimate ? '' : 'Сузьте диапазон дат в форме, чтобы пересобрать'}
        >
          {starting
            ? 'Запускаем…'
            : estimate
              ? `🔄 Собрать свежие данные (~${estimate.requests} запросов)`
              : '🔄 Свежие данные (сузьте даты)'}
        </button>
      </div>
      {error && <div className="fb-error">{error}</div>}
    </div>
  )
}
