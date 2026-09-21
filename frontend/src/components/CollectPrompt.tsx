import type { Estimate } from '../data/searchClient'

function fmtDuration(sec: number): string {
  if (sec < 60) return `~${sec} с`
  return `~${Math.round(sec / 60)} мин`
}

// Явное подтверждение перед сбором новых данных: сколько запросов и времени.
// Пока пользователь не нажмёт — ничего не загружается.
export function CollectPrompt({
  origin,
  destination,
  estimate,
  starting,
  error,
  onCollect,
}: {
  origin: string
  destination: string
  estimate: Estimate
  starting: boolean
  error?: string | null
  onCollect: () => void
}) {
  return (
    <div className="collect-prompt">
      <div className="cp-badge">Нужны новые данные</div>
      <div className="cp-title">
        {origin} → {destination} ещё не собран
      </div>
      <div className="cp-est">
        Чтобы показать варианты, нужно обратиться к Aviasales:{' '}
        <b>~{estimate.requests} запросов</b> · <b>{fmtDuration(estimate.seconds)}</b>
      </div>
      <div className="cp-actions">
        <button className="btn-primary" onClick={onCollect} disabled={starting}>
          {starting ? 'Запускаем…' : `Собрать данные (~${estimate.requests} запросов)`}
        </button>
      </div>
      {error && <div className="cp-error">{error}</div>}
      <div className="cp-note">
        Ничего не загрузится, пока вы не нажмёте. Если промахнулись с маршрутом или датами —{' '}
        просто вернитесь и измените поиск.
      </div>
    </div>
  )
}
