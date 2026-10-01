import { plural } from '../../lib/format'
import type { PlannerEstimate } from '../types'
import type { Validation } from '../validation'

function fmtSeconds(s: number): string {
  if (s < 90) return `~${s} сек`
  return `~${Math.round(s / 60)} мин`
}

// Зона 2: оценка объёма сбора рядом с кнопкой «Загрузить данные».
export function PlanEstimateBar({
  estimate,
  validation,
  disabled,
  label = 'Загрузить данные →',
  onLoad,
}: {
  estimate: PlannerEstimate
  validation: Validation
  disabled: boolean
  label?: string
  onLoad: () => void
}) {
  const reqWord = plural(estimate.requests, 'страницу', 'страницы', 'страниц')
  const fromStore = estimate.source === 'tickets'
  const cachedWord = fromStore ? 'уже в складе' : 'в кэше уже'

  return (
    <div className="pl-estimate">
      {/* Оценку показываем только для корректного маршрута — иначе число бессмысленно. */}
      {validation.ok && (
        <div className="pl-est-info">
          <div className="pl-est-num">
            {fromStore && estimate.cold === 0 ? (
              <>
                Все рейсы уже в складе билетов — <b>{estimate.requests}</b> {reqWord} загружать не нужно
              </>
            ) : (
              <>
                Потребуется загрузить <b>{estimate.cold ?? estimate.requests}</b> {plural(estimate.cold ?? estimate.requests, 'страницу', 'страницы', 'страниц')}
                {estimate.cached !== undefined && estimate.cached > 0 && (
                  <span className="pl-est-sec"> · {cachedWord} {estimate.cached}</span>
                )}
                <span className="pl-est-sec">
                  {' '}
                  · {estimate.cold === 0 ? 'всё из кэша, секунды' : fmtSeconds(estimate.seconds)}
                </span>
              </>
            )}
          </div>
          <div className="pl-est-legs">
            {estimate.legs.map((leg, i) => (
              <span className="mchip" key={i}>
                {leg.fromLabel} → {leg.toLabel}: <b>{leg.requests}</b>
                {leg.cached ? <span className="pl-anytag"> ({fromStore ? 'в складе' : 'в кэше'} {leg.cached})</span> : null}
                <span className="pl-anytag"> · {leg.days} дн.</span>
              </span>
            ))}
          </div>
          <div className="pl-est-hint">
            {fromStore
              ? 'Рейсы берутся из склада билетов (все города на 180 дней вперёд, обновляется раз в 1–3 суток). В источник идём только за городами и днями, которых в складе нет; страница = до 400 билетов.'
              : 'Страница = до 400 билетов. Оценка — по потолку страниц на серию; серии, загруженные за последние сутки, берутся из кэша.'}
          </div>
        </div>
      )}

      <button type="button" className="btn-primary" disabled={disabled} onClick={onLoad}>
        {label}
      </button>

      {!validation.ok && (
        <ul className="pl-issues">
          {validation.general.map((g, i) => (
            <li key={`g${i}`}>{g}</li>
          ))}
          {validation.issues.map((iss, i) => (
            <li key={`i${i}`}>{iss.message}</li>
          ))}
        </ul>
      )}
    </div>
  )
}
