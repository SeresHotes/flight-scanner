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

  return (
    <div className="pl-estimate">
      {/* Оценку показываем только для корректного маршрута — иначе число бессмысленно
          (напр. два «любых» подряд → плечо нечем заякорить). */}
      {validation.ok && (
        <div className="pl-est-info">
          <div className="pl-est-num">
            Потребуется загрузить <b>{estimate.requests}</b> {reqWord}
            {estimate.cached !== undefined && estimate.cached > 0 && (
              <span className="pl-est-sec"> · в кэше уже {estimate.cached}</span>
            )}
            <span className="pl-est-sec">
              {' '}
              · {estimate.cold === 0 ? 'всё из кэша, секунды' : fmtSeconds(estimate.seconds)}
            </span>
          </div>
          <div className="pl-est-legs">
            {estimate.legs.map((leg, i) => (
              <span className="mchip" key={i}>
                {leg.fromLabel} → {leg.toLabel}: <b>{leg.requests}</b>
                {leg.cached ? <span className="pl-anytag"> (в кэше {leg.cached})</span> : null}
                <span className="pl-anytag"> · {leg.days} дн.</span>
              </span>
            ))}
          </div>
          <div className="pl-est-hint">
            Страница = до 400 билетов. Оценка — по потолку страниц на серию; серии, загруженные
            за последние сутки, берутся из кэша. Бюджет поездки сужает загрузку «любых» городов.
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
