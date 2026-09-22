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
  onLoad,
}: {
  estimate: PlannerEstimate
  validation: Validation
  disabled: boolean
  onLoad: () => void
}) {
  const reqWord = plural(estimate.requests, 'запрос', 'запроса', 'запросов')

  return (
    <div className="pl-estimate">
      <div className="pl-est-info">
        <div className="pl-est-num">
          Потребуется загрузить <b>{estimate.requests}</b> {reqWord}
          <span className="pl-est-sec"> · {fmtSeconds(estimate.seconds)}</span>
        </div>
        <div className="pl-est-legs">
          {estimate.legs.map((leg, i) => (
            <span className="mchip" key={i}>
              {leg.fromLabel} → {leg.toLabel}: <b>{leg.days}</b>
              {leg.anyLeg && <span className="pl-anytag"> · все направления</span>}
            </span>
          ))}
        </div>
      </div>

      <button type="button" className="btn-primary" disabled={disabled} onClick={onLoad}>
        Загрузить данные →
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
