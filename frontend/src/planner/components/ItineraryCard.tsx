import type { Segment } from '../../types'
import { durFmt, fmtDT, dayW, plural, makeMoney } from '../../lib/format'
import type { Itinerary, ItineraryStop } from '../types'

const money = makeMoney('RUB')

function TransferBadge({ seg }: { seg: Segment }) {
  const n = seg.transfers || 0
  if (!n) return <span className="tbadge direct">● прямой рейс</span>
  const word = plural(n, 'пересадка', 'пересадки', 'пересадок')
  return (
    <span className={`tbadge conn ${n >= 2 ? 'hi' : ''}`}>
      ✈ {n} {word}
    </span>
  )
}

function LegRow({ seg }: { seg: Segment }) {
  const dep = fmtDT(seg.departure_at)
  const arr = fmtDT(seg.arrival_at)
  return (
    <div className="leg">
      <div className="pt">
        <div className="code">{seg.origin}</div>
        <div className="cty">{seg.origin_city || ''}</div>
        <div className="when">{dep.d}</div>
        <div className="clock">{dep.t}</div>
      </div>
      <div className="mid">
        <div className="line">
          <span>вылет</span>
          <span className="track"></span>🛬<span>прилёт</span>
        </div>
        <div className="dest">
          🛬 <b>{seg.destination}</b>
          {seg.destination_city ? ' · ' + seg.destination_city : ''}
          <span style={{ color: 'var(--muted)' }}>
            {' '}
            — {arr.d} {arr.t}
          </span>
        </div>
        <div className="info2">
          <TransferBadge seg={seg} />
          <span className="i">🕓 {durFmt(seg.duration)}</span>
          {seg.airline && <span className="i">🛩 {seg.airline}</span>}
        </div>
      </div>
      <div className="buy">
        {seg.link ? (
          <a href={seg.link} target="_blank" rel="noopener">
            <span className="p">{money(seg.price)}</span> →
          </a>
        ) : (
          <span className="p">{money(seg.price)}</span>
        )}
      </div>
    </div>
  )
}

// endpoint — старт/финиш: сколько мы там до вылета / после прилёта, не показываем.
function StayBar({ stop, endpoint = false }: { stop: ItineraryStop; endpoint?: boolean }) {
  return (
    <div className="stopstay region">
      🏙 {stop.flag} {stop.city}
      {!endpoint && (
        <>
          :{' '}
          <b>
            &nbsp;{stop.days} {dayW(stop.days)}
          </b>
        </>
      )}
      {stop.resolvedFromAny && <span className="pl-anytag">&nbsp;· подобран</span>}
      {!endpoint && stop.weekendCovered && <span className="pl-wknd">&nbsp;· выходные ✓</span>}
    </div>
  )
}

export function ItineraryCard({ it }: { it: Itinerary }) {
  const codes = it.stops.map((s) => s.code)
  return (
    <div className="card">
      <div className="top">
        <div className="route">
          {codes.join(' → ')}
          <span className="type-pill t-two">{codes.length - 1} перелёта</span>
        </div>
        <div className="price">
          {money(it.total_price)} <small>всего</small>
        </div>
      </div>
      <div className="meta-row">
        <span className="mchip korea">
          🧳 поездка: <b>{it.total_days} {dayW(it.total_days)}</b>
        </span>
        <span className="mchip">
          🕓 в пути: <b>{durFmt(it.travel_minutes)}</b>
        </span>
        <span className="mchip tr">
          🔁 пересадок: <b>{it.total_transfers}</b>
        </span>
      </div>
      <div className="legs">
        <StayBar stop={it.stops[0]} endpoint />
        {it.segments.map((s, i) => (
          <div key={i} style={{ display: 'contents' }}>
            <LegRow seg={s} />
            {it.stops[i + 1] && <StayBar stop={it.stops[i + 1]} endpoint={i + 1 === it.segments.length} />}
          </div>
        ))}
      </div>
    </div>
  )
}
