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

// Конец перелёта: код города, аэропорт (если отличается от кода города), дата и время.
function LegPoint({ code, city, airport, iso }: { code: string; city?: string; airport?: string; iso: string }) {
  const t = fmtDT(iso)
  return (
    <div className="pt">
      <div className="code">
        {code}
        {airport && airport !== code && <span className="apt"> · {airport}</span>}
      </div>
      <div className="cty">{city || ''}</div>
      <div className="when">{t.d}</div>
      <div className="clock">{t.t}</div>
    </div>
  )
}

// Точки пересадок на линии перелёта. Если города не распарсились из ссылки —
// рисуем столько же безымянных точек. Ожидание источник отдаёт только суммой по
// всем пересадкам, поэтому под точкой оно — лишь когда пересадка одна.
function TransferDots({ seg }: { seg: Segment }) {
  const n = seg.transfers || 0
  if (!n) return null
  const pts = seg.transfer_points || []
  const known = pts.length === n
  const wait = n === 1 && seg.layover_minutes ? durFmt(seg.layover_minutes) : null
  return (
    <div className="pl-dots">
      {Array.from({ length: n }, (_, i) => {
        const p = known ? pts[i] : null
        const title = (p ? `${p.city} (${p.code})` : 'город — на Aviasales') + (wait ? `, ожидание ${wait}` : '')
        return (
          <div key={i} className="pl-dot" title={title}>
            <span className="pl-dot-code">{p ? p.code : '?'}</span>
            <span className="pl-dot-mark" />
            <span className="pl-dot-city">{p ? p.city : ''}</span>
            {wait && <span className="pl-dot-wait">⏳ {wait}</span>}
          </div>
        )
      })}
    </div>
  )
}

// Подпись под линией: весь путь и — при пересадках — чистое время в воздухе.
function LegDuration({ seg }: { seg: Segment }) {
  const air = seg.layover_minutes ? seg.duration - seg.layover_minutes : null
  return (
    <div className="pl-dur">
      🕓 {durFmt(seg.duration)} в пути
      {air ? <span className="pl-dur-air"> · ✈ {durFmt(air)} в воздухе</span> : null}
    </div>
  )
}

function LegRow({ seg }: { seg: Segment }) {
  return (
    <div className="leg pl-leg">
      <LegPoint code={seg.origin} city={seg.origin_city} airport={seg.origin_airport} iso={seg.departure_at} />
      <div className="mid">
        <div className="pl-track">
          <span className="pl-track-line" />
          <TransferDots seg={seg} />
          <span className="pl-track-end">🛬</span>
        </div>
        <LegDuration seg={seg} />
        <div className="info2">
          <TransferBadge seg={seg} />
          {(seg.transfers || 0) >= 2 && seg.layover_minutes ? (
            <span className="i">⏳ на пересадках: {durFmt(seg.layover_minutes)}</span>
          ) : null}
          {seg.airline && <span className="i">🛩 {seg.airline}</span>}
        </div>
      </div>
      <LegPoint
        code={seg.destination}
        city={seg.destination_city}
        airport={seg.destination_airport}
        iso={seg.arrival_at}
      />
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
