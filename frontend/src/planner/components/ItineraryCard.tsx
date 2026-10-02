import type { Segment } from '../../types'
import { durFmt, fmtDT, dayW, plural, makeMoney } from '../../lib/format'
import type { Itinerary, ItineraryStop } from '../types'

const money = makeMoney('RUB')

// Hidden-city: билет куплен до final, выходим в точке прилёта сегмента.
function HiddenCityBadge({ seg }: { seg: Segment }) {
  const h = seg.hidden_city
  if (!h) return null
  const bag = h.baggage?.known ? (h.baggage.included ? 'багаж включён' : 'без багажа') : 'багаж ?'
  const title =
    `Билет ${h.chain.join('→')} до ${h.final_city || h.final}: выходим в ${seg.destination}, ` +
    `остаток не летим. Только ручная кладь (${bag}), в одну сторону. Время прилёта — оценка.`
  return (
    <span className="tbadge hidden" title={title}>
      🎯 hidden-city → {h.final}
    </span>
  )
}

function TransferBadge({ seg }: { seg: Segment }) {
  const n = seg.transfers || 0
  if (seg.hidden_city) return <HiddenCityBadge seg={seg} />
  if (!n) return <span className="tbadge direct">● прямой рейс</span>
  const word = plural(n, 'пересадка', 'пересадки', 'пересадок')
  return (
    <span className={`tbadge conn ${n >= 2 ? 'hi' : ''}`}>
      ✈ {n} {word}
    </span>
  )
}

// Багаж по билету (GraphQL). У hidden-city — «!»: чемодан зарегистрируют до конечного
// пункта билета, в хабе его могут не выдать.
function BaggageBadge({ seg }: { seg: Segment }) {
  const b = seg.baggage
  if (!b) return null
  const text = !b.known ? '🧳 багаж ?' : b.included ? `🧳 ${b.pieces ?? 1} × ${b.kg ? `${b.kg} кг` : 'багаж'}` : '🧳 без багажа'
  const warn = seg.hidden_city ? '!' : ''
  const title = seg.hidden_city
    ? 'hidden-city: багаж зарегистрируют до конечного пункта билета — в точке выхода его могут не выдать'
    : b.known
      ? b.included
        ? 'зарегистрированный багаж включён в тариф'
        : 'только ручная кладь'
      : 'условия тарифа неизвестны'
  return (
    <span className={`i pl-bag ${b.included ? 'yes' : 'no'}`} title={title}>
      {text}
      {warn ? <b className="pl-bag-warn">{warn}</b> : null}
    </span>
  )
}

// Конец перелёта: код города, аэропорт (если отличается от кода города), дата и время.
function LegPoint({
  code,
  city,
  airport,
  iso,
  approx,
}: {
  code: string
  city?: string
  airport?: string
  iso: string
  approx?: boolean // время — оценка (hidden-city: источник не отдаёт время пересадки)
}) {
  const t = fmtDT(iso)
  return (
    <div className="pt">
      <div className="code">
        {code}
        {airport && airport !== code && <span className="apt"> · {airport}</span>}
      </div>
      <div className="cty">{city || ''}</div>
      <div className="when">{t.d}</div>
      <div className="clock" title={approx ? 'оценка: время пересадки источник не отдаёт' : undefined}>
        {approx ? '≈' : ''}
        {t.t}
      </div>
    </div>
  )
}

// Точки пересадок на линии перелёта. Если города не распарсились из ссылки —
// рисуем столько же безымянных точек. Ожидание по каждой пересадке есть у данных
// GraphQL (p.minutes); у старых данных — только сумма, показываем её при одной пересадке.
function TransferDots({ seg }: { seg: Segment }) {
  const n = seg.transfers || 0
  if (!n) return null
  const pts = seg.transfer_points || []
  const known = pts.length === n
  const totalWait = n === 1 && seg.layover_minutes ? durFmt(seg.layover_minutes) : null
  return (
    <div className="pl-dots">
      {Array.from({ length: n }, (_, i) => {
        const p = known ? pts[i] : null
        const wait = p?.minutes ? durFmt(p.minutes) : totalWait
        const flags = p ? `${p.night ? ', ночная' : ''}${p.visa ? ', нужна виза' : ''}` : ''
        const title =
          (p ? `${p.city} (${p.code})` : 'город — на Aviasales') + (wait ? `, ожидание ${wait}` : '') + flags
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
          <BaggageBadge seg={seg} />
        </div>
      </div>
      <LegPoint
        code={seg.destination}
        city={seg.destination_city}
        airport={seg.destination_airport}
        iso={seg.arrival_at}
        approx={!!seg.hidden_city?.arrival_estimated}
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
      {stop.departFrom && (
        <span
          className="pl-hop"
          title="Дальше летим из соседнего города: переезд по земле сами, билеты раздельные"
        >
          &nbsp;· 🚆 переезд в {stop.departFrom.flag} {stop.departFrom.city || stop.departFrom.code}
          {stop.departFrom.km != null ? ` (${stop.departFrom.km} км)` : ''}
        </span>
      )}
    </div>
  )
}

// skipped — названия пропущенных остановок запроса (маршрут в обход).
export function ItineraryCard({ it, skipped }: { it: Itinerary; skipped?: string[] }) {
  const codes = it.stops.map((s) => (s.departFrom ? `${s.code}⇢${s.departFrom.code}` : s.code))
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
        {skipped?.length ? (
          <span className="mchip pl-skiptag" title="Остановка пропущена: перелёт в обход">
            ⤼ без <b>{skipped.join(', ')}</b>
          </span>
        ) : null}
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
