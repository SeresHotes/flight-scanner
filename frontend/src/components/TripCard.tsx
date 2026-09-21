import type { Meta, Segment, Stopover, Trip } from '../types'
import type { RenderItem } from '../lib/filters'
import { fmtDT, durFmt, dayW, dateChip, plural } from '../lib/format'

// Рендер карточек — перенос cardOneway/cardRound/legRow/cityBar/... из index.html.

interface Ctx {
  meta: Meta
  money: (n: number) => string
  region: string
}

function TransferBadge({ seg }: { seg: Segment }) {
  const n = seg.transfers || 0
  if (!n) return <span className="tbadge direct">● прямой рейс</span>
  const word = plural(n, 'пересадка', 'пересадки', 'пересадок')
  const pts = seg.transfer_points || []
  const via = pts.length ? ` в ${pts.map((p) => p.city).join(' → ')}` : ' (город — на Aviasales)'
  return (
    <span className={`tbadge conn ${n >= 2 ? 'hi' : ''}`}>
      ✈ {n} {word}
      {via}
    </span>
  )
}

function LegRow({ seg, money }: { seg: Segment; money: (n: number) => string }) {
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

function CityBar({ so, days, tail }: { so: Stopover; days: number; tail: string }) {
  return (
    <div className="stopstay">
      🏙 {so.flag} {so.city}: остановка{' '}
      <b>&nbsp;{days} {dayW(days)}</b>
      &nbsp; · {tail}
      {so.transfer && (
        <div className="xfer">
          🛬 прилёт {so.transfer.from_city} ({so.transfer.from}) → 🚗{' '}
          {Math.round(so.transfer.distance_km)} км → 🛫 вылет {so.transfer.to_city} (
          {so.transfer.to})
        </div>
      )}
    </div>
  )
}

function RegionBar({ days, ctx }: { days: number; ctx: Ctx }) {
  return (
    <div className="stopstay region">
      {ctx.region}:{' '}
      <b>
        &nbsp;{days} {dayW(days)}
        {ctx.meta.event ? ' · событие здесь' : ''}
      </b>
    </div>
  )
}

function StopChip({ so }: { so: Stopover }) {
  const xf = so.transfer ? ` ↹ ${so.transfer.from}/${so.transfer.to}` : ''
  return (
    <span className="mchip stay">
      🏙 {so.flag} {so.city}:{' '}
      <b>
        {so.days} {dayW(so.days)}
      </b>
      {xf}
    </span>
  )
}

// Сегменты одного плеча + плашка остановки после первого сегмента.
function LegSegments({ opt, tail, money }: { opt: Trip; tail: string; money: (n: number) => string }) {
  return (
    <>
      {opt.segments.map((s, i) => (
        <div key={i} style={{ display: 'contents' }}>
          <LegRow seg={s} money={money} />
          {opt.stop && i === 0 && opt.stopover && (
            <CityBar so={opt.stopover} days={opt.sdays} tail={tail} />
          )}
        </div>
      ))}
    </>
  )
}

interface Pill {
  cls: string
  l: string
}

function Card({
  codes,
  pill,
  price,
  priceLbl,
  meta,
  legs,
  money,
}: {
  codes: string[]
  pill: Pill
  price: number
  priceLbl: string
  meta: React.ReactNode
  legs: React.ReactNode
  money: (n: number) => string
}) {
  return (
    <div className="card">
      <div className="top">
        <div className="route">
          {codes.join(' → ')}
          <span className={`type-pill ${pill.cls}`}>{pill.l}</span>
        </div>
        <div className="price">
          {money(price)} <small>{priceLbl}</small>
        </div>
      </div>
      <div className="meta-row">{meta}</div>
      <div className="legs">{legs}</div>
    </div>
  )
}

function CardOneway({ o, ctx }: { o: Trip; ctx: Ctx }) {
  const { money, meta, region } = ctx
  const last = o.segments[o.segments.length - 1]
  const codes = o.segments.map((s) => s.origin).concat(last.destination)
  const tail = o.direction === 'there' ? `далее в ${region}` : 'далее домой'
  const flag = meta.region_flag || '📍'

  const metaChips: React.ReactNode[] = []
  if (o.direction === 'there') {
    metaChips.push(
      <span className="mchip" key="dep">
        🛫 вылет из Москвы: <b>{dateChip(o.segments[0].departure_at)}</b>
      </span>,
      <span className="mchip korea" key="arr">
        {flag} прилёт: <b>{dateChip(o.hub_date)}</b>
      </span>,
    )
  } else {
    metaChips.push(
      <span className="mchip korea" key="dep">
        {flag} вылет: <b>{dateChip(o.hub_date)}</b>
      </span>,
      <span className="mchip" key="arr">
        🛬 в Москве: <b>{dateChip(last.arrival_at)}</b>
      </span>,
    )
  }
  if (o.stop && o.stopover) metaChips.push(<StopChip so={o.stopover} key="stop" />)
  metaChips.push(
    <span className="mchip" key="trav">
      🕓 в пути: <b>{durFmt(o.travel_minutes)}</b>
    </span>,
    <span className="mchip tr" key="tr">
      🔁 пересадок: <b>{o.total_transfers}</b>
    </span>,
  )

  const pill: Pill =
    o.direction === 'there'
      ? { cls: 't-there', l: o.stop ? 'Туда · с остановкой' : 'Туда' }
      : { cls: 't-back', l: o.stop ? 'Обратно · с остановкой' : 'Обратно' }

  return (
    <Card
      codes={codes}
      pill={pill}
      price={o.total_price}
      priceLbl="в одну сторону"
      money={money}
      meta={metaChips}
      legs={<LegSegments opt={o} tail={tail} money={money} />}
    />
  )
}

function CardRound({ th, bk, korea, ctx }: { th: Trip; bk: Trip; korea: number; ctx: Ctx }) {
  const { money, meta } = ctx
  const segs = th.segments.concat(bk.segments)
  const last = segs[segs.length - 1]
  const codes = segs.map((s) => s.origin).concat(last.destination)
  const flag = meta.region_flag || '📍'

  const metaChips: React.ReactNode[] = [
    <span className="mchip korea" key="korea">
      {flag} на месте:{' '}
      <b>
        {korea} {dayW(korea)}
      </b>
    </span>,
  ]
  if (th.stop && th.stopover) metaChips.push(<StopChip so={th.stopover} key="sth" />)
  if (bk.stop && bk.stopover) metaChips.push(<StopChip so={bk.stopover} key="sbk" />)
  metaChips.push(
    <span className="mchip" key="dep">
      🛫 вылет: <b>{dateChip(th.segments[0].departure_at)}</b>
    </span>,
    <span className="mchip" key="back">
      🛬 назад: <b>{dateChip(bk.segments[bk.segments.length - 1].arrival_at)}</b>
    </span>,
    <span className="mchip" key="trav">
      🕓 в пути: <b>{durFmt(th.travel_minutes + bk.travel_minutes)}</b>
    </span>,
    <span className="mchip tr" key="tr">
      🔁 пересадок всего: <b>{th.total_transfers + bk.total_transfers}</b>
    </span>,
  )

  const stops = (th.stop ? 1 : 0) + (bk.stop ? 1 : 0)
  const pill: Pill =
    stops === 0
      ? { cls: 't-direct', l: 'Туда-обратно · без остановок' }
      : { cls: 't-two', l: `Туда-обратно · остановки: ${stops}` }

  return (
    <Card
      codes={codes}
      pill={pill}
      price={th.total_price + bk.total_price}
      priceLbl="всего"
      money={money}
      meta={metaChips}
      legs={
        <>
          <LegSegments opt={th} tail="далее к цели" money={money} />
          <RegionBar days={korea} ctx={ctx} />
          <LegSegments opt={bk} tail="далее домой" money={money} />
        </>
      }
    />
  )
}

export function TripCard({ item, ctx }: { item: RenderItem; ctx: Ctx }) {
  if (item.kind === 'oneway') return <CardOneway o={item.trip} ctx={ctx} />
  return <CardRound th={item.th} bk={item.bk} korea={item.korea} ctx={ctx} />
}

export type { Ctx }
