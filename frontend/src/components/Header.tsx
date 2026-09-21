import type { Meta } from '../types'
import { fmtDT } from '../lib/format'

// Порт header.hero из index.html: заголовок, подзаголовок, бейджи события.
export function Header({ meta }: { meta?: Meta }) {
  if (!meta) {
    return (
      <header className="hero">
        <h1>✈️ Планировщик перелётов</h1>
        <div className="sub">Выбери маршрут и даты — покажем варианты «туда-обратно с остановкой»</div>
      </header>
    )
  }

  const badges: React.ReactNode[] = []
  if (meta.event) {
    badges.push(
      <span className="badge" key="name">
        🎫 <b>{meta.event.name}</b>
      </span>,
    )
    if (meta.event.date) {
      badges.push(
        <span className="badge" key="date">
          📅 {fmtDT(meta.event.date + 'T20:00').d}
        </span>,
      )
    }
    if (meta.event.venue) {
      badges.push(
        <span className="badge" key="venue">
          📍 {meta.event.venue}
        </span>,
      )
    }
  }

  return (
    <header className="hero">
      <h1>✈️ {meta.title}</h1>
      <div className="sub">
        {meta.origin} ⇄ {meta.destination} · минимум {meta.min_stay} дн. на месте · валюта:{' '}
        {meta.currency}
      </div>
      {badges.length > 0 && <div className="concert">{badges}</div>}
    </header>
  )
}
