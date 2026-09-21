import type { Meta } from '../types'

// Порт блока .stats из index.html.
export function Stats({ meta, money }: { meta: Meta; money: (n: number) => string }) {
  const rows: [React.ReactNode, string][] = [
    [meta.counts.there, 'плеч «туда»'],
    [meta.counts.back, 'плеч «обратно»'],
    [meta.there_cities.length, 'городов остановки туда'],
    [meta.back_cities.length, 'городов остановки обратно'],
    [money(meta.there_price_range[0] + meta.back_price_range[0]), 'round-trip от'],
  ]
  return (
    <div className="stats">
      {rows.map(([n, l], i) => (
        <div className="stat" key={i}>
          <div className="n">{n}</div>
          <div className="l">{l}</div>
        </div>
      ))}
    </div>
  )
}
