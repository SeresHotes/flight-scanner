import type { SearchParams } from '../data/searchClient'

// SearchParams ↔ query-строка: результаты шарятся ссылкой, страницы независимы.
export function paramsToQuery(p: SearchParams): string {
  const sp = new URLSearchParams()
  sp.set('o', p.origin)
  sp.set('d', p.destination)
  if (p.leg1_dates[0]) sp.set('l1a', p.leg1_dates[0])
  if (p.leg1_dates[1]) sp.set('l1b', p.leg1_dates[1])
  if (p.leg2_dates[0]) sp.set('l2a', p.leg2_dates[0])
  if (p.leg2_dates[1]) sp.set('l2b', p.leg2_dates[1])
  sp.set('ms', String(p.min_stay))
  return sp.toString()
}

export function queryToParams(search: string): SearchParams | null {
  const sp = new URLSearchParams(search)
  const o = sp.get('o')
  const d = sp.get('d')
  if (!o || !d) return null
  return {
    origin: o,
    destination: d,
    leg1_dates: [sp.get('l1a') ?? '', sp.get('l1b') ?? ''],
    leg2_dates: [sp.get('l2a') ?? '', sp.get('l2b') ?? ''],
    min_stay: Number(sp.get('ms') ?? '7'),
  }
}
