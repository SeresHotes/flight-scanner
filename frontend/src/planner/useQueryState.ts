import { useEffect, useMemo, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { resolveAirports } from '../data/airports'
import { decodeQuery, type PlanQuery } from './query'

let stopSeq = 0
export const nextStopId = () => `s${stopSeq++}`

// Запрос из URL (или null) + дорезолв названий/флагов городов: в ссылке только коды.
export function useQueryFromUrl(): [PlanQuery | null, (q: PlanQuery) => void] {
  const [sp] = useSearchParams()
  const initial = useMemo(() => decodeQuery(sp, nextStopId), []) // eslint-disable-line react-hooks/exhaustive-deps
  const [query, setQuery] = useState<PlanQuery | null>(initial)
  useResolveCities(query, setQuery)
  return [query, setQuery]
}

export function useResolveCities(query: PlanQuery | null, setQuery: (q: PlanQuery) => void) {
  useEffect(() => {
    if (!query) return
    const codes = query.stops
      .filter((s) => s.kind === 'cities')
      .flatMap((s) => s.airports)
      .filter((a) => a.code && !a.city)
      .map((a) => a.code)
    if (codes.length === 0) return
    let cancelled = false
    resolveAirports(codes).then((map) => {
      if (cancelled || map.size === 0) return
      setQuery({
        ...query,
        stops: query.stops.map((s) =>
          s.kind !== 'cities'
            ? s
            : { ...s, airports: s.airports.map((a) => (!a.city && map.has(a.code) ? map.get(a.code)! : a)) },
        ),
      })
    })
    return () => {
      cancelled = true
    }
  }, [query, setQuery])
}
