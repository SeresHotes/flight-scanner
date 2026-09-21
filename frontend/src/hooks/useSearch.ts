import { useQuery } from '@tanstack/react-query'
import { search, type SearchParams, type SearchResult } from '../data/searchClient'

// Ключ запроса стабилизируем по значимым полям — при смене A/B или дат плеч
// TanStack Query автоматически перезапросит (позже — сходит в /api/search).
function queryKey(p: SearchParams) {
  return ['search', p.origin, p.destination, ...p.leg1_dates, ...p.leg2_dates, p.min_stay]
}

export function useSearch(params: SearchParams | null) {
  return useQuery<SearchResult>({
    queryKey: params ? queryKey(params) : ['search', 'idle'],
    queryFn: () => search(params as SearchParams),
    enabled: params !== null,
    staleTime: 5 * 60 * 1000,
  })
}
