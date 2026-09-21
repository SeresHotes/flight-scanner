import { useQuery } from '@tanstack/react-query'
import { fetchRoutes } from '../data/routesApi'

export function useRoutes() {
  return useQuery({
    queryKey: ['routes'],
    queryFn: fetchRoutes,
    // Рефетч при каждом заходе на страницу выбора — чтобы после сбора новый
    // маршрут сразу появлялся в «Готово к просмотру» (без перезагрузки).
    staleTime: 0,
    refetchOnMount: 'always',
  })
}
