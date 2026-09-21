// Справочник точек A/B теперь приходит с бэка (GET /api/airports?q=) — автокомплит.

export interface AirportOption {
  code: string
  city: string
  country?: string
  flag?: string
  label: string
}

const API_BASE = import.meta.env.VITE_API_BASE ?? '/api'

export async function searchAirports(q: string, limit = 8): Promise<AirportOption[]> {
  const query = q.trim()
  if (!query) return []
  const res = await fetch(`${API_BASE}/airports?q=${encodeURIComponent(query)}&limit=${limit}`)
  if (!res.ok) return []
  const json = (await res.json()) as { airports: AirportOption[] }
  return json.airports
}

// Дефолтные точки — единственный уже собранный маршрут.
export const DEFAULT_ORIGIN: AirportOption = {
  code: 'MOW',
  city: 'Москва',
  flag: '🇷🇺',
  label: 'Москва (MOW)',
}
export const DEFAULT_DESTINATION: AirportOption = {
  code: 'ICN',
  city: 'Seoul',
  flag: '🇰🇷',
  label: 'Сеул / Инчхон (ICN)',
}
