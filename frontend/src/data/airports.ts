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

// Резолв IATA-кода в полную карточку: в URL храним только код, а имя/флаг
// подтягиваем через тот же /api/airports (код ищется точным совпадением).
export async function resolveAirport(code: string): Promise<AirportOption | null> {
  const cu = code.trim().toUpperCase()
  if (!cu) return null
  const res = await searchAirports(cu, 10)
  return res.find((a) => a.code === cu) ?? null
}

// Пакетный резолв набора кодов (с дедупликацией) → карта code → карточка.
export async function resolveAirports(codes: string[]): Promise<Map<string, AirportOption>> {
  const uniq = [...new Set(codes.map((c) => c.trim().toUpperCase()).filter(Boolean))]
  const entries = await Promise.all(uniq.map(async (code) => [code, await resolveAirport(code)] as const))
  const map = new Map<string, AirportOption>()
  for (const [code, opt] of entries) if (opt) map.set(code, opt)
  return map
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
