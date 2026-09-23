// Утилиты дат для формы поиска и фильтров (все даты — ISO 'YYYY-MM-DD').

export function todayISO(): string {
  return new Date().toISOString().slice(0, 10)
}

export function addDaysISO(iso: string, days: number): string {
  const d = new Date(iso + 'T00:00:00')
  d.setDate(d.getDate() + days)
  return d.toISOString().slice(0, 10)
}

// Число календарных дней между двумя ISO-датами (b - a); отрицательное, если b < a.
export function daysBetweenISO(a: string, b: string): number {
  const ms = new Date(b + 'T00:00:00').getTime() - new Date(a + 'T00:00:00').getTime()
  return Math.round(ms / 86400000)
}

// Ограничить дату диапазоном [min, max] (пустые границы игнорируются).
export function clampISO(iso: string, min?: string, max?: string): string {
  if (min && iso < min) return min
  if (max && iso > max) return max
  return iso
}

// Первый день ISO-даты (для сравнения диапазонов дат с датами-со-временем).
export function dateOnly(iso: string | null | undefined): string {
  return iso ? iso.slice(0, 10) : ''
}
