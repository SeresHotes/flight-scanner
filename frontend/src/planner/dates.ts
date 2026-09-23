// Небольшая дата-арифметика для планировщика. Работаем со строками YYYY-MM-DD
// и ISO-датавременем YYYY-MM-DDTHH:MM — берём только дату, чтобы не зависеть от TZ.

const DAY_MS = 24 * 3600 * 1000

function toUTC(dateStr: string): number {
  const [y, m, d] = dateStr.slice(0, 10).split('-').map(Number)
  return Date.UTC(y, (m || 1) - 1, d || 1)
}

export function dateOnly(iso: string): string {
  return iso.slice(0, 10)
}

// Число календарных дат в окне [a, b] включительно (min 0 при пустых значениях).
export function daysInWindow([a, b]: [string, string]): number {
  if (!a || !b) return 0
  const diff = Math.round((toUTC(b) - toUTC(a)) / DAY_MS)
  return diff < 0 ? 0 : diff + 1
}

// Сколько календарных месяцев покрывает окно [a, b] (для оценки month-matrix).
export function monthsInWindow([a, b]: [string, string]): number {
  if (!a || !b) return 0
  const [ya, ma] = a.slice(0, 7).split('-').map(Number)
  const [yb, mb] = b.slice(0, 7).split('-').map(Number)
  return (yb - ya) * 12 + (mb - ma) + 1
}

// Разница в днях между двумя датами (b - a), по календарю.
export function dayCountBetween(a: string, b: string): number {
  if (!a || !b) return 0
  return Math.round((toUTC(b) - toUTC(a)) / DAY_MS)
}

export function addDays(dateStr: string, n: number): string {
  const t = toUTC(dateStr) + n * DAY_MS
  return new Date(t).toISOString().slice(0, 10)
}

// Пребывание [arrive..depart] покрывает всё требуемое окно [f..t].
export function coversWindow(arriveIso: string, departIso: string, [f, t]: [string, string]): boolean {
  if (!f || !t) return true
  return dateOnly(arriveIso) <= f && dateOnly(departIso) >= t
}

// Оба выходных (суббота И воскресенье) попадают в пребывание [arrive..depart].
export function hasBothWeekendDays(arriveIso: string, departIso: string): boolean {
  const start = toUTC(arriveIso)
  const end = toUTC(departIso)
  if (end < start) return false
  let sat = false
  let sun = false
  for (let t = start; t <= end; t += DAY_MS) {
    const wd = new Date(t).getUTCDay() // 0 — вс, 6 — сб
    if (wd === 6) sat = true
    if (wd === 0) sun = true
    if (sat && sun) return true
  }
  return false
}
