// Форматтеры — перенос из web/index.html 1:1.

const WD = ['вс', 'пн', 'вт', 'ср', 'чт', 'пт', 'сб']
const MON = ['янв', 'фев', 'мар', 'апр', 'мая', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']

export function makeMoney(currency: string) {
  const cur = currency === 'RUB' ? '₽' : currency
  return (n: number) => n.toLocaleString('ru-RU') + ' ' + cur
}

export function plural(n: number, one: string, few: string, many: string): string {
  const m10 = n % 10
  const m100 = n % 100
  if (m10 === 1 && m100 !== 11) return one
  if (m10 >= 2 && m10 <= 4 && (m100 < 10 || m100 >= 20)) return few
  return many
}

export interface DateTimeParts {
  t: string
  d: string
}

export function fmtDT(iso: string | null | undefined): DateTimeParts {
  if (!iso) return { t: '—', d: '' }
  const m = iso.match(/(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/)
  if (!m) return { t: iso, d: '' }
  const [, Y, Mo, Da, H, Mi] = m
  const dObj = new Date(Number(Y), Number(Mo) - 1, Number(Da))
  return { t: `${H}:${Mi}`, d: `${+Da} ${MON[Number(Mo) - 1]}, ${WD[dObj.getDay()]}` }
}

export function durFmt(min: number | undefined): string {
  if (!min) return ''
  const h = Math.floor(min / 60)
  const m = min % 60
  return `${h}ч${m ? ' ' + m + 'м' : ''}`
}

export const dayW = (n: number) => plural(n, 'день', 'дня', 'дней')

// Дата сбора данных: ISO (с временем или только дата) → «21 сен 2026».
export function fmtDate(iso: string | null | undefined): string {
  if (!iso) return '—'
  const m = iso.match(/(\d{4})-(\d{2})-(\d{2})/)
  if (!m) return iso
  const [, Y, Mo, Da] = m
  return `${+Da} ${MON[Number(Mo) - 1]} ${Y}`
}

export const dateChip = (iso: string) => fmtDT(iso.length === 10 ? iso + 'T00:00' : iso).d
