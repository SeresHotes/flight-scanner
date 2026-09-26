// Страна города для фильтров планировщика. Отдельного поля страны в данных нет,
// но у каждого города есть флаг-эмодзи (бэк строит его из ISO2 страны) — из него
// ISO2 и восстанавливаем, а русское название берём из Intl.

const REGIONAL_A = 0x1f1e6

// '🇹🇷' → 'TR'; не флаг → ''.
export function flagToIso2(flag: string | undefined): string {
  const cps = Array.from(flag ?? '', (ch) => ch.codePointAt(0) ?? 0)
  if (cps.length !== 2) return ''
  if (cps.some((cp) => cp < REGIONAL_A || cp > REGIONAL_A + 25)) return ''
  return cps.map((cp) => String.fromCharCode(cp - REGIONAL_A + 65)).join('')
}

let names: Intl.DisplayNames | null | undefined

export function countryName(iso2: string): string {
  if (!iso2) return 'Без страны'
  if (names === undefined) {
    try {
      names = new Intl.DisplayNames(['ru'], { type: 'region' })
    } catch {
      names = null
    }
  }
  return names?.of(iso2) ?? iso2
}
