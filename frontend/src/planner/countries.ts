// Страна города для фильтров планировщика. Отдельного поля страны в данных нет,
// но у каждого города есть флаг-эмодзи (бэк строит его из ISO2 страны) — из него
// ISO2 и восстанавливаем, а названия берём из Intl.

const REGIONAL_A = 0x1f1e6

// '🇹🇷' → 'TR'; не флаг → ''.
export function flagToIso2(flag: string | undefined): string {
  const cps = Array.from(flag ?? '', (ch) => ch.codePointAt(0) ?? 0)
  if (cps.length !== 2) return ''
  if (cps.some((cp) => cp < REGIONAL_A || cp > REGIONAL_A + 25)) return ''
  return cps.map((cp) => String.fromCharCode(cp - REGIONAL_A + 65)).join('')
}

const cache = new Map<string, Intl.DisplayNames | null>()

function regionName(lang: string, iso2: string): string {
  if (!cache.has(lang)) {
    try {
      cache.set(lang, new Intl.DisplayNames([lang], { type: 'region' }))
    } catch {
      cache.set(lang, null)
    }
  }
  return cache.get(lang)?.of(iso2) ?? iso2
}

// Показываем по-английски — как и названия городов в данных.
export function countryName(iso2: string): string {
  return iso2 ? regionName('en', iso2) : 'No country'
}

// Привычные названия, которых нет в Intl (там «Türkiye», «ОАЭ»).
const ALIASES: Record<string, string> = {
  TR: 'turkey',
  AE: 'uae emirates эмираты',
  GB: 'uk britain великобритания англия',
  US: 'usa сша америка',
}

// Нижний регистр без диакритики: «Türkiye» → «turkiye».
export function normalizeSearch(text: string): string {
  return text.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase()
}

// Строка для поиска: английское и русское название, синонимы и ISO2.
export function countrySearchText(iso2: string): string {
  if (!iso2) return ''
  return normalizeSearch(`${regionName('en', iso2)} ${regionName('ru', iso2)} ${ALIASES[iso2] ?? ''} ${iso2}`)
}
