import { useEffect, useMemo, useRef, useState } from 'react'
import { plural } from '../../lib/format'
import { countryName, countrySearchText, flagToIso2, normalizeSearch } from '../countries'
import type { CityOption } from './CityFilterCard'

interface Country {
  iso2: string
  name: string
  search: string // en + ru + синонимы + ISO2, нормализовано
  flag?: string
  codes: string[]
}

type Item = { kind: 'country'; country: Country } | { kind: 'city'; city: CityOption; countryName: string }

const MAX_ITEMS = 40

function groupCountries(options: CityOption[]): Map<string, Country> {
  const byIso = new Map<string, Country>()
  for (const o of options) {
    const iso2 = flagToIso2(o.flag)
    let c = byIso.get(iso2)
    if (!c) byIso.set(iso2, (c = { iso2, name: countryName(iso2), search: countrySearchText(iso2), flag: o.flag, codes: [] }))
    c.codes.push(o.code)
  }
  return byIso
}

// Выбор городов остановки в фильтрах: чипсы выбранного + поиск по городам и
// странам (как в скелете маршрута). Страна добавляет все свои города разом и
// показывается одним чипом, пока выбраны все её города. Ничего не выбрано —
// подходит любой город. В модели — список разрешённых кодов (allowedCodes).
export function CityPicker({
  options,
  allowed,
  onChange,
}: {
  options: CityOption[]
  allowed: string[] | null
  onChange: (allowed: string[] | null) => void
}) {
  const [text, setText] = useState('')
  const [open, setOpen] = useState(false)
  const [active, setActive] = useState(0)
  const boxRef = useRef<HTMLDivElement>(null)

  const countries = useMemo(() => groupCountries(options), [options])
  const byCode = useMemo(() => new Map(options.map((o) => [o.code, o])), [options])
  const countryOfCode = useMemo(
    () => new Map(options.map((o) => [o.code, countries.get(flagToIso2(o.flag))])),
    [options, countries],
  )

  const selected = useMemo<string[]>(() => allowed ?? [], [allowed])
  const selectedSet = useMemo(() => new Set(selected), [selected])

  // Пусто или выбраны все — снова «любой» (null).
  const commit = (next: string[]) => {
    const nextSet = new Set(next)
    const all = options.every((o) => nextSet.has(o.code))
    onChange(next.length === 0 || all ? null : next)
  }

  const add = (codes: string[]) => commit([...selected, ...codes.filter((c) => !selectedSet.has(c))])
  const remove = (codes: string[]) => commit(selected.filter((c) => !codes.includes(c)))

  // Чипсы: страна целиком (если выбраны все её города и их больше одного), иначе города.
  const chips = useMemo(() => {
    const out: { key: string; label: string; hint?: string; codes: string[] }[] = []
    const covered = new Set<string>()
    for (const c of countries.values()) {
      if (c.codes.length > 1 && c.codes.every((code) => selectedSet.has(code))) {
        out.push({
          key: `country-${c.iso2}`,
          label: `${c.flag ? `${c.flag} ` : ''}${c.name}`,
          hint: `${c.codes.length} ${plural(c.codes.length, 'город', 'города', 'городов')}`,
          codes: c.codes,
        })
        c.codes.forEach((code) => covered.add(code))
      }
    }
    for (const code of selected) {
      if (covered.has(code)) continue
      const o = byCode.get(code)
      out.push({ key: code, label: `${o?.flag ? `${o.flag} ` : ''}${o?.city ?? code}`, hint: code, codes: [code] })
    }
    return out
  }, [countries, selected, selectedSet, byCode])

  // Подсказки: сперва страны (с >1 городом), затем города; уже выбранное скрываем.
  const items = useMemo<Item[]>(() => {
    const q = normalizeSearch(text.trim())
    const countryItems: Item[] = [...countries.values()]
      .filter((c) => c.codes.length > 1 && !c.codes.every((code) => selectedSet.has(code)))
      .filter((c) => !q || c.search.includes(q))
      .sort((a, b) => b.codes.length - a.codes.length || a.name.localeCompare(b.name, 'ru'))
      .map((country) => ({ kind: 'country', country }))
    const cityItems: Item[] = options
      .filter((o) => !selectedSet.has(o.code))
      .filter((o) => {
        if (!q) return true
        const cs = countryOfCode.get(o.code)?.search ?? ''
        return normalizeSearch(o.city).includes(q) || o.code.toLowerCase().startsWith(q) || cs.includes(q)
      })
      .sort((a, b) => a.city.localeCompare(b.city, 'ru'))
      .map((city) => ({ kind: 'city', city, countryName: countryOfCode.get(city.code)?.name ?? '' }))
    return [...countryItems, ...cityItems].slice(0, MAX_ITEMS)
  }, [text, countries, options, selectedSet, countryOfCode])

  useEffect(() => setActive(0), [text])

  // Закрытие по клику вне.
  useEffect(() => {
    function onDocClick(e: MouseEvent) {
      if (boxRef.current && !boxRef.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', onDocClick)
    return () => document.removeEventListener('mousedown', onDocClick)
  }, [])

  const pick = (it: Item) => {
    add(it.kind === 'country' ? it.country.codes : [it.city.code])
    setText('')
  }

  function onKeyDown(e: React.KeyboardEvent) {
    if (e.key === 'Backspace' && !text && chips.length) {
      remove(chips[chips.length - 1].codes)
      return
    }
    if (!open || !items.length) return
    if (e.key === 'ArrowDown') {
      e.preventDefault()
      setActive((a) => Math.min(a + 1, items.length - 1))
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      setActive((a) => Math.max(a - 1, 0))
    } else if (e.key === 'Enter') {
      e.preventDefault()
      pick(items[Math.min(active, items.length - 1)])
    } else if (e.key === 'Escape') {
      setOpen(false)
    }
  }

  const total = options.length
  const summary =
    allowed === null
      ? `любой из ${total} ${plural(total, 'города', 'городов', 'городов')}`
      : `подходит ${allowed.length} из ${total}`

  return (
    <div className="pl-citypicker">
      <div className="pl-cp-head">
        <span className="pl-cp-summary">{summary}</span>
        {allowed !== null && (
          <button type="button" className="pl-cp-reset" onClick={() => onChange(null)}>
            сбросить
          </button>
        )}
      </div>

      {chips.length > 0 && (
        <div className="pl-chips">
          {chips.map((c) => (
            <span className="pl-chip" key={c.key}>
              {c.label} {c.hint && <span className="pl-chip-code">{c.hint}</span>}
              <button type="button" className="pl-chip-x" title="Убрать" onClick={() => remove(c.codes)}>
                ✕
              </button>
            </span>
          ))}
        </div>
      )}

      <div className="combobox" ref={boxRef}>
        <input
          type="text"
          value={text}
          placeholder="Добавить город или страну"
          onChange={(e) => {
            setText(e.target.value)
            setOpen(true)
          }}
          onFocus={() => setOpen(true)}
          onKeyDown={onKeyDown}
        />
        {open && items.length > 0 && (
          <ul className="combo-list">
            {items.map((it, i) => (
              <li
                key={it.kind === 'country' ? `country-${it.country.iso2}` : it.city.code}
                className={i === active ? 'active' : ''}
                onMouseDown={(e) => {
                  e.preventDefault()
                  pick(it)
                }}
                onMouseEnter={() => setActive(i)}
              >
                {it.kind === 'country' ? (
                  <>
                    <span className="ac-city">
                      {it.country.flag} <b>{it.country.name}</b> — вся страна
                    </span>
                    <span className="ac-code">{it.country.codes.length}</span>
                  </>
                ) : (
                  <>
                    <span className="ac-city">
                      {it.city.flag} {it.city.city}
                      {it.countryName && <span className="ac-country"> · {it.countryName}</span>}
                    </span>
                    <span className="ac-code">{it.city.code}</span>
                  </>
                )}
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  )
}
