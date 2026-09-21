import { useEffect, useRef, useState } from 'react'
import { searchAirports, type AirportOption } from '../data/airports'

// A/B combobox с автокомплитом из GET /api/airports?q=.
export function AirportCombobox({
  label,
  value,
  onChange,
}: {
  label: string
  value: AirportOption
  onChange: (opt: AirportOption) => void
}) {
  const [text, setText] = useState(value.label)
  const [options, setOptions] = useState<AirportOption[]>([])
  const [open, setOpen] = useState(false)
  const [active, setActive] = useState(0)
  const boxRef = useRef<HTMLDivElement>(null)

  // Синхронизируем отображаемый текст при внешней смене value (например, swap A/B).
  useEffect(() => {
    setText(value.label)
  }, [value])

  // Дебаунс-запрос подсказок.
  useEffect(() => {
    if (!open) return
    const q = text.trim()
    if (!q || q === value.label) {
      setOptions([])
      return
    }
    const t = setTimeout(async () => {
      const res = await searchAirports(q)
      setOptions(res)
      setActive(0)
    }, 180)
    return () => clearTimeout(t)
  }, [text, open, value.label])

  // Закрытие по клику вне.
  useEffect(() => {
    function onDocClick(e: MouseEvent) {
      if (boxRef.current && !boxRef.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', onDocClick)
    return () => document.removeEventListener('mousedown', onDocClick)
  }, [])

  function pick(opt: AirportOption) {
    onChange(opt)
    setText(opt.label)
    setOpen(false)
    setOptions([])
  }

  function onKeyDown(e: React.KeyboardEvent) {
    if (!open || !options.length) return
    if (e.key === 'ArrowDown') {
      e.preventDefault()
      setActive((a) => Math.min(a + 1, options.length - 1))
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      setActive((a) => Math.max(a - 1, 0))
    } else if (e.key === 'Enter') {
      e.preventDefault()
      pick(options[active])
    } else if (e.key === 'Escape') {
      setOpen(false)
    }
  }

  return (
    <div className="field">
      <label>{label}</label>
      <div className="combobox" ref={boxRef}>
        <input
          type="text"
          value={text}
          placeholder="Город или IATA-код"
          onChange={(e) => {
            setText(e.target.value)
            setOpen(true)
          }}
          onFocus={() => setOpen(true)}
          onKeyDown={onKeyDown}
        />
        {open && options.length > 0 && (
          <ul className="combo-list">
            {options.map((o, i) => (
              <li
                key={o.code}
                className={i === active ? 'active' : ''}
                onMouseDown={(e) => {
                  e.preventDefault()
                  pick(o)
                }}
                onMouseEnter={() => setActive(i)}
              >
                <span className="ac-city">
                  {o.flag} {o.city}
                </span>
                <span className="ac-code">{o.code}</span>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  )
}
