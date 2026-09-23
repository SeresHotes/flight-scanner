import { useEffect, useRef, useState } from 'react'
import { todayISO } from '../lib/dates'

// Выбор диапазона дат одним всплывающим календариком: тык по полю → открывается
// месяц, первый клик — «от», второй — «до» (в том же календаре). min ограничивает прошлое.

const WD = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
const MON = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 'Июль',
  'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']

function iso(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
}
function parse(s: string): Date {
  const [y, m, d] = s.split('-').map(Number)
  return new Date(y, m - 1, d)
}
function fmtShort(s: string): string {
  if (!s) return '—'
  const d = parse(s)
  return `${d.getDate()} ${['янв', 'фев', 'мар', 'апр', 'мая', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек'][d.getMonth()]}`
}

export function DateRangePicker({
  label,
  from,
  to,
  min,
  onChange,
}: {
  label: string
  from: string
  to: string
  min?: string
  onChange: (from: string, to: string) => void
}) {
  const [open, setOpen] = useState(false)
  const minISO = min || todayISO()
  const [view, setView] = useState<Date>(() => parse(from || minISO))
  const ref = useRef<HTMLDivElement>(null)

  // Закрытие по клику вне компонента.
  useEffect(() => {
    if (!open) return
    function onDoc(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', onDoc)
    return () => document.removeEventListener('mousedown', onDoc)
  }, [open])

  function pick(dayISO: string) {
    // Новый выбор, если ещё нет «от» или уже выбрана пара — начинаем заново с «от».
    if (!from || (from && to)) {
      onChange(dayISO, '')
    } else if (dayISO < from) {
      onChange(dayISO, '')
    } else {
      onChange(from, dayISO)
      setOpen(false)
    }
  }

  // Сетка месяца (понедельник — первый день).
  const first = new Date(view.getFullYear(), view.getMonth(), 1)
  const startOffset = (first.getDay() + 6) % 7 // 0=Пн
  const daysInMonth = new Date(view.getFullYear(), view.getMonth() + 1, 0).getDate()
  const cells: (string | null)[] = []
  for (let i = 0; i < startOffset; i++) cells.push(null)
  for (let d = 1; d <= daysInMonth; d++) cells.push(iso(new Date(view.getFullYear(), view.getMonth(), d)))

  return (
    <div className="drp" ref={ref}>
      <button type="button" className="drp-trigger" onClick={() => setOpen((o) => !o)}>
        <span className="drp-label">{label}</span>
        <span className="drp-val">
          {from ? fmtShort(from) : 'выберите'} <span className="drp-dash">–</span> {to ? fmtShort(to) : '…'}
        </span>
      </button>

      {open && (
        <div className="drp-pop">
          <div className="drp-head">
            <button type="button" onClick={() => setView(new Date(view.getFullYear(), view.getMonth() - 1, 1))}>‹</button>
            <div className="drp-title">{MON[view.getMonth()]} {view.getFullYear()}</div>
            <button type="button" onClick={() => setView(new Date(view.getFullYear(), view.getMonth() + 1, 1))}>›</button>
          </div>
          <div className="drp-grid drp-wd">
            {WD.map((w) => <div key={w} className="drp-wdc">{w}</div>)}
          </div>
          <div className="drp-grid">
            {cells.map((c, i) => {
              if (!c) return <div key={i} className="drp-cell drp-empty" />
              const disabled = c < minISO
              const isFrom = c === from
              const isTo = c === to
              const inRange = from && to && c > from && c < to
              const cls = [
                'drp-cell',
                disabled ? 'drp-dis' : '',
                isFrom || isTo ? 'drp-sel' : '',
                inRange ? 'drp-inrange' : '',
              ].filter(Boolean).join(' ')
              return (
                <button
                  key={i}
                  type="button"
                  className={cls}
                  disabled={disabled}
                  onClick={() => pick(c)}
                >
                  {parse(c).getDate()}
                </button>
              )
            })}
          </div>
          <div className="drp-hint">
            {!from || to ? 'Выберите дату вылета «от»' : 'Теперь выберите «до»'}
          </div>
        </div>
      )}
    </div>
  )
}
