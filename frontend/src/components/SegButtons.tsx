// Сегментные кнопки со счётчиками — порт segHTML/segbtns из index.html.
export interface SegOption<V extends string> {
  v: V
  l: string
}

export function SegButtons<V extends string>({
  options,
  current,
  countFn,
  onPick,
  big = false,
  disabled = false,
}: {
  options: SegOption<V>[]
  current: V
  countFn?: (v: V) => number
  onPick: (v: V) => void
  big?: boolean
  disabled?: boolean
}) {
  return (
    <div className={`segbtns${big ? ' big' : ''}${disabled ? ' disabled' : ''}`}>
      {options.map((o) => {
        const n = countFn ? countFn(o.v) : null
        const isActive = current === o.v
        const isZero = n === 0
        const cls = [isActive ? 'active' : '', isZero ? 'zero' : ''].filter(Boolean).join(' ')
        return (
          <button
            key={o.v}
            className={cls}
            onClick={() => {
              if (isZero) return
              onPick(o.v)
            }}
          >
            {o.l}
            {n !== null && <span className="cnt">{n}</span>}
          </button>
        )
      })}
    </div>
  )
}
