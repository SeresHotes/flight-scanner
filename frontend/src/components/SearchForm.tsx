import { useEffect, useMemo, useState } from 'react'
import type { SearchParams } from '../data/searchClient'
import type { AvailableRoute } from '../data/routesApi'
import { DEFAULT_ORIGIN, DEFAULT_DESTINATION, type AirportOption } from '../data/airports'
import { AirportCombobox } from './AirportCombobox'
import { todayISO, addDaysISO, clampISO } from '../lib/dates'
import { fmtDate } from '../lib/format'

// Горизонт вылета: билеты обычно продают на ~11 месяцев вперёд.
const HORIZON_DAYS = 330

// Форма «что мы хотим посмотреть/скачать»: точки A/B (автокомплит) + диапазоны дат плеч.
// Отдаёт готовые SearchParams наверх; навигацию делает страница.
export function SearchForm({
  availableRoutes = [],
  onSubmit,
}: {
  availableRoutes?: AvailableRoute[]
  onSubmit: (params: SearchParams) => void
}) {
  const [origin, setOrigin] = useState<AirportOption>(DEFAULT_ORIGIN)
  const [destination, setDestination] = useState<AirportOption>(DEFAULT_DESTINATION)
  const [leg1From, setLeg1From] = useState('')
  const [leg1To, setLeg1To] = useState('')
  const [leg2From, setLeg2From] = useState('')
  const [leg2To, setLeg2To] = useState('')

  const today = todayISO()
  const maxDate = addDaysISO(today, HORIZON_DAYS)
  const clamp = (iso: string) => clampISO(iso, today, maxDate)

  // Собранный маршрут под текущие A/B (если есть) — из него берём границы дат и min_stay.
  const matched = useMemo(
    () =>
      availableRoutes.find(
        (r) => r.origin === origin.code && r.destination === destination.code,
      ),
    [availableRoutes, origin.code, destination.code],
  )

  // При смене маршрута — умный дефолт: собранный диапазон (если есть), иначе разумное
  // окно на будущее. Прошлые даты подтягиваем к сегодня (min = сегодня, не 25.10).
  useEffect(() => {
    if (matched) {
      setLeg1From(clamp(matched.dep_there_range[0]))
      setLeg1To(clamp(matched.dep_there_range[1]))
      setLeg2From(clamp(matched.dep_back_range[0]))
      setLeg2To(clamp(matched.dep_back_range[1]))
    } else {
      setLeg1From(addDaysISO(today, 14))
      setLeg1To(addDaysISO(today, 28))
      setLeg2From(addDaysISO(today, 35))
      setLeg2To(addDaysISO(today, 49))
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [matched])

  // Пресеты-чипсы для плеча: подставляют готовый диапазон в одно нажатие.
  function presetMonth(which: 1 | 2) {
    const base = which === 1 ? 14 : 35
    if (which === 1) {
      setLeg1From(addDaysISO(today, base))
      setLeg1To(addDaysISO(today, base + 30))
    } else {
      setLeg2From(addDaysISO(today, base))
      setLeg2To(addDaysISO(today, base + 30))
    }
  }
  function presetWiden(which: 1 | 2) {
    if (which === 1) {
      setLeg1From(clamp(addDaysISO(leg1From || today, -3)))
      setLeg1To(clamp(addDaysISO(leg1To || today, 3)))
    } else {
      setLeg2From(clamp(addDaysISO(leg2From || today, -3)))
      setLeg2To(clamp(addDaysISO(leg2To || today, 3)))
    }
  }
  function presetWhole() {
    if (!matched) return
    setLeg1From(clamp(matched.dep_there_range[0]))
    setLeg1To(clamp(matched.dep_there_range[1]))
    setLeg2From(clamp(matched.dep_back_range[0]))
    setLeg2To(clamp(matched.dep_back_range[1]))
  }

  function swap() {
    setOrigin(destination)
    setDestination(origin)
  }

  function submit(e: React.FormEvent) {
    e.preventDefault()
    onSubmit({
      origin: origin.code,
      destination: destination.code,
      leg1_dates: [leg1From, leg1To],
      leg2_dates: [leg2From, leg2To],
      min_stay: matched?.min_stay ?? 1,
    })
  }

  return (
    <form className="searchform" onSubmit={submit}>
      <div className="sfhead">
        <div className="sftitle">🔎 Куда и когда летим</div>
        <div className="sfhint">
          {matched
            ? `✅ Данные собраны${matched.collected_at ? ` (${fmtDate(matched.collected_at)})` : ''} — покажем сразу`
            : '📡 Данных пока нет — можно собрать по кнопке на результатах'}
        </div>
      </div>

      <div className="sfgrid">
        <AirportCombobox label="Откуда (A)" value={origin} onChange={setOrigin} />
        <button type="button" className="swap" onClick={swap} title="Поменять A и B местами">
          ⇄
        </button>
        <AirportCombobox label="Куда (B)" value={destination} onChange={setDestination} />
      </div>

      <div className="sflegs">
        <div className="sflegbox">
          <div className="lg">🛫 Плечо ТУДА — даты вылета</div>
          <div className="daterow">
            <input
              type="date"
              value={leg1From}
              min={today}
              max={maxDate}
              onChange={(e) => setLeg1From(e.target.value)}
            />
            <span>–</span>
            <input
              type="date"
              value={leg1To}
              min={today}
              max={maxDate}
              onChange={(e) => setLeg1To(e.target.value)}
            />
          </div>
          <div className="sfchips">
            {matched && (
              <button type="button" onClick={presetWhole}>весь диапазон</button>
            )}
            <button type="button" onClick={() => presetMonth(1)}>ближайший месяц</button>
            <button type="button" onClick={() => presetWiden(1)}>±3 дня</button>
          </div>
        </div>
        <div className="sflegbox">
          <div className="lg">🛬 Плечо ОБРАТНО — даты вылета</div>
          <div className="daterow">
            <input
              type="date"
              value={leg2From}
              min={leg1To || today}
              max={maxDate}
              onChange={(e) => setLeg2From(e.target.value)}
            />
            <span>–</span>
            <input
              type="date"
              value={leg2To}
              min={leg1To || today}
              max={maxDate}
              onChange={(e) => setLeg2To(e.target.value)}
            />
          </div>
          <div className="sfchips">
            {matched && (
              <button type="button" onClick={presetWhole}>весь диапазон</button>
            )}
            <button type="button" onClick={() => presetMonth(2)}>ближайший месяц</button>
            <button type="button" onClick={() => presetWiden(2)}>±3 дня</button>
          </div>
        </div>
      </div>

      <div className="sfactions">
        <button type="submit" className="btn-primary">
          Показать маршруты →
        </button>
      </div>
    </form>
  )
}
