import { useEffect, useMemo, useState } from 'react'
import type { SearchParams } from '../data/searchClient'
import type { AvailableRoute } from '../data/routesApi'
import { DEFAULT_ORIGIN, DEFAULT_DESTINATION, type AirportOption } from '../data/airports'
import { AirportCombobox } from './AirportCombobox'

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

  // Собранный маршрут под текущие A/B (если есть) — из него берём границы дат и min_stay.
  const matched = useMemo(
    () =>
      availableRoutes.find(
        (r) => r.origin === origin.code && r.destination === destination.code,
      ),
    [availableRoutes, origin.code, destination.code],
  )

  // При смене маршрута подставляем его диапазоны дат и минимум пребывания.
  useEffect(() => {
    if (matched) {
      setLeg1From(matched.dep_there_range[0])
      setLeg1To(matched.dep_there_range[1])
      setLeg2From(matched.dep_back_range[0])
      setLeg2To(matched.dep_back_range[1])
    } else {
      setLeg1From('')
      setLeg1To('')
      setLeg2From('')
      setLeg2To('')
    }
  }, [matched])

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

  const depThere = matched?.dep_there_range
  const depBack = matched?.dep_back_range

  return (
    <form className="searchform" onSubmit={submit}>
      <div className="sfhead">
        <div className="sftitle">🔎 Куда и когда летим</div>
        <div className="sfhint">
          {matched ? 'Данные по маршруту собраны — покажем сразу' : 'Данных пока нет — бэкенд догрузит'}
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
              min={depThere?.[0]}
              max={depThere?.[1]}
              onChange={(e) => setLeg1From(e.target.value)}
            />
            <span>–</span>
            <input
              type="date"
              value={leg1To}
              min={depThere?.[0]}
              max={depThere?.[1]}
              onChange={(e) => setLeg1To(e.target.value)}
            />
          </div>
        </div>
        <div className="sflegbox">
          <div className="lg">🛬 Плечо ОБРАТНО — даты вылета</div>
          <div className="daterow">
            <input
              type="date"
              value={leg2From}
              min={depBack?.[0]}
              max={depBack?.[1]}
              onChange={(e) => setLeg2From(e.target.value)}
            />
            <span>–</span>
            <input
              type="date"
              value={leg2To}
              min={depBack?.[0]}
              max={depBack?.[1]}
              onChange={(e) => setLeg2To(e.target.value)}
            />
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
