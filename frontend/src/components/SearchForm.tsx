import { useMemo, useState } from 'react'
import type { SearchParams } from '../data/searchClient'
import type { AvailableRoute } from '../data/routesApi'
import { DEFAULT_ORIGIN, DEFAULT_DESTINATION, type AirportOption } from '../data/airports'
import { AirportCombobox } from './AirportCombobox'
import { DateRangePicker } from './DateRangePicker'
import { todayISO, addDaysISO, daysBetweenISO } from '../lib/dates'
import { fmtDate } from '../lib/format'

// Форма: точки A/B + диапазоны дат плеч. Выбор НЕ зависит от собранных данных —
// «готовое к просмотру» лишь подсвечивает кнопку «Показать», но не ограничивает ввод.
export function SearchForm({
  availableRoutes = [],
  onShow,
  onCollect,
}: {
  availableRoutes?: AvailableRoute[]
  onShow: (params: SearchParams) => void
  onCollect: (params: SearchParams) => void
}) {
  const today = todayISO()
  const [origin, setOrigin] = useState<AirportOption>(DEFAULT_ORIGIN)
  const [destination, setDestination] = useState<AirportOption>(DEFAULT_DESTINATION)
  // Дефолтное окно — независимо от данных (просто разумная поездка на будущее).
  const [leg1From, setLeg1From] = useState(addDaysISO(today, 14))
  const [leg1To, setLeg1To] = useState(addDaysISO(today, 21))
  const [leg2From, setLeg2From] = useState(addDaysISO(today, 35))
  const [leg2To, setLeg2To] = useState(addDaysISO(today, 42))

  // Собранный маршрут под текущие A/B — только для доступности «Показать» и свежести.
  const matched = useMemo(
    () => availableRoutes.find((r) => r.origin === origin.code && r.destination === destination.code),
    [availableRoutes, origin.code, destination.code],
  )

  const params = (): SearchParams => ({
    origin: origin.code,
    destination: destination.code,
    leg1_dates: [leg1From, leg1To],
    leg2_dates: [leg2From, leg2To],
    min_stay: matched?.min_stay ?? 1,
  })

  const datesValid =
    !!(leg1From && leg1To && leg2From && leg2To) &&
    leg1From <= leg1To &&
    leg2From <= leg2To

  // «Показать» доступно, только если выбранные даты уже покрыты собранными данными
  // (иначе — новые города или диапазон шире имеющегося → нечего показывать, надо собрать).
  const withinCoverage =
    !!matched &&
    leg1From >= matched.dep_there_range[0] &&
    leg1To <= matched.dep_there_range[1] &&
    leg2From >= matched.dep_back_range[0] &&
    leg2To <= matched.dep_back_range[1]

  const showEnabled = datesValid && withinCoverage
  const collectEnabled = datesValid

  const dataAge =
    matched?.collected_at != null ? daysBetweenISO(matched.collected_at.slice(0, 10), today) : null

  function swap() {
    setOrigin(destination)
    setDestination(origin)
  }

  return (
    <div className="searchform">
      <div className="sfhead">
        <div className="sftitle">🔎 Куда и когда летим</div>
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
          <DateRangePicker
            label="Вылет из A"
            from={leg1From}
            to={leg1To}
            min={today}
            onChange={(f, t) => {
              setLeg1From(f)
              setLeg1To(t)
            }}
          />
        </div>
        <div className="sflegbox">
          <div className="lg">🛬 Плечо ОБРАТНО — даты вылета</div>
          <DateRangePicker
            label="Вылет из B"
            from={leg2From}
            to={leg2To}
            min={leg1To || today}
            onChange={(f, t) => {
              setLeg2From(f)
              setLeg2To(t)
            }}
          />
        </div>
      </div>

      <div className="sfactions">
        <button
          type="button"
          className="btn-primary"
          disabled={!showEnabled}
          onClick={() => onShow(params())}
          title={showEnabled ? '' : 'Данных под этот выбор ещё нет — нажмите «Загрузить данные»'}
        >
          Показать маршруты →
        </button>
        <button
          type="button"
          className="btn-ghost"
          disabled={!collectEnabled}
          onClick={() => onCollect(params())}
          title="Скачать свежие данные с Aviasales для выбранных городов и дат"
        >
          ⤓ Загрузить данные
        </button>

        {showEnabled && (
          <span className="sf-fresh">
            данные от {fmtDate(matched!.collected_at)}
            {dataAge != null && dataAge > 0 ? ` · ${dataAge} дн. назад` : ''}
          </span>
        )}
        {!showEnabled && datesValid && (
          <span className="sf-fresh sf-fresh-warn">
            {matched ? 'выбранные даты шире собранных — нужно загрузить' : 'этот маршрут ещё не собран — нужно загрузить'}
          </span>
        )}
      </div>
    </div>
  )
}
