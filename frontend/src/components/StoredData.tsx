import type { AvailableRoute, Collection, StoredCoverage } from '../data/routesApi'
import { fmtDate } from '../lib/format'

// Реальный запрос сбора: origin/destination = null → «любой» (все направления).
function QueryRoute({ c }: { c: Collection }) {
  const anyA = c.origin === null
  const anyB = c.destination === null
  return (
    <>
      <span className={anyA ? 'q-any' : ''}>{c.origin ?? 'любой'}</span>
      {' → '}
      <span className={anyB ? 'q-any' : ''}>{c.destination ?? 'любой'}</span>
    </>
  )
}

// Панель «какие данные у нас уже хранятся»: готовые маршруты + агрегат по хранилищу.
export function StoredData({
  available,
  stored,
  onPick,
}: {
  available: AvailableRoute[]
  stored: StoredCoverage
  onPick: (origin: string, destination: string) => void
}) {
  return (
    <div className="stored">
      <div className="stored-block">
        <div className="stored-h">✅ Готово к просмотру</div>
        {available.length ? (
          <div className="ready-cards">
            {available.map((r) => (
              <button
                key={`${r.origin}-${r.destination}`}
                className="ready-card"
                onClick={() => onPick(r.origin, r.destination)}
              >
                <div className="rc-route">
                  {r.origin} → {r.destination}
                  <span className="rc-region">
                    {r.region_flag} {r.region_label}
                  </span>
                </div>
                <div className="rc-meta">
                  <span>
                    <b>{r.trips.toLocaleString('ru-RU')}</b> плеч
                  </span>
                  <span>
                    от <b>{(r.there_price_range[0] + r.back_price_range[0]).toLocaleString('ru-RU')} ₽</b>
                  </span>
                </div>
                <div className="rc-dates">
                  туда {r.dep_there_range[0]} … {r.dep_there_range[1]} · обратно {r.dep_back_range[0]} …{' '}
                  {r.dep_back_range[1]}
                </div>
                {r.collected_at && <div className="rc-collected">🗓 собрано {fmtDate(r.collected_at)}</div>}
              </button>
            ))}
          </div>
        ) : (
          <div className="stored-empty">Пока нет собранных маршрутов</div>
        )}
      </div>

      <div className="stored-block">
        <div className="stored-h">
          🗄 Что собирали · <b>{stored.total_quotes.toLocaleString('ru-RU')}</b> котировок в хранилище
        </div>
        <div className="stored-sub">
          Реальные запросы к API (сбор «с остановкой» — это два запроса: <b>X → любой</b> и{' '}
          <b>любой → Y</b>). «любой» = все направления.
        </div>
        <div className="coll-table-wrap">
          <table className="coll-table">
            <thead>
              <tr>
                <th>Тип</th>
                <th>Направление</th>
                <th>Вылеты</th>
                <th>Собрано</th>
                <th className="num">Записей</th>
              </tr>
            </thead>
            <tbody>
              {stored.collections.map((c, i) => (
                <tr key={`${c.origin}-${c.destination}-${c.direct}-${i}`}>
                  <td>
                    <span className={`coll-type ${c.direct ? 'dir' : 'ind'}`}>
                      {c.direct ? 'Прямые' : 'С пересадками'}
                    </span>
                  </td>
                  <td className="coll-route">
                    <QueryRoute c={c} />
                  </td>
                  <td className="coll-dates">
                    {c.dep_from} … {c.dep_to}
                  </td>
                  <td className="coll-collected">{fmtDate(c.collected_at)}</td>
                  <td className="num">{c.flights.toLocaleString('ru-RU')}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
