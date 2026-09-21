// Пустое состояние: маршрут не собран и сбор не запущен (напр. не заданы даты плеч).
export function BackendNote({
  origin,
  destination,
  message,
}: {
  origin: string
  destination: string
  message?: string
}) {
  return (
    <div className="backend-note">
      <div className="bn-title">
        🔎 {origin} → {destination}
      </div>
      <div>{message ?? 'Данных по этому направлению пока нет.'}</div>
      <div style={{ marginTop: 8 }}>
        Укажите даты вылета обоих плеч на форме поиска — и мы соберём данные под запрос.
      </div>
    </div>
  )
}
