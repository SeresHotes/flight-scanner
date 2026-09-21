// Порт блока .legend из index.html.
export function Legend() {
  return (
    <div className="legend">
      Пересадки:
      <span className="tbadge direct">● прямой рейс</span>
      <span className="tbadge conn">✈ 1 пересадка</span>
      <span className="tbadge conn hi">✈ 2+ пересадок</span>
      <span style={{ marginLeft: 'auto' }}></span>
      <span
        className="tbadge"
        style={{
          background: 'rgba(240,180,41,.07)',
          border: '1px dashed rgba(240,180,41,.3)',
          color: 'var(--gold)',
        }}
      >
        🏙 длительная остановка в городе
      </span>
    </div>
  )
}
