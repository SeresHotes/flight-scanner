// Прогресс сбора (в v1 — мок; форма совпадает с реальным JobProgress).
export function CollectProgress({ progress, total }: { progress: number; total: number }) {
  const pct = total ? Math.round((100 * progress) / total) : 0
  return (
    <div className="backend-note">
      <div className="bn-title">⏳ Собираем данные по маршруту…</div>
      <div>
        Выполнено запросов: <b>{progress}</b> из <b>{total}</b>
      </div>
      <div className="progressbar">
        <div className="progressbar-fill" style={{ width: `${pct}%` }} />
      </div>
      <div style={{ marginTop: 8, fontSize: 12 }}>
        Демо-режим: данные фиктивные. Реальный сбор с Aviasales подключим отдельно.
      </div>
    </div>
  )
}
