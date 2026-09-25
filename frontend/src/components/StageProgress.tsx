import type { JobStage } from '../data/jobsApi'

// Прогресс фонового сбора по этапам: степпер «очередь → загрузка → сборка →
// сохранение» и детали текущего этапа (какой переход грузим и сколько сделано).
// stepWord — как называть шаг загрузки («Переход» в планировщике, «Шаг» в поиске).
export function StageProgress({
  title,
  progress,
  total,
  stage,
  stepWord,
}: {
  title: string
  progress: number
  total: number
  stage?: JobStage | null
  stepWord: string
}) {
  const current = stage ? stage.stages.findIndex((s) => s.key === stage.key) : -1
  return (
    <div className="backend-note">
      <div className="bn-title">⏳ {title}</div>
      {stage && (
        <ol className="stage-list">
          {stage.stages.map((s, i) => {
            const state = i < current ? 'done' : i === current ? 'active' : 'todo'
            return (
              <li key={s.key} className={`stage stage-${state}`}>
                <span className="stage-mark">{state === 'done' ? '✓' : i + 1}</span>
                {s.label}
              </li>
            )
          })}
        </ol>
      )}
      {stage ? (
        <StageDetail stage={stage} progress={progress} total={total} stepWord={stepWord} />
      ) : (
        <RequestsBar progress={progress} total={total} />
      )}
    </div>
  )
}

function StageDetail({
  stage,
  progress,
  total,
  stepWord,
}: {
  stage: JobStage
  progress: number
  total: number
  stepWord: string
}) {
  if (stage.key === 'queued') {
    return <div>Ждём, пока сервер закончит предыдущий сбор…</div>
  }
  if (stage.key === 'fetch') {
    const step = stage.step
    return (
      <>
        {step && (
          <div className="stage-step">
            {stepWord} <b>{step.index + 1}</b> из <b>{step.count}</b>: <b>{step.label}</b>
            <div>
              запросов <b>{Math.min(step.done, step.total)}</b> из <b>{step.total}</b>
            </div>
            <Bar pct={pctOf(step.done, step.total)} small />
          </div>
        )}
        <RequestsBar progress={progress} total={total} cached={stage.cached} />
      </>
    )
  }
  const flights = stage.flights != null ? `Загружено рейсов: ${stage.flights}. ` : ''
  const what = stage.key === 'build' ? 'Собираем варианты из загруженных рейсов…' : 'Сохраняем котировки…'
  return (
    <>
      <div>
        {flights}
        {what}
      </div>
      <div className="progressbar">
        <div className="progressbar-fill progressbar-indeterminate" />
      </div>
    </>
  )
}

function RequestsBar({ progress, total, cached }: { progress: number; total: number; cached?: number }) {
  return (
    <>
      <div>
        Всего запросов: <b>{progress}</b> из <b>{total || '?'}</b>
        {cached ? <span className="stage-cached"> · из кэша: {cached}</span> : null}
      </div>
      <Bar pct={pctOf(progress, total)} />
    </>
  )
}

function Bar({ pct, small }: { pct: number; small?: boolean }) {
  return (
    <div className={small ? 'progressbar progressbar-small' : 'progressbar'}>
      <div className="progressbar-fill" style={{ width: `${pct}%` }} />
    </div>
  )
}

function pctOf(done: number, total: number): number {
  return total ? Math.min(100, Math.round((100 * done) / total)) : 0
}
