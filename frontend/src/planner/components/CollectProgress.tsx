import { useEffect, useRef, useState } from 'react'
import { StageProgress } from '../../components/StageProgress'
import type { JobStage } from '../../data/jobsApi'
import { rescueJobs } from '../api'

// Сколько прогресс может стоять на месте, прежде чем предложить «Починить».
// Совпадает с HUNG_JOB_SECONDS на бэке: раньше сервер всё равно ничего не сбросит.
const STALL_MS = 60_000
const STALL_CHECK_MS = 5_000

// Прогресс сбора цепочки: этап (очередь → загрузка → стыковка → сохранение),
// текущий переход и сколько его запросов уже сделано. Если прогресс застыл —
// кнопка «Починить» сбрасывает зависший на сервере сбор.
export function CollectProgress({
  progress,
  total,
  stage,
}: {
  progress: number
  total: number
  stage?: JobStage | null
}) {
  const stalled = useStalled(
    `${progress}|${stage?.key}|${stage?.step?.done}|${stage?.cached}|${stage?.build?.explored}`,
  )
  return (
    <>
      <StageProgress
        title="Собираем данные по маршруту…"
        progress={progress}
        total={total}
        stage={stage}
        stepWord="Переход"
      />
      {stalled && <RescueBar />}
    </>
  )
}

// true, если signature не менялась дольше STALL_MS.
function useStalled(signature: string): boolean {
  const changedAt = useRef(Date.now())
  const [stalled, setStalled] = useState(false)

  useEffect(() => {
    changedAt.current = Date.now()
    setStalled(false)
  }, [signature])

  useEffect(() => {
    const timer = setInterval(() => setStalled(Date.now() - changedAt.current > STALL_MS), STALL_CHECK_MS)
    return () => clearInterval(timer)
  }, [])

  return stalled
}

function RescueBar() {
  const [busy, setBusy] = useState(false)
  const [note, setNote] = useState<string | null>(null)

  async function rescue() {
    setBusy(true)
    try {
      const rescued = await rescueJobs()
      setNote(
        rescued.length
          ? 'Зависший сбор сброшен — очередь идёт дальше.'
          : 'Зависших сборов не нашлось: сервер ещё работает, подождите немного.',
      )
    } catch {
      setNote('Не удалось связаться с сервером.')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="freshbar">
      <span className="fb-info">Прогресс не меняется больше минуты — похоже, сбор завис.</span>
      <button className="btn-ghost" onClick={rescue} disabled={busy}>
        {busy ? 'Чиню…' : '🛠 Починить'}
      </button>
      {note && <span className="fb-info fb-note">{note}</span>}
    </div>
  )
}
