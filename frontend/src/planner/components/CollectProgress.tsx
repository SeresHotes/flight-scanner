import { StageProgress } from '../../components/StageProgress'
import type { JobStage } from '../../data/jobsApi'

// Прогресс сбора цепочки: этап (очередь → загрузка → стыковка → сохранение),
// текущий переход и сколько его запросов уже сделано.
export function CollectProgress({
  progress,
  total,
  stage,
}: {
  progress: number
  total: number
  stage?: JobStage | null
}) {
  return (
    <StageProgress
      title="Собираем данные по маршруту…"
      progress={progress}
      total={total}
      stage={stage}
      stepWord="Переход"
    />
  )
}
