import { Fragment, type ReactNode } from 'react'
import { BrowserRouter, Routes, Route, Navigate, useParams } from 'react-router-dom'
import { PlanPage } from './pages/PlanPage'
import { CombosPage } from './pages/CombosPage'
import { RoutesPage } from './pages/RoutesPage'

// Планировщик v2: запрос (/) → наборы городов (/combos/:job) → маршруты (/routes/:job).
export default function App() {
  return (
    <BrowserRouter>
      <div className="wrap">
        <Routes>
          <Route path="/" element={<PlanPage />} />
          <Route path="/combos/:jobId" element={<PerJob><CombosPage /></PerJob>} />
          <Route path="/routes/:jobId" element={<PerJob><RoutesPage /></PerJob>} />
          <Route path="/planner" element={<Navigate to="/" replace />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </div>
    </BrowserRouter>
  )
}

// Правка запроса на странице результата уводит на новую джобу той же страницы
// (/routes/A → /routes/B): перемонтируем, чтобы запрос заново прочитался из URL.
function PerJob({ children }: { children: ReactNode }) {
  const { jobId } = useParams()
  return <Fragment key={jobId}>{children}</Fragment>
}
