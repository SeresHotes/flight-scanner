import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { PlanPage } from './pages/PlanPage'
import { CombosPage } from './pages/CombosPage'
import { RoutesPage } from './pages/RoutesPage'
import { DynamicsPage } from './pages/DynamicsPage'

// Планировщик v2: запрос (/) → наборы городов (/combos/:job) → маршруты (/routes/:job).
// Отдельно — динамика цены направления по снимкам озера (/dynamics).
export default function App() {
  return (
    <BrowserRouter>
      <div className="wrap">
        <Routes>
          <Route path="/" element={<PlanPage />} />
          <Route path="/combos/:jobId" element={<CombosPage />} />
          <Route path="/routes/:jobId" element={<RoutesPage />} />
          <Route path="/dynamics" element={<DynamicsPage />} />
          <Route path="/planner" element={<Navigate to="/" replace />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </div>
    </BrowserRouter>
  )
}
