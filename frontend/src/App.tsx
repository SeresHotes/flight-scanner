import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { SearchPage } from './pages/SearchPage'
import { ResultsPage } from './pages/ResultsPage'
import { PlannerPage } from './pages/PlannerPage'

export default function App() {
  return (
    <BrowserRouter>
      <div className="wrap">
        <Routes>
          <Route path="/" element={<SearchPage />} />
          <Route path="/results" element={<ResultsPage />} />
          <Route path="/planner" element={<PlannerPage />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </div>
    </BrowserRouter>
  )
}
