import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Один origin в проде обеспечивает Caddy; в деве проксируем /api на будущий FastAPI.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
  // Тот же прокси для `npm run preview` (иначе /api уходит на статик-сервер и падает).
  preview: {
    port: 4173,
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
})
