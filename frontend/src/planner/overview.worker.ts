// Воркер режима «наборы городов»: расчёт обзора (overview.ts) вне главного потока.
// Новые фильтры прерывают текущий расчёт: каждые SLICE_MS он отдаёт управление,
// и если за это время пришёл запрос новее — бросается. Кэш движка (гистограммы
// по длине поездки) при этом не теряется, в отличие от worker.terminate().

import { OverviewEngine, prepareGraph, type OverviewResult } from './overview'
import type { PlanGraph, PlannerFilters } from './types'

export type OverviewRequest =
  | { type: 'graph'; graph: PlanGraph }
  | { type: 'compute'; id: number; filters: PlannerFilters }

export interface OverviewResponse {
  id: number
  result: OverviewResult
}

const SLICE_MS = 30

// В tsconfig — DOM-типы, а не WebWorker: описываем нужное из scope воркера сами.
const scope = self as unknown as {
  postMessage(message: OverviewResponse): void
  onmessage: ((e: MessageEvent<OverviewRequest>) => void) | null
}

let engine: OverviewEngine | null = null
let latest = 0

scope.onmessage = (e) => {
  const msg = e.data
  if (msg.type === 'graph') {
    engine = new OverviewEngine(prepareGraph(msg.graph))
    return
  }
  latest = msg.id
  if (engine) void compute(engine, msg.id, msg.filters)
}

const yieldToMessages = () => new Promise((resolve) => setTimeout(resolve, 0))

async function compute(eng: OverviewEngine, id: number, filters: PlannerFilters) {
  const steps = eng.steps(filters)
  let sliceStart = performance.now()
  for (;;) {
    const r = steps.next()
    if (r.done) {
      scope.postMessage({ id, result: r.value })
      return
    }
    if (performance.now() - sliceStart < SLICE_MS) continue
    await yieldToMessages()
    if (id !== latest || eng !== engine) return // пришли новые фильтры или граф
    sliceStart = performance.now()
  }
}
