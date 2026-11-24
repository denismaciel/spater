import { Link } from '@tanstack/react-router'
import { ChartNoAxesColumn } from 'lucide-react'
import type { components } from './api/schema'
import { duration, label } from './features/shared'

type Run = components['schemas']['RunSummary']

export function RunHistory({ runs }: { runs: Run[] }) {
  const completed = runs.filter((run) => run.finished_at != null)
  const maxDuration = Math.max(0.001, ...completed.map((run) => run.finished_at! - run.started_at))
  const counts = new Map<string, number>()
  for (const run of runs) counts.set(run.status, (counts.get(run.status) ?? 0) + 1)
  return (
    <section className="activity-card" aria-label="Duration and status of runs on this page">
      <div className="activity-meta">
        <span className="chart-symbol">
          <ChartNoAxesColumn size={19} />
        </span>
        <div>
          <h2>Run duration</h2>
          <span>
            {runs.length} {runs.length === 1 ? 'run' : 'runs'} on this page
          </span>
        </div>
      </div>
      <div className="activity-plot">
        <div className="activity-bars">
          {runs.map((run) => {
            const elapsed = run.finished_at == null ? 0 : run.finished_at - run.started_at
            const title = [run.workflow, label(run.status), duration(run.started_at, run.finished_at)].join(
              ' · ',
            )
            return (
              <Link
                to="/runs/$id"
                key={run.id}
                params={{ id: run.id }}
                className={'activity-slot activity-' + run.status}
                title={title}
                aria-label={title}
              >
                <i style={{ height: `${Math.max(8, (elapsed / maxDuration) * 100)}%` }} />
              </Link>
            )
          })}
        </div>
        <div className="plot-axis">
          <span>Newest</span>
          <span>Oldest</span>
        </div>
      </div>
      <div className="activity-legend">
        {Array.from(counts, ([state, count]) => (
          <span className={'legend-' + state} key={state}>
            <i />
            {label(state)} <b>{count}</b>
          </span>
        ))}
      </div>
    </section>
  )
}
