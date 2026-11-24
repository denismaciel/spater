import { createFileRoute, Link } from '@tanstack/react-router'
import { CalendarDays, ListFilter, RefreshCw, Search } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'
import { RunHistory } from '../RunHistory'
import { $api } from '../api/client'
import {
  countLabel,
  ErrorMessage,
  label,
  PAGE_SIZE,
  Pager,
  positiveInt,
  RunTable,
  statuses,
  statusParam,
  textParam,
  type RunStatus,
} from '../features/shared'

type SearchState = {
  q?: string
  workflow?: string
  status?: RunStatus
  since?: number
  until?: number
  page?: number
}

const epoch = (value: unknown) =>
  typeof value === 'number' && Number.isFinite(value) && value >= 0 && value < 253402300800
    ? value
    : undefined
const validate = (raw: Record<string, unknown>): SearchState => ({
  q: textParam(raw.q),
  workflow: textParam(raw.workflow),
  status: statusParam(raw.status),
  since: epoch(raw.since),
  until: epoch(raw.until),
  page: positiveInt(raw.page),
})
const runOptions = ({ q, workflow, status, since, until, page = 1 }: SearchState) =>
  $api.queryOptions('get', '/api/v1/runs', {
    params: {
      query: { q, workflow, status, since, until, limit: PAGE_SIZE, offset: (page - 1) * PAGE_SIZE },
    },
  })
const workflowOptions = $api.queryOptions('get', '/api/v1/workflows')
const filterDate = new Intl.DateTimeFormat('en-GB', {
  day: 'numeric',
  month: 'short',
  year: 'numeric',
  timeZone: 'UTC',
})

function rangeLabel(since?: number, until?: number) {
  if (since !== undefined && until !== undefined && since <= until) {
    return filterDate.formatRange(since * 1000, until * 1000)
  }
  if (since !== undefined) return `From ${filterDate.format(since * 1000)}`
  if (until !== undefined) return `Through ${filterDate.format(until * 1000)}`
  return 'All time'
}

export const Route = createFileRoute('/runs/')({
  head: () => ({ meta: [{ title: 'Runs · Spater' }] }),
  validateSearch: validate,
  loaderDeps: ({ search }) => search,
  loader: async ({ context, deps }) => {
    await Promise.all([
      context.queryClient.ensureQueryData(runOptions(deps)),
      context.queryClient.ensureQueryData(workflowOptions),
    ])
  },
  component: Runs,
})

function Runs() {
  const search = Route.useSearch()
  const navigate = Route.useNavigate()
  const { q, status, workflow, since, until, page = 1 } = search
  const runs = $api.useQuery(
    'get',
    '/api/v1/runs',
    {
      params: {
        query: { q, status, workflow, since, until, offset: (page - 1) * PAGE_SIZE, limit: PAGE_SIZE },
      },
    },
    { refetchInterval: 8000 },
  )
  const workflows = $api.useQuery('get', '/api/v1/workflows')
  const dateFilter = useRef<HTMLDetailsElement>(null)
  const [isOpen, setOpen] = useState(false)
  useEffect(() => {
    if (!isOpen) return
    const dismiss = (event: PointerEvent) => {
      const filter = dateFilter.current
      if (filter && event.target instanceof Node && !filter.contains(event.target)) filter.open = false
    }
    const escape = (event: KeyboardEvent) => {
      const filter = dateFilter.current
      if (event.key === 'Escape' && filter?.open) {
        filter.open = false
        filter.querySelector('summary')?.focus()
      }
    }
    document.addEventListener('pointerdown', dismiss)
    document.addEventListener('keydown', escape)
    return () => {
      document.removeEventListener('pointerdown', dismiss)
      document.removeEventListener('keydown', escape)
    }
  }, [isOpen])
  function update(changes: SearchState) {
    void navigate({ search: { ...search, page: undefined, ...changes } })
  }
  const dayValue = (value?: number) =>
    value === undefined ? '' : new Date(value * 1000).toISOString().slice(0, 10)
  return (
    <section className="page">
      <header className="page-head">
        <div>
          <h1>Runs</h1>
        </div>
        <div className="page-actions">
          <button
            type="button"
            className="button"
            onClick={() => void runs.refetch()}
            disabled={runs.isFetching}
          >
            <RefreshCw size={15} />
            Refresh
          </button>
          <Link className="button primary" to="/workflows">
            Launch workflow
          </Link>
        </div>
      </header>

      <section className="panel" aria-label="Run history">
        <div className="toolbar filters">
          <form
            role="search"
            className="search-field"
            key={q ?? ''}
            onSubmit={(event) => {
              event.preventDefault()
              update({ q: textParam(new FormData(event.currentTarget).get('q')) })
            }}
          >
            <Search size={16} aria-hidden="true" />
            <input
              type="search"
              name="q"
              aria-label="Search runs"
              placeholder="Search runs…"
              defaultValue={q ?? ''}
              onChange={(event) => {
                if (!event.target.value && q) update({ q: undefined })
              }}
            />
            <button className="icon-link" type="submit" aria-label="Apply search">
              ↵
            </button>
          </form>
          <label className="select-field">
            <ListFilter size={15} aria-hidden="true" />
            <select
              aria-label="Workflow filter"
              value={workflow ?? ''}
              onChange={(event) => update({ workflow: textParam(event.target.value) })}
            >
              <option value="">All workflows</option>
              {workflow && !workflows.data?.items.some((item) => item.name === workflow) && (
                <option>{workflow}</option>
              )}
              {workflows.data?.items.map((item) => (
                <option key={item.name}>{item.name}</option>
              ))}
            </select>
          </label>
          <label className="select-field">
            <select
              aria-label="Status filter"
              value={status ?? ''}
              onChange={(event) => update({ status: statusParam(event.target.value) })}
            >
              <option value="">All statuses</option>
              {statuses.map((s) => (
                <option key={s} value={s}>
                  {label(s)}
                </option>
              ))}
            </select>
          </label>
          <details
            className="date-filter"
            ref={dateFilter}
            onToggle={(event) => setOpen(event.currentTarget.open)}
          >
            <summary className="button">
              <CalendarDays size={15} />
              {rangeLabel(since, until)}
            </summary>
            <div className="date-fields">
              <label>
                From (UTC)
                <input
                  type="date"
                  max={dayValue(until)}
                  value={dayValue(since)}
                  onChange={(event) => update({ since: epoch(Date.parse(event.target.value) / 1000) })}
                />
              </label>
              <label>
                Through (UTC)
                <input
                  type="date"
                  min={dayValue(since)}
                  value={dayValue(until)}
                  onChange={(event) =>
                    update({ until: epoch(Date.parse(event.target.value) / 1000 + 86400 - 0.000001) })
                  }
                />
              </label>
              <button
                className="button"
                type="button"
                onClick={() => update({ since: undefined, until: undefined })}
              >
                Clear dates
              </button>
            </div>
          </details>
        </div>
        <div className="section-head">
          <div>
            <h2>Run history</h2>
            <small className="muted">Top-level runs. Open a mapped run to see its children.</small>
          </div>
          {runs.data && <span className="muted">{countLabel(runs.data.total, 'run')}</span>}
        </div>
        {runs.error && <ErrorMessage error={runs.error} />}
        {runs.data && (
          <>
            <RunTable rows={runs.data.items} observedAt={runs.dataUpdatedAt / 1000} />
            <Pager total={runs.data.total} page={page} change={(page) => update({ page })} />
          </>
        )}
      </section>
      {runs.data && runs.data.items.length > 0 && (
        <details className="duration-disclosure">
          <summary>Run duration chart</summary>
          <RunHistory runs={runs.data.items} />
        </details>
      )}
    </section>
  )
}
