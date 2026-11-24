import { createFileRoute, Link } from '@tanstack/react-router'
import { ChevronRight, Play, Search } from 'lucide-react'
import { $api } from '../api/client'
import {
  active,
  countLabel,
  Timestamp,
  ErrorMessage,
  PAGE_SIZE,
  Pager,
  positiveInt,
  Status,
  textParam,
} from '../features/shared'

type Filters = { q?: string; page?: number }
const validate = (s: Record<string, unknown>): Filters => ({ q: textParam(s.q), page: positiveInt(s.page) })
export const Route = createFileRoute('/workflows/')({
  validateSearch: validate,
  head: () => ({ meta: [{ title: 'Workflows · Spater' }] }),
  loader: async ({ context }) => {
    await context.queryClient.ensureQueryData($api.queryOptions('get', '/api/v1/workflows'))
  },
  component: Workflows,
})

function Workflows() {
  const { q, page = 1 } = Route.useSearch()
  const navigate = Route.useNavigate()
  const query = $api.useQuery('get', '/api/v1/workflows', undefined, {
    refetchInterval: (query) =>
      query.state.data?.items.some((w) => w.latest && active(w.latest.status)) ? 5000 : false,
  })
  const workflows =
    query.data?.items.filter(
      (w) =>
        !q ||
        [w.name, w.input_type, w.output_type].some((text) =>
          text.toLocaleLowerCase().includes(q.toLocaleLowerCase()),
        ),
    ) ?? []
  const shown = workflows.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE)
  return (
    <section className="page">
      <header className="page-head">
        <h1>Workflows</h1>
      </header>
      <section className="panel">
        <div className="toolbar">
          <form
            role="search"
            className="search-field"
            key={q ?? ''}
            onSubmit={(e) => {
              e.preventDefault()
              void navigate({ search: { q: textParam(new FormData(e.currentTarget).get('q')) } })
            }}
          >
            <Search size={16} />
            <input
              type="search"
              name="q"
              aria-label="Search workflows"
              placeholder="Search workflows or types…"
              defaultValue={q}
              onChange={(e) => {
                if (!e.target.value && q) void navigate({ search: {} })
              }}
            />
            <button type="submit" className="icon-link" aria-label="Apply search">
              ↵
            </button>
          </form>
        </div>
        <div className="section-head">
          <h2>Registered workflows</h2>
          <span className="muted">{countLabel(workflows.length, 'workflow')}</span>
        </div>
        {query.error && <ErrorMessage error={query.error} />}
        {shown.length ? (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Workflow / version</th>
                  <th>Input → output</th>
                  <th>Latest run</th>
                  <th>
                    <span className="sr-only">Actions</span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {shown.map((w) => (
                  <tr key={w.name}>
                    <td>
                      <Link to="/workflows/$name" params={{ name: w.name }} className="table-title">
                        {w.name}
                      </Link>
                      <div className="mono muted" title={w.version}>
                        {w.version}
                      </div>
                    </td>
                    <td className="type-cell">
                      {w.input_type} <span className="muted">→</span>{' '}
                      {w.output_type === 'None' ? 'No output' : w.output_type}
                    </td>
                    <td>
                      {w.latest ? (
                        <Link className="latest-run" to="/runs/$id" params={{ id: w.latest.id }}>
                          <Status value={w.latest.status} />
                          <small className="muted">
                            <Timestamp time={w.latest.started_at} />
                          </small>
                        </Link>
                      ) : (
                        <span className="muted">No runs yet</span>
                      )}
                    </td>
                    <td>
                      {w.launchable ? (
                        <Link
                          className="button"
                          to="/workflows/$name"
                          params={{ name: w.name }}
                          search={{ tab: 'launch' }}
                        >
                          <Play size={14} />
                          Launch
                        </Link>
                      ) : (
                        <Link className="button" to="/workflows/$name" params={{ name: w.name }}>
                          Inspect <ChevronRight size={14} />
                        </Link>
                      )}
                      {w.launch_reason && <small className="availability">{w.launch_reason}</small>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="empty">{q ? 'No workflows match this search.' : 'No workflows are registered.'}</p>
        )}
        <Pager
          page={page}
          total={workflows.length}
          change={(page) => void navigate({ search: { q, page } })}
        />
      </section>
    </section>
  )
}
