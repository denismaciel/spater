import { createFileRoute, Link } from '@tanstack/react-router'
import { useQueryClient } from '@tanstack/react-query'
import { Braces, ChartNoAxesGantt, GitFork, Layers3, RotateCcw } from 'lucide-react'
import type { components } from '../api/schema'
import { useState } from 'react'
import { FailureSummary, RerunComparison } from '../features/run-insights'
import { $api } from '../api/client'
import { AttemptInspector, FlowGraph, Timing } from '../features/graph'
import {
  active,
  countLabel,
  Timestamp,
  CopyValue,
  duration,
  ErrorMessage,
  JsonView,
  label,
  PAGE_SIZE,
  Pager,
  positiveInt,
  RunLink,
  RunTable,
  shortId,
  Status,
  statuses,
  statusParam,
  textParam,
  type RunStatus,
  type Run,
} from '../features/shared'

const tabs = ['graph', 'timing', 'children', 'data', 'rerun'] as const
type Tab = (typeof tabs)[number]
type Selection = {
  tab?: Tab
  node?: string
  attempt?: number
  page?: number
  map?: string
  status?: RunStatus
}
const validate = (search: Record<string, unknown>): Selection => ({
  tab: tabs.find((tab) => tab === search.tab),
  node: textParam(search.node),
  attempt: positiveInt(search.attempt),
  page: positiveInt(search.page),
  map: textParam(search.map),
  status: statusParam(search.status),
})
const childParams = (id: string, s: Selection) => ({
  params: {
    path: { id },
    query: { node: s.map, status: s.status, offset: ((s.page ?? 1) - 1) * PAGE_SIZE, limit: PAGE_SIZE },
  },
})

export const Route = createFileRoute('/runs/$id')({
  validateSearch: validate,
  loaderDeps: ({ search: { tab, page, map, status } }) => ({ tab, page, map, status }),
  loader: async ({ context, params: { id }, deps }) => {
    const [detail] = await Promise.all([
      context.queryClient.ensureQueryData(
        $api.queryOptions('get', '/api/v1/runs/{id}', { params: { path: { id } } }),
      ),
      deps.tab === 'children' &&
        context.queryClient.ensureQueryData(
          $api.queryOptions('get', '/api/v1/runs/{id}/children', childParams(id, deps)),
        ),
    ])
    if (deps.tab === 'rerun') {
      await context.queryClient.prefetchQuery(
        $api.queryOptions('get', '/api/v1/workflows/{name}', {
          params: { path: { name: detail.run.workflow } },
        }),
      )
    }
    return {
      parent: detail.run.parent_id,
      parentNode: detail.run.parent_node,
      breadcrumb: `${detail.run.workflow} · ${shortId(id)}`,
    }
  },
  head: ({ loaderData }) => ({ meta: [{ title: `${loaderData?.breadcrumb ?? 'Run'} · Spater` }] }),
  component: RunDetail,
})

function RerunPanel({ run, saved }: { run: Run; saved: string }) {
  const { id, workflow } = run
  const navigate = Route.useNavigate()
  const qc = useQueryClient()
  const current = $api.useQuery('get', '/api/v1/workflows/{name}', { params: { path: { name: workflow } } })
  const rerun = $api.useMutation('post', '/api/v1/runs/{id}/rerun')
  const registered = current.data?.workflow
  const [raw, setRaw] = useState(saved)
  const [syntax, setSyntax] = useState('')
  const [page, setPage] = useState(1)
  const history = $api.useQuery(
    'get',
    '/api/v1/runs',
    { params: { query: { rerun_of: id, limit: PAGE_SIZE, offset: (page - 1) * PAGE_SIZE } } },
    { refetchOnMount: 'always', refetchInterval: 5000 },
  )
  const edited = raw !== saved
  function submit() {
    let body: components['schemas']['RerunRequest'] = {}
    if (edited) {
      try {
        body = { input: JSON.parse(raw) }
      } catch {
        setSyntax('Enter valid JSON before starting the rerun.')
        return
      }
    }
    rerun.mutate(
      { params: { path: { id } }, body, bodySerializer: () => (edited ? `{"input":${raw}}` : '{}') },
      {
        onSuccess: async ({ run_id }) => {
          await Promise.all([
            qc.invalidateQueries({ queryKey: ['get', '/api/v1/runs'], refetchType: 'none' }),
            qc.invalidateQueries({ queryKey: ['get', '/api/v1/workflows'], refetchType: 'none' }),
            qc.invalidateQueries({ queryKey: ['get', '/api/v1/workflows/{name}'], refetchType: 'none' }),
          ])
          await navigate({ to: '/runs/$id', params: { id: run_id }, search: {} })
        },
      },
    )
  }
  return (
    <section className="panel inspector action-panel">
      <h2>Rerun workflow</h2>
      <p>Every step runs again, including steps that send messages or update external systems.</p>
      <dl className="facts">
        <dt>Source run</dt>
        <dd>
          <RunLink id={id} full />
        </dd>
        <dt>Source version</dt>
        <dd className="mono">{run.version}</dd>
        <dt>Current version</dt>
        <dd className="mono">
          {registered?.launchable ? registered.version : current.isPending ? 'Loading…' : 'Unavailable'}
        </dd>
      </dl>
      {registered?.launchable && (
        <p>
          {registered.version === run.version
            ? 'Same definition as the source run.'
            : 'The registered definition has changed.'}{' '}
          {edited ? 'The input has been edited.' : 'Using the saved input.'}{' '}
          {registered.version === run.version &&
            !edited &&
            run.status === 'failed' &&
            'The same failure may recur.'}
        </p>
      )}
      <label className="field">
        Rerun input JSON
        <textarea
          className="code input-editor"
          rows={6}
          spellCheck={false}
          value={raw}
          onChange={(e) => {
            setRaw(e.target.value)
            setSyntax('')
            rerun.reset()
          }}
        />
      </label>
      {edited && (
        <button
          className="button"
          onClick={() => {
            setRaw(saved)
            setSyntax('')
            rerun.reset()
          }}
        >
          Restore saved input
        </button>
      )}
      {syntax && <p role="alert">{syntax}</p>}
      {current.error && <ErrorMessage error={current.error} />}
      {registered && !registered.launchable && (
        <p className="muted">No executable version is registered for this workflow.</p>
      )}
      {rerun.error && <ErrorMessage error={rerun.error} />}
      <button
        className="button primary"
        disabled={!registered?.launchable || rerun.isPending}
        onClick={submit}
      >
        <RotateCcw size={15} />
        {rerun.isPending ? 'Starting…' : 'Start rerun'}
      </button>
      <h3>Runs started from this run</h3>
      {history.error && <ErrorMessage error={history.error} />}
      {history.data &&
        (history.data.total ? (
          <>
            <RunTable rows={history.data.items} observedAt={history.dataUpdatedAt / 1000} />
            <Pager page={page} total={history.data.total} change={setPage} />
          </>
        ) : (
          <p>No reruns yet.</p>
        ))}
    </section>
  )
}

function RunDetail() {
  const { id } = Route.useParams()
  const s = Route.useSearch()
  const navigate = Route.useNavigate()
  const tab = s.tab ?? 'graph'
  const request = $api.useQuery(
    'get',
    '/api/v1/runs/{id}',
    { params: { path: { id } } },
    { refetchInterval: (query) => (query.state.data && active(query.state.data.run.status) ? 2000 : false) },
  )
  const children = $api.useQuery('get', '/api/v1/runs/{id}/children', childParams(id, s), {
    enabled: tab === 'children',
    refetchInterval: tab === 'children' && request.data && active(request.data.run.status) ? 2000 : false,
  })
  if (!request.data)
    return request.error ? (
      <section className="page">
        <ErrorMessage error={request.error} />
        <Link to="/runs">Back to runs</Link>
      </section>
    ) : null
  const { run, graph, attempts, input, result } = request.data
  const latestAttempt = attempts.reduce<(typeof attempts)[number] | undefined>(
    (latest, item) => (!latest || (item.started_at ?? 0) > (latest.started_at ?? 0) ? item : latest),
    undefined,
  )
  const selectedNodeId = s.node ?? run.node_id ?? latestAttempt?.node_id
  const node = graph.nodes.find((n) => n.id === selectedNodeId)
  const nodeAttempts = attempts.filter((a) => a.node_id === selectedNodeId)
  const attempt = s.attempt
    ? nodeAttempts.find((a) => a.attempt === s.attempt)
    : nodeAttempts.reduce<(typeof nodeAttempts)[number] | undefined>(
        (best, a) => (!best || best.attempt < a.attempt ? a : best),
        undefined,
      )
  const maps = graph.nodes.filter((n) => n.kind === 'map')
  function update(change: Selection) {
    void navigate({ search: { ...s, ...change } })
  }
  const sectionTabs = [
    { name: 'graph', title: 'Graph', icon: GitFork },
    { name: 'timing', title: 'Timing', icon: ChartNoAxesGantt },
    ...(maps.length || tab === 'children'
      ? [{ name: 'children', title: 'Mapped children', icon: Layers3 }]
      : []),
    { name: 'data', title: 'Input and result', icon: Braces },
    { name: 'rerun', title: 'Rerun', icon: RotateCcw },
  ] as const
  return (
    <section className="page">
      <header className="page-head">
        <div>
          <p className="eyebrow">
            Run{' '}
            <span className="mono" title={id}>
              {shortId(id)}
            </span>
          </p>
          <h1>{run.workflow}</h1>
        </div>
        <div className="page-actions">
          <Status value={run.status} />
        </div>
      </header>
      <div className="page-meta run-meta">
        <div>
          <span>Run ID</span>
          <CopyValue key={id} name="run ID" value={id} />
        </div>
        <div>
          <span>Started</span>
          <strong>
            <Timestamp time={run.started_at} />
          </strong>
        </div>
        <div>
          <span>Duration</span>
          <strong>{duration(run.started_at, run.finished_at ?? request.dataUpdatedAt / 1000)}</strong>
        </div>
        <div>
          <span>Definition version</span>
          <CopyValue key={run.version} name="definition version" value={run.version} />
        </div>
        <div>
          <span>Workflow</span>
          <Link to="/workflows/$name" params={{ name: run.workflow }}>
            {run.workflow}
          </Link>
        </div>
        {run.wake_at != null && (
          <div>
            <span>Next wake</span>
            <strong>
              <Timestamp time={run.wake_at} />
            </strong>
          </div>
        )}
        {run.rerun_of && (
          <div>
            <span>Rerun of</span>
            <RunLink id={run.rerun_of} />
          </div>
        )}
        {run.parent_id && (
          <div>
            <span>Parent run</span>
            <Link
              className="run-link mono"
              to="/runs/$id"
              params={{ id: run.parent_id }}
              search={{ tab: 'children', map: run.parent_node ?? undefined }}
            >
              {shortId(run.parent_id)}
            </Link>
            <small>
              {run.parent_node}
              {run.item_index != null ? ` · item ${run.item_index}` : ''}
            </small>
          </div>
        )}
      </div>
      <FailureSummary detail={request.data} />
      <RerunComparison detail={request.data} />
      <nav className="tabs" aria-label="Run views">
        {sectionTabs.map((item) => (
          <Link
            key={item.name}
            to="/runs/$id"
            params={{ id }}
            search={{
              ...s,
              tab: !s.tab && item.name === 'graph' ? undefined : tabs.find((t) => t === item.name),
            }}
            activeOptions={{ exact: true }}
          >
            <item.icon size={16} />
            {item.title}
          </Link>
        ))}
      </nav>
      {request.error && <ErrorMessage error={request.error} />}
      {(tab === 'graph' || tab === 'timing') && (
        <div className="split">
          <section className="panel">
            <div className="section-head">
              <h2>{tab === 'graph' ? 'Execution graph' : 'Attempt timing'}</h2>
              <span className="muted">
                {countLabel(new Set(attempts.map((a) => a.node_id)).size, 'step') +
                  ' ran · ' +
                  countLabel(attempts.filter((a) => a.attempt > 1).length, 'retry', 'retries')}
              </span>
            </div>
            {tab === 'graph' ? (
              <FlowGraph
                graph={graph}
                runStatus={run.status}
                attempts={attempts}
                selected={selectedNodeId}
                onSelect={(node) => update({ node, attempt: undefined })}
              />
            ) : (
              <Timing
                attempts={attempts}
                selected={attempt}
                observedAt={request.dataUpdatedAt / 1000}
                choose={(a) => update({ node: a.node_id, attempt: a.attempt })}
              />
            )}
          </section>
          <div className="inspector-column">
            <AttemptInspector
              node={node}
              attempts={attempts}
              selected={attempt}
              choose={(attempt) => update({ attempt })}
            />
            {node?.kind === 'map' && (
              <Link
                className="button"
                to="/runs/$id"
                params={{ id }}
                search={{ tab: 'children', map: node.id }}
              >
                View mapped children
              </Link>
            )}
          </div>
        </div>
      )}
      {tab === 'children' && (
        <section className="panel">
          <div className="section-head">
            <h2>Mapped children</h2>
            {children.data && (
              <span className="muted">{countLabel(children.data.total, 'child', 'children')}</span>
            )}
          </div>
          <div className="toolbar filters">
            <select
              aria-label="Map node"
              value={s.map ?? ''}
              onChange={(e) => update({ map: textParam(e.target.value), page: undefined })}
            >
              <option value="">All map nodes</option>
              {maps.map((n) => (
                <option key={n.id}>{n.id}</option>
              ))}
            </select>
            <select
              aria-label="Child status"
              value={s.status ?? ''}
              onChange={(e) => update({ status: statusParam(e.target.value), page: undefined })}
            >
              <option value="">All statuses</option>
              {statuses.map((value) => (
                <option key={value} value={value}>
                  {label(value)}
                </option>
              ))}
            </select>
          </div>
          {children.error && <ErrorMessage error={children.error} />}
          {children.data && (
            <>
              <RunTable rows={children.data.items} mapped observedAt={children.dataUpdatedAt / 1000} />
              <Pager page={s.page ?? 1} total={children.data.total} change={(page) => update({ page })} />
            </>
          )}
        </section>
      )}
      {tab === 'data' && (
        <div className="split payloads">
          <section className="panel inspector">
            <h2>Input</h2>
            <JsonView value={input} />
          </section>
          <section className="panel inspector">
            <h2>Result</h2>
            {run.status === 'succeeded' ? (
              <JsonView value={result} />
            ) : (
              <p className="muted">
                {active(run.status) ? 'The run is still in progress.' : 'No result was produced.'}
              </p>
            )}
          </section>
        </div>
      )}
      {tab === 'rerun' && (
        <RerunPanel key={id} run={run} saved={request.data.input_json ?? JSON.stringify(input, null, 2)} />
      )}
    </section>
  )
}
