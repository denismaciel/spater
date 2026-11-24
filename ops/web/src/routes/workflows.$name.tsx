import { useQueryClient } from '@tanstack/react-query'
import { createFileRoute, Link, useNavigate } from '@tanstack/react-router'
import { Braces, GitFork, History, Play } from 'lucide-react'
import { useState, type FormEvent } from 'react'
import { $api } from '../api/client'
import type { components } from '../api/schema'
import { FlowGraph } from '../features/graph'
import {
  active,
  countLabel,
  ErrorMessage,
  JsonView,
  label,
  RunTable,
  Status,
  textParam,
} from '../features/shared'

const tabs = ['graph', 'definition', 'launch'] as const
const recentParams = (workflow: string) => ({ params: { query: { workflow, limit: 5 } } })
type View = { tab?: (typeof tabs)[number]; node?: string }
const validate = (s: Record<string, unknown>): View => ({
  tab: tabs.find((t) => t === s.tab),
  node: textParam(s.node),
})
export const Route = createFileRoute('/workflows/$name')({
  validateSearch: validate,
  loaderDeps: ({ search }) => ({ showHistory: !search.tab || search.tab === 'graph' }),
  loader: async ({ params: { name }, context, deps }) => {
    await Promise.all([
      context.queryClient.ensureQueryData(
        $api.queryOptions('get', '/api/v1/workflows/{name}', { params: { path: { name } } }),
      ),
      deps.showHistory &&
        context.queryClient.ensureQueryData($api.queryOptions('get', '/api/v1/runs', recentParams(name))),
    ])
  },
  head: ({ params }) => ({ meta: [{ title: `${params.name} · Spater` }] }),
  component: WorkflowDetail,
})

function RecentRuns({ name }: { name: string }) {
  const history = $api.useQuery('get', '/api/v1/runs', recentParams(name), { refetchInterval: 8000 })
  return (
    <section className="panel">
      <div className="section-head">
        <h2>Recent top-level runs</h2>
      </div>
      {history.error && <ErrorMessage error={history.error} />}
      {history.data &&
        (history.data.items.length ? (
          <RunTable rows={history.data.items} observedAt={history.dataUpdatedAt / 1000} />
        ) : (
          <p className="empty">No top-level runs yet.</p>
        ))}
    </section>
  )
}

function Launch({ detail }: { detail: components['schemas']['WorkflowDetail'] }) {
  const { workflow, example_input } = detail
  const example = JSON.stringify(example_input, null, 2) ?? 'null'
  const qc = useQueryClient()
  const draftKey = ['launch-input', workflow.name, workflow.version]
  const [raw, setRaw] = useState(() => qc.getQueryData<string>(draftKey) ?? example)
  const [syntaxError, setSyntaxError] = useState('')
  const launch = $api.useMutation('post', '/api/v1/workflows/{name}/launch')
  const navigate = useNavigate()
  function submit(e: FormEvent) {
    e.preventDefault()
    let input: unknown
    try {
      input = JSON.parse(raw)
    } catch (error) {
      setSyntaxError(error instanceof Error ? error.message : 'Enter valid JSON.')
      return
    }
    setSyntaxError('')
    launch.mutate(
      {
        params: { path: { name: workflow.name } },
        body: { input },
        // Send the validated text so integer literals retain their precision in Python.
        bodySerializer: () => `{"input":${raw}}`,
      },
      {
        onSuccess: async ({ run_id }) => {
          qc.setQueryData(draftKey, raw)
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
  if (!workflow.launchable)
    return (
      <div className="panel inspector">
        <h2>Launch unavailable</h2>
        <p>{workflow.launch_reason}</p>
      </div>
    )
  return (
    <form className="panel inspector action-panel" onSubmit={submit}>
      <div className="section-head">
        <h2>Launch workflow</h2>
        <span className="mono muted">{workflow.version}</span>
      </div>
      {Boolean(detail.input_fields?.length) && (
        <div className="field-guide">
          <h3>Input fields</h3>
          <dl>
            {detail.input_fields?.map((f, i) => (
              <div key={i}>
                <dt>{f.name}</dt>
                <dd>
                  {f.kind} · {f.required ? 'Required' : 'Optional'}
                </dd>
              </div>
            ))}
          </dl>
        </div>
      )}
      <p className="muted">{detail.example_note}</p>
      <label className="field" htmlFor="workflow-input">
        Input JSON <span className="muted">{workflow.input_type}</span>
      </label>
      <textarea
        id="workflow-input"
        className="code input-editor"
        spellCheck={false}
        value={raw}
        rows={10}
        aria-invalid={Boolean(syntaxError || launch.error)}
        aria-describedby={syntaxError || launch.error ? 'input-error' : undefined}
        onChange={(e) => {
          setRaw(e.target.value)
          setSyntaxError('')
          if (launch.error) launch.reset()
        }}
      />
      {(syntaxError || launch.error) && (
        <div id="input-error">
          <ErrorMessage error={syntaxError ? new Error(syntaxError) : launch.error} />
        </div>
      )}
      <div className="page-actions">
        <button className="button primary" type="submit" disabled={launch.isPending}>
          <Play size={15} />
          {launch.isPending ? 'Starting…' : 'Start run'}
        </button>
        <button
          className="button"
          type="button"
          onClick={() => {
            setRaw(example)
            setSyntaxError('')
            launch.reset()
          }}
          disabled={launch.isPending}
        >
          Reset example
        </button>
      </div>
    </form>
  )
}

function WorkflowDetail() {
  const { name } = Route.useParams()
  const s = Route.useSearch()
  const navigate = Route.useNavigate()
  const tab = s.tab ?? 'graph'
  const request = $api.useQuery(
    'get',
    '/api/v1/workflows/{name}',
    { params: { path: { name } } },
    {
      refetchInterval: (query) =>
        query.state.data?.workflow.latest && active(query.state.data.workflow.latest.status) ? 5000 : false,
    },
  )
  if (!request.data)
    return request.error ? (
      <section className="page">
        <ErrorMessage error={request.error} />
        <Link to="/workflows">Back to workflows</Link>
      </section>
    ) : null
  const { workflow, definition, graph, nodes } = request.data
  const selected =
    graph.nodes.find((node) => node.id === s.node) ??
    (!s.node ? graph.nodes.find((node) => nodes.some((config) => config.id === node.id)) : undefined)
  const config = nodes.find((n) => n.id === selected?.id)
  return (
    <section className="page">
      <header className="page-head">
        <div>
          <p className="eyebrow">Workflow definition</p>
          <h1>{name}</h1>
        </div>
        <div className="page-actions">
          <Link className="button" to="/runs" search={{ workflow: name }}>
            <History size={15} />
            Run history
          </Link>
          {workflow.launchable && (
            <Link
              className="button primary"
              to="/workflows/$name"
              params={{ name }}
              search={{ tab: 'launch' }}
            >
              <Play size={15} />
              Launch workflow
            </Link>
          )}
        </div>
      </header>
      <div className="page-meta">
        <div>
          <span>Input</span>
          <strong>{workflow.input_type}</strong>
        </div>
        <div>
          <span>Output</span>
          <strong>{workflow.output_type === 'None' ? 'No output' : workflow.output_type}</strong>
        </div>
        <div>
          <span>Definition version</span>
          <strong className="mono" title={workflow.version}>
            {workflow.version}
          </strong>
        </div>
        <div>
          <span>Latest run</span>
          {workflow.latest ? (
            <Link to="/runs/$id" params={{ id: workflow.latest.id }}>
              <Status value={workflow.latest.status} />
            </Link>
          ) : (
            <strong>No runs yet</strong>
          )}
        </div>
      </div>
      {workflow.launch_reason && <p className="availability">Launch unavailable. {workflow.launch_reason}</p>}
      <nav className="tabs" aria-label="Workflow views">
        {tabs
          .filter((t) => t !== 'launch' || workflow.launchable || tab === 'launch')
          .map((t) => {
            const Icon = { graph: GitFork, definition: Braces, launch: Play }[t]
            return (
              <Link
                key={t}
                to="/workflows/$name"
                params={{ name }}
                search={{ ...s, tab: !s.tab && t === 'graph' ? undefined : t }}
                activeOptions={{ exact: true }}
              >
                <Icon size={15} />
                {label(t)}
              </Link>
            )
          })}
      </nav>
      {request.error && <ErrorMessage error={request.error} />}
      {tab === 'graph' && (
        <div className="split">
          <section className="panel">
            <div className="section-head">
              <h2>Workflow graph</h2>
              <span className="muted">{countLabel(graph.nodes.length, 'node')}</span>
            </div>
            <FlowGraph
              graph={graph}
              selected={selected?.id}
              onSelect={(node) => void navigate({ search: { ...s, node } })}
            />
          </section>
          <aside className="panel inspector">
            {selected ? (
              <>
                <p className="eyebrow">{label(selected.kind)}</p>
                <h2>{selected.id}</h2>
                {selected.label !== selected.id && <p className="muted">{selected.label}</p>}
                <dl className="facts">
                  {config?.callable && (
                    <>
                      <dt>Function</dt>
                      <dd>
                        <details>
                          <summary>{config.callable.split(':').pop()}</summary>
                          <code>{config.callable}</code>
                        </details>
                      </dd>
                    </>
                  )}
                  <dt>Timeout</dt>
                  <dd>{config?.timeout ?? 'None'}</dd>
                  <dt>Retry</dt>
                  <dd>{config?.retry ?? 'None'}</dd>
                </dl>
              </>
            ) : (
              <>
                <h2>Node configuration</h2>
                <p>Select a node to inspect its configuration.</p>
              </>
            )}
          </aside>
        </div>
      )}
      {tab === 'definition' && (
        <section className="panel inspector">
          <h2>Definition JSON</h2>
          <JsonView value={definition} />
        </section>
      )}
      {tab === 'graph' && <RecentRuns name={name} />}
      {tab === 'launch' && <Launch key={workflow.name + workflow.version} detail={request.data} />}
    </section>
  )
}
