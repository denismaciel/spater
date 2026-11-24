import { Link } from '@tanstack/react-router'
import { CircleAlert } from 'lucide-react'
import type { components } from '../api/schema'
import { branchName, JsonView, RunLink, Status, type Attempt, type Graph } from './shared'

type Detail = components['schemas']['RunDetail']

function precedingDecisions(graph: Graph, attempts: Attempt[], failed: string) {
  const latest = new Map<string, Attempt>()
  for (const a of attempts) if ((latest.get(a.node_id)?.attempt ?? 0) < a.attempt) latest.set(a.node_id, a)
  const found = new Map<string, Attempt>()
  const visited = new Set([failed])
  for (const id of visited) {
    for (const edge of graph.edges) {
      if (edge.dst !== id) continue
      const prior = latest.get(edge.src)
      if (edge.label) {
        if (prior?.status === 'succeeded' && prior.arm && branchName(prior.arm) === edge.label)
          found.set(edge.src, prior)
      } else if (
        prior?.status === 'succeeded' ||
        (!prior && graph.nodes.some((n) => n.id === edge.src && n.kind === 'join'))
      ) {
        visited.add(edge.src)
      }
    }
  }
  return [...found.values()]
}

export function FailureSummary({ detail }: { detail: Detail }) {
  const { run, input, graph, attempts } = detail
  if (!run.error) return null
  const node = graph.nodes.find((n) => n.id === run.node_id)
  const decisions = node ? precedingDecisions(graph, attempts, node.id) : []
  const title =
    node?.kind === 'fail'
      ? 'Workflow ended on a failure branch'
      : run.node_id === '$delivery'
        ? 'Run delivery failed'
        : run.error.startsWith('TimeoutError:')
          ? 'Step timed out'
          : 'Execution failed'
  return (
    <section className="failure-summary" role="alert">
      <h2>
        <CircleAlert size={17} />
        {title}
      </h2>
      <p>
        Failed{' '}
        {run.node_id && (
          <>
            at{' '}
            {node ? (
              <Link
                className="failure-node"
                to="/runs/$id"
                params={{ id: run.id }}
                search={{ tab: 'graph', node: node.id }}
              >
                {node.id}
              </Link>
            ) : (
              run.node_id
            )}
          </>
        )}
        : {run.error}
      </p>
      {decisions.map((a) => (
        <div key={a.node_id} className="decision-evidence">
          <strong>{a.node_id}</strong> took the <strong>{branchName(a.arm!)}</strong> branch.
          {a.preview && (
            <>
              <span>Decision output{a.preview.truncated ? ' (partial)' : ''}</span>
              <JsonView value={a.preview.value} />
            </>
          )}
        </div>
      ))}
      <details>
        <summary>Run input</summary>
        <JsonView value={input} />
      </details>
    </section>
  )
}

export function RerunComparison({ detail }: { detail: Detail }) {
  const { comparison: c, run } = detail
  if (!run.rerun_of || !c) return null
  return (
    <section className="panel inspector compare-panel" aria-label="Rerun comparison">
      <h2>
        Compared with source run <RunLink id={run.rerun_of} />
      </h2>
      <p>
        <Status value={c.source_status} /> → <Status value={run.status} /> ·{' '}
        {c.definition_changed ? 'Definition changed' : 'Same definition'} ·{' '}
        {c.input_changed ? 'Input changed' : 'Same input'}
      </p>
      {c.input_changes.length > 0 && (
        <details>
          <summary>Changed input fields</summary>
          <dl className="input-changes">
            {c.input_changes.map((change) => (
              <div key={change.path}>
                <dt>
                  <code>{change.path}</code>
                </dt>
                <dd>
                  <code>{change.before ?? '(absent)'}</code> → <code>{change.after ?? '(absent)'}</code>
                </dd>
              </div>
            ))}
          </dl>
          {c.more_input_changes && <p>Showing the first 20 changes.</p>}
        </details>
      )}
      {(c.source_failure || run.error) && (
        <p>
          {c.source_failure === run.error && c.source_node === run.node_id
            ? 'Same failure: ' + run.error
            : 'Failure: ' +
              ([c.source_node, c.source_failure].filter(Boolean).join(' · ') || 'None') +
              ' → ' +
              ([run.node_id, run.error].filter(Boolean).join(' · ') || 'None')}
        </p>
      )}
      {c.steps.length === 0 ? (
        <p>No differences in recorded step outcomes or output previews.</p>
      ) : (
        <details>
          <summary>
            {c.steps.length} changed {c.steps.length === 1 ? 'step' : 'steps'}
          </summary>
          {c.steps.map((s) => (
            <section key={s.node}>
              <h3>{s.node}</h3>
              <div className="compare-values">
                {[s.before, s.after].map((a, i) => (
                  <div key={i}>
                    <strong>{i ? 'This run' : 'Source run'}</strong>
                    {a ? (
                      <>
                        <p>
                          <Status value={a.status} /> · Attempt {a.attempt}
                          {a.arm && ' · ' + branchName(a.arm)}
                        </p>
                        {a.error && <p>{a.error}</p>}
                        {a.preview && (
                          <>
                            <small>Output preview{a.preview.truncated ? ' (partial)' : ''}</small>
                            <JsonView value={a.preview.value} />
                          </>
                        )}
                      </>
                    ) : (
                      <p>Step did not run</p>
                    )}
                  </div>
                ))}
              </div>
            </section>
          ))}
        </details>
      )}
    </section>
  )
}
