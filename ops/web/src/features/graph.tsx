import { Graph as LayoutGraph, layout, type GraphLabel, type NodeLabel, type EdgeLabel } from '@dagrejs/dagre'
import { useEffect, useId, useMemo, useRef, useState } from 'react'
import {
  Circle,
  CircleAlert,
  Clock3,
  GitBranch,
  GitFork,
  Layers3,
  Play,
  Square,
  Workflow,
} from 'lucide-react'
import type { Attempt, Graph, RunStatus } from './shared'
import { branchName, duration, label, Status, StatusIcon, Timestamp } from './shared'

const WIDTH = 202,
  HEIGHT = 62

function nodeTitle(node: Graph['nodes'][number]) {
  if (node.kind === 'fork') return 'Start parallel branches'
  if (node.kind === 'join') return 'All branches complete'
  return node.label
}

function positions(graph: Graph) {
  const diagram = new LayoutGraph<GraphLabel, NodeLabel, EdgeLabel>({ multigraph: true }).setGraph({
    rankdir: 'TB',
    marginx: 16,
    marginy: 16,
    ranksep: 32,
    nodesep: 26,
  })
  const successors = new Map<string, string[]>()
  for (const n of graph.nodes) diagram.setNode(n.id, { width: WIDTH, height: HEIGHT })
  graph.edges.forEach(({ src, dst, label }, i) => {
    const outgoing = successors.get(src) ?? []
    outgoing.push(dst)
    successors.set(src, outgoing)
    diagram.setEdge(
      src,
      dst,
      { width: label ? 20 + label.length * 7 : 0, height: label ? 15 : 0, labelpos: 'c' },
      String(i),
    )
  })
  if (graph.nodes.length) layout(diagram)
  const points = new Map(
    graph.nodes.map((n) => {
      const { x, y } = diagram.node(n.id)
      return [n.id, { x: x! - WIDTH / 2, y: y! - HEIGHT / 2 }]
    }),
  )
  const edges = graph.edges.map(({ src, dst }, i) => diagram.edge(src, dst, String(i)))
  return { points, edges, width: diagram.graph().width ?? 1, height: diagram.graph().height ?? 1, successors }
}

function parallelJoins(graph: Graph, successors: Map<string, string[]>) {
  const joins = new Set(graph.nodes.filter((node) => node.kind === 'join').map((node) => node.id))
  const owners = new Map<string, string>()

  function reachableFrom(entry: string) {
    const reached = new Set([entry])
    // Set iteration visits newly added entries in breadth-first order.
    for (const id of reached) {
      for (const next of successors.get(id) ?? []) reached.add(next)
    }
    return reached
  }

  for (const fork of graph.nodes.filter((node) => node.kind === 'fork')) {
    const [first, ...rest] = successors.get(fork.id) ?? []
    if (!first) continue
    // Each continuing branch of a structured parallel block passes through
    // its own join before it can reach any later shared join.
    const otherBranches = rest.map(reachableFrom)
    for (const id of reachableFrom(first)) {
      if (joins.has(id) && otherBranches.every((branch) => branch.has(id))) {
        owners.set(id, fork.id)
        break
      }
    }
  }
  return owners
}

export function FlowGraph({
  graph,
  selected,
  onSelect,
  attempts = [],
  runStatus,
}: {
  graph: Graph
  selected?: string
  onSelect: (node: string) => void
  attempts?: Attempt[]
  runStatus?: RunStatus
}) {
  const id = useId()
  const { points, edges, width, height, successors } = useMemo(() => positions(graph), [graph])
  const joinOwners = useMemo(() => parallelJoins(graph, successors), [graph, successors])
  const viewport = useRef<HTMLDivElement>(null)
  const [fitScale, setFitScale] = useState(1)
  const [mobileGraph, setMobileGraph] = useState(false)
  const [fit, setFit] = useState(true)
  useEffect(() => {
    const element = viewport.current
    if (!element) return
    const measure = () => {
      if (!element.clientWidth) return
      const remaining = window.innerHeight - Math.max(0, element.getBoundingClientRect().top) - 12
      const available = Math.min(
        parseFloat(getComputedStyle(element).maxHeight) - 2,
        Math.max(190, remaining),
      )
      setFitScale(Math.min(1, element.clientWidth / width, available / height))
    }
    measure()
    const observer = new ResizeObserver(measure)
    observer.observe(element)
    window.addEventListener('resize', measure)
    return () => {
      observer.disconnect()
      window.removeEventListener('resize', measure)
    }
  }, [width, height])
  const scale = fit ? fitScale : 1
  const latest = new Map<string, Attempt>()
  const firstStart = new Map<string, number>()
  for (const a of attempts) {
    firstStart.set(a.node_id, Math.min(firstStart.get(a.node_id) ?? Infinity, a.started_at ?? Infinity))
    if ((latest.get(a.node_id)?.attempt ?? -1) < a.attempt) latest.set(a.node_id, a)
  }
  // Start, join and done have no attempt rows. The runner stores each parallel
  // block's completion on its fork, and finalization on the run itself.
  const reached = new Set(latest.keys())
  for (const node of graph.nodes) {
    if (node.kind === 'start' && runStatus) reached.add(node.id)
    if (node.kind === 'done' && runStatus === 'succeeded') reached.add(node.id)
  }
  for (const [join, fork] of joinOwners) {
    if (latest.get(fork)?.status === 'succeeded') reached.add(join)
  }

  const nodes = new Map(graph.nodes.map((node) => [node.id, node]))
  function chosenEdge({ src, dst, label }: Graph['edges'][number]) {
    if (!reached.has(src) || !reached.has(dst)) return false
    const source = nodes.get(src)
    if (source?.kind === 'start' || source?.kind === 'join') return true
    // A fork starts its children before the fork attempt can succeed.
    if (source?.kind === 'fork') return true
    const attempt = latest.get(src)
    if (attempt?.status !== 'succeeded') return false
    if (!label) return true
    return Boolean(attempt.arm && branchName(attempt.arm) === label)
  }
  if (!graph.nodes.length) return <div className="empty">No graph is available for this definition.</div>
  return (
    <div className={runStatus ? 'execution-view' : ''} data-mobile={mobileGraph ? 'graph' : 'steps'}>
      {runStatus && (
        <div className="mobile-execution">
          <div className="page-actions" role="group" aria-label="Execution display">
            <button className="button" aria-pressed={!mobileGraph} onClick={() => setMobileGraph(false)}>
              Steps
            </button>
            <button className="button" aria-pressed={mobileGraph} onClick={() => setMobileGraph(true)}>
              Graph
            </button>
          </div>
          {!mobileGraph && (
            <ol className="execution-steps">
              {[...latest.values()]
                .sort((a, b) => firstStart.get(a.node_id)! - firstStart.get(b.node_id)!)
                .map((a) => (
                  <li key={a.node_id}>
                    <button
                      type="button"
                      aria-pressed={a.node_id === selected}
                      onClick={() => onSelect(a.node_id)}
                    >
                      <strong>{a.node_id}</strong>
                      <Status value={a.status} />
                      {a.arm && <small>Took branch {branchName(a.arm)}</small>}
                    </button>
                  </li>
                ))}
              {!latest.size && <li>No steps have started.</li>}
            </ol>
          )}
        </div>
      )}
      <div className="graph-zoom page-actions" role="group" aria-label="Graph zoom">
        <button type="button" className="button" aria-pressed={fit} onClick={() => setFit(true)}>
          Fit
        </button>
        <button type="button" className="button" aria-pressed={!fit} onClick={() => setFit(false)}>
          100%
        </button>
      </div>
      <div ref={viewport} className="graph-canvas" role="region" aria-label="Workflow graph" tabIndex={0}>
        <div
          style={{
            width: width * scale,
            height: height * scale,
            overflow: 'clip',
            marginInline: 'auto',
          }}
        >
          <div
            className="graph-surface"
            style={{
              width,
              height,
              position: 'relative',
              transform: `scale(${scale})`,
              transformOrigin: 'top left',
            }}
          >
            <svg aria-hidden="true" width={width} height={height} style={{ position: 'absolute', inset: 0 }}>
              <defs>
                <marker
                  id={id}
                  viewBox="0 0 8 8"
                  markerWidth="6"
                  markerHeight="6"
                  refX="7"
                  refY="4"
                  orient="auto"
                >
                  <path d="M0 0 L8 4 L0 8 Z" fill="currentColor" />
                </marker>
              </defs>
              {graph.edges.map((edge, index) => {
                const route = edges[index]
                if (!route?.points) return null
                const path = route.points.map(({ x, y }, i) => (i ? 'L' : 'M') + x + ' ' + y).join(' ')
                const text = edge.label?.split('.').pop()
                const chosen = chosenEdge(edge)
                return (
                  <g
                    key={index}
                    className={chosen ? 'chosen-edge' : undefined}
                    style={chosen ? { color: 'var(--forest)', strokeWidth: 3 } : undefined}
                  >
                    <title>{[edge.src, edge.label, edge.dst].filter(Boolean).join(' → ')}</title>
                    <path d={path} stroke="currentColor" fill="none" markerEnd={`url(#${id})`} />
                    {text && (
                      <text textAnchor="middle" x={route.x} y={(route.y ?? 0) + 3}>
                        {chosen ? '✓ ' : ''}
                        {text}
                      </text>
                    )}
                  </g>
                )
              })}
            </svg>
            <ul className="sr-only" aria-label="Graph connections">
              {graph.edges.map((edge, i) => (
                <li key={i}>
                  {edge.src} → {edge.dst}
                  {edge.label && ` on ${edge.label}`}
                  {chosenEdge(edge) && ' (traversed)'}
                </li>
              ))}
            </ul>
            {graph.nodes.map((node) => {
              const point = points.get(node.id)!
              const attempt = latest.get(node.id)
              const unvisited = !!runStatus && !reached.has(node.id)
              const status =
                attempt?.status ?? (node.kind === 'join' && reached.has(node.id) ? 'succeeded' : undefined)
              const nodeClass = [
                'graph-node',
                selected === node.id ? 'selected' : '',
                status ? 'node-' + status : '',
                unvisited ? 'node-unvisited' : '',
              ]
                .filter(Boolean)
                .join(' ')
              const Icon =
                (
                  {
                    start: Play,
                    done: Square,
                    wait: Clock3,
                    sleep: Clock3,
                    fork: GitFork,
                    join: GitFork,
                    decide: GitBranch,
                    map: Layers3,
                    fail: CircleAlert,
                    step: Workflow,
                  } as Record<string, typeof Circle>
                )[node.kind] ?? Circle
              return (
                <button
                  key={node.id}
                  type="button"
                  className={nodeClass}
                  style={{ position: 'absolute', left: point.x, top: point.y, width: WIDTH, height: HEIGHT }}
                  aria-pressed={node.id === selected}
                  aria-label={`${node.kind} · ${node.label}${status ? ` · ${status}` : ''}`}
                  title={node.label}
                  onClick={() => onSelect(node.id)}
                >
                  <span className="graph-kind">
                    <Icon size={13} /> {label(node.kind)}
                    {status && (
                      <span className={`graph-status-icon node-status-${status}`}>
                        <StatusIcon value={status} />
                        <span className="sr-only">{label(status)}</span>
                      </span>
                    )}
                  </span>
                  <span className="graph-label">{nodeTitle(node)}</span>
                </button>
              )
            })}
          </div>
        </div>
      </div>
    </div>
  )
}

export function Timing({
  attempts,
  observedAt,
  selected,
  choose,
}: {
  attempts: Attempt[]
  observedAt: number
  selected?: Attempt
  choose: (a: Attempt) => void
}) {
  const timed = attempts.filter((a) => a.started_at != null).sort((a, b) => a.started_at! - b.started_at!)
  if (!timed.length) return <p className="empty">No attempts have started.</p>
  const first = timed[0].started_at!,
    last = Math.max(...timed.map((a) => a.finished_at ?? observedAt), first)
  const span = last - first || 1
  return (
    <div className="timeline" role="region" aria-label="Attempt timeline" tabIndex={0}>
      <div className="timeline-axis">
        <span>
          <Timestamp time={first} precise />
        </span>
        <span>Attempt span · {duration(first, last)}</span>
      </div>
      {timed.map((a) => (
        <button
          type="button"
          className={`timeline-row ${a === selected ? 'selected' : ''}`}
          aria-pressed={a === selected}
          key={a.node_id + ':' + a.attempt}
          title={label(a.status)}
          aria-label={`${a.node_id}, attempt ${a.attempt}, ${a.status}, ${duration(a.started_at, a.finished_at ?? observedAt)}`}
          onClick={() => choose(a)}
        >
          <span className="timeline-node">
            {a.node_id} <small>#{a.attempt}</small>
          </span>
          <span className="timeline-track">
            <span
              className={`timeline-bar status-${a.status}`}
              style={{
                left: ((a.started_at! - first) / span) * 100 + '%',
                width: (Math.max(0, (a.finished_at ?? observedAt) - a.started_at!) / span) * 100 + '%',
              }}
            />
          </span>
          <span className="numeric">{duration(a.started_at, a.finished_at ?? observedAt)}</span>
        </button>
      ))}
    </div>
  )
}

export function AttemptInspector({
  node,
  attempts,
  selected,
  choose,
}: {
  node?: Graph['nodes'][number]
  attempts: Attempt[]
  selected?: Attempt
  choose: (n: number) => void
}) {
  if (!node)
    return (
      <aside className="panel inspector">
        <h2>Node details</h2>
        <p className="muted">Select a node to inspect its attempts.</p>
      </aside>
    )
  const options = attempts.filter((a) => a.node_id === node.id).sort((a, b) => b.attempt - a.attempt)
  return (
    <aside className="panel inspector">
      <p className="eyebrow">{label(node.kind)}</p>
      <h2>{node.id}</h2>
      {node.label !== node.id && <p className="muted">{node.label}</p>}
      {options.length > 0 && (
        <label className="field">
          Attempt
          <select value={selected?.attempt ?? ''} onChange={(e) => choose(Number(e.target.value))}>
            {options.map((a) => (
              <option value={a.attempt} key={a.attempt}>
                #{a.attempt} · {label(a.status)}
              </option>
            ))}
          </select>
        </label>
      )}
      {selected ? (
        <>
          <dl className="facts">
            <dt>Status</dt>
            <dd>
              <Status value={selected.status} />
            </dd>
            <dt>Started</dt>
            <dd>
              <Timestamp time={selected.started_at} precise />
            </dd>
            {selected.finished_at != null && (
              <>
                <dt>Finished</dt>
                <dd>
                  <Timestamp time={selected.finished_at} precise />
                </dd>
              </>
            )}
            {selected.due != null && (
              <>
                <dt>Next wake</dt>
                <dd>
                  <Timestamp time={selected.due} precise />
                </dd>
              </>
            )}
            {selected.deadline != null && (
              <>
                <dt>Deadline</dt>
                <dd>
                  <Timestamp time={selected.deadline} precise />
                </dd>
              </>
            )}
            {selected.arm && (
              <>
                <dt>Took branch</dt>
                <dd title={selected.arm}>{branchName(selected.arm)}</dd>
              </>
            )}
          </dl>
          {selected.error && <p className="error">{selected.error}</p>}
          {selected.input_preview ? (
            <>
              <h3>Step input{selected.input_preview.truncated ? ' (partial)' : ''}</h3>
              <pre className="code">{JSON.stringify(selected.input_preview.value, null, 2)}</pre>
            </>
          ) : (
            ['step', 'decide', 'map'].includes(node.kind) && (
              <p className="muted">Step input was not recorded for this attempt.</p>
            )
          )}
          {selected.preview && (
            <>
              <h3>Output preview</h3>
              <pre className="code">{JSON.stringify(selected.preview.value, null, 2)}</pre>
              {selected.preview.truncated && (
                <p className="muted">
                  Truncated from {selected.preview.original_bytes.toLocaleString('en-US')} bytes
                  {selected.preview.omitted_root
                    ? ` · ${selected.preview.omitted_root} top-level entries omitted`
                    : ''}
                  .
                </p>
              )}
            </>
          )}
        </>
      ) : (
        <p className="muted">No attempt recorded.</p>
      )}
    </aside>
  )
}
