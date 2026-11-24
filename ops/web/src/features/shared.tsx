import { Link } from '@tanstack/react-router'
import {
  Check,
  ChevronLeft,
  ChevronRight,
  Circle,
  CircleAlert,
  CircleX,
  Clock3,
  Copy,
  RotateCcw,
  SkipForward,
  LoaderCircle,
} from 'lucide-react'
import { useRef, useState } from 'react'
import type { components } from '../api/schema'

export type Run = components['schemas']['RunSummary']
export type RunStatus = Run['status']
export type Graph = components['schemas']['Graph']
export type Attempt = components['schemas']['Attempt']
export const PAGE_SIZE = 20
export const statuses: RunStatus[] = ['running', 'succeeded', 'failed', 'cancelling', 'cancelled']
export const active = (status: string) => status === 'running' || status === 'cancelling'
export const label = (value: string) => value.charAt(0).toUpperCase() + value.slice(1)
export const countLabel = (n: number, one: string, many = one + 's') =>
  `${n.toLocaleString('en-US')} ${n === 1 ? one : many}`
export const textParam = (value: unknown) =>
  typeof value === 'string' && value.trim() ? value.trim() : undefined
export const positiveInt = (value: unknown) =>
  typeof value === 'number' && Number.isSafeInteger(value) && value > 0 ? value : undefined
export const statusParam = (value: unknown) => statuses.find((s) => s === value)
export const shortId = (id: string) => (id.length > 14 ? id.slice(0, 8) : id)

export function branchName(arm: string) {
  const name = arm.split(':').pop() ?? arm
  return name === 'NoneType' ? 'None' : name === 'Timeout' ? 'timeout' : name
}

const clockFormat = {
  month: 'short',
  day: '2-digit',
  year: 'numeric',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  timeZone: 'UTC',
  hour12: false,
} as const
const seconds = new Intl.DateTimeFormat('en-GB', clockFormat)
const milliseconds = new Intl.DateTimeFormat('en-GB', { ...clockFormat, fractionalSecondDigits: 3 })
export function Timestamp({
  time,
  precise = false,
  showZone = true,
}: {
  time?: number | null
  precise?: boolean
  showZone?: boolean
}) {
  if (time == null) return <>—</>
  const instant = new Date(time * 1000)
  const iso = instant.toISOString()
  return (
    <time dateTime={iso} title={iso}>
      {(precise ? milliseconds : seconds).format(instant)}
      {showZone && ' UTC'}
    </time>
  )
}

export function CopyValue({ name, value }: { name: string; value: string }) {
  const [feedback, setFeedback] = useState('')
  const text = useRef<HTMLElement>(null)
  async function copy() {
    try {
      await navigator.clipboard.writeText(value)
      setFeedback('Copied')
    } catch {
      if (!text.current) return
      const range = document.createRange()
      range.selectNodeContents(text.current)
      const selection = window.getSelection()
      selection?.removeAllRanges()
      selection?.addRange(range)
      setFeedback('Selected for copying')
    }
  }
  return (
    <span className="copy-value">
      <code ref={text}>{value}</code>
      <button
        type="button"
        className="icon-link"
        aria-label={`Copy ${name}`}
        title={`Copy ${name}`}
        onClick={copy}
      >
        <Copy size={14} />
      </button>
      <small role="status">{feedback}</small>
    </span>
  )
}
export function duration(start?: number | null, end?: number | null) {
  if (start == null || end == null) return '—'
  const secs = Math.max(0, end - start)
  if (secs > 0 && secs < 0.001) return '<1 ms'
  if (secs < 1) return Math.round(secs * 1000) + ' ms'
  if (secs < 60) return Number(secs.toFixed(1)) + ' s'
  if (secs < 3600) return Math.floor(secs / 60) + 'm ' + Math.floor(secs % 60) + 's'
  return Math.floor(secs / 3600) + 'h ' + Math.floor((secs % 3600) / 60) + 'm'
}

export function StatusIcon({ value }: { value: string }) {
  const Icon =
    (
      {
        succeeded: Check,
        running: LoaderCircle,
        failed: CircleAlert,
        cancelled: CircleX,
        cancelling: Clock3,
        sleeping: Clock3,
        retried: RotateCcw,
        skipped: SkipForward,
        waiting: Clock3,
      } as Record<string, typeof Check>
    )[value] ?? Circle
  return <Icon size={13} aria-hidden="true" />
}

export function Status({ value }: { value: string }) {
  return (
    <span className={`status status-${value}`}>
      <StatusIcon value={value} />
      {label(value)}
    </span>
  )
}

export function ErrorMessage({ error }: { error: unknown }) {
  const message =
    error instanceof Error
      ? error.message
      : error && typeof error === 'object' && 'detail' in error && typeof error.detail === 'string'
        ? error.detail
        : 'The request failed. Please try again.'
  return (
    <p className="error" role="alert">
      <CircleAlert size={16} aria-hidden="true" /> {message}
    </p>
  )
}

export function JsonView({ value }: { value: unknown }) {
  return <pre className="code">{JSON.stringify(value, null, 2) ?? '—'}</pre>
}

export function RunLink({ id, full = false }: { id: string; full?: boolean }) {
  return (
    <Link className="run-link mono" to="/runs/$id" params={{ id }} title={id}>
      {full ? id : shortId(id)}
    </Link>
  )
}

export function Pager({
  page,
  total,
  change,
}: {
  page: number
  total: number
  change: (page: number) => void
}) {
  const pages = Math.max(1, Math.ceil(total / PAGE_SIZE))
  return (
    <nav className="pager" aria-label="Pagination">
      <span>
        {total
          ? `${Math.min((page - 1) * PAGE_SIZE + 1, total)}–${Math.min(page * PAGE_SIZE, total)} of ${total}`
          : '0 results'}
      </span>
      <div className="page-actions">
        <button
          className="button"
          type="button"
          disabled={page <= 1}
          onClick={() => change(page - 1)}
          aria-label="Previous page"
        >
          <ChevronLeft size={16} />
        </button>
        <span>
          Page {page} of {pages}
        </span>
        <button
          className="button"
          type="button"
          disabled={page >= pages}
          onClick={() => change(page + 1)}
          aria-label="Next page"
        >
          <ChevronRight size={16} />
        </button>
      </div>
    </nav>
  )
}

export function RunTable({
  rows,
  observedAt,
  mapped = false,
}: {
  rows: Run[]
  observedAt: number
  mapped?: boolean
}) {
  if (!rows.length) return <div className="empty">No runs match these filters.</div>
  return (
    <>
      <p className="table-scroll-hint">Scroll across for start time and duration →</p>
      <div className="table-wrap">
        <table className="data-table">
          <thead>
            <tr>
              {mapped && <th>Item</th>}
              <th>Workflow / run ID</th>
              <th>Status</th>
              <th>Started (UTC)</th>
              <th>Duration</th>
              <th>
                <span className="sr-only">Details</span>
              </th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.id}>
                {mapped && (
                  <td className="mono">
                    {row.item_index ?? '—'}
                    {row.item_summary && <small className="item-summary">{row.item_summary}</small>}
                  </td>
                )}
                <td>
                  <Link to="/runs/$id" params={{ id: row.id }} className="table-title">
                    {row.workflow}
                  </Link>
                  <div>
                    <RunLink id={row.id} />
                    {row.status === 'failed' && (
                      <small className="failure-note" title={row.error ?? undefined}>
                        Failed{row.node_id && ` at ${row.node_id}`}
                        {row.error && `: ${row.error}`}
                      </small>
                    )}
                    {row.wake_at != null && (
                      <small className="row-note">
                        {' '}
                        · Wake <Timestamp time={row.wake_at} />
                      </small>
                    )}
                    {row.rerun_of && (
                      <small className="row-note">
                        {' '}
                        · Rerun of <RunLink id={row.rerun_of} />
                      </small>
                    )}
                  </div>
                </td>
                <td>
                  <Status value={row.status} />
                </td>
                <td className="numeric">
                  <Timestamp time={row.started_at} showZone={false} />
                </td>
                <td className="numeric">
                  {duration(row.started_at, row.finished_at ?? (active(row.status) ? observedAt : null))}
                </td>
                <td>
                  <Link
                    to="/runs/$id"
                    params={{ id: row.id }}
                    className="icon-link"
                    aria-label={`Open run ${row.id}`}
                  >
                    <ChevronRight size={15} />
                  </Link>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  )
}
