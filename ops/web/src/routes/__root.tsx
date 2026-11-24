import type { QueryClient } from '@tanstack/react-query'
import {
  createRootRouteWithContext,
  HeadContent,
  Link,
  Outlet,
  Scripts,
  useRouterState,
  useMatch,
} from '@tanstack/react-router'
import { Activity, ArrowUpRight, ChevronRight, Clock3, GitFork, SquareCode } from 'lucide-react'
import type { ReactNode } from 'react'
import '@fontsource-variable/dm-sans'
import '@fontsource/instrument-serif/400.css'
import '../styles.css'

export const Route = createRootRouteWithContext<{ queryClient: QueryClient }>()({
  head: () => ({
    meta: [
      { charSet: 'utf-8' },
      { name: 'viewport', content: 'width=device-width, initial-scale=1' },
      { title: 'Spater · Operations' },
    ],
  }),
  shellComponent: Document,
  component: Workspace,
  notFoundComponent: () => (
    <section className="page">
      <h1>Page not found</h1>
      <Link to="/runs">Back to runs</Link>
    </section>
  ),
  errorComponent: ({ error, reset }) => (
    <section className="page">
      <h1>Unable to load this view</h1>
      <p>{error instanceof Error ? error.message : 'Check the operations API connection.'}</p>
      <button className="button" onClick={reset}>
        Try again
      </button>
    </section>
  ),
})

function Document({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <head>
        <HeadContent />
      </head>
      <body>
        {children}
        <Scripts />
      </body>
    </html>
  )
}

function Workspace() {
  const path = useRouterState({ select: (s) => s.location.pathname })
  const section = path.startsWith('/workflows') ? 'Workflows' : 'Runs'
  const destination = section === 'Workflows' ? '/workflows' : '/runs'
  const run = useMatch({ from: '/runs/$id', shouldThrow: false })
  const workflow = useMatch({ from: '/workflows/$name', shouldThrow: false })
  const detail = run?.loaderData?.breadcrumb ?? workflow?.params.name
  return (
    <div className="workspace">
      <a className="skip" href="#content">
        Skip to content
      </a>
      <aside className="rail">
        <Link to="/runs" className="brand" aria-label="Spater home">
          <span className="brand-mark" aria-hidden="true">
            <i />
            <i />
            <i />
          </span>
          Spater
        </Link>
        <nav aria-label="Main navigation">
          <Link to="/runs">
            <Activity size={18} />
            Runs
          </Link>
          <Link to="/workflows">
            <GitFork size={18} />
            Workflows
          </Link>
        </nav>
        <a className="schema-link" href="/api/openapi.json" target="_blank" rel="noreferrer">
          <SquareCode size={17} />
          API specification
          <ArrowUpRight size={14} />
        </a>
      </aside>
      <div className="desk">
        <header className="topbar">
          <div>
            Operations <ChevronRight size={13} />
            <Link to={destination}>{section}</Link>
            {run?.loaderData?.parent && (
              <>
                <ChevronRight size={13} />
                <Link
                  to="/runs/$id"
                  params={{ id: run.loaderData.parent }}
                  search={{ tab: 'children', map: run.loaderData.parentNode ?? undefined }}
                >
                  Parent run {run.loaderData.parent.slice(0, 8)}
                </Link>
              </>
            )}
            {detail && (
              <>
                <ChevronRight size={13} />
                <span aria-current="page">{detail}</span>
              </>
            )}
          </div>
          <span className="time-zone">
            <Clock3 size={13} />
            UTC
          </span>
        </header>
        <main id="content" tabIndex={-1}>
          <Outlet />
        </main>
      </div>
    </div>
  )
}
