import { createFileRoute } from '@tanstack/react-router'
import { upstreamOrigin } from '../api/upstream.server'

async function proxy({ request }: { request: Request }) {
  const url = new URL(request.url)
  if (!url.pathname.startsWith('/api/v1/') && url.pathname !== '/api/openapi.json') {
    return Response.json({ detail: 'Unknown API route' }, { status: 404 })
  }
  const writes = request.method === 'POST'
  if (
    writes &&
    (request.headers.get('sec-fetch-site') === 'cross-site' ||
      (request.headers.has('origin') && request.headers.get('origin') !== url.origin))
  ) {
    return Response.json({ detail: 'Cross-origin operation refused' }, { status: 403 })
  }
  if (writes && request.headers.get('content-type')?.split(';')[0] !== 'application/json') {
    return Response.json({ detail: 'Send an application/json body' }, { status: 415 })
  }
  const target = new URL(upstreamOrigin())
  target.pathname = url.pathname === '/api/openapi.json' ? '/openapi.json' : url.pathname
  target.search = url.search
  try {
    const upstream = await fetch(target, {
      method: request.method,
      headers: writes ? { 'content-type': 'application/json' } : {},
      body: writes ? request.body : undefined,
      ...(writes && { duplex: 'half' }),
      redirect: 'error',
      signal: request.signal,
    })
    return new Response(upstream.body, {
      status: upstream.status,
      headers: {
        'content-type': upstream.headers.get('content-type') || 'application/json',
        'cache-control': 'no-store',
      },
    })
  } catch {
    return Response.json({ detail: 'The operations API is unavailable' }, { status: 502 })
  }
}
export const Route = createFileRoute('/api/$')({ server: { handlers: { GET: proxy, POST: proxy } } })
