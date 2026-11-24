import { createIsomorphicFn } from '@tanstack/react-start'
import { notFound } from '@tanstack/react-router'
import createClient from 'openapi-fetch'
import createQueryClient from 'openapi-react-query'
import type { paths } from './schema'
import { upstreamOrigin } from './upstream.server'

const origin = createIsomorphicFn()
  .server(() => upstreamOrigin())
  .client(() => window.location.origin)

export const api = createClient<paths>({ baseUrl: origin() })
api.use({
  async onResponse({ request, response }) {
    if (response.ok) return
    if (request.method === 'GET' && response.status === 404) throw notFound()
    const problem: unknown = await response
      .clone()
      .json()
      .catch(() => null)
    const detail = problem && typeof problem === 'object' && 'detail' in problem ? problem.detail : null
    throw new Error(typeof detail === 'string' ? detail : `Request failed (${response.status})`)
  },
})
export const $api = createQueryClient(api)
