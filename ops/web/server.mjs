import { serve } from '@hono/node-server'
import { serveStatic } from '@hono/node-server/serve-static'
import { Hono } from 'hono'
import start from './dist/server/server.js'

const app = new Hono()
app.use('*', async (ctx, next) => {
  const host = new URL(ctx.req.url).hostname
  if (host !== 'localhost' && host !== '127.0.0.1' && host !== '[::1]') {
    return ctx.text('Use the loopback address of the operations center.', 403)
  }
  await next()
})
app.use('/assets/*', serveStatic({ root: './dist/client' }))
app.all('*', (ctx) => start.fetch(ctx.req.raw))
const port = Number(process.env.PORT ?? 3410)
serve({ fetch: app.fetch, hostname: '127.0.0.1', port }, () => {
  console.log(`Spater operations center: http://127.0.0.1:${port}`)
})
