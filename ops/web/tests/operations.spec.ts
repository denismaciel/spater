import { expect, test } from '@playwright/test'

test('SSR history, filter, inspect failed step and follow a mapped child', async ({ page, request }) => {
  const html = await request.get('/runs')
  expect(html.ok()).toBe(true)
  expect(await html.text()).toContain('weekly_reports')
  await page.goto('/runs')
  await page.getByLabel('Status filter').selectOption('failed')
  await expect(page).toHaveURL(/status=failed/)
  const failed = page.getByRole('row').filter({ hasText: 'triage' }).first()
  await failed.getByRole('link', { name: 'triage', exact: true }).click()
  await expect(page.getByRole('alert')).toContainText('spam')
  await page.getByRole('link', { name: 'Timing', exact: true }).click()
  await expect(page.getByRole('heading', { name: 'Attempt timing' })).toBeVisible()
  await page.reload()
  await expect(page.getByRole('heading', { name: 'Attempt timing' })).toBeVisible()
  await page.goto('/runs?workflow=weekly_reports')
  await page.getByRole('link', { name: 'weekly_reports', exact: true }).first().click()
  await page.getByRole('link', { name: 'Mapped children', exact: true }).click()
  await expect(page.getByRole('table').getByRole('row')).toHaveCount(4)
  await page.getByRole('link', { name: 'archived_report', exact: true }).first().click()
  await expect(page.getByText('Parent run', { exact: true })).toBeVisible()
})

test('validate input, launch a real report and rerun with recorded lineage', async ({ page }) => {
  await page.goto('/workflows/archived_report?tab=launch')
  await page.getByLabel('Input JSON').fill('{"date": 10}')
  await page.getByRole('button', { name: 'Start run' }).click()
  await expect(page.getByRole('alert')).toBeVisible()
  await page.getByLabel('Input JSON').fill('{"date":"2026-11-18"}')
  await page.getByRole('button', { name: 'Start run' }).click()
  await expect(page).toHaveURL(/\/runs\//)
  const source = new URL(page.url()).pathname
  await page.getByRole('link', { name: 'Input and result' }).click()
  await expect(page.getByText(/"total": 15/)).toBeVisible()
  await page.getByRole('link', { name: 'Rerun', exact: true }).click()
  await page.getByRole('button', { name: 'Start rerun' }).click()
  await expect(page.getByText('Rerun of', { exact: true })).toBeVisible()
  expect(new URL(page.url()).pathname).not.toBe(source)
  await page.reload()
  await expect(
    page
      .locator('.run-meta')
      .getByRole('link')
      .filter({ hasText: source.split('/').pop()!.slice(0, 8) }),
  ).toHaveAttribute('href', source)
})

test('mobile navigation and same-origin mutation boundary', async ({ page, request }) => {
  await page.setViewportSize({ width: 390, height: 800 })
  await page.goto('/runs')
  await page.getByRole('navigation', { name: 'Main navigation' }).getByText('Workflows').click()
  await expect(page.getByRole('heading', { name: 'Workflows', exact: true })).toBeVisible()
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true)
  const denied = await request.post('/api/v1/workflows/archived_report/launch', {
    headers: { origin: 'https://unrelated.example' },
    data: { input: { date: 'x' } },
  })
  expect(denied.status()).toBe(403)
})

test('failure drill-down identifies the run and preserves selection across tabs', async ({
  page,
  context,
}) => {
  await context.grantPermissions(['clipboard-read', 'clipboard-write'])
  await page.goto('/runs?status=failed')
  const failed = page.getByRole('row').filter({ hasText: 'Failed at fail_1: spam' }).first()
  await failed.getByRole('link', { name: 'triage', exact: true }).click()
  const id = new URL(page.url()).pathname.split('/').pop()!
  await expect(page).toHaveTitle(`triage · ${id.slice(0, 8)} · Spater`)
  await expect(page.locator('.topbar')).toContainText('triage · ' + id.slice(0, 8))
  await page.getByRole('button', { name: 'Copy run ID' }).focus()
  await page.keyboard.press('Enter')
  await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toBe(id)
  await page.getByRole('alert').getByRole('link', { name: 'fail_1' }).click()
  await expect(page).toHaveURL(/node=fail_1/)
  await page.getByRole('link', { name: 'Rerun', exact: true }).click()
  await expect(
    page.getByRole('navigation', { name: 'Run views' }).locator('[aria-current="page"]'),
  ).toHaveText('Rerun')
  await expect(page.getByRole('main')).toHaveCount(1)
})

test('date filter, mobile run table, and missing run recovery', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 })
  await page.goto('/runs')
  const table = page.locator('.table-wrap')
  expect(await table.evaluate((element) => element.scrollWidth > element.clientWidth)).toBe(true)
  await table.evaluate((element) => {
    element.scrollLeft = element.scrollWidth
  })
  await expect(page.getByRole('columnheader', { name: 'Duration' })).toBeInViewport()
  const range = page.locator('details.date-filter')
  const summary = range.locator('summary')
  await summary.click()
  await range.getByLabel('From (UTC)').fill('2024-11-17')
  await expect(summary).toHaveText('From 17 Nov 2024')
  await page.keyboard.press('Escape')
  await expect(summary).toBeFocused()
  await expect(range).not.toHaveAttribute('open', '')
  await summary.click()
  await page.getByRole('heading', { name: 'Runs', exact: true }).click()
  await expect(range).not.toHaveAttribute('open', '')
  const response = await page.goto('/runs/missing-id')
  expect(response?.status()).toBe(404)
  await page.getByRole('link', { name: 'Back to runs' }).click()
  await expect(page).toHaveTitle('Runs · Spater')
})

test('failed triage highlights its recorded path and exposes precise attempt times', async ({
  page,
  request,
}) => {
  const runs = await request.get('/api/v1/runs?status=failed&workflow=triage')
  const { id } = (await runs.json()).items[0]
  const detail = await request.get(`/api/v1/runs/${id}`)
  const { attempts } = await detail.json()
  await page.goto(`/runs/${id}`)
  const graph = page.getByRole('region', { name: 'Workflow graph' })
  await expect(graph.locator('.chosen-edge title')).toHaveText([
    'start → classify',
    'classify → branch_classify',
    'branch_classify → Spam → fail_1',
  ])
  await expect(graph.getByRole('button', { name: /fail: spam · failed/i })).toBeVisible()
  await expect(graph.getByRole('button', { name: /draft_answer/ })).toHaveClass(/node-unvisited/)
  await graph.focus()
  await expect(graph).toBeFocused()

  await page.getByRole('link', { name: 'Timing', exact: true }).click()
  const timeline = page.getByRole('region', { name: 'Attempt timeline' })
  await expect(timeline).toContainText('Attempt span')
  for (const attempt of attempts) {
    const row = timeline.getByRole('button', { name: new RegExp('^' + attempt.node_id + ',') })
    const elapsed = attempt.finished_at - attempt.started_at
    if (elapsed > 0 && elapsed < 0.001) await expect(row).toContainText('<1 ms')
    await row.click()
    const started = page.locator('.inspector time').first()
    await expect(started).toHaveAttribute('title', new Date(attempt.started_at * 1000).toISOString())
    await expect(started).toHaveText(/\d{2}:\d{2}:\d{2}\.\d{3} UTC/)
  }
})

test('diagnose a branch, edit its rerun, compare outcomes and find it again from its source', async ({
  page,
}) => {
  await page.goto('/runs?status=failed&workflow=triage')
  await page.getByRole('link', { name: 'triage', exact: true }).first().click()
  const source = new URL(page.url()).pathname
  const failure = page.getByRole('alert')
  await expect(failure).toContainText('Workflow ended on a failure branch')
  await expect(failure).toContainText('branch_classify took the Spam branch')
  await expect(failure).toContainText('0.9')
  await failure.getByText('Run input').click()
  await expect(failure).toContainText('Unsolicited offer')
  await page.getByRole('link', { name: 'Rerun', exact: true }).click()
  await expect(page.getByText(/same failure may recur/i)).toBeVisible()
  await page
    .getByLabel('Rerun input JSON')
    .fill('{"thread_id":"investigation","sender":"broker@example.org","body":"Can you help?"}')
  await page.getByRole('button', { name: 'Start rerun' }).click()
  const comparison = page.getByRole('region', { name: 'Rerun comparison' })
  await expect(comparison).toContainText('Succeeded')
  await expect(comparison).toContainText('Input changed')
  await comparison.getByText('Changed input fields').click()
  await expect(comparison).toContainText('/body')
  await expect(comparison).toContainText('Can you help?')
  await comparison.getByText(/changed steps?$/).click()
  await expect(comparison).toContainText('Question')
  const rerun = new URL(page.url()).pathname
  await page.goBack()
  await expect(page).toHaveURL(new RegExp(source + '[?]tab=rerun'))
  await expect(
    page.getByRole('table').getByRole('link', { name: 'triage', exact: true }).first(),
  ).toHaveAttribute('href', rerun)
})

test('launch field guidance, validation and Back preserve operator input', async ({ page }) => {
  await page.goto('/workflows/triage?tab=launch')
  await expect(page.locator('.field-guide')).toContainText('sender')
  const input = page.getByLabel('Input JSON')
  await input.fill('{"thread_id":"preview","body":"Ready?"}')
  await page.getByRole('button', { name: 'Start run' }).click()
  await expect(page.getByRole('alert')).toContainText('Missing required field: sender')
  const valid = '{"thread_id":"preview","sender":"ops@example.org","body":"Ready?"}'
  await input.fill(valid)
  await page.getByRole('button', { name: 'Start run' }).click()
  await expect(page).toHaveURL(/\/runs\//)
  await page.goBack()
  await expect(input).toHaveValue(valid)
  await page.goto('/workflows')
  await expect(page.getByRole('row').filter({ hasText: 'carrier_quote' })).toContainText('Launch')
})

test('mobile step list, item identity and graph fit', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 })
  await page.goto('/runs?workflow=weekly_reports')
  await page.getByRole('link', { name: 'weekly_reports', exact: true }).first().click()
  const steps = page.locator('.execution-steps')
  await expect(steps).toBeVisible()
  await steps.getByRole('button', { name: /map_archived_report/ }).click()
  await expect(page.locator('.inspector')).toContainText('Step input')
  await expect(page.getByRole('region', { name: 'Workflow graph' })).toBeHidden()
  await page.getByRole('button', { name: 'Graph', exact: true }).click()
  await expect(page.getByRole('region', { name: 'Workflow graph' })).toBeVisible()
  await page.getByRole('link', { name: 'Mapped children', exact: true }).click()
  await expect(page.getByRole('row').filter({ hasText: '2026-11-17' })).toContainText('archived_report')
  await page.setViewportSize({ width: 1440, height: 900 })
  await page.goto('/workflows/triage')
  const canvas = page.getByRole('region', { name: 'Workflow graph' })
  await expect
    .poll(() =>
      canvas.evaluate((el) => el.scrollHeight <= el.clientHeight + 1 && el.scrollWidth <= el.clientWidth + 1),
    )
    .toBe(true)
  await expect(canvas.getByRole('button', { name: /^done ·/ })).toBeInViewport({ ratio: 0.9999 })
  // A bypass edge must not cross a node that the edge skips.
  const clear = await canvas.evaluate((el) => {
    const target = [...el.querySelectorAll('g')]
      .find((g) => g.querySelector('title')?.textContent === 'draft_answer → send')
      ?.querySelector('path')
    const obstacle = el.querySelector('button[aria-label*="draft_ack"]')?.getBoundingClientRect()
    if (!target || !obstacle) return false
    for (let i = 1; i < 100; i++) {
      const point = target
        .getPointAtLength((target.getTotalLength() * i) / 100)
        .matrixTransform(target.getScreenCTM()!)
      if (
        point.x > obstacle.left &&
        point.x < obstacle.right &&
        point.y > obstacle.top &&
        point.y < obstacle.bottom
      )
        return false
    }
    return true
  })
  expect(clear).toBe(true)
})
