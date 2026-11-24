import { execFileSync } from 'node:child_process'
import { readFile, writeFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import openapiTS, { astToString } from 'openapi-typescript'

const web = fileURLToPath(new URL('..', import.meta.url))
const spec = JSON.parse(
  execFileSync(
    'cargo',
    ['run', '--quiet', '--locked', '--manifest-path', '../api/Cargo.toml', '--', 'openapi'],
    { cwd: web, encoding: 'utf8' },
  ),
)
const artifacts = {
  'openapi.json': JSON.stringify(spec, null, 2) + '\n',
  'src/api/schema.d.ts':
    '// Generated from the Rust operations API. Run npm run gen:openapi.\n' +
    astToString(await openapiTS(spec)),
}
for (const [name, content] of Object.entries(artifacts)) {
  const target = new URL('../' + name, import.meta.url)
  if (process.argv.includes('--check')) {
    if ((await readFile(target, 'utf8')) !== content)
      throw new Error(name + ' differs from the Rust contract')
  } else {
    await writeFile(target, content)
  }
}
