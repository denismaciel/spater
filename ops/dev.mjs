// Start and stop the local Rust listener, its runner, and the Start dev server together.
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'

const root = fileURLToPath(new URL('../', import.meta.url))
const web = fileURLToPath(new URL('./web/', import.meta.url))
if (process.argv.slice(2).some((arg) => arg !== '--demo')) {
  console.error('Usage: node ops/dev.mjs [--demo]')
  process.exit(2)
}
const env = { ...process.env }
if (process.argv.includes('--demo')) env.SPATER_OPS_DEMO = '1'
const address = env.SPATER_OPS_ADDR || '127.0.0.1:8410'
env.SPATER_OPS_API_URL = 'http://' + address
const children = new Set()
let stopping = false
function stop(code) {
  if (stopping) return
  stopping = true
  process.exitCode = code
  for (const child of children) {
    try {
      process.kill(-child.pid, 'SIGTERM')
    } catch (error) {
      if (error.code !== 'ESRCH') console.error(error.message)
    }
  }
}
function start(command, args, cwd) {
  const child = spawn(command, args, { cwd, env, detached: true, stdio: 'inherit' })
  children.add(child)
  child.once('error', (error) => {
    console.error(error.message)
    stop(1)
  })
  child.once('exit', (code) => {
    children.delete(child)
    stop(code ?? 1)
  })
}
process.on('SIGINT', () => stop(0))
process.on('SIGTERM', () => stop(0))
start('cargo', ['run', '--locked', '--manifest-path', 'ops/api/Cargo.toml', '--', 'serve'], root)
start('npm', ['run', 'dev'], web)
