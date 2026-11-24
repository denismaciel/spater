import { defineConfig } from '@playwright/test'

const api = '127.0.0.1:18410'
const site = 'http://127.0.0.1:13410'
export default defineConfig({
  testDir: './tests',
  workers: 1,
  use: {
    baseURL: site,
    launchOptions: { executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE },
  },
  webServer: [
    {
      command: 'cargo run --locked --manifest-path ../api/Cargo.toml -- serve',
      env: {
        SPATER_OPS_ADDR: api,
        SPATER_OPS_STORE: 'memory://spater-ops',
        SPATER_OPS_JOBS: 'flows.jobs:ALL',
        SPATER_OPS_DEMO: '1',
      },
      url: 'http://' + api + '/api/v1/workflows',
      timeout: 180_000,
    },
    {
      command: 'npm start',
      env: { SPATER_OPS_API_URL: 'http://' + api, PORT: '13410' },
      url: site + '/api/openapi.json',
      timeout: 45_000,
    },
  ],
})
