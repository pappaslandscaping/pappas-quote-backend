import { defineConfig, devices } from "@playwright/test";

const frontendPort = Number(process.env.PLAYWRIGHT_FRONTEND_PORT || 3001);
const backendPort = Number(process.env.PLAYWRIGHT_BACKEND_PORT || 3010);
const backendUrl = `http://127.0.0.1:${backendPort}`;

export default defineConfig({
  testDir: "./tests",
  timeout: 30_000,
  expect: {
    timeout: 10_000
  },
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 1 : 0,
  reporter: [["list"]],
  use: {
    baseURL: `http://127.0.0.1:${frontendPort}`,
    trace: "on-first-retry"
  },
  webServer: [
    {
      command: `PORT=${backendPort} npm --prefix .. run dev:backend`,
      url: `${backendUrl}/api/quotes`,
      reuseExistingServer: !process.env.CI,
      timeout: 120_000,
      stdout: "pipe",
      stderr: "pipe"
    },
    {
      command: `NEXT_PUBLIC_API_BASE_URL=${backendUrl} npm run dev`,
      url: `http://127.0.0.1:${frontendPort}/login`,
      reuseExistingServer: !process.env.CI,
      timeout: 120_000,
      stdout: "pipe",
      stderr: "pipe"
    }
  ],
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] }
    }
  ]
});
