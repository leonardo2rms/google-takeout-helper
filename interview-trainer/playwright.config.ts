import { defineConfig } from '@playwright/test';

// Parallel runs (several worktrees, CI shards) must not share a server: pick a free port with E2E_PORT.
const PORT = Number(process.env.E2E_PORT ?? 4173);

export default defineConfig({
  testDir: 'e2e',
  outputDir: 'e2e/.results',
  timeout: 45_000,
  fullyParallel: true,
  workers: 4,
  reporter: [['list']],
  use: {
    baseURL: `http://localhost:${PORT}`,
    // A phone: this is a mobile-first app, and the screenshots are meant to show that.
    viewport: { width: 390, height: 844 },
    deviceScaleFactor: 2,
    isMobile: true,
    hasTouch: true,
    // No fade-in animations, so screenshots show the settled state.
    contextOptions: { reducedMotion: 'reduce' },
    screenshot: 'only-on-failure',
  },
  webServer: {
    // Build + serve the production bundle with AI grading forced off, so runs are deterministic
    // (open questions use the offline self-assessment path) and never spend API credits.
    command: `npm run build && npm run start`,
    url: `http://localhost:${PORT}/api/health`,
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
    env: { PORT: String(PORT), ANTHROPIC_API_KEY: '', ANTHROPIC_AUTH_TOKEN: '' },
  },
});
