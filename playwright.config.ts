import { defineConfig, devices } from '@playwright/test';

/**
 * Read environment variables from file.
 * https://github.com/motdotla/dotenv
 */
// import dotenv from 'dotenv';
// import path from 'path';
// dotenv.config({ path: path.resolve(__dirname, '.env') });

/**
 * See https://playwright.dev/docs/test-configuration.
 */
export default defineConfig({
  testDir: './tests',
  /* Platform-keyed spec selection: the e2etest platform's fault-injection
   * spec and the default donation/localization specs never cross-run.
   * e2etest_multifile's PayloadFiles spec (tests/multifile.spec.ts) is kept
   * off both of those, and off the default run too — testMatch narrows it
   * to run ONLY that spec, so a future spec file added to tests/ doesn't
   * accidentally start running under VITE_PLATFORM=e2etest_multifile too. */
  testMatch: process.env.VITE_PLATFORM === 'e2etest_multifile' ? ['**/multifile.spec.ts'] : undefined,
  testIgnore:
    process.env.VITE_PLATFORM === 'e2etest'
      ? ['**/donation.spec.ts', '**/localization.spec.ts', '**/multifile.spec.ts']
      : process.env.VITE_PLATFORM === 'e2etest_multifile'
      ? []
      : ['**/error-flow.spec.ts', '**/multifile.spec.ts'],
  /* Run tests in files in parallel */
  fullyParallel: true,
  /* Fail the build on CI if you accidentally left test.only in the source code. */
  forbidOnly: !!process.env.CI,
  /* Retry on CI only */
  retries: process.env.CI ? 2 : 0,
  /* Opt out of parallel tests on CI. Cap local workers at 2: each worker
   * boots a full Pyodide runtime (CDN fetch + wheel install) in its own
   * browser context, and more than 2 simultaneous boots can starve the
   * 90s first-render wait on a cold CDN cache — observed as spurious
   * blank-page timeouts under Playwright's uncapped default. */
  workers: process.env.CI ? 1 : 2,
  /* Reporter to use. See https://playwright.dev/docs/test-reporters */
  reporter: 'html',
  /* Increase timeout since Pyodide takes time to initialize */
  timeout: 120000,
  /* Shared settings for all the projects below. See https://playwright.dev/docs/api/class-testoptions. */
  use: {
    /* Base URL to use in actions like `await page.goto('/')`. */
    baseURL: 'http://localhost:3000',
    /* Collect trace when retrying the failed test. See https://playwright.dev/docs/trace-viewer */
    trace: 'on-first-retry',
    /* Capture screenshots on failure */
    screenshot: 'only-on-failure',
  },

  /* Configure projects for major browsers */
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    }
  ],

  /* Run your local dev server before starting the tests */
  webServer: {
    command: 'pnpm run start',
    url: 'http://localhost:3000',
    reuseExistingServer: !process.env.CI,
    timeout: 120 * 1000,
  },
});
