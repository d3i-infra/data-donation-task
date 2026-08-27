import { test, expect } from '@playwright/test';
import * as path from 'path';

/**
 * End-to-end proof for Issue #123: a participant who hits a Python error must
 * NOT terminate with the success exit (code 0). The error flow ends with a
 * nonzero CommandSystemExit, so the host keeps the task pending instead of
 * recording a completion.
 *
 * Runs against the e2e-only test platform (VITE_PLATFORM=e2etest), which
 * raises on purpose when the uploaded zip contains `trigger_error.txt`
 * (see port/platforms/e2etest.py). Production platforms carry no trigger.
 */
test('error flow donates report, shows task-incomplete page, and exits nonzero', async ({ page }) => {
  const consoleMessages: string[] = [];
  page.on('console', (msg) => consoleMessages.push(msg.text()));

  await page.goto('http://localhost:3000/');
  await expect(page.getByRole('heading', { name: 'Select your example file' })).toBeVisible({ timeout: 90000 });

  // Upload the fixture that makes the example extractor raise
  const fileChooserPromise = page.waitForEvent('filechooser');
  await page.getByText('Choose file').click();
  const fileChooser = await fileChooserPromise;
  await fileChooser.setFiles(path.join(__dirname, 'error-trigger.zip'));
  await page.getByText('Continue').click();

  // Consent-gated error page (ADR-0022)
  await expect(page.getByText('Something went wrong')).toBeVisible({ timeout: 60000 });

  // Capture the error-report donation while consenting to it
  const submission = new Promise<string | null>((resolve) => {
    page.route('/data-submission', async (route) => {
      await route.fulfill({ json: { ok: true } });
      resolve(route.request().postData());
    });
  });
  await page.getByText('Report error', { exact: true }).click();

  const submittedData = await submission;
  expect(submittedData).toEqual(expect.stringContaining('error-report'));
  expect(submittedData).toEqual(expect.stringContaining('Intentional test error'));

  // Terminal task-incomplete page instead of a stale error page
  await expect(page.getByText('Task not completed')).toBeVisible();
  await expect(page.getByText('This task could not be completed', { exact: false })).toBeVisible();
  await page.getByText('OK', { exact: true }).click();

  // The flow must end with the nonzero (error-end) exit, never the success exit
  await expect
    .poll(() => consoleMessages.find((m) => m.includes('[FakeBridge] received exit')), { timeout: 30000 })
    .toContain('received exit: 1=Error flow completed');
  expect(consoleMessages.find((m) => m.includes('received exit: 0='))).toBeUndefined();
});
