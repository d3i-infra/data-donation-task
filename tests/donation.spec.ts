import { test, expect, Page } from '@playwright/test';
import * as path from 'path';

/**
 * Common setup for tests: navigate to the page, upload a test file
 */
async function setupTestWithFileUpload(page: Page): Promise<void> {
  // Navigate to the local development server
  await page.goto('http://localhost:3000/');

  // Wait for Pyodide to initialize and render the page (can take a while on CI)
  await expect(page.getByRole('heading', { name: 'Select your example file' })).toBeVisible({ timeout: 90000 });
  
  // Create a temporary file input for file upload
  const fileChooserPromise = page.waitForEvent('filechooser');
  await page.getByText('Choose file').click();
  const fileChooser = await fileChooserPromise;
  
  // Set a test zip file path
  const zipFilePath = path.join(__dirname, 'test.zip');
  await fileChooser.setFiles(zipFilePath);
  
  // Click continue to process the file
  await page.getByText('Continue').click();
}

/**
 * Helper to handle data submission and return the submitted data
 */

function setupRouteForDataSubmission(page: Page): Promise<string|null> {
  return new Promise<string|null>((resolve) => {
    page.route('/data-submission', async route => {
      const json = {ok: true};
      await route.fulfill({ json });
      resolve(route.request().postData());
    });
  });
}

async function submitDataAndGetResult(page: Page): Promise<string | null> {
  const result = setupRouteForDataSubmission(page);
  await page.getByText('Yes, share for research', { exact: true }).click();
  return result;
}

test('can submit data', async ({ page }) => {
  await setupTestWithFileUpload(page);
  
  const submittedData = await submitDataAndGetResult(page);
  
  // The submitted data should contain the expected file
  expect(submittedData).toEqual(expect.stringContaining("hello_world.txt"));
});

test('can remove rows from submission', async ({ page }) => {
  await setupTestWithFileUpload(page);

  // Select all rows for deletion (CheckBox renders as div#selectAll + img,
  // not an ARIA checkbox — see follow-up to add roles/testids to the viz table)
  await page.locator('#selectAll').click();

  await page.getByText(/^Delete/).first().click();
  await expect(page.locator('table').getByText('hello_world.txt')).not.toBeVisible();

  const submittedData = await submitDataAndGetResult(page);

  // The submitted data should not contain the deleted file
  expect(submittedData).not.toEqual(expect.stringContaining("hello_world.txt"));
  // It should contain the deleted row count (tables serialize as an array of
  // { [tableId]: rows, "deleted row count": "N" } objects)
  const parsedData = JSON.parse(submittedData!);
  const tables = JSON.parse(parsedData.data!);
  const fileStats = tables.find((t: any) => 'example_file_stats' in t);
  expect(fileStats['deleted row count']).toEqual('1');
});

test('can undo row removal before submission', async ({ page }) => {
  await setupTestWithFileUpload(page);

  // Select all rows for deletion
  const table = page.locator('table');
  await page.locator('#selectAll').click();

  await page.getByText(/^Delete/).first().click();
  await expect(table.getByText('hello_world.txt')).not.toBeVisible();

  // Click the undo icon next to the "1 deleted" label in the table summary
  // (an unlabeled <img> with an inlined data-URI src — see follow-up to add
  // button semantics/testids to the consent viz table)
  await page.getByText('1 deleted').locator('img').click();

  // Verify the deleted file is visible again (.first(): the filename shows
  // in both the filename and basename columns)
  await expect(table.getByText('hello_world.txt').first()).toBeVisible();

  const submittedData = await submitDataAndGetResult(page);

  // The submitted data should contain the previously deleted file
  expect(submittedData).toEqual(expect.stringContaining("hello_world.txt"));
});

test('can cancel submission', async ({ page }) => {
  await setupTestWithFileUpload(page);

  // Setup the route to capture the submission data
  const result = setupRouteForDataSubmission(page);
  await page.getByText('No', { exact: true }).click();
  const submittedData = await result;

  // The submitted data should not contain the table contents
  expect(submittedData).not.toEqual(expect.stringContaining("hello_world.txt"));
  // The submitted data should contain the cancellation message
  expect(submittedData).toEqual(expect.stringContaining("data_submission declined"));
});
