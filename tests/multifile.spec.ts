import { test, expect, Page, Locator } from '@playwright/test';
import * as path from 'path';
import * as fs from 'fs';
import * as os from 'os';

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

/**
 * Same `/data-submission` route contract as setupRouteForDataSubmission,
 * but for asserting a POST does NOT happen (a terminal pre-validation error
 * must never reach the donate step) rather than awaiting one that will.
 * Register before driving the flow; check `wasCalled()` afterwards.
 */
function watchForDataSubmission(page: Page): { wasCalled: () => boolean } {
  let called = false;
  page.route('/data-submission', async route => {
    called = true;
    await route.fulfill({ json: { ok: true } });
  });
  return { wasCalled: () => called };
}

/**
 * Locate one specific consent-form table by its exact title text, not just
 * "the first/any <table> on the page" — e2etest_multifile renders TWO tables
 * (membership and content_preview), each with its own filename column, so
 * an unscoped `page.getByRole('cell', { name: '<filename>' })` can resolve
 * to a cell in EITHER table. There is no data-testid to key off: grepped
 * `packages/data-collector/src/components/consent_form_viz/*.tsx` for
 * `data-testid` — zero matches; `table_container.tsx` keys each table's
 * container `<div>` by `table.id` via React's `key` prop, which is a
 * reconciliation hint only and is never emitted as a DOM attribute.
 *
 * What IS structural (table_container.tsx): a table's title (rendered by
 * `Title4` as a plain `<div>{title}</div>`, not a heading role) and its
 * `<table>` element are both descendants of one shared per-table container
 * `<div>`, as siblings under it — so the nearest ancestor `<div>` of the
 * title text that also contains a `<table>` descendant is exactly that
 * table's own container, regardless of how many other tables are on the
 * page or how deeply nested the title text itself is.
 */
function tableWithTitle(page: Page, title: string): Locator {
  return page.getByText(title, { exact: true }).first()
    .locator('xpath=ancestor::div[.//table][1]//table');
}

test('can select two zip parts and submit data sourced from both', async ({ page }) => {
  await page.goto('http://localhost:3000/');
  await expect(page.getByRole('heading', { name: 'Select your e2etest_multifile file' })).toBeVisible({ timeout: 90000 });

  const fileChooserPromise = page.waitForEvent('filechooser');
  await page.getByText('Choose file(s)').click();
  const fileChooser = await fileChooserPromise;
  await fileChooser.setFiles([
    path.join(__dirname, 'test-split-1.zip'),
    path.join(__dirname, 'test-split-2.zip'),
  ]);

  // Both filenames are listed, each with its own remove control.
  await expect(page.getByText('test-split-1.zip', { exact: true })).toBeVisible();
  await expect(page.getByText('test-split-2.zip', { exact: true })).toBeVisible();
  await expect(page.locator('span.truncate')).toHaveCount(2);
  await expect(page.locator('button:has(img)')).toHaveCount(2);

  // Continue resolves the PayloadFiles prompt and proceeds to extraction.
  await page.getByText('Continue').click();
  await expect(page.getByRole('heading', { name: 'Your e2etest_multifile data' })).toBeVisible({ timeout: 90000 });

  // The consent tables' rows are sourced from members of BOTH uploaded
  // parts — test-split-1.zip owns test_file_0001.txt/0003.json,
  // test-split-2.zip owns test_file_0002.csv/0004.log (see
  // tests/generate_test_zip.py --split).
  //
  // Cell values, not free-text search: `<td>` maps to the ARIA "cell"
  // role, and `exact: true` is load-bearing here, not decorative —
  // 'test_file_0002.csv' is a substring of the content_preview table's
  // 'FILE:test_file_0002.csv' cell (see below), so a non-exact match would
  // be ambiguous between the two tables. `.first()` guards strict mode
  // without changing which table is asserted against, since each exact
  // value legitimately appears in exactly one cell across the page.
  await expect(page.getByRole('cell', { name: 'test_file_0001.txt', exact: true }).first()).toBeVisible();
  await expect(page.getByRole('cell', { name: 'test_file_0002.csv', exact: true }).first()).toBeVisible();

  // This exact value is NOT the filename column — it's the content_preview
  // column of the e2etest_multifile_content_preview table, whose only source
  // is archive_content_preview_to_df's reader.raw() call, which routes
  // through ArchiveSet.read_member() to reopen test-split-2.zip (the part
  // that owns test_file_0002.csv) and read its actual bytes. Each split
  // fixture's member content starts with a `FILE:<name>` marker line (see
  // generate_test_zip.py's --split mode) precisely so this value could
  // only appear here if that read really happened — a table showing only
  // filenames/part indices (metadata ArchiveSet.part_index_of never reads
  // a byte for) would not prove that. Exact matching also rules out this
  // cell being confused with a plain 'test_file_0002.csv' filename cell.
  await expect(page.getByRole('cell', { name: 'FILE:test_file_0002.csv', exact: true }).first()).toBeVisible();

  const submittedData = await submitDataAndGetResult(page);

  // Donation completes and the submitted payload carries rows from both
  // parts, including the content-derived value from part 2. (These check
  // substring presence in the serialized JSON payload, not a DOM query —
  // Playwright's strict-mode/exact-match concerns don't apply to a plain
  // string .toEqual(expect.stringContaining(...)) assertion.)
  expect(submittedData).toEqual(expect.stringContaining('test_file_0001.txt'));
  expect(submittedData).toEqual(expect.stringContaining('test_file_0002.csv'));
  expect(submittedData).toEqual(expect.stringContaining('FILE:test_file_0002.csv'));
});

test('adding the same file twice shows the duplicate notice and keeps one entry', async ({ page }) => {
  await page.goto('http://localhost:3000/');
  await expect(page.getByRole('heading', { name: 'Select your e2etest_multifile file' })).toBeVisible({ timeout: 90000 });

  const firstChooserPromise = page.waitForEvent('filechooser');
  await page.getByText('Choose file(s)').click();
  const firstChooser = await firstChooserPromise;
  await firstChooser.setFiles(path.join(__dirname, 'test-split-1.zip'));

  await expect(page.locator('span.truncate')).toHaveCount(1);

  // Re-opening the picker and choosing the same file again is a no-op merge:
  // the duplicate notice appears and the selection still has one entry.
  // (Requires FileInputMultiple's handleSelect to reset the native input's
  // value after each selection — otherwise the browser never fires a
  // second `change` event for an identical re-pick, and this is the most
  // common way a participant would trigger the duplicate path.)
  const secondChooserPromise = page.waitForEvent('filechooser');
  await page.getByText('Choose file(s)').click();
  const secondChooser = await secondChooserPromise;
  await secondChooser.setFiles(path.join(__dirname, 'test-split-1.zip'));

  // duplicatesNoticeText's en copy is "Already added: {names}" — asserting
  // the exact resolved string (the dev server's default locale is en) both
  // pins the real notice text and avoids a loose substring match.
  await expect(page.getByText('Already added: test-split-1.zip', { exact: true })).toBeVisible();
  await expect(page.locator('span.truncate')).toHaveCount(1);
  await expect(page.locator('span.truncate').first()).toHaveText('test-split-1.zip');
});

test('uploading a renamed copy of the same archive does not duplicate the donated data', async ({ page }) => {
  // The real-world "double download" case: a participant's browser saves
  // the same Takeout part twice under two different names (e.g.
  // 'takeout-...-001.zip' then 'takeout-...-001 (1).zip'). Two DIFFERENT
  // layers are responsible for two DIFFERENT problems here, and this test
  // pins both without conflating them:
  //   - FileInputMultiple's duplicate notice (select.ts's addFiles) keys on
  //     (name, size, lastModified). A renamed copy has a different name, so
  //     this layer correctly does NOT flag it — catching identical CONTENT
  //     under a different NAME is not this layer's job, and it shouldn't
  //     try (it would have to hash file contents client-side to do so).
  //   - ArchiveSet's canonical (name, size) part ordering + first-part-wins
  //     member resolution (ADR-0039) is the layer that actually dedupes
  //     identical member content across parts: the "loser" part's members
  //     are counted in ArchiveSet.duplicates["DuplicateMemberAcrossParts"]
  //     — visible to a researcher inspecting extraction errors, never
  //     shown to the participant as a UI notice.
  //
  // A third fixture is deliberately NOT committed for this: the renamed
  // copy is created on the fly (Node fs.copyFileSync into a unique tmpdir
  // subdirectory, so the File.name the browser reports is exactly
  // 'test-split-1 (1).zip' regardless of the full path) and removed after
  // the test.
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'e2etest-multifile-'));
  const renamedCopyPath = path.join(tmpDir, 'test-split-1 (1).zip');
  fs.copyFileSync(path.join(__dirname, 'test-split-1.zip'), renamedCopyPath);

  try {
    await page.goto('http://localhost:3000/');
    await expect(page.getByRole('heading', { name: 'Select your e2etest_multifile file' })).toBeVisible({ timeout: 90000 });

    const fileChooserPromise = page.waitForEvent('filechooser');
    await page.getByText('Choose file(s)').click();
    const fileChooser = await fileChooserPromise;
    await fileChooser.setFiles([
      path.join(__dirname, 'test-split-1.zip'),
      renamedCopyPath,
      path.join(__dirname, 'test-split-2.zip'),
    ]);

    // (a) All three filenames appear in the selection list — different
    // names, so the component correctly keeps all three and shows no
    // duplicate notice; that layer cannot (and should not) catch this.
    await expect(page.getByText('test-split-1.zip', { exact: true })).toBeVisible();
    await expect(page.getByText('test-split-1 (1).zip', { exact: true })).toBeVisible();
    await expect(page.getByText('test-split-2.zip', { exact: true })).toBeVisible();
    await expect(page.locator('span.truncate')).toHaveCount(3);
    await expect(page.getByText('Already added:')).not.toBeVisible();

    await page.getByText('Continue').click();
    await expect(page.getByRole('heading', { name: 'Your e2etest_multifile data' })).toBeVisible({ timeout: 90000 });

    // (b) ArchiveSet dedupes by content across the 3 uploaded parts: each
    // of the 4 distinct underlying files (test-split-1.zip and its renamed
    // copy both own test_file_0001.txt/0003.json; test-split-2.zip owns
    // test_file_0002.csv/0004.log) shows up exactly ONCE in the membership
    // table — not once per uploading part, which a naive union (no
    // first-part-wins dedupe) would show as duplicate rows for the two
    // shared members.
    //
    // Scoped to the membership table specifically (see tableWithTitle):
    // e2etest_multifile_content_preview ALSO has a filename column, so an
    // unscoped page-wide cell lookup would find 2 matches per name on a
    // correctly-working app (one per table) — a false failure on the
    // success path that would not actually detect a broken dedupe (which
    // would show up as 2 matches WITHIN the membership table instead).
    const membershipTable = tableWithTitle(page, 'Files across uploaded parts');
    await expect(membershipTable.getByRole('cell', { name: 'test_file_0001.txt', exact: true })).toHaveCount(1);
    await expect(membershipTable.getByRole('cell', { name: 'test_file_0002.csv', exact: true })).toHaveCount(1);
    await expect(membershipTable.getByRole('cell', { name: 'test_file_0003.json', exact: true })).toHaveCount(1);
    await expect(membershipTable.getByRole('cell', { name: 'test_file_0004.log', exact: true })).toHaveCount(1);
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test('too many files shows the safety error page', async ({ page }) => {
  // uploads.check_payload_size() runs before validate_file/extract_data —
  // MAX_UPLOAD_FILES=16 (uploads.py), so 17 selected parts must stop the
  // flow at the safety check. Previously pinned only at the pytest layer
  // (test_flow_builder.py); this closes the browser leg.
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'e2etest-multifile-toomany-'));
  const partPaths: string[] = [];
  for (let i = 1; i <= 17; i++) {
    const partPath = path.join(tmpDir, `part-${String(i).padStart(2, '0')}.zip`);
    fs.copyFileSync(path.join(__dirname, 'test-split-1.zip'), partPath);
    partPaths.push(partPath);
  }

  try {
    const submissionWatch = watchForDataSubmission(page);

    await page.goto('http://localhost:3000/');
    await expect(page.getByRole('heading', { name: 'Select your e2etest_multifile file' })).toBeVisible({ timeout: 90000 });

    const fileChooserPromise = page.waitForEvent('filechooser');
    await page.getByText('Choose file(s)').click();
    const fileChooser = await fileChooserPromise;
    await fileChooser.setFiles(partPaths);

    await page.getByText('Continue').click();

    // LAYER 1 (stable semantics — should survive any presentation
    // redesign): the flow reaches a terminal error state before
    // validation. The consent table (which only renders after
    // validate_file + extract_data succeed) never appears, and no
    // donation request is ever made — 17 files never reach the donate
    // step at all.
    await expect(page.getByRole('heading', { name: 'Your e2etest_multifile data' })).not.toBeVisible({ timeout: 5000 });
    expect(submissionWatch.wasCalled()).toBe(false);

    // Presentation layer: these assertions pin the CURRENT safety-page
    // rendering, which interpolates raw exception text (English-only)
    // into localized UI. A planned redesign of participant error
    // presentation will break these deliberately — update them with the
    // new presentation, keep Layer 1 untouched.
    await expect(page.getByRole('heading', { name: 'File cannot be processed' })).toBeVisible({ timeout: 90000 });
    await expect(page.getByText('at most 16')).toBeVisible();

    // Still LAYER 1: the flow terminates here (FlowBuilder returns
    // unconditionally after rendering this page, regardless of which
    // button is clicked) — it must not loop back to the file prompt.
    await page.getByText('Continue', { exact: true }).first().click();
    await expect(page.getByRole('heading', { name: 'Select your e2etest_multifile file' })).not.toBeVisible();
    expect(submissionWatch.wasCalled()).toBe(false);
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test('combined size over the limit shows the safety error page', async ({ page }) => {
  // uploads.check_payload_size()'s PayloadFiles branch rejects a set whose
  // members' sizes sum to more than MAX_TOTAL_UPLOAD_BYTES (10 GiB,
  // uploads.py) before validate_file/extract_data ever run. Previously
  // pinned only at the pytest layer; this closes the browser leg.
  //
  // The oversized part is allocated SPARSE (fs.openSync + fs.ftruncateSync
  // to set its logical size without writing real bytes) rather than
  // actually written, because the entire pre-validation chain is
  // metadata-only: the browser's File.size comes from OS file-size
  // metadata (which reflects a sparse file's truncated length, not its
  // allocated blocks), and check_payload_size() never reads a byte — it
  // only sums .size across the selected files. Writing 11 real GiB per
  // test run would be needlessly slow and disk-hungry for a check that is
  // provably never about content.
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'e2etest-multifile-toolarge-'));
  const sparsePath = path.join(tmpDir, 'takeout-sparse-001.zip');

  try {
    let fd: number | undefined;
    try {
      fd = fs.openSync(sparsePath, 'w');
      fs.ftruncateSync(fd, 11 * 1024 ** 3); // 11 GiB logical size, ~0 bytes on disk
    } catch (e) {
      test.skip(true, `sparse file allocation failed on this filesystem, cannot exercise the aggregate-size path without writing real gigabytes: ${(e as Error).message}`);
    } finally {
      if (fd !== undefined) fs.closeSync(fd);
    }

    const submissionWatch = watchForDataSubmission(page);

    await page.goto('http://localhost:3000/');
    await expect(page.getByRole('heading', { name: 'Select your e2etest_multifile file' })).toBeVisible({ timeout: 90000 });

    const fileChooserPromise = page.waitForEvent('filechooser');
    await page.getByText('Choose file(s)').click();
    const fileChooser = await fileChooserPromise;
    await fileChooser.setFiles([sparsePath, path.join(__dirname, 'test-split-1.zip')]);

    await page.getByText('Continue').click();

    // LAYER 1 (stable semantics — should survive any presentation
    // redesign): terminal error before validation, no consent table, no
    // donation request.
    await expect(page.getByRole('heading', { name: 'Your e2etest_multifile data' })).not.toBeVisible({ timeout: 5000 });
    expect(submissionWatch.wasCalled()).toBe(false);

    // Presentation layer: these assertions pin the CURRENT safety-page
    // rendering, which interpolates raw exception text (English-only)
    // into localized UI. A planned redesign of participant error
    // presentation will break these deliberately — update them with the
    // new presentation, keep Layer 1 untouched. Fragment only, not exact
    // numbers — the aggregate byte count includes test-split-1.zip's few
    // KB alongside the 11 GiB sparse file, so the rendered MiB figure is
    // not a round number.
    await expect(page.getByRole('heading', { name: 'File cannot be processed' })).toBeVisible({ timeout: 90000 });
    await expect(page.getByText('limit is')).toBeVisible();
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});
