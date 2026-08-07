import { test, expect, Page } from '@playwright/test';
import * as path from 'path';
import uiLocales from '../packages/data-collector/src/locale/ui_locales.json';

/**
 * Localization e2e — the dev-only `?locale=` query param (App.tsx) is the
 * injection point; production locale comes from mono's live-init only.
 * App.tsx hands the raw value to ScriptHostComponent as `locale` together with
 * `mapLocale={normalizeLocale}`, so everything below exercises the real
 * resolution chain: URL → normalizeLocale → Assembly → Translator/resolveText.
 *
 * Run with the example platform, same as donation.spec.ts:
 *   env VITE_PLATFORM=example pnpm test:e2e
 *
 * The supported-locale policy is imported from the single source of truth
 * (packages/data-collector/src/locale/ui_locales.json) and used to guard each
 * test's premise — never restated as literals here.
 */
const SUPPORTED_UI_LOCALES: string[] = uiLocales.supported;
const DEFAULT_UI_LOCALE: string = uiLocales.default;

/** Pyodide boot budget for the first heading — same as donation.spec.ts. */
const BOOT_TIMEOUT = 90000;
/** Zip validation + extraction before the consent page appears. */
const EXTRACTION_TIMEOUT = 60000;

interface FileStepCopy {
  /** Header of the file-selection page (FlowBuilder.UI_TEXT["submit_file_header"]). */
  heading: string;
  /** Label of the file picker button (feldspar FileInput selectButtonLabel). */
  selectButton: string;
  /** Label of the confirm button (feldspar FileInput continueButtonLabel). */
  continueButton: string;
}

/**
 * Localized twin of donation.spec.ts's setupTestWithFileUpload: open the app at
 * `url`, wait for the first heading in the expected language, then upload
 * tests/test.zip and continue.
 */
async function bootAndUpload(page: Page, url: string, copy: FileStepCopy): Promise<void> {
  await page.goto(url);

  // Wait for Pyodide to initialize and render the page (can take a while on CI)
  await expect(page.getByRole('heading', { name: copy.heading })).toBeVisible({ timeout: BOOT_TIMEOUT });

  const fileChooserPromise = page.waitForEvent('filechooser');
  await page.getByText(copy.selectButton).click();
  const fileChooser = await fileChooserPromise;

  const zipFilePath = path.join(__dirname, 'test.zip');
  await fileChooser.setFiles(zipFilePath);

  await page.getByText(copy.continueButton).click();
}

/** Identical to donation.spec.ts: intercept the submission and hand back its body. */
function setupRouteForDataSubmission(page: Page): Promise<string | null> {
  return new Promise<string | null>((resolve) => {
    page.route('/data-submission', async route => {
      const json = { ok: true };
      await route.fulfill({ json });
      resolve(route.request().postData());
    });
  });
}

async function submitDataAndGetResult(page: Page, donateButton: string): Promise<string | null> {
  const result = setupRouteForDataSubmission(page);
  await page.getByText(donateButton, { exact: true }).click();
  return result;
}

/**
 * R6 seam: `de` IS a supported locale, so no normalization is involved — the
 * framework/helper chrome renders in German. The example platform's CONFIG
 * content (table title/description) only ships en + nl, so the missing `de`
 * key falls back to the default locale and the table title stays English.
 * Both must be true on the same consent page.
 */
test('supported locale de: German chrome with English config content', async ({ page }) => {
  // Premise: de is supported (no normalization), en-fallback is the default.
  expect(SUPPORTED_UI_LOCALES).toContain('de');
  expect(DEFAULT_UI_LOCALE).toBe('en');

  await bootAndUpload(page, '/?locale=de', {
    heading: 'Wählen Sie Ihre example-Datei aus',
    selectButton: 'Datei auswählen',
    continueButton: 'Weiter',
  });

  // German chrome, second helper string: the consent page header.
  await expect(page.getByRole('heading', { name: 'Ihre example-Daten' }))
    .toBeVisible({ timeout: EXTRACTION_TIMEOUT });
  // German chrome, third: the donate button supplied by port_helpers.
  await expect(page.getByText('Ja, für Forschung teilen', { exact: true })).toBeVisible();

  // ...alongside the example config's ENGLISH table title: configs/example_config.json
  // carries only en + nl, so the de miss resolves to the default locale.
  await expect(page.getByText('Files in the zip')).toBeVisible();
  // The Dutch title is the other config key — it must not leak in.
  await expect(page.getByText('Bestanden in de zip')).toHaveCount(0);
});

/**
 * es-ES is NOT in the supported list; its primary subtag es is. Spanish chrome
 * therefore proves mapLocale (normalizeLocale) ran on the dev path — without it
 * the Translator would find no 'es-ES' key and fall back to English.
 */
test('es-ES normalizes to es: Spanish chrome', async ({ page }) => {
  expect(SUPPORTED_UI_LOCALES).not.toContain('es-ES');
  expect(SUPPORTED_UI_LOCALES).toContain('es');

  await page.goto('/?locale=es-ES');

  await expect(page.getByRole('heading', { name: 'Seleccione su archivo de example' }))
    .toBeVisible({ timeout: BOOT_TIMEOUT });
  await expect(page.getByText('Elegir archivo')).toBeVisible();
  // Not the English chrome that a failed normalization would produce.
  await expect(page.getByRole('heading', { name: 'Select your example file' })).toHaveCount(0);
});

/**
 * Dutch end-to-end: chrome, config content (nl IS in the example config) and a
 * completed donation.
 *
 * Note on the plan's "questionnaire continue is Doorgaan": the example platform
 * flow (FlowBuilder.start_flow) never renders a questionnaire — upload →
 * validate → extract → consent → donate. 'Doorgaan' exists as the nl label in
 * packages/data-collector/src/components/questionnaire/questionnaire.tsx:94 but
 * is unreachable from this e2e flow, so it is not asserted here.
 */
test('nl full flow: Dutch chrome and content, donation completes', async ({ page }) => {
  expect(SUPPORTED_UI_LOCALES).toContain('nl');

  await bootAndUpload(page, '/?locale=nl', {
    heading: 'Selecteer uw example bestand',
    selectButton: 'Kies bestand',
    continueButton: 'Verder',
  });

  await expect(page.getByRole('heading', { name: 'Uw example gegevens' }))
    .toBeVisible({ timeout: EXTRACTION_TIMEOUT });
  // Config content: the example config ships nl, so the table title is Dutch.
  await expect(page.getByText('Bestanden in de zip')).toBeVisible();

  const submittedData = await submitDataAndGetResult(page, 'Ja, deel voor onderzoek');

  expect(submittedData).toEqual(expect.stringContaining('hello_world.txt'));
});

/**
 * Regression for the live crash: an unsupported locale must be normalized to
 * the default locale, not passed through. `ro` is doubly interesting — feldspar's
 * own bundles DO carry 'ro' strings, so a locale that reaches the Translator
 * unnormalized renders a Romanian/English mix instead of clean English.
 * Assertions mirror donation.spec.ts's 'can submit data' end to end.
 */
test('unsupported locale ro falls back to English and donates', async ({ page }) => {
  expect(SUPPORTED_UI_LOCALES).not.toContain('ro');
  expect(DEFAULT_UI_LOCALE).toBe('en');

  await page.goto('/?locale=ro');

  await expect(page.getByRole('heading', { name: 'Select your example file' }))
    .toBeVisible({ timeout: BOOT_TIMEOUT });
  // feldspar's file_input bundle has a 'ro' entry; it must not be reachable.
  await expect(page.getByText('Alegeți fișier')).toHaveCount(0);

  const fileChooserPromise = page.waitForEvent('filechooser');
  await page.getByText('Choose file').click();
  const fileChooser = await fileChooserPromise;
  await fileChooser.setFiles(path.join(__dirname, 'test.zip'));
  await page.getByText('Continue').click();

  await expect(page.getByRole('heading', { name: 'Your example data' }))
    .toBeVisible({ timeout: EXTRACTION_TIMEOUT });
  await expect(page.getByText('Files in the zip')).toBeVisible();

  const submittedData = await submitDataAndGetResult(page, 'Yes, share for research');

  expect(submittedData).toEqual(expect.stringContaining('hello_world.txt'));
});
