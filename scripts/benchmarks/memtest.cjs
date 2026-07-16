// Memory test: drive a 2.5 GiB zip through the example flow and sample the
// full Chrome process-tree RSS at checkpoints. Success criteria:
//  (1) flow reaches the consent page (no NotReadableError / error page),
//  (2) peak browser memory stays far below the 2.5 GiB payload size
//      (streaming invariant, ADR-0026 / issue #61 regression signature).
const { chromium } = require('@playwright/test');
const { execSync } = require('child_process');

const ZIP = process.env.MEMTEST_ZIP;
if (!ZIP) { console.error('Set MEMTEST_ZIP=/path/to/test.zip'); process.exit(1); }

function treeRssMb() {
  // Sum RSS over all Playwright headless-shell processes (the user's real
  // Chrome is a different binary, so this only counts the test browser).
  try {
    const out = execSync("ps -eo rss=,args= | grep 'chrome-headless-shell' | grep -v grep | awk '{s+=$1} END {print s+0}'").toString().trim();
    return Math.round(Number(out) / 1024);
  } catch { return -1; }
}

(async () => {
  const browser = await chromium.launch();
  
  const page = await browser.newPage();
  const samples = [];
  const mark = (label) => {
    const mb = treeRssMb();
    samples.push([label, mb]);
    console.log(`[mem] ${label}: ${mb} MB`);
  };

  page.on('pageerror', (e) => console.log('[pageerror]', String(e).slice(0, 200)));

  await page.goto('http://localhost:3000/');
  await page.getByRole('heading', { name: 'Select your example file' }).waitFor({ timeout: 120000 });
  mark('app ready (pyodide loaded)');

  const chooser = page.waitForEvent('filechooser');
  await page.getByText('Choose file').click();
  await (await chooser).setFiles(ZIP);
  mark('file attached (2.5 GiB)');

  await page.getByText('Continue').click();

  // Poll memory while validation + extraction run.
  let peak = 0;
  const t0 = Date.now();
  const done = page.getByRole('heading', { name: 'Your example data' }).waitFor({ timeout: 300000 }).then(() => true).catch(() => false);
  let finished = false;
  done.then(() => { finished = true; });
  while (!finished && Date.now() - t0 < 300000) {
    const mb = treeRssMb();
    if (mb > peak) peak = mb;
    await new Promise((r) => setTimeout(r, 2000));
  }
  const ok = await done;
  mark('flow finished');
  console.log(`[mem] peak during processing: ${peak} MB`);
  console.log(`[result] consent page reached: ${ok}`);
  if (!ok) {
    console.log('[page text]', (await page.textContent('body') || '').slice(0, 400));
  }
  // Check the file-stats table actually lists the 4 members
  const rows = await page.locator('table tbody tr').count().catch(() => -1);
  console.log(`[result] table rows rendered: ${rows}`);
  await browser.close();
})();
