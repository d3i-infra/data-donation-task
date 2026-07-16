// Advisor-spec memory A/B harness.
// - single fixed browser executable (logged, with version)
// - baseline & final = medians over stable sampling windows, not point reads
// - RSS and PSS summed over this run's process tree, split by process type
// - forced-GC diagnostic window after the final window
// - workload-identity assertions (per-table row labels, svg word count)
// Emits one JSON result line prefixed RESULT> for aggregation.
const { chromium } = require('@playwright/test');
const { execSync } = require('child_process');
const fs = require('fs');

const ZIP = process.env.MEMTEST_ZIP;
if (!ZIP) { console.error('Set MEMTEST_ZIP=/path/to/test.zip'); process.exit(1); }
const LABEL = process.env.RUN_LABEL || 'unlabeled';

function procTree(rootPid) {
  const lines = execSync('ps -eo pid=,ppid=').toString().trim().split('\n');
  const children = {};
  for (const l of lines) {
    const [pid, ppid] = l.trim().split(/\s+/).map(Number);
    (children[ppid] = children[ppid] || []).push(pid);
  }
  const pids = []; const stack = [rootPid];
  while (stack.length) {
    const p = stack.pop(); pids.push(p);
    for (const c of children[p] || []) stack.push(c);
  }
  return pids;
}

function classify(pid) {
  try {
    const cmd = fs.readFileSync(`/proc/${pid}/cmdline`).toString().replace(/\0/g, ' ');
    const m = cmd.match(/--type=(\w+)/);
    return m ? m[1] : 'browser';
  } catch { return 'gone'; }
}

function measure(rootPid) {
  const byType = {};
  let rss = 0; let pss = 0;
  for (const pid of procTree(rootPid)) {
    try {
      const roll = fs.readFileSync(`/proc/${pid}/smaps_rollup`).toString();
      const r = Number((roll.match(/^Rss:\s+(\d+)/m) || [])[1] || 0);
      const p = Number((roll.match(/^Pss:\s+(\d+)/m) || [])[1] || 0);
      rss += r; pss += p;
      const t = classify(pid);
      byType[t] = (byType[t] || 0) + p;
    } catch { /* process exited between listing and reading */ }
  }
  return { rssMb: Math.round(rss / 1024), pssMb: Math.round(pss / 1024), byType };
}

function median(xs) { const s = [...xs].sort((a, b) => a - b); return s[Math.floor(s.length / 2)]; }

async function windowStats(rootPid, seconds) {
  const rss = []; const pss = []; let last = null;
  for (let i = 0; i < seconds; i++) {
    last = measure(rootPid);
    rss.push(last.rssMb); pss.push(last.pssMb);
    await new Promise((r) => setTimeout(r, 1000));
  }
  const slope = (rss[rss.length - 1] - rss[0]) / seconds;
  return { rss: median(rss), pss: median(pss), slopeMbPerS: Math.round(slope * 10) / 10, byType: last.byType };
}

(async () => {
  const server = await chromium.launchServer();
  const rootPid = server.process().pid;
  const browser = await chromium.connect(server.wsEndpoint());
  console.log(`[env] label=${LABEL} browser=${browser.version()} exe=${chromium.executablePath()}`);
  const page = await browser.newPage({ viewport: { width: 1280, height: 1600 } });
  page.on('pageerror', (e) => console.log('[pageerror]', String(e).slice(0, 200)));

  await page.goto('http://localhost:3000/');
  await page.getByRole('heading', { name: 'Select your TikTok file' }).waitFor({ timeout: 180000 });
  await page.evaluate(() => document.fonts.ready);
  const base = await windowStats(rootPid, 15);

  const chooser = page.waitForEvent('filechooser');
  await page.getByText('Choose file').click();
  await (await chooser).setFiles(ZIP);
  await page.getByText('Continue').click();
  await page.getByRole('heading', { name: 'Your TikTok data' }).waitFor({ timeout: 300000 });
  await page.evaluate(() => document.fonts.ready);
  await page.waitForTimeout(8000); // viz workers settle
  const fin = await windowStats(rootPid, 20);

  // forced-GC diagnostic on the page's renderer
  try {
    const cdp = await page.context().newCDPSession(page);
    await cdp.send('HeapProfiler.collectGarbage');
  } catch (e) { console.log('[gc] cdp failed:', String(e).slice(0, 120)); }
  const postGc = await windowStats(rootPid, 10);

  const jsHeap = await page.evaluate(() => Math.round((performance).memory?.usedJSHeapSize / 1048576) || -1);
  // workload identity: per-table "N columns, M rows" labels + svg words
  const labels = await page.locator('text=/\\d+ columns/').allTextContents().catch(() => []);
  const rowLabels = (await page.getByText(/rows/).allTextContents().catch(() => [])).map((s) => s.trim());
  const svgWords = await page.locator('svg text').count();
  const tables = await page.locator('table').count();

  const result = {
    label: LABEL,
    browser: browser.version(),
    baseline: base, final: fin, postGc,
    deltaRss: fin.rss - base.rss, deltaPss: fin.pss - base.pss,
    postGcDeltaPss: postGc.pss - base.pss,
    jsHeapMb: jsHeap, tables, svgWords, rowLabels,
  };
  console.log('RESULT>' + JSON.stringify(result));
  await browser.close();
  await server.close();
})();
