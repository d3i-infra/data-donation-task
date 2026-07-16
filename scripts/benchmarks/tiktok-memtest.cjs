// TikTok memory test: drive a real JSON-format TikTok DDP through the full
// flow, sampling RSS over THIS run's browser process tree only (immune to
// other chrome-headless-shell processes on the machine).
const { chromium } = require('@playwright/test');
const { execSync } = require('child_process');

const ZIP = process.env.MEMTEST_ZIP;
if (!ZIP) { console.error('Set MEMTEST_ZIP=/path/to/test.zip'); process.exit(1); }

function treeRssMb(rootPid) {
  try {
    const lines = execSync('ps -eo pid=,ppid=,rss=').toString().trim().split('\n');
    const children = {}; const rss = {};
    for (const l of lines) {
      const [pid, ppid, r] = l.trim().split(/\s+/).map(Number);
      (children[ppid] = children[ppid] || []).push(pid);
      rss[pid] = r;
    }
    let total = 0; const stack = [rootPid];
    while (stack.length) {
      const p = stack.pop();
      total += rss[p] || 0;
      for (const c of children[p] || []) stack.push(c);
    }
    return Math.round(total / 1024);
  } catch { return -1; }
}

(async () => {
  const server = await chromium.launchServer();
  const rootPid = server.process().pid;
  const browser = await chromium.connect(server.wsEndpoint());
  const page = await browser.newPage({ viewport: { width: 1280, height: 1600 } });
  page.on('pageerror', (e) => console.log('[pageerror]', String(e).slice(0, 300)));

  const timeline = [];
  let phase = 'startup';
  const sampler = setInterval(() => timeline.push({ phase, mb: treeRssMb(rootPid) }), 1000);

  await page.goto('http://localhost:3000/');
  await page.getByRole('heading', { name: 'Select your TikTok file' }).waitFor({ timeout: 120000 });
  const baseline = treeRssMb(rootPid);
  console.log(`[mem] baseline, app ready (pyodide loaded): ${baseline} MB`);

  phase = 'upload';
  const chooser = page.waitForEvent('filechooser');
  await page.getByText('Choose file').click();
  await (await chooser).setFiles(ZIP);
  const afterAttach = treeRssMb(rootPid);
  console.log(`[mem] file attached: ${afterAttach} MB (delta ${afterAttach - baseline})`);

  phase = 'processing';
  const t0 = Date.now();
  await page.getByText('Continue').click();
  await page.getByRole('heading', { name: 'Your TikTok data' }).waitFor({ timeout: 300000 });
  const tProc = ((Date.now() - t0) / 1000).toFixed(1);

  phase = 'rendered';
  await page.waitForTimeout(6000);
  clearInterval(sampler);

  const final = treeRssMb(rootPid);
  const procSamples = timeline.filter((s) => s.phase === 'processing').map((s) => s.mb);
  const peakProc = Math.max(...(procSamples.length ? procSamples : [afterAttach]), afterAttach);

  console.log(`[mem] peak during validation+extraction: ${peakProc} MB (delta ${peakProc - baseline})`);
  console.log(`[mem] final, consent page rendered: ${final} MB (delta ${final - baseline})`);
  console.log(`[time] validation+extraction+render: ${tProc}s`);

  const tables = await page.locator('table').count();
  const svgWords = await page.locator('svg text').count();
  console.log(`[result] tables: ${tables}, wordcloud svg words: ${svgWords}`);
  await browser.close();
  await server.close();
})();
