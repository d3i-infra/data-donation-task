// Peak-pressure harness: samples every 250 ms across the WHOLE flow
// (navigation -> consent page -> donate + settle), tracking the maximum instantaneous
// footprint of (a) the full browser process tree and (b) the renderer
// process alone (closest proxy for iOS WebContent). Reports absolute peaks.
const { chromium } = require('@playwright/test');
const { execSync } = require('child_process');
const fs = require('fs');

const ZIP = process.env.MEMTEST_ZIP;
if (!ZIP) { console.error('Set MEMTEST_ZIP=/path/to/test.zip'); process.exit(1); }
// Colon-separated for a multi-part (ArchiveSet) upload; a single path (no ':')
// is byte-compatible with today's behavior below.
const ZIP_PATHS = ZIP.split(':');
const LABEL = process.env.RUN_LABEL || 'unlabeled';
// Drives the two heading locators below so the harness can target a platform
// other than TikTok without touching phase logic or sampling.
const PLATFORM_LABEL = process.env.MEMTEST_PLATFORM_LABEL || 'TikTok';

function treePids(rootPid) {
  const lines = execSync('ps -eo pid=,ppid=').toString().trim().split('\n');
  const children = {};
  for (const l of lines) {
    const [pid, ppid] = l.trim().split(/\s+/).map(Number);
    (children[ppid] = children[ppid] || []).push(pid);
  }
  const pids = []; const stack = [rootPid];
  while (stack.length) { const p = stack.pop(); pids.push(p); for (const c of children[p] || []) stack.push(c); }
  return pids;
}

function typeOf(pid) {
  try {
    const cmd = fs.readFileSync(`/proc/${pid}/cmdline`).toString();
    const m = cmd.match(/--type=(\w+)/);
    return m ? m[1] : 'browser';
  } catch { return 'gone'; }
}

function rssKb(pid) {
  try {
    const s = fs.readFileSync(`/proc/${pid}/status`).toString();
    return Number((s.match(/^VmRSS:\s+(\d+)/m) || [])[1] || 0);
  } catch { return 0; }
}

(async () => {
  const server = await chromium.launchServer();
  const rootPid = server.process().pid;
  const browser = await chromium.connect(server.wsEndpoint());
  const page = await browser.newPage({ viewport: { width: 1280, height: 1600 } });
  page.on('pageerror', (e) => console.log('[pageerror]', String(e).slice(0, 200)));

  let peakTree = 0; let peakRenderer = 0; let peakPhase = ''; let phase = 'load';
  const rendererPeaks = {}; // per-phase renderer peak
  const sampler = setInterval(() => {
    let tree = 0; let renderer = 0;
    for (const pid of treePids(rootPid)) {
      const r = rssKb(pid);
      tree += r;
      if (typeOf(pid) === 'renderer') renderer = Math.max(renderer, r);
    }
    if (tree > peakTree) { peakTree = tree; peakPhase = phase; }
    if (renderer > peakRenderer) peakRenderer = renderer;
    rendererPeaks[phase] = Math.max(rendererPeaks[phase] || 0, renderer);
  }, 250);

  await page.goto('http://localhost:3000/');
  await page.getByRole('heading', { name: `Select your ${PLATFORM_LABEL} file` }).waitFor({ timeout: 180000 });
  phase = 'idle-ready';
  await page.waitForTimeout(3000);

  phase = 'upload+process';
  const chooser = page.waitForEvent('filechooser');
  await page.getByText('Choose file').click();
  await (await chooser).setFiles(ZIP_PATHS.length > 1 ? ZIP_PATHS : ZIP_PATHS[0]);
  await page.getByText('Continue').click();
  await page.getByRole('heading', { name: `Your ${PLATFORM_LABEL} data` }).waitFor({ timeout: 300000 });

  phase = 'render+settle';
  await page.waitForTimeout(12000);

  phase = 'donate';
  // Label comes from generate_review_data_prompt; adapt alongside the two
  // heading selectors when targeting a different platform/flow.
  await page.getByText('Yes, share for research').click();
  // The serialization spike is synchronous with the click; a fixed settle
  // captures it without coupling to whatever page the flow shows next.
  await page.waitForTimeout(8000);
  clearInterval(sampler);

  const mb = (kb) => Math.round(kb / 1024);
  console.log('RESULT>' + JSON.stringify({
    label: LABEL,
    peakTreeMb: mb(peakTree),
    peakRendererMb: mb(peakRenderer),
    peakPhase,
    rendererPeaksByPhase: Object.fromEntries(Object.entries(rendererPeaks).map(([k, v]) => [k, mb(v)])),
  }));
  await browser.close();
  await server.close();
})();
