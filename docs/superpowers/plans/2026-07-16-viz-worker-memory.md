# Viz-Worker Memory Fix (Issue #122) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut consent-page peak renderer memory by sending only viz-relevant columns to visualization workers, terminating each worker after it computes, and removing the double table parse in `consent_form_viz.tsx`; while touching the file, adopt the Milestone-8 `DonateButtons` component in place of the hand-rolled donate/cancel block.

**Provenance (verified 2026-07-16):** The entire `consent_form_viz/` tree (component, visualization_plugin, worker) is D3I-custom — upstream `eyra/feldspar` has no counterpart anywhere (added by D3I commit `3b884df`, "developed by @kasperwelbers"). Tasks 1–3 touch no shared upstream surface. Task 4 touches `packages/feldspar/src/index.ts`, which already diverges via a D3I exports block (being renamed from "EXPORTS ADDED BY NdS" in the same step); the addition is one line in that block. Our `button.tsx` deliberately diverges from upstream (PR #78 spinner-collapse fix) but has an identical props API, so `DonateButtons` (byte-identical to upstream in our tree) composes correctly with it.

**Architecture:** All changes are inside `packages/data-collector/src/components/consent_form_viz/`. A new pure function `selectVisualizationColumns(table, visualization)` projects a `Table` down to the columns a visualization actually reads (the worker resolves columns by name via `getTableColumn`, so projection is transparent to it). `useVisualizationData` is restructured from one persistent worker per figure to an ephemeral worker per computation — spawned in the `[table, visualization]` effect, terminated on result and in effect cleanup. The double parse is fixed with a skip-first-render ref. **No donated data is truncated or dropped** — projection affects only what crosses `postMessage` into the worker; React state, the table UI, and the donation payload are untouched (issue #122 advisor review: display-side changes must never shrink the donated dataset).

**Tech Stack:** TypeScript/React 19 (Vite), Web Workers, zod (existing types), jest + ts-jest (new to `@eyra/data-collector`, mirroring `@eyra/feldspar`), Playwright e2e, memory benchmark harnesses in `scripts/benchmarks/`.

## Global Constraints

- **Never truncate or mutate donated data** — projection is for the worker message only (issue #122).
- **No real participant DDPs in version control** (ADR-0014) — the reference DDP stays a local file referenced via `MEMTEST_ZIP`.
- **ADR-0016** (prefer standard feldspar prompts) — satisfied: we modify the existing custom consent-viz prompt, no new custom prompt.
- **Claude never runs `git commit` or `git push`** — each commit step stages files and hands the exact command to Danielle.
- **Branch:** `fix/viz-worker-memory` off `development`, landed via PR to `d3i-infra/data-donation-task`. Use a worktree (superpowers:using-git-worktrees) at execution time.
- **Acceptance:** peak renderer RSS ≤ ~824 MB on the 65k-row reference DDP, measured with `MEMTEST_ZIP=<local ddp> node scripts/benchmarks/memtest-v3-peak.cjs` (methodology: `scripts/benchmarks/README.md`). The harness gains a `donate` phase (Task 5) so the donate-click serialization spike — the moment iOS memory kills are most likely — is measured, not assumed; A/B numbers must include it.
- **Button-pattern memory verdict (code-verified, to be confirmed by the donate-phase numbers):** direct import of `DonateButtons` is allocation-identical to the current hand-rolled block (one transient `serializeConsentData()` stringify at click). The factory route via M8's `DataSubmissionPage` is memory-negative as shipped: it (a) holds a serialized copy of all table data in a page-lifetime ref (`DataSubmissionData`, pushed via `onDataSubmissionDataChanged` on mount and every edit), (b) stacks `Object.fromEntries` + `JSON.stringify` copies on top of that at donate click, and (c) does an extra payload-sized `JSON.stringify` purely for `console.log` at `data_submission_page.tsx:23,29`. Do not adopt the factory route for large tables until those upstream behaviors change (tracked in Task 7).
- **After editing:** run `adg lean index --model docs/decisions --root .` (validates the decision model; silent-on-success).

---

### Task 1: `selectVisualizationColumns` — pure column projection (+ jest infra for data-collector)

**Files:**
- Modify: `packages/data-collector/package.json` (add jest devDeps + test script)
- Create: `packages/data-collector/jest.config.js`
- Create: `packages/data-collector/src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/selectVisualizationColumns.ts`
- Test: `packages/data-collector/src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/selectVisualizationColumns.test.ts`

**Interfaces:**
- Consumes: `Table`, `VisualizationType`, `ChartVisualization`, `TextVisualization` from `../types` (existing zod-derived types).
- Produces: `selectVisualizationColumns(table: Table, visualization: VisualizationType): Table` — later tasks import it by this exact name.

- [ ] **Step 1: Add jest infrastructure (mirrors `packages/feldspar`)**

In `packages/data-collector/package.json`, change the test script and add devDependencies (versions copied from `packages/feldspar/package.json`):

```jsonc
// scripts:
"test": "jest",
// devDependencies (add):
"@types/jest": "30.0.0",
"jest": "30.4.2",
"ts-jest": "29.4.11",
```

Create `packages/data-collector/jest.config.js` (identical to feldspar's):

```js
/** @type {import('ts-jest').JestConfigWithTsJest} */
export default {
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: ['<rootDir>/src'],
  testMatch: ['**/*.test.ts'],
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx'],
  transform: {
    '^.+\\.tsx?$': ['ts-jest', {
      useESM: true,
    }],
  },
  extensionsToTreatAsEsm: ['.ts', '.tsx'],
};
```

Run: `pnpm install`
Expected: lockfile updated, deps installed.

- [ ] **Step 2: Write the failing test**

`selectVisualizationColumns.test.ts`:

```ts
import { selectVisualizationColumns } from './selectVisualizationColumns'
import { Table, VisualizationType } from '../types'

function makeTable (): Table {
  return {
    id: 'tiktok_videos',
    head: { cells: ['date', 'title', 'url', 'duration', 'category'] },
    body: {
      rows: [
        { id: 'r1', cells: ['2024-01-01', 'video one', 'https://a.example', '10', 'music'] },
        { id: 'r2', cells: ['2024-01-02', 'video two', 'https://b.example', '20', 'sports'] }
      ]
    }
  }
}

describe('selectVisualizationColumns', () => {
  it('keeps only the columns a chart visualization reads', () => {
    const visualization: VisualizationType = {
      title: { en: 'per day' },
      type: 'line',
      group: { column: 'date' },
      values: [{ column: 'duration', aggregate: 'sum', group_by: 'category' }]
    }
    const projected = selectVisualizationColumns(makeTable(), visualization)
    expect(projected.head.cells).toEqual(['date', 'duration', 'category'])
    expect(projected.body.rows).toEqual([
      { id: 'r1', cells: ['2024-01-01', '10', 'music'] },
      { id: 'r2', cells: ['2024-01-02', '20', 'sports'] }
    ])
    expect(projected.id).toBe('tiktok_videos')
  })

  it('does not materialize the .COUNT pseudo-column', () => {
    const visualization: VisualizationType = {
      title: { en: 'count per day' },
      type: 'bar',
      group: { column: 'date' },
      values: [{ column: '.COUNT' }]
    }
    const projected = selectVisualizationColumns(makeTable(), visualization)
    expect(projected.head.cells).toEqual(['date'])
  })

  it('keeps textColumn and valueColumn for a wordcloud', () => {
    const visualization: VisualizationType = {
      title: { en: 'words' },
      type: 'wordcloud',
      textColumn: 'title',
      valueColumn: 'duration'
    }
    const projected = selectVisualizationColumns(makeTable(), visualization)
    expect(projected.head.cells).toEqual(['title', 'duration'])
    expect(projected.body.rows[0].cells).toEqual(['video one', '10'])
  })

  it('drops referenced columns that do not exist in the table (worker reports the error)', () => {
    const visualization: VisualizationType = {
      title: { en: 'bad' },
      type: 'bar',
      group: { column: 'no_such_column' },
      values: [{ column: 'duration' }]
    }
    const projected = selectVisualizationColumns(makeTable(), visualization)
    expect(projected.head.cells).toEqual(['duration'])
  })

  it('preserves row ids so rowId-based deletion still works', () => {
    const visualization: VisualizationType = {
      title: { en: 'per day' },
      type: 'area',
      group: { column: 'date' },
      values: [{ column: 'duration' }]
    }
    const projected = selectVisualizationColumns(makeTable(), visualization)
    expect(projected.body.rows.map((r) => r.id)).toEqual(['r1', 'r2'])
  })
})
```

Note: `zAggregationValue.column` has a zod default of `.COUNT`, but these tests construct plain TS objects (no zod parse), so `column` is passed explicitly where needed.

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm --filter @eyra/data-collector test`
Expected: FAIL — `Cannot find module './selectVisualizationColumns'`.

- [ ] **Step 4: Write the implementation**

`selectVisualizationColumns.ts`:

```ts
import { ChartVisualization, TextVisualization, VisualizationType, Table } from '../types'

/**
 * Project a table down to only the columns the visualization reads, so that
 * postMessage structured-clones a fraction of the table into the worker
 * (issue #122). The worker resolves columns by name (getTableColumn), so the
 * projection is transparent to it. Display and donation data are unaffected.
 */
export function selectVisualizationColumns (table: Table, visualization: VisualizationType): Table {
  const columns = visualizationColumns(visualization).filter((column) => table.head.cells.includes(column))
  const indices = columns.map((column) => table.head.cells.indexOf(column))
  return {
    id: table.id,
    head: { cells: columns },
    body: {
      rows: table.body.rows.map((row) => ({
        id: row.id,
        cells: indices.map((index) => row.cells[index])
      }))
    }
  }
}

function visualizationColumns (visualization: VisualizationType): string[] {
  const columns = new Set<string>()

  if (['line', 'bar', 'area'].includes(visualization.type)) {
    const chart = visualization as ChartVisualization
    columns.add(chart.group.column)
    for (const value of chart.values) {
      if (value.column !== undefined) columns.add(value.column)
      if (value.group_by !== undefined) columns.add(value.group_by)
      if (value.z !== undefined) columns.add(value.z)
    }
  }

  if (visualization.type === 'wordcloud') {
    const text = visualization as TextVisualization
    columns.add(text.textColumn)
    if (text.valueColumn !== undefined) columns.add(text.valueColumn)
  }

  return Array.from(columns)
}
```

Rationale notes:
- `.COUNT` (and any column missing from `head.cells`, e.g. a typo in a viz spec) is filtered out; `getTableColumn` in the worker special-cases `.COUNT` and still throws its existing clear error for genuinely missing columns, so error behavior is unchanged.
- `value.z` is included defensively — it is in `zAggregationValue` even though `prepareChartData` does not read it today; if it starts being read, projection won't silently starve it.

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @eyra/data-collector test`
Expected: PASS (5 tests).

- [ ] **Step 6: Stage and hand off commit**

```bash
git add packages/data-collector/package.json packages/data-collector/jest.config.js pnpm-lock.yaml \
  packages/data-collector/src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/selectVisualizationColumns.ts \
  packages/data-collector/src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/selectVisualizationColumns.test.ts
```

Hand to Danielle:
`git commit -m "feat(viz): add selectVisualizationColumns projection + jest infra for data-collector"`

---

### Task 2: Ephemeral, column-scoped workers in `useVisualizationData`

**Files:**
- Modify: `packages/data-collector/src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/useVisualizationData.tsx` (full rewrite of the hook body)

**Interfaces:**
- Consumes: `selectVisualizationColumns(table, visualization): Table` from Task 1.
- Produces: unchanged hook signature `useVisualizationData(table: Table, visualization: VisualizationType): [VisualizationData | undefined, Status]` — `figure.tsx` needs no changes.

- [ ] **Step 1: Rewrite the hook**

Replace the entire contents of `useVisualizationData.tsx` with:

```tsx
import { VisualizationType, VisualizationData, Table } from '../types'
import { useEffect, useState } from 'react'
import { selectVisualizationColumns } from './selectVisualizationColumns'

type Status = 'loading' | 'success' | 'error'

export default function useVisualizationData (
  table: Table,
  visualization: VisualizationType
): [VisualizationData | undefined, Status] {
  const [visualizationData, setVisualizationData] = useState<VisualizationData>()
  const [status, setStatus] = useState<Status>('loading')

  useEffect(() => {
    if (window.Worker === undefined) {
      setStatus('error')
      return
    }
    setStatus('loading')
    // Spawn a worker per computation and terminate it as soon as it answers,
    // instead of keeping a persistent worker holding a clone of the table
    // alive for the lifetime of the figure (issue #122).
    const worker = new Worker(
      new URL('./visualizationDataWorker.ts', import.meta.url), { type: 'module' })
    worker.onmessage = (e: MessageEvent<{ status: Status, visualizationData: VisualizationData }>) => {
      setVisualizationData(e.data.visualizationData)
      setStatus(e.data.status)
      worker.terminate()
    }
    worker.postMessage({ table: selectVisualizationColumns(table, visualization), visualization })
    return () => {
      worker.terminate()
    }
  }, [table, visualization])

  return [visualizationData, status]
}
```

Behavior notes (verify while implementing):
- The cleanup `worker.terminate()` also kills stale in-flight computations when `table` changes (row delete/undo, search filter), eliminating the previous stale-response window. Terminating an already-terminated worker is a no-op.
- The old `setWorker` state and the dead `try/catch` around `setState` calls are gone; `window.Worker === undefined` now yields an explicit `'error'` status instead of silently staying `'loading'`.

- [ ] **Step 2: Typecheck/build**

Run: `pnpm build`
Expected: builds clean (this also type-checks `data-collector` via Vite/tsc).

- [ ] **Step 3: Manual smoke via dev server**

Run: `VITE_PLATFORM=tiktok BROWSER=none pnpm start`, load `http://localhost:3000`, drive a small test zip (`tests/test.zip`) to the consent page.
Expected: charts and wordclouds render; deleting rows updates figures; DevTools → Sources shows no lingering `visualizationDataWorker` threads after figures settle.

- [ ] **Step 4: Stage and hand off commit**

```bash
git add packages/data-collector/src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/useVisualizationData.tsx
```

Hand to Danielle:
`git commit -m "fix(viz): project columns before postMessage and terminate workers after computing (#122)"`

---

### Task 3: Fix the double parse in `consent_form_viz.tsx`

**Files:**
- Modify: `packages/data-collector/src/components/consent_form_viz/consent_form_viz.tsx:19,26-33`

**Interfaces:**
- Consumes: nothing from other tasks (independent).
- Produces: no API change.

- [ ] **Step 1: Skip the mount-time re-parse**

The lazy `useState` initializer (line 26) already parses `props.tables`; the `[props.tables]` effect (lines 31–33) re-parses the same value on mount. Keep the effect for genuine prop changes, skip its first run:

Change the react import (line 19):

```tsx
import { useCallback, useEffect, useRef, useState } from "react"
```

Replace lines 26–33:

```tsx
export const ConsentFormViz = (props: Props): JSX.Element => {
  const [tables, setTables] = useState<TableWithContext[]>(() => parseTables(props.tables))
  const { locale, resolve } = props
  const { description, donateQuestion, donateButton, cancelButton } = prepareCopy(props)
  const [isDonating, setIsDonating] = useState(false)
  // The state initializer above already parsed props.tables; only re-parse
  // when the host actually sends new tables (issue #122 double parse).
  // Previous-value comparison rather than a boolean flag: a flag with no
  // cleanup reset is defeated by StrictMode's dev double-invocation.
  const parsedTables = useRef(props.tables)

  useEffect(() => {
    if (parsedTables.current === props.tables) return
    parsedTables.current = props.tables
    setTables(parseTables(props.tables))
  }, [props.tables])
```

(Only the `parsedTables` ref and the effect guard are new; the surrounding lines are shown for placement.)

- [ ] **Step 2: Typecheck/build**

Run: `pnpm build`
Expected: clean.

- [ ] **Step 3: Stage and hand off commit**

```bash
git add packages/data-collector/src/components/consent_form_viz/consent_form_viz.tsx
```

Hand to Danielle:
`git commit -m "fix(viz): parse consent tables once on mount, not twice (#122)"`

---

### Task 4: Adopt `DonateButtons` (direct import) in `consent_form_viz.tsx`

Memory-neutral by construction (see Global Constraints); gains the M8 waiting behavior ("Transferring data… Please keep this window open."), deletes the hand-rolled duplicate of `donate_buttons.tsx`, and keeps D3I default copy by passing our existing bundles as props. Note `generate_review_data_prompt` (Python) always sets `donate_question`/`donate_button` explicitly, so the component defaults are a fallback only.

**Files:**
- Modify: `packages/feldspar/src/index.ts` (one line in the existing "EXPORTS ADDED BY NdS" block)
- Modify: `packages/data-collector/src/components/consent_form_viz/types.ts` (fix the latent `Text` type bug)
- Modify: `packages/data-collector/src/components/consent_form_viz/consent_form_viz.tsx`

**Interfaces:**
- Consumes: `DonateButtons({ onDonate, onCancel, locale, donateQuestion?, donateButton? })` from feldspar prompts (identical to upstream).
- Produces: no API change; donation payload format unchanged (`PayloadJSON` of the serialized tables array).

- [ ] **Step 1: Export DonateButtons from feldspar; rename the divergence marker**

In `packages/feldspar/src/index.ts`, rename the block comment `// EXPORTS ADDED BY NdS` to:

```ts
// D3I additions to the upstream feldspar exports
```

and add to that block:

```ts
export { DonateButtons } from './framework/visualization/react/ui/prompts/donate_buttons'
```

- [ ] **Step 2: Fix the latent `Text` type**

`consent_form_viz/types.ts` uses `Text` without defining or importing it — today it silently resolves to the **DOM global `Text`** node type. Passing `props.donateQuestion` to `DonateButtons` (which expects feldspar's `Text = Translatable | string`) surfaces this. Add at the top of `types.ts`, following the plugin's type-duplication convention (see `visualization_plugin/types.ts` header comment):

```ts
// Matching feldspar's Text/Translatable (duplicated like visualization_plugin/types.ts
// to keep the plugin self-contained). Previously `Text` silently resolved to the DOM
// global Text node type.
export interface Translatable {
  translations: { [locale: string]: string }
}
export type Text = Translatable | string
```

If `pnpm build` then flags `Translator.translate` call sites, resolve per-site with the real type (do not cast to `any`).

- [ ] **Step 3: Replace the hand-rolled button block**

In `consent_form_viz.tsx`:

Imports — drop `LabelButton`/`PrimaryButton`, add `DonateButtons`:

```tsx
import {
  DonateButtons,
  BodyLarge,
  Translator,
  ReactFactoryContext,
} from "@eyra/feldspar"
```

Remove the `isDonating` state (line 29) and simplify `handleDonate`:

```tsx
  function handleDonate(): void {
    const value = serializeConsentData()
    resolve?.({ __type__: "PayloadJSON", "value": value })
  }
```

Shrink `prepareCopy`/`Copy` to `description` only (DonateButtons owns question/button/cancel copy):

```tsx
interface Copy {
  description: string
}

function prepareCopy({ description, locale }: Props): Copy {
  return {
    description: Translator.translate(description ?? defaultDescription, locale),
  }
}
```

and destructure only `const { description } = prepareCopy(props)`.

Replace the question + buttons block (previously lines 174–186) with:

```tsx
        <DonateButtons
          onDonate={handleDonate}
          onCancel={handleCancel}
          locale={locale}
          donateQuestion={props.donateQuestion ?? defaultDonateQuestionLabel}
          donateButton={props.donateButton ?? defaultDonateButtonLabel}
        />
```

Delete `defaultCancelButtonLabel` and `defaultDonateQuestionLabel`/`defaultDonateButtonLabel`? **No** — delete only `defaultCancelButtonLabel` (cancel copy now comes from DonateButtons); keep the donate bundles so D3I "share for research" wording remains the fallback.

- [ ] **Step 4: Build and smoke**

Run: `pnpm build`
Expected: clean. Then dev-server smoke: donate on a small zip shows the spinner **and** the "Transferring data… Please keep this window open." message; cancel resolves false.

- [ ] **Step 5: Stage and hand off commit**

```bash
git add packages/feldspar/src/index.ts \
  packages/data-collector/src/components/consent_form_viz/types.ts \
  packages/data-collector/src/components/consent_form_viz/consent_form_viz.tsx
```

Hand to Danielle:
`git commit -m "refactor(viz): adopt M8 DonateButtons in consent_form_viz, fix latent Text type"`

---

### Task 5: Add a `donate` phase to `memtest-v3-peak.cjs`

The harness currently stops at `render+settle` — the donate-click serialization spike is unmeasured, yet it is the likeliest iOS kill point and the exact place button-pattern choices differ. The harness already tracks per-phase renderer peaks, so this is additive.

**Files:**
- Modify: `scripts/benchmarks/memtest-v3-peak.cjs:73-75`
- Modify: `scripts/benchmarks/README.md` (document the new phase)

**Interfaces:**
- Consumes: nothing from other tasks (works on both `development` and the fix branch — needed for the A/B baseline).
- Produces: `rendererPeaksByPhase.donate` in the `RESULT>` JSON.

- [ ] **Step 1: Extend the flow**

Replace lines 73–75 of `memtest-v3-peak.cjs`:

```js
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
```

Also update the header comment (line 2) from `(navigation -> consent page + settle)` to `(navigation -> consent page -> donate + settle)`.

- [ ] **Step 2: Document in the benchmarks README**

In `scripts/benchmarks/README.md`, extend the `memtest-v3-peak.cjs` row / notes: the flow now includes clicking the donate button, and `rendererPeaksByPhase.donate` isolates the donation-serialization spike; the donate-button label selector must be adapted along with the two heading selectors for non-TikTok flows.

- [ ] **Step 3: Verify on a small zip**

Run (against a running build, e.g. dev server): `MEMTEST_ZIP=tests/test.zip node scripts/benchmarks/memtest-v3-peak.cjs`
Expected: `RESULT>` JSON includes a `donate` key in `rendererPeaksByPhase`.

- [ ] **Step 4: Stage and hand off commit**

```bash
git add scripts/benchmarks/memtest-v3-peak.cjs scripts/benchmarks/README.md
```

Hand to Danielle:
`git commit -m "feat(benchmarks): measure donate-click peak as its own phase in memtest-v3-peak"`

---

### Task 6: Verification — tests, e2e, benchmark A/B, adg index

**Files:**
- No source changes; produces benchmark evidence for the PR description.

**Interfaces:**
- Consumes: the completed Tasks 1–5 on the branch.
- Produces: pass/fail against the #122 acceptance criterion (peak renderer ≤ ~824 MB), including the donate-phase peak, for both `development` and the fix branch.

- [ ] **Step 1: Full test suite**

Run:
```bash
pnpm --filter @eyra/data-collector test
pnpm --filter @eyra/feldspar test
pnpm test            # python tests
pnpm typecheck:py
pnpm build
pnpm test:e2e        # Playwright donation.spec.ts
```
Expected: all pass.

- [ ] **Step 2: Benchmark the branch (needs the local reference DDP from Danielle)**

Per `scripts/benchmarks/README.md` (v3 peak methodology; the harness expects the TikTok flow):

```bash
VITE_PLATFORM=tiktok NODE_ENV=development pnpm run build
cd packages/data-collector/dist && python3 -m http.server 3000 &
cd -
MEMTEST_ZIP=/path/to/65k-row-reference.zip node scripts/benchmarks/memtest-v3-peak.cjs
```

Expected: peak renderer RSS ≤ ~824 MB. Also run the same (Task 5-extended) harness on a `development` build — clean-build both artifacts with one toolchain, same Chromium, interleaved runs per README — and report per-phase renderer peaks for both, as deltas over each run's own baseline. The `donate` phase numbers are the evidence for the button-pattern verdict in Global Constraints: direct-import `DonateButtons` must show no donate-phase regression vs `development`.

- [ ] **Step 3: Validate the decision model**

Run: `adg lean index --model docs/decisions --root .`
Expected: exits clean (silent on success; the 0016 "no Why yet" warning is pre-existing).

- [ ] **Step 4: Stage plan doc; hand off branch**

```bash
git add docs/superpowers/plans/2026-07-16-viz-worker-memory.md
```

Hand to Danielle: commit command plus push/PR commands (stating the remote URL, e.g. `https://github.com/d3i-infra/data-donation-task.git`, branch `fix/viz-worker-memory` → base `development`). PR references #122 and includes the benchmark table.

---

### Task 7 (recommended): Record the pattern as a lean ADR; track the factory-route findings

**Files:**
- Create: `docs/decisions/00XX-*.md` via `adg lean new` (number assigned by the tool)
- Modify: `/home/dmm/src/d3i/UPSTREAM_REQUESTS.md` (outside this repo — not part of the branch)

**Interfaces:**
- Consumes: the donate-phase benchmark numbers from Task 6 (cite them in the records).
- Produces: ADR authored with the write-adr plugin (write-lean-adr skill), never by hand.

- [ ] **Step 1: Author the record**

Use the write-lean-adr skill / `adg lean new`. Substance to capture:
- **Decision:** Visualization workers are ephemeral and column-scoped — send only the columns a visualization reads, terminate the worker once it answers. Consent-viz consumes `DonateButtons` by direct import, not via the `DataSubmissionPage` factory route.
- **Invariant:** Display-side memory work must never shrink the donated dataset; any future row truncation must be display-only (issue #122 advisor review).
- **Why (factory route rejected for now):** M8's `DataSubmissionPage` holds serialized prompt data in a page-lifetime ref, stacks extra full copies at donate click, and stringifies the payload again for `console.log` — measurably hostile to the iOS peak-memory budget on large tables. Also implies breaking changes to the Python page composition and donated payload shape.
- **applies_to:** `packages/data-collector/src/components/consent_form_viz/**`

- [ ] **Step 2: Track the upstream proposal**

Add an entry to `/home/dmm/src/d3i/UPSTREAM_REQUESTS.md` (per its convention: repo, title, diagnosis, suggested fix, date): `eyra/feldspar` — `DataSubmissionPage` donation flow allocates several payload-sized copies at donate time (`data_submission_page.tsx`: page-lifetime `DataSubmissionData` ref, `Object.fromEntries`+`JSON.stringify` at click, payload-sized `JSON.stringify` inside `console.log` at lines 23/29). Suggested fix: pull prompt data lazily at donate time and drop payload logging. Cite the donate-phase benchmark numbers from Task 6.

- [ ] **Step 3: Re-index and stage**

Run: `adg lean index --model docs/decisions --root .`
Expected: exits clean. Stage the new record; hand commit command to Danielle.

---

## Self-Review (done at planning time)

- **Spec coverage:** #122's three fix items map to Tasks 1+2 (columns + terminate) and Task 3 (double parse); the acceptance criterion is Task 6 Step 2, now including the donate-click phase added in Task 5. Danielle's follow-ups are covered: provenance (header note), M8 button adoption (Task 4, same PR as agreed), factory-vs-direct memory question (Global Constraints verdict + Task 5/6 measurement + Task 7 tracking), NdS-block rename (Task 4 Step 1). The issue's `truncateRows`/`MAX_ROWS` observation is deliberately out of scope — the advisor-reviewed fix forbids truncating donated data.
- **Placeholder scan:** all code steps carry full code; commands carry expected output.
- **Type consistency:** `selectVisualizationColumns(table: Table, visualization: VisualizationType): Table` is the only cross-task interface; Task 2 imports it by that exact name and signature. Task 4's `Text` fix is self-contained in `consent_form_viz/types.ts`.
