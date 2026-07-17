---
status: accepted
date: "2026-07-16"
category: Data collector
applies_to:
    - packages/data-collector/src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/useVisualizationData.tsx
    - packages/data-collector/src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/selectVisualizationColumns.ts
    - packages/data-collector/src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/visualizationDataWorker.ts
priority: default
companions:
    - packages/data-collector/src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/selectVisualizationColumns.test.ts
checks:
    - desc: the hook still projects columns before postMessage
      grep: 'selectVisualizationColumns\('
      in: ["packages/data-collector/src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/useVisualizationData.tsx"]
      expect: present
    - desc: the hook still terminates its workers
      grep: 'worker\.terminate\(\)'
      in: ["packages/data-collector/src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/useVisualizationData.tsx"]
      expect: present
---

# Visualization workers are ephemeral and column-scoped

## Decision

Each visualization computation runs in a fresh Web Worker that receives only the columns the visualization reads and is terminated as soon as it answers. No persistent workers, no full-table postMessage.

## Guidance

- Post to a visualization worker only a table projected with `selectVisualizationColumns(table, visualization)`, and call `worker.terminate()` both in the message handler and in the effect cleanup — never keep a worker (and its structured-clone payload) alive across computations.
- A new visualization type must declare the columns it reads in `selectVisualizationColumns` (with a case in its test) before the worker can compute it.
- Review rejects module-scope or state-held workers, and any postMessage of an unprojected table.

## Why

postMessage structured-clones its payload into renderer memory; persistent full-table workers put 65k-row consent pages ~300 MB over the iOS-class kill threshold (issue #122; A/B benchmark 2026-07-16: peak renderer 1333 MB -> 1029 MB after this rule).
