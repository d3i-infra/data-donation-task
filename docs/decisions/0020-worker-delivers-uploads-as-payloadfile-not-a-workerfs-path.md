---
adr_id: "0003"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-16 14:57:59"
    - author: Danielle McCool
      comment: "2"
      date: "2026-03-16 15:14:32"
    - author: Danielle McCool
      comment: "3"
      date: "2026-03-16 16:28:00"
    - author: Danielle McCool
      comment: "4"
      date: "2026-03-16 16:28:08"
    - author: Danielle McCool
      comment: "5"
      date: "2026-03-17 13:21:12"
links:
    precedes: []
    succeeds: []
status: decided
tags:
    - worker-protocol
    - file-delivery
    - backwards-compatibility
title: Migrate file delivery from WORKERFS path to PayloadFile reader
---

## <a name="question"></a> Context and Problem Statement

The py_worker delivers files from the browser to Python scripts running in Pyodide. The old approach (WORKERFS via d3i_py_worker.js) copies the entire file into Pyodide's virtual filesystem and passes a path string (PayloadString). The new approach (py_worker.js, from eyra/feldspar) passes an on-demand file reader (PayloadFile) that avoids the full copy, preventing OOM crashes on large DDPs. During the eyra/feldspar integration, App.tsx was switched from d3i_py_worker.js to py_worker.js (Task 8), but the Python scripts were not updated to handle PayloadFile. The result: script.py checks file_result.__type__ == 'PayloadString', gets 'PayloadFile' instead, falls to the else branch, and the entire extraction is skipped. Multiple researcher forks have scripts in production that expect PayloadString paths. How should the worker transition from WORKERFS/PayloadString to PayloadFile while maintaining backwards compatibility?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Revert to WORKERFS only
2. <a name="option-2"></a> Switch to PayloadFile only, update all scripts immediately
3. <a name="option-3"></a> Support both: WORKERFS copy with PayloadString plus PayloadFile reader, deprecation path
4. <a name="option-4"></a> Support both: let scripts opt in to PayloadFile via a capability flag

## <a name="criteria"></a> Decision Drivers
Memory safety: Large DDPs (100MB+) cause OOM when fully copied into Pyodide WORKERFS. PayloadFile reads slices on demand, avoiding this.
Backwards compatibility: 5+ platform scripts in this repo and scripts in researcher forks use PayloadString paths with zipfile.ZipFile(path). Breaking these blocks all studies.
Upstream alignment: eyra/feldspar uses PayloadFile. Staying aligned reduces future merge friction.
Transition time: Researcher forks cannot be updated on our schedule. Need a deprecation path, not a flag day.
Complexity budget: The worker is a ~150-line JS file. Adding too much abstraction creates maintenance burden.
### Pros and Cons

**Revert to WORKERFS only**
* Good, because all existing scripts work without modification
* Good, because minimal code change (revert unwrap())
* Bad, because loses the OOM fix — large DDPs will still crash
* Bad, because diverges from eyra upstream direction

**Switch to PayloadFile only, update all scripts immediately**
* Good, because clean break — one protocol, no legacy
* Good, because fully aligned with eyra upstream
* Bad, because requires updating every platform script in this repo (5 platforms)
* Bad, because breaks all researcher forks that haven't updated their scripts

**Support both: WORKERFS copy with PayloadString plus PayloadFile reader, deprecation path**
* Good, because existing scripts work immediately
* Good, because PayloadFile is available for new/updated scripts
* Neutral, because file is still copied (memory cost exists for legacy scripts)
* Bad, because dual-protocol complexity in the worker
* Bad, because no incentive to migrate (WORKERFS just works)

**Support both: let scripts opt in to PayloadFile via a capability flag**
* Good, because existing scripts work without modification (WORKERFS default)
* Good, because new scripts get the memory benefit of PayloadFile
* Good, because migration happens per-script, at each script author's pace
* Good, because clear deprecation path: eventually flip the default, then remove WORKERFS
* Neutral, because adds a capability negotiation mechanism to the worker


## <a name="outcome"></a> Decision Outcome
We decided for [Option 2](#option-2) because: PayloadFile does not cross the feldspar-mono boundary (postMessage bridge) — it is entirely internal to the iframe worker. Platform scripts (linkedin.py, twitter.py, etc.) are unaffected because script.py materializes the AsyncFileAdapter to a temp file path before calling them. Only script.py needs a small PayloadFile branch, making the migration trivial. The WORKERFS fallback and env var switch added complexity without meaningful benefit, since no boundary or deployment constraint requires the old protocol. Aligning with eyra upstream on PayloadFile-only eliminates dual-protocol maintenance.

## <a name="comments"></a> Comments
<a name="comment-5"></a>5. (2026-03-17 13:21:12) Danielle McCool: Implementation update (2026-03-17): The extraction consolidation design moves PayloadFile materialization from script.py into helpers/uploads.py materialize_file(). FlowBuilder calls materialize_file() which handles both PayloadFile (write to /tmp) and PayloadString (pass through path). Platform validate_file() and extract_data() continue to receive path strings. main.py ScriptWrapper still wraps JS readers with AsyncFileAdapter. The original note that 'only script.py needs a small PayloadFile branch' is superseded — the branch now lives in shared infrastructure called by FlowBuilder.
