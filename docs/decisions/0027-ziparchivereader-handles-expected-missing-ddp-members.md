---
adr_id: "0006"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-20 15:29:51"
    - author: Danielle McCool
      comment: "2"
      date: "2026-03-21 15:15:01"
    - author: Danielle McCool
      comment: "3"
      date: "2026-03-21 15:15:11"
    - author: Danielle McCool
      comment: "4"
      date: "2026-03-21 15:24:08"
links:
    precedes: []
    succeeds: []
status: decided
tags:
    - extraction-helpers
    - ddp-compatibility
    - error-handling
title: Handle expected-missing DDP files without error cascades
---

## <a name="question"></a> Context and Problem Statement

extract_file_from_zip() treats a missing file as an exception — logging an error and returning empty bytes. Downstream helpers then try to parse the empty bytes (2 more errors) and platform code catches the empty result (1 more error). This produces 4 error log lines per missing file. For platforms like Facebook where DDPs vary by version — language — and download options — dozens of files are routinely absent. A single Facebook extraction produces 31K lines of diagnostic noise in the browser console and 559+ TimestampParseError counts. How should extraction helpers distinguish expected-missing files from actual errors?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Return a sentinel from extract_file_from_zip for missing files — callers skip without cascading
2. <a name="option-2"></a> Pre-filter DDP_CATEGORIES against the zip namelist before extraction
3. <a name="option-3"></a> Keep current behavior but downgrade missing-file logs to DEBUG
4. <a name="option-4"></a> Hybrid: validation caches archive inventory — ZipArchiveReader resolves members and signals found/not-found — platforms skip absent files

## <a name="criteria"></a> Decision Drivers
DDPs vary across platform versions — languages — and download time ranges — many files in DDP_CATEGORIES will not be present in any given DDP
The current cascade (FileNotFound → empty BytesIO → JSONDecodeError × 2 → KeyError) produces ~4 error lines per missing file
Error counts in ExtractionResult become inflated with expected absences — making real errors hard to spot
Facebook 3-year DDP produced 31K lines of Pyodide stderr and 559 TimestampParseError counts
Researchers configuring which tables to extract need to know which files actually failed vs which were simply absent
### Pros and Cons

**Return a sentinel from extract_file_from_zip for missing files — callers skip without cascading**
* Good, because eliminates the cascade at the source
* Good, because callers can distinguish missing from corrupt
* Neutral, because requires updating all platform extraction functions to check the sentinel

**Pre-filter DDP_CATEGORIES against the zip namelist before extraction**
* Good, because only attempts extraction on files known to exist
* Good, because eliminates all missing-file errors at once
* Neutral, because requires a new step between validation and extraction

**Keep current behavior but downgrade missing-file logs to DEBUG**
* Good, because minimal code change
* Bad, because the cascade still happens — just quieter
* Bad, because error counts remain inflated


## <a name="outcome"></a> Decision Outcome
We decided for [Option 4](#option-4) because: Validation already walks the zip namelist — caching it on ValidateInput avoids redundant zip opens (25+ per Facebook extraction). ZipArchiveReader in helpers/ provides json()/csv()/raw() methods with found/not-found result types so platforms can cleanly skip absent files with 0 false errors. Deterministic path-boundary-aware resolution replaces the fragile first-regex-suffix-match behavior that caused wrong-file extractions. Options 1-2 were partial: Option 1 (sentinel) still cascades through read_json_from_bytes. Option 2 (pre-filter) puts file-existence checks in the wrong layer. Option 3 (downgrade logs) was rejected — hiding errors is not fixing them. The hybrid approach keeps responsibilities clean: validation discovers — helper resolves — platform parses.

## <a name="comments"></a> Comments
<a name="comment-4"></a>4. (2026-03-21 15:24:08) Danielle McCool: Follow-up: ExtractionResult.errors (Counter[str]) currently provides type→count only. For richer diagnostics (e.g. AmbiguousMemberMatch should include the requested filename) — consider extending to a structured error list alongside the Counter. This feeds into the future consent-gated error donation page. Not in scope for the initial ZipArchiveReader implementation.
