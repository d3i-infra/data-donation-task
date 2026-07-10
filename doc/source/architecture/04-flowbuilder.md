# FlowBuilder

`FlowBuilder` is the base class for every per-platform donation flow. It
owns the complete lifecycle of a single platform's donation: prompting for
a file, validating it, extracting data, presenting a consent form, and
donating the result.

**File:** `packages/python/port/helpers/flow_builder.py`

---

## How it fits in

`script.py` is the study-level orchestrator. Each build targets one platform
(selected by `VITE_PLATFORM`): `script.py` validates that platform's config,
imports `port.platforms.<platform>`, and calls `module.process(session_id)`,
which returns `<Platform>Flow(session_id).start_flow()`. There is no platform
registry and no iteration — a build runs a single platform.

```mermaid
flowchart LR
    subgraph script.py
        P["process(session_id, platform)"]
    end
    subgraph selected["port.platforms (one, by VITE_PLATFORM)"]
        M["module.process(session_id)"]
        PF["PlatformFlow(session_id)"]
    end
    subgraph FlowBuilder
        SF["start_flow()"]
    end

    P -- "validate + import_module" --> M
    M -- "returns" --> PF
    PF -- "inherits" --> SF
```

---

## The flow

`start_flow()` is a generator that implements a fixed lifecycle. It loops
so the participant can retry file selection, and breaks out of the loop once
extraction succeeds. The upload arrives as a `PayloadFile` whose value is an
`AsyncFileAdapter` — a seekable file-like passed directly to validation and
extraction, never materialized to a path.

```mermaid
flowchart TD
    A["1. Render file prompt\nCommandUIRender + PropsUIPromptFileInput"]
    B{"PayloadFile?"}
    D["2. Safety check\nuploads.check_payload_size()\nsize metadata only"]
    E{"Safe?"}
    F["Render safety error page\nreturn"]
    G["3. Validate\nself.validate_file(archive)"]
    H{"Valid?\nstatus == 0"}
    I["4. Retry prompt\nCommandUIRender + PropsUIPromptConfirm"]
    J{"Try again?"}
    K["5. Extract\nself.extract_data(archive, validation)"]
    L["6. Log extraction summary\nph.emit_log() — counts only"]
    M{"7. Any tables?"}
    N["Render no-data page\nreturn"]
    O["8. Render consent form\nCommandUIRender + PropsUIPromptConsentFormViz"]
    P{"PayloadJSON\nor PayloadFalse?"}
    Q["9. Donate\nCommandSystemDonate(session_id-platform, json)"]
    R{"Donation\nsucceeded?"}
    S["Render failure page\nreturn"]
    T["Flow complete\nreturn"]

    A --> B
    B -- "no (skip)" --> T
    B -- "yes" --> D --> E
    E -- "no" --> F
    E -- "yes" --> G --> H
    H -- "no" --> I --> J
    J -- "PayloadTrue" --> A
    J -- "PayloadFalse" --> T
    H -- "yes" --> K --> L --> M
    M -- "no tables" --> N
    M -- "yes" --> O --> P
    P --> Q --> R
    R -- "failed + not decline" --> S
    R -- "success or decline" --> T
```

---

## Emit log milestones

`start_flow()` calls `ph.emit_log()` at each significant step. These are the
messages that appear in the host's log stream. They are always PII-free —
platform name, status code, and counts, never participant data.

| Step | Log message |
|---|---|
| Upload prompt sent | `[Platform] Upload prompt sent` |
| Upload received | `[Platform] Upload received: size=N` |
| Upload skipped | `[Platform] Upload skipped: type=PayloadFalse` |
| Safety check failed | `[Platform] Safety check failed: FileTooLargeError` |
| Validation passed | `[Platform] Validation: valid (category_id)` |
| Validation failed | `[Platform] Validation: invalid` |
| Extraction complete | `[Platform] Extraction complete: N tables, M rows; errors: ErrorType×count` |
| Consent form shown | `[Platform] Consent form shown` |
| Consent accepted | `[Platform] Consent: accepted` |
| Consent declined | `[Platform] Consent: declined` |
| Donation started | `[Platform] Donation started: payload size=N bytes` |
| Donation result | `[Platform] Donation result: success/failed` |

---

## Implementing a platform

Subclass `FlowBuilder` and implement two methods:

```python
class LinkedInFlow(FlowBuilder):
    def __init__(self, session_id: str):
        super().__init__(session_id, "LinkedIn")  # sets self.platform_name

    def validate_file(self, file) -> validate.ValidateInput:
        return validate.validate_zip(DDP_CATEGORIES, file)

    def extract_data(self, file, validation: validate.ValidateInput) -> ExtractionResult:
        return extraction(file, validation)
```

- `validate_file(archive)` — returns a `ValidateInput`. Status 0 = valid; non-zero = invalid.
  `archive` is the upload adapter (seekable file-like), not a path.
- `extract_data(archive, validation)` — returns an `ExtractionResult`. Can also be a generator
  (`yield from`) if you need to yield intermediate commands during extraction.

Everything else — the file prompt, the retry loop, the consent form, the
donation, the logging — is handled by `start_flow()`.

---

## UI text

`FlowBuilder.__init__()` calls `_initialize_ui_text()`, which builds a dict
of `Translatable` strings for the file prompt header, consent form header,
retry header, and review description. These are constructed from
`self.platform_name` so they automatically use the platform name you pass to
`super().__init__()`.

Override `_initialize_ui_text()` or modify `self.UI_TEXT` after `super().__init__()`
if you need custom text.

---

## Key files

| File | Role |
|---|---|
| `packages/python/port/helpers/flow_builder.py` | `FlowBuilder` base class |
| `packages/python/port/script.py` | `process()` — iterates platforms |
| `packages/python/port/platforms/linkedin.py` | Example platform implementation |
| `packages/python/port/helpers/port_helpers.py` | `emit_log`, `render_page`, `donate` helpers |

---

→ [Extraction](05-extraction.md) — how `extract_data()` works inside a platform
