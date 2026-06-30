# Migrating to v2.0.0

This guide is for researchers with downstream forks of d3i-infra/data-donation-task
who need to update their code for the v2.0.0 release.

## What changed and why

The extraction architecture was consolidated around **FlowBuilder** — a template
class that handles the common donation flow (file prompt → validation → extraction →
consent → donation) so that each platform only needs to implement two methods:
`validate_file()` and `extract_data()`.

This replaces the previous pattern where each platform's `process()` function
contained the full flow logic, including UI construction, retry handling, and
donation. The old `d3i_example_script.py` and `donation_flows/` system have been
removed.

For detailed rationale, see:
- `docs/decisions/0010-no-parallel-extraction-architecture.md` — consolidation decision
- `docs/decisions/0023-flowbuilder-template-for-per-platform-extraction-flows.md` — FlowBuilder template pattern
- `docs/decisions/0027-ziparchivereader-handles-expected-missing-ddp-members.md` — ZipArchiveReader and error handling

## If you use the default platforms as-is

Minimal changes needed:

1. **Update your fork** — merge or rebase onto v2.0.0
2. **Check `DDP_CATEGORIES`** — each platform defines which file formats it
   supports. If your participants use a DDP format not listed, add a new
   `DDPCategory` entry
3. **Verify** — run `pnpm doctor` to check your environment, `pnpm test` to
   run the test suite, `pnpm start` to test locally

## If you have a custom script.py

The main migration. Here's what changed:

### Before (v1.x pattern)

```python
# script.py or d3i_example_script.py
def process(sessionId):
    # Manually build file prompt
    promptFile = prompt_file("application/zip, text/plain")
    fileResult = yield render_page([promptFile])

    if fileResult.__type__ == "PayloadString":
        zipfile_ref = get_zipfile(fileResult.value)
        files = get_files(zipfile_ref)
        extraction_result = []
        for filename in files:
            extraction_result.append(extract_file(zipfile_ref, filename))

        # Manually build consent UI
        for prompt in prompt_consent(extraction_result):
            result = yield prompt
            if result.__type__ == "PayloadJSON":
                yield donate(f"{sessionId}-key", result.value)
```

### After (v2.0.0 FlowBuilder pattern)

```python
# platforms/my_platform.py
from port.helpers.flow_builder import FlowBuilder
from port.helpers.validate import DDPCategory, DDPFiletype, Language
import port.helpers.validate as validate

DDP_CATEGORIES = [
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=["data.json", "profile.json"]
    )
]

class MyPlatformFlow(FlowBuilder):
    def __init__(self, session_id: str):
        super().__init__(session_id, "MyPlatform")

    def validate_file(self, file):
        return validate.validate_zip(DDP_CATEGORIES, file)

    def extract_data(self, file_value, validation):
        return extraction(file_value, validation)

def process(session_id):
    flow = MyPlatformFlow(session_id)
    return flow.start_flow()
```

```python
# script.py — thin orchestrator
from importlib import import_module
import port.helpers.port_helpers as ph

PLATFORM_REGISTRY = [
    ("MyPlatform", "port.platforms.my_platform", "MyPlatformFlow"),
]

def process(session_id: str, platform: str | None = None):
    entries = PLATFORM_REGISTRY
    if platform:
        entries = [(n, m, c) for n, m, c in entries if n.lower() == platform.lower()]

    for name, module_path, class_name in entries:
        mod = import_module(module_path)
        FlowClass = getattr(mod, class_name)
        flow = FlowClass(session_id)
        yield from flow.start_flow()
```

FlowBuilder handles: file prompting, retry on invalid files, upload safety
checks, consent UI, donation, and no-data acknowledgment. You only write the
validation and extraction logic.

See `packages/python/port/platforms/linkedin.py` for a complete working example.

## If you have custom extraction functions

Extraction functions should return an `ExtractionResult`:

```python
from port.api.d3i_props import ExtractionResult
from collections import Counter

def extraction(zip_path: str, validation) -> ExtractionResult:
    errors = Counter()
    reader = ZipArchiveReader(zip_path, validation.archive_members, errors)

    tables = [
        # your consent form tables here
    ]
    return ExtractionResult(tables=tables, errors=errors)
```

`ZipArchiveReader` provides `reader.json()`, `reader.csv()`, and `reader.raw()`
methods that return found/not-found results instead of raising exceptions on
missing files. This eliminates the error cascade where missing DDP files
produced multiple spurious error log lines.

## PayloadFile

File delivery changed from WORKERFS (mounting files into Pyodide's virtual
filesystem) to PayloadFile (streaming via FileReaderSync). However,
**ScriptWrapper auto-materializes PayloadFile uploads to `/tmp`** and converts
the payload type, so most scripts work without changes.

You only need to update code if you explicitly check for `PayloadString`:

```python
# Before
if fileResult.__type__ == "PayloadString":

# After — either works
if fileResult.__type__ == "PayloadFile":
# or (ScriptWrapper converts for you):
if fileResult.__type__ == "PayloadString":
```

If you use FlowBuilder, this is handled automatically.

## Logging

Python `logging.getLogger()` calls still work for local diagnostics — log
output appears in the browser console. However, these logs are **not forwarded
to the host platform**.

For host-visible milestones (operational observability), use explicit
`CommandSystemLog` yields via the `port_helpers.emit_log()` helper:

```python
import port.helpers.port_helpers as ph

yield from ph.emit_log("info", f"[{platform}] Extraction complete: {len(tables)} tables")
```

Host-visible log messages must be **PII-free** — no participant data, file
paths, or data values. See `docs/decisions/0013-three-logging-boundaries-for-diagnostics-milestones-and-consent-gated-errors.md`.

## Donation key format

Donation keys changed from `{session_id}` to `{session_id}-{platform_name}`.
If your downstream data pipeline parses donation keys, update the parsing logic.

FlowBuilder handles this automatically. If you build keys manually:

```python
# Before
yield donate(f"{session_id}-mykey", data)

# After
yield donate(f"{session_id}-{platform_name.lower()}", data)
```

## Testing your migration

```sh
pnpm doctor          # check environment (13 checks)
pnpm test            # run 74 Python tests
pnpm start           # local dev server at localhost:3000
pnpm run build       # full production build
```
