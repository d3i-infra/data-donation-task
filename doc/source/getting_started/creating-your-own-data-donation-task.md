# Creating your own data donation task

Each platform is implemented as a single Python module in `packages/python/port/platforms/`. The module has three responsibilities: validating the uploaded file, extracting tables from it, and subclassing `FlowBuilder` to plug both into the shared donation flow.

`example.py` is the minimal working implementation. It accepts any zip file and returns a table of file statistics from the zip's central directory — intentionally simple, so the structure is easy to follow. This guide is based on that file.

## How it works

When a participant wants to donate their data, the data donation task runs the following sequence automatically:

1. Prompt the participant for a file.
2. Run a safety check on the upload (size limits).
3. Call your `validate_file()` — if it fails, show a retry prompt and loop back.
4. Call your `extract_data()` — if it returns no tables, show a no-data page.
5. Render the consent form with the extracted tables.
6. Send the donation.

You implement steps 3 and 4. Everything else is handled by the framework.

The files involved are:

| File | Role |
|---|---|
| `platforms/example.py` | Minimal working platform — start here |
| `configs/example_config.json` | Generated config that drives extraction and table display |
| `helpers/flow_builder.py` | Shared flow logic |
| `helpers/validate.py` | DDP validation utilities |
| `helpers/extraction_helpers.py` | `ZipArchiveReader` for reading files out of a zip |
| `helpers/table_extractor.py` | Loads the config and runs extractor functions |

---

## Adding a new platform

### Step 1 — Copy the example

```sh
cp packages/python/port/platforms/example.py packages/python/port/platforms/myplatform.py
```

Read through the copy before modifying it. Each section is commented and the structure maps directly to the steps below.

### Step 2 — Write extractor functions

An extractor reads one or more files from the zip and returns a `pd.DataFrame`:

```python
from collections import Counter
import pandas as pd
from port.helpers.extraction_helpers import ZipArchiveReader

def my_data_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.json("path/to/data.json")
    if not result.found:
        return pd.DataFrame()
    return pd.DataFrame(result.data)
```

`ZipArchiveReader` provides `reader.json()`, `reader.csv()`, and `reader.raw()`. Each returns a result object with a `found` attribute — if the file is absent, `found` is `False` and no exception is raised. This is important because DDP exports are not always consistent across participants.

Each extractor function must also include a `Table config::` JSON block in its docstring. The config generator reads these blocks to produce the consent-form table definition:

```python
def my_data_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract my data.

    Table config::

        {
          "id": "myplatform_my_data",
          "title": {"en": "My data", "nl": "Mijn data"},
          "description": {
            "en": "Description shown to the participant.",
            "nl": "Beschrijving voor de deelnemer."
          },
          "headers": {
            "column_a": {"en": "Column A", "nl": "Kolom A"}
          },
          "visualizations": []
        }
    """
    ...
```

See `example.py` for a complete block, including a word cloud visualization.

### Step 3 — Register your extractors

At the module level, map each function name to its implementation:

```python
EXTRACTOR_REGISTRY: dict[str, Callable[..., pd.DataFrame]] = {
    "my_data_to_df": my_data_to_df,
}
```

One entry per extractor. The keys are referenced by the config file.

### Step 4 — Generate the config

```sh
pnpm generate-config myplatform
```

This scans the `Table config::` blocks in your docstrings and writes `packages/python/port/configs/myplatform_config.json`. The generator will not overwrite an existing file, so manual edits are safe.

After generating, open the config and adjust the titles and descriptions to match your study's language and framing.

### Step 5 — Run the dev server

```sh
VITE_PLATFORM=myplatform pnpm start
```

Open `http://localhost:3000` and upload a zip to test your platform.

---

## Implementation details

### FlowBuilder

Subclass `FlowBuilder` and implement `validate_file` and `extract_data`:

```python
class MyPlatformFlow(FlowBuilder):
    def __init__(self, session_id: str):
        super().__init__(session_id, "myplatform")

    def validate_file(self, file) -> ValidateInput:
        return validate_my_file(file)

    def extract_data(self, file, validation: ValidateInput) -> ExtractionResult:
        return extraction(file, validation)


def process(session_id: str):
    flow = MyPlatformFlow(session_id)
    return flow.start_flow()
```

`FlowBuilder` calls these methods at the appropriate points in the flow. You do not call them directly.

### The `extraction()` function

```python
def extraction(zip_path, validation: ValidateInput) -> ExtractionResult:
    config = load_port_config(EXTRACTOR_REGISTRY, "myplatform")
    errors: Counter = Counter()
    reader = ZipArchiveReader(zip_path, validation.archive_members, errors)
    return run_extraction(reader, errors, config)
```

`load_port_config` reads the config JSON and matches each table entry to an extractor in `EXTRACTOR_REGISTRY`. `run_extraction` calls each extractor and returns the non-empty results as an `ExtractionResult`. Passing `validation.archive_members` avoids re-opening the zip.

### Validation

Validation determines whether the uploaded file is the expected kind of zip.

In `example.py` this is intentionally minimal — it only checks that the file opens as a valid zip:

```python
def validate_zip_file(path_to_zip) -> ValidateInput:
    status_codes = [
        StatusCode(id=0, description="Valid zip file"),
        StatusCode(id=1, description="Not a valid zip file"),
    ]
    v = ValidateInput(status_codes, [])
    try:
        with zipfile.ZipFile(path_to_zip, "r") as zf:
            v.archive_members = zf.namelist()
        v.set_current_status_code_by_id(0)
    except zipfile.BadZipFile:
        v.set_current_status_code_by_id(1)
    return v
```

Status code `0` means valid; any other value triggers the retry prompt in `FlowBuilder`.

For a real study, use `validate.validate_zip(DDP_CATEGORIES, file)` instead. This opens the zip, reads filenames from the central directory, and calls `infer_ddp_category()`, which computes what fraction of the `known_files` for each category are present. If at least 5% of the known files for any category are found, that category is matched and status code `0` is returned. Otherwise the zip is rejected.

`DDP_CATEGORIES` lists the file names that are characteristic of the platform's export format:

```python
from port.helpers.validate import DDPCategory, DDPFiletype, Language

DDP_CATEGORIES = [
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=["personal_information.json", "liked_posts.json", "followers.json"],
    ),
]
```

The matched category is stored on the returned `ValidateInput` as `validation.current_ddp_category` and is available inside `extract_data` if your extractor needs to branch on format.

---

## Comparison with a real platform

Once you're comfortable with the example, `packages/python/port/platforms/instagram.py` is a useful reference. It follows the same structure, but differs from the example in a few key ways:

- `DDP_CATEGORIES` lists dozens of known Instagram export filenames, so the validator only accepts genuine Instagram exports.
- `validate.validate_zip(DDP_CATEGORIES, file)` replaces the bare zip-open check.
- Multiple extractor functions each parse a specific file from the export (liked posts, followers, login activity, and others).
- Extractors read actual file contents with `reader.json()` rather than only inspecting the zip central directory.

The example skips DDP matching on purpose — it lets you test the full flow with any zip file. For a study collecting real participant data, define `DDP_CATEGORIES` and write extractors for the specific files your platform exports.

---

## Multi-file platforms

Some exports arrive as several zip parts that belong together — a Google
Takeout export split across multiple files is the common case. To accept
that instead of a single zip, set `expected_file_payload` on your flow
class and annotate both hook overrides as `ArchiveSet`:

```python
from port.helpers.archive_set import ArchiveSet

class MyPlatformFlow(FlowBuilder):
    expected_file_payload = "PayloadFiles"

    def __init__(self, session_id: str):
        super().__init__(session_id, "myplatform")

    def validate_file(self, archive_set: ArchiveSet) -> ValidateInput:
        return validate_my_archive_set(archive_set)

    def extract_data(self, archive_set: ArchiveSet, validation: ValidateInput) -> ExtractionResult:
        return extraction(archive_set, validation)
```

This one attribute changes three things: the participant sees a multi-select
file picker instead of a single-file one, `FlowBuilder` unions whatever
parts they select into one `ArchiveSet` (raising a retry prompt instead of a
traceback if a part is corrupt), and `validate_file`/`extract_data` receive
that `ArchiveSet` rather than a single reader. Everything downstream is
unchanged — `ZipArchiveReader(archive_set, validation.archive_members,
errors)` works the same way it does for a single-file platform, because
`ArchiveSet` satisfies the same `ArchiveSource` protocol
`SingleArchiveSource` wraps a single zip in.

See `packages/python/port/platforms/e2etest_multifile.py` for a minimal
working multi-file platform, and `tests/multifile.spec.ts` for the
Playwright coverage of the multi-select upload flow.

---

## Python packages

The task runs in the participant's browser via [Pyodide](https://pyodide.org/en/stable/), a Python runtime compiled to WebAssembly, so locally installed packages are not available. Check the [Pyodide package list](https://pyodide.org/en/stable/usage/packages-in-pyodide.html) and add what you need to `packages/data-collector/public/py_worker.js`:

```javascript
function loadPackages() {
  return self.pyodide.loadPackage(['micropip', 'numpy', 'pandas', 'lxml'])
}
```

---

## Parsing large HTML files

Some exports include huge HTML files — a heavy user's watch or search history can run to
hundreds of megabytes. Parse those with streaming lxml, not BeautifulSoup: BeautifulSoup
has no streaming mode, so it builds the whole DOM in memory before you can read anything
out of it — too slow in Pyodide, and it blows the participant's memory budget on a
multi-hundred-MB member. Reading the whole member into memory first with `.read()` before
handing it to any parser has the same problem, streaming lxml or not.

Use `etree.iterparse` fed directly from `ZipArchiveReader.open_member()` (a context
manager yielding the member's decompression stream, not a materialized buffer), and clear
each element once you're done with it so the tree doesn't grow with the file:

```python
from lxml import etree

with reader.open_member("path/to/History.html") as stream:
    if stream is None:
        return pd.DataFrame()
    records = []
    for _, cell in etree.iterparse(stream, html=True, tag="div", events=("end",), encoding="utf-8"):
        if "the-class-you-want" in (cell.get("class") or ""):
            records.append(...)  # read what you need out of `cell` here
        cell.clear()
        while cell.getprevious() is not None:
            del cell.getparent()[0]
```

Two more things worth knowing before you copy this: pass `encoding` explicitly.
Google Takeout's activity HTML ships with no `<meta charset>` in its `<head>`, and lxml's
HTML parser silently falls back to latin-1 when it can't find one — every non-ASCII byte
comes out double-decoded (mojibake) with no error raised anywhere. Takeout's export bytes
are UTF-8; don't assume every export is, but always pin the encoding you've verified
rather than trusting the parser's guess.

`port/platforms/google.py`'s `_parse_activity_html` is the reference implementation —
it streams an `mdl-grid`/`outer-cell` activity page this way, one activity at a time,
and its docstring has the full account of what this bounds and what it doesn't: it
keeps the *parse* proportional to one activity rather than to file size, but the
records list it returns — and everything downstream of it (tables, consent transport,
donation serialization) — still scales with row count and stays unbounded. See the
memory ADR (`docs/decisions/0034-*.md`) for the measured numbers behind that split.

---

## Practical notes

**Use the browser console for debugging.** `print()` and `logging` output appears in DevTools and stays local — nothing is sent to the server.

**Test with several different exports.** Filenames, folder layout, and JSON structure can differ by platform language and export version.

**Extractors should not raise.** An uncaught exception in an extractor will stop the donation task. Use `try/except` and record errors with `errors[type(e).__name__] += 1`, then return an empty DataFrame.

**Before going live**, work through the [data donation checklist](data-donation-checklist.md).
