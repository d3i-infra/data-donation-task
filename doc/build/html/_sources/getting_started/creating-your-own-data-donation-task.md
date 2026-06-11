# Creating your own data donation task

After you have forked or cloned and installed the repository, you can start building your own donation task.

The Python code lives in `packages/python/port`. The main pieces are:

* `script.py` — loads the right platform and starts the donation flow
* `platforms/` — one file per platform (e.g. `instagram.py`, `linkedin.py`)
* `configs/` — one JSON config file per platform (e.g. `instagram_config.json`)
* `helpers/flow_builder.py` — runs the full donation flow (upload → validate → extract → consent → donate)
* `helpers/validate.py` — checks that the uploaded zip is the right kind of file
* `helpers/extraction_helpers.py` — tools for reading files out of a zip

## How to add a new platform

### Step 1 — Copy the example platform

```sh
cp packages/python/port/platforms/example.py packages/python/port/platforms/myplatform.py
```

`example.py` is a fully working platform. It accepts any zip file and shows a table of the files inside it. Read through it — it explains every part you need.

### Step 2 — Write your extractor functions

An extractor function reads files from the zip and returns a `pd.DataFrame`. Here is the minimal shape:

```python
def my_data_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.json("data.json")
    if not result.found:
        return pd.DataFrame()
    return pd.DataFrame(result.data)
```

Each extractor function needs a `Table config::` JSON block in its docstring. This block describes how the table looks in the consent form (title, column headers, visualizations). See `example.py` for a complete example.

### Step 3 — Register your extractors

Add each extractor function to `EXTRACTOR_REGISTRY` at the bottom of your platform file:

```python
EXTRACTOR_REGISTRY: dict[str, Callable[..., pd.DataFrame]] = {
    "my_data_to_df": my_data_to_df,
}
```

### Step 4 — Generate the config file

```sh
pnpm generate-config myplatform
```

This reads the `Table config::` blocks from your extractor docstrings and creates `packages/python/port/configs/myplatform_config.json`. Once the file exists, the generator will not overwrite it — your edits are safe.

After generating, open the config and edit the titles and descriptions to match your study.

### Step 5 — Start the dev server

```sh
VITE_PLATFORM=myplatform pnpm start
```

Visit `http://localhost:3000` to see your platform in action.

---

## How FlowBuilder works

Every platform has a **FlowBuilder subclass**. You only need to implement two methods:

* `validate_file(archive)` — check that the uploaded zip is the right kind of file
* `extract_data(archive, validation)` — extract tables from the zip and return an `ExtractionResult`

FlowBuilder handles everything else: asking the participant for a file, showing a retry prompt on bad uploads, rendering the consent form, and sending the donation.

```python
class MyPlatformFlow(FlowBuilder):
    def __init__(self, session_id: str):
        super().__init__(session_id, "myplatform")

    def validate_file(self, archive) -> ValidateInput:
        return validate.validate_zip(DDP_CATEGORIES, archive)

    def extract_data(self, archive, validation: ValidateInput) -> ExtractionResult:
        return extraction(archive, validation)


def process(session_id: str):
    flow = MyPlatformFlow(session_id)
    return flow.start_flow()
```

---

## How extraction works

The `extraction()` function loads the config and runs all extractors:

```python
def extraction(zip_path: str, validation: ValidateInput) -> ExtractionResult:
    config = load_port_config(EXTRACTOR_REGISTRY, "myplatform")
    errors: Counter = Counter()
    reader = ZipArchiveReader(archive, validation.archive_members, errors)
    return run_extraction(reader, errors, config)
```

`load_port_config` reads `configs/myplatform_config.json` and matches each table entry to an extractor in `EXTRACTOR_REGISTRY`. `run_extraction` calls each extractor and returns the non-empty tables.

---

## DDP_CATEGORIES

Each platform defines which zip formats it supports. `validate.validate_zip()` checks the uploaded file against these categories by comparing the zip's file list against the `known_files` for each category.

```python
DDP_CATEGORIES = [
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=["conversations.json", "user.json"]
    ),
    DDPCategory(
        id="csv_en",
        ddp_filetype=DDPFiletype.CSV,
        language=Language.EN,
        known_files=["data.csv", "profile.csv"]
    ),
]
```

If your participants use a format not covered here, add a new `DDPCategory` entry.

The example platform skips `DDP_CATEGORIES` entirely and accepts any zip. That is fine for getting started, but for a real study you should validate the zip.

---

## Install Python packages

The donation task runs in the participant's browser using [Pyodide](https://pyodide.org/en/stable/) — a Python runtime compiled to WebAssembly. Packages installed on your computer are not available inside Pyodide.

Check the [list of packages available in Pyodide](https://pyodide.org/en/stable/usage/packages-in-pyodide.html). If you need a package, add it to `loadPackages` in `packages/data-collector/public/py_worker.js`:

```javascript
function loadPackages() {
  return self.pyodide.loadPackage(['micropip', 'numpy', 'pandas', 'lxml'])
}
```

---

## Tips

**Use ZipArchiveReader for reading files.**
`reader.json()`, `reader.csv()`, and `reader.raw()` return a result with a `found` field. If the file is missing, `found` is `False` and no error is raised. This is important because DDPs vary — a file that exists for one participant may be missing for another.

**Use the browser console for debugging.**
`print()` and `logging.getLogger()` output appears in the browser's DevTools console. These messages stay local and are never sent to the host.

**Keep the diverse nature of DDPs in mind.**
Test with several different DDPs. File names, JSON keys, and folder structure can vary depending on the platform language and download settings.

**Do not let your code crash.**
If your extraction function raises an uncaught exception, the donation task stops. Use `try/except` in your extractor functions and record errors with `errors[type(e).__name__] += 1`.

**Data donation checklist.**
See the [data donation checklist](data-donation-checklist.md) for a full list of things to check before going live.
