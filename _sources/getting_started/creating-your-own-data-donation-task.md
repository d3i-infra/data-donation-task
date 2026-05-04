# Creating your own data donation task

After you have forked or cloned and installed the repository you can start creating your own donation task.

The extraction code lives in `packages/python/port`. The key pieces are:

* `script.py` — orchestrator that sequences platforms and delegates to FlowBuilder
* `platforms/` — one module per platform, each containing a FlowBuilder subclass
* `helpers/flow_builder.py` — the shared donation flow (file prompt → validate → extract → consent → donate)
* `helpers/validate.py` — DDP validation using `DDP_CATEGORIES`
* `helpers/extraction_helpers.py` — `ZipArchiveReader` and data parsing utilities

## How FlowBuilder works

Each platform implements a **FlowBuilder subclass** with two methods:

* `validate_file(file)` — check whether the uploaded zip is a valid DDP for this platform
* `extract_data(file_value, validation)` — extract tables from the zip and return an `ExtractionResult`

FlowBuilder handles everything else: prompting the participant for a file, retry on invalid uploads, safety checks, rendering consent tables, and submitting the donation.

Here is a minimal example (based on [LinkedIn](https://github.com/d3i-infra/data-donation-task/blob/master/packages/python/port/platforms/linkedin.py)):

```python
# platforms/my_platform.py
from collections import Counter
import pandas as pd

import port.api.props as props
import port.api.d3i_props as d3i_props
from port.api.d3i_props import ExtractionResult
from port.helpers.extraction_helpers import ZipArchiveReader
from port.helpers.flow_builder import FlowBuilder
import port.helpers.validate as validate
from port.helpers.validate import DDPCategory, DDPFiletype, Language

DDP_CATEGORIES = [
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=["data.json", "profile.json"]
    )
]

def extraction(zip_path: str, validation) -> ExtractionResult:
    errors = Counter()
    reader = ZipArchiveReader(zip_path, validation.archive_members, errors)

    result = reader.json("data.json")
    if result.found:
        df = pd.DataFrame(result.data)
    else:
        df = pd.DataFrame()

    tables = [
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="my_table",
            data_frame=df,
            title=props.Translatable({"en": "My Data", "nl": "Mijn gegevens"}),
        )
    ]
    return ExtractionResult(tables=tables, errors=errors)


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

## Registering your platform in script.py

`script.py` maintains a registry of platforms. Add your platform:

```python
PLATFORM_REGISTRY = [
    ("MyPlatform", "port.platforms.my_platform", "MyPlatformFlow"),
    # ... other platforms
]
```

Platforms are imported lazily, so only the platform(s) needed for a given build are loaded.

## DDP_CATEGORIES

Each platform defines which DDP formats it supports. `validate.validate_zip()` checks the uploaded file against these categories by comparing the zip's file list against the `known_files` for each category.

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

If your participants use a DDP format not covered by the existing categories, add a new `DDPCategory` entry with the expected files.

## The usage of `yield`

`yield` makes sure that whenever the code resumes after a page render, it starts where it left off. FlowBuilder uses `yield` internally — your extraction code does not need to yield unless you want to show progress messages during extraction.

The `script.py` orchestrator uses `yield from flow.start_flow()` to delegate the full flow to FlowBuilder.

## Install Python packages

The data donation task runs in the browser of the participant using [Pyodide](https://pyodide.org/en/stable/) — Python compiled to WebAssembly. Packages available on your system install of Python won't automatically be available in the browser.

If you want to use external packages they should be available for Pyodide. You can check the list of available packages [here](https://pyodide.org/en/stable/usage/packages-in-pyodide.html).
If you have found a package you want to use, add it to the `loadPackages` function in `packages/data-collector/public/py_worker.js`:

```javascript
function loadPackages() {
  console.log('[ProcessingWorker] loading packages')
  return self.pyodide.loadPackage(['micropip', 'numpy', 'pandas', 'lxml'])
}
```

You can now import the packages as you would normally do in Python.

## Try the donation task from the perspective of the participant

Follow the installation instructions and start the server with `pnpm start`. Visit `http://localhost:3000` to see the donation flow.

## Tips

**Use ZipArchiveReader for file access.**
`reader.json()`, `reader.csv()`, and `reader.raw()` return found/not-found results instead of raising exceptions. This avoids error cascades when DDP files are missing (which is expected — DDPs vary by platform version and language).

**Use the browser console for debugging.**
`print()` and `logging.getLogger()` output appears in the browser console. These logs stay local and are not sent to the host platform.

**Keep the diverse nature of DDPs in mind.**
Check a variety of DDPs to make sure your extraction handles: different data formats (HTML, JSON, CSV), language settings (which affect file names and JSON keys), and different download options (which affect which files are present).

**Don't let your code crash.**
Uncaught exceptions stop the donation task. Use try/except in extraction functions and count errors via `errors[type(e).__name__] += 1` rather than letting them propagate.

**Data donation checklist.**
Check out the [wiki article](https://github.com/d3i-infra/data-donation-task/wiki/Data-donation-checklist) for a comprehensive checklist.

## Limits

* The data donation task is a frontend — you need Next (or a compatible host) to deploy it.
* Pyodide can handle files up to around 4 GiB, but less is better. For platforms like YouTube, participants should exclude personal videos from their DDP.
