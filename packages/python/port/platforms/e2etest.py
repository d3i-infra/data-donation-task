"""
E2E Test Platform
=================

Test-only platform for the Playwright suite — **never ship this to
participants**. ``release.sh`` excludes it from platform discovery; it can
only be built by explicitly setting ``VITE_PLATFORM=e2etest``.

It delegates validation and extraction to the example platform so the
baseline donation specs exercise the same code paths, and adds exactly one
behavior for fault injection: uploading a zip that contains a file named
``trigger_error.txt`` makes ``extraction`` raise on purpose. That exercises
the consent-gated error-report flow end-to-end (error page → report/skip →
task-incomplete page → nonzero exit) — see ``tests/error-flow.spec.ts``.

It also deliberately impersonates the example platform's participant-visible
identity (``platform_name="example"`` → headings, donation key), so the
donation specs pass unchanged against an e2etest build.

Run the whole e2e suite against this platform::

    VITE_PLATFORM=e2etest pnpm test:e2e

This is also the way to preview the error flow in the dev server::

    VITE_PLATFORM=e2etest pnpm start

Platform info::

    {
        "name": "e2etest",
        "filetypes": ["zip"],
        "languages": ["en", "nl"],
        "description": "Test-only platform for the Playwright e2e suite: the example platform plus a deliberate error trigger. Excluded from release discovery; never deployed to participants.",
        "time_last_tested": "not yet implemented"
    }
"""
import logging
import os
from collections import Counter
from typing import Callable

import pandas as pd

from port.helpers.extraction_helpers import ZipArchiveReader
from port.helpers.flow_builder import FlowBuilder
from port.helpers.validate import ValidateInput
from port.api.d3i_props import ExtractionResult
from port.api.file_utils import SeekableBinaryReader
from port.helpers.table_extractor import (
    load_port_config,
    run_extraction,
)
from port.platforms import example

logger = logging.getLogger(__name__)


#: Uploading a zip containing a file with this name makes ``extraction`` raise
#: on purpose, so the error flow can be exercised without a broken extractor.
ERROR_TRIGGER_FILENAME = "trigger_error.txt"


def file_stats_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Delegate to the example platform's file-statistics extractor.

    The docstring blocks below are copies of the example platform's so
    ``pnpm generate-config e2etest`` produces an equivalent config (the
    generator is AST-only and reads them from this module).

    Table documentation::

        {
          "summary": "Each row represents one file entry inside the donated zip archive, including its name, original size, compressed size, and last-modified date.",
          "source_file": "the zip archive itself (central directory)",
          "columns": {
            "filename": "Full path of the file inside the zip archive.",
            "basename": "File name without directory path.",
            "size": "Uncompressed file size in bytes.",
            "compressed_size": "Compressed size in bytes as stored in the zip.",
            "date_modified": "ISO 8601 timestamp of the file's last-modified date recorded in the zip."
          }
        }

    Table config::

        {
          "id": "example_file_stats",
          "title": {
            "en": "Files in the zip",
            "nl": "Bestanden in de zip"
          },
          "description": {
            "en": "This table lists every file found inside the uploaded zip archive together with its size and date information.",
            "nl": "Deze tabel bevat alle bestanden in het geüploade zip-archief, inclusief grootte en datuminformatie."
          },
          "headers": {
            "filename":        {"en": "Filename",                "nl": "Bestandsnaam"},
            "basename":        {"en": "File name",               "nl": "Bestandsnaam (kort)"},
            "size":            {"en": "Size (bytes)",            "nl": "Grootte (bytes)"},
            "compressed_size": {"en": "Compressed size (bytes)", "nl": "Gecomprimeerde grootte (bytes)"},
            "date_modified":   {"en": "Date modified",           "nl": "Datum gewijzigd"}
          },
          "visualizations": [
            {
              "title": {"en": "File names", "nl": "Bestandsnamen"},
              "type": "wordcloud",
              "textColumn": "basename"
            }
          ]
        }
    """
    return example.file_stats_to_df(reader, errors)


EXTRACTOR_REGISTRY: dict[str, Callable[..., pd.DataFrame]] = {
    "file_stats_to_df": file_stats_to_df,
}


def extraction(zip_path: SeekableBinaryReader, validation: ValidateInput) -> ExtractionResult:
    """Extract like the example platform, unless the archive is the trigger.

    The trigger check runs before any config or file access, so tripping it
    exercises the uncaught-exception path through ``ScriptWrapper``.
    """
    if any(os.path.basename(m) == ERROR_TRIGGER_FILENAME for m in validation.archive_members):
        raise RuntimeError(
            f"Intentional test error: archive contains {ERROR_TRIGGER_FILENAME}"
        )
    config = load_port_config(EXTRACTOR_REGISTRY, "e2etest")
    errors: Counter = Counter()
    reader = ZipArchiveReader(zip_path, validation.archive_members, errors)
    return run_extraction(reader, errors, config)


class E2eTestFlow(FlowBuilder):
    """Flow for the e2e test platform.

    Passes ``"example"`` as the participant-visible platform name on purpose:
    the baseline donation specs assert the example platform's headings, and
    this platform is a stand-in for it.
    """

    def __init__(self, session_id: str):
        super().__init__(session_id, "example")

    def validate_file(self, file: SeekableBinaryReader) -> ValidateInput:
        return example.validate_zip_file(file)

    def extract_data(self, file_value: SeekableBinaryReader, validation: ValidateInput) -> ExtractionResult:
        return extraction(file_value, validation)


def process(session_id: str):
    flow = E2eTestFlow(session_id)
    return flow.start_flow()
