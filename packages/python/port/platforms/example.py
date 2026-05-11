"""
Example Platform
================

This module is a minimal, fully working example of how to build a data
donation platform for this framework.  Read this file if you want to add a
new platform — it shows every piece you need to implement and explains the
role of each part.

What this platform does
-----------------------
* **Validator** — accepts any valid zip file.  No DDP-category matching is
  performed; the only check is whether the file can be opened as a zip.  This
  is intentionally the simplest possible validator so the example stays easy
  to follow.

* **Extractor** — opens the zip and collects file statistics (filename,
  uncompressed size, compressed size, last-modified date) for every entry.
  The result is presented to the participant as a single consent-form table.

How to add your own platform
-----------------------------
1. Copy this file and rename it to ``<platform_name>.py`` (lowercase, no
   spaces).
2. Replace ``validate_zip_file`` with a validator that checks for your
   platform's known files using ``validate.validate_zip(DDP_CATEGORIES, …)``.
3. Replace ``file_stats_to_df`` with extractor functions that parse the files
   you care about and return a ``pd.DataFrame``.
4. Update ``EXTRACTOR_REGISTRY`` so it maps the string names used in
   ``port_config.json`` to your extractor functions.
5. Generate ``port_config.json`` with ``pnpm generate-config <platform_name>``.

Configuration
-------------
The ``extraction`` function is driven by ``port_config.json``.  Generate one
with::

    pnpm generate-config example

Each extractor function carries its own table config in a ``Table config::``
JSON block inside its docstring.  The generator reads those blocks and
assembles the JSON file.

Platform info::

    {
        "name": "example",
        "filetypes": ["zip"],
        "languages": ["en", "nl"],
        "description": "Example platform: accepts any zip and returns a table of file statistics. Use this as a starting point when adding a new platform.",
        "time_last_tested": "not yet implemented"
    }
"""
import logging
import zipfile
from collections import Counter
from datetime import datetime
from typing import Callable

import pandas as pd

from port.helpers.extraction_helpers import ZipArchiveReader
from port.helpers.flow_builder import FlowBuilder
from port.helpers.validate import (
    StatusCode,
    ValidateInput,
)
from port.api.d3i_props import ExtractionResult
from port.helpers.table_extractor import (
    load_port_config,
    run_extraction,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------

def validate_zip_file(path_to_zip: str) -> ValidateInput:
    """Validate that the uploaded file is a readable zip archive.

    This is the simplest possible validator: it accepts any valid zip without
    checking which files are inside.  For a real platform you would normally
    call ``validate.validate_zip(DDP_CATEGORIES, path_to_zip)`` instead, which
    additionally matches the zip contents against a list of known files to
    confirm the participant uploaded the right export.

    Parameters
    ----------
    path_to_zip:
        Path on disk to the file supplied by the participant.

    Returns
    -------
    ValidateInput
        ``get_status_code_id() == 0`` when the file is a valid zip;
        ``1`` when the file cannot be opened as a zip.
    """
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


# ---------------------------------------------------------------------------
# Extractor
# ---------------------------------------------------------------------------

def file_stats_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract file statistics for every entry in the zip archive.

    This extractor demonstrates the minimal extractor contract: receive a
    ``ZipArchiveReader`` and an ``errors`` counter, and return a
    ``pd.DataFrame``.  It intentionally does not read any file *contents* —
    instead it reads the zip's central directory, which is always available
    without knowing what platform the zip came from.

    For a real platform you would use ``reader.json()``, ``reader.csv()``, or
    ``reader.json_all()`` to read specific files from the archive.

    Parameters
    ----------
    reader:
        Archive reader whose ``zip_path`` attribute points to the zip on disk.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``filename``, ``size``, ``compressed_size``, ``date_modified``.
        Empty DataFrame when the zip cannot be read.

    Table documentation::

        {
          "summary": "Each row represents one file entry inside the donated zip archive, including its name, original size, compressed size, and last-modified date.",
          "source_file": "the zip archive itself (central directory)",
          "columns": {
            "filename": "Path of the file inside the zip archive.",
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
            "size":            {"en": "Size (bytes)",            "nl": "Grootte (bytes)"},
            "compressed_size": {"en": "Compressed size (bytes)", "nl": "Gecomprimeerde grootte (bytes)"},
            "date_modified":   {"en": "Date modified",           "nl": "Datum gewijzigd"}
          },
          "visualizations": [
            {
              "title": {"en": "File sizes", "nl": "Bestandsgroottes"},
              "type": "bar",
              "group": "filename",
              "values": [{"label": {"en": "Size (bytes)", "nl": "Grootte (bytes)"}, "column": "size"}]
            }
          ]
        }
    """
    rows = []
    try:
        with zipfile.ZipFile(reader.zip_path, "r") as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                date_str = ""
                if info.date_time:
                    try:
                        date_str = datetime(*info.date_time).isoformat()
                    except Exception:
                        pass
                rows.append({
                    "filename": info.filename,
                    "size": info.file_size,
                    "compressed_size": info.compress_size,
                    "date_modified": date_str,
                })
    except Exception as e:
        logger.error("file_stats_to_df error: %s", e)
        errors[type(e).__name__] += 1
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Extractor registry & platform wiring
# ---------------------------------------------------------------------------

#: Maps string names used in port_config.json to the actual extractor functions.
#: Add an entry here for every extractor function you add above.
EXTRACTOR_REGISTRY: dict[str, Callable[..., pd.DataFrame]] = {
    "file_stats_to_df": file_stats_to_df,
}


def extraction(zip_path: str, validation: ValidateInput) -> ExtractionResult:
    """Extract file statistics from the donated zip and return consent-form tables.

    Parameters
    ----------
    zip_path:
        Path to the zip archive on disk.
    validation:
        Validation result whose ``archive_members`` list is forwarded to
        ``ZipArchiveReader`` so it does not have to re-open the zip.
    """
    config = load_port_config(EXTRACTOR_REGISTRY)
    errors: Counter = Counter()
    reader = ZipArchiveReader(zip_path, validation.archive_members, errors)
    return run_extraction(reader, errors, config)


class ExamplePlatformFlow(FlowBuilder):
    """Flow implementation for the example platform study.

    Subclass ``FlowBuilder`` and implement ``validate_file`` and
    ``extract_data`` — that's all that's needed to plug into the shared
    upload → validate → extract → consent → donate flow.
    """

    def __init__(self, session_id: str):
        super().__init__(session_id, "example")

    def validate_file(self, file: str) -> ValidateInput:
        return validate_zip_file(file)

    def extract_data(self, file_value: str, validation: ValidateInput) -> ExtractionResult:
        return extraction(file_value, validation)


def process(session_id: str):
    flow = ExamplePlatformFlow(session_id)
    return flow.start_flow()
