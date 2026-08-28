"""
E2E Test Multi-File Platform
=============================

Test-only platform for the Playwright multi-file suite — **never ship this
to participants**. ``release.sh`` excludes it from platform discovery, the
same way it excludes ``e2etest`` (whose test-only naming tier this platform
was renamed to join); it can only be built by explicitly setting
``VITE_PLATFORM=e2etest_multifile``.

It demonstrates the ``PayloadFiles`` (multi-part upload) flow end-to-end:
setting ``expected_file_payload = "PayloadFiles"`` makes ``FlowBuilder``
present the file picker with multi-select enabled, union whatever parts the
participant selects into one ``ArchiveSet`` (ADR-0040), and hand that
``ArchiveSet`` to ``validate_file``/``extract_data`` here instead of a single
``AsyncFileAdapter``. Two extractors read through ``ZipArchiveReader`` — the
same reader real multi-part platforms use: ``archive_membership_to_df``
lists every member with the index of the part it came from (metadata only,
via ``ArchiveSet.part_index_of`` — no bytes read), and
``archive_content_preview_to_df`` reads each member's actual *content*
through ``reader.raw()``, which routes through ``ArchiveSet.read_member()``
— the real per-part reopen and materialization-time size guard every
content-parsing extractor exercises. Both together let a Playwright test
assert that the consent form shows data sourced from every uploaded part,
not just the first, and that at least one shown value is provably read from
member bytes rather than restated from the zip's central directory.

Run the whole multi-file e2e suite against this platform::

    VITE_PLATFORM=e2etest_multifile pnpm test:e2e:multi

This is also the way to preview the multi-file upload flow in the dev
server::

    VITE_PLATFORM=e2etest_multifile pnpm start

Platform info::

    {
        "name": "e2etest_multifile",
        "filetypes": ["zip"],
        "languages": ["en", "nl"],
        "description": "Test-only platform for the Playwright multi-file e2e suite: accepts multiple zip parts and reports which part each archive member came from. Excluded from release discovery; never deployed to participants.",
        "time_last_tested": "not yet implemented"
    }
"""
import logging
from collections import Counter
from typing import Callable

import pandas as pd

from port.helpers.archive_set import ArchiveSet
from port.helpers.extraction_helpers import ZipArchiveReader
from port.helpers.flow_builder import FlowBuilder
from port.helpers.validate import StatusCode, ValidateInput
from port.api.d3i_props import ExtractionResult
from port.helpers.table_extractor import (
    load_port_config,
    run_extraction,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Extractor
# ---------------------------------------------------------------------------

def archive_membership_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """List every archive member together with the index of the part it came from.

    Unlike ``example.py``'s ``file_stats_to_df`` — which returns an empty
    frame for an ``ArchiveSource``-backed reader because it needs direct
    ``zipfile.ZipFile`` access to read central-directory metadata — this
    extractor works through ``reader``'s public interface only, so it
    functions for both a single-file upload and a multi-part ``ArchiveSet``.
    ``reader.archive`` is the ``ArchiveSet`` itself for a multi-part upload
    (``ZipArchiveReader`` stores whatever archive it was constructed with),
    which is where ``part_index_of`` comes from.

    Parameters
    ----------
    reader:
        Archive reader wrapping the participant's uploaded parts.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction. Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``filename``, ``part_index``. Exactly one filename column —
        earlier revisions of this table carried both ``filename`` and
        ``basename`` with near-identical headers ("Filename" / "File name"),
        which rendered as two visually-duplicate columns for the flat
        (no-directory) names these fixtures use. A platform whose members
        have real directory structure and wants both the full path and the
        bare name should give the two columns clearly distinct headers
        instead of restoring this ambiguity.

    Table documentation::

        {
          "summary": "Each row represents one file entry across the participant's uploaded archive parts, including which part it was found in.",
          "source_file": "the uploaded zip parts themselves (central directories)",
          "columns": {
            "filename": "Full path of the file inside the archive set.",
            "part_index": "Position (0-based) of the uploaded part that owns this member, in canonical (name, size) order."
          }
        }

    Table config::

        {
          "id": "e2etest_multifile_membership",
          "title": {
            "en": "Files across uploaded parts",
            "nl": "Bestanden over geüploade delen"
          },
          "description": {
            "en": "This table lists every file found across the uploaded archive parts, together with which part it came from.",
            "nl": "Deze tabel bevat alle bestanden in de geüploade archiefdelen, inclusief het deel waar ze vandaan komen."
          },
          "headers": {
            "filename":    {"en": "Filename",             "nl": "Bestandsnaam"},
            "part_index":  {"en": "Part",                  "nl": "Deel"}
          },
          "visualizations": [
            {
              "title": {"en": "File names", "nl": "Bestandsnamen"},
              "type": "wordcloud",
              "textColumn": "filename"
            }
          ]
        }
    """
    rows = []
    archive = reader.archive
    for member in reader.archive_members:
        part_index = archive.part_index_of(member) if isinstance(archive, ArchiveSet) else 0
        rows.append({
            "filename": member,
            "part_index": part_index,
        })
    return pd.DataFrame(rows)


def archive_content_preview_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Read each archive member's actual bytes and show a one-line preview.

    Unlike ``archive_membership_to_df`` above (metadata only —
    ``part_index_of`` never reads a byte), this extractor calls
    ``reader.raw(member)`` for every member. ``raw()`` resolves the member
    and calls ``self._source.read_member(member_path)``
    (``extraction_helpers.py``), which for an ``ArchiveSet`` is
    ``ArchiveSet.read_member`` — opening exactly the owning part's
    ``zipfile.ZipFile`` on demand and enforcing
    ``MAX_MEMBER_UNCOMPRESSED_BYTES`` before returning bytes (ADR-0040).
    This is the code path every real content-parsing extractor
    (``reader.json()``/``csv()``/``raw()``) exercises; ``file_stats_to_df``
    in ``example.py`` never calls it, since it reads only the zip's central
    directory.

    The preview is the member's first line, decoded permissively — good
    enough to prove the shown value came from a real read, without needing
    the fixture content to be valid JSON/CSV.

    Parameters
    ----------
    reader:
        Archive reader wrapping the participant's uploaded parts.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction. Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``filename``, ``part_index``, ``content_preview``.

    Table documentation::

        {
          "summary": "Each row shows the first line actually read from one archive member's content, proving the value came from the member's bytes and not just its name.",
          "source_file": "the uploaded zip parts themselves (member content, read on demand)",
          "columns": {
            "filename": "Full path of the file inside the archive set.",
            "part_index": "Position (0-based) of the uploaded part that owns this member, in canonical (name, size) order.",
            "content_preview": "The first line of the member's actual content, read via ArchiveSet.read_member."
          }
        }

    Table config::

        {
          "id": "e2etest_multifile_content_preview",
          "title": {
            "en": "Content read from each part",
            "nl": "Inhoud gelezen uit elk deel"
          },
          "description": {
            "en": "This table shows the first line actually read from each uploaded file's content, confirming data was read from every part.",
            "nl": "Deze tabel toont de eerste regel die daadwerkelijk is gelezen uit elk geüpload bestand, ter bevestiging dat er gegevens uit elk deel zijn gelezen."
          },
          "headers": {
            "filename":         {"en": "Filename",        "nl": "Bestandsnaam"},
            "part_index":       {"en": "Part",             "nl": "Deel"},
            "content_preview":  {"en": "Content preview",  "nl": "Inhoudsvoorbeeld"}
          },
          "visualizations": []
        }
    """
    rows = []
    archive = reader.archive
    for member in reader.archive_members:
        part_index = archive.part_index_of(member) if isinstance(archive, ArchiveSet) else 0
        result = reader.raw(member)
        if not result.found:
            continue
        raw_bytes = result.data.getvalue()
        first_line = raw_bytes.split(b"\n", 1)[0]
        preview = first_line.decode("utf-8", errors="replace")
        rows.append({
            "filename": member,
            "part_index": part_index,
            "content_preview": preview,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Extractor registry & platform wiring
# ---------------------------------------------------------------------------

EXTRACTOR_REGISTRY: dict[str, Callable[..., pd.DataFrame]] = {
    "archive_membership_to_df": archive_membership_to_df,
    "archive_content_preview_to_df": archive_content_preview_to_df,
}


def extraction(archive_set: ArchiveSet, validation: ValidateInput) -> ExtractionResult:
    """Extract archive-membership statistics from the uploaded parts.

    Identical shape to ``example.py``'s ``extraction``: build a
    ``ZipArchiveReader`` over the validated archive and its cached member
    list, then drive the extractor registry through ``run_extraction``. The
    only difference is the archive is an ``ArchiveSet`` union of N parts
    instead of a single ``SeekableBinaryReader`` (ADR-0040).
    """
    config = load_port_config(EXTRACTOR_REGISTRY, "e2etest_multifile")
    errors: Counter = Counter()
    reader = ZipArchiveReader(archive_set, validation.archive_members, errors)
    return run_extraction(reader, errors, config)


class E2eTestMultifileFlow(FlowBuilder):
    """Flow for the e2e-only multi-file demo platform.

    Sets ``expected_file_payload = "PayloadFiles"`` so ``FlowBuilder``
    renders the multi-select file prompt, unions whatever parts the
    participant selects into an ``ArchiveSet``, and passes that
    ``ArchiveSet`` — never a single reader — to the two overrides below. See
    the ``FlowBuilder.expected_file_payload`` docstring and ADR-0040.
    """

    expected_file_payload = "PayloadFiles"

    def __init__(self, session_id: str):
        super().__init__(session_id, "e2etest_multifile")

    def validate_file(self, archive_set: ArchiveSet) -> ValidateInput:
        """Accept the archive-set unconditionally.

        By the time this runs, ``FlowBuilder`` has already constructed the
        ``ArchiveSet`` from every uploaded part, which raises
        ``zipfile.BadZipFile`` on a corrupt part before validation ever
        starts (routed to the retry prompt, ADR-0040) — so there is nothing
        left to check here. A real multi-part platform would inspect
        ``archive_set.members`` against its own ``DDP_CATEGORIES`` instead.
        """
        status_codes = [StatusCode(id=0, description="Archive set readable")]
        validation = ValidateInput(status_codes, [])
        validation.set_current_status_code_by_id(0)
        validation.archive_members = list(archive_set.members)
        return validation

    def extract_data(self, archive_set: ArchiveSet, validation: ValidateInput) -> ExtractionResult:
        return extraction(archive_set, validation)


def process(session_id: str):
    flow = E2eTestMultifileFlow(session_id)
    return flow.start_flow()
