"""FlowBuilder — shared per-platform donation flow orchestration.

Subclass this to implement a platform-specific donation flow.
Override validate_file() and extract_data(). Call start_flow()
as a generator from script.py via `yield from`.
"""
from abc import abstractmethod
from collections.abc import Generator
import json
import logging
from typing import cast
import zipfile

import port.api.props as props
import port.api.d3i_props as d3i_props
from port.api.file_utils import SeekableBinaryReader
import port.helpers.port_helpers as ph
import port.helpers.validate as validate
import port.helpers.uploads as uploads
from port.helpers.archive_set import ArchiveSet

logger = logging.getLogger(__name__)


class TaskIncompleteError(Exception):
    """Flow ended without completion. ScriptWrapper maps this to a nonzero
    exit command so the host keeps the task pending (never completed).

    Raised with a reason key only: the fixed (code, info) pair comes from
    EXITS, so a raise site can never put exception or participant text on
    the bridge — exit info crosses it unconsented (ADR-0022/0023). Codes
    are a fork-local convention pending an agreed exit-code contract with
    Eyra; the host only distinguishes 0 from nonzero today (see ADR-0039).
    """

    EXITS = {
        "abandoned": (2, "Participant abandoned the task"),
        "donation_failed": (3, "Donation delivery failed"),
        "upload_rejected": (4, "Upload rejected"),
    }

    def __init__(self, reason: str):
        exit_code, exit_info = self.EXITS[reason]
        super().__init__(exit_info)
        self.reason = reason
        self.exit_code = exit_code
        self.exit_info = exit_info


class FlowBuilder:
    # Subclasses handling a Google-Takeout-style chunked export set this to
    # "PayloadFiles"; the file prompt, safety check, and archive-set
    # construction below key off this attribute. See ADR-0040 (ArchiveSet).
    expected_file_payload: str = "PayloadFile"

    def __init__(self, session_id: str, platform_name: str):
        self.session_id = session_id
        self.platform_name = platform_name
        self._initialize_ui_text()

    def _initialize_ui_text(self):
        """Initialize UI text based on platform name."""
        self.UI_TEXT = {
            "submit_file_header": props.Translatable({
                "en": f"Select your {self.platform_name} file",
                "nl": f"Selecteer uw {self.platform_name} bestand",
                "de": f"Wählen Sie Ihre {self.platform_name}-Datei aus",
                "it": f"Selezioni il suo file di {self.platform_name}",
                "es": f"Seleccione su archivo de {self.platform_name}",
            }),
            "review_data_header": props.Translatable({
                "en": f"Your {self.platform_name} data",
                "nl": f"Uw {self.platform_name} gegevens",
                "de": f"Ihre {self.platform_name}-Daten",
                "it": f"I suoi dati di {self.platform_name}",
                "es": f"Sus datos de {self.platform_name}",
            }),
            "retry_header": props.Translatable({
                "en": "Try again",
                "nl": "Probeer opnieuw",
                "de": "Erneut versuchen",
                "it": "Riprova",
                "es": "Intentar de nuevo",
            }),
            "review_data_description": props.Translatable({
                "en": f"Below you will find a curated selection of {self.platform_name} data.",
                "nl": f"Hieronder vindt u een zorgvuldig samengestelde selectie van {self.platform_name} gegevens.",
                "de": f"Nachfolgend finden Sie eine sorgfältig zusammengestellte Auswahl von {self.platform_name}-Daten.",
                "it": f"Di seguito trova una selezione curata dei dati di {self.platform_name}.",
                "es": f"A continuación encontrará una selección cuidada de los datos de {self.platform_name}.",
            }),
        }

    def start_flow(self):
        """Main per-platform flow: file→materialize→safety→validate→retry→extract→consent→donate.

        This is a generator. script.py calls it via `yield from flow.start_flow()`.
        Control flow rules:
        - continue: retry upload only
        - break: successful extraction, proceed to consent
        - return: terminal paths that ARE completions (exit 0 at the host)
        - raise TaskIncompleteError: terminal paths that are NOT completions —
          ScriptWrapper shows the task-incomplete page and exits nonzero so
          the host keeps the task pending (ADR-0039)

        Flow milestones are sent to the host via explicit CommandSystemLog yields
        (through emit_log). These must be PII-free. Local logger keeps full
        diagnostic detail in browser console only.
        """
        while True:
            # 1. Render file prompt → receive payload
            logger.info("Prompt for file for %s", self.platform_name)
            file_prompt = self.generate_file_prompt()
            yield from ph.emit_log("info", f"[{self.platform_name}] Upload prompt sent")
            file_result = yield ph.render_page(self.UI_TEXT["submit_file_header"], file_prompt)

            # Skip: the established participant-declined shapes. These
            # are pinned by ADR-0026 (PayloadString retirement) and stay
            # silent — no protocol-error page for a legitimate skip.
            # Anything else that isn't the type this flow expects is a
            # genuine protocol mismatch (version skew), not a skip, and
            # gets a visible error page instead — see the docstring above
            # `expected_file_payload`.
            if file_result.__type__ == self.expected_file_payload:
                pass
            elif file_result.__type__ in ("PayloadFalse", "PayloadVoid", "PayloadString"):
                logger.info("Skipped at file selection for %s", self.platform_name)
                yield from ph.emit_log(
                    "info",
                    f"[{self.platform_name}] Upload skipped: type={file_result.__type__}",
                )
                raise TaskIncompleteError("abandoned")
            else:
                logger.info(
                    "Protocol mismatch for %s: expected=%s got=%s",
                    self.platform_name,
                    self.expected_file_payload,
                    file_result.__type__,
                )
                yield from ph.emit_log(
                    "info",
                    f"[{self.platform_name}] Protocol mismatch: expected={self.expected_file_payload} got={file_result.__type__}",
                )
                _ = yield ph.render_protocol_error_page(self.platform_name)
                # Nothing usable arrived, so nothing can be donated: the
                # payload is rejected and the run must not exit 0 (ADR-0039).
                raise TaskIncompleteError("upload_rejected")

            is_multi = self.expected_file_payload == "PayloadFiles"

            if is_multi:
                # A PayloadFiles set — file_result.value is a list of
                # AsyncFileAdapters, one per uploaded part. Never
                # materialized. See ADR-0026/ADR-0040.
                parts = file_result.value
                total_size = sum(p.size for p in parts)
                yield from ph.emit_log(
                    "info",
                    f"[{self.platform_name}] Upload received: files={len(parts)} total_size={total_size}",
                )
            else:
                # AsyncFileAdapter — file-like, passed directly to validators
                # and extractors. Never materialized to a path. See ADR-0026.
                archive = file_result.value
                yield from ph.emit_log(
                    "info",
                    f"[{self.platform_name}] Upload received: size={archive.size}",
                )

            # 2. Safety check (size only — uses JS metadata, no read)
            try:
                uploads.check_payload_size(file_result)
            except (uploads.FileTooLargeError, uploads.TooManyFilesError) as e:
                logger.error("Safety check failed for %s: %s", self.platform_name, e)
                yield from ph.emit_log("info", f"[{self.platform_name}] Safety check failed: {type(e).__name__}")
                _ = yield ph.render_safety_error_page(self.platform_name, e)
                raise TaskIncompleteError("upload_rejected")

            if is_multi:
                # Build the union archive-set from the uploaded parts. A
                # corrupt/unreadable part surfaces here as zipfile.BadZipFile
                # (ADR-0040) — route it through the existing retry-prompt
                # path rather than a traceback, same as an invalid single
                # file failing DDP validation below.
                try:
                    archive = ArchiveSet(parts)
                except zipfile.BadZipFile:
                    logger.info("Corrupt part in %s upload; prompting retry", self.platform_name)
                    if (yield from self._prompt_retry()):
                        continue  # loop back to step 1
                    yield from ph.emit_log("info", f"[{self.platform_name}] Retry declined")
                    raise TaskIncompleteError("abandoned")

            # 3. Validate
            # `archive` is a SeekableBinaryReader for a PayloadFile flow, or
            # an ArchiveSet for a PayloadFiles flow — see the base
            # validate_file()/extract_data() docstrings. The cast keeps the
            # common single-file platforms' unannotated overrides typed
            # against the narrower, far more common SeekableBinaryReader;
            # a PayloadFiles subclass narrows its own override to ArchiveSet.
            validation = self.validate_file(cast(SeekableBinaryReader, archive))
            status = validation.get_status_code_id()
            category = getattr(validation, "current_ddp_category", None)
            category_id = getattr(category, "id", "unknown") if category else "unknown"

            if status == 0:
                yield from ph.emit_log("info", f"[{self.platform_name}] Validation: valid ({category_id})")
            else:
                yield from ph.emit_log("info", f"[{self.platform_name}] Validation: invalid")

            # 4. If invalid → retry prompt
            if status != 0:
                logger.info("Invalid %s file; prompting retry", self.platform_name)
                if (yield from self._prompt_retry()):
                    continue  # loop back to step 1
                yield from ph.emit_log("info", f"[{self.platform_name}] Retry declined")
                raise TaskIncompleteError("abandoned")

            # 5. Extract
            logger.info("Extracting data for %s", self.platform_name)
            raw_result = self.extract_data(cast(SeekableBinaryReader, archive), validation)
            if isinstance(raw_result, Generator):
                result = yield from raw_result
            else:
                result = raw_result

            # 6. Log extraction summary (PII-free: counts only)
            total_rows = sum(len(t.data_frame) for t in result.tables)
            if result.errors:
                error_summary = ", ".join(f"{k}×{v}" for k, v in result.errors.items())
                yield from ph.emit_log("info", f"[{self.platform_name}] Extraction complete: {len(result.tables)} tables, {total_rows} rows; errors: {error_summary}")
            else:
                yield from ph.emit_log("info", f"[{self.platform_name}] Extraction complete: {len(result.tables)} tables, {total_rows} rows; errors: none")

            # 7. If no tables → no-data page (clean empties only: zero tables
            # WITH extraction errors is an extraction failure, never presented
            # as "no data found" — the no-data/extraction-bug separation in
            # the no-data ADR. Raising routes it through the consent-gated
            # error flow, so the participant stays pending.)
            if not result.tables:
                if result.errors:
                    raise RuntimeError(
                        f"Extraction produced no tables with errors: "
                        f"{', '.join(f'{k}×{v}' for k, v in result.errors.items())}"
                    )
                logger.info("No data extracted for %s", self.platform_name)
                _ = yield ph.render_no_data_page(self.platform_name)
                return

            break  # proceed to consent

        # 8. Render consent form
        yield from ph.emit_log("info", f"[{self.platform_name}] Consent form shown")
        review_data_prompt = self.generate_review_data_prompt(result.tables)
        consent_result = yield ph.render_page(self.UI_TEXT["review_data_header"], review_data_prompt)

        # 9. Donate with per-platform key
        if consent_result.__type__ == "PayloadJSON":
            reviewed_data = consent_result.value
            yield from ph.emit_log("info", f"[{self.platform_name}] Consent: accepted")
        elif consent_result.__type__ == "PayloadFalse":
            reviewed_data = json.dumps({"status": "data_submission declined"})
            yield from ph.emit_log("info", f"[{self.platform_name}] Consent: declined")
        else:
            return

        donate_key = f"{self.session_id}-{self.platform_name.lower()}"
        is_decline = consent_result.__type__ == "PayloadFalse"
        yield from ph.emit_log("info", f"[{self.platform_name}] Donation started: payload size={len(reviewed_data)} bytes")
        donate_result = yield ph.donate(donate_key, reviewed_data)

        # 11. Inspect donate result
        # For declines, don't show failure UI — the participant chose not to donate,
        # so a failure to record that decision is invisible infrastructure, not their problem.
        if not ph.handle_donate_result(donate_result):
            if is_decline:
                logger.warning("Decline status donation failed for %s (silent)", self.platform_name)
                yield from ph.emit_log("info", f"[{self.platform_name}] Donation result: decline record failed (silent)")
                return
            logger.error("Donation failed for %s", self.platform_name)
            yield from ph.emit_log("info", f"[{self.platform_name}] Donation result: failed")
            _ = yield ph.render_donate_failure_page(self.platform_name)
            raise TaskIncompleteError("donation_failed")

        yield from ph.emit_log("info", f"[{self.platform_name}] Donation result: success")

    def _prompt_retry(self):
        """Render the retry prompt and report the participant's choice.

        Generator helper: `yield from` it to get back True (retry — caller
        should `continue`) or False (declined — caller should raise
        TaskIncompleteError("abandoned"), since a declined retry is not a
        completion; ADR-0039). Shared by the invalid-file path and the
        corrupt-archive-set path, which both fall back to the same retry UI.
        """
        retry_prompt = self.generate_retry_prompt()
        retry_result = yield ph.render_page(self.UI_TEXT["retry_header"], retry_prompt)
        return retry_result.__type__ == "PayloadTrue"

    # Methods to be overridden by platform-specific implementations
    def generate_file_prompt(self):
        """Generate platform-specific file prompt."""
        return ph.generate_file_prompt(
            "application/zip", multiple=self.expected_file_payload == "PayloadFiles"
        )

    @abstractmethod
    def validate_file(self, file: SeekableBinaryReader) -> validate.ValidateInput:
        """Validate the file according to platform-specific rules.

        `file` is the `AsyncFileAdapter` wrapping the browser upload — a
        seekable binary reader, never a path — for the default single-file
        flow (`expected_file_payload == "PayloadFile"`). A subclass that
        sets `expected_file_payload = "PayloadFiles"` receives the unioned
        `ArchiveSet` over all uploaded parts here instead (ADR-0040); such a
        subclass should narrow this annotation locally to `ArchiveSet`.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def extract_data(self, file: SeekableBinaryReader, validation: validate.ValidateInput) -> d3i_props.ExtractionResult:
        """Extract data from file using platform-specific logic.

        `file` is the `AsyncFileAdapter` wrapping the browser upload — a
        seekable binary reader, never a path — for the default single-file
        flow (`expected_file_payload == "PayloadFile"`). A subclass that
        sets `expected_file_payload = "PayloadFiles"` receives the unioned
        `ArchiveSet` over all uploaded parts here instead (ADR-0040); such a
        subclass should narrow this annotation locally to `ArchiveSet`.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def generate_retry_prompt(self):
        """Generate platform-specific retry prompt."""
        return ph.generate_retry_prompt(self.platform_name)

    def generate_review_data_prompt(self, table_list):
        """Generate platform-specific review data prompt."""
        return ph.generate_review_data_prompt(
            description=self.UI_TEXT["review_data_description"],
            table_list=table_list,
        )
