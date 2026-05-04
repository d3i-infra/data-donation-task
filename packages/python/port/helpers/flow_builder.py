"""FlowBuilder — shared per-platform donation flow orchestration.

Subclass this to implement a platform-specific donation flow.
Override validate_file() and extract_data(). Call start_flow()
as a generator from script.py via `yield from`.
"""
from abc import abstractmethod
from collections.abc import Generator
import json
import logging

import port.api.props as props
import port.api.d3i_props as d3i_props
import port.helpers.port_helpers as ph
import port.helpers.validate as validate
import port.helpers.uploads as uploads

logger = logging.getLogger(__name__)


class FlowBuilder:
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
            }),
            "review_data_header": props.Translatable({
                "en": f"Your {self.platform_name} data",
                "nl": f"Uw {self.platform_name} gegevens",
            }),
            "retry_header": props.Translatable({
                "en": "Try again",
                "nl": "Probeer opnieuw",
            }),
            "review_data_description": props.Translatable({
                "en": f"Below you will find a curated selection of {self.platform_name} data.",
                "nl": f"Hieronder vindt u een zorgvuldig samengestelde selectie van {self.platform_name} gegevens.",
            }),
        }

    def start_flow(self):
        """Main per-platform flow: file→materialize→safety→validate→retry→extract→consent→donate.

        This is a generator. script.py calls it via `yield from flow.start_flow()`.
        Control flow rules:
        - continue: retry upload only
        - break: successful extraction, proceed to consent
        - return: every terminal path

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

            # Skip: anything other than a PayloadFile. PayloadString/
            # WORKERFS support was retired with extraction/AD0007.
            # Distinguish the participant-skip case from an unexpected
            # payload type so a legacy/mismatched worker is observable.
            if file_result.__type__ != "PayloadFile":
                logger.info("Skipped at file selection for %s", self.platform_name)
                yield from ph.emit_log(
                    "info",
                    f"[{self.platform_name}] Upload skipped: type={file_result.__type__}",
                )
                return

            # AsyncFileAdapter — file-like, passed directly to validators
            # and extractors. Never materialized to a path. See AD0007.
            archive = file_result.value
            yield from ph.emit_log(
                "info",
                f"[{self.platform_name}] Upload received: size={archive.size}",
            )

            # 2. Safety check (size only — uses JS metadata, no read)
            try:
                uploads.check_payload_size(file_result)
            except (uploads.FileTooLargeError, uploads.ChunkedExportError) as e:
                logger.error("Safety check failed for %s: %s", self.platform_name, e)
                yield from ph.emit_log("info", f"[{self.platform_name}] Safety check failed: {type(e).__name__}")
                _ = yield ph.render_safety_error_page(self.platform_name, e)
                return

            # 3. Validate
            validation = self.validate_file(archive)
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
                retry_prompt = self.generate_retry_prompt()
                retry_result = yield ph.render_page(self.UI_TEXT["retry_header"], retry_prompt)
                if retry_result.__type__ == "PayloadTrue":
                    continue  # loop back to step 1
                return  # user declined retry

            # 5. Extract
            logger.info("Extracting data for %s", self.platform_name)
            raw_result = self.extract_data(archive, validation)
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

            # 7. If no tables → no-data page
            if not result.tables:
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
            return

        yield from ph.emit_log("info", f"[{self.platform_name}] Donation result: success")

    # Methods to be overridden by platform-specific implementations
    def generate_file_prompt(self):
        """Generate platform-specific file prompt."""
        return ph.generate_file_prompt("application/zip")

    @abstractmethod
    def validate_file(self, file: str) -> validate.ValidateInput:
        """Validate the file according to platform-specific rules."""
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def extract_data(self, file: str, validation: validate.ValidateInput) -> d3i_props.ExtractionResult:
        """Extract data from file using platform-specific logic."""
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
