"""Tests for FlowBuilder.start_flow() — all six flow paths.

FlowBuilder yields CommandSystemLog milestones between UI commands.
Tests use advance_past_logs() / start_and_skip_logs() to skip past
log commands to the next UI/donate command.

Per ADR-0026, PayloadFile is the only accepted upload type;
PayloadString/WORKERFS support was retired. The upload pipeline does
not materialize the file to a path — the AsyncFileAdapter is passed
directly to validate_file/extract_data, and size policy is enforced
via check_payload_size() against adapter.size before any read.
"""
import io
import json
import zipfile
from collections import Counter
from unittest.mock import MagicMock, patch

import pytest
from port.helpers.flow_builder import FlowBuilder, TaskIncompleteError
from port.helpers.uploads import FileTooLargeError
from port.helpers.archive_set import ArchiveSet
from port.api.commands import CommandUIRender, CommandSystemDonate, CommandSystemLog
from port.api.d3i_props import ExtractionResult
import port.api.props as props
import port.api.d3i_props as d3i_props
from port.helpers.validate import ValidateInput


class StubFlow(FlowBuilder):
    """Concrete FlowBuilder for testing."""

    def __init__(self, session_id="test-session", validation_status=0, tables=None):
        super().__init__(session_id, "TestPlatform")
        self._validation_status = validation_status
        self._tables = tables if tables is not None else [
            d3i_props.PropsUIPromptConsentFormTableViz(
                id="test_table",
                data_frame=__import__("pandas").DataFrame({"col": [1, 2]}),
                title=props.Translatable({"en": "Test", "nl": "Test"}),
            )
        ]

    def validate_file(self, file):
        v = MagicMock(spec=ValidateInput)
        v.get_status_code_id.return_value = self._validation_status
        v.current_ddp_category = MagicMock(id="json_en")
        return v

    def extract_data(self, file, validation):
        return ExtractionResult(tables=self._tables, errors=Counter())


def make_payload(type_name, **attrs):
    p = MagicMock()
    p.__type__ = type_name
    for k, v in attrs.items():
        setattr(p, k, v)
    return p


def make_payload_file(size: int = 1024) -> MagicMock:
    """Build a PayloadFile-shaped payload whose adapter reports `size` bytes.

    ADR-0026: the upload-path safety check reads adapter.size from JS
    metadata, never the bytes themselves. Tests construct adapters that
    only need a `.size` attribute set.
    """
    adapter = MagicMock()
    adapter.size = size
    return make_payload("PayloadFile", value=adapter)


def advance_past_logs(gen, response=None):
    """Send response to generator, skip any CommandSystemLog commands, return next non-log command."""
    cmd = gen.send(response)
    while isinstance(cmd, CommandSystemLog):
        cmd = gen.send(make_payload("PayloadVoid"))
    return cmd


def start_and_skip_logs(gen):
    """Start generator and skip any initial log commands."""
    cmd = next(gen)
    while isinstance(cmd, CommandSystemLog):
        cmd = gen.send(make_payload("PayloadVoid"))
    return cmd


class TestHappyPath:
    """User uploads valid file → extraction has data → consents → donates."""

    def test_happy_path_yields_donate(self):
        flow = StubFlow()
        gen = flow.start_flow()

        # Step 1: file prompt
        cmd = start_and_skip_logs(gen)
        assert isinstance(cmd, CommandUIRender)

        # Step 2: user uploads file → milestones → consent form
        cmd = advance_past_logs(gen, make_payload_file())
        assert isinstance(cmd, CommandUIRender)

        # Step 3: user consents → milestones → donate command
        consent_payload = make_payload("PayloadJSON", value='{"data": "test"}')
        cmd = advance_past_logs(gen, consent_payload)
        assert isinstance(cmd, CommandSystemDonate)
        assert cmd.key == "test-session-testplatform"

        # Donate result → final milestone → generator exhausts
        with pytest.raises(StopIteration):
            advance_past_logs(gen, make_payload("PayloadVoid"))


class TestRetryPath:
    """User uploads invalid file → retries → uploads valid file → succeeds."""

    def test_retry_loops_back(self):
        call_count = [0]
        flow = StubFlow()

        def varying_validate(file):
            call_count[0] += 1
            v = MagicMock(spec=ValidateInput)
            v.get_status_code_id.return_value = 1 if call_count[0] == 1 else 0
            v.current_ddp_category = MagicMock(id="json_en")
            return v

        flow.validate_file = varying_validate

        gen = flow.start_flow()

        # File prompt
        cmd = start_and_skip_logs(gen)
        assert isinstance(cmd, CommandUIRender)

        # Upload invalid file → milestones → retry prompt
        cmd = advance_past_logs(gen, make_payload_file())
        assert isinstance(cmd, CommandUIRender)

        # User clicks "Try again" → loops back → file prompt
        cmd = advance_past_logs(gen, make_payload("PayloadTrue"))
        assert isinstance(cmd, CommandUIRender)

        # Upload valid file → milestones → consent form
        cmd = advance_past_logs(gen, make_payload_file())
        assert isinstance(cmd, CommandUIRender)

    def test_retry_declined_raises_abandoned(self):
        """Declining the retry prompt must NOT exhaust the generator (exit 0 /
        completed at the host) — it raises TaskIncompleteError with the
        participant-abandoned exit code so the host keeps the task pending."""
        flow = StubFlow(validation_status=1)
        gen = flow.start_flow()

        # File prompt
        cmd = start_and_skip_logs(gen)
        assert isinstance(cmd, CommandUIRender)

        # Upload invalid file → milestones → retry prompt
        cmd = advance_past_logs(gen, make_payload_file())
        assert isinstance(cmd, CommandUIRender)

        # User clicks "Continue" (declines retry)
        with pytest.raises(TaskIncompleteError) as exc:
            advance_past_logs(gen, make_payload("PayloadFalse"))
        assert exc.value.exit_code == 2


class TestSkipPath:
    """User skips file selection (anything other than PayloadFile)."""

    def test_skip_raises_abandoned(self):
        flow = StubFlow()
        gen = flow.start_flow()

        # File prompt
        cmd = start_and_skip_logs(gen)
        assert isinstance(cmd, CommandUIRender)

        # User skips with non-PayloadFile response — emits an
        # "Upload skipped" diagnostic log, then ends abandoned so the
        # host keeps the task pending instead of completing it.
        with pytest.raises(TaskIncompleteError) as exc:
            advance_past_logs(gen, make_payload("PayloadFalse"))
        assert exc.value.exit_code == 2

    def test_payload_string_now_treated_as_skip(self):
        """SRC compat dropped per ADR-0026: PayloadString is not a valid upload."""
        flow = StubFlow()
        gen = flow.start_flow()

        cmd = start_and_skip_logs(gen)
        assert isinstance(cmd, CommandUIRender)

        # PayloadString hits the same skip branch as any other non-PayloadFile,
        # which emits an "Upload skipped" diagnostic log and ends abandoned.
        with pytest.raises(TaskIncompleteError) as exc:
            advance_past_logs(gen, make_payload("PayloadString", value="/tmp/legacy.zip"))
        assert exc.value.exit_code == 2


class TestNoDataPath:
    """Valid file but extraction returns empty table list."""

    def test_no_data_shows_page_then_returns(self):
        flow = StubFlow(tables=[])
        gen = flow.start_flow()

        # File prompt
        cmd = start_and_skip_logs(gen)
        assert isinstance(cmd, CommandUIRender)

        # Upload valid file → milestones → no-data page
        cmd = advance_past_logs(gen, make_payload_file())
        assert isinstance(cmd, CommandUIRender)

        # User acknowledges
        with pytest.raises(StopIteration):
            gen.send(make_payload("PayloadTrue"))

    def test_no_data_with_extraction_errors_is_a_failure_not_a_completion(self):
        """Zero tables WITH extraction errors is an extraction failure, never
        the clean no-data acknowledgment (which completes) — see ADR-0019's
        no-data/extraction-bug separation. It routes through the uncaught-error
        path, so the participant is offered the error report and stays pending."""
        flow = StubFlow(tables=[])
        flow.extract_data = lambda file, validation: ExtractionResult(
            tables=[], errors=Counter({"KeyError": 3})
        )
        gen = flow.start_flow()

        start_and_skip_logs(gen)
        with pytest.raises(RuntimeError):
            advance_past_logs(gen, make_payload_file())


class TestSafetyErrorPath:
    """File fails safety check (oversize)."""

    @patch(
        "port.helpers.flow_builder.uploads.check_payload_size",
        side_effect=FileTooLargeError("too big"),
    )
    def test_safety_error_shows_page_then_raises_upload_rejected(self, mock_check):
        flow = StubFlow()
        gen = flow.start_flow()

        # File prompt
        cmd = start_and_skip_logs(gen)
        assert isinstance(cmd, CommandUIRender)

        # Upload file that fails safety → milestones → safety error page
        cmd = advance_past_logs(gen, make_payload_file(size=3 * 1024**3))
        assert isinstance(cmd, CommandUIRender)

        # User acknowledges → upload-rejected exit, not a completion
        with pytest.raises(TaskIncompleteError) as exc:
            gen.send(make_payload("PayloadTrue"))
        assert exc.value.exit_code == 4


class TestDonateFailurePath:
    """Donation fails after consent."""

    @patch("port.helpers.flow_builder.ph.handle_donate_result", return_value=False)
    def test_donate_failure_shows_page_then_raises_donation_failed(self, mock_handle):
        flow = StubFlow()
        gen = flow.start_flow()

        # File prompt
        cmd = start_and_skip_logs(gen)

        # Upload valid file → milestones → consent form
        cmd = advance_past_logs(gen, make_payload_file())
        assert isinstance(cmd, CommandUIRender)

        # User consents → milestones → donate command
        cmd = advance_past_logs(gen, make_payload("PayloadJSON", value='{"data": "test"}'))
        assert isinstance(cmd, CommandSystemDonate)

        # Donate result → milestones → donate failure page
        cmd = advance_past_logs(gen, make_payload("PayloadResponse", success=False))
        assert isinstance(cmd, CommandUIRender)

        # User acknowledges → donation-failed exit, not a completion
        with pytest.raises(TaskIncompleteError) as exc:
            gen.send(make_payload("PayloadTrue"))
        assert exc.value.exit_code == 3

    @patch("port.helpers.flow_builder.ph.handle_donate_result", return_value=False)
    def test_decline_record_failure_stays_silent_completion(self, mock_handle):
        """A failed decline-record delivery is invisible infrastructure: the
        participant declined to donate, so the flow still completes (exit 0)."""
        flow = StubFlow()
        gen = flow.start_flow()

        start_and_skip_logs(gen)
        cmd = advance_past_logs(gen, make_payload_file())
        assert isinstance(cmd, CommandUIRender)

        # User declines consent → decline record donated
        cmd = advance_past_logs(gen, make_payload("PayloadFalse"))
        assert isinstance(cmd, CommandSystemDonate)

        # Delivery of the decline record fails → silent, plain exhaustion
        with pytest.raises(StopIteration):
            advance_past_logs(gen, make_payload("PayloadResponse", success=False))


class TestSessionIdType:
    def test_session_id_accepts_string(self):
        flow = StubFlow(session_id="abc-123")
        assert flow.session_id == "abc-123"


class TestDonateKeyFormat:
    def test_donate_key_includes_platform(self):
        """Donate key should be '{session_id}-{platform_name.lower()}'."""
        flow = StubFlow(session_id="sess-42")
        gen = flow.start_flow()
        start_and_skip_logs(gen)  # file prompt
        advance_past_logs(gen, make_payload_file())  # consent form
        cmd = advance_past_logs(gen, make_payload("PayloadJSON", value="{}"))  # donate
        assert cmd.key == "sess-42-testplatform"


class TestUploadAdapterPassthrough:
    """Verify the adapter (file_result.value) is passed to validate/extract,
    not a path string. ADR-0026 streaming invariant.
    """

    def test_validate_file_receives_adapter(self):
        """validate_file is called with file_result.value, not a path."""
        flow = StubFlow()
        observed = []
        original_validate = flow.validate_file

        def spy_validate(file):
            observed.append(file)
            return original_validate(file)

        flow.validate_file = spy_validate

        gen = flow.start_flow()
        start_and_skip_logs(gen)
        adapter = MagicMock()
        adapter.size = 1024
        advance_past_logs(gen, make_payload("PayloadFile", value=adapter))

        assert observed == [adapter]

    def test_extract_data_receives_adapter(self):
        """extract_data is called with file_result.value, not a path."""
        flow = StubFlow()
        observed = []
        original_extract = flow.extract_data

        def spy_extract(file, validation):
            observed.append(file)
            return original_extract(file, validation)

        flow.extract_data = spy_extract

        gen = flow.start_flow()
        start_and_skip_logs(gen)
        adapter = MagicMock()
        adapter.size = 1024
        advance_past_logs(gen, make_payload("PayloadFile", value=adapter))

        assert observed == [adapter]


class MultiStubFlow(StubFlow):
    """Concrete FlowBuilder configured for a PayloadFiles (chunked-export) upload."""

    expected_file_payload = "PayloadFiles"


def make_payload_files(n=2):
    """Build a PayloadFiles-shaped payload with `n` valid, readable zip parts."""
    payload = MagicMock()
    payload.__type__ = "PayloadFiles"
    parts = []
    for i in range(n):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr(f"Takeout/f{i}.json", "{}")
        buf.seek(0)
        buf.name = f"takeout-{i + 1}-001.zip"
        buf.size = 100
        parts.append(buf)
    payload.value = parts
    return payload


class TestMultiFileFlow:
    """PayloadFiles sets are unioned into one ArchiveSet before validate/extract."""

    def test_set_reaches_validate_and_extract_as_archive_set(self):
        flow = MultiStubFlow()
        observed = []
        original_validate = flow.validate_file

        def spy_validate(archive_set):
            observed.append(archive_set)
            return original_validate(archive_set)

        flow.validate_file = spy_validate

        gen = flow.start_flow()

        # File prompt
        cmd = start_and_skip_logs(gen)
        assert isinstance(cmd, CommandUIRender)

        # Upload a two-part set → milestones → consent form (StubFlow's
        # validate_file stub always reports status 0)
        cmd = advance_past_logs(gen, make_payload_files(n=2))
        assert isinstance(cmd, CommandUIRender)

        assert len(observed) == 1
        assert isinstance(observed[0], ArchiveSet)
        assert observed[0].members == ["Takeout/f0.json", "Takeout/f1.json"]

    def test_single_payload_to_multi_flow_raises_upload_rejected(self):
        flow = MultiStubFlow()
        gen = flow.start_flow()

        # File prompt
        cmd = start_and_skip_logs(gen)
        assert isinstance(cmd, CommandUIRender)

        # A singular PayloadFile arriving when this flow expects PayloadFiles
        # is a protocol mismatch, not a participant skip: a visible error
        # page, whose prompt came from ph.render_protocol_error_page.
        cmd = advance_past_logs(gen, make_payload_file())
        assert isinstance(cmd, CommandUIRender)
        assert cmd.page.header.title.translations["en"] == "Something went wrong"
        assert "out of sync" in cmd.page.body.text.translations["en"]

        # Acknowledging the error page is not a completion: nothing usable
        # was ever uploaded, so the run exits nonzero (ADR-0039).
        with pytest.raises(TaskIncompleteError) as exc:
            gen.send(make_payload("PayloadTrue"))
        assert exc.value.exit_code == 4

    def test_multi_payload_to_single_flow_raises_upload_rejected(self):
        flow = StubFlow()
        gen = flow.start_flow()

        # File prompt
        cmd = start_and_skip_logs(gen)
        assert isinstance(cmd, CommandUIRender)

        # A PayloadFiles set arriving when this flow expects a singular
        # PayloadFile is the symmetric protocol mismatch — also a visible
        # error page, never a participant skip.
        cmd = advance_past_logs(gen, make_payload_files())
        assert isinstance(cmd, CommandUIRender)
        assert cmd.page.header.title.translations["en"] == "Something went wrong"
        assert "out of sync" in cmd.page.body.text.translations["en"]

        with pytest.raises(TaskIncompleteError) as exc:
            gen.send(make_payload("PayloadTrue"))
        assert exc.value.exit_code == 4

    def test_unknown_type_raises_upload_rejected(self):
        flow = StubFlow()
        gen = flow.start_flow()

        # File prompt
        cmd = start_and_skip_logs(gen)
        assert isinstance(cmd, CommandUIRender)

        # An unrecognized __type__ is a protocol mismatch too, not a skip.
        cmd = advance_past_logs(gen, make_payload("PayloadBogus"))
        assert isinstance(cmd, CommandUIRender)
        assert cmd.page.header.title.translations["en"] == "Something went wrong"

        with pytest.raises(TaskIncompleteError) as exc:
            gen.send(make_payload("PayloadTrue"))
        assert exc.value.exit_code == 4

    def test_established_skips_still_raise_abandoned_for_multi_flow(self):
        flow = MultiStubFlow()
        gen = flow.start_flow()

        # File prompt
        cmd = start_and_skip_logs(gen)
        assert isinstance(cmd, CommandUIRender)

        # PayloadFalse is still a silent skip, even for a flow that expects
        # PayloadFiles — the pinned skip set (PayloadFalse/PayloadVoid/
        # PayloadString) never routes to the protocol-error page. It is
        # still an abandonment, so it exits nonzero rather than completing.
        with pytest.raises(TaskIncompleteError) as exc:
            advance_past_logs(gen, make_payload("PayloadFalse"))
        assert exc.value.exit_code == 2

    def test_corrupt_part_retry_declined_raises_abandoned(self):
        flow = MultiStubFlow()
        gen = flow.start_flow()

        # File prompt
        cmd = start_and_skip_logs(gen)
        assert isinstance(cmd, CommandUIRender)

        good = make_payload_files(n=1).value[0]
        bad = io.BytesIO(b"not a zip file")
        bad.seek(0)
        bad.name = "takeout-2-001.zip"
        bad.size = len(b"not a zip file")

        # One good part + one unreadable part → ArchiveSet(...) raises
        # zipfile.BadZipFile → routed to the existing retry prompt, not a
        # traceback.
        cmd = advance_past_logs(gen, make_payload("PayloadFiles", value=[good, bad]))
        assert isinstance(cmd, CommandUIRender)
        assert cmd.page.header.title.translations["en"] == "Try again"

        # User declines the retry → abandonment, not a completion. Same
        # nonzero exit as declining retry after an invalid single file.
        with pytest.raises(TaskIncompleteError) as exc:
            advance_past_logs(gen, make_payload("PayloadFalse"))
        assert exc.value.exit_code == 2


class TestTooManyFilesSafetyPath:
    """A PayloadFiles set over uploads.MAX_UPLOAD_FILES hits the safety-error
    page — not an uncaught TooManyFilesError. Widens the flow_builder safety
    catch from FileTooLargeError alone to (FileTooLargeError, TooManyFilesError).
    """

    def test_too_many_files_shows_safety_error_page_then_raises_upload_rejected(self):
        flow = MultiStubFlow()
        gen = flow.start_flow()

        # File prompt
        cmd = start_and_skip_logs(gen)
        assert isinstance(cmd, CommandUIRender)

        # 17 files exceeds MAX_UPLOAD_FILES (16) → TooManyFilesError, caught
        # and rendered as the safety-error page.
        cmd = advance_past_logs(gen, make_payload_files(n=17))
        assert isinstance(cmd, CommandUIRender)
        assert cmd.page.header.title.translations["en"] == "File cannot be processed"

        # User acknowledges → upload-rejected exit, not a completion. The
        # count guard reaches the same terminal path as the size guard.
        with pytest.raises(TaskIncompleteError) as exc:
            gen.send(make_payload("PayloadTrue"))
        assert exc.value.exit_code == 4
