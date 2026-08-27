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
import json
from collections import Counter
from unittest.mock import MagicMock, patch

import pytest
from port.helpers.flow_builder import FlowBuilder, TaskIncompleteError
from port.helpers.uploads import FileTooLargeError
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

    def test_no_data_try_again_loops_back_to_file_prompt(self):
        """Fork behavior: the no-data page offers "Try again" (PayloadTrue),
        which loops back to the file prompt instead of ending the flow."""
        flow = StubFlow(tables=[])
        gen = flow.start_flow()

        # File prompt
        cmd = start_and_skip_logs(gen)
        assert isinstance(cmd, CommandUIRender)

        # Upload valid file → milestones → no-data page
        cmd = advance_past_logs(gen, make_payload_file())
        assert isinstance(cmd, CommandUIRender)

        # "Try again" → back to the file prompt
        cmd = advance_past_logs(gen, make_payload("PayloadTrue"))
        assert isinstance(cmd, CommandUIRender)

    def test_no_data_continue_completes(self):
        """"Continue" (PayloadFalse) on the no-data page ends the flow as a
        completion — clean no-data is a legitimate exit-0 outcome."""
        flow = StubFlow(tables=[])
        gen = flow.start_flow()

        start_and_skip_logs(gen)
        cmd = advance_past_logs(gen, make_payload_file())
        assert isinstance(cmd, CommandUIRender)

        with pytest.raises(StopIteration):
            gen.send(make_payload("PayloadFalse"))

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
    """File fails safety check (oversize / chunked-export sentinel)."""

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
