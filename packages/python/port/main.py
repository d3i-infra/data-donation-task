import traceback
import json
import datetime
from collections.abc import Generator

from port.api.commands import CommandSystemExit, CommandUIRender, CommandSystemDonate
from port.api.file_utils import AsyncFileAdapter
from port.helpers import ui_locale
from port.helpers.flow_builder import TaskIncompleteError
from port.script import process
import port.api.props as props
import port.helpers.port_helpers as ph


def error_flow(platform: str | None, tb: str):
    """
    Generator that handles a Python exception in the donation flow.

    Yields an error consent page, then optionally donates the error log
    if the participant consents.

    This is a PII safety boundary (ADR-0022): uncaught exceptions are caught
    here in Python and routed through consent-gated UI, preventing them
    from reaching the JS-side LogForwarder which would forward unsanitized
    to mono.

    Args:
        platform: Name of the active platform when the error occurred.
        tb: Full traceback string from traceback.format_exc().
    """
    header = props.PropsUIHeader(
        props.Translatable({
            "nl": "Er is iets misgegaan",
            "en": "Something went wrong",
            "de": "Es ist etwas schiefgelaufen",
            "it": "Si è verificato un errore",
            "es": "Se ha producido un error",
        })
    )
    body = [
        props.PropsUIPromptText(text=props.Translatable({"nl": tb, "en": tb, "de": tb, "it": tb, "es": tb})),
        props.PropsUIPromptConfirm(
            text=props.Translatable({
                "nl": "Wilt u de fout rapporteren zodat we het probleem kunnen oplossen?",
                "en": "Would you like to report this error so we can fix the problem?",
                "de": "Möchten Sie diesen Fehler melden, damit wir das Problem beheben können?",
                "it": "Desidera segnalare questo errore in modo che possiamo risolvere il problema?",
                "es": "¿Desea informar de este error para que podamos solucionar el problema?",
            }),
            ok=props.Translatable({
                "nl": "Fout rapporteren",
                "en": "Report error",
                "de": "Fehler melden",
                "it": "Segnala errore",
                "es": "Informar del error",
            }),
            cancel=props.Translatable({
                "nl": "Overslaan",
                "en": "Skip",
                "de": "Überspringen",
                "it": "Salta",
                "es": "Omitir",
            }),
        ),
    ]
    page = props.PropsUIPageDataSubmission(platform or "error", header, body)
    consent_result = yield CommandUIRender(page)

    if consent_result is not None and getattr(consent_result, "__type__", None) == "PayloadTrue":
        error_data = json.dumps({
            "platform": platform,
            "traceback": tb,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        })
        yield CommandSystemDonate("error-report", error_data)

    # Terminal task-incomplete page: without it the participant would be left
    # on the stale error page after the nonzero exit halts the run cycle
    # (#123). Its Confirm must resolve so the generator can exhaust (ADR-0025).
    yield ph.render_task_incomplete_page(platform or "error")


def incomplete_flow(platform: str | None):
    """Terminal handler for TaskIncompleteError: a flow that ended without
    completion but with nothing to report — no error-report consent step.

    Resolvable pre-exit acknowledgment (ADR-0039); must never become an
    unresolved end page (the ADR-0025 EndPage hang).
    """
    yield ph.render_task_incomplete_page(platform or "error")


class ScriptWrapper(Generator):
    def __init__(self, script, platform: str | None = None):
        self.script = script
        self.platform = platform or "unknown"
        self._error_handler = None
        # (code, info) for the terminal exit once a handler flow exhausts.
        # Defaults are the error-flow pair; TaskIncompleteError overrides
        # them with its own category (ADR-0039).
        self._exit_code = 1
        self._exit_info = "Error flow completed"

    def send(self, data):
        if self._error_handler is not None:
            try:
                command = self._error_handler.send(data)
                return command.toDict()
            except StopIteration:
                # Handler-end, not flow-end: a nonzero code tells the host the
                # task was NOT completed (Issue #123). The info string crosses
                # the bridge unconsented and must stay free of exception text
                # (ADR-0022/0023).
                return CommandSystemExit(self._exit_code, self._exit_info).toDict()

        # Automatically wrap JS file readers with AsyncFileAdapter
        if data and getattr(data, "__type__", None) == "PayloadFile":
            data.value = AsyncFileAdapter(data.value)
        elif data and getattr(data, "__type__", None) == "PayloadFiles":
            data.value = [AsyncFileAdapter(reader) for reader in data.value]

        try:
            command = self.script.send(data)
            # If the script yields None (e.g. bare `yield` used as a checkpoint),
            # continue the generator immediately with None so the next step runs.
            while command is None:
                command = self.script.send(None)
        except StopIteration:
            return CommandSystemExit(0, "End of script").toDict()
        except TaskIncompleteError as e:
            # The flow declared itself incomplete (abandoned, upload
            # rejected, donation failed). No error to report — go straight
            # to the task-incomplete page, then exit with the flow's own
            # fixed (code, info) pair.
            self._exit_code = e.exit_code
            self._exit_info = e.exit_info
            self._error_handler = incomplete_flow(self.platform)
            command = next(self._error_handler)
            return command.toDict()
        except Exception:
            tb = traceback.format_exc()
            self._error_handler = error_flow(self.platform, tb)
            command = next(self._error_handler)
            return command.toDict()

        return command.toDict()

    def throw(self, _type=None, _value=None, _traceback=None):
        raise StopIteration


def start(data):
    """Entry from py_worker.js.

    `data` is the #960-style context dict {"sessionId", "locale", "platform"}
    posted by WorkerProcessingEngine.firstRunCycle. sessionId arrives as a JSON
    string (Assembly builds it with String(Date.now())), so downstream
    donation-key logic (ADR-0020) always sees str.
    """
    session_id = data.get("sessionId")
    platform = data.get("platform")
    ui_locale.set_ui_locale(data.get("locale"))
    script = process(session_id, platform)
    return ScriptWrapper(script, platform=platform)
