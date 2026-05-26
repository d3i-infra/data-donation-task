import traceback
import json
import datetime
from collections.abc import Generator

from port.api.commands import CommandSystemExit, CommandUIRender, CommandSystemDonate
from port.api.file_utils import AsyncFileAdapter
from port.script import process
import port.api.props as props


def error_flow(platform: str | None, tb: str):
    """
    Generator that handles a Python exception in the donation flow.

    Yields an error consent page, then optionally donates the error log
    if the participant consents.

    This is a PII safety boundary (AD0009): uncaught exceptions are caught
    here in Python and routed through consent-gated UI, preventing them
    from reaching the JS-side LogForwarder which would forward unsanitized
    to mono.

    Args:
        platform: Name of the active platform when the error occurred.
        tb: Full traceback string from traceback.format_exc().
    """
    header = props.PropsUIHeader(
        props.Translatable({"nl": "Er is iets misgegaan", "en": "Something went wrong"})
    )
    body = [
        props.PropsUIPromptText(text=props.Translatable({"nl": tb, "en": tb})),
        props.PropsUIPromptConfirm(
            text=props.Translatable({
                "nl": "Wilt u de fout rapporteren zodat we het probleem kunnen oplossen?",
                "en": "Would you like to report this error so we can fix the problem?",
            }),
            ok=props.Translatable({"nl": "Fout rapporteren", "en": "Report error"}),
            cancel=props.Translatable({"nl": "Overslaan", "en": "Skip"}),
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


class ScriptWrapper(Generator):
    def __init__(self, script, platform: str | None = None):
        self.script = script
        self.platform = platform or "unknown"
        self._error_handler = None

    def send(self, data):
        if self._error_handler is not None:
            try:
                command = self._error_handler.send(data)
                return command.toDict()
            except StopIteration:
                return CommandSystemExit(0, "End of script").toDict()

        # Automatically wrap JS file readers with AsyncFileAdapter
        if data and getattr(data, "__type__", None) == "PayloadFile":
            data.value = AsyncFileAdapter(data.value)

        try:
            command = self.script.send(data)
            # If the script yields None (e.g. bare `yield` used as a checkpoint),
            # continue the generator immediately with None so the next step runs.
            while command is None:
                command = self.script.send(None)
        except StopIteration:
            return CommandSystemExit(0, "End of script").toDict()
        except Exception:
            tb = traceback.format_exc()
            self._error_handler = error_flow(self.platform, tb)
            command = next(self._error_handler)
            return command.toDict()

        return command.toDict()

    def throw(self, _type=None, _value=None, _traceback=None):
        raise StopIteration


def start(sessionId, platform):
    script = process(sessionId, platform)
    return ScriptWrapper(script, platform=platform)
