"""Session-fixed UI locale received from the host via port.start.

This is the locale the participant-facing UI renders in, set once per
session from the #960-shaped context (`port.start({"sessionId", "locale",
"platform"})`). It is unrelated to `helpers.validate.Language` — the DDP
*export* language enum used when parsing a participant's exported data —
and the two must never be synced or conflated.
"""
DEFAULT_UI_LOCALE = "en"
_current: str = DEFAULT_UI_LOCALE


def set_ui_locale(raw) -> None:
    global _current
    _current = raw if isinstance(raw, str) and raw else DEFAULT_UI_LOCALE


def get_ui_locale() -> str:
    return _current
