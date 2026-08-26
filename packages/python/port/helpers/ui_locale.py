"""Session-fixed UI locale received from the host via port.start.

This is the locale the participant-facing UI renders in, set once per
session from the #960-shaped context (`port.start({"sessionId", "locale",
"platform"})`). It is unrelated to `helpers.validate.Language` — the DDP
*export* language enum used when parsing a participant's exported data —
and the two must never be synced or conflated.
"""
import json
from importlib import resources
from typing import Any

_data: dict[str, Any] = json.loads(
    resources.files("port.helpers").joinpath("ui_locales.json").read_text(encoding="utf-8")
)

SUPPORTED_UI_LOCALES: list[str] = _data["supported"]
DEFAULT_UI_LOCALE: str = _data["default"]
PROVISIONAL_UI_LOCALES: list[str] = _data["provisional"]

_current: str = DEFAULT_UI_LOCALE


def normalize_ui_locale(raw: Any) -> str:
    """Mirror of `policy.ts`'s `normalizeLocale`: non-string -> default,
    else the lowercased primary subtag (split on '-'/'_') if supported,
    else default."""
    if not isinstance(raw, str):
        return DEFAULT_UI_LOCALE
    primary = raw.strip().lower().split("-")[0].split("_")[0]
    return primary if primary in SUPPORTED_UI_LOCALES else DEFAULT_UI_LOCALE


def set_ui_locale(raw: Any) -> None:
    global _current
    _current = normalize_ui_locale(raw)


def get_ui_locale() -> str:
    return _current
