"""Study orchestration — platform loading and sequencing.

Each platform needs a ``configs/<platform>_config.json`` file.
Generate one with:  pnpm generate-config <platform>

In dev mode set VITE_PLATFORM to select the platform::

    VITE_PLATFORM=example pnpm start
"""
from importlib import import_module

import port.helpers.port_helpers as ph
from port.helpers.port_config_validator import validate_or_raise


def _check_platform_config(platform: str) -> None:
    """Raise if ``configs/<platform>_config.json`` is missing or invalid.

    Raises
    ------
    FileNotFoundError
        If the config file is absent.
    ValidationError
        If the config file is malformed (invalid JSON or schema/registry errors).
    """
    validate_or_raise(platform)


def process(session_id: str, platform: str | None):
    """Run the data donation study.

    Args:
        session_id: Unique session identifier (from host).
        platform: Platform name passed from VITE_PLATFORM via the JS layer.
            A host that sends no platform yields None here; because this is a
            generator, the resulting error is raised on the first send() and
            ScriptWrapper turns it into the consent-gated error report — the
            same path the previous `None.lower()` AttributeError took.
    """
    if not platform:
        raise ValueError("No platform supplied by the host; cannot start the study")

    _check_platform_config(platform.lower())

    module = import_module(f"port.platforms.{platform.lower()}")

    yield from ph.emit_log("info", f"Starting platform: {platform}")
    yield from module.process(session_id)

    yield from ph.emit_log("info", "Study complete")
