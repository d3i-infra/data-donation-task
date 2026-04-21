"""Study orchestration — platform loading and sequencing.

A ``port_config.py`` file must be present in the port package.
Generate one with:  pnpm generate-config <platform>
"""
import importlib.resources
import json
from importlib import import_module

import port.helpers.port_helpers as ph
from port.helpers.port_config_validator import validate_or_raise


def _read_port_config() -> str:
    """Read and validate port_config.json, returning the platform name.

    Raises
    ------
    FileNotFoundError
        If port_config.json has not been generated yet.
    ValidationError
        If port_config.json fails schema or registry validation.
    """
    try:
        ref = importlib.resources.files("port") / "port_config.json"
        raw = json.loads(ref.read_text(encoding="utf-8"))
    except (FileNotFoundError, TypeError) as exc:
        raise FileNotFoundError(
            "port_config.json not found. "
            "Generate it first by running:  pnpm generate-config <platform>"
        ) from exc
    except json.JSONDecodeError as exc:
        raise json.JSONDecodeError(
            f"port_config.json contains invalid JSON at line {exc.lineno}, column {exc.colno}.\n"
            f"  {exc.msg}",
            exc.doc,
            exc.pos,
        ) from exc

    platform_info = raw.get("platform_info", {})
    platform = platform_info.get("name") if isinstance(platform_info, dict) else None
    if not platform:
        raise FileNotFoundError(
            "port_config.json is missing 'platform_info.name'. "
            "Regenerate it with:  pnpm generate-config <platform>"
        )
    validate_or_raise(platform.lower())
    return platform


def process(session_id: str, platform: str | None = None):
    """Run the data donation study.

    Args:
        session_id: Unique session identifier (from host).
        platform: If set (via VITE_PLATFORM), run only this platform.
                  When None, platform is read from port_config.json.
    """
    active_platform = platform or _read_port_config()

    module = import_module(f"port.platforms.{active_platform.lower()}")

    yield from ph.emit_log("info", f"Starting platform: {active_platform}")
    yield from module.process(session_id)

    yield from ph.emit_log("info", "Study complete")
