"""Study orchestration — platform list, filtering, sequencing.

This module defines which platforms are included in the study and
delegates per-platform flows to FlowBuilder subclasses via `yield from`.

Platform modules are imported lazily to minimize startup time in
per-platform builds where only one platform is needed.
"""
from importlib import import_module

import port.helpers.port_helpers as ph


# Registry: platform display name → (module path, class name)
PLATFORM_REGISTRY = [

    ("TikTok", "port.platforms.tiktok", "TikTokFlow"),
]


def process(session_id: str, platform: str | None = None):
    """Run the data donation study.

    Args:
        session_id: Unique session identifier (from host).
        platform: If set (via VITE_PLATFORM), run only this platform.
    """
    entries = PLATFORM_REGISTRY
    if platform:
        entries = [
            (name, mod, cls) for name, mod, cls in entries
            if name.lower() == platform.lower()
        ]

    for platform_name, module_path, class_name in entries:
        module = import_module(module_path)
        flow_class = getattr(module, class_name)
        flow = flow_class(session_id)

        yield from ph.emit_log("info", f"Starting platform: {platform_name}")
        yield from flow.start_flow()

    yield from ph.emit_log("info", "Study complete")
