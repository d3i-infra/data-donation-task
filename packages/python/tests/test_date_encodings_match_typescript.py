"""Pins ``DATE_ENCODINGS`` (Python) and ``zDateEncoding`` (TypeScript) together.

Both lists name the encodings the front end's ``interpretTimestamp`` knows
(ADR-0043). Nothing previously checked that they agreed — this reads the
TypeScript source directly (a regex over the ``z.enum([...])`` literal) rather
than importing it, since this suite runs under plain pytest, not a JS runtime.
"""
import re
from pathlib import Path

from port.helpers.port_config_validator import DATE_ENCODINGS

# Relative to this test file's location: packages/python/tests/ -> repo root.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_TYPES_TS = (
    _REPO_ROOT
    / "packages" / "data-collector" / "src" / "components" / "consent_form_viz"
    / "visualization_plugin" / "types.ts"
)

_Z_DATE_ENCODING_RE = re.compile(
    r"export const zDateEncoding = z\.enum\(\[(?P<members>[^\]]*)\]\)"
)


def _read_z_date_encoding_members() -> tuple[str, ...]:
    text = _TYPES_TS.read_text(encoding="utf-8")
    match = _Z_DATE_ENCODING_RE.search(text)
    assert match, f"zDateEncoding = z.enum([...]) not found in {_TYPES_TS}"
    members_src = match.group("members")
    members = re.findall(r"['\"]([^'\"]+)['\"]", members_src)
    assert members, f"no string members parsed out of: {members_src!r}"
    return tuple(members)


def test_date_encodings_match_the_typescript_enum():
    ts_members = _read_z_date_encoding_members()
    assert set(ts_members) == set(DATE_ENCODINGS), (
        f"DATE_ENCODINGS {sorted(DATE_ENCODINGS)} != "
        f"zDateEncoding {sorted(ts_members)} in {_TYPES_TS}"
    )
    # Also pin the file exists at the path the DATE_ENCODINGS comment now points to.
    assert _TYPES_TS.is_file()
