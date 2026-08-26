"""
Sync check between the two locale-policy data files.

JS owns locale policy (`packages/data-collector/src/locale/ui_locales.json`);
Python carries a byte-identical mirror
(`packages/python/port/helpers/ui_locales.json`) so `normalize_ui_locale`
stays a defense-in-depth copy of `policy.ts`'s `normalizeLocale`, not a
second source of truth. This test asserts the two never drift — it hard-fails
(never skips) if either side is missing.
"""
import json
from pathlib import Path

from port.helpers import ui_locale

# packages/python/tests/test_ui_locales_sync.py -> repo root is 3 parents up.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_DATA_COLLECTOR_JSON = _REPO_ROOT / "packages" / "data-collector" / "src" / "locale" / "ui_locales.json"


def test_data_collector_json_exists():
    assert _DATA_COLLECTOR_JSON.is_file(), f"data-collector locale JSON not found at {_DATA_COLLECTOR_JSON}"


def test_python_mirror_matches_data_collector_source():
    with _DATA_COLLECTOR_JSON.open(encoding="utf-8") as f:
        js_data = json.load(f)

    py_data = {
        "supported": ui_locale.SUPPORTED_UI_LOCALES,
        "default": ui_locale.DEFAULT_UI_LOCALE,
        "provisional": ui_locale.PROVISIONAL_UI_LOCALES,
    }

    assert py_data == js_data
