"""Tests for the release-artifact verifier (scripts/verify_release_wheel.py).

This script is repo tooling, not part of the ``port`` package, so it is
loaded by file path the same way ``tests/test_validate_launcher.py`` loads
``scripts/validate_port_config.py`` — see ADR-0004 for why that boundary
exists (no environment awareness leaks into ``port/``).

Wheels and sdist tarballs are real zip/tar files, so the forbidden/required
path checks are tested against small synthetic archives built in
``tmp_path`` — no real build is needed to exercise the logic that matters:
the forbidden e2etest and e2etest_multifile paths (exact, and as drift — any
path referencing "e2etest" or "e2etest_multifile" at all) must be absent, and
the selected platform's own module + config must be present in the wheel.
"""

import importlib.util
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
VERIFIER = REPO_ROOT / "scripts" / "verify_release_wheel.py"


def _load_verifier_module():
    spec = importlib.util.spec_from_file_location("verify_release_wheel", VERIFIER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


verifier = _load_verifier_module()


def _make_wheel(path: Path, names: list[str]) -> Path:
    with zipfile.ZipFile(path, "w") as z:
        for name in names:
            z.writestr(name, "synthetic content")
    return path


def _make_sdist(path: Path, names: list[str], prefix: str = "port-0.0.0") -> Path:
    """A synthetic sdist tarball — real sdists nest every member under
    ``<name>-<version>/``, so the fixture does too (the point of the
    drift-only check is that it must not depend on that prefix)."""
    with tarfile.open(path, "w:gz") as t:
        for name in names:
            data = b"synthetic content"
            info = tarfile.TarInfo(name=f"{prefix}/{name}")
            info.size = len(data)
            import io

            t.addfile(info, io.BytesIO(data))
    return path


CLEAN_REAL_PLATFORM_NAMES = [
    "port/platforms/example.py",
    "port/configs/example_config.json",
    "port/platforms/tiktok.py",
    "port/configs/tiktok_config.json",
]


class TestCheckWheelNames:
    """Unit tests against the in-memory list of archive member names —
    no filesystem or zip involved, for the fast path/logic cases."""

    def test_clean_wheel_passes(self):
        violations = verifier.check_wheel_names(CLEAN_REAL_PLATFORM_NAMES, "example")
        assert violations == []

    def test_forbidden_module_present_is_a_violation(self):
        names = CLEAN_REAL_PLATFORM_NAMES + ["port/platforms/e2etest.py"]
        violations = verifier.check_wheel_names(names, "example")
        assert any("port/platforms/e2etest.py" in v for v in violations)

    def test_forbidden_config_present_is_a_violation(self):
        names = CLEAN_REAL_PLATFORM_NAMES + ["port/configs/e2etest_config.json"]
        violations = verifier.check_wheel_names(names, "example")
        assert any("port/configs/e2etest_config.json" in v for v in violations)

    def test_both_forbidden_paths_present_are_both_reported(self):
        names = CLEAN_REAL_PLATFORM_NAMES + [
            "port/platforms/e2etest.py",
            "port/configs/e2etest_config.json",
        ]
        violations = verifier.check_wheel_names(names, "example")
        assert len(violations) == 2

    def test_forbidden_e2etest_multifile_module_present_is_a_violation(self):
        names = CLEAN_REAL_PLATFORM_NAMES + ["port/platforms/e2etest_multifile.py"]
        violations = verifier.check_wheel_names(names, "example")
        assert any("port/platforms/e2etest_multifile.py" in v for v in violations)

    def test_forbidden_e2etest_multifile_config_present_is_a_violation(self):
        names = CLEAN_REAL_PLATFORM_NAMES + ["port/configs/e2etest_multifile_config.json"]
        violations = verifier.check_wheel_names(names, "example")
        assert any("port/configs/e2etest_multifile_config.json" in v for v in violations)

    def test_all_four_forbidden_paths_present_are_all_reported(self):
        names = CLEAN_REAL_PLATFORM_NAMES + [
            "port/platforms/e2etest.py",
            "port/configs/e2etest_config.json",
            "port/platforms/e2etest_multifile.py",
            "port/configs/e2etest_multifile_config.json",
        ]
        violations = verifier.check_wheel_names(names, "example")
        assert len(violations) == 4

    def test_missing_selected_platform_module_is_a_violation(self):
        """A wheel that merely lacks e2etest is not proof the verifier works —
        it must also positively require the selected platform's own files."""
        names = ["port/configs/example_config.json"]  # module missing
        violations = verifier.check_wheel_names(names, "example")
        assert any("port/platforms/example.py" in v for v in violations)

    def test_missing_selected_platform_config_is_a_violation(self):
        names = ["port/platforms/example.py"]  # config missing
        violations = verifier.check_wheel_names(names, "example")
        assert any("port/configs/example_config.json" in v for v in violations)

    def test_empty_wheel_fails_both_positive_checks(self):
        violations = verifier.check_wheel_names([], "example")
        assert len(violations) == 2

    def test_checks_the_requested_platform_not_a_hardcoded_one(self):
        violations = verifier.check_wheel_names(CLEAN_REAL_PLATFORM_NAMES, "tiktok")
        assert violations == []
        violations = verifier.check_wheel_names(CLEAN_REAL_PLATFORM_NAMES, "facebook")
        assert len(violations) == 2

    # --- drift: anything referencing e2etest, not just the two known paths ---

    def test_renamed_config_drift_is_caught(self):
        """A config renamed away from the exact forbidden name (but still
        referencing e2etest) must still be caught — the exact-path list
        alone would miss it."""
        names = CLEAN_REAL_PLATFORM_NAMES + ["port/configs/e2etest_config_v2.json"]
        violations = verifier.check_wheel_names(names, "example")
        assert any("e2etest_config_v2.json" in v for v in violations)

    def test_helper_module_drift_is_caught(self):
        names = CLEAN_REAL_PLATFORM_NAMES + ["port/platforms/e2etest_helpers.py"]
        violations = verifier.check_wheel_names(names, "example")
        assert any("e2etest_helpers.py" in v for v in violations)

    def test_stray_backup_file_drift_is_caught(self):
        names = CLEAN_REAL_PLATFORM_NAMES + ["port/platforms/e2etest.py.bak"]
        violations = verifier.check_wheel_names(names, "example")
        assert any("e2etest.py.bak" in v for v in violations)

    def test_drift_match_is_case_insensitive(self):
        names = CLEAN_REAL_PLATFORM_NAMES + ["port/platforms/E2ETest_extra.py"]
        violations = verifier.check_wheel_names(names, "example")
        assert any("E2ETest_extra.py" in v for v in violations)

    def test_e2etest_multifile_drift_is_caught(self):
        """The same drift backstop applies to e2etest_multifile, not just e2etest."""
        names = CLEAN_REAL_PLATFORM_NAMES + ["port/configs/e2etest_multifile_config_v2.json"]
        violations = verifier.check_wheel_names(names, "example")
        assert any("e2etest_multifile_config_v2.json" in v for v in violations)

    def test_exact_known_path_is_not_double_reported_as_drift(self):
        """The two named paths get one violation each (from the exact
        check), not a second one from the substring/drift check."""
        names = CLEAN_REAL_PLATFORM_NAMES + ["port/platforms/e2etest.py"]
        violations = verifier.check_wheel_names(names, "example")
        matching = [v for v in violations if "e2etest.py" in v]
        assert len(matching) == 1


class TestCheckArchiveForDrift:
    """The absence-only check used for non-wheel archives (sdist tarballs),
    whose member names carry a version prefix the wheel doesn't."""

    def test_clean_archive_passes(self):
        names = [f"port-0.0.0/{n}" for n in CLEAN_REAL_PLATFORM_NAMES]
        assert verifier.check_archive_for_drift(names) == []

    def test_forbidden_module_under_sdist_prefix_is_caught(self):
        names = [f"port-0.0.0/{n}" for n in CLEAN_REAL_PLATFORM_NAMES]
        names.append("port-0.0.0/port/platforms/e2etest.py")
        violations = verifier.check_archive_for_drift(names)
        assert any("e2etest.py" in v for v in violations)

    def test_forbidden_config_under_sdist_prefix_is_caught(self):
        names = [f"port-0.0.0/{n}" for n in CLEAN_REAL_PLATFORM_NAMES]
        names.append("port-0.0.0/port/configs/e2etest_config.json")
        violations = verifier.check_archive_for_drift(names)
        assert any("e2etest_config.json" in v for v in violations)

    def test_drift_under_sdist_prefix_is_caught(self):
        names = [f"port-0.0.0/{n}" for n in CLEAN_REAL_PLATFORM_NAMES]
        names.append("port-0.0.0/port/platforms/e2etest_helpers.py")
        violations = verifier.check_archive_for_drift(names)
        assert any("e2etest_helpers.py" in v for v in violations)

    def test_ancient_sdist_with_no_platforms_or_configs_passes(self):
        """Matches the shape of the currently-tracked, stale
        packages/data-collector/public/port-0.0.0.tar.gz: no platforms/ or
        configs/ directory at all."""
        names = [
            "port-0.0.0/port/__init__.py",
            "port-0.0.0/port/main.py",
            "port-0.0.0/port/script.py",
            "port-0.0.0/pyproject.toml",
            "port-0.0.0/PKG-INFO",
        ]
        assert verifier.check_archive_for_drift(names) == []


class TestSyntheticWheelFixtures:
    """End-to-end against real (small, synthetic) wheel/zip files on disk,
    per the three fixtures called for in the design notes: clean, module
    present, config present."""

    def test_clean_synthetic_wheel(self, tmp_path):
        wheel = _make_wheel(tmp_path / "port-0.0.0-py3-none-any.whl", CLEAN_REAL_PLATFORM_NAMES)
        with zipfile.ZipFile(wheel) as z:
            violations = verifier.check_wheel_names(z.namelist(), "example")
        assert violations == []

    def test_synthetic_wheel_containing_forbidden_module(self, tmp_path):
        wheel = _make_wheel(
            tmp_path / "port-0.0.0-py3-none-any.whl",
            CLEAN_REAL_PLATFORM_NAMES + ["port/platforms/e2etest.py"],
        )
        with zipfile.ZipFile(wheel) as z:
            violations = verifier.check_wheel_names(z.namelist(), "example")
        assert any("e2etest.py" in v for v in violations)

    def test_synthetic_wheel_containing_forbidden_config(self, tmp_path):
        wheel = _make_wheel(
            tmp_path / "port-0.0.0-py3-none-any.whl",
            CLEAN_REAL_PLATFORM_NAMES + ["port/configs/e2etest_config.json"],
        )
        with zipfile.ZipFile(wheel) as z:
            violations = verifier.check_wheel_names(z.namelist(), "example")
        assert any("e2etest_config.json" in v for v in violations)

    def test_synthetic_wheel_has_selected_platforms_files(self, tmp_path):
        """Positive assertion: the selected platform's module+config are
        actually present, not merely that e2etest is absent."""
        wheel = _make_wheel(tmp_path / "port-0.0.0-py3-none-any.whl", CLEAN_REAL_PLATFORM_NAMES)
        with zipfile.ZipFile(wheel) as z:
            names = z.namelist()
        assert "port/platforms/example.py" in names
        assert "port/configs/example_config.json" in names
        assert verifier.check_wheel_names(names, "example") == []


class TestFindWheel:
    def test_no_wheel_found_raises(self, tmp_path):
        with pytest.raises(SystemExit, match="no wheel"):
            verifier._find_wheel(tmp_path)

    def test_multiple_wheels_raises(self, tmp_path):
        _make_wheel(tmp_path / "a-0.0.0-py3-none-any.whl", CLEAN_REAL_PLATFORM_NAMES)
        _make_wheel(tmp_path / "b-0.0.0-py3-none-any.whl", CLEAN_REAL_PLATFORM_NAMES)
        with pytest.raises(SystemExit, match="exactly one wheel"):
            verifier._find_wheel(tmp_path)

    def test_single_wheel_found(self, tmp_path):
        wheel = _make_wheel(tmp_path / "port-0.0.0-py3-none-any.whl", CLEAN_REAL_PLATFORM_NAMES)
        assert verifier._find_wheel(tmp_path) == wheel


class TestFindSdistTarballs:
    def test_no_tarballs_is_fine(self, tmp_path):
        assert verifier._find_sdist_tarballs(tmp_path) == []

    def test_finds_port_tarballs_only(self, tmp_path):
        _make_sdist(tmp_path / "port-0.0.0.tar.gz", CLEAN_REAL_PLATFORM_NAMES)
        (tmp_path / "something-else-0.0.0.tar.gz").write_bytes(b"not a port archive")
        found = verifier._find_sdist_tarballs(tmp_path)
        assert [p.name for p in found] == ["port-0.0.0.tar.gz"]


class TestCliExitCodes:
    """Exercises main() through the CLI like release.sh will invoke it."""

    def test_cli_passes_on_clean_wheel(self, tmp_path):
        _make_wheel(tmp_path / "port-0.0.0-py3-none-any.whl", CLEAN_REAL_PLATFORM_NAMES)
        result = subprocess.run(
            [sys.executable, str(VERIFIER), "example", "--dist-dir", str(tmp_path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        assert "OK" in result.stdout

    def test_cli_fails_on_wheel_with_forbidden_module(self, tmp_path):
        _make_wheel(
            tmp_path / "port-0.0.0-py3-none-any.whl",
            CLEAN_REAL_PLATFORM_NAMES + ["port/platforms/e2etest.py"],
        )
        result = subprocess.run(
            [sys.executable, str(VERIFIER), "example", "--dist-dir", str(tmp_path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1
        assert "e2etest.py" in result.stderr

    def test_cli_fails_on_wheel_with_forbidden_config(self, tmp_path):
        _make_wheel(
            tmp_path / "port-0.0.0-py3-none-any.whl",
            CLEAN_REAL_PLATFORM_NAMES + ["port/configs/e2etest_config.json"],
        )
        result = subprocess.run(
            [sys.executable, str(VERIFIER), "example", "--dist-dir", str(tmp_path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1
        assert "e2etest_config.json" in result.stderr

    def test_cli_fails_on_wheel_with_forbidden_e2etest_multifile_module(self, tmp_path):
        _make_wheel(
            tmp_path / "port-0.0.0-py3-none-any.whl",
            CLEAN_REAL_PLATFORM_NAMES + ["port/platforms/e2etest_multifile.py"],
        )
        result = subprocess.run(
            [sys.executable, str(VERIFIER), "example", "--dist-dir", str(tmp_path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1
        assert "e2etest_multifile.py" in result.stderr

    def test_cli_fails_when_no_wheel_present(self, tmp_path):
        result = subprocess.run(
            [sys.executable, str(VERIFIER), "example", "--dist-dir", str(tmp_path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0

    def test_cli_checks_sdist_tarball_alongside_the_wheel(self, tmp_path):
        """The verifier's blind spot this closes: a second port-* archive
        (the sdist) ships into dist/ alongside the wheel and must be
        checked too, even though it isn't the wheel the required-presence
        check targets."""
        _make_wheel(tmp_path / "port-0.0.0-py3-none-any.whl", CLEAN_REAL_PLATFORM_NAMES)
        _make_sdist(tmp_path / "port-0.0.0.tar.gz", CLEAN_REAL_PLATFORM_NAMES)
        result = subprocess.run(
            [sys.executable, str(VERIFIER), "example", "--dist-dir", str(tmp_path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr

    def test_cli_fails_when_sdist_tarball_contains_e2etest(self, tmp_path):
        _make_wheel(tmp_path / "port-0.0.0-py3-none-any.whl", CLEAN_REAL_PLATFORM_NAMES)
        _make_sdist(
            tmp_path / "port-0.0.0.tar.gz",
            CLEAN_REAL_PLATFORM_NAMES + ["port/platforms/e2etest.py"],
        )
        result = subprocess.run(
            [sys.executable, str(VERIFIER), "example", "--dist-dir", str(tmp_path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1
        assert "port-0.0.0.tar.gz" in result.stderr
        assert "e2etest.py" in result.stderr

    def test_cli_passes_against_the_ancient_stale_tracked_sdist_shape(self, tmp_path):
        """Regression guard for the specific tarball this repo tracks today
        (packages/data-collector/public/port-0.0.0.tar.gz): no platforms/ or
        configs/ at all, so it must not be flagged."""
        _make_wheel(tmp_path / "port-0.0.0-py3-none-any.whl", CLEAN_REAL_PLATFORM_NAMES)
        _make_sdist(
            tmp_path / "port-0.0.0.tar.gz",
            ["port/__init__.py", "port/main.py", "port/script.py", "pyproject.toml", "PKG-INFO"],
        )
        result = subprocess.run(
            [sys.executable, str(VERIFIER), "example", "--dist-dir", str(tmp_path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
