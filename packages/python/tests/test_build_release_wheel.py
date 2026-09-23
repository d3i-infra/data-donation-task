"""Tests for the release wheel build script's poetry-version guard
(scripts/build_release_wheel.sh).

The script uses poetry-2-only flags (`--clean`, the `-C <dir>` project-
directory form), so it fails fast with a clear message on an older poetry
rather than let a stale-provisioned VM hit an opaque unknown-flag error
partway through a researcher's release build. These tests run the real
script under `subprocess`, with a fake `poetry` executable prepended to
PATH, so no real wheel build (and no real old poetry) is needed.
"""

import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
BUILD_SCRIPT = REPO_ROOT / "scripts" / "build_release_wheel.sh"
BASH = shutil.which("bash") or "/usr/bin/bash"


def _fake_poetry(bin_dir: Path, version: str, build_behavior: str = "fail") -> None:
    """A stand-in `poetry` executable on a tmp PATH entry.

    `--version` reports `version`; any other invocation (i.e. the real
    build command, once the version guard has been passed) exits non-zero
    with a recognizable marker — enough to prove the script proceeded past
    the guard without needing a real poetry-core build.
    """
    poetry_path = bin_dir / "poetry"
    poetry_path.write_text(
        "#!/usr/bin/env bash\n"
        'if [ "$1" = "--version" ]; then\n'
        f'    echo "Poetry (version {version})"\n'
        "    exit 0\n"
        "fi\n"
        'echo "FAKE_POETRY_BUILD_INVOKED: $*" >&2\n'
        "exit 1\n"
    )
    poetry_path.chmod(poetry_path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)


def _run_with_fake_poetry(bin_dir: Path) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env["PATH"] = f"{bin_dir}{os.pathsep}{env.get('PATH', '')}"
    return subprocess.run(
        [BASH, str(BUILD_SCRIPT)],
        capture_output=True,
        text=True,
        env=env,
        cwd=REPO_ROOT,
    )


class TestPoetryVersionGuard:
    def test_rejects_poetry_1_x_before_doing_any_work(self, tmp_path):
        _fake_poetry(tmp_path, "1.8.3")
        result = _run_with_fake_poetry(tmp_path)
        assert result.returncode != 0
        assert "poetry >= 2.0" in result.stderr
        assert "1.8.3" in result.stderr
        # Never reached the real build invocation.
        assert "FAKE_POETRY_BUILD_INVOKED" not in result.stderr

    def test_rejects_poetry_0_x(self, tmp_path):
        _fake_poetry(tmp_path, "0.12.17")
        result = _run_with_fake_poetry(tmp_path)
        assert result.returncode != 0
        assert "poetry >= 2.0" in result.stderr

    def test_accepts_poetry_2_x_and_proceeds_past_the_guard(self, tmp_path):
        """A 2.x poetry must pass the guard and reach the real build
        invocation (which then fails for an unrelated reason — the fake
        poetry doesn't implement `build` — proving the guard let it
        through rather than proving the whole build succeeds)."""
        _fake_poetry(tmp_path, "2.4.1")
        result = _run_with_fake_poetry(tmp_path)
        assert "poetry >= 2.0" not in result.stderr
        assert "FAKE_POETRY_BUILD_INVOKED" in result.stderr

    def test_accepts_a_future_major_version(self, tmp_path):
        _fake_poetry(tmp_path, "3.0.0")
        result = _run_with_fake_poetry(tmp_path)
        assert "poetry >= 2.0" not in result.stderr
        assert "FAKE_POETRY_BUILD_INVOKED" in result.stderr

    def test_missing_poetry_on_path_is_a_clear_guard_failure(self, tmp_path):
        """No `poetry` executable at all on PATH (beyond tmp_path, which is
        empty) must fail with the same clear message, not a bash
        "command not found" with no explanation."""
        empty_bin = tmp_path / "empty"
        empty_bin.mkdir()
        env = dict(os.environ)
        # A PATH with no poetry on it at all (not even the real one).
        env["PATH"] = str(empty_bin)
        result = subprocess.run(
            [BASH, str(BUILD_SCRIPT)],
            capture_output=True,
            text=True,
            env=env,
            cwd=REPO_ROOT,
        )
        assert result.returncode != 0
        assert "poetry >= 2.0" in result.stderr
