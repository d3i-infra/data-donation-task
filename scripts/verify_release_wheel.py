#!/usr/bin/env python3
"""Postcondition check for the release artifacts embedded in a build's dist dir.

`scripts/build_release_wheel.sh` builds a wheel from a staged copy of
`packages/python` with the e2etest fault-injection module, the
e2etest_multifile test platform, and their configs removed. This script
verifies the *artifacts that will actually ship* — every `port-*` archive
Vite copies into `packages/data-collector/dist` (the wheel, and any sdist
tarball), not just the staged build that produced the wheel. It is
release.sh's last check before zipping; see ADR-0004 for the release-wheel
boundary this enforces.

Two archive types are in scope, because both can end up in `dist/` and both
get zipped by `release.sh`:

* The wheel (`port-*.whl`) — exactly one is required. It gets the full
  check: the selected platform's own module/config must be present (so the
  check can't pass vacuously against an empty or broken wheel) and no member
  path may reference e2etest.
* Any sdist tarball (`port-*.tar.gz`) — zero or more. A stale sdist
  (`port-0.0.0.tar.gz`) used to be tracked in the data collector's public/
  directory and shipped in every release zip alongside the wheel; it has
  been removed, but nothing structurally prevents another `port-*` archive
  from appearing — so every `port-*` archive found in the dist directory is
  checked. It predated the multi-platform config system and contained neither
  `platforms/` nor `configs/`, but had the same risk: an sdist regenerated
  with current content could carry e2etest past a verifier that only ever
  looked at the wheel. Sdist member names carry a `port-<version>/` prefix
  the wheel doesn't have, so only the absence check applies here — the
  presence/required check is wheel-specific by construction.

Usage
-----
    python3 scripts/verify_release_wheel.py <platform> [--dist-dir DIR]

Exits non-zero, with a message on stderr, if:
  * no wheel (or more than one) is found in DIR,
  * any `port-*` archive (wheel or tarball) in DIR contains a member path
    that references e2etest or e2etest_multifile (exact forbidden path, or
    merely containing the substring "e2etest"/"e2etest_multifile",
    case-insensitive — catching a renamed config, an `e2etest_helpers.py`,
    a stray `.bak`, or similar drift that an exact-path check alone would
    miss), or
  * the selected platform's own module or config is missing from the wheel.

The checking logic (`check_wheel_names`, `check_archive_for_drift`) takes a
plain list of archive member names so it can be unit-tested against small
synthetic wheels/tarballs without touching the filesystem beyond a tmp_path
fixture — see packages/python/tests/test_verify_release_wheel.py.
"""

from __future__ import annotations

import argparse
import glob
import sys
import tarfile
import zipfile
from pathlib import Path

FORBIDDEN_PATHS = (
    "port/platforms/e2etest.py",
    "port/configs/e2etest_config.json",
    "port/platforms/e2etest_multifile.py",
    "port/configs/e2etest_multifile_config.json",
)

# Catches drift an exact-path list can't: a renamed config, an
# `e2etest_helpers.py`, a `.bak` left behind, anything with "e2etest" or
# "e2etest_multifile" anywhere in its archive path. Exact FORBIDDEN_PATHS stays
# as the named, precise signal for the four files the build script is
# responsible for removing; this is the cheap, broad backstop underneath it.
FORBIDDEN_SUBSTRING = ("e2etest", "e2etest_multifile")


def required_paths(platform: str) -> tuple[str, str]:
    return (
        f"port/platforms/{platform}.py",
        f"port/configs/{platform}_config.json",
    )


def find_forbidden_substring_matches(names: list[str]) -> list[str]:
    """Archive member names that reference e2etest or e2etest_multifile anywhere, case-insensitively."""
    needles = [s.lower() for s in FORBIDDEN_SUBSTRING]
    return [n for n in names if any(needle in n.lower() for needle in needles)]


def check_archive_for_drift(names: list[str]) -> list[str]:
    """Absence-only check usable against any archive type (wheel or sdist).

    No required-presence check here: a sdist's member names carry a
    `port-<version>/` prefix the wheel doesn't, so "does this archive have
    <platform>'s files" isn't a well-defined question without per-archive-type
    prefix handling this script doesn't need — the wheel is the one artifact
    whose presence matters, and it's checked separately by check_wheel_names.
    """
    return [
        f"member path references a test-only platform (forbidden): {match}"
        for match in find_forbidden_substring_matches(names)
    ]


def check_wheel_names(names: list[str], platform: str) -> list[str]:
    """Return a list of human-readable violations; empty means the wheel is clean.

    Checks two independent things: no member path may reference e2etest or
    e2etest_multifile, and the selected platform's own module + config must be
    present (a verifier that only ever checked for absence could pass
    against an empty or broken wheel).
    """
    names_set = set(names)
    violations: list[str] = []

    for forbidden in FORBIDDEN_PATHS:
        if forbidden in names_set:
            violations.append(f"forbidden test-only platform path present in wheel: {forbidden}")

    # Drift beyond the four named paths (a rename, a helper module, a stray
    # .bak, ...). Matches already reported above as an exact FORBIDDEN_PATHS
    # hit are skipped here so a plain removal failure isn't reported twice.
    for match in find_forbidden_substring_matches(names):
        if match not in FORBIDDEN_PATHS:
            violations.append(f"member path references a test-only platform (forbidden, not one of the known paths): {match}")

    for required in required_paths(platform):
        if required not in names_set:
            violations.append(f"required path for platform '{platform}' missing from wheel: {required}")

    return violations


def _find_wheel(dist_dir: Path) -> Path:
    matches = sorted(glob.glob(str(dist_dir / "*.whl")))
    if not matches:
        raise SystemExit(f"ERROR: no wheel (*.whl) found in {dist_dir}")
    if len(matches) > 1:
        joined = ", ".join(matches)
        raise SystemExit(
            f"ERROR: expected exactly one wheel in {dist_dir}, found {len(matches)}: {joined}"
        )
    return Path(matches[0])


def _find_sdist_tarballs(dist_dir: Path) -> list[Path]:
    """Every `port-*.tar.gz` in dist_dir — zero or more, all checked."""
    return [Path(p) for p in sorted(glob.glob(str(dist_dir / "port-*.tar.gz")))]


def _archive_member_names(path: Path) -> list[str]:
    name = path.name
    if name.endswith(".whl"):
        with zipfile.ZipFile(path) as archive:
            return archive.namelist()
    if name.endswith(".tar.gz") or name.endswith(".tgz"):
        with tarfile.open(path, mode="r:*") as archive:
            return archive.getnames()
    raise ValueError(f"unrecognized release archive type: {path}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("platform", help="the platform this build was built for (VITE_PLATFORM)")
    parser.add_argument(
        "--dist-dir",
        default="packages/data-collector/dist",
        help="directory containing the built release artifacts (default: packages/data-collector/dist)",
    )
    args = parser.parse_args(argv)

    dist_dir = Path(args.dist_dir)
    wheel_path = _find_wheel(dist_dir)
    wheel_names = _archive_member_names(wheel_path)

    violations: list[str] = [f"{wheel_path.name}: {v}" for v in check_wheel_names(wheel_names, args.platform)]

    checked_archives = [wheel_path.name]
    for tarball_path in _find_sdist_tarballs(dist_dir):
        checked_archives.append(tarball_path.name)
        tarball_names = _archive_member_names(tarball_path)
        violations += [f"{tarball_path.name}: {v}" for v in check_archive_for_drift(tarball_names)]

    if violations:
        print(f"ERROR: release artifact verification failed in {dist_dir}:", file=sys.stderr)
        for v in violations:
            print(f"  - {v}", file=sys.stderr)
        return 1

    archives_desc = ", ".join(checked_archives)
    print(f"OK: {archives_desc} contains {args.platform}'s files and no e2etest/e2etest_multifile references.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
