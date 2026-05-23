"""Validate package metadata and release artifacts before publishing."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import shutil
import subprocess
import sys

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 release helper path.
    import tomli as tomllib

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-clean", action="store_true")
    parser.add_argument("--skip-tests", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()

    version = _project_version()
    _check_init_version(version)
    _check_changelog(version)
    if not args.skip_clean:
        _check_clean_worktree()
    if not args.skip_tests:
        _run([sys.executable, "-m", "ruff", "check", "."])
        _run([sys.executable, "-m", "compileall", "-q", "src", "tests", "examples"])
        _run([sys.executable, "-m", "pytest", "-q"])
    if not args.skip_build:
        _build_and_check_dist()
    print(f"release checks passed for {version}")
    return 0


def _project_version() -> str:
    with (ROOT / "pyproject.toml").open("rb") as handle:
        data = tomllib.load(handle)
    version = data["project"]["version"]
    if not re.fullmatch(r"\d+\.\d+\.\d+(?:[a-zA-Z0-9.+-]+)?", version):
        raise SystemExit(f"Unexpected project version format: {version!r}")
    return version


def _check_init_version(version: str) -> None:
    content = (ROOT / "src" / "datastore_pandas" / "__init__.py").read_text()
    match = re.search(r'^__version__ = "([^"]+)"$', content, re.MULTILINE)
    if match is None:
        raise SystemExit("__version__ not found in src/datastore_pandas/__init__.py")
    if match.group(1) != version:
        raise SystemExit(
            f"Version mismatch: pyproject.toml={version}, __init__.py={match.group(1)}"
        )


def _check_changelog(version: str) -> None:
    content = (ROOT / "CHANGELOG.md").read_text()
    if f"## {version}" not in content:
        raise SystemExit(f"CHANGELOG.md does not contain a section for {version}")


def _check_clean_worktree() -> None:
    result = _run(["git", "status", "--porcelain"], capture=True)
    if result.stdout.strip():
        raise SystemExit("Working tree is not clean; commit or stash changes before release.")


def _build_and_check_dist() -> None:
    dist = ROOT / "dist"
    if dist.exists():
        shutil.rmtree(dist)
    _run([sys.executable, "-m", "build"])
    artifacts = sorted(str(path) for path in dist.glob("*"))
    if not artifacts:
        raise SystemExit("No distribution artifacts were built.")
    _run([sys.executable, "-m", "twine", "check", "--strict", *artifacts])


def _run(command: list[str], *, capture: bool = False) -> subprocess.CompletedProcess[str]:
    kwargs = {
        "cwd": ROOT,
        "text": True,
        "check": True,
    }
    if capture:
        kwargs["capture_output"] = True
    print("+ " + " ".join(command))
    try:
        return subprocess.run(command, **kwargs)
    except FileNotFoundError as exc:
        raise SystemExit(f"Required command not found: {command[0]}") from exc
    except subprocess.CalledProcessError as exc:
        raise SystemExit(exc.returncode) from exc


if __name__ == "__main__":
    raise SystemExit(main())
