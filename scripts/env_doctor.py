#!/usr/bin/env python3
"""Project environment doctor for ares-osint-telemetry."""

from __future__ import annotations

import argparse
import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MANDATORY_PACKAGES = [
    ("requests", "requests"),
    ("yaml", "PyYAML"),
    ("bs4", "beautifulsoup4"),
]
OPTIONAL_PACKAGES = [
    ("playwright", "playwright"),
]


def add_result(results: List[Tuple[str, str, str]], status: str, name: str, detail: str) -> None:
    results.append((status, name, detail))


def check_python(results: List[Tuple[str, str, str]]) -> None:
    major, minor = sys.version_info[:2]
    current = f"{major}.{minor}"
    if (major, minor) >= (3, 10):
        add_result(results, "PASS", "python_version", f"{current} (recommended: >=3.10)")
        return
    if (major, minor) >= (3, 9):
        add_result(results, "WARN", "python_version", f"{current} (supported but project recommends >=3.10)")
        return
    add_result(results, "FAIL", "python_version", f"{current} (minimum: 3.9, recommended: >=3.10)")


def check_packages(results: List[Tuple[str, str, str]]) -> None:
    for module_name, package_name in MANDATORY_PACKAGES:
        if importlib.util.find_spec(module_name):
            add_result(results, "PASS", f"dependency:{package_name}", "installed")
        else:
            add_result(
                results,
                "FAIL",
                f"dependency:{package_name}",
                f"missing, install with `pip install {package_name}`",
            )

    for module_name, package_name in OPTIONAL_PACKAGES:
        if importlib.util.find_spec(module_name):
            add_result(results, "PASS", f"optional:{package_name}", "installed")
        else:
            add_result(
                results,
                "WARN",
                f"optional:{package_name}",
                "missing, only needed for future browser-based crawling",
            )


def check_vault_env(results: List[Tuple[str, str, str]]) -> None:
    vault_path = os.getenv("ARES_VAULT_PATH")
    if not vault_path:
        add_result(
            results,
            "WARN",
            "env:ARES_VAULT_PATH",
            "not set (postmatch markdown will fallback to `draft_reports/`)",
        )
        return

    path = Path(vault_path).expanduser()
    if path.exists() and path.is_dir():
        add_result(results, "PASS", "env:ARES_VAULT_PATH", f"set to {path}")
    else:
        add_result(results, "FAIL", "env:ARES_VAULT_PATH", f"path does not exist or is not a directory: {path}")


def check_write_paths(results: List[Tuple[str, str, str]]) -> None:
    for rel in ("raw_reports", "draft_reports"):
        target = PROJECT_ROOT / rel
        try:
            target.mkdir(parents=True, exist_ok=True)
            probe = target / ".write_probe"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
            add_result(results, "PASS", f"path:{rel}", "writable")
        except Exception as exc:
            add_result(results, "FAIL", f"path:{rel}", f"not writable: {exc}")


def check_entrypoints(results: List[Tuple[str, str, str]]) -> None:
    scripts = [
        PROJECT_ROOT / "src" / "data" / "osint_crawler.py",
        PROJECT_ROOT / "src" / "data" / "osint_postmatch.py",
    ]
    for script_path in scripts:
        if not script_path.exists():
            add_result(results, "FAIL", f"entry:{script_path.name}", "file not found")
            continue
        cmd = [sys.executable, str(script_path), "--help"]
        run = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        if run.returncode == 0:
            add_result(results, "PASS", f"entry:{script_path.name}", "ok (`--help` exits 0)")
        else:
            err = (run.stderr or run.stdout or "").strip().splitlines()
            tail = err[-1] if err else "unknown error"
            add_result(results, "FAIL", f"entry:{script_path.name}", tail)


def print_report(results: List[Tuple[str, str, str]]) -> None:
    print("Ares OSINT Telemetry - Environment Doctor")
    print(f"Project root: {PROJECT_ROOT}")
    print("-" * 72)
    for status, name, detail in results:
        print(f"[{status:<4}] {name:<30} {detail}")
    print("-" * 72)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run local environment checks for this project.")
    parser.add_argument(
        "--strict-warnings",
        action="store_true",
        help="Treat WARN as failure (exit code 1).",
    )
    args = parser.parse_args()

    results: List[Tuple[str, str, str]] = []
    check_python(results)
    check_packages(results)
    check_vault_env(results)
    check_write_paths(results)
    check_entrypoints(results)
    print_report(results)

    fail_count = sum(1 for status, _, _ in results if status == "FAIL")
    warn_count = sum(1 for status, _, _ in results if status == "WARN")
    print(f"Summary: {fail_count} FAIL, {warn_count} WARN")

    if fail_count > 0:
        return 1
    if args.strict_warnings and warn_count > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
