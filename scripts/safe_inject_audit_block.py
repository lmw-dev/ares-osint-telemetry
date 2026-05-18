#!/usr/bin/env python3
"""Safe block injector for prematch audit markdown files.

This script updates only a bounded marker region:
  <!-- ARES_BLOCK:{key}:START -->
  ...
  <!-- ARES_BLOCK:{key}:END -->

If markers do not exist, it appends them to the end of file.
No broad regex replacement over arbitrary headings.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path


def _find_existing_h2_section(text: str, heading_line: str) -> tuple[int, int] | None:
    """Find an existing H2 section by exact heading line."""
    if not heading_line.startswith("## "):
        return None
    start_match = re.search(rf"(?m)^{re.escape(heading_line)}\s*$", text)
    if not start_match:
        return None
    start = start_match.start()
    next_h2 = re.search(r"(?m)^##\s+", text[start_match.end() :])
    if next_h2:
        end = start_match.end() + next_h2.start()
    else:
        end = len(text)
    return (start, end)


def _section_bounds_for_heading(text: str, heading_line: str) -> list[tuple[int, int]]:
    bounds: list[tuple[int, int]] = []
    for m in re.finditer(rf"(?m)^{re.escape(heading_line)}\s*$", text):
        start = m.start()
        next_h2 = re.search(r"(?m)^##\s+", text[m.end() :])
        end = m.end() + (next_h2.start() if next_h2 else len(text[m.end() :]))
        bounds.append((start, end))
    return bounds


def _dedupe_heading_outside_marker(text: str, *, heading_line: str, marker_start: str, marker_end: str) -> str:
    if not heading_line:
        return text
    s_idx = text.find(marker_start)
    e_idx = text.find(marker_end)
    if s_idx == -1 or e_idx == -1:
        return text
    e_idx2 = e_idx + len(marker_end)

    bounds = _section_bounds_for_heading(text, heading_line)
    if len(bounds) <= 1:
        return text

    keep: tuple[int, int] | None = None
    for b in bounds:
        # keep the heading section that overlaps marker range
        if b[0] < e_idx2 and b[1] > s_idx:
            keep = b
            break
    if keep is None:
        return text

    out = text
    for b in sorted(bounds, key=lambda x: x[0], reverse=True):
        if b == keep:
            continue
        out = out[: b[0]] + out[b[1] :].lstrip("\n")
    return out


def inject_block(path: Path, key: str, body: str, *, dry_run: bool = False, backup: bool = False) -> bool:
    start = f"<!-- ARES_BLOCK:{key}:START -->"
    end = f"<!-- ARES_BLOCK:{key}:END -->"
    block = f"{start}\n{body.rstrip()}\n{end}\n"

    text = path.read_text(encoding="utf-8")
    s_matches = list(re.finditer(re.escape(start), text))
    e_matches = list(re.finditer(re.escape(end), text))

    if len(s_matches) == 1 and len(e_matches) == 1:
        s_idx = s_matches[0].start()
        e_idx = e_matches[0].start()
        if e_idx < s_idx:
            raise ValueError(f"Invalid marker order in {path}")
        e_idx2 = e_idx + len(end)
        new_text = text[:s_idx] + block + text[e_idx2:]
    elif len(s_matches) == 0 and len(e_matches) == 0:
        heading_line = next((ln.strip() for ln in body.splitlines() if ln.strip().startswith("## ")), "")
        section = _find_existing_h2_section(text, heading_line) if heading_line else None
        if section is not None:
            sec_start, sec_end = section
            head = text[:sec_start].rstrip("\n")
            tail = text[sec_end:].lstrip("\n")
            new_text = f"{head}\n\n{block}\n{tail}".rstrip("\n") + "\n"
        else:
            sep = "\n" if text.endswith("\n") else "\n\n"
            new_text = text + sep + block
    else:
        raise ValueError(f"Broken or duplicate markers in {path} for key={key}")

    heading_line = next((ln.strip() for ln in body.splitlines() if ln.strip().startswith("## ")), "")
    new_text = _dedupe_heading_outside_marker(
        new_text,
        heading_line=heading_line,
        marker_start=start,
        marker_end=end,
    )

    if new_text != text:
        if dry_run:
            return True
        if backup:
            bak = path.with_suffix(path.suffix + ".bak")
            bak.write_text(text, encoding="utf-8")
        path.write_text(new_text, encoding="utf-8")
        return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Safely inject bounded markdown block")
    parser.add_argument("--file", required=True, help="Markdown file path")
    parser.add_argument("--key", required=True, help="Block key, e.g. EPL_ODDS_INTEL")
    parser.add_argument("--body-file", required=True, help="Path to markdown fragment body")
    parser.add_argument("--dry-run", action="store_true", help="Preview only; do not write")
    parser.add_argument("--backup", action="store_true", help="Write .bak file before update")
    args = parser.parse_args()

    target = Path(args.file).expanduser().resolve()
    body_path = Path(args.body_file).expanduser().resolve()
    if not target.exists():
        raise FileNotFoundError(target)
    if not body_path.exists():
        raise FileNotFoundError(body_path)

    body = body_path.read_text(encoding="utf-8")
    changed = inject_block(target, args.key, body, dry_run=args.dry_run, backup=args.backup)
    print(f"changed={str(changed).lower()} file={target} key={args.key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
