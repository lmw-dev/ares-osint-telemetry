#!/usr/bin/env python3
"""
validate_claims.py — Tactical Claim Validation Helper Script
Skill: tactical-claim-validation v1.0
Issue: LMW-127

This script handles:
  1. Reading and validating claims.md
  2. Parsing all claim blocks
  3. Writing structured validation output to AresVault validation/
  4. Generating blocked reports on failure

The LLM (Ares/Codex) performs the actual validation decisions.
This script handles I/O, claim parsing, and output formatting.

Usage:
    # Print claims for LLM validation
    ./venv/bin/python src/skills/tactical-claim-validation/scripts/validate_claims.py \\
        --claims "/path/to/claims.md" \\
        --team "Arsenal" \\
        --print-claims

    # Write LLM-validated results
    ./venv/bin/python src/skills/tactical-claim-validation/scripts/validate_claims.py \\
        --claims "/path/to/claims.md" \\
        --team "Arsenal" \\
        --validation-json "/path/to/validation_results.json" \\
        --write-output

Guardrails:
    - Does NOT patch Team Archive
    - Does NOT create memory cards
    - Does NOT overwrite source claims file
    - Does NOT write to notebooklm_outputs/
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("validate_claims")

# ── Vault paths ──────────────────────────────────────────────────────────────

VAULT_BASE = Path(
    os.environ.get("ARES_VAULT_PATH", "/Users/liumingwei/vaults/AresVault")
)
VALIDATION_DIR = (
    VAULT_BASE / "04_RAG_Raw_Data" / "youtube_tactical_sources" / "validation"
)

# ── Valid values ──────────────────────────────────────────────────────────────

VALID_STATUSES = {"validated", "rejected", "needs_review"}
VALID_REASON_CODES = {
    "validated_source_supported",
    "rejected_fact_conflict",
    "rejected_source_mismatch",
    "needs_review_insufficient_evidence",
    "needs_review_language_ambiguity",
    "needs_review_tactical_interpretation",
    "needs_review_source_quality",
}


# ── Parsers ───────────────────────────────────────────────────────────────────


def extract_frontmatter(content: str) -> tuple[dict[str, Any], str]:
    """Parse YAML frontmatter from markdown content."""
    if not content.startswith("---"):
        return {}, content

    end_idx = content.find("\n---", 3)
    if end_idx == -1:
        return {}, content

    fm_text = content[3:end_idx].strip()
    body = content[end_idx + 4:].strip()

    fm: dict[str, Any] = {}
    current_key = None
    current_list: list[str] | None = None

    for line in fm_text.split("\n"):
        if line.startswith("  - ") and current_list is not None:
            current_list.append(line[4:].strip())
        elif ":" in line and not line.startswith(" "):
            if current_key and current_list is not None:
                fm[current_key] = current_list
                current_list = None
            key, _, val = line.partition(":")
            key = key.strip()
            val = val.strip()
            if val == "":
                current_key = key
                current_list = []
            else:
                fm[key] = val
                current_key = None

    if current_key and current_list is not None:
        fm[current_key] = current_list

    return fm, body


def parse_claims(body: str) -> list[dict[str, Any]]:
    """
    Parse claim blocks from claims.md body.

    Each claim block starts with '## Claim NNN' and contains:
    - ID, Type, Confidence, Team
    - Claim text
    - Evidence quote
    - Location
    - Metadata
    """
    claims = []

    # Split on claim headers
    claim_blocks = re.split(r"\n## Claim \d+\n", body)

    for block in claim_blocks[1:]:  # Skip content before first claim
        claim: dict[str, Any] = {}

        # Extract claim_id
        id_match = re.search(r"\*\*ID\*\*:\s*`([^`]+)`", block)
        if id_match:
            claim["claim_id"] = id_match.group(1)

        # Extract claim_type
        type_match = re.search(r"\*\*Type\*\*:\s*`([^`]+)`", block)
        if type_match:
            claim["claim_type"] = type_match.group(1)

        # Extract confidence
        conf_match = re.search(r"\*\*Confidence\*\*:\s*`([^`]+)`", block)
        if conf_match:
            claim["confidence"] = conf_match.group(1)

        # Extract claim text (after **Claim**: line)
        claim_text_match = re.search(r"\*\*Claim\*\*:\s*(.+?)(?=\n\n|\*\*Evidence\*\*)", block, re.DOTALL)
        if claim_text_match:
            claim["claim_text"] = claim_text_match.group(1).strip()

        # Extract evidence quote
        evidence_match = re.search(r'>\s*"(.+?)"', block, re.DOTALL)
        if evidence_match:
            claim["evidence_quote"] = evidence_match.group(1).strip()

        # Extract location
        location_match = re.search(r"\*\*Location\*\*:\s*(.+?)(?=\n)", block)
        if location_match:
            claim["evidence_location"] = location_match.group(1).strip()

        # Extract confidence_note
        note_match = re.search(r"`confidence_note`:\s*(.+?)(?=\n|$)", block)
        if note_match:
            claim["confidence_note"] = note_match.group(1).strip()

        if claim.get("claim_id"):
            claims.append(claim)

    return claims


def validate_claims_file(fm: dict[str, Any], claims: list) -> tuple[bool, str]:
    """Validate claims file structure."""
    if not fm:
        return False, "invalid_frontmatter"

    source_authority = fm.get("source_authority", "")
    if source_authority != "extracted_claims":
        return False, "invalid_frontmatter"

    downstream_allowed = fm.get("downstream_allowed", [])
    if isinstance(downstream_allowed, str):
        downstream_allowed = [downstream_allowed]
    if "validation" not in downstream_allowed:
        return False, "validation_not_allowed"

    if not claims:
        return False, "no_claims_found"

    return True, ""


# ── Output builders ───────────────────────────────────────────────────────────


def build_validation_markdown(
    fm: dict[str, Any],
    validations: list[dict[str, Any]],
    validated_at: str,
    source_claims_name: str,
) -> str:
    """Build the validation output markdown."""
    video_id = fm.get("video_id", "UNKNOWN")
    source_url = fm.get("source_url", "")
    target_team = fm.get("target_team", "")
    target_league = fm.get("target_league", "")
    source_channel = fm.get("source_channel", "")
    language = fm.get("language", "")

    # Count by status and reason code
    status_counts = {"validated": 0, "rejected": 0, "needs_review": 0}
    reason_counts: dict[str, int] = {}

    for v in validations:
        status = v.get("validation_status", "needs_review")
        status_counts[status] = status_counts.get(status, 0) + 1
        rc = v.get("validation_reason_code", "needs_review_insufficient_evidence")
        reason_counts[rc] = reason_counts.get(rc, 0) + 1

    reason_yaml = "\n".join(
        f"  {rc}: {cnt}" for rc, cnt in sorted(reason_counts.items())
    )

    frontmatter = f"""---
source_kind: tactical_claim_validation
source_claims: {source_claims_name}
video_id: {video_id}
source_url: {source_url}
target_team: {target_team}
target_league: {target_league}
source_channel: {source_channel}
language: {language}
validation_skill: tactical-claim-validation v1.0
validated_at: {validated_at}
total_claims: {len(validations)}
validation_summary:
  validated: {status_counts['validated']}
  rejected: {status_counts['rejected']}
  needs_review: {status_counts['needs_review']}
reason_code_summary:
{reason_yaml}
team_archive_patch_allowed: false
downstream_allowed:
  - team_archive_patch_proposal
downstream_forbidden:
  - direct_team_archive_patch
  - prematch_conclusion
---"""

    header = f"""
# Tactical Claim Validation — {target_team} — {source_channel} — {video_id}

## Source

| Field | Value |
|-------|-------|
| Claims File | {source_claims_name} |
| Video URL | {source_url} |
| Channel | {source_channel} |
| Target Team | {target_team} |
| Target League | {target_league} |
| Language | {language} |
| Validated At | {validated_at} |

## ⚠️ Boundary Notice

> **This is a validation report. Validated claims are candidates for review, not approved for Team Archive patch.**
>
> - ✅ Allowed as input for: Team Archive patch proposal (YT-05)
> - ❌ Must NOT directly patch Team Archive
> - ❌ Must NOT be treated as verified tactical memory without YT-05 review

## Validation Summary

| Status | Count |
|--------|-------|
| validated | {status_counts['validated']} |
| rejected | {status_counts['rejected']} |
| needs_review | {status_counts['needs_review']} |
| **Total** | **{len(validations)}** |

---
"""

    validation_blocks = []
    for i, v in enumerate(validations, 1):
        claim_id = v.get("claim_id", f"UNKNOWN-{i:03d}")
        claim_type = v.get("claim_type", "other_tactical")
        orig_confidence = v.get("original_confidence", "medium")
        status = v.get("validation_status", "needs_review")
        reason_code = v.get("validation_reason_code", "needs_review_insufficient_evidence")
        summary = v.get("validation_summary", "")
        conflict_notes = v.get("conflict_notes", "none")
        confidence_after = v.get("confidence_after_validation", "low")
        patch_rec = v.get("team_archive_patch_recommendation", "none")
        claim_text = v.get("claim_text", "")

        # Build evidence checked section
        evidence_items = v.get("evidence_checked", [])
        if evidence_items:
            evidence_lines = []
            for e in evidence_items:
                src_name = e.get("source_name", "Unknown")
                src_url = e.get("source_url", "N/A")
                src_summary = e.get("evidence_summary", "")
                if src_url and src_url != "N/A":
                    evidence_lines.append(f"- **{src_name}**: {src_summary} ([link]({src_url}))")
                else:
                    evidence_lines.append(f"- **{src_name}**: {src_summary}")
            evidence_str = "\n".join(evidence_lines)
        else:
            evidence_str = "- No external evidence checked"

        # Status emoji
        status_emoji = {"validated": "✅", "rejected": "❌", "needs_review": "⚠️"}.get(status, "⚠️")

        block = f"""## Validation {i:03d} — {claim_id}

**Claim Type**: `{claim_type}`
**Original Confidence**: `{orig_confidence}`
**Validation Status**: {status_emoji} `{status}`
**Reason Code**: `{reason_code}`

**Claim**: {claim_text}

**Validation Summary**: {summary}

**Evidence Checked**:
{evidence_str}

**Conflict Notes**: {conflict_notes}

**Confidence After Validation**: `{confidence_after}`
**Team Archive Patch Allowed**: `false`
**Patch Recommendation**: `{patch_rec}`

---"""
        validation_blocks.append(block)

    handoff = f"""
## Handoff Note

Next step: YT-05 Team Archive Candidate Patch
Input path: 04_RAG_Raw_Data/youtube_tactical_sources/validation/{source_claims_name.replace('_claims.md', '_validation.md')}
Only claims with patch_recommendation: candidate_after_review are eligible for YT-05.
"""

    return frontmatter + header + "\n".join(validation_blocks) + handoff


def build_blocked_report(
    fm: dict[str, Any],
    source_claims_name: str,
    failure_reason: str,
    failure_details: str,
    blocked_at: str,
) -> str:
    """Build a blocked validation report."""
    video_id = fm.get("video_id", "UNKNOWN")
    target_team = fm.get("target_team", "")
    source_channel = fm.get("source_channel", "")

    return f"""---
source_kind: validation_blocked
source_claims: {source_claims_name}
video_id: {video_id}
target_team: {target_team}
source_channel: {source_channel}
blocked_at: {blocked_at}
failure_reason: {failure_reason}
status: blocked
---

# Validation Blocked — {target_team} — {source_channel} — {video_id}

## Summary

| Field | Value |
|-------|-------|
| Source Claims | {source_claims_name} |
| Failure Reason | `{failure_reason}` |
| Blocked At | {blocked_at} |

## Failure Details

{failure_details}

## Next Suggested Action

Review the source claims file and ensure it has valid frontmatter with `source_authority: extracted_claims` and `downstream_allowed: [validation]`.

> Truth > Completeness. No validation results were fabricated.
"""


def build_output_basename(source_claims_path: Path) -> str:
    """Build output basename from claims filename."""
    name = source_claims_path.stem  # e.g. 2026-05-22_Arsenal_Tifo_GxvSAS97L9c_claims
    if name.endswith("_claims"):
        return name[: -len("_claims")]
    return name


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Tactical Claim Validation Helper — I/O and formatting"
    )
    parser.add_argument("--claims", required=True, help="Path to claims.md")
    parser.add_argument("--team", required=True, help="Target team (English standard name)")
    parser.add_argument(
        "--validation-json",
        default=None,
        help="Path to JSON file containing LLM validation results",
    )
    parser.add_argument(
        "--write-output",
        action="store_true",
        help="Write output to AresVault validation/ directory",
    )
    parser.add_argument(
        "--print-claims",
        action="store_true",
        help="Print parsed claims to stdout (for LLM validation input)",
    )
    parser.add_argument("--vault-path", default=None, help="Override ARES_VAULT_PATH")
    args = parser.parse_args()

    if args.vault_path:
        global VAULT_BASE, VALIDATION_DIR
        VAULT_BASE = Path(args.vault_path)
        VALIDATION_DIR = VAULT_BASE / "04_RAG_Raw_Data" / "youtube_tactical_sources" / "validation"

    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)

    claims_path = Path(args.claims)
    blocked_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # ── Step 1: Read claims file ──────────────────────────────────────────────
    if not claims_path.exists():
        logger.error("Claims file not found: %s", claims_path)
        blocked = build_blocked_report(
            fm={"video_id": "UNKNOWN", "target_team": args.team, "source_channel": "UNKNOWN"},
            source_claims_name=claims_path.name,
            failure_reason="claims_file_missing",
            failure_details=f"File not found: {claims_path}",
            blocked_at=blocked_at,
        )
        blocked_path = VALIDATION_DIR / f"{claims_path.stem}_validation_blocked.md"
        blocked_path.write_text(blocked, encoding="utf-8")
        logger.warning("Blocked report written: %s", blocked_path)
        return 1

    content = claims_path.read_text(encoding="utf-8", errors="replace")
    fm, body = extract_frontmatter(content)

    # ── Step 2: Parse claims ──────────────────────────────────────────────────
    claims = parse_claims(body)

    # ── Step 3: Validate structure ────────────────────────────────────────────
    is_valid, failure_reason = validate_claims_file(fm, claims)
    if not is_valid:
        blocked = build_blocked_report(
            fm=fm or {"video_id": "UNKNOWN", "target_team": args.team, "source_channel": "UNKNOWN"},
            source_claims_name=claims_path.name,
            failure_reason=failure_reason,
            failure_details=f"Validation failed: {failure_reason}",
            blocked_at=blocked_at,
        )
        basename = build_output_basename(claims_path)
        blocked_path = VALIDATION_DIR / f"{basename}_validation_blocked.md"
        blocked_path.write_text(blocked, encoding="utf-8")
        logger.warning("Validation failed (%s). Blocked report: %s", failure_reason, blocked_path)
        return 1

    logger.info("Parsed %d claims from: %s", len(claims), claims_path.name)

    # ── Step 4: Print claims for LLM (if requested) ───────────────────────────
    if args.print_claims:
        print("=" * 60)
        print("CLAIMS FOR VALIDATION (for LLM input)")
        print("=" * 60)
        print(f"Target team: {fm.get('target_team', args.team)}")
        print(f"Language: {fm.get('language', 'unknown')}")
        print(f"Total claims: {len(claims)}")
        print("=" * 60)
        for i, c in enumerate(claims, 1):
            print(f"\n--- Claim {i:03d} ---")
            print(f"ID: {c.get('claim_id', 'UNKNOWN')}")
            print(f"Type: {c.get('claim_type', 'unknown')}")
            print(f"Confidence: {c.get('confidence', 'unknown')}")
            print(f"Claim: {c.get('claim_text', '')}")
            print(f"Evidence: {c.get('evidence_quote', '')[:200]}...")
            print(f"Location: {c.get('evidence_location', 'unknown')}")
        print("=" * 60)
        print("\nINSTRUCTION FOR LLM:")
        print("Validate each claim above. For each claim, provide:")
        print("  - validation_status: validated | rejected | needs_review")
        print("  - validation_reason_code: (see SKILL.md for codes)")
        print("  - validation_summary: one sentence")
        print("  - evidence_checked: list of sources checked")
        print("  - conflict_notes: any conflicts found")
        print("  - confidence_after_validation: low | medium | high")
        print("  - team_archive_patch_recommendation: none | candidate_after_review")
        print("\nKEY FACT: 2024/25 Premier League champion = Liverpool (NOT Arsenal)")
        print("Any claim stating Arsenal won 2024/25 PL title must be rejected_fact_conflict")
        return 0

    # ── Step 5: Load validation results from JSON ─────────────────────────────
    if args.validation_json:
        val_path = Path(args.validation_json)
        if not val_path.exists():
            logger.error("Validation JSON not found: %s", val_path)
            return 1

        with open(val_path, encoding="utf-8") as f:
            val_data = json.load(f)

        if isinstance(val_data, dict) and "validations" in val_data:
            validations = val_data["validations"]
        elif isinstance(val_data, list):
            validations = val_data
        else:
            logger.error("Invalid validation JSON format")
            return 1

        # Normalize and validate
        valid_validations = []
        for v in validations:
            if not isinstance(v, dict):
                continue
            if not v.get("claim_id"):
                continue
            # Normalize status
            if v.get("validation_status") not in VALID_STATUSES:
                v["validation_status"] = "needs_review"
            # Normalize reason code
            if v.get("validation_reason_code") not in VALID_REASON_CODES:
                v["validation_reason_code"] = "needs_review_insufficient_evidence"
            # Ensure required fields
            v.setdefault("team_archive_patch_allowed", False)
            v.setdefault("team_archive_patch_recommendation", "none")
            v.setdefault("confidence_after_validation", "low")
            v.setdefault("evidence_checked", [])
            v.setdefault("conflict_notes", "none")
            valid_validations.append(v)

        if not valid_validations:
            logger.error("No valid validation results found in JSON")
            return 1

        validated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        basename = build_output_basename(claims_path)
        output_path = VALIDATION_DIR / f"{basename}_validation.md"

        markdown = build_validation_markdown(
            fm=fm,
            validations=valid_validations,
            validated_at=validated_at,
            source_claims_name=claims_path.name,
        )

        if args.write_output:
            output_path.write_text(markdown, encoding="utf-8")
            logger.info("✅ Validation written: %s", output_path)
            logger.info("   Total: %d | validated: %d | rejected: %d | needs_review: %d",
                       len(valid_validations),
                       sum(1 for v in valid_validations if v.get("validation_status") == "validated"),
                       sum(1 for v in valid_validations if v.get("validation_status") == "rejected"),
                       sum(1 for v in valid_validations if v.get("validation_status") == "needs_review"))
        else:
            print(markdown)

        return 0

    # ── Default: print info ───────────────────────────────────────────────────
    logger.info("Claims file validated. %d claims ready for validation.", len(claims))
    logger.info("Run with --print-claims to get claims for LLM validation.")
    logger.info("Run with --validation-json <path> --write-output to write results.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
