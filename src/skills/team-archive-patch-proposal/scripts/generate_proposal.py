#!/usr/bin/env python3
"""
generate_proposal.py — Team Archive Patch Proposal Generator
Skill: team-archive-patch-proposal v1.0
Issue: LMW-128

This script:
  1. Reads a validation.md file
  2. Filters validated + candidate_after_review items
  3. Generates a structured Team Archive patch proposal
  4. Writes to AresVault patch_proposals/ directory

Guardrails:
    - Does NOT modify Team Archive
    - Does NOT create memory cards
    - Does NOT re-validate claims
    - Only includes validated + candidate_after_review items
    - Every patch item has apply_allowed: false + requires_human_review: true
"""

from __future__ import annotations

import argparse
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
logger = logging.getLogger("generate_proposal")

# ── Vault paths ──────────────────────────────────────────────────────────────

VAULT_BASE = Path(
    os.environ.get("ARES_VAULT_PATH", "/Users/liumingwei/vaults/AresVault")
)
PATCH_PROPOSALS_DIR = (
    VAULT_BASE / "04_RAG_Raw_Data" / "youtube_tactical_sources" / "patch_proposals"
)

# ── Target section mapping ────────────────────────────────────────────────────

TARGET_SECTION_MAP = {
    "build_up": "tactical_analysis_candidates.build_up",
    "defensive_block": "tactical_analysis_candidates.defensive_shape",
    "set_piece": "tactical_analysis_candidates.set_pieces",
    "player_role": "tactical_analysis_candidates.player_roles",
    "pressing": "tactical_analysis_candidates.pressing",
    "team_identity": "tactical_analysis_candidates.team_identity",
    "coach_principle": "tactical_analysis_candidates.coach_principles",
    "transition": "tactical_analysis_candidates.transitions",
    "chance_creation": "tactical_analysis_candidates.chance_creation",
    "formation_shape": "tactical_analysis_candidates.formation",
    "opponent_specific": "tactical_analysis_candidates.opponent_specific",
    "other_tactical": "tactical_analysis_candidates.other",
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


def parse_validations(body: str) -> list[dict[str, Any]]:
    """Parse validation blocks from validation.md body."""
    validations = []

    # Split on validation headers
    val_blocks = re.split(r"\n## Validation \d+", body)

    for block in val_blocks[1:]:
        v: dict[str, Any] = {}

        # Extract claim_id from header
        id_match = re.search(r"—\s*([A-Za-z0-9_\-]+)\s*$", block.split("\n")[0])
        if id_match:
            v["claim_id"] = id_match.group(1).strip()

        # Extract claim_type
        type_match = re.search(r"\*\*Claim Type\*\*:\s*`([^`]+)`", block)
        if type_match:
            v["claim_type"] = type_match.group(1)

        # Extract original confidence
        orig_conf_match = re.search(r"\*\*Original Confidence\*\*:\s*`([^`]+)`", block)
        if orig_conf_match:
            v["original_confidence"] = orig_conf_match.group(1)

        # Extract validation status
        status_match = re.search(r"\*\*Validation Status\*\*:.*?`(validated|rejected|needs_review)`", block)
        if status_match:
            v["validation_status"] = status_match.group(1)

        # Extract reason code
        reason_match = re.search(r"\*\*Reason Code\*\*:\s*`([^`]+)`", block)
        if reason_match:
            v["validation_reason_code"] = reason_match.group(1)

        # Extract claim text
        claim_text_match = re.search(r"\*\*Claim\*\*:\s*(.+?)(?=\n\n|\*\*Validation Summary\*\*)", block, re.DOTALL)
        if claim_text_match:
            v["claim_text"] = claim_text_match.group(1).strip()

        # Extract validation summary
        summary_match = re.search(r"\*\*Validation Summary\*\*:\s*(.+?)(?=\n\n|\*\*Evidence)", block, re.DOTALL)
        if summary_match:
            v["validation_summary"] = summary_match.group(1).strip()

        # Extract evidence checked (simplified)
        evidence_matches = re.findall(r"\*\*([^*]+)\*\*:\s*(.+?)(?=\n-|\n\n|\Z)", block)
        evidence_items = []
        for src_name, src_detail in evidence_matches:
            if "link" in src_detail or "http" in src_detail:
                url_match = re.search(r"\(([^)]+)\)", src_detail)
                url = url_match.group(1) if url_match else "N/A"
                summary = re.sub(r"\s*\([^)]+\)\s*$", "", src_detail).strip()
                evidence_items.append({
                    "source_name": src_name.strip(),
                    "source_url": url,
                    "evidence_summary": summary,
                })
        if evidence_items:
            v["evidence_checked"] = evidence_items

        # Extract confidence after validation
        conf_after_match = re.search(r"\*\*Confidence After Validation\*\*:\s*`([^`]+)`", block)
        if conf_after_match:
            v["confidence_after_validation"] = conf_after_match.group(1)

        # Extract patch recommendation
        patch_rec_match = re.search(r"\*\*Patch Recommendation\*\*:\s*`([^`]+)`", block)
        if patch_rec_match:
            v["team_archive_patch_recommendation"] = patch_rec_match.group(1)

        if v.get("claim_id") and v.get("validation_status"):
            validations.append(v)

    return validations


def is_candidate(v: dict[str, Any]) -> tuple[bool, str]:
    """Check if a validation item is a patch candidate."""
    if v.get("validation_status") != "validated":
        return False, f"validation_status={v.get('validation_status')}"

    if v.get("team_archive_patch_recommendation") != "candidate_after_review":
        return False, f"patch_recommendation={v.get('team_archive_patch_recommendation')}"

    if not v.get("evidence_checked"):
        return False, "no_evidence_checked"

    # Exclude low confidence + language ambiguity
    if (v.get("confidence_after_validation") == "low" and
            "language_ambiguity" in v.get("validation_reason_code", "")):
        return False, "low_confidence_language_ambiguity"

    return True, ""


def build_proposed_text(v: dict[str, Any], source_channel: str, video_id: str, source_date: str) -> str:
    """Build proposed text for Team Archive from validation item."""
    claim_text = v.get("claim_text", "")
    claim_type = v.get("claim_type", "other_tactical")
    confidence = v.get("confidence_after_validation", "medium")

    # Build a concise proposed text
    source_ref = f"[{source_channel} / {video_id} / {source_date}]"
    proposed = f"{claim_text} {source_ref} (confidence: {confidence}, source_authority: secondary_synthesis)"
    return proposed


# ── Output builders ───────────────────────────────────────────────────────────


def build_proposal_markdown(
    fm: dict[str, Any],
    candidates: list[dict[str, Any]],
    excluded: list[tuple[dict[str, Any], str]],
    target_archive_path: str,
    proposed_at: str,
    source_validation_name: str,
) -> str:
    """Build the patch proposal markdown."""
    video_id = fm.get("video_id", "UNKNOWN")
    source_url = fm.get("source_url", "")
    target_team = fm.get("target_team", "")
    target_league = fm.get("target_league", "")
    source_channel = fm.get("source_channel", "")
    language = fm.get("language", "")

    # Extract date from validation filename
    date_match = re.match(r"(\d{4}-\d{2}-\d{2})", source_validation_name)
    source_date = date_match.group(1) if date_match else "unknown"

    exclusion_reasons = ", ".join(set(reason for _, reason in excluded)) if excluded else "none"

    frontmatter = f"""---
source_kind: team_archive_patch_proposal
source_validation: {source_validation_name}
video_id: {video_id}
source_url: {source_url}
target_team: {target_team}
target_league: {target_league}
source_channel: {source_channel}
language: {language}
target_archive_path: {target_archive_path}
proposal_skill: team-archive-patch-proposal v1.0
proposed_at: {proposed_at}
total_candidates: {len(candidates)}
excluded_count: {len(excluded)}
patch_status: proposed
apply_allowed: false
requires_human_review: true
---"""

    header = f"""
# Team Archive Patch Proposal — {target_team} — {source_channel} — {video_id}

## Source

| Field | Value |
|-------|-------|
| Validation File | {source_validation_name} |
| Video URL | {source_url} |
| Channel | {source_channel} |
| Target Team | {target_team} |
| Target Archive | {target_archive_path} |
| Proposed At | {proposed_at} |

## ⚠️ Review Gate

> **This is a patch proposal only. No changes have been made to Team Archive.**
>
> - `apply_allowed: false` — must not be applied without human review
> - `requires_human_review: true` — human must review each item before applying
> - Source authority: `secondary_synthesis` (not `profile_authority`)

## Proposal Summary

| Metric | Value |
|--------|-------|
| Total validation items | {len(candidates) + len(excluded)} |
| Candidates included | {len(candidates)} |
| Items excluded | {len(excluded)} |
| Exclusion reasons | {exclusion_reasons} |

---
"""

    patch_blocks = []
    for i, v in enumerate(candidates, 1):
        claim_id = v.get("claim_id", f"UNKNOWN-{i:03d}")
        claim_type = v.get("claim_type", "other_tactical")
        confidence = v.get("confidence_after_validation", "medium")
        validation_summary = v.get("validation_summary", "")
        claim_text = v.get("claim_text", "")

        target_section = TARGET_SECTION_MAP.get(claim_type, "tactical_analysis_candidates.other")
        patch_id = f"{target_team}-{video_id}-patch-{i:03d}"

        proposed_text = build_proposed_text(v, source_channel, video_id, source_date)

        # Build evidence summary
        evidence_items = v.get("evidence_checked", [])
        if evidence_items:
            evidence_str = "; ".join(
                f"{e.get('source_name', 'Unknown')}: {e.get('evidence_summary', '')[:100]}"
                for e in evidence_items[:2]
            )
        else:
            evidence_str = "See validation file"

        block = f"""## Patch Item {i:03d}

**Patch ID**: `{patch_id}`
**Claim ID**: `{claim_id}`
**Claim Type**: `{claim_type}`
**Target Section**: `{target_section}`
**Confidence**: `{confidence}`

**Proposed Text**:
> {proposed_text}

**Rationale**: {validation_summary}

**Evidence Summary**: {evidence_str}

**Source**: [{source_channel} — {video_id}]({source_url})

**Metadata**:
- `apply_allowed: false`
- `requires_human_review: true`
- `patch_status: proposed`
- `validation_file`: {source_validation_name}

---"""
        patch_blocks.append(block)

    # Excluded items section
    if excluded:
        excluded_section = "\n## Excluded Items\n\n"
        excluded_section += "| Claim ID | Reason |\n|----------|--------|\n"
        for v, reason in excluded:
            excluded_section += f"| `{v.get('claim_id', 'UNKNOWN')}` | {reason} |\n"
    else:
        excluded_section = ""

    handoff = f"""
## Handoff Note

Next step: YT-06 Postmatch / Data Promotion Review (human review)
Input path: 04_RAG_Raw_Data/youtube_tactical_sources/patch_proposals/{source_validation_name.replace('_validation.md', '_team_archive_patch_proposal.md')}
Only items approved in YT-06 may be applied to Team Archive.
"""

    return frontmatter + header + "\n".join(patch_blocks) + excluded_section + handoff


def build_output_basename(source_validation_path: Path) -> str:
    """Build output basename from validation filename."""
    name = source_validation_path.stem  # e.g. 2025-10-11_Arsenal_HALFSPACETHEORY_VHymL0kvIXQ_validation
    if name.endswith("_validation"):
        return name[: -len("_validation")]
    return name


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Team Archive Patch Proposal Generator"
    )
    parser.add_argument("--validation", required=True, help="Path to validation.md")
    parser.add_argument("--team", required=True, help="Target team (English standard name)")
    parser.add_argument("--archive", required=True, help="Target Team Archive file path (reference only)")
    parser.add_argument(
        "--write-output",
        action="store_true",
        help="Write output to AresVault patch_proposals/ directory",
    )
    parser.add_argument("--vault-path", default=None, help="Override ARES_VAULT_PATH")
    args = parser.parse_args()

    if args.vault_path:
        global VAULT_BASE, PATCH_PROPOSALS_DIR
        VAULT_BASE = Path(args.vault_path)
        PATCH_PROPOSALS_DIR = VAULT_BASE / "04_RAG_Raw_Data" / "youtube_tactical_sources" / "patch_proposals"

    PATCH_PROPOSALS_DIR.mkdir(parents=True, exist_ok=True)

    validation_path = Path(args.validation)
    proposed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # ── Step 1: Read validation file ──────────────────────────────────────────
    if not validation_path.exists():
        logger.error("Validation file not found: %s", validation_path)
        return 1

    content = validation_path.read_text(encoding="utf-8", errors="replace")
    fm, body = extract_frontmatter(content)

    if not fm:
        logger.error("Invalid frontmatter in validation file")
        return 1

    # ── Step 2: Parse validations ─────────────────────────────────────────────
    validations = parse_validations(body)
    logger.info("Parsed %d validation items from: %s", len(validations), validation_path.name)

    # ── Step 3: Filter candidates ─────────────────────────────────────────────
    candidates = []
    excluded = []

    for v in validations:
        is_cand, reason = is_candidate(v)
        if is_cand:
            candidates.append(v)
        else:
            excluded.append((v, reason))

    logger.info("Candidates: %d | Excluded: %d", len(candidates), len(excluded))
    for v, reason in excluded:
        logger.info("  Excluded: %s (%s)", v.get("claim_id", "UNKNOWN"), reason)

    if not candidates:
        logger.warning("No candidates found. No proposal generated.")
        return 0

    # ── Step 4: Build proposal ────────────────────────────────────────────────
    basename = build_output_basename(validation_path)
    output_path = PATCH_PROPOSALS_DIR / f"{basename}_team_archive_patch_proposal.md"

    markdown = build_proposal_markdown(
        fm=fm,
        candidates=candidates,
        excluded=excluded,
        target_archive_path=args.archive,
        proposed_at=proposed_at,
        source_validation_name=validation_path.name,
    )

    if args.write_output:
        output_path.write_text(markdown, encoding="utf-8")
        logger.info("✅ Patch proposal written: %s", output_path)
        logger.info("   Candidates: %d | Excluded: %d", len(candidates), len(excluded))
        logger.info("   Team Archive NOT modified: %s", args.archive)
    else:
        print(markdown)

    return 0


if __name__ == "__main__":
    sys.exit(main())
