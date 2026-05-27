#!/usr/bin/env python3
"""
extract_claims.py — Transcript-to-Tactical-Claims Helper Script
Skill: transcript-to-tactical-claims v1.0
Issue: LMW-126

This script handles:
  1. Reading and validating transcript_raw.md
  2. Cleaning VTT/SRT noise from transcript text
  3. Preparing a clean text payload for LLM claim extraction
  4. Writing the structured claims output to AresVault claims/
  5. Generating blocked/empty reports on failure

The LLM (Ares/Codex) performs the actual claim extraction.
This script handles I/O, cleaning, and formatting.

Usage:
    ./venv/bin/python src/skills/transcript-to-tactical-claims/scripts/extract_claims.py \\
        --transcript "/path/to/transcript_raw.md" \\
        --team "Arsenal" \\
        --max-claims 20

    # With LLM-extracted claims JSON piped in:
    ./venv/bin/python src/skills/transcript-to-tactical-claims/scripts/extract_claims.py \\
        --transcript "/path/to/transcript_raw.md" \\
        --team "Arsenal" \\
        --claims-json "/path/to/claims.json" \\
        --write-output

Guardrails:
    - Does NOT fetch YouTube transcripts
    - Does NOT download video or audio
    - Does NOT validate claims
    - Does NOT modify Team Archive
    - Does NOT overwrite raw transcript files
    - Generates blocked report on any failure
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
logger = logging.getLogger("extract_claims")

# ── Vault paths ──────────────────────────────────────────────────────────────

VAULT_BASE = Path(
    os.environ.get("ARES_VAULT_PATH", "/Users/liumingwei/vaults/AresVault")
)
CLAIMS_DIR = (
    VAULT_BASE / "04_RAG_Raw_Data" / "youtube_tactical_sources" / "claims"
)

# ── Claim type vocabulary ─────────────────────────────────────────────────────

VALID_CLAIM_TYPES = {
    "team_identity",
    "formation_shape",
    "pressing",
    "build_up",
    "chance_creation",
    "defensive_block",
    "transition",
    "player_role",
    "coach_principle",
    "set_piece",
    "opponent_specific",
    "other_tactical",
}

VALID_CONFIDENCE = {"low", "medium", "high"}


# ── VTT/SRT Cleaner ───────────────────────────────────────────────────────────


def clean_vtt_text(raw_text: str) -> str:
    """
    Clean VTT/SRT subtitle noise from transcript text.

    Removes:
    - VTT header (WEBVTT, NOTE lines)
    - Timestamp lines (HH:MM:SS.mmm --> HH:MM:SS.mmm)
    - Inline time tags (<HH:MM:SS.mmm><c>...</c>)
    - align/position metadata
    - Duplicate consecutive lines
    - Empty lines

    Returns cleaned plain text.
    """
    lines = raw_text.split("\n")
    cleaned: list[str] = []
    seen: set[str] = set()

    for line in lines:
        # Skip VTT header
        if line.strip().startswith("WEBVTT") or line.strip().startswith("NOTE"):
            continue

        # Skip timestamp lines
        if re.match(r"^\d{2}:\d{2}:\d{2}\.\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}", line):
            continue

        # Skip align/position metadata lines
        if "align:start" in line or "position:" in line:
            continue

        # Remove inline time tags: <00:00:05.269><c>text</c> → text
        line = re.sub(r"<\d{2}:\d{2}:\d{2}\.\d{3}>", "", line)
        line = re.sub(r"<c>|</c>", "", line)

        # Remove VTT cue settings
        line = re.sub(r"\s+align:\w+\s+position:\d+%", "", line)

        # Strip whitespace
        line = line.strip()

        # Skip empty lines
        if not line:
            continue

        # Skip duplicate consecutive lines
        if line in seen:
            continue

        # Deduplicate: keep only unique lines in a sliding window
        seen.add(line)
        if len(seen) > 50:
            # Reset seen set periodically to allow re-occurrence of common phrases
            seen = set(cleaned[-10:]) if len(cleaned) >= 10 else set(cleaned)

        cleaned.append(line)

    return "\n".join(cleaned)


def extract_frontmatter(content: str) -> tuple[dict[str, Any], str]:
    """
    Parse YAML frontmatter from markdown content.

    Returns (frontmatter_dict, body_text).
    """
    if not content.startswith("---"):
        return {}, content

    end_idx = content.find("\n---", 3)
    if end_idx == -1:
        return {}, content

    fm_text = content[3:end_idx].strip()
    body = content[end_idx + 4:].strip()

    # Simple YAML parser for flat key-value pairs and lists
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


def validate_transcript(fm: dict[str, Any], body: str) -> tuple[bool, str]:
    """
    Validate transcript frontmatter and body.

    Returns (is_valid, failure_reason).
    """
    if not fm:
        return False, "invalid_frontmatter"

    source_authority = fm.get("source_authority", "")
    if source_authority != "raw_transcript":
        return False, "invalid_frontmatter"

    downstream_allowed = fm.get("downstream_allowed", [])
    if isinstance(downstream_allowed, str):
        downstream_allowed = [downstream_allowed]
    if "claim_extraction" not in downstream_allowed:
        return False, "claim_extraction_not_allowed"

    if not body.strip():
        return False, "empty_transcript"

    return True, ""


def assess_transcript_quality(cleaned_text: str, language: str) -> tuple[bool, str]:
    """
    Assess cleaned transcript quality.

    Returns (is_usable, failure_reason).
    """
    word_count = len(cleaned_text.split())

    if word_count < 100:
        return False, "transcript_too_short"

    # Check for speech content (basic heuristic)
    # If text is mostly timestamps or numbers, it's likely not speech
    non_numeric = re.sub(r"[\d:.\->\s]", "", cleaned_text)
    if len(non_numeric) < 50:
        return False, "no_speech_content"

    return True, ""


# ── Output builders ───────────────────────────────────────────────────────────


def build_claims_markdown(
    fm: dict[str, Any],
    claims: list[dict[str, Any]],
    extracted_at: str,
    source_transcript_name: str,
) -> str:
    """Build the claims output markdown."""
    video_id = fm.get("video_id", "UNKNOWN")
    source_url = fm.get("source_url", "")
    target_team = fm.get("target_team", "")
    target_league = fm.get("target_league", "")
    source_channel = fm.get("source_channel", "")
    language = fm.get("language", "")
    transcript_source = fm.get("transcript_source", "")

    # Count by type
    type_counts: dict[str, int] = {}
    conf_counts = {"high": 0, "medium": 0, "low": 0}
    for c in claims:
        ct = c.get("claim_type", "other_tactical")
        type_counts[ct] = type_counts.get(ct, 0) + 1
        conf = c.get("confidence", "low")
        conf_counts[conf] = conf_counts.get(conf, 0) + 1

    claims_by_type_yaml = "\n".join(
        f"  {ct}: {cnt}" for ct, cnt in sorted(type_counts.items())
    )

    frontmatter = f"""---
source_kind: tactical_claims
source_transcript: {source_transcript_name}
video_id: {video_id}
source_url: {source_url}
target_team: {target_team}
target_league: {target_league}
source_channel: {source_channel}
language: {language}
transcript_source: {transcript_source}
extraction_skill: transcript-to-tactical-claims v1.0
extracted_at: {extracted_at}
total_claims: {len(claims)}
claims_by_type:
{claims_by_type_yaml}
source_authority: extracted_claims
profile_authority: false
requires_validation: true
validation_status: not_validated
team_archive_patch_allowed: false
downstream_allowed:
  - validation
downstream_forbidden:
  - direct_team_archive_patch
  - prematch_conclusion
---"""

    header = f"""
# Tactical Claims — {target_team} — {source_channel} — {video_id}

## Source

| Field | Value |
|-------|-------|
| Transcript File | {source_transcript_name} |
| Video URL | {source_url} |
| Channel | {source_channel} |
| Target Team | {target_team} |
| Target League | {target_league} |
| Language | {language} |
| Extracted At | {extracted_at} |

## ⚠️ Boundary Notice

> **These are unvalidated tactical claims extracted from raw transcript.**
>
> - ✅ Allowed as input for: validation (YT-04)
> - ❌ Must NOT directly patch Team Archive
> - ❌ Must NOT be treated as verified tactical memory
> - ❌ Must NOT be used to generate prematch conclusions without YT-04 validation

## Claims Summary

Total: {len(claims)} claims | High confidence: {conf_counts['high']} | Medium: {conf_counts['medium']} | Low: {conf_counts['low']}

---
"""

    claim_blocks = []
    for i, claim in enumerate(claims, 1):
        claim_id = claim.get("claim_id", f"{target_team}-{video_id}-{i:03d}")
        claim_type = claim.get("claim_type", "other_tactical")
        confidence = claim.get("confidence", "low")
        claim_text = claim.get("claim_text", "")
        evidence_quote = claim.get("evidence_quote", "")
        evidence_location = claim.get("evidence_location", "unknown")
        confidence_note = claim.get("confidence_note", "none")

        block = f"""## Claim {i:03d}

**ID**: `{claim_id}`
**Type**: `{claim_type}`
**Confidence**: `{confidence}`
**Team**: {target_team}

**Claim**: {claim_text}

**Evidence**:
> "{evidence_quote}"

**Location**: {evidence_location}

**Metadata**:
- `requires_validation: true`
- `validation_status: not_validated`
- `team_archive_patch_allowed: false`
- `confidence_note`: {confidence_note}

---"""
        claim_blocks.append(block)

    handoff = f"""
## Handoff Note

Next step: YT-04 Tactical Claim Validation
Input path: 04_RAG_Raw_Data/youtube_tactical_sources/claims/{source_transcript_name.replace('_transcript_raw.md', '_claims.md')}
"""

    return frontmatter + header + "\n".join(claim_blocks) + handoff


def build_blocked_report(
    fm: dict[str, Any],
    source_transcript_name: str,
    failure_reason: str,
    failure_details: str,
    blocked_at: str,
    manual_review: bool = True,
) -> str:
    """Build a blocked/empty report."""
    video_id = fm.get("video_id", "UNKNOWN")
    target_team = fm.get("target_team", "")
    source_channel = fm.get("source_channel", "")

    status = "blocked" if failure_reason != "no_tactical_content" else "empty"

    next_action_map = {
        "transcript_file_missing": "確認 transcript 文件路径是否正确",
        "invalid_frontmatter": "检查 transcript frontmatter 格式",
        "claim_extraction_not_allowed": "检查 transcript 的 downstream_allowed 字段",
        "empty_transcript": "重新运行 youtube-transcript-ingestion (YT-02)",
        "transcript_too_short": "检查 transcript 文件是否完整，或重新提取",
        "no_speech_content": "确认视频是否有语音内容，考虑换一个视频",
        "no_tactical_content": "此视频可能不包含战术分析内容，考虑换一个视频",
        "evidence_ungroundable": "人工审查 transcript，确认是否有战术内容",
        "language_too_noisy": "考虑获取更高质量的字幕（手动字幕或英文原声）",
    }
    next_action = next_action_map.get(failure_reason, "人工审查 transcript 文件")

    return f"""---
source_kind: claims_blocked
source_transcript: {source_transcript_name}
video_id: {video_id}
target_team: {target_team}
source_channel: {source_channel}
blocked_at: {blocked_at}
failure_reason: {failure_reason}
status: {status}
---

# Claims {status.capitalize()} — {target_team} — {source_channel} — {video_id}

## Summary

| Field | Value |
|-------|-------|
| Source Transcript | {source_transcript_name} |
| Failure Reason | `{failure_reason}` |
| Status | {status} |
| Blocked At | {blocked_at} |

## Failure Details

{failure_details}

## Manual Review Recommended

{"Yes — human review of transcript content is recommended." if manual_review else "No — automated retry may resolve this."}

## Next Suggested Action

{next_action}

> Truth > Completeness. No claims were invented.
"""


def build_output_basename(fm: dict[str, Any], source_transcript_path: Path) -> str:
    """Build output basename from transcript filename."""
    # Derive from transcript filename: replace _transcript_raw.md with _claims.md
    name = source_transcript_path.stem  # e.g. 2026-05-22_Arsenal_Tifo_GxvSAS97L9c_transcript_raw
    if name.endswith("_transcript_raw"):
        return name[: -len("_transcript_raw")]
    return name


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Transcript-to-Tactical-Claims Helper — I/O and formatting"
    )
    parser.add_argument("--transcript", required=True, help="Path to transcript_raw.md")
    parser.add_argument("--team", required=True, help="Target team (English standard name)")
    parser.add_argument(
        "--claims-json",
        default=None,
        help="Path to JSON file containing LLM-extracted claims (if available)",
    )
    parser.add_argument(
        "--write-output",
        action="store_true",
        help="Write output to AresVault claims/ directory",
    )
    parser.add_argument(
        "--print-cleaned",
        action="store_true",
        help="Print cleaned transcript text to stdout (for LLM input)",
    )
    parser.add_argument(
        "--max-claims",
        type=int,
        default=30,
        help="Maximum number of claims to include (default 30)",
    )
    parser.add_argument("--vault-path", default=None, help="Override ARES_VAULT_PATH")
    args = parser.parse_args()

    # Override vault path if provided
    if args.vault_path:
        global VAULT_BASE, CLAIMS_DIR
        VAULT_BASE = Path(args.vault_path)
        CLAIMS_DIR = VAULT_BASE / "04_RAG_Raw_Data" / "youtube_tactical_sources" / "claims"

    CLAIMS_DIR.mkdir(parents=True, exist_ok=True)

    transcript_path = Path(args.transcript)
    blocked_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # ── Step 1: Read transcript file ──────────────────────────────────────────
    if not transcript_path.exists():
        logger.error("Transcript file not found: %s", transcript_path)
        # Write blocked report with minimal info
        blocked = build_blocked_report(
            fm={"video_id": "UNKNOWN", "target_team": args.team, "source_channel": "UNKNOWN"},
            source_transcript_name=transcript_path.name,
            failure_reason="transcript_file_missing",
            failure_details=f"File not found: {transcript_path}",
            blocked_at=blocked_at,
        )
        blocked_path = CLAIMS_DIR / f"{transcript_path.stem}_claims_blocked.md"
        blocked_path.write_text(blocked, encoding="utf-8")
        logger.warning("Blocked report written: %s", blocked_path)
        return 1

    content = transcript_path.read_text(encoding="utf-8", errors="replace")
    fm, body = extract_frontmatter(content)

    # ── Step 2: Validate ──────────────────────────────────────────────────────
    is_valid, failure_reason = validate_transcript(fm, body)
    if not is_valid:
        blocked = build_blocked_report(
            fm=fm or {"video_id": "UNKNOWN", "target_team": args.team, "source_channel": "UNKNOWN"},
            source_transcript_name=transcript_path.name,
            failure_reason=failure_reason,
            failure_details=f"Validation failed: {failure_reason}",
            blocked_at=blocked_at,
        )
        basename = build_output_basename(fm or {}, transcript_path)
        blocked_path = CLAIMS_DIR / f"{basename}_claims_blocked.md"
        blocked_path.write_text(blocked, encoding="utf-8")
        logger.warning("Validation failed (%s). Blocked report: %s", failure_reason, blocked_path)
        return 1

    # ── Step 3: Extract transcript body and clean ─────────────────────────────
    # Find the "## Raw Transcript" section
    raw_transcript_match = re.search(r"## Raw Transcript\n(.*?)(?=\n## |$)", body, re.DOTALL)
    if raw_transcript_match:
        raw_text = raw_transcript_match.group(1)
    else:
        raw_text = body

    cleaned_text = clean_vtt_text(raw_text)

    # ── Step 4: Quality assessment ────────────────────────────────────────────
    language = fm.get("language", "unknown")
    is_usable, quality_reason = assess_transcript_quality(cleaned_text, language)
    if not is_usable:
        blocked = build_blocked_report(
            fm=fm,
            source_transcript_name=transcript_path.name,
            failure_reason=quality_reason,
            failure_details=f"Cleaned text quality insufficient: {quality_reason}. Word count: {len(cleaned_text.split())}",
            blocked_at=blocked_at,
        )
        basename = build_output_basename(fm, transcript_path)
        blocked_path = CLAIMS_DIR / f"{basename}_claims_blocked.md"
        blocked_path.write_text(blocked, encoding="utf-8")
        logger.warning("Quality check failed (%s). Blocked report: %s", quality_reason, blocked_path)
        return 1

    word_count = len(cleaned_text.split())
    logger.info("Transcript cleaned. Word count: %d | Language: %s", word_count, language)

    # ── Step 5: Print cleaned text for LLM (if requested) ────────────────────
    if args.print_cleaned:
        print("=" * 60)
        print("CLEANED TRANSCRIPT TEXT (for LLM claim extraction)")
        print("=" * 60)
        print(f"Target team: {fm.get('target_team', args.team)}")
        print(f"Language: {language}")
        print(f"Word count: {word_count}")
        print(f"Source: {transcript_path.name}")
        print("=" * 60)
        # Print first 3000 words to avoid overwhelming output
        words = cleaned_text.split()
        preview = " ".join(words[:3000])
        print(preview)
        if len(words) > 3000:
            print(f"\n... [{len(words) - 3000} more words truncated] ...")
        print("=" * 60)
        print("\nINSTRUCTION FOR LLM:")
        print("Extract tactical claims from the above transcript.")
        print("Each claim must have a direct evidence_quote from the text.")
        print(f"Max claims: {args.max_claims}")
        print("Output format: JSON array of claim objects (see SKILL.md for schema)")
        return 0

    # ── Step 6: Load claims from JSON (if provided) ───────────────────────────
    if args.claims_json:
        claims_path = Path(args.claims_json)
        if not claims_path.exists():
            logger.error("Claims JSON not found: %s", claims_path)
            return 1

        with open(claims_path, encoding="utf-8") as f:
            claims_data = json.load(f)

        if isinstance(claims_data, dict) and "claims" in claims_data:
            claims = claims_data["claims"]
        elif isinstance(claims_data, list):
            claims = claims_data
        else:
            logger.error("Invalid claims JSON format")
            return 1

        # Validate and cap claims
        valid_claims = []
        for c in claims[: args.max_claims]:
            if not isinstance(c, dict):
                continue
            # Ensure required fields
            if not c.get("claim_text") or not c.get("evidence_quote"):
                logger.warning("Skipping claim without claim_text or evidence_quote")
                continue
            # Normalize claim_type
            if c.get("claim_type") not in VALID_CLAIM_TYPES:
                c["claim_type"] = "other_tactical"
            # Normalize confidence
            if c.get("confidence") not in VALID_CONFIDENCE:
                c["confidence"] = "low"
            # Ensure required metadata
            c.setdefault("requires_validation", True)
            c.setdefault("validation_status", "not_validated")
            c.setdefault("team_archive_patch_allowed", False)
            c.setdefault("target_team", fm.get("target_team", args.team))
            c.setdefault("source_file", transcript_path.name)
            c.setdefault("source_url", fm.get("source_url", ""))
            c.setdefault("video_id", fm.get("video_id", ""))
            c.setdefault("source_channel", fm.get("source_channel", ""))
            c.setdefault("language", language)
            c.setdefault("transcript_source", fm.get("transcript_source", ""))
            valid_claims.append(c)

        if not valid_claims:
            blocked = build_blocked_report(
                fm=fm,
                source_transcript_name=transcript_path.name,
                failure_reason="no_tactical_content",
                failure_details="Claims JSON provided but no valid claims found after validation.",
                blocked_at=blocked_at,
                manual_review=True,
            )
            basename = build_output_basename(fm, transcript_path)
            blocked_path = CLAIMS_DIR / f"{basename}_claims_blocked.md"
            blocked_path.write_text(blocked, encoding="utf-8")
            logger.warning("No valid claims. Blocked report: %s", blocked_path)
            return 1

        extracted_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        basename = build_output_basename(fm, transcript_path)
        output_path = CLAIMS_DIR / f"{basename}_claims.md"

        markdown = build_claims_markdown(
            fm=fm,
            claims=valid_claims,
            extracted_at=extracted_at,
            source_transcript_name=transcript_path.name,
        )

        if args.write_output:
            output_path.write_text(markdown, encoding="utf-8")
            logger.info("✅ Claims written: %s", output_path)
            logger.info("   Total claims: %d", len(valid_claims))
        else:
            print(markdown)

        return 0

    # ── Default: print info for manual LLM extraction ────────────────────────
    logger.info("Transcript validated and cleaned successfully.")
    logger.info("Word count: %d | Language: %s", word_count, language)
    logger.info("")
    logger.info("To extract claims:")
    logger.info("  1. Run with --print-cleaned to get cleaned text for LLM")
    logger.info("  2. Have LLM extract claims and save to JSON")
    logger.info("  3. Run with --claims-json <path> --write-output to write output")
    logger.info("")
    logger.info("Or use the SKILL.md directly in Antigravity/Kiro for agent-based extraction.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
