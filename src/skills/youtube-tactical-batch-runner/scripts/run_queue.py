#!/usr/bin/env python3
"""
run_queue.py — YouTube Tactical Intelligence Batch Runner
Skill: youtube-tactical-batch-runner v1.0
Issue: LMW-132

Reads an ingestion_queue.json and orchestrates YT-02 -> YT-06 for eligible items.

Usage:
    # Dry run (eligibility check only)
    ./venv/bin/python src/skills/youtube-tactical-batch-runner/scripts/run_queue.py \\
        --queue "/path/to/queue.json" --dry-run

    # Default run (skip already_exists)
    ./venv/bin/python src/skills/youtube-tactical-batch-runner/scripts/run_queue.py \\
        --queue "/path/to/queue.json"

    # Reuse existing outputs, only rerun missing stages
    ./venv/bin/python src/skills/youtube-tactical-batch-runner/scripts/run_queue.py \\
        --queue "/path/to/queue.json" --reuse-existing

    # Force rerun all stages
    ./venv/bin/python src/skills/youtube-tactical-batch-runner/scripts/run_queue.py \\
        --queue "/path/to/queue.json" --force

Safety boundaries:
    - Does NOT process pending_user_lookup items
    - Does NOT invent missing URLs
    - Does NOT download video or audio
    - Does NOT write profile_authority: true
    - Does NOT overwrite existing Team Archive content
    - Does NOT use NotebookLM as primary route
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("run_queue")

# ── Vault / project paths ─────────────────────────────────────────────────────

VAULT_BASE = Path(
    os.environ.get("ARES_VAULT_PATH", "/Users/liumingwei/vaults/AresVault")
)
PROJECT_BASE = Path(__file__).resolve().parents[3]  # ares-osint-telemetry root

TRANSCRIPTS_DIR = VAULT_BASE / "04_RAG_Raw_Data" / "youtube_tactical_sources" / "transcripts"
CLAIMS_DIR = VAULT_BASE / "04_RAG_Raw_Data" / "youtube_tactical_sources" / "claims"
VALIDATION_DIR = VAULT_BASE / "04_RAG_Raw_Data" / "youtube_tactical_sources" / "validation"
PATCH_PROPOSALS_DIR = VAULT_BASE / "04_RAG_Raw_Data" / "youtube_tactical_sources" / "patch_proposals"
BATCH_RUNS_DIR = VAULT_BASE / "04_RAG_Raw_Data" / "youtube_tactical_sources" / "batch_runs"
TEAM_ARCHIVES_DIR = VAULT_BASE / "02_Team_Archives"

# ── Source authority rules ────────────────────────────────────────────────────

ALLOWED_SOURCE_TIERS = {"tier_1", "tier_2"}
ALLOWED_SOURCE_QUALITIES = {"T1_tactical", "T2_mixed"}
EXCLUDED_SOURCE_QUALITIES = {"low_trust", "excluded"}

# ── Eligibility check ─────────────────────────────────────────────────────────


def check_eligibility(item: dict[str, Any]) -> tuple[bool, str]:
    """
    Check if a queue item is eligible for processing.
    Returns (is_eligible, skip_reason).
    """
    # Must be queued
    if item.get("ingestion_status") != "queued":
        return False, f"not_queued (status={item.get('ingestion_status')})"

    # Must be routed to transcript_ingestion
    if item.get("recommended_next_step") != "transcript_ingestion":
        return False, f"wrong_next_step ({item.get('recommended_next_step')})"

    # Must have a real source_url
    source_url = item.get("source_url", "")
    if not source_url or source_url == "PENDING_USER_LOOKUP":
        return False, "missing_url (PENDING_USER_LOOKUP)"

    # Must have a real video_id
    video_id = item.get("video_id", "")
    if not video_id or video_id == "PENDING_USER_LOOKUP":
        return False, "missing_video_id (PENDING_USER_LOOKUP)"

    # Source tier must be allowed
    source_tier = item.get("source_tier", "")
    channel_tier = item.get("channel_tier", "")
    # Normalize: T1 -> tier_1, T2 -> tier_2
    tier_map = {"T1": "tier_1", "T2": "tier_2", "tier_1": "tier_1", "tier_2": "tier_2"}
    effective_tier = tier_map.get(source_tier) or tier_map.get(channel_tier) or source_tier or channel_tier
    if effective_tier not in ALLOWED_SOURCE_TIERS:
        return False, f"source_authority_not_allowed (tier={source_tier or channel_tier})"

    # Source quality must not be excluded
    source_quality = item.get("source_quality", "")
    if source_quality in EXCLUDED_SOURCE_QUALITIES:
        return False, f"source_quality_excluded ({source_quality})"

    return True, ""


# ── Canonical filename builders ───────────────────────────────────────────────


def build_canonical_basename(item: dict[str, Any]) -> str:
    """Build canonical output basename from queue item."""
    date = item.get("source_date", "unknown")
    team = re.sub(r"[^\w]", "", item.get("target_team", "Unknown"))
    channel = re.sub(r"[^\w]", "", item.get("source_channel", "Unknown"))
    video_id = item.get("video_id", "UNKNOWN")
    return f"{date}_{team}_{channel}_{video_id}"


def check_existing_outputs(basename: str) -> dict[str, bool]:
    """Check which pipeline outputs already exist."""
    return {
        "transcript": (TRANSCRIPTS_DIR / f"{basename}_transcript_raw.md").exists(),
        "claims": (CLAIMS_DIR / f"{basename}_claims.md").exists(),
        "validation": (VALIDATION_DIR / f"{basename}_validation.md").exists(),
        "patch_proposal": (PATCH_PROPOSALS_DIR / f"{basename}_team_archive_patch_proposal.md").exists(),
    }


# ── Stage runners ─────────────────────────────────────────────────────────────


def run_yt02(item: dict[str, Any], basename: str) -> tuple[bool, str]:
    """Run YT-02 transcript ingestion."""
    script = PROJECT_BASE / "src" / "skills" / "youtube-transcript-ingestion" / "scripts" / "fetch_transcript.py"
    venv_python = PROJECT_BASE / "venv" / "bin" / "python"

    cmd = [
        str(venv_python), str(script),
        "--url", item["source_url"],
        "--team", item["target_team"],
        "--channel", re.sub(r"[^\w-]", "", item.get("source_channel", "Unknown")),
        "--date", item.get("source_date", "unknown"),
        "--league", item.get("target_league", ""),
        "--coach", item.get("coach_context", ""),
        "--use-cookies",
    ]

    logger.info("  [YT-02] Running transcript ingestion for %s", item["video_id"])
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if result.returncode == 0:
            output_path = TRANSCRIPTS_DIR / f"{basename}_transcript_raw.md"
            return True, str(output_path)
        else:
            logger.warning("  [YT-02] Failed: %s", result.stderr[:200])
            return False, f"yt02_failed: {result.stderr[:100]}"
    except subprocess.TimeoutExpired:
        return False, "yt02_timeout"
    except Exception as e:
        return False, f"yt02_error: {e}"


def run_yt03(item: dict[str, Any], basename: str) -> tuple[bool, str, int]:
    """
    Run YT-03 claim extraction.
    Returns (success, output_path_or_error, claims_count).
    Note: YT-03 requires LLM for actual claim extraction.
    This function prepares the transcript and signals that LLM extraction is needed.
    """
    transcript_path = TRANSCRIPTS_DIR / f"{basename}_transcript_raw.md"
    if not transcript_path.exists():
        return False, "transcript_not_found", 0

    script = PROJECT_BASE / "src" / "skills" / "transcript-to-tactical-claims" / "scripts" / "extract_claims.py"
    venv_python = PROJECT_BASE / "venv" / "bin" / "python"

    # Check if transcript is valid and print cleaned text
    cmd = [
        str(venv_python), str(script),
        "--transcript", str(transcript_path),
        "--team", item["target_team"],
        "--print-cleaned",
    ]

    logger.info("  [YT-03] Validating transcript for claim extraction: %s", basename)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            # Transcript is valid — in agent mode, LLM would extract claims here
            # In script mode, we signal that manual/LLM extraction is needed
            logger.info("  [YT-03] Transcript validated. LLM claim extraction required.")
            return True, f"transcript_ready_for_extraction:{transcript_path}", 0
        else:
            return False, f"yt03_validation_failed: {result.stderr[:100]}", 0
    except Exception as e:
        return False, f"yt03_error: {e}", 0


# ── Item processor ────────────────────────────────────────────────────────────


def process_item(
    item: dict[str, Any],
    run_mode: str,
    dry_run: bool,
) -> dict[str, Any]:
    """Process a single queue item through the pipeline."""
    video_id = item.get("video_id", "UNKNOWN")
    basename = build_canonical_basename(item)

    result: dict[str, Any] = {
        "item_id": video_id,
        "source_url": item.get("source_url", ""),
        "video_id": video_id,
        "target_team": item.get("target_team", ""),
        "source_channel": item.get("source_channel", ""),
        "source_tier": item.get("source_tier", ""),
        "status": "pending",
        "skip_reason": None,
        "stage_results": {
            "transcript": "pending",
            "claims": "pending",
            "validation": "pending",
            "patch_proposal": "pending",
            "candidate_apply": "pending",
        },
        "outputs": {
            "transcript_file": None,
            "claims_file": None,
            "validation_file": None,
            "patch_proposal_file": None,
            "apply_log": None,
        },
        "counts": {
            "claims_total": 0,
            "validated": 0,
            "rejected": 0,
            "needs_review": 0,
            "candidate_after_review": 0,
        },
        "notes": "",
    }

    # ── Eligibility check ──────────────────────────────────────────────────
    is_eligible, skip_reason = check_eligibility(item)
    if not is_eligible:
        result["status"] = "skipped"
        result["skip_reason"] = skip_reason
        result["stage_results"] = {k: "skipped" for k in result["stage_results"]}
        logger.info("  SKIP: %s — %s", video_id, skip_reason)
        return result

    # ── Idempotency check ──────────────────────────────────────────────────
    existing = check_existing_outputs(basename)

    if run_mode == "default" and existing["transcript"]:
        result["status"] = "already_exists"
        result["skip_reason"] = "already_exists"
        result["stage_results"]["transcript"] = "already_exists"
        result["outputs"]["transcript_file"] = str(TRANSCRIPTS_DIR / f"{basename}_transcript_raw.md")
        if existing["claims"]:
            result["stage_results"]["claims"] = "already_exists"
            result["outputs"]["claims_file"] = str(CLAIMS_DIR / f"{basename}_claims.md")
        if existing["validation"]:
            result["stage_results"]["validation"] = "already_exists"
            result["outputs"]["validation_file"] = str(VALIDATION_DIR / f"{basename}_validation.md")
        if existing["patch_proposal"]:
            result["stage_results"]["patch_proposal"] = "already_exists"
            result["outputs"]["patch_proposal_file"] = str(PATCH_PROPOSALS_DIR / f"{basename}_team_archive_patch_proposal.md")
        logger.info("  ALREADY_EXISTS: %s — skipping (use --reuse-existing or --force to override)", video_id)
        return result

    if dry_run:
        result["status"] = "dry_run_eligible"
        result["notes"] = "Eligible for processing. Dry run — no execution."
        logger.info("  DRY_RUN: %s — eligible, would process", video_id)
        return result

    # ── YT-02: Transcript ingestion ────────────────────────────────────────
    if run_mode == "reuse_existing" and existing["transcript"]:
        logger.info("  [YT-02] Reusing existing transcript: %s", basename)
        result["stage_results"]["transcript"] = "reused"
        result["outputs"]["transcript_file"] = str(TRANSCRIPTS_DIR / f"{basename}_transcript_raw.md")
        yt02_success = True
    else:
        yt02_success, yt02_output = run_yt02(item, basename)
        if yt02_success:
            result["stage_results"]["transcript"] = "success"
            result["outputs"]["transcript_file"] = yt02_output
        else:
            result["stage_results"]["transcript"] = "blocked"
            result["status"] = "blocked"
            result["notes"] = f"YT-02 blocked: {yt02_output}"
            result["stage_results"]["claims"] = "skipped"
            result["stage_results"]["validation"] = "skipped"
            result["stage_results"]["patch_proposal"] = "skipped"
            result["stage_results"]["candidate_apply"] = "skipped"
            logger.warning("  BLOCKED at YT-02: %s", video_id)
            return result

    # ── YT-03: Claim extraction ────────────────────────────────────────────
    if run_mode == "reuse_existing" and existing["claims"]:
        logger.info("  [YT-03] Reusing existing claims: %s", basename)
        result["stage_results"]["claims"] = "reused"
        result["outputs"]["claims_file"] = str(CLAIMS_DIR / f"{basename}_claims.md")
        yt03_success = True
    else:
        yt03_success, yt03_output, claims_count = run_yt03(item, basename)
        if yt03_success:
            result["stage_results"]["claims"] = "transcript_ready"
            result["notes"] += " YT-03: transcript validated, LLM claim extraction required for full pipeline."
            logger.info("  [YT-03] Transcript ready. LLM extraction needed for %s", video_id)
        else:
            result["stage_results"]["claims"] = "blocked"
            result["status"] = "partial"
            result["notes"] += f" YT-03 blocked: {yt03_output}"
            result["stage_results"]["validation"] = "skipped"
            result["stage_results"]["patch_proposal"] = "skipped"
            result["stage_results"]["candidate_apply"] = "skipped"
            return result

    # ── YT-04/05/06: Require LLM in agent mode ────────────────────────────
    # In script mode, these stages require LLM (agent) execution
    # Mark as pending_agent_execution
    if not existing["claims"]:
        result["stage_results"]["validation"] = "pending_agent_execution"
        result["stage_results"]["patch_proposal"] = "pending_agent_execution"
        result["stage_results"]["candidate_apply"] = "pending_agent_execution"
        result["status"] = "partial"
        result["notes"] += " YT-04/05/06 require agent (LLM) execution."
    else:
        # Claims exist — check validation
        if run_mode == "reuse_existing" and existing["validation"]:
            result["stage_results"]["validation"] = "reused"
            result["outputs"]["validation_file"] = str(VALIDATION_DIR / f"{basename}_validation.md")
        if run_mode == "reuse_existing" and existing["patch_proposal"]:
            result["stage_results"]["patch_proposal"] = "reused"
            result["outputs"]["patch_proposal_file"] = str(PATCH_PROPOSALS_DIR / f"{basename}_team_archive_patch_proposal.md")
        result["status"] = "partial"
        result["notes"] += " Reused existing outputs where available."

    if result["status"] == "pending":
        result["status"] = "partial"

    return result


# ── Batch runner ──────────────────────────────────────────────────────────────


def run_batch(
    queue_path: Path,
    run_mode: str,
    dry_run: bool,
    max_items: int | None,
) -> dict[str, Any]:
    """Run the batch pipeline for all items in the queue."""
    BATCH_RUNS_DIR.mkdir(parents=True, exist_ok=True)

    # Load queue
    with open(queue_path, encoding="utf-8") as f:
        queue_data = json.load(f)

    metadata = queue_data.get("queue_metadata", {})
    items = queue_data.get("items", [])

    if max_items:
        items = items[:max_items]

    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    team = metadata.get("target_team", "Unknown")
    queue_basename = queue_path.stem[:30]
    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%d')}_{team}_{queue_basename[-20:]}"

    logger.info("=" * 60)
    logger.info("Batch Run: %s", run_id)
    logger.info("Queue: %s (%d items)", queue_path.name, len(items))
    logger.info("Mode: %s | Dry run: %s", run_mode, dry_run)
    logger.info("=" * 60)

    item_results = []
    summary = {
        "total_items": len(items),
        "eligible_items": 0,
        "skipped_items": 0,
        "already_exists_items": 0,
        "reused_items": 0,
        "processed_items": 0,
        "failed_items": 0,
        "success_items": 0,
        "auto_applied_items": 0,
        "manual_review_items": 0,
    }

    for i, item in enumerate(items, 1):
        video_id = item.get("video_id", f"item_{i}")
        logger.info("\n[%d/%d] Processing: %s", i, len(items), video_id)

        result = process_item(item, run_mode, dry_run)
        item_results.append(result)

        # Update summary
        status = result["status"]
        if status == "skipped":
            summary["skipped_items"] += 1
        elif status == "already_exists":
            summary["already_exists_items"] += 1
        elif status in ("success", "partial", "dry_run_eligible", "transcript_ready"):
            summary["eligible_items"] += 1
            summary["processed_items"] += 1
            if status == "success":
                summary["success_items"] += 1
        elif status == "blocked":
            summary["eligible_items"] += 1
            summary["failed_items"] += 1

        if "reused" in str(result.get("stage_results", {})):
            summary["reused_items"] += 1

        logger.info("  → Status: %s", status)

    # Build report
    report = {
        "run_id": run_id,
        "run_date": run_date,
        "queue_file": queue_path.name,
        "fixture_type": metadata.get("fixture_type", "unknown"),
        "runner_version": "youtube-tactical-batch-runner v1.0",
        "run_mode": run_mode,
        "summary": summary,
        "items": item_results,
    }

    # Write JSON report
    json_report_path = BATCH_RUNS_DIR / f"{run_id}_batch_run_report.json"
    with open(json_report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    # Write Markdown report
    md_report_path = BATCH_RUNS_DIR / f"{run_id}_batch_run_report.md"
    md_content = build_markdown_report(report)
    md_report_path.write_text(md_content, encoding="utf-8")

    logger.info("\n" + "=" * 60)
    logger.info("Batch complete: %s", run_id)
    logger.info("  Total: %d | Eligible: %d | Skipped: %d | Already exists: %d",
                summary["total_items"], summary["eligible_items"],
                summary["skipped_items"], summary["already_exists_items"])
    logger.info("  Processed: %d | Success: %d | Failed: %d",
                summary["processed_items"], summary["success_items"], summary["failed_items"])
    logger.info("Report: %s", md_report_path)

    return report


def build_markdown_report(report: dict[str, Any]) -> str:
    """Build markdown batch run report."""
    s = report["summary"]
    lines = [
        f"---",
        f"run_id: {report['run_id']}",
        f"run_date: {report['run_date']}",
        f"queue_file: {report['queue_file']}",
        f"fixture_type: {report['fixture_type']}",
        f"runner_version: {report['runner_version']}",
        f"run_mode: {report['run_mode']}",
        f"total_items: {s['total_items']}",
        f"eligible_items: {s['eligible_items']}",
        f"skipped_items: {s['skipped_items']}",
        f"already_exists_items: {s['already_exists_items']}",
        f"reused_items: {s['reused_items']}",
        f"processed_items: {s['processed_items']}",
        f"failed_items: {s['failed_items']}",
        f"success_items: {s['success_items']}",
        f"auto_applied_items: {s['auto_applied_items']}",
        f"manual_review_items: {s['manual_review_items']}",
        f"---",
        f"",
        f"# Batch Run Report — {report['queue_file']} — {report['run_date']}",
        f"",
        f"## Run Summary",
        f"",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Run ID | {report['run_id']} |",
        f"| Queue File | {report['queue_file']} |",
        f"| Fixture Type | {report['fixture_type']} |",
        f"| Run Mode | {report['run_mode']} |",
        f"| Total Items | {s['total_items']} |",
        f"| Eligible | {s['eligible_items']} |",
        f"| Skipped | {s['skipped_items']} |",
        f"| Already Exists | {s['already_exists_items']} |",
        f"| Reused | {s['reused_items']} |",
        f"| Processed | {s['processed_items']} |",
        f"| Success | {s['success_items']} |",
        f"| Failed | {s['failed_items']} |",
        f"| Auto Applied | {s['auto_applied_items']} |",
        f"| Manual Review | {s['manual_review_items']} |",
        f"",
        f"---",
        f"",
        f"## Item Results",
        f"",
    ]

    for i, item in enumerate(report["items"], 1):
        sr = item.get("stage_results", {})
        lines += [
            f"### Item {i:03d} — {item['video_id']}",
            f"",
            f"| Field | Value |",
            f"|-------|-------|",
            f"| Source URL | {item['source_url']} |",
            f"| Video ID | {item['video_id']} |",
            f"| Target Team | {item['target_team']} |",
            f"| Channel | {item['source_channel']} |",
            f"| Source Tier | {item['source_tier']} |",
            f"| Status | **{item['status']}** |",
            f"| Skip Reason | {item.get('skip_reason') or 'none'} |",
            f"",
            f"**Stage Results**:",
            f"",
            f"| Stage | Status | Output |",
            f"|-------|--------|--------|",
            f"| YT-02 transcript | {sr.get('transcript', '-')} | {item['outputs'].get('transcript_file') or '-'} |",
            f"| YT-03 claims | {sr.get('claims', '-')} | {item['outputs'].get('claims_file') or '-'} |",
            f"| YT-04 validation | {sr.get('validation', '-')} | {item['outputs'].get('validation_file') or '-'} |",
            f"| YT-05 patch proposal | {sr.get('patch_proposal', '-')} | {item['outputs'].get('patch_proposal_file') or '-'} |",
            f"| YT-06 auto-apply | {sr.get('candidate_apply', '-')} | {item['outputs'].get('apply_log') or '-'} |",
            f"",
        ]
        if item.get("notes"):
            lines += [f"**Notes**: {item['notes']}", f""]
        lines.append("---")
        lines.append("")

    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(
        description="YouTube Tactical Intelligence Batch Runner"
    )
    parser.add_argument("--queue", required=True, help="Path to ingestion_queue.json")
    parser.add_argument("--reuse-existing", action="store_true",
                        help="Reuse existing outputs, only rerun missing stages")
    parser.add_argument("--force", action="store_true",
                        help="Force rerun all stages (overwrite existing outputs)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Check eligibility only, no execution")
    parser.add_argument("--max-items", type=int, default=None,
                        help="Maximum number of items to process")
    parser.add_argument("--vault-path", default=None, help="Override ARES_VAULT_PATH")
    args = parser.parse_args()

    if args.vault_path:
        global VAULT_BASE, TRANSCRIPTS_DIR, CLAIMS_DIR, VALIDATION_DIR
        global PATCH_PROPOSALS_DIR, BATCH_RUNS_DIR
        VAULT_BASE = Path(args.vault_path)
        TRANSCRIPTS_DIR = VAULT_BASE / "04_RAG_Raw_Data" / "youtube_tactical_sources" / "transcripts"
        CLAIMS_DIR = VAULT_BASE / "04_RAG_Raw_Data" / "youtube_tactical_sources" / "claims"
        VALIDATION_DIR = VAULT_BASE / "04_RAG_Raw_Data" / "youtube_tactical_sources" / "validation"
        PATCH_PROPOSALS_DIR = VAULT_BASE / "04_RAG_Raw_Data" / "youtube_tactical_sources" / "patch_proposals"
        BATCH_RUNS_DIR = VAULT_BASE / "04_RAG_Raw_Data" / "youtube_tactical_sources" / "batch_runs"

    # Determine run mode
    if args.force:
        run_mode = "force"
    elif args.reuse_existing:
        run_mode = "reuse_existing"
    else:
        run_mode = "default"

    queue_path = Path(args.queue)
    if not queue_path.exists():
        logger.error("Queue file not found: %s", queue_path)
        return 1

    report = run_batch(
        queue_path=queue_path,
        run_mode=run_mode,
        dry_run=args.dry_run,
        max_items=args.max_items,
    )

    return 0 if report["summary"]["failed_items"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
