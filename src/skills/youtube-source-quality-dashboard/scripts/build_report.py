#!/usr/bin/env python3
"""
build_report.py — YouTube Source Quality Dashboard
Skill: youtube-source-quality-dashboard v1.0
Issue: LMW-133

Reads existing pipeline artifacts and generates source quality metrics.
Does NOT modify any artifacts, Team Archive, or source registry.

Usage:
    ./venv/bin/python src/skills/youtube-source-quality-dashboard/scripts/build_report.py
    ./venv/bin/python src/skills/youtube-source-quality-dashboard/scripts/build_report.py --label "2026-05-27"
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("build_report")

VAULT_BASE = Path(
    os.environ.get("ARES_VAULT_PATH", "/Users/liumingwei/vaults/AresVault")
)
YT_BASE = VAULT_BASE / "04_RAG_Raw_Data" / "youtube_tactical_sources"

QUEUE_DIR = YT_BASE / "ingestion_queue"
TRANSCRIPTS_DIR = YT_BASE / "transcripts"
BLOCKED_DIR = YT_BASE / "transcripts" / "blocked"
CLAIMS_DIR = YT_BASE / "claims"
VALIDATION_DIR = YT_BASE / "validation"
PATCH_PROPOSALS_DIR = YT_BASE / "patch_proposals"
BATCH_RUNS_DIR = YT_BASE / "batch_runs"
QUALITY_REPORTS_DIR = YT_BASE / "quality_reports"


# ── Parsers ───────────────────────────────────────────────────────────────────

def parse_frontmatter(content: str) -> dict[str, Any]:
    """Extract YAML frontmatter from markdown."""
    if not content.startswith("---"):
        return {}
    end = content.find("\n---", 3)
    if end == -1:
        return {}
    fm_text = content[3:end].strip()
    fm: dict[str, Any] = {}
    current_key = None
    current_list: list | None = None
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
    return fm


def count_claims_in_file(path: Path) -> dict[str, Any]:
    """Count claims and their types/confidence from a claims.md file."""
    content = path.read_text(encoding="utf-8", errors="replace")
    fm = parse_frontmatter(content)

    total = int(fm.get("total_claims", 0))
    claims_by_type_raw = fm.get("claims_by_type", {})

    # Parse claims_by_type — handle both dict and multiline YAML block
    claims_by_type: dict[str, int] = {}
    if isinstance(claims_by_type_raw, dict):
        for k, v in claims_by_type_raw.items():
            try:
                claims_by_type[k] = int(v)
            except (ValueError, TypeError):
                pass
    elif isinstance(claims_by_type_raw, list):
        # Fallback: parse from raw content using regex
        for match in re.finditer(r"^\s{2}(\w+):\s*(\d+)", content, re.MULTILINE):
            claims_by_type[match.group(1)] = int(match.group(2))

    # Count confidence levels from body
    high = len(re.findall(r"\*\*Confidence\*\*:\s*`high`", content))
    medium = len(re.findall(r"\*\*Confidence\*\*:\s*`medium`", content))
    low = len(re.findall(r"\*\*Confidence\*\*:\s*`low`", content))

    return {
        "total": total,
        "by_type": claims_by_type,
        "high": high,
        "medium": medium,
        "low": low,
        "source_channel": fm.get("source_channel", "unknown"),
        "target_team": fm.get("target_team", "unknown"),
        "video_id": fm.get("video_id", "unknown"),
        "language": fm.get("language", "unknown"),
    }


def count_validation_in_file(path: Path) -> dict[str, Any]:
    """Count validation results from a validation.md file."""
    content = path.read_text(encoding="utf-8", errors="replace")
    fm = parse_frontmatter(content)

    summary = fm.get("validation_summary", {})
    if isinstance(summary, dict):
        validated = int(summary.get("validated", 0))
        rejected = int(summary.get("rejected", 0))
        needs_review = int(summary.get("needs_review", 0))
    else:
        validated = rejected = needs_review = 0

    reason_raw = fm.get("reason_code_summary", {})
    reason_codes: dict[str, int] = {}
    if isinstance(reason_raw, dict):
        for k, v in reason_raw.items():
            try:
                reason_codes[k] = int(v)
            except (ValueError, TypeError):
                pass

    # Count candidate_after_review from body
    candidates = len(re.findall(r"`candidate_after_review`", content))

    return {
        "validated": validated,
        "rejected": rejected,
        "needs_review": needs_review,
        "total": validated + rejected + needs_review,
        "reason_codes": reason_codes,
        "candidate_after_review": candidates,
        "source_channel": fm.get("source_channel", "unknown"),
        "target_team": fm.get("target_team", "unknown"),
        "video_id": fm.get("video_id", "unknown"),
    }


def get_transcript_info(path: Path) -> dict[str, Any]:
    """Extract metadata from a transcript_raw.md file."""
    content = path.read_text(encoding="utf-8", errors="replace")
    fm = parse_frontmatter(content)
    # Estimate word count from body
    body_start = content.find("\n---", 3)
    body = content[body_start + 4:] if body_start != -1 else content
    word_count = len(body.split())
    return {
        "source_channel": fm.get("source_channel", "unknown"),
        "target_team": fm.get("target_team", "unknown"),
        "video_id": fm.get("video_id", "unknown"),
        "language": fm.get("language", "unknown"),
        "transcript_source": fm.get("transcript_source", "unknown"),
        "word_count": word_count,
        "status": "success",
    }


def get_blocked_info(path: Path) -> dict[str, Any]:
    """Extract metadata from a transcript_blocked.md file."""
    content = path.read_text(encoding="utf-8", errors="replace")
    fm = parse_frontmatter(content)
    return {
        "source_channel": fm.get("source_channel", "unknown"),
        "target_team": fm.get("target_team", "unknown"),
        "video_id": fm.get("video_id", "unknown"),
        "failure_reason": fm.get("failure_reason", "unknown"),
        "status": "blocked",
    }


# ── Metrics builder ───────────────────────────────────────────────────────────

def build_metrics(vault_path: Path, min_sample: int) -> dict[str, Any]:
    """Build all quality metrics from available artifacts."""
    global VAULT_BASE, YT_BASE, QUEUE_DIR, TRANSCRIPTS_DIR, BLOCKED_DIR
    global CLAIMS_DIR, VALIDATION_DIR, PATCH_PROPOSALS_DIR, BATCH_RUNS_DIR

    if vault_path != VAULT_BASE:
        VAULT_BASE = vault_path
        YT_BASE = VAULT_BASE / "04_RAG_Raw_Data" / "youtube_tactical_sources"
        QUEUE_DIR = YT_BASE / "ingestion_queue"
        TRANSCRIPTS_DIR = YT_BASE / "transcripts"
        BLOCKED_DIR = YT_BASE / "transcripts" / "blocked"
        CLAIMS_DIR = YT_BASE / "claims"
        VALIDATION_DIR = YT_BASE / "validation"
        PATCH_PROPOSALS_DIR = YT_BASE / "patch_proposals"
        BATCH_RUNS_DIR = YT_BASE / "batch_runs"

    metrics: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "vault_base": str(vault_path),
        "sample_size_warning": f"Sample size is small (min_sample={min_sample}). Recommendations are conservative.",
    }

    # ── Pipeline volume from batch run reports ────────────────────────────────
    pipeline = {
        "total_queue_items": 0,
        "eligible_items": 0,
        "skipped_items": 0,
        "processed_items": 0,
        "blocked_items": 0,
        "success_items": 0,
        "already_exists_items": 0,
    }

    # Count from queue files
    for qf in QUEUE_DIR.glob("*.json"):
        try:
            data = json.loads(qf.read_text(encoding="utf-8"))
            items = data.get("items", [])
            pipeline["total_queue_items"] += len(items)
            for item in items:
                if (item.get("ingestion_status") == "queued" and
                        item.get("recommended_next_step") == "transcript_ingestion" and
                        item.get("source_url", "") not in ("", "PENDING_USER_LOOKUP")):
                    pipeline["eligible_items"] += 1
                else:
                    pipeline["skipped_items"] += 1
        except Exception:
            pass

    # Count from batch run reports
    for bf in BATCH_RUNS_DIR.glob("*.json"):
        try:
            data = json.loads(bf.read_text(encoding="utf-8"))
            s = data.get("summary", {})
            pipeline["processed_items"] = max(pipeline["processed_items"], s.get("processed_items", 0))
            pipeline["blocked_items"] += s.get("failed_items", 0)
            pipeline["success_items"] += s.get("success_items", 0)
            pipeline["already_exists_items"] += s.get("already_exists_items", 0)
        except Exception:
            pass

    metrics["pipeline_volume"] = pipeline

    # ── Transcript quality ────────────────────────────────────────────────────
    transcripts = []
    for tf in TRANSCRIPTS_DIR.glob("*_transcript_raw.md"):
        transcripts.append(get_transcript_info(tf))

    blocked_transcripts = []
    if BLOCKED_DIR.exists():
        for bf in BLOCKED_DIR.glob("*_transcript_blocked.md"):
            blocked_transcripts.append(get_blocked_info(bf))

    lang_dist: dict[str, int] = defaultdict(int)
    word_counts = []
    for t in transcripts:
        lang_dist[t["language"]] += 1
        word_counts.append(t["word_count"])

    metrics["transcript_quality"] = {
        "transcript_success_count": len(transcripts),
        "transcript_blocked_count": len(blocked_transcripts),
        "transcript_language_distribution": dict(lang_dist),
        "auto_caption_count": sum(1 for t in transcripts if "yt_dlp" in t.get("transcript_source", "")),
        "average_cleaned_word_count": int(sum(word_counts) / len(word_counts)) if word_counts else 0,
    }

    # ── Claim quality ─────────────────────────────────────────────────────────
    all_claims: list[dict] = []
    claims_by_type_total: dict[str, int] = defaultdict(int)
    for cf in CLAIMS_DIR.glob("*_claims.md"):
        info = count_claims_in_file(cf)
        all_claims.append(info)
        for k, v in info["by_type"].items():
            claims_by_type_total[k] += v

    total_claims = sum(c["total"] for c in all_claims)
    metrics["claim_quality"] = {
        "claims_total": total_claims,
        "claims_by_type": dict(claims_by_type_total),
        "average_claims_per_successful_transcript": round(total_claims / len(transcripts), 1) if transcripts else 0,
        "high_confidence_claims": sum(c["high"] for c in all_claims),
        "medium_confidence_claims": sum(c["medium"] for c in all_claims),
        "low_confidence_claims": sum(c["low"] for c in all_claims),
    }

    # ── Validation quality ────────────────────────────────────────────────────
    all_validations: list[dict] = []
    reason_codes_total: dict[str, int] = defaultdict(int)
    for vf in VALIDATION_DIR.glob("*_validation.md"):
        # Skip old-format files that don't follow the new schema
        content = vf.read_text(encoding="utf-8", errors="replace")
        if "validation_summary:" not in content:
            continue
        info = count_validation_in_file(vf)
        all_validations.append(info)
        for k, v in info["reason_codes"].items():
            reason_codes_total[k] += v

    total_val = sum(v["total"] for v in all_validations)
    total_validated = sum(v["validated"] for v in all_validations)
    total_rejected = sum(v["rejected"] for v in all_validations)
    total_needs_review = sum(v["needs_review"] for v in all_validations)
    total_candidates = sum(v["candidate_after_review"] for v in all_validations)

    metrics["validation_quality"] = {
        "validated_count": total_validated,
        "rejected_count": total_rejected,
        "needs_review_count": total_needs_review,
        "validated_rate": round(total_validated / total_val, 2) if total_val else 0,
        "rejected_rate": round(total_rejected / total_val, 2) if total_val else 0,
        "needs_review_rate": round(total_needs_review / total_val, 2) if total_val else 0,
        "reason_code_distribution": dict(reason_codes_total),
    }

    # ── Team Archive value ────────────────────────────────────────────────────
    patch_count = len(list(PATCH_PROPOSALS_DIR.glob("*_team_archive_patch_proposal.md")))
    metrics["team_archive_value"] = {
        "candidate_after_review_count": total_candidates,
        "patch_proposal_count": patch_count,
        "auto_applied_count": 3,  # ARS_YT_001/002/003 applied in LMW-129
        "manual_review_required_count": max(0, total_candidates - 3),
        "archive_write_rate": round(3 / total_candidates, 2) if total_candidates else 0,
    }

    # ── Source performance ────────────────────────────────────────────────────
    channel_stats: dict[str, dict] = {}

    # Aggregate by channel from transcripts
    for t in transcripts:
        ch = t["source_channel"]
        if ch not in channel_stats:
            channel_stats[ch] = {
                "source_channel": ch,
                "source_tier": "tier_1" if ch in ("HALF-SPACE THEORY", "HALF-SPACE-THEORY") else "tier_2",
                "videos_seen": 0,
                "transcript_success": 0,
                "transcript_blocked": 0,
                "total_claims": 0,
                "validated_claims": 0,
                "rejected_claims": 0,
                "candidate_claims": 0,
            }
        channel_stats[ch]["videos_seen"] += 1
        channel_stats[ch]["transcript_success"] += 1

    for b in blocked_transcripts:
        ch = b["source_channel"]
        if ch not in channel_stats:
            channel_stats[ch] = {
                "source_channel": ch,
                "source_tier": "tier_1" if ch in ("HALF-SPACE THEORY", "HALF-SPACE-THEORY") else "tier_2",
                "videos_seen": 0,
                "transcript_success": 0,
                "transcript_blocked": 0,
                "total_claims": 0,
                "validated_claims": 0,
                "rejected_claims": 0,
                "candidate_claims": 0,
            }
        channel_stats[ch]["videos_seen"] += 1
        channel_stats[ch]["transcript_blocked"] += 1

    for c in all_claims:
        ch = c["source_channel"]
        if ch in channel_stats:
            channel_stats[ch]["total_claims"] += c["total"]

    for v in all_validations:
        ch = v["source_channel"]
        if ch in channel_stats:
            channel_stats[ch]["validated_claims"] += v["validated"]
            channel_stats[ch]["rejected_claims"] += v["rejected"]
            channel_stats[ch]["candidate_claims"] += v["candidate_after_review"]

    # Compute rates and recommendations
    source_performance = []
    for ch, s in channel_stats.items():
        videos = s["videos_seen"]
        ts_rate = round(s["transcript_success"] / videos, 2) if videos else 0
        avg_claims = round(s["total_claims"] / max(s["transcript_success"], 1), 1)
        val_rate = round(s["validated_claims"] / max(s["total_claims"], 1), 2)
        blocked_rate = round(s["transcript_blocked"] / videos, 2) if videos else 0
        cand_rate = round(s["candidate_claims"] / max(s["validated_claims"], 1), 2)

        # Recommendation logic
        if videos < min_sample:
            rec = "needs_more_sample"
            notes = f"Only {videos} video(s) processed. Need ≥{min_sample} for reliable assessment."
        elif blocked_rate > 0.5:
            rec = "watch"
            notes = f"High blocked rate ({blocked_rate:.0%}). May be network/availability issue, not source quality."
        elif ts_rate >= 0.8 and avg_claims >= 5 and val_rate >= 0.5:
            rec = "keep"
            notes = "High transcript success, good claim density, strong validation rate."
        elif ts_rate >= 0.5 and avg_claims >= 3:
            rec = "watch"
            notes = "Promising but needs more samples to confirm quality."
        else:
            rec = "watch"
            notes = "Insufficient data for strong recommendation."

        source_performance.append({
            "source_channel": ch,
            "source_tier": s["source_tier"],
            "videos_seen": videos,
            "transcript_success_rate": ts_rate,
            "average_claims_per_video": avg_claims,
            "validated_rate": val_rate,
            "candidate_after_review_rate": cand_rate,
            "blocked_rate": blocked_rate,
            "notes": notes,
            "recommendation": rec,
        })

    metrics["source_performance"] = sorted(source_performance, key=lambda x: x["videos_seen"], reverse=True)

    return metrics


# ── Report builders ───────────────────────────────────────────────────────────

def build_markdown_report(metrics: dict[str, Any], label: str) -> str:
    pv = metrics["pipeline_volume"]
    tq = metrics["transcript_quality"]
    cq = metrics["claim_quality"]
    vq = metrics["validation_quality"]
    ta = metrics["team_archive_value"]
    sp = metrics["source_performance"]

    lines = [
        f"---",
        f"report_type: youtube_source_quality_dashboard",
        f"report_label: {label}",
        f"generated_at: {metrics['generated_at']}",
        f"sample_size_warning: \"{metrics['sample_size_warning']}\"",
        f"---",
        f"",
        f"# YouTube Source Quality Dashboard — {label}",
        f"",
        f"> ⚠️ **{metrics['sample_size_warning']}**",
        f"",
        f"---",
        f"",
        f"## Pipeline Volume",
        f"",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Total Queue Items | {pv['total_queue_items']} |",
        f"| Eligible Items | {pv['eligible_items']} |",
        f"| Skipped Items | {pv['skipped_items']} |",
        f"| Processed Items | {pv['processed_items']} |",
        f"| Blocked Items | {pv['blocked_items']} |",
        f"| Success Items | {pv['success_items']} |",
        f"| Already Exists | {pv['already_exists_items']} |",
        f"",
        f"---",
        f"",
        f"## Transcript Quality",
        f"",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Transcript Success | {tq['transcript_success_count']} |",
        f"| Transcript Blocked | {tq['transcript_blocked_count']} |",
        f"| Auto Caption | {tq['auto_caption_count']} |",
        f"| Avg Cleaned Word Count | {tq['average_cleaned_word_count']} |",
        f"| Language Distribution | {tq['transcript_language_distribution']} |",
        f"",
        f"---",
        f"",
        f"## Claim Quality",
        f"",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Claims Total | {cq['claims_total']} |",
        f"| Avg Claims / Transcript | {cq['average_claims_per_successful_transcript']} |",
        f"| High Confidence | {cq['high_confidence_claims']} |",
        f"| Medium Confidence | {cq['medium_confidence_claims']} |",
        f"| Low Confidence | {cq['low_confidence_claims']} |",
        f"",
        f"**Claims by Type**:",
        f"",
        f"| Type | Count |",
        f"|------|-------|",
    ]
    for k, v in sorted(cq["claims_by_type"].items()):
        lines.append(f"| `{k}` | {v} |")

    lines += [
        f"",
        f"---",
        f"",
        f"## Validation Quality",
        f"",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Validated | {vq['validated_count']} ({vq['validated_rate']:.0%}) |",
        f"| Rejected | {vq['rejected_count']} ({vq['rejected_rate']:.0%}) |",
        f"| Needs Review | {vq['needs_review_count']} ({vq['needs_review_rate']:.0%}) |",
        f"",
        f"**Reason Code Distribution**:",
        f"",
        f"| Reason Code | Count |",
        f"|-------------|-------|",
    ]
    for k, v in sorted(vq["reason_code_distribution"].items()):
        lines.append(f"| `{k}` | {v} |")

    lines += [
        f"",
        f"---",
        f"",
        f"## Team Archive Value",
        f"",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Candidate After Review | {ta['candidate_after_review_count']} |",
        f"| Patch Proposals | {ta['patch_proposal_count']} |",
        f"| Auto Applied | {ta['auto_applied_count']} |",
        f"| Manual Review Required | {ta['manual_review_required_count']} |",
        f"| Archive Write Rate | {ta['archive_write_rate']:.0%} |",
        f"",
        f"---",
        f"",
        f"## Source Performance",
        f"",
        f"| Channel | Tier | Videos | Transcript % | Avg Claims | Validated % | Candidate % | Blocked % | Recommendation |",
        f"|---------|------|--------|-------------|-----------|------------|------------|----------|----------------|",
    ]
    for s in sp:
        rec_emoji = {"keep": "✅", "watch": "⚠️", "downgrade": "⬇️", "exclude": "❌", "needs_more_sample": "🔍"}.get(s["recommendation"], "❓")
        lines.append(
            f"| {s['source_channel']} | {s['source_tier']} | {s['videos_seen']} "
            f"| {s['transcript_success_rate']:.0%} | {s['average_claims_per_video']} "
            f"| {s['validated_rate']:.0%} | {s['candidate_after_review_rate']:.0%} "
            f"| {s['blocked_rate']:.0%} | {rec_emoji} `{s['recommendation']}` |"
        )

    lines += [
        f"",
        f"### Source Notes",
        f"",
    ]
    for s in sp:
        lines.append(f"- **{s['source_channel']}**: {s['notes']}")

    lines += [
        f"",
        f"---",
        f"",
        f"## Sample Size Caveat",
        f"",
        f"> This report is based on a small sample ({sum(s['videos_seen'] for s in sp)} total videos processed).",
        f"> Recommendations should be treated as preliminary. Do not permanently downgrade or exclude",
        f"> Tier 1/Tier 2 sources based on this report alone.",
        f"",
        f"## Handoff Note",
        f"",
        f"This report is read-only. To act on recommendations:",
        f"- `keep` sources: continue adding to ingestion queues",
        f"- `watch` sources: add 1-2 more videos before deciding",
        f"- `needs_more_sample`: add more videos to queue",
        f"- Source tier changes require manual update to `规范 - Ares YouTube Channel Tier Registry v1.md`",
    ]

    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="YouTube Source Quality Dashboard")
    parser.add_argument("--label", default=None, help="Report label (default: today's date)")
    parser.add_argument("--min-sample", type=int, default=2, help="Min sample size for recommendations")
    parser.add_argument("--vault-path", default=None, help="Override ARES_VAULT_PATH")
    args = parser.parse_args()

    vault_path = Path(args.vault_path) if args.vault_path else VAULT_BASE
    label = args.label or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    QUALITY_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Building source quality report: %s", label)
    metrics = build_metrics(vault_path, args.min_sample)

    # Write JSON
    json_path = QUALITY_REPORTS_DIR / f"{label}_source_quality_report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    # Write Markdown
    md_path = QUALITY_REPORTS_DIR / f"{label}_source_quality_report.md"
    md_path.write_text(build_markdown_report(metrics, label), encoding="utf-8")

    logger.info("✅ Quality report written: %s", md_path)

    # Print summary
    sp = metrics["source_performance"]
    logger.info("Source performance summary:")
    for s in sp:
        logger.info("  %s (%s): %d videos, recommendation=%s",
                    s["source_channel"], s["source_tier"],
                    s["videos_seen"], s["recommendation"])

    return 0


if __name__ == "__main__":
    sys.exit(main())
