#!/usr/bin/env python3
"""
fetch_transcript.py — YouTube Transcript Ingestion Helper Script
Skill: youtube-transcript-ingestion v1.0
Issue: LMW-119

Usage:
    ./venv/bin/python src/skills/youtube-transcript-ingestion/scripts/fetch_transcript.py \\
        --url "https://www.youtube.com/watch?v=GxvSAS97L9c" \\
        --team "Arsenal" \\
        --channel "Tifo" \\
        --date "2026-05-22" \\
        --use-cookies

Guardrails:
    - Always uses --skip-download (no video/audio download)
    - Records used_browser_cookies in frontmatter
    - Generates blocked report on any failure
    - Does NOT extract tactical claims
    - Does NOT modify Team Archive
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("fetch_transcript")

# ── Vault paths ──────────────────────────────────────────────────────────────

VAULT_BASE = Path(
    os.environ.get("ARES_VAULT_PATH", "/Users/liumingwei/vaults/AresVault")
)
TRANSCRIPT_DIR = (
    VAULT_BASE / "04_RAG_Raw_Data" / "youtube_tactical_sources" / "transcripts"
)
BLOCKED_DIR = TRANSCRIPT_DIR / "blocked"
TMP_DIR = Path("/tmp")


# ── Helpers ───────────────────────────────────────────────────────────────────


def parse_video_id(url: str) -> str | None:
    """Extract YouTube video_id from various URL formats."""
    patterns = [
        r"(?:v=)([A-Za-z0-9_-]{11})",
        r"(?:youtu\.be/)([A-Za-z0-9_-]{11})",
        r"(?:shorts/)([A-Za-z0-9_-]{11})",
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


def build_output_basename(date: str, team: str, channel: str, video_id: str) -> str:
    """Build canonical output filename (without extension)."""
    # Sanitize components for filesystem safety
    team_safe = re.sub(r"[^\w]", "", team)
    channel_safe = re.sub(r"[^\w]", "", channel)
    return f"{date}_{team_safe}_{channel_safe}_{video_id}"


def run_ytdlp(
    url: str,
    video_id: str,
    use_cookies: bool,
    language_preference: str,
) -> tuple[bool, str, list[Path]]:
    """
    Run yt-dlp subtitle-only extraction.

    Returns:
        (success, failure_reason, subtitle_files)
    """
    tmp_output_template = str(TMP_DIR / f"ares_transcript_{video_id}.%(ext)s")

    cmd = [
        "yt-dlp",
        "--skip-download",
        "--write-subs",
        "--write-auto-subs",
        "--sub-langs", language_preference,
        "--sub-format", "vtt/srt/best",
        "--output", tmp_output_template,
    ]

    if use_cookies:
        cmd += ["--cookies-from-browser", "chrome"]

    cmd.append(url)

    logger.info("Running yt-dlp: %s", " ".join(cmd))

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        return False, "yt_dlp_failed", []
    except FileNotFoundError:
        return False, "yt_dlp_not_installed", []

    if result.returncode != 0:
        logger.warning("yt-dlp stderr: %s", result.stderr[:500])
        if "Sign in" in result.stderr or "cookies" in result.stderr.lower():
            return False, "cookie_auth_failed", []
        if "This video is not available" in result.stderr:
            return False, "access_restricted", []
        return False, "yt_dlp_failed", []

    # Find generated subtitle files
    subtitle_files = list(TMP_DIR.glob(f"ares_transcript_{video_id}.*"))
    subtitle_files = [
        f for f in subtitle_files
        if f.suffix in {".vtt", ".srt", ".ass", ".lrc"}
    ]

    if not subtitle_files:
        return False, "no_subtitles_available", []

    return True, "", subtitle_files


def parse_subtitle_file(subtitle_path: Path) -> tuple[str, bool]:
    """
    Parse subtitle file content.

    Returns:
        (transcript_text, include_timestamps)
    """
    content = subtitle_path.read_text(encoding="utf-8", errors="replace")

    if subtitle_path.suffix == ".vtt":
        # Remove VTT header
        lines = content.split("\n")
        cleaned = []
        for line in lines:
            if line.startswith("WEBVTT") or line.startswith("NOTE"):
                continue
            # Keep timestamp lines and text lines
            cleaned.append(line)
        return "\n".join(cleaned).strip(), True

    # SRT / other formats — return as-is
    return content.strip(), True


def build_frontmatter(
    video_id: str,
    source_url: str,
    target_team: str,
    target_league: str,
    coach_context: str,
    source_channel: str,
    transcript_source: str,
    extraction_method: str,
    language: str,
    include_timestamps: bool,
    used_browser_cookies: bool,
    created_at: str,
) -> str:
    timestamps_str = "true" if include_timestamps else "false"
    cookies_str = "true" if used_browser_cookies else "false"

    return f"""---
source_kind: youtube_video
video_id: {video_id}
source_url: {source_url}
target_team: {target_team}
target_league: {target_league}
coach_context: {coach_context}
source_channel: {source_channel}
transcript_source: {transcript_source}
extraction_method: {extraction_method}
language: {language}
include_timestamps: {timestamps_str}
used_browser_cookies: {cookies_str}
source_authority: raw_transcript
profile_authority: false
created_at: {created_at}
downstream_allowed:
  - claim_extraction
  - validation
downstream_forbidden:
  - direct_team_archive_patch
  - claim_extraction_during_ingestion
  - video_download
  - audio_download
---"""


def build_transcript_markdown(
    frontmatter: str,
    video_id: str,
    source_url: str,
    target_team: str,
    target_league: str,
    coach_context: str,
    source_channel: str,
    source_date: str,
    transcript_source: str,
    extraction_method: str,
    language: str,
    include_timestamps: bool,
    used_browser_cookies: bool,
    created_at: str,
    transcript_text: str,
) -> str:
    timestamps_str = "true" if include_timestamps else "false"
    cookies_str = "true" if used_browser_cookies else "false"
    word_count = len(transcript_text.split())
    line_count = len(transcript_text.splitlines())

    return f"""{frontmatter}

# Raw Transcript — {target_team} — {source_channel} — {video_id}

## Source Metadata

| Field | Value |
|-------|-------|
| Video URL | {source_url} |
| Video ID | {video_id} |
| Channel | {source_channel} |
| Target Team | {target_team} |
| Target League | {target_league} |
| Coach Context | {coach_context} |
| Published | {source_date} |

## Extraction Metadata

| Field | Value |
|-------|-------|
| Transcript Source | {transcript_source} |
| Extraction Method | {extraction_method} |
| Language | {language} |
| Timestamps Included | {timestamps_str} |
| Browser Cookies Used | {cookies_str} |
| Extracted At | {created_at} |

## ⚠️ Boundary Notice

> **This file is raw transcript material only.**
>
> - ✅ Allowed as input for: claim extraction (YT-03), validation (YT-04)
> - ❌ Must NOT directly patch Team Archive
> - ❌ Must NOT be treated as verified tactical memory
> - ❌ Must NOT be used to generate prematch conclusions without YT-04 validation

---

## Raw Transcript

{transcript_text}

---

## Extraction Notes

- Subtitle type: auto-generated / manual (see transcript_source)
- Language detected: {language}
- Timestamp format: HH:MM:SS (VTT/SRT)
- Total lines: ~{line_count}
- Estimated word count: ~{word_count}

## Handoff Note

Next step: YT-03 Transcript-to-Tactical-Claims Extraction
Input path: 04_RAG_Raw_Data/youtube_tactical_sources/transcripts/<this_file>
"""


def build_blocked_report(
    video_id: str,
    source_url: str,
    target_team: str,
    source_channel: str,
    attempted_method: str,
    used_browser_cookies: bool,
    failure_reason: str,
    blocked_at: str,
    failure_details: str,
) -> str:
    cookies_str = "true" if used_browser_cookies else "false"

    return f"""---
source_kind: youtube_video
video_id: {video_id}
source_url: {source_url}
target_team: {target_team}
source_channel: {source_channel}
blocked_at: {blocked_at}
attempted_method: {attempted_method}
used_browser_cookies: {cookies_str}
failure_reason: {failure_reason}
status: blocked
---

# Transcript Blocked Report — {target_team} — {source_channel} — {video_id}

## Blocked Summary

| Field | Value |
|-------|-------|
| Video URL | {source_url} |
| Video ID | {video_id} |
| Channel | {source_channel} |
| Target Team | {target_team} |
| Attempted Method | {attempted_method} |
| Browser Cookies Used | {cookies_str} |
| Failure Reason | {failure_reason} |
| Blocked At | {blocked_at} |

## Failure Details

{failure_details}

## Next Suggested Action

1. 在 YouTube 手动确认视频是否有字幕
2. 尝试不同语言参数：`--sub-langs "en.*,zh.*"`
3. 若确认无字幕，考虑 YT-02b NotebookLM secondary synthesis（optional fallback）
4. 或跳过此视频，寻找替代来源

## Note

> Transcript content was NOT guessed or fabricated.
> Truth > Completeness.
"""


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(
        description="YouTube Transcript Ingestion — yt-dlp subtitle-only extraction"
    )
    parser.add_argument("--url", required=True, help="YouTube video URL")
    parser.add_argument("--team", required=True, help="Target team (English standard name)")
    parser.add_argument("--channel", required=True, help="Source channel name")
    parser.add_argument("--date", required=True, help="Source date (YYYY-MM-DD)")
    parser.add_argument("--league", default="", help="Target league (EPL / La_liga / etc.)")
    parser.add_argument("--coach", default="", help="Coach context (optional)")
    parser.add_argument("--video-id", default=None, help="YouTube video_id (auto-parsed if omitted)")
    parser.add_argument("--language", default="en,zh-Hans,zh-Hant", help="Subtitle language preference")
    parser.add_argument("--use-cookies", action="store_true", help="Use Chrome browser cookies")
    parser.add_argument("--output-basename", default=None, help="Custom output basename (no extension)")
    parser.add_argument("--vault-path", default=None, help="Override ARES_VAULT_PATH")
    args = parser.parse_args()

    # Override vault path if provided
    if args.vault_path:
        global VAULT_BASE, TRANSCRIPT_DIR, BLOCKED_DIR
        VAULT_BASE = Path(args.vault_path)
        TRANSCRIPT_DIR = VAULT_BASE / "04_RAG_Raw_Data" / "youtube_tactical_sources" / "transcripts"
        BLOCKED_DIR = TRANSCRIPT_DIR / "blocked"

    # Ensure output directories exist
    TRANSCRIPT_DIR.mkdir(parents=True, exist_ok=True)
    BLOCKED_DIR.mkdir(parents=True, exist_ok=True)

    # Parse video_id
    video_id = args.video_id or parse_video_id(args.url)
    if not video_id:
        logger.error("Cannot parse video_id from URL: %s", args.url)
        video_id = "UNKNOWN"

    # Build output basename
    basename = args.output_basename or build_output_basename(
        args.date, args.team, args.channel, video_id
    )

    created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Build extraction method string for frontmatter
    extraction_method = (
        f"yt-dlp --skip-download --write-subs --write-auto-subs "
        f"--sub-langs {args.language!r} --sub-format vtt/srt/best"
        + (" --cookies-from-browser chrome" if args.use_cookies else "")
    )

    logger.info("Starting transcript extraction for: %s", args.url)
    logger.info("Target team: %s | Channel: %s | video_id: %s", args.team, args.channel, video_id)

    # Run yt-dlp
    success, failure_reason, subtitle_files = run_ytdlp(
        url=args.url,
        video_id=video_id,
        use_cookies=args.use_cookies,
        language_preference=args.language,
    )

    if not success:
        # Generate blocked report
        blocked_path = BLOCKED_DIR / f"{basename}_transcript_blocked.md"
        failure_details = f"Failure reason: `{failure_reason}`\n\nyt-dlp returned no usable subtitle files."

        blocked_content = build_blocked_report(
            video_id=video_id,
            source_url=args.url,
            target_team=args.team,
            source_channel=args.channel,
            attempted_method=extraction_method,
            used_browser_cookies=args.use_cookies,
            failure_reason=failure_reason,
            blocked_at=created_at,
            failure_details=failure_details,
        )

        blocked_path.write_text(blocked_content, encoding="utf-8")
        logger.warning("Extraction failed (%s). Blocked report: %s", failure_reason, blocked_path)
        return 1

    # Parse subtitle content
    subtitle_file = subtitle_files[0]
    transcript_text, include_timestamps = parse_subtitle_file(subtitle_file)

    if not transcript_text.strip():
        # Empty transcript → blocked report
        blocked_path = BLOCKED_DIR / f"{basename}_transcript_blocked.md"
        blocked_content = build_blocked_report(
            video_id=video_id,
            source_url=args.url,
            target_team=args.team,
            source_channel=args.channel,
            attempted_method=extraction_method,
            used_browser_cookies=args.use_cookies,
            failure_reason="empty_transcript",
            blocked_at=created_at,
            failure_details="yt-dlp succeeded but subtitle file is empty.",
        )
        blocked_path.write_text(blocked_content, encoding="utf-8")
        logger.warning("Empty transcript. Blocked report: %s", blocked_path)
        # Cleanup tmp
        subtitle_file.unlink(missing_ok=True)
        return 1

    # Detect language from filename
    lang_match = re.search(r"\.([a-z]{2}(?:-[A-Za-z]+)?)\.(?:vtt|srt)", subtitle_file.name)
    detected_language = lang_match.group(1) if lang_match else args.language.split(",")[0]

    # Build frontmatter
    frontmatter = build_frontmatter(
        video_id=video_id,
        source_url=args.url,
        target_team=args.team,
        target_league=args.league,
        coach_context=args.coach,
        source_channel=args.channel,
        transcript_source="yt_dlp_subtitles",
        extraction_method=extraction_method,
        language=detected_language,
        include_timestamps=include_timestamps,
        used_browser_cookies=args.use_cookies,
        created_at=created_at,
    )

    # Build full markdown
    markdown = build_transcript_markdown(
        frontmatter=frontmatter,
        video_id=video_id,
        source_url=args.url,
        target_team=args.team,
        target_league=args.league,
        coach_context=args.coach,
        source_channel=args.channel,
        source_date=args.date,
        transcript_source="yt_dlp_subtitles",
        extraction_method=extraction_method,
        language=detected_language,
        include_timestamps=include_timestamps,
        used_browser_cookies=args.use_cookies,
        created_at=created_at,
        transcript_text=transcript_text,
    )

    # Write output
    output_path = TRANSCRIPT_DIR / f"{basename}_transcript_raw.md"
    output_path.write_text(markdown, encoding="utf-8")

    # Cleanup tmp subtitle file
    subtitle_file.unlink(missing_ok=True)

    logger.info("✅ Transcript saved: %s", output_path)
    logger.info("   video_id: %s", video_id)
    logger.info("   language: %s", detected_language)
    logger.info("   used_browser_cookies: %s", args.use_cookies)
    logger.info("   word count: ~%d", len(transcript_text.split()))
    logger.info("Next step: YT-03 Transcript-to-Tactical-Claims Extraction")

    return 0


if __name__ == "__main__":
    sys.exit(main())
