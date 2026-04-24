import argparse
import json
import logging
import os
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from audit_router import AuditRouter, load_dotenv_into_env, normalize_vault_path


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("AresTelemetry.PostmatchCleanup")


def _parse_datetime(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _read_manifest(vault_root: Path, issue: str) -> Dict[str, Any]:
    manifest_path = vault_root / "04_RAG_Raw_Data" / "Cold_Data_Lake" / f"{issue}_dispatch_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"找不到 dispatch manifest: {manifest_path}")
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _build_issue_window(manifest: Dict[str, Any]) -> Tuple[Optional[datetime], Optional[datetime], List[datetime]]:
    match_dates: List[datetime] = []
    for match in manifest.get("matches", []):
        candidate = _parse_datetime(
            match.get("understat_date")
            or match.get("fbref_date")
            or match.get("football_data_date")
        )
        if candidate is not None:
            match_dates.append(candidate)

    if not match_dates:
        return None, None, []

    match_dates.sort()
    anchor = match_dates[len(match_dates) // 2]
    window_days = int(os.getenv("ARES_POSTMATCH_ISSUE_WINDOW_DAYS", "3"))
    lower = anchor - timedelta(days=window_days)
    upper = anchor + timedelta(days=window_days)
    in_window = [dt for dt in match_dates if lower <= dt <= upper]
    if not in_window:
        return None, None, match_dates
    return min(in_window), max(in_window), match_dates


def _split_frontmatter(text: str) -> Tuple[Dict[str, Any], str]:
    if not text.startswith("---\n"):
        return {}, text
    parts = text.split("---\n", 2)
    if len(parts) < 3:
        return {}, text
    frontmatter_text = parts[1]
    body = parts[2]
    data = yaml.safe_load(frontmatter_text) or {}
    if not isinstance(data, dict):
        data = {}
    return data, body


def _dump_markdown(frontmatter: Dict[str, Any], body: str) -> str:
    yaml_text = yaml.safe_dump(frontmatter, sort_keys=False, allow_unicode=True)
    return f"---\n{yaml_text}---\n{body}"


def _extract_match_id(path: Path) -> Optional[str]:
    normalized_stem = re.sub(r"^(STALE|PENDING-VERIFY)-", "", path.stem)
    match = re.search(r"^\d+_(.+?)_postmatch$", normalized_stem)
    if match:
        return match.group(1)
    fallback = re.search(r"(\d+)_postmatch$", normalized_stem)
    return fallback.group(1) if fallback else None


def _match_lookup(manifest: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for match in manifest.get("matches", []):
        match_id = str(match.get("understat_id") or "").strip()
        if match_id:
            lookup[match_id] = match
    return lookup


def _cold_match_info_date(vault_root: Path, issue: str, match_id: str) -> Optional[datetime]:
    info_path = vault_root / "04_RAG_Raw_Data" / "Cold_Data_Lake" / f"{issue}_{match_id}_understat_match_info_raw.json"
    if not info_path.exists():
        return None
    try:
        payload = json.loads(info_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return _parse_datetime(payload.get("date"))


def _prepend_review_banner(body: str, review_status: str, reasons: List[str], reviewed_at: str) -> str:
    lines = [
        "",
        f"> [!warning] Postmatch Cleanup Status: `{review_status}`",
        f"> Reviewed At: {reviewed_at}",
        f"> Reasons: {'; '.join(reasons)}",
        "",
    ]
    banner = "\n".join(lines)
    if "Postmatch Cleanup Status:" in body:
        body = re.sub(
            r"\n> \[!warning\] Postmatch Cleanup Status:.*?\n(?=\n#|\n##|\Z)",
            "\n",
            body,
            flags=re.DOTALL,
        )
    title_match = re.search(r"^# .+$", body, flags=re.MULTILINE)
    if not title_match:
        return banner + body
    insert_at = title_match.end()
    return body[:insert_at] + banner + body[insert_at:]


def _mark_report_content(
    path: Path,
    review_status: str,
    reasons: List[str],
    reviewed_at: str,
) -> None:
    text = path.read_text(encoding="utf-8")
    frontmatter, body = _split_frontmatter(text)
    result = frontmatter.get("result")
    if not isinstance(result, dict):
        result = {}
    result["validation_passed"] = False
    frontmatter["result"] = result
    frontmatter["postmatch_review_status"] = review_status
    frontmatter["postmatch_review_reasons"] = reasons
    frontmatter["postmatch_reviewed_at"] = reviewed_at
    body = _prepend_review_banner(body, review_status, reasons, reviewed_at)
    path.write_text(_dump_markdown(frontmatter, body), encoding="utf-8")


def _move_to_legacy(issue_dirs: Dict[str, Path], src: Path, prefix: str) -> Path:
    target = issue_dirs["postmatch_legacy_dir"] / f"{prefix}-{src.name}"
    if target.exists():
        target = issue_dirs["postmatch_legacy_dir"] / f"{prefix}-{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}-{src.name}"
    shutil.move(str(src), str(target))
    return target


def _classify_report(
    *,
    issue: str,
    path: Path,
    manifest_match: Optional[Dict[str, Any]],
    vault_root: Path,
    issue_window_start: Optional[datetime],
    issue_window_end: Optional[datetime],
) -> Tuple[str, List[str], Optional[datetime], Optional[str]]:
    match_id = _extract_match_id(path)
    reasons: List[str] = []
    expected_dt = None

    if manifest_match:
        expected_dt = _parse_datetime(
            manifest_match.get("understat_date")
            or manifest_match.get("fbref_date")
            or manifest_match.get("football_data_date")
        )
    if expected_dt is None and match_id:
        expected_dt = _cold_match_info_date(vault_root, issue, match_id)

    official_score = None
    if manifest_match:
        official_score = str(manifest_match.get("official_score") or manifest_match.get("result_score") or "").strip() or None

    if issue_window_start and issue_window_end and expected_dt:
        if expected_dt < issue_window_start or expected_dt > issue_window_end:
            reasons.append(
                "expected_match_date_outside_issue_window:"
                f"{expected_dt.strftime('%Y-%m-%d %H:%M:%S')}"
            )

    if not official_score:
        reasons.append("missing_official_score_or_result_score")

    if any(reason.startswith("expected_match_date_outside_issue_window:") for reason in reasons):
        return "stale", reasons, expected_dt, official_score
    if reasons:
        return "pending_verification", reasons, expected_dt, official_score
    return "verified", reasons, expected_dt, official_score


def _write_cleanup_report(
    *,
    issue: str,
    review_dir: Path,
    issue_window_start: Optional[datetime],
    issue_window_end: Optional[datetime],
    summary: Dict[str, Any],
) -> Path:
    lines: List[str] = []
    lines.append(f"# Review {issue} - Postmatch Cleanup")
    lines.append("")
    lines.append(f"- Updated At: {summary['reviewed_at']}")
    lines.append(f"- Issue Window: {summary['issue_window']}")
    lines.append(f"- Manifest Matches: {summary['manifest_match_count']}")
    lines.append(f"- Manifest Scored Matches: {summary['manifest_scored_count']}")
    lines.append(f"- Main Index Before: {len(summary['before_main'])}")
    lines.append(f"- Main Index After: {len(summary['after_main'])}")
    lines.append(f"- Newly Quarantined Stale: {len(summary['new_stale'])}")
    lines.append(f"- Newly Marked Pending Verification: {len(summary['new_pending'])}")
    lines.append(f"- Already In Legacy: {len(summary['already_legacy'])}")
    lines.append("")

    lines.append("## Cleanup Rules")
    lines.append("- `STALE`: manifest/cold-data 对应比赛日期落在 issue 时间窗口之外，自动剔出主索引。")
    lines.append("- `PENDING_VERIFICATION`: manifest 缺少 `official_score/result_score`，不得继续视为已验证结果。")
    lines.append("")

    lines.append("## Main Index Before")
    if summary["before_main"]:
        lines.extend(f"- `{name}`" for name in summary["before_main"])
    else:
        lines.append("- None")
    lines.append("")

    lines.append("## New Stale Quarantine")
    if summary["new_stale"]:
        for item in summary["new_stale"]:
            lines.append(
                f"- `{item['source']}` -> `{item['target']}`"
                f" | expected_date={item['expected_date']} | reasons={'; '.join(item['reasons'])}"
            )
    else:
        lines.append("- None")
    lines.append("")

    lines.append("## New Pending Verification")
    if summary["new_pending"]:
        for item in summary["new_pending"]:
            lines.append(
                f"- `{item['source']}` -> `{item['target']}`"
                f" | expected_date={item['expected_date']} | reasons={'; '.join(item['reasons'])}"
            )
    else:
        lines.append("- None")
    lines.append("")

    lines.append("## Already In Legacy")
    if summary["already_legacy"]:
        for item in summary["already_legacy"]:
            lines.append(
                f"- `{item['path']}`"
                f" | status={item['status']} | expected_date={item['expected_date']} | reasons={'; '.join(item['reasons'])}"
            )
    else:
        lines.append("- None")
    lines.append("")

    lines.append("## Main Index After")
    if summary["after_main"]:
        lines.extend(f"- `{name}`" for name in summary["after_main"])
    else:
        lines.append("- None")
    lines.append("")

    target = review_dir / f"REVIEW-{issue}-Postmatch_Cleanup.md"
    target.write_text("\n".join(lines), encoding="utf-8")
    return target


def cleanup_issue_postmatch(issue: str, vault_path: Optional[str] = None) -> Dict[str, Any]:
    resolved_vault = normalize_vault_path(vault_path or os.getenv("ARES_VAULT_PATH", ""))
    if not resolved_vault:
        raise ValueError("未配置 ARES_VAULT_PATH，无法执行 postmatch 存量清理。")

    vault_root = Path(resolved_vault)
    manifest = _read_manifest(vault_root, issue)
    match_lookup = _match_lookup(manifest)
    issue_window_start, issue_window_end, manifest_dates = _build_issue_window(manifest)
    reviewed_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")

    router = AuditRouter(base_dir=Path(__file__).resolve().parents[2], vault_path=resolved_vault)
    issue_dirs = router._ensure_issue_dirs(issue)
    postmatch_dir = vault_root / "03_Match_Audits" / "Postmatch_Telemetry"
    before_main = sorted(path.name for path in postmatch_dir.glob(f"{issue}_*_postmatch.md"))
    legacy_before = {
        path.name for path in issue_dirs["postmatch_legacy_dir"].glob(f"*{issue}_*_postmatch.md")
    }

    new_stale: List[Dict[str, Any]] = []
    new_pending: List[Dict[str, Any]] = []
    already_legacy: List[Dict[str, Any]] = []

    for path in sorted(postmatch_dir.glob(f"{issue}_*_postmatch.md")):
        match_id = _extract_match_id(path)
        manifest_match = match_lookup.get(match_id or "")
        status, reasons, expected_dt, _ = _classify_report(
            issue=issue,
            path=path,
            manifest_match=manifest_match,
            vault_root=vault_root,
            issue_window_start=issue_window_start,
            issue_window_end=issue_window_end,
        )
        if status == "verified":
            continue

        prefix = "STALE" if status == "stale" else "PENDING-VERIFY"
        target = _move_to_legacy(issue_dirs, path, prefix)
        _mark_report_content(target, status, reasons, reviewed_at)
        item = {
            "source": path.name,
            "target": target.name,
            "status": status,
            "reasons": reasons,
            "expected_date": expected_dt.strftime("%Y-%m-%d %H:%M:%S") if expected_dt else "unknown",
        }
        if status == "stale":
            new_stale.append(item)
        else:
            new_pending.append(item)

    for path in sorted(issue_dirs["postmatch_legacy_dir"].glob(f"*{issue}_*_postmatch.md")):
        status = "legacy"
        if path.name.startswith("STALE-"):
            status = "stale"
        elif path.name.startswith("PENDING-VERIFY-"):
            status = "pending_verification"
        else:
            continue

        match_id = _extract_match_id(path)
        manifest_match = match_lookup.get(match_id or "")
        classified_status, reasons, expected_dt, _ = _classify_report(
            issue=issue,
            path=path,
            manifest_match=manifest_match,
            vault_root=vault_root,
            issue_window_start=issue_window_start,
            issue_window_end=issue_window_end,
        )
        _mark_report_content(path, classified_status, reasons or [f"legacy_prefixed:{status}"], reviewed_at)
        if path.name in legacy_before:
            already_legacy.append(
                {
                    "path": path.name,
                    "status": classified_status,
                    "reasons": reasons or [f"legacy_prefixed:{status}"],
                    "expected_date": expected_dt.strftime("%Y-%m-%d %H:%M:%S") if expected_dt else "unknown",
                }
            )

    after_main = sorted(path.name for path in postmatch_dir.glob(f"{issue}_*_postmatch.md"))

    issue_window_text = "unknown"
    if issue_window_start and issue_window_end:
        issue_window_text = (
            f"{issue_window_start.strftime('%Y-%m-%d %H:%M:%S')} -> "
            f"{issue_window_end.strftime('%Y-%m-%d %H:%M:%S')}"
        )

    scored_count = sum(
        1 for match in manifest.get("matches", [])
        if (match.get("official_score") or match.get("result_score"))
    )
    summary = {
        "reviewed_at": reviewed_at,
        "issue_window": issue_window_text,
        "manifest_match_count": len(manifest.get("matches", [])),
        "manifest_scored_count": scored_count,
        "before_main": before_main,
        "after_main": after_main,
        "new_stale": new_stale,
        "new_pending": new_pending,
        "already_legacy": already_legacy,
        "manifest_dates": [dt.strftime("%Y-%m-%d %H:%M:%S") for dt in manifest_dates],
    }
    report_path = _write_cleanup_report(
        issue=issue,
        review_dir=issue_dirs["review_dir"],
        issue_window_start=issue_window_start,
        issue_window_end=issue_window_end,
        summary=summary,
    )
    router.ensure_issue_governance(issue=issue, manifest=manifest, create_prematch_stubs=False)
    summary["report_path"] = str(report_path)
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ares Postmatch Cleanup - quarantine stale/unverified legacy reports")
    parser.add_argument("--issue", type=str, required=True, help="中国体彩期号，如 26065")
    parser.add_argument("--vault-path", type=str, required=False, help="显式指定 Ares Vault 路径")
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parents[2]
    load_dotenv_into_env(base_dir)
    summary = cleanup_issue_postmatch(issue=args.issue, vault_path=args.vault_path)
    logger.info(
        "Postmatch cleanup 完成 issue=%s, before=%s, after=%s, new_stale=%s, new_pending=%s, already_legacy=%s, report=%s",
        args.issue,
        len(summary["before_main"]),
        len(summary["after_main"]),
        len(summary["new_stale"]),
        len(summary["new_pending"]),
        len(summary["already_legacy"]),
        summary["report_path"],
    )
