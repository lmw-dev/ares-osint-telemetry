import argparse
import json
import logging
import math
import os
import sqlite3
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from audit_router import load_dotenv_into_env, normalize_vault_path
from team_forge import build_archive_path, iter_issue_teams, split_frontmatter


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("AresTelemetry.PrematchPreflight")


PLACEHOLDER_MARKERS = (
    "Baseline profile initialized by `team_forge.py`",
    "Add tactical observations, injury patterns, and review snapshots below.",
    "待定向抓取补充",
    "待更新",
)

TACTICAL_LOGIC_KEYS = ("P", "Space", "F", "H", "Set_Piece")
DEFAULT_PHYSICAL_PROFILE = {
    "avg_xG_last_5": 1.0,
    "conversion_efficiency": 0.05,
    "defensive_leakage": 0.5,
    "actual_tactical_entropy": 0.4,
}


def _normalize_team_key(value: str) -> str:
    ascii_name = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
    normalized = "".join(ch for ch in ascii_name.lower() if ch.isalnum())
    if normalized:
        return normalized
    return "".join(ch for ch in str(value).strip().lower() if ch.isalnum())


def _split_match_english(english: str) -> Tuple[str, str]:
    if " vs " in english:
        home, away = english.split(" vs ", 1)
        return home.strip(), away.strip()
    if " VS " in english:
        home, away = english.split(" VS ", 1)
        return home.strip(), away.strip()
    return english.strip(), ""


def _is_smoke_manual_anchor(match: Dict[str, Any]) -> bool:
    mode = str(match.get("manual_anchor_mode") or "").strip().lower()
    notes = str(match.get("manual_anchor_notes") or "").strip().lower()
    fbref_url = str(match.get("fbref_url") or "").strip().lower()
    return mode == "smoke" or "[smoke]" in notes or fbref_url.startswith("https://anchor.local/")


def _resolve_engine_dir(explicit_engine_dir: Optional[str], base_dir: Path) -> Path:
    sibling = base_dir.parent / "20-ares-v4-engine"
    raw_path = explicit_engine_dir or os.getenv("ARES_ENGINE_DIR", str(sibling))
    return Path(raw_path).expanduser().resolve()


def _resolve_manifest_path(vault_root: Path, issue: str, base_dir: Path) -> Path:
    primary = vault_root / "04_RAG_Raw_Data" / "Cold_Data_Lake" / f"{issue}_dispatch_manifest.json"
    if primary.exists():
        return primary
    fallback = base_dir / "raw_reports" / f"{issue}_dispatch_manifest.json"
    if fallback.exists():
        return fallback
    raise FileNotFoundError(f"找不到 dispatch manifest: {primary}")


def _load_manifest(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_rag_team_doc_counts(engine_dir: Path) -> Dict[str, int]:
    chroma_db = engine_dir / "chromadb" / "chroma.sqlite3"
    if not chroma_db.exists():
        return {}

    conn = sqlite3.connect(f"file:{chroma_db}?mode=ro", uri=True)
    with conn:
        rows = conn.execute(
            "select string_value, count(*) from embedding_metadata where key='team' and string_value is not null group by string_value"
        ).fetchall()

    counts: Dict[str, int] = {}
    for raw_team, count in rows:
        if not raw_team:
            continue
        counts[_normalize_team_key(raw_team)] = int(count or 0)
    return counts


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_iso_like_date(raw_value: Any) -> Optional[datetime]:
    text = str(raw_value or "").strip()
    if not text:
        return None
    candidates = [text]
    if text.endswith("Z"):
        candidates.append(text[:-1] + "+00:00")
    for candidate in candidates:
        try:
            parsed = datetime.fromisoformat(candidate)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text[:10], fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _collect_archive_gaps(frontmatter: Dict[str, Any], body_text: str) -> Dict[str, Any]:
    gaps: List[str] = []
    intel_base = frontmatter.get("intel_base")
    intel_base = intel_base if isinstance(intel_base, dict) else {}
    tactical_logic = frontmatter.get("tactical_logic")
    tactical_logic = tactical_logic if isinstance(tactical_logic, dict) else {}
    physical_reality = frontmatter.get("physical_reality")
    physical_reality = physical_reality if isinstance(physical_reality, dict) else {}

    manager_doctrine = str(intel_base.get("manager_doctrine") or "").strip()
    if not manager_doctrine or manager_doctrine.lower() == "unknown":
        gaps.append("missing_manager_doctrine")

    recent_news_summary = str(intel_base.get("recent_news_summary") or "").strip()
    if not recent_news_summary:
        gaps.append("missing_recent_news_summary")

    key_nodes = intel_base.get("key_node_dependency")
    if not isinstance(key_nodes, list) or not [str(item).strip() for item in key_nodes if str(item).strip()]:
        legacy_key_nodes = frontmatter.get("key_node_dependency")
        if not isinstance(legacy_key_nodes, list) or not [str(item).strip() for item in legacy_key_nodes if str(item).strip()]:
            gaps.append("missing_key_node_dependency")

    missing_tactical_keys = []
    for key in TACTICAL_LOGIC_KEYS:
        raw = str(tactical_logic.get(key) or "").strip()
        if not raw or raw.lower() == "unknown":
            missing_tactical_keys.append(key)
    if missing_tactical_keys:
        gaps.append("incomplete_tactical_logic")

    default_physical_fields: List[str] = []
    for key, default_value in DEFAULT_PHYSICAL_PROFILE.items():
        current = _safe_float(physical_reality.get(key))
        if current is None or abs(current - default_value) < 1e-9:
            default_physical_fields.append(key)
    if len(default_physical_fields) == len(DEFAULT_PHYSICAL_PROFILE):
        gaps.append("default_physical_profile")

    archive_quality = str(frontmatter.get("archive_quality") or "").strip().lower()
    if not archive_quality:
        gaps.append("missing_archive_quality")

    last_modified = _parse_iso_like_date(frontmatter.get("last_modified_date"))
    stale_days = None
    stale_threshold_days = int(os.getenv("ARES_PREMATCH_ARCHIVE_STALE_DAYS", "21"))
    if last_modified is None:
        gaps.append("missing_last_modified_date")
    else:
        stale_days = max(0, int((datetime.now(timezone.utc) - last_modified).days))
        if stale_days >= stale_threshold_days:
            gaps.append("stale_archive")

    if len(body_text.strip()) < 180:
        gaps.append("thin_archive_body")

    return {
        "gaps": gaps,
        "missing_tactical_keys": missing_tactical_keys,
        "default_physical_fields": default_physical_fields,
        "stale_days": stale_days,
        "needs_enrichment": bool(gaps),
    }


def _inspect_team_archive_content(path: Path) -> Dict[str, Any]:
    diagnostics = {
        "archive_exists": path.exists(),
        "placeholder": False,
        "markers": [],
        "unknown_count": 0,
        "char_count": 0,
        "archive_quality": None,
        "archive_status": "missing",
        "frontmatter": {},
        "gaps": [],
        "missing_tactical_keys": [],
        "default_physical_fields": [],
        "needs_enrichment": False,
        "stale_days": None,
    }
    if not path.exists():
        return diagnostics

    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        diagnostics["placeholder"] = True
        diagnostics["markers"] = ["unreadable_archive"]
        diagnostics["archive_status"] = "placeholder"
        return diagnostics

    frontmatter, _ = split_frontmatter(text)
    if not frontmatter and text.startswith("---\n"):
        closing_marker_index = text.find("\n---\n", 4)
        if closing_marker_index != -1:
            frontmatter_raw = text[4:closing_marker_index]
            try:
                loaded = yaml.safe_load(frontmatter_raw) or {}
                if isinstance(loaded, dict):
                    frontmatter = loaded
            except Exception:
                pass

    archive_quality = ""
    if isinstance(frontmatter, dict):
        archive_quality = str(frontmatter.get("archive_quality") or "").strip().lower()
    diagnostics["archive_quality"] = archive_quality or None
    diagnostics["frontmatter"] = frontmatter if isinstance(frontmatter, dict) else {}

    diagnostics["char_count"] = len(text.strip())
    diagnostics["unknown_count"] = text.count("Unknown")
    markers = [marker for marker in PLACEHOLDER_MARKERS if marker in text]
    if diagnostics["unknown_count"] >= 5:
        markers.append("high_unknown_density")

    explicit_statuses = {"usable", "placeholder", "placeholder_backfilled"}
    if archive_quality in explicit_statuses:
        archive_status = archive_quality
        if archive_status == "placeholder_backfilled":
            markers.append("archive_quality_placeholder_backfilled")
    else:
        if archive_quality:
            markers.append(f"archive_quality_unrecognized:{archive_quality}")
        archive_status = "placeholder" if markers else "usable"

    diagnostics["archive_status"] = archive_status
    diagnostics["placeholder"] = archive_status in {"placeholder", "placeholder_backfilled"}
    gap_diagnostics = _collect_archive_gaps(diagnostics["frontmatter"], text)
    diagnostics["gaps"] = gap_diagnostics["gaps"]
    diagnostics["missing_tactical_keys"] = gap_diagnostics["missing_tactical_keys"]
    diagnostics["default_physical_fields"] = gap_diagnostics["default_physical_fields"]
    diagnostics["needs_enrichment"] = gap_diagnostics["needs_enrichment"] or diagnostics["placeholder"]
    diagnostics["stale_days"] = gap_diagnostics["stale_days"]
    markers.extend(gap_diagnostics["gaps"])
    diagnostics["markers"] = sorted(set(markers))
    return diagnostics


def _inspect_rag_readiness(engine_dir: Path, manifest: Dict[str, Any]) -> Dict[str, Any]:
    chroma_db = engine_dir / "chromadb" / "chroma.sqlite3"
    diagnostics: Dict[str, Any] = {
        "ok": True,
        "summary": "",
        "details": [],
        "doc_count": 0,
        "issue_teams": [],
        "covered_teams": [],
        "missing_teams": [],
    }
    if not chroma_db.exists():
        diagnostics.update(
            ok=False,
            summary=f"找不到 RAG 数据库: {chroma_db}",
            details=[f"缺少 `{chroma_db}`，无法做 prematch 预检。"],
        )
        return diagnostics

    issue_team_map: Dict[str, str] = {}
    for match in manifest.get("matches", []):
        english = str(match.get("english", "")).strip()
        if not english:
            continue
        home, away = _split_match_english(english)
        for team in (home, away):
            if not team:
                continue
            issue_team_map.setdefault(_normalize_team_key(team), team)
    diagnostics["issue_teams"] = sorted(issue_team_map.values())

    conn = sqlite3.connect(f"file:{chroma_db}?mode=ro", uri=True)
    with conn:
        row = conn.execute("select count(*) from embeddings").fetchone()
        diagnostics["doc_count"] = int(row[0]) if row and row[0] is not None else 0
        team_rows = conn.execute(
            "select distinct string_value from embedding_metadata where key='team' and string_value is not null"
        ).fetchall()

    team_values = {_normalize_team_key(row[0]) for row in team_rows if row and row[0]}
    covered_keys = sorted(set(issue_team_map) & team_values)
    missing_keys = sorted(set(issue_team_map) - team_values)
    diagnostics["covered_teams"] = [issue_team_map[key] for key in covered_keys]
    diagnostics["missing_teams"] = [issue_team_map[key] for key in missing_keys]

    min_doc_count = int(os.getenv("ARES_PREMATCH_RAG_MIN_DOC_COUNT", "3"))
    min_team_coverage_ratio = float(os.getenv("ARES_PREMATCH_RAG_MIN_TEAM_COVERAGE_RATIO", "0.75"))
    max_missing_teams = int(os.getenv("ARES_PREMATCH_RAG_MAX_MISSING_TEAMS", "4"))
    required_team_coverage = math.ceil(len(issue_team_map) * min_team_coverage_ratio) if issue_team_map else 0

    blockers: List[str] = []
    if diagnostics["doc_count"] < min_doc_count:
        blockers.append(f"RAG 总文档数 `{diagnostics['doc_count']}` 低于阈值 `{min_doc_count}`。")
    if issue_team_map and len(covered_keys) < required_team_coverage:
        blockers.append(
            f"Issue 球队覆盖 `{len(covered_keys)}/{len(issue_team_map)}`，低于阈值 `{required_team_coverage}/{len(issue_team_map)}`。"
        )
    if issue_team_map and len(missing_keys) > max_missing_teams:
        blockers.append(f"缺失球队 `{len(missing_keys)}` 支，高于允许上限 `{max_missing_teams}`。")

    diagnostics["ok"] = not blockers
    diagnostics["summary"] = (
        f"RAG readiness OK: docs={diagnostics['doc_count']}, covered={len(covered_keys)}/{len(issue_team_map)}"
        if not blockers
        else "RAG readiness 未通过。"
    )
    diagnostics["details"] = blockers or [
        f"RAG 总文档数: {diagnostics['doc_count']}",
        f"Issue 球队覆盖: {len(covered_keys)}/{len(issue_team_map)}",
        f"缺失球队: {len(missing_keys)}",
    ]
    return diagnostics


def build_preflight_report(
    *,
    issue: str,
    base_dir: Path,
    vault_root: Path,
    engine_dir: Path,
    manifest: Dict[str, Any],
    manifest_path: Path,
) -> Dict[str, Any]:
    rag_readiness = _inspect_rag_readiness(engine_dir, manifest)
    rag_team_doc_counts = _load_rag_team_doc_counts(engine_dir) if (engine_dir / "chromadb" / "chroma.sqlite3").exists() else {}

    team_records: Dict[str, Dict[str, Any]] = {}
    for team, league in iter_issue_teams(base_dir, vault_root, issue):
        team_key = _normalize_team_key(team)
        archive_path = build_archive_path(vault_root, team, league)
        archive_diagnostics = _inspect_team_archive_content(archive_path)
        team_records[team_key] = {
            "team": team,
            "league": league,
            "archive_path": str(archive_path),
            "rag_doc_count": rag_team_doc_counts.get(team_key, 0),
            **archive_diagnostics,
        }

    mapping_counts = Counter(str(match.get("mapping_source") or "unknown") for match in manifest.get("matches", []))
    matches: List[Dict[str, Any]] = []
    weak_matches: List[Dict[str, Any]] = []
    for match in manifest.get("matches", []):
        english = str(match.get("english", "")).strip()
        home, away = _split_match_english(english)
        issues: List[str] = []
        mapping_source = str(match.get("mapping_source") or "unknown")
        if mapping_source == "unmapped":
            issues.append("unmapped_fixture")
        if _is_smoke_manual_anchor(match):
            issues.append("smoke_anchor_fixture")

        for team in (home, away):
            team_key = _normalize_team_key(team)
            record = team_records.get(team_key)
            if not record:
                issues.append(f"team_not_registered:{team}")
                continue
            archive_status = str(record.get("archive_status") or "missing")
            if archive_status == "missing":
                issues.append(f"missing_archive:{team}")
            elif archive_status == "placeholder":
                issues.append(f"placeholder_archive:{team}")
            elif archive_status == "placeholder_backfilled":
                issues.append(f"placeholder_backfilled_archive:{team}")
            if record.get("needs_enrichment"):
                issues.append(f"needs_archive_enrichment:{team}")
            if record["rag_doc_count"] <= 1:
                issues.append(f"thin_rag_docs:{team}")

        row = {
            "index": int(match.get("index", 0) or 0),
            "chinese": str(match.get("chinese") or ""),
            "english": english,
            "league": str(match.get("league") or ""),
            "mapping_source": mapping_source,
            "understat_id": match.get("understat_id"),
            "fbref_url": match.get("fbref_url"),
            "football_data_match_id": match.get("football_data_match_id"),
            "manual_anchor_applied": bool(match.get("manual_anchor_applied")),
            "manual_anchor_mode": str(match.get("manual_anchor_mode") or "").strip().lower() or None,
            "manual_anchor_notes": str(match.get("manual_anchor_notes") or "").strip(),
            "issues": sorted(set(issues)),
        }
        matches.append(row)
        if row["issues"]:
            weak_matches.append(row)

    archive_status_counts = Counter(str(record.get("archive_status") or "missing") for record in team_records.values())
    usable_teams = archive_status_counts.get("usable", 0)
    placeholder_teams = archive_status_counts.get("placeholder", 0)
    placeholder_backfilled_teams = archive_status_counts.get("placeholder_backfilled", 0)
    missing_teams = archive_status_counts.get("missing", 0)
    low_quality_teams = placeholder_teams + placeholder_backfilled_teams
    thin_rag_teams = sum(1 for record in team_records.values() if record["rag_doc_count"] <= 1)
    enrichment_needed_teams = sum(1 for record in team_records.values() if record.get("needs_enrichment"))
    total_matches = len(matches)
    total_teams = len(team_records)
    unmapped_matches = mapping_counts.get("unmapped", 0)
    smoke_anchor_matches = sum(1 for row in matches if "smoke_anchor_fixture" in row.get("issues", []))

    status = "READY"
    recommended_action = "可以进入 prematch 主流程。"
    if not rag_readiness["ok"]:
        status = "BLOCKED"
        recommended_action = "先修复 RAG readiness 阻断项，再进入 prematch。"
    elif total_matches and (
        unmapped_matches >= math.ceil(total_matches * 0.5)
        or (total_teams and low_quality_teams >= math.ceil(total_teams * 0.5))
        or (total_teams and thin_rag_teams >= math.ceil(total_teams * 0.5))
    ):
        status = "HOLD"
        recommended_action = "不建议直接跑全量 prematch；先补充队档实质内容（新闻/战术/物理指标）并按需重建 RAG，再决定是否全量执行。"
    elif weak_matches:
        status = "CAUTION"
        recommended_action = "可按单场或小批量执行；并行补充薄弱队档的实质内容，避免模板档案直接入模。"
    if status == "READY" and smoke_anchor_matches > 0:
        status = "CAUTION"
        recommended_action = "检测到 smoke 锚点，仅可用于流程回归；生产执行前请先替换为真实锚点并重跑 preflight。"

    summary = [
        f"manifest 已落盘：`{manifest_path}`",
        f"本期共 `{total_matches}` 场，`mapping_source=unmapped` 有 `{unmapped_matches}` 场。",
        f"本期使用 smoke 锚点的比赛有 `{smoke_anchor_matches}` 场（仅回归测试，不视为生产可用映射）。",
        f"本期球队共 `{total_teams}` 支：usable `{usable_teams}`、placeholder `{placeholder_teams}`、placeholder_backfilled `{placeholder_backfilled_teams}`、missing `{missing_teams}`。",
        f"低质量模板队档（placeholder + placeholder_backfilled）共 `{low_quality_teams}` 支。",
        f"需要补强的球队共 `{enrichment_needed_teams}` 支（含结构缺口、过期时间戳、默认物理值、缺新闻摘要等）。",
        f"RAG team metadata 覆盖 `{len(rag_readiness['covered_teams'])}/{len(rag_readiness['issue_teams'])}`，但 `thin_rag_docs` 球队有 `{thin_rag_teams}` 支。",
        recommended_action,
    ]

    return {
        "issue": issue,
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ"),
        "status": status,
        "recommended_action": recommended_action,
        "manifest_path": str(manifest_path),
        "engine_dir": str(engine_dir),
        "mapping_counts": dict(sorted(mapping_counts.items())),
        "rag_readiness": rag_readiness,
        "summary": summary,
        "matches": matches,
        "weak_matches": weak_matches,
        "teams": sorted(team_records.values(), key=lambda item: (item["league"], item["team"])),
        "usable_team_archives": usable_teams,
        "placeholder_team_archives": placeholder_teams,
        "placeholder_backfilled_team_archives": placeholder_backfilled_teams,
        "low_quality_team_archives": low_quality_teams,
        "enrichment_needed_teams": enrichment_needed_teams,
        "missing_team_archives": missing_teams,
        "thin_rag_teams": thin_rag_teams,
        "unmapped_matches": unmapped_matches,
        "smoke_anchor_matches": smoke_anchor_matches,
        "total_matches": total_matches,
        "total_teams": total_teams,
    }


def render_markdown(report: Dict[str, Any]) -> str:
    issue = report["issue"]
    today = report["updated_at"][:10]
    lines: List[str] = []
    lines.extend(
        [
            "---",
            "tags:",
            "  - project/ares-v4/osint-telemetry",
            "  - area/prematch-preflight",
            "  - type/report",
            "  - obsidian",
            f'status: "{report["status"].lower()}"',
            "version: 0.1",
            f"creation_date: {today}",
            f"last_modified_date: {today}",
            "project: Ares-Matrix-DB",
            'owner: "Ares"',
            "related:",
            f'  - "[[REVIEW-{issue}-Prematch_Data_Quality]]"',
            f'  - "[[README]]"',
            "---",
            "",
            f"# Audit-{issue}",
            "",
        ]
    )

    lines.append("## 1. 目的")
    lines.append("")
    lines.append(f"本笔记用于在执行 `issue={issue}` 的全量 prematch 前，统一检查映射质量、RAG readiness、Team Archive 质量，以及是否适合直接进入主流程。")
    lines.append("")

    lines.append("## 2. 结论摘要")
    lines.append("")
    lines.append("| 项目 | 结果 |")
    lines.append("| --- | --- |")
    lines.append(f"| 更新时间 | `{report['updated_at']}` |")
    lines.append(f"| 预检状态 | `{report['status']}` |")
    lines.append(f"| 建议动作 | {report['recommended_action']} |")
    lines.append(f"| Manifest | `{report['manifest_path']}` |")
    lines.append(f"| Engine 目录 | `{report['engine_dir']}` |")
    lines.append(f"| 比赛总数 | `{report['total_matches']}` |")
    lines.append(f"| `unmapped` 场次 | `{report['unmapped_matches']}` |")
    lines.append(f"| `smoke` 锚点场次 | `{report['smoke_anchor_matches']}` |")
    lines.append(f"| 球队总数 | `{report['total_teams']}` |")
    lines.append(f"| Usable 队档 | `{report['usable_team_archives']}` |")
    lines.append(f"| Placeholder 队档 | `{report['placeholder_team_archives']}` |")
    lines.append(f"| Placeholder Backfilled 队档 | `{report['placeholder_backfilled_team_archives']}` |")
    lines.append(f"| 低质量模板队档 | `{report['low_quality_team_archives']}` |")
    lines.append(f"| 需要补强球队 | `{report['enrichment_needed_teams']}` |")
    lines.append(f"| 缺失队档 | `{report['missing_team_archives']}` |")
    lines.append(f"| Thin RAG Docs 球队 | `{report['thin_rag_teams']}` |")
    lines.append("")

    lines.append("## 3. 核心发现")
    lines.append("")
    lines.extend(f"- {item}" for item in report["summary"])
    lines.append("")

    lines.append("## 4. 映射概览")
    lines.append("")
    lines.append("| Mapping Source | 场次 |")
    lines.append("| --- | ---: |")
    for key, count in report["mapping_counts"].items():
        lines.append(f"| `{key}` | `{count}` |")
    if not report["mapping_counts"]:
        lines.append("| `none` | `0` |")
    lines.append("")

    lines.append("## 5. RAG Readiness")
    lines.append("")
    lines.append("| 指标 | 结果 |")
    lines.append("| --- | --- |")
    lines.append(f"| 摘要 | {report['rag_readiness']['summary']} |")
    lines.append(f"| 覆盖球队 | `{len(report['rag_readiness']['covered_teams'])}/{len(report['rag_readiness']['issue_teams'])}` |")
    lines.append(f"| 缺失球队 | `{len(report['rag_readiness']['missing_teams'])}` |")
    lines.append("")
    if report["rag_readiness"]["details"]:
        lines.append("补充说明：")
        lines.extend(f"- {item}" for item in report["rag_readiness"]["details"])
        lines.append("")

    lines.append("## 6. 比赛看板")
    lines.append("")
    lines.append("| 场次 | 对阵 | 联赛 | Mapping | 外部锚点 | 风险信号 |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for match in report["matches"]:
        anchors: List[str] = []
        if match["understat_id"]:
            anchors.append(f"understat={match['understat_id']}")
        if match["football_data_match_id"]:
            anchors.append(f"football-data={match['football_data_match_id']}")
        if match["fbref_url"]:
            anchors.append("fbref=yes")
        if str(match.get("manual_anchor_mode") or "").strip().lower() == "smoke":
            anchors.append("manual=smoke")
        anchor_text = "<br>".join(anchors) if anchors else "无"
        issue_text = "<br>".join(match["issues"]) if match["issues"] else "无"
        lines.append(
            f"| `{match['index']:02d}` | `{match['english']}` | `{match['league'] or 'unknown'}` | `{match['mapping_source']}` | {anchor_text} | {issue_text} |"
        )
    if not report["matches"]:
        lines.append("| `--` | 无 | 无 | 无 | 无 | 无 |")
    lines.append("")

    lines.append("## 7. 球队档案诊断")
    lines.append("")
    lines.append("| 球队 | 联赛 | 档案状态 | 补强需求 | RAG 文档数 | 异常标记 |")
    lines.append("| --- | --- | --- | --- | ---: | --- |")
    for team in report["teams"]:
        archive_status = str(team.get("archive_status") or "missing")
        markers = "<br>".join(team["markers"]) if team["markers"] else "无"
        needs_enrichment = "yes" if team.get("needs_enrichment") else "no"
        lines.append(
            f"| `{team['team']}` | `{team['league']}` | `{archive_status}` | `{needs_enrichment}` | `{team['rag_doc_count']}` | {markers} |"
        )
    if not report["teams"]:
        lines.append("| `--` | 无 | 无 | 无 | `0` | 无 |")
    lines.append("")

    lines.append("## 8. 需要补强球队")
    lines.append("")
    enrichment_targets = [team for team in report["teams"] if team.get("needs_enrichment")]
    if enrichment_targets:
        lines.append("| 球队 | 联赛 | 档案状态 | 关键缺口 | 档案路径 |")
        lines.append("| --- | --- | --- | --- | --- |")
        for team in enrichment_targets:
            gaps = team.get("gaps") or []
            gap_text = "<br>".join(gaps[:5]) if gaps else "待人工复核"
            lines.append(
                f"| `{team['team']}` | `{team['league']}` | `{team['archive_status']}` | {gap_text} | `{team['archive_path']}` |"
            )
    else:
        lines.append("- 无")
    lines.append("")

    lines.append("## 9. 重点风险场次")
    lines.append("")
    if report["weak_matches"]:
        lines.append("| 场次 | 对阵 | Mapping | 风险原因 |")
        lines.append("| --- | --- | --- | --- |")
        for match in report["weak_matches"]:
            lines.append(
                f"| `{match['index']:02d}` | `{match['english']}` | `{match['mapping_source']}` | {'<br>'.join(match['issues'])} |"
            )
    else:
        lines.append("- 无")
    lines.append("")

    lines.append("## 10. Next Actions")
    if report["status"] == "BLOCKED":
        lines.append("1. 先修复 RAG 数据库或 team metadata 覆盖，再进入主流程。")
    elif report["status"] == "HOLD":
        if report.get("enrichment_needed_teams", 0) > 0:
            lines.append(f"1. 先补 `03_Match_Audits/{issue}/03_Review_Reports/TEAM-INTEL-{issue}.generated.json` 中列出的缺口，再执行 `team_archive_backfill.py`。")
            lines.append("2. 优先补 `placeholder` / `placeholder_backfilled` 队档的实质内容（新闻、战术上下文、物理指标），不要重复跑模板回填。")
            lines.append(f"3. 对 `unmapped` 比赛先补 `03_Match_Audits/{issue}/03_Review_Reports/UNMAPPED-ANCHORS-{issue}.generated.json` 锚点，再决定是否全量执行。")
            lines.append("4. 补录后按需重建或同步 RAG，再重新运行 `prematch_preflight.py --issue <issue>`。")
        else:
            lines.append(f"1. 本期队档结构已收敛，无需继续模板回填；下一步聚焦 `03_Match_Audits/{issue}/03_Review_Reports/UNMAPPED-ANCHORS-{issue}.generated.json` 锚点补录。")
            lines.append("2. 优先补强 RAG 薄样本（`thin_rag_docs`），把每队文档覆盖从 1 提升到可用阈值。")
            lines.append("3. 完成锚点与 RAG 补强后，重新运行 `prematch_preflight.py --issue <issue>`。")
    elif report["status"] == "CAUTION":
        lines.append(f"1. 优先查看 `03_Match_Audits/{issue}/03_Review_Reports/TEAM-INTEL-{issue}.generated.json`，补齐仍有缺口的球队。")
        lines.append("2. 先单场验证强队或已有实质档案的比赛。")
        lines.append("3. 并行补薄弱队档的实质内容，并在必要时重新同步 RAG。")
        if report.get("smoke_anchor_matches", 0) > 0:
            lines.append(f"4. 当前含 smoke 锚点 `{report['smoke_anchor_matches']}` 场，仅用于回归；生产前请先执行 `unmapped_anchor_seed.py --issue {issue} --clear-smoke` 并替换真实锚点。")
            lines.append("5. 补录后重新运行 `prematch_preflight.py --issue <issue>`。")
        else:
            lines.append("4. 补录后重新运行 `prematch_preflight.py --issue <issue>`。")
    else:
        lines.append("1. 可以继续执行 `python src/data/osint_pipeline.py --issue <issue>`。")
    lines.append("")

    return "\n".join(lines)


def write_report(vault_root: Path, issue: str, content: str) -> Path:
    issue_dir = vault_root / "03_Match_Audits" / str(issue)
    issue_dir.mkdir(parents=True, exist_ok=True)
    target = issue_dir / f"Audit-{issue}.md"
    target.write_text(content, encoding="utf-8")
    return target


def write_team_diagnostics(vault_root: Path, issue: str, report: Dict[str, Any]) -> Path:
    issue_dir = vault_root / "03_Match_Audits" / str(issue)
    issue_dir.mkdir(parents=True, exist_ok=True)
    target = issue_dir / f"Audit-{issue}-team-diagnostics.json"
    payload = {
        "issue": issue,
        "updated_at": report["updated_at"],
        "status": report["status"],
        "teams": [
            {
                "team": team["team"],
                "league": team["league"],
                "archive_path": team["archive_path"],
                "archive_status": team["archive_status"],
                "archive_quality": team.get("archive_quality"),
                "needs_enrichment": team.get("needs_enrichment", False),
                "gaps": team.get("gaps", []),
                "markers": team.get("markers", []),
                "stale_days": team.get("stale_days"),
                "rag_doc_count": team.get("rag_doc_count", 0),
            }
            for team in report["teams"]
        ],
    }
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def _extract_team_intel_snapshot(team: Dict[str, Any]) -> Dict[str, Any]:
    frontmatter = team.get("frontmatter") or {}
    intel_base = frontmatter.get("intel_base") if isinstance(frontmatter.get("intel_base"), dict) else {}
    tactical_logic = frontmatter.get("tactical_logic") if isinstance(frontmatter.get("tactical_logic"), dict) else {}
    physical_reality = (
        frontmatter.get("physical_reality") if isinstance(frontmatter.get("physical_reality"), dict) else {}
    )
    reality_gap = frontmatter.get("reality_gap") if isinstance(frontmatter.get("reality_gap"), dict) else {}

    payload: Dict[str, Any] = {
        "team": team["team"],
        "league": team["league"],
        "archive_status": team.get("archive_status"),
        "archive_quality": team.get("archive_quality"),
        "archive_path": team.get("archive_path"),
        "gaps": team.get("gaps", []),
        "markers": team.get("markers", []),
        "rag_doc_count": team.get("rag_doc_count", 0),
        "manager_doctrine": str(intel_base.get("manager_doctrine") or "").strip(),
        "market_sentiment": str(intel_base.get("market_sentiment") or "").strip(),
        "recent_news_summary": str(intel_base.get("recent_news_summary") or "").strip(),
        "key_node_dependency": intel_base.get("key_node_dependency") if isinstance(intel_base.get("key_node_dependency"), list) else [],
        "tactical_logic": tactical_logic,
        "avg_xG_last_5": physical_reality.get("avg_xG_last_5"),
        "conversion_efficiency": physical_reality.get("conversion_efficiency"),
        "defensive_leakage": physical_reality.get("defensive_leakage"),
        "actual_tactical_entropy": physical_reality.get("actual_tactical_entropy"),
        "bias_type": str(reality_gap.get("bias_type") or "").strip(),
        "S_dynamic_modifier": reality_gap.get("S_dynamic_modifier"),
        "prematch_focus_items": [],
    }
    return payload


def write_generated_intel_skeleton(vault_root: Path, issue: str, report: Dict[str, Any]) -> Path:
    issue_dir = vault_root / "03_Match_Audits" / str(issue) / "03_Review_Reports"
    issue_dir.mkdir(parents=True, exist_ok=True)
    target = issue_dir / f"TEAM-INTEL-{issue}.generated.json"
    teams = [_extract_team_intel_snapshot(team) for team in report["teams"] if team.get("needs_enrichment")]
    payload = {
        "issue": issue,
        "updated_at": report["updated_at"],
        "source": "prematch_preflight.py",
        "description": "Auto-generated enrichment skeleton. Fill substantive fields, then pass this file to team_archive_backfill.py --intel-file.",
        "teams": teams,
    }
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def write_unmapped_anchor_skeleton(vault_root: Path, issue: str, report: Dict[str, Any]) -> Path:
    issue_dir = vault_root / "03_Match_Audits" / str(issue) / "03_Review_Reports"
    issue_dir.mkdir(parents=True, exist_ok=True)
    target = issue_dir / f"UNMAPPED-ANCHORS-{issue}.generated.json"
    editable_target = issue_dir / f"UNMAPPED-ANCHORS-{issue}.json"
    matches = []
    for match in report.get("matches", []):
        if str(match.get("mapping_source") or "").lower() != "unmapped":
            continue
        matches.append(
            {
                "index": match.get("index"),
                "english": match.get("english"),
                "league": match.get("league"),
                "understat_id": None,
                "fbref_url": None,
                "football_data_match_id": None,
                "mapping_source": "manual_anchor",
                "anchor_mode": "production",
                "notes": "Fill at least one anchor field to override unmapped status.",
            }
        )
    payload = {
        "issue": issue,
        "updated_at": report["updated_at"],
        "source": "prematch_preflight.py",
        "description": "Auto-generated anchor override skeleton for unmapped fixtures.",
        "matches": matches,
    }
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if not editable_target.exists():
        editable_target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def main() -> int:
    parser = argparse.ArgumentParser(description="Ares Prematch Preflight Overview")
    parser.add_argument("--issue", required=True, help="中国体彩期号，如 26066")
    parser.add_argument("--engine-dir", required=False, help="显式指定 20-engine 仓库路径")
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent.parent.parent
    load_dotenv_into_env(base_dir)

    vault_env = os.getenv("ARES_VAULT_PATH")
    if not vault_env:
        raise EnvironmentError("未检测到 ARES_VAULT_PATH，无法生成 issue 预检总揽。")

    vault_root = Path(normalize_vault_path(vault_env)).expanduser()
    engine_dir = _resolve_engine_dir(args.engine_dir, base_dir)
    manifest_path = _resolve_manifest_path(vault_root, args.issue, base_dir)
    manifest = _load_manifest(manifest_path)

    report = build_preflight_report(
        issue=args.issue,
        base_dir=base_dir,
        vault_root=vault_root,
        engine_dir=engine_dir,
        manifest=manifest,
        manifest_path=manifest_path,
    )
    target = write_report(vault_root, args.issue, render_markdown(report))
    diagnostics_target = write_team_diagnostics(vault_root, args.issue, report)
    intel_skeleton_target = write_generated_intel_skeleton(vault_root, args.issue, report)
    unmapped_skeleton_target = write_unmapped_anchor_skeleton(vault_root, args.issue, report)
    logger.info("Prematch preflight 总揽已写入 -> %s", target)
    logger.info("Prematch preflight 诊断已写入 -> %s", diagnostics_target)
    logger.info("Prematch preflight intel skeleton 已写入 -> %s", intel_skeleton_target)
    logger.info("Prematch preflight unmapped skeleton 已写入 -> %s", unmapped_skeleton_target)
    logger.info("Issue=%s status=%s action=%s", args.issue, report["status"], report["recommended_action"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
