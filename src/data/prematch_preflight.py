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

CANONICAL_ARCHIVE_QUALITIES = {"usable", "placeholder", "placeholder_backfilled", "missing"}
PLACEHOLDER_ARCHIVE_STATUSES = {"placeholder", "placeholder_backfilled"}

TACTICAL_LOGIC_KEYS = ("P", "Space", "F", "H", "Set_Piece")
RESILIENCE_CORE_KEYS = (
    "concede_first_comeback_rate",
    "lead_protection_rate",
    "late_phase_resilience",
)
MARKET_BEHAVIOR_CORE_KEYS = (
    "opening_to_live_direction",
    "water_level_slope",
    "bookmaker_divergence_index",
)
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


def _resolve_manifest_path(vault_root: Path, run_id: str, base_dir: Path) -> Path:
    primary = vault_root / "04_RAG_Raw_Data" / "Cold_Data_Lake" / f"{run_id}_dispatch_manifest.json"
    if primary.exists():
        return primary
    fallback = base_dir / "raw_reports" / f"{run_id}_dispatch_manifest.json"
    if fallback.exists():
        return fallback
    raise FileNotFoundError(f"找不到 dispatch manifest: {primary}")


def _load_manifest(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_gate_snapshot(vault_root: Path, issue: str) -> Dict[str, Any]:
    gate_path = vault_root / "03_Match_Audits" / str(issue) / "03_Review_Reports" / f"REVIEW-{issue}-Prematch_Input_Gate.json"
    if not gate_path.exists():
        return {}
    try:
        payload = json.loads(gate_path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


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
    resilience_core = frontmatter.get("resilience_core")
    resilience_core = resilience_core if isinstance(resilience_core, dict) else {}
    market_behavior_core = frontmatter.get("market_behavior_core")
    market_behavior_core = market_behavior_core if isinstance(market_behavior_core, dict) else {}

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

    missing_resilience_keys: List[str] = []
    for key in RESILIENCE_CORE_KEYS:
        value = resilience_core.get(key)
        text_value = str(value or "").strip().lower()
        if value in (None, "") or text_value in {"unknown", "n/a", "na", "null"}:
            missing_resilience_keys.append(key)
    if not resilience_core:
        gaps.append("missing_resilience_core")
    elif missing_resilience_keys:
        gaps.append("incomplete_resilience_core")

    missing_market_behavior_keys: List[str] = []
    for key in MARKET_BEHAVIOR_CORE_KEYS:
        value = market_behavior_core.get(key)
        text_value = str(value or "").strip().lower()
        if value in (None, "") or text_value in {"unknown", "n/a", "na", "null"}:
            missing_market_behavior_keys.append(key)
    if not market_behavior_core:
        gaps.append("missing_market_behavior_core")
    elif missing_market_behavior_keys:
        gaps.append("incomplete_market_behavior_core")

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

    injured_nodes = frontmatter.get("injured_nodes") if isinstance(frontmatter.get("injured_nodes"), list) else []
    inactive_keywords = ("transferred", "inactive", "loaned out", "retired", "转会", "不再")
    if any(any(k in str(node).lower() for k in inactive_keywords) for node in injured_nodes):
        gaps.append("inactive_player_in_injured_nodes")

    avg_xg = _safe_float(physical_reality.get("avg_xG_last_5"))
    conversion = _safe_float(physical_reality.get("conversion_efficiency"))
    if avg_xg is not None and conversion is not None and avg_xg >= 1.0 and abs(conversion) < 1e-9:
        gaps.append("conversion_efficiency_suspicious_zero")

    return {
        "gaps": gaps,
        "missing_tactical_keys": missing_tactical_keys,
        "default_physical_fields": default_physical_fields,
        "missing_resilience_keys": missing_resilience_keys,
        "missing_market_behavior_keys": missing_market_behavior_keys,
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
        "archive_strength": "unknown",
        "frontmatter": {},
        "gaps": [],
        "missing_tactical_keys": [],
        "missing_resilience_keys": [],
        "missing_market_behavior_keys": [],
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
        diagnostics["archive_strength"] = "unknown"
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

    gap_diagnostics = _collect_archive_gaps(diagnostics["frontmatter"], text)

    if archive_quality in CANONICAL_ARCHIVE_QUALITIES:
        archive_status = archive_quality
        if archive_status == "placeholder_backfilled":
            markers.append("archive_quality_placeholder_backfilled")
    else:
        if archive_quality:
            markers.append(f"archive_quality_unrecognized:{archive_quality}")
        archive_status = "placeholder" if markers else "usable"

    diagnostics["archive_status"] = archive_status
    diagnostics["archive_strength"] = (
        "weak"
        if archive_status in PLACEHOLDER_ARCHIVE_STATUSES or gap_diagnostics["needs_enrichment"]
        else "strong"
        if archive_status == "usable"
        else "unknown"
    )
    diagnostics["placeholder"] = archive_status in PLACEHOLDER_ARCHIVE_STATUSES
    diagnostics["gaps"] = gap_diagnostics["gaps"]
    diagnostics["missing_tactical_keys"] = gap_diagnostics["missing_tactical_keys"]
    diagnostics["missing_resilience_keys"] = gap_diagnostics["missing_resilience_keys"]
    diagnostics["missing_market_behavior_keys"] = gap_diagnostics["missing_market_behavior_keys"]
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
    gate_snapshot = _load_gate_snapshot(vault_root, issue)
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

    leakage_bucket: Dict[str, int] = {}
    for record in team_records.values():
        pr = record.get("frontmatter", {}).get("physical_reality") if isinstance(record.get("frontmatter"), dict) else {}
        leakage = _safe_float(pr.get("defensive_leakage")) if isinstance(pr, dict) else None
        if leakage is None:
            continue
        key = f"{leakage:.2f}"
        leakage_bucket[key] = leakage_bucket.get(key, 0) + 1
    repeated_leakage_values = {k for k, v in leakage_bucket.items() if v >= 3}
    for record in team_records.values():
        pr = record.get("frontmatter", {}).get("physical_reality") if isinstance(record.get("frontmatter"), dict) else {}
        leakage = _safe_float(pr.get("defensive_leakage")) if isinstance(pr, dict) else None
        if leakage is None:
            continue
        key = f"{leakage:.2f}"
        if key in repeated_leakage_values:
            markers = record.get("markers") if isinstance(record.get("markers"), list) else []
            gaps = record.get("gaps") if isinstance(record.get("gaps"), list) else []
            markers.append("default_value_contamination:defensive_leakage")
            gaps.append("default_value_contamination:defensive_leakage")
            record["markers"] = sorted(set(markers))
            record["gaps"] = sorted(set(gaps))
            record["needs_enrichment"] = True

    mapping_counts = Counter(str(match.get("mapping_source") or "unknown") for match in manifest.get("matches", []))
    matches: List[Dict[str, Any]] = []
    weak_matches: List[Dict[str, Any]] = []
    for match in manifest.get("matches", []):
        english = str(match.get("english", "")).strip()
        home, away = _split_match_english(english)
        issues: List[str] = []
        mapping_source = str(match.get("mapping_source") or "unknown")
        titan_prematch = match.get("titan_prematch") if isinstance(match.get("titan_prematch"), dict) else {}
        titan_signals = titan_prematch.get("signals") if isinstance(titan_prematch.get("signals"), dict) else {}
        titan_coverage = str(titan_signals.get("coverage") or "none").strip().lower() or "none"
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
            elif str(record.get("archive_strength") or "unknown") == "weak":
                issues.append(f"weak_archive:{team}")
            elif archive_status == "usable" and record.get("needs_enrichment"):
                issues.append(f"archive_enrichment_required:{team}")
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
            "titan_prematch_coverage": titan_coverage,
            "titan_prematch_ok_pages": int(titan_signals.get("ok_page_count") or 0),
            "titan_prematch_total_pages": int(titan_signals.get("total_page_count") or 0),
            "manual_anchor_applied": bool(match.get("manual_anchor_applied")),
            "manual_anchor_mode": str(match.get("manual_anchor_mode") or "").strip().lower() or None,
            "manual_anchor_notes": str(match.get("manual_anchor_notes") or "").strip(),
            "issues": sorted(set(issues)),
        }
        matches.append(row)
        if row["issues"]:
            weak_matches.append(row)

    archive_status_counts = Counter(str(record.get("archive_status") or "missing") for record in team_records.values())
    strength_counts = Counter(str(record.get("archive_strength") or "unknown") for record in team_records.values())
    usable_teams = archive_status_counts.get("usable", 0)
    usable_strong_teams = strength_counts.get("strong", 0)
    usable_weak_teams = strength_counts.get("weak", 0)
    placeholder_teams = archive_status_counts.get("placeholder", 0)
    placeholder_backfilled_teams = archive_status_counts.get("placeholder_backfilled", 0)
    missing_teams = archive_status_counts.get("missing", 0)
    low_quality_teams = usable_weak_teams
    thin_rag_teams = sum(1 for record in team_records.values() if record["rag_doc_count"] <= 1)
    enrichment_needed_teams = sum(1 for record in team_records.values() if record.get("needs_enrichment"))
    resilience_gap_teams = sum(1 for record in team_records.values() if record.get("missing_resilience_keys"))
    market_behavior_gap_teams = sum(1 for record in team_records.values() if record.get("missing_market_behavior_keys"))
    total_matches = len(matches)
    total_teams = len(team_records)
    unmapped_matches = mapping_counts.get("unmapped", 0)
    smoke_anchor_matches = sum(1 for row in matches if "smoke_anchor_fixture" in row.get("issues", []))
    titan_prematch_available_matches = sum(
        1 for row in matches if str(row.get("titan_prematch_coverage") or "none") in {"full", "partial"}
    )
    titan_prematch_full_matches = sum(
        1 for row in matches if str(row.get("titan_prematch_coverage") or "none") == "full"
    )
    titan_prematch_missing_matches = max(0, total_matches - titan_prematch_available_matches)
    market_fallback_ready_matches = sum(
        1
        for m in manifest.get("matches", [])
        if isinstance(m.get("market_odds_history"), list) and len(m.get("market_odds_history")) > 0
    )

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

    gate_status = str(gate_snapshot.get("issue_status") or "").strip().upper()
    gate_selected = int(gate_snapshot.get("selected_matches") or 0) if gate_snapshot else 0
    gate_filtered = int(gate_snapshot.get("filtered_matches") or 0) if gate_snapshot else 0
    gate_rows = gate_snapshot.get("rows") if isinstance(gate_snapshot.get("rows"), list) else []
    if gate_status == "BLOCKED":
        status = "BLOCKED"
        recommended_action = "Prematch Input Gate 当前阻断，需先补齐队档/RAG/补强项后再推进。"
    elif gate_status == "HOLD" and status != "BLOCKED":
        status = "HOLD"
        recommended_action = "Prematch Input Gate 当前仅允许部分或暂不建议推进，请先完成最小补料清单。"
    elif not gate_snapshot and status not in {"BLOCKED"}:
        status = "HOLD"
        recommended_action = "Prematch Input Gate 快照缺失，需先生成 gate 产物后再信任 READY 结论。"

    if titan_prematch_missing_matches > 0:
        if market_fallback_ready_matches <= 0:
            status = "HOLD"
            recommended_action = "Titan 缺失且无市场 fallback 证据，需先补齐市场行为源。"

    summary = [
        f"manifest 已落盘：`{manifest_path}`",
        f"本期共 `{total_matches}` 场，`mapping_source=unmapped` 有 `{unmapped_matches}` 场。",
        f"本期使用 smoke 锚点的比赛有 `{smoke_anchor_matches}` 场（仅回归测试，不视为生产可用映射）。",
        f"Titan prematch 覆盖 `{titan_prematch_available_matches}/{total_matches}` 场（full `{titan_prematch_full_matches}` 场，missing `{titan_prematch_missing_matches}` 场）。",
        (
            f"Market fallback（500 等）可用 `{market_fallback_ready_matches}/{total_matches}` 场。"
            if market_fallback_ready_matches > 0
            else "Market fallback（500 等）当前不可用。"
        ),
        f"本期球队共 `{total_teams}` 支：usable_strong `{usable_strong_teams}`、usable_weak `{usable_weak_teams}`、placeholder `{placeholder_teams}`、placeholder_backfilled `{placeholder_backfilled_teams}`、missing `{missing_teams}`。",
        f"低质量/待补强队档（usable_weak + placeholder + placeholder_backfilled）共 `{low_quality_teams}` 支。",
        f"需要补强的球队共 `{enrichment_needed_teams}` 支（含结构缺口、过期时间戳、默认物理值、缺新闻摘要等）。",
        f"其中 resilience_core 缺口球队 `{resilience_gap_teams}` 支，market_behavior_core 缺口球队 `{market_behavior_gap_teams}` 支。",
        f"RAG team metadata 覆盖 `{len(rag_readiness['covered_teams'])}/{len(rag_readiness['issue_teams'])}`，但 `thin_rag_docs` 球队有 `{thin_rag_teams}` 支。",
        (
            f"defensive_leakage 重复值疑似默认污染：{', '.join(sorted(repeated_leakage_values))}（命中>=3队）。"
            if repeated_leakage_values
            else "defensive_leakage 未检测到明显重复默认污染。"
        ),
        (
            f"Prematch Input Gate: `{gate_status or 'UNKNOWN'}`，"
            f"selected `{gate_selected}` / total `{total_matches}` / filtered `{gate_filtered}`。"
            if gate_snapshot
            else "Prematch Input Gate: 尚未生成 gate 快照（按规则应 HOLD，不可直接视为 READY）。"
        ),
        recommended_action,
    ]
    primary_hold_reasons: List[str] = []
    if market_fallback_ready_matches <= 0 and titan_prematch_missing_matches > 0:
        primary_hold_reasons.append("MARKET_SOURCE_MISSING")
    if not gate_snapshot:
        primary_hold_reasons.append("PREMATCH_INPUT_GATE_MISSING")
    if enrichment_needed_teams > 0:
        primary_hold_reasons.append("TEAM_ARCHIVE_ENRICHMENT_REQUIRED")
    if any("inactive_player_in_injured_nodes" in (r.get("gaps") or []) for r in team_records.values()):
        primary_hold_reasons.append("PLAYER_NODE_CONTAMINATION")
    if any("conversion_efficiency_suspicious_zero" in (r.get("gaps") or []) for r in team_records.values()):
        primary_hold_reasons.append("METRIC_SANITY_UNRESOLVED")

    if gate_snapshot and gate_rows:
        weak_matches = []
        for row in gate_rows:
            reasons = row.get("reasons") if isinstance(row.get("reasons"), list) else []
            if reasons:
                weak_matches.append(
                    {
                        "index": int(row.get("index") or 0),
                        "english": str(row.get("match") or ""),
                        "mapping_source": "gate",
                        "issues": [str(item) for item in reasons],
                    }
                )

    return {
        "issue": issue,
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ"),
        "status": status,
        "recommended_action": recommended_action,
        "manifest_path": str(manifest_path),
        "engine_dir": str(engine_dir),
        "mapping_counts": dict(sorted(mapping_counts.items())),
        "rag_readiness": rag_readiness,
        "gate_snapshot": gate_snapshot,
        "summary": summary,
        "primary_hold_reasons": primary_hold_reasons,
        "matches": matches,
        "weak_matches": weak_matches,
        "teams": sorted(team_records.values(), key=lambda item: (item["league"], item["team"])),
        "usable_team_archives": usable_teams,
        "usable_strong_team_archives": usable_strong_teams,
        "usable_weak_team_archives": usable_weak_teams,
        "placeholder_team_archives": placeholder_teams,
        "placeholder_backfilled_team_archives": placeholder_backfilled_teams,
        "low_quality_team_archives": low_quality_teams,
        "enrichment_needed_teams": enrichment_needed_teams,
        "resilience_gap_teams": resilience_gap_teams,
        "market_behavior_gap_teams": market_behavior_gap_teams,
        "missing_team_archives": missing_teams,
        "thin_rag_teams": thin_rag_teams,
        "unmapped_matches": unmapped_matches,
        "smoke_anchor_matches": smoke_anchor_matches,
        "titan_prematch_available_matches": titan_prematch_available_matches,
        "titan_prematch_full_matches": titan_prematch_full_matches,
        "titan_prematch_missing_matches": titan_prematch_missing_matches,
        "market_fallback_ready_matches": market_fallback_ready_matches,
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
    lines.append(f"| Titan Prematch 覆盖场次 | `{report['titan_prematch_available_matches']}` |")
    lines.append(f"| Titan Prematch Full 场次 | `{report['titan_prematch_full_matches']}` |")
    lines.append(f"| Titan Prematch Missing 场次 | `{report['titan_prematch_missing_matches']}` |")
    lines.append(f"| 球队总数 | `{report['total_teams']}` |")
    lines.append(f"| Legacy Usable 队档 | `{report['usable_team_archives']}` |")
    lines.append(f"| Usable Strong 队档 | `{report.get('usable_strong_team_archives', 0)}` |")
    lines.append(f"| Usable Weak 队档 | `{report.get('usable_weak_team_archives', 0)}` |")
    lines.append(f"| Placeholder 队档 | `{report['placeholder_team_archives']}` |")
    lines.append(f"| Placeholder Backfilled 队档 | `{report['placeholder_backfilled_team_archives']}` |")
    lines.append(f"| 低质量/待补强队档 | `{report['low_quality_team_archives']}` |")
    lines.append(f"| 需要补强球队 | `{report['enrichment_needed_teams']}` |")
    lines.append(f"| Resilience Core 缺口球队 | `{report.get('resilience_gap_teams', 0)}` |")
    lines.append(f"| Market Behavior Core 缺口球队 | `{report.get('market_behavior_gap_teams', 0)}` |")
    lines.append(f"| 缺失队档 | `{report['missing_team_archives']}` |")
    lines.append(f"| Thin RAG Docs 球队 | `{report['thin_rag_teams']}` |")
    lines.append(f"| Market Fallback 可用场次 | `{report.get('market_fallback_ready_matches', 0)}` |")
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
    lines.append("| 场次 | 对阵 | 联赛 | Mapping | Titan Prematch | 外部锚点 | 风险信号 |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- |")
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
        titan_text = f"{match.get('titan_prematch_coverage', 'none')} ({match.get('titan_prematch_ok_pages', 0)}/{match.get('titan_prematch_total_pages', 0)})"
        issue_text = "<br>".join(match["issues"]) if match["issues"] else "无"
        lines.append(
            f"| `{match['index']:02d}` | `{match['english']}` | `{match['league'] or 'unknown'}` | `{match['mapping_source']}` | `{titan_text}` | {anchor_text} | {issue_text} |"
        )
    if not report["matches"]:
        lines.append("| `--` | 无 | 无 | 无 | 无 | 无 | 无 |")
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
        hold_reasons = set(report.get("primary_hold_reasons") or [])
        step = 1
        if "MARKET_SOURCE_MISSING" in hold_reasons:
            lines.append(f"{step}. 先补齐市场源：Titan 或 500 fallback，目标 `Market Fallback 可用场次 = 比赛总数`。")
            step += 1
        if "PREMATCH_INPUT_GATE_MISSING" in hold_reasons:
            lines.append(f"{step}. 生成 Prematch Input Gate 三件套：`REVIEW-{issue}-Prematch_Input_Gate.md`、`REVIEW-{issue}-Team_Enrichment_Queue.md`、`TEAM-ENRICHMENT-QUEUE-{issue}.json`。")
            step += 1
        if "TEAM_ARCHIVE_ENRICHMENT_REQUIRED" in hold_reasons:
            lines.append(f"{step}. 仅针对 `needs_enrichment=true` 球队补料：先填 `TEAM-INTEL-{issue}.generated.json`，再跑 `team_archive_backfill.py`。")
            step += 1
        if "PLAYER_NODE_CONTAMINATION" in hold_reasons:
            lines.append(f"{step}. 清洗球员节点污染（如已转会球员从 `injured_nodes` 移出并转入 inactive/transfer context）。")
            step += 1
        if "METRIC_SANITY_UNRESOLVED" in hold_reasons:
            lines.append(f"{step}. 校验可疑指标来源（如 `conversion_efficiency=0` 与高 xG 冲突），确认后再允许作为方向证据。")
            step += 1
        lines.append(f"{step}. 完成上述项后重跑 `prematch_preflight.py --issue {issue}`。")
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
    def _team_class_hint(team_name: str) -> str:
        elite_teams = {
            "arsenal",
            "liverpool",
            "manchester united",
            "manchester city",
            "chelsea",
            "tottenham hotspur",
            "newcastle united",
            "bayern munich",
            "bayer leverkusen",
            "borussia dortmund",
            "rasenballsport leipzig",
            "rb leipzig",
            "juventus",
            "inter milan",
            "ac milan",
            "napoli",
            "atalanta",
            "barcelona",
            "real madrid",
            "atletico madrid",
            "athletic club",
            "paris saint-germain",
            "paris saint germain",
            "psg",
        }
        low = str(team_name or "").strip().lower()
        if low in elite_teams:
            return "elite_depth"
        return "standard"

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
                "archive_strength": team.get("archive_strength"),
                "needs_enrichment": team.get("needs_enrichment", False),
                "gaps": team.get("gaps", []),
                "markers": team.get("markers", []),
                "missing_resilience_keys": team.get("missing_resilience_keys", []),
                "missing_market_behavior_keys": team.get("missing_market_behavior_keys", []),
                "stale_days": team.get("stale_days"),
                "rag_doc_count": team.get("rag_doc_count", 0),
                "team_class_hint": _team_class_hint(team["team"]),
                "injured_nodes": (
                    team.get("frontmatter", {}).get("injured_nodes")
                    if isinstance(team.get("frontmatter", {}).get("injured_nodes"), list)
                    else []
                ),
                "suspended_nodes": (
                    team.get("frontmatter", {}).get("suspended_nodes")
                    if isinstance(team.get("frontmatter", {}).get("suspended_nodes"), list)
                    else []
                ),
                "avg_xG_last_5": (
                    team.get("frontmatter", {}).get("physical_reality", {}).get("avg_xG_last_5")
                    if isinstance(team.get("frontmatter", {}).get("physical_reality"), dict)
                    else None
                ),
                "conversion_efficiency": (
                    team.get("frontmatter", {}).get("physical_reality", {}).get("conversion_efficiency")
                    if isinstance(team.get("frontmatter", {}).get("physical_reality"), dict)
                    else None
                ),
                "conversion_zero_gate_explain": (
                    "triggered: avg_xG_last_5 >= 1.0 and conversion_efficiency == 0.0"
                    if (
                        _safe_float(
                            team.get("frontmatter", {}).get("physical_reality", {}).get("avg_xG_last_5")
                            if isinstance(team.get("frontmatter", {}).get("physical_reality"), dict)
                            else None
                        )
                        is not None
                        and _safe_float(
                            team.get("frontmatter", {}).get("physical_reality", {}).get("avg_xG_last_5")
                            if isinstance(team.get("frontmatter", {}).get("physical_reality"), dict)
                            else None
                        )
                        >= 1.0
                        and _safe_float(
                            team.get("frontmatter", {}).get("physical_reality", {}).get("conversion_efficiency")
                            if isinstance(team.get("frontmatter", {}).get("physical_reality"), dict)
                            else None
                        )
                        == 0.0
                    )
                    else "not_triggered: avg_xG_last_5 below 1.0 threshold or conversion_efficiency non-zero"
                ),
                "defensive_leakage": (
                    team.get("frontmatter", {}).get("physical_reality", {}).get("defensive_leakage")
                    if isinstance(team.get("frontmatter", {}).get("physical_reality"), dict)
                    else None
                ),
            }
            for team in report["teams"]
        ],
    }
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def _extract_team_intel_snapshot(team: Dict[str, Any]) -> Dict[str, Any]:
    frontmatter = team.get("frontmatter") or {}
    intel_base = frontmatter.get("intel_base") if isinstance(frontmatter.get("intel_base"), dict) else {}
    market_osint = frontmatter.get("market_osint") if isinstance(frontmatter.get("market_osint"), dict) else {}
    resilience_core = frontmatter.get("resilience_core") if isinstance(frontmatter.get("resilience_core"), dict) else {}
    market_behavior_core = (
        frontmatter.get("market_behavior_core") if isinstance(frontmatter.get("market_behavior_core"), dict) else {}
    )
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
        "archive_strength": team.get("archive_strength"),
        "archive_path": team.get("archive_path"),
        "gaps": team.get("gaps", []),
        "markers": team.get("markers", []),
        "missing_resilience_keys": team.get("missing_resilience_keys", []),
        "missing_market_behavior_keys": team.get("missing_market_behavior_keys", []),
        "rag_doc_count": team.get("rag_doc_count", 0),
        "manager_doctrine": str(intel_base.get("manager_doctrine") or "").strip(),
        "market_sentiment": str(intel_base.get("market_sentiment") or "").strip(),
        "recent_news_summary": str(intel_base.get("recent_news_summary") or "").strip(),
        "key_node_dependency": intel_base.get("key_node_dependency") if isinstance(intel_base.get("key_node_dependency"), list) else [],
        "tactical_logic": tactical_logic,
        "resilience_core": resilience_core,
        "market_behavior_core": market_behavior_core,
        "avg_xG_last_5": physical_reality.get("avg_xG_last_5"),
        "conversion_efficiency": physical_reality.get("conversion_efficiency"),
        "defensive_leakage": physical_reality.get("defensive_leakage"),
        "actual_tactical_entropy": physical_reality.get("actual_tactical_entropy"),
        "bias_type": str(reality_gap.get("bias_type") or "").strip(),
        "S_dynamic_modifier": reality_gap.get("S_dynamic_modifier"),
        "prematch_focus_items": [],
        "market_external_notes": market_osint.get("market_external_notes")
        if isinstance(market_osint.get("market_external_notes"), list)
        else [],
        "youtube_tactical_briefs": market_osint.get("youtube_tactical_briefs")
        if isinstance(market_osint.get("youtube_tactical_briefs"), list)
        else [],
        "source_items": [],
        "absences": [],
        "expected_core_availability": "UNKNOWN",
        "lineup_stability_precheck": "UNKNOWN",
        "key_node_absence_risk": "UNKNOWN",
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
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--issue", help="中国体彩期号，如 26066")
    mode_group.add_argument("--date", help="按日期分析，格式 YYYYMMDD，如 20260502")
    parser.add_argument("--scope", default="top5", help="date 模式范围，当前支持 top5")
    parser.add_argument("--engine-dir", required=False, help="显式指定 20-engine 仓库路径")
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent.parent.parent
    load_dotenv_into_env(base_dir)

    vault_env = os.getenv("ARES_VAULT_PATH")
    if not vault_env:
        raise EnvironmentError("未检测到 ARES_VAULT_PATH，无法生成 issue 预检总揽。")

    vault_root = Path(normalize_vault_path(vault_env)).expanduser()
    engine_dir = _resolve_engine_dir(args.engine_dir, base_dir)
    run_id = args.issue if args.issue else f"DATE-{args.date}-{str(args.scope or 'top5').strip().lower()}"
    manifest_path = _resolve_manifest_path(vault_root, run_id, base_dir)
    manifest = _load_manifest(manifest_path)

    report = build_preflight_report(
        issue=run_id,
        base_dir=base_dir,
        vault_root=vault_root,
        engine_dir=engine_dir,
        manifest=manifest,
        manifest_path=manifest_path,
    )
    target = write_report(vault_root, run_id, render_markdown(report))
    diagnostics_target = write_team_diagnostics(vault_root, run_id, report)
    intel_skeleton_target = write_generated_intel_skeleton(vault_root, run_id, report)
    unmapped_skeleton_target = write_unmapped_anchor_skeleton(vault_root, run_id, report)
    logger.info("Prematch preflight 总揽已写入 -> %s", target)
    logger.info("Prematch preflight 诊断已写入 -> %s", diagnostics_target)
    logger.info("Prematch preflight intel skeleton 已写入 -> %s", intel_skeleton_target)
    logger.info("Prematch preflight unmapped skeleton 已写入 -> %s", unmapped_skeleton_target)
    logger.info("Issue=%s status=%s action=%s", run_id, report["status"], report["recommended_action"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
