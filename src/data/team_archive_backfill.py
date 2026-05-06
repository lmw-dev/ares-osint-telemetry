import argparse
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from audit_router import AuditRouter, load_dotenv_into_env, normalize_vault_path
from team_forge import (
    DEFAULT_FRONTMATTER,
    build_markdown,
    ensure_team_archive,
    iter_issue_teams,
    merge_frontmatter_defaults,
    read_existing_content,
    write_markdown_safely,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("AresTelemetry.TeamArchiveBackfill")


PLACEHOLDER_MARKERS = (
    "Baseline profile initialized by `team_forge.py`",
    "Add tactical observations, injury patterns, and review snapshots below.",
    "待定向抓取补充",
    "待更新",
)


DEFAULT_TAGS = [
    "project/ares-v4/osint-telemetry",
    "area/team-archive",
    "type/note",
    "obsidian",
]

PLACEHOLDER_TEXT_VALUES = {"unknown", "待补充", "默认占位", "n/a", "none"}
DEFAULT_PHYSICAL_PROFILE = {
    "avg_xG_last_5": 1.0,
    "conversion_efficiency": 0.05,
    "defensive_leakage": 0.5,
    "actual_tactical_entropy": 0.4,
}
DEFAULT_RESILIENCE_CORE = {
    "concede_first_comeback_rate": None,
    "lead_protection_rate": None,
    "late_phase_resilience": None,
}
DEFAULT_MARKET_BEHAVIOR_CORE = {
    "opening_to_live_direction": "",
    "water_level_slope": None,
    "bookmaker_divergence_index": None,
}

LINEUP_ENUMS = {
    "expected_core_availability": {"FULL", "MOSTLY_AVAILABLE", "PARTIAL", "DAMAGED", "UNKNOWN"},
    "lineup_stability_precheck": {"GREEN", "YELLOW", "RED", "UNKNOWN"},
    "key_node_absence_risk": {"LOW", "MEDIUM", "HIGH", "CRITICAL", "UNKNOWN"},
}
ROTATION_ENUMS = {"LOW", "MEDIUM", "HIGH", "UNKNOWN"}
LINEUP_SNAPSHOT_STATUS_ENUMS = {"LIVE_OK", "LIVE_BLOCKED", "SEEDED", "UNKNOWN"}


def _split_match_english(english: str) -> Tuple[str, str]:
    if " vs " in english:
        home, away = english.split(" vs ", 1)
        return home.strip(), away.strip()
    if " VS " in english:
        home, away = english.split(" VS ", 1)
        return home.strip(), away.strip()
    return english.strip(), ""


def _inspect_placeholder(content: str) -> Dict[str, Any]:
    diagnostics = {
        "placeholder": False,
        "markers": [],
        "unknown_count": 0,
    }
    text = str(content or "")
    diagnostics["unknown_count"] = text.count("Unknown")
    markers = [marker for marker in PLACEHOLDER_MARKERS if marker in text]
    if diagnostics["unknown_count"] >= 5:
        markers.append("high_unknown_density")
    diagnostics["markers"] = markers
    diagnostics["placeholder"] = bool(markers)
    return diagnostics


def _load_manifest(vault_root: Path, issue: str) -> Dict[str, Any]:
    manifest_path = vault_root / "04_RAG_Raw_Data" / "Cold_Data_Lake" / f"{issue}_dispatch_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"找不到 dispatch manifest: {manifest_path}")
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _build_team_match_lookup(manifest: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    lookup: Dict[str, List[Dict[str, Any]]] = {}
    for match in manifest.get("matches", []):
        english = str(match.get("english") or "").strip()
        home, away = _split_match_english(english)
        row = {
            "index": int(match.get("index", 0) or 0),
            "english": english,
            "league": str(match.get("league") or ""),
            "mapping_source": str(match.get("mapping_source") or "unknown"),
        }
        for team in (home, away):
            if not team:
                continue
            lookup.setdefault(team, []).append(row)
    return lookup


def _merge_tags(frontmatter: Dict[str, Any]) -> List[str]:
    existing = frontmatter.get("tags")
    tags: List[str] = []
    if isinstance(existing, list):
        for item in existing:
            text = str(item).strip()
            if text and text not in tags:
                tags.append(text)
    for item in DEFAULT_TAGS:
        if item not in tags:
            tags.append(item)
    return tags


def _normalize_string_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _normalize_tactical_logic(value: Any) -> Dict[str, str]:
    keys = ["P", "Space", "F", "H", "Set_Piece"]
    result: Dict[str, str] = {}
    if isinstance(value, dict):
        for key in keys:
            raw = str(value.get(key) or "").strip()
            if raw:
                result[key] = raw
    return result


def _normalize_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_meaningful_text(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    return text.lower() not in PLACEHOLDER_TEXT_VALUES


def _is_meaningful_string_list(value: Any) -> bool:
    if not isinstance(value, list):
        return False
    return any(_is_meaningful_text(item) for item in value)


def _is_meaningful_tactical_logic(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    return any(_is_meaningful_text(raw) for raw in value.values())


def _normalize_intel_payload(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    normalized: Dict[str, Any] = {}
    for key in [
        "manager_doctrine",
        "market_sentiment",
        "recent_news_summary",
        "bias_type",
        "prematch_focus",
    ]:
        text = str(payload.get(key) or "").strip()
        if text:
            normalized[key] = text

    key_nodes = _normalize_string_list(payload.get("key_node_dependency"))
    if key_nodes:
        normalized["key_node_dependency"] = key_nodes

    tactical_logic = _normalize_tactical_logic(payload.get("tactical_logic"))
    if tactical_logic:
        normalized["tactical_logic"] = tactical_logic

    prematch_focus_items = _normalize_string_list(payload.get("prematch_focus_items"))
    if prematch_focus_items:
        normalized["prematch_focus_items"] = prematch_focus_items

    market_external_notes = _normalize_string_list(payload.get("market_external_notes"))
    if market_external_notes:
        normalized["market_external_notes"] = market_external_notes

    youtube_tactical_briefs = _normalize_string_list(payload.get("youtube_tactical_briefs"))
    if youtube_tactical_briefs:
        normalized["youtube_tactical_briefs"] = youtube_tactical_briefs
    source_items = payload.get("source_items")
    if isinstance(source_items, list):
        norm_sources: List[Dict[str, Any]] = []
        for item in source_items:
            if not isinstance(item, dict):
                continue
            source_name = str(item.get("source_name") or "").strip()
            url = str(item.get("url") or "").strip()
            fetched_at = str(item.get("fetched_at") or "").strip()
            reliability = str(item.get("reliability") or "").strip().upper() or "UNKNOWN"
            raw_status = str(item.get("raw_status") or "").strip().upper() or "UNKNOWN"
            if not source_name and not url:
                continue
            norm_sources.append(
                {
                    "source_name": source_name,
                    "url": url,
                    "fetched_at": fetched_at,
                    "reliability": reliability,
                    "raw_status": raw_status,
                }
            )
        if norm_sources:
            normalized["source_items"] = norm_sources

    absences = payload.get("absences")
    if isinstance(absences, list):
        norm_absences: List[Dict[str, Any]] = []
        for item in absences:
            if not isinstance(item, dict):
                continue
            player = str(item.get("player") or "").strip()
            if not player:
                continue
            status = str(item.get("status") or "UNKNOWN").strip().upper()
            role = str(item.get("role") or "UNKNOWN").strip().upper()
            confidence = str(item.get("confidence") or "UNKNOWN").strip().upper()
            source_url = str(item.get("source_url") or "").strip()
            fetched_at = str(item.get("fetched_at") or "").strip()
            key_node = bool(item.get("key_node"))
            impact_level = str(item.get("impact_level") or ("HIGH" if key_node else "MEDIUM")).strip().upper()
            norm_absences.append(
                {
                    "player": player,
                    "status": status,
                    "role": role,
                    "confidence": confidence,
                    "key_node": key_node,
                    "impact_level": impact_level,
                    "source_url": source_url,
                    "fetched_at": fetched_at,
                }
            )
        if norm_absences:
            normalized["absences"] = norm_absences

    lineup_risk_profile: Dict[str, str] = {}
    for key, allowed in LINEUP_ENUMS.items():
        raw = str(payload.get(key) or "UNKNOWN").strip().upper()
        lineup_risk_profile[key] = raw if raw in allowed else "UNKNOWN"
    normalized["lineup_risk_profile"] = lineup_risk_profile
    last_snapshot = payload.get("last_match_lineup_snapshot")
    if isinstance(last_snapshot, dict):
        source = last_snapshot.get("source") if isinstance(last_snapshot.get("source"), dict) else {}
        normalized["last_match_lineup_snapshot"] = {
            "source": {
                "provider": str(source.get("provider") or "").strip(),
                "url": str(source.get("url") or "").strip(),
                "fetched_at": str(source.get("fetched_at") or "").strip(),
            },
            "starting_xi": _normalize_string_list(last_snapshot.get("starting_xi")),
            "substitutes": _normalize_string_list(last_snapshot.get("substitutes")),
        }
    rotation = payload.get("lineup_rotation_signals")
    if isinstance(rotation, dict):
        level = str(rotation.get("rotation_intensity") or "UNKNOWN").strip().upper()
        if level not in ROTATION_ENUMS:
            level = "UNKNOWN"
        normalized["lineup_rotation_signals"] = {
            "rotation_intensity": level,
            "rotation_triggers": _normalize_string_list(rotation.get("rotation_triggers")),
            "confidence": str(rotation.get("confidence") or "LOW").strip().upper(),
        }
    lineup_snapshot_status = str(payload.get("lineup_snapshot_status") or "UNKNOWN").strip().upper()
    normalized["lineup_snapshot_status"] = (
        lineup_snapshot_status if lineup_snapshot_status in LINEUP_SNAPSHOT_STATUS_ENUMS else "UNKNOWN"
    )

    for key in [
        "avg_xG_last_5",
        "conversion_efficiency",
        "defensive_leakage",
        "actual_tactical_entropy",
        "S_dynamic_modifier",
    ]:
        value = _normalize_float(payload.get(key))
        if value is not None:
            normalized[key] = value

    resilience_core = payload.get("resilience_core")
    if isinstance(resilience_core, dict):
        normalized_core: Dict[str, Any] = {}
        for key in DEFAULT_RESILIENCE_CORE:
            value = _normalize_float(resilience_core.get(key))
            if value is not None:
                normalized_core[key] = value
        if normalized_core:
            normalized["resilience_core"] = normalized_core

    market_behavior_core = payload.get("market_behavior_core")
    if isinstance(market_behavior_core, dict):
        normalized_market: Dict[str, Any] = {}
        direction = str(market_behavior_core.get("opening_to_live_direction") or "").strip()
        if direction:
            normalized_market["opening_to_live_direction"] = direction
        for key in ("water_level_slope", "bookmaker_divergence_index"):
            value = _normalize_float(market_behavior_core.get(key))
            if value is not None:
                normalized_market[key] = value
        if normalized_market:
            normalized["market_behavior_core"] = normalized_market
    return normalized


def _build_intel_lookup(raw_payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    teams = raw_payload.get("teams")
    if not isinstance(teams, list):
        return lookup
    for item in teams:
        if not isinstance(item, dict):
            continue
        team = str(item.get("team") or "").strip()
        if not team:
            continue
        normalized = _normalize_intel_payload(item)
        if normalized:
            lookup[team] = normalized
    return lookup


def _load_issue_intel_payload(vault_root: Path, issue: str, explicit_path: Optional[str]) -> Tuple[Dict[str, Dict[str, Any]], Optional[Path]]:
    candidate_paths: List[Path] = []
    if explicit_path:
        candidate_paths.append(Path(explicit_path).expanduser())
    issue_dir = vault_root / "03_Match_Audits" / str(issue)
    candidate_paths.extend(
        [
            issue_dir / "03_Review_Reports" / f"TEAM-INTEL-{issue}.json",
            issue_dir / "03_Review_Reports" / f"TEAM-INTEL-{issue}.generated.json",
            issue_dir / f"TEAM-INTEL-{issue}.json",
        ]
    )

    for path in candidate_paths:
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        return _build_intel_lookup(payload), path
    return {}, None


def _load_preflight_diagnostics(vault_root: Path, issue: str) -> Dict[str, Dict[str, Any]]:
    target = vault_root / "03_Match_Audits" / str(issue) / f"Audit-{issue}-team-diagnostics.json"
    if not target.exists():
        return {}
    payload = json.loads(target.read_text(encoding="utf-8"))
    teams = payload.get("teams")
    if not isinstance(teams, list):
        return {}
    lookup: Dict[str, Dict[str, Any]] = {}
    for item in teams:
        if not isinstance(item, dict):
            continue
        team = str(item.get("team") or "").strip()
        if not team:
            continue
        lookup[team] = item
    return lookup


def _has_substantive_intel(intel: Dict[str, Any]) -> bool:
    if not intel:
        return False
    if _is_meaningful_text(intel.get("manager_doctrine")):
        return True
    if _is_meaningful_text(intel.get("recent_news_summary")):
        return True
    if _is_meaningful_text(intel.get("prematch_focus")):
        return True
    if _is_meaningful_string_list(intel.get("key_node_dependency")):
        return True
    if _is_meaningful_string_list(intel.get("prematch_focus_items")):
        return True
    if _is_meaningful_string_list(intel.get("market_external_notes")):
        return True
    if _is_meaningful_string_list(intel.get("youtube_tactical_briefs")):
        return True
    if _is_meaningful_tactical_logic(intel.get("tactical_logic")):
        return True
    if isinstance(intel.get("resilience_core"), dict) and any(
        _normalize_float(v) is not None for v in intel.get("resilience_core", {}).values()
    ):
        return True
    if isinstance(intel.get("market_behavior_core"), dict):
        market_core = intel.get("market_behavior_core", {})
        if _is_meaningful_text(market_core.get("opening_to_live_direction")):
            return True
        if _normalize_float(market_core.get("water_level_slope")) is not None:
            return True
        if _normalize_float(market_core.get("bookmaker_divergence_index")) is not None:
            return True
    for key in ["avg_xG_last_5", "conversion_efficiency", "defensive_leakage", "actual_tactical_entropy"]:
        if key in intel:
            value = _normalize_float(intel.get(key))
            default = DEFAULT_PHYSICAL_PROFILE[key]
            if value is not None and abs(value - default) > 1e-9:
                return True
    if _is_meaningful_text(intel.get("bias_type")) and str(intel.get("bias_type")).strip().lower() != "aligned":
        return True
    if _normalize_float(intel.get("S_dynamic_modifier")) not in (None, 0.0):
        return True
    if isinstance(intel.get("absences"), list) and any(str(item.get("player") or "").strip() for item in intel.get("absences", [])):
        return True
    profile = intel.get("lineup_risk_profile") if isinstance(intel.get("lineup_risk_profile"), dict) else {}
    if any(str(profile.get(key) or "UNKNOWN").strip().upper() != "UNKNOWN" for key in LINEUP_ENUMS):
        return True
    return False


def _derive_archive_quality(intel: Dict[str, Any], substantive_intel: bool) -> str:
    if not substantive_intel:
        return "placeholder_backfilled"
    resilience_core = intel.get("resilience_core") if isinstance(intel.get("resilience_core"), dict) else {}
    market_behavior_core = intel.get("market_behavior_core") if isinstance(intel.get("market_behavior_core"), dict) else {}
    resilience_ready = all(_normalize_float(resilience_core.get(key)) is not None for key in DEFAULT_RESILIENCE_CORE)
    market_ready = bool(str(market_behavior_core.get("opening_to_live_direction") or "").strip()) and all(
        _normalize_float(market_behavior_core.get(key)) is not None
        for key in ("water_level_slope", "bookmaker_divergence_index")
    )
    return "usable_strong" if resilience_ready and market_ready else "usable_weak"


def _frontmatter_text(frontmatter: Dict[str, Any], key: str, fallback: str = "") -> str:
    value = str(frontmatter.get(key) or "").strip()
    return value or fallback


def _frontmatter_list(frontmatter: Dict[str, Any], key: str) -> List[str]:
    value = frontmatter.get(key)
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _fmt_float(value: Any, digits: int = 2) -> str:
    numeric = _normalize_float(value)
    if numeric is None:
        return "unknown"
    return f"{numeric:.{digits}f}"


def _physical_signal_text(avg_xg: Any, conversion: Any, leakage: Any, entropy: Any) -> str:
    avg_xg_num = _normalize_float(avg_xg)
    conversion_num = _normalize_float(conversion)
    leakage_num = _normalize_float(leakage)
    entropy_num = _normalize_float(entropy)

    phrases: List[str] = []
    if avg_xg_num is not None:
        if avg_xg_num >= 1.6:
            phrases.append("进攻产量偏强")
        elif avg_xg_num >= 1.2:
            phrases.append("进攻产量中上")
        else:
            phrases.append("进攻产量偏保守")
    if conversion_num is not None:
        if conversion_num >= 1.1:
            phrases.append("终结效率偏热")
        elif conversion_num <= 0.9:
            phrases.append("终结效率偏冷")
    if leakage_num is not None:
        if leakage_num <= 0.4:
            phrases.append("防守泄漏控制较稳")
        elif leakage_num >= 0.65:
            phrases.append("防守泄漏偏高")
    if entropy_num is not None:
        if entropy_num >= 0.6:
            phrases.append("比赛波动较大")
        elif entropy_num <= 0.4:
            phrases.append("结构相对稳定")
    return "，".join(phrases) if phrases else "物理面仍偏基础占位"


def _build_prematch_summary(
    *,
    team: str,
    coach: str,
    base_formation: str,
    tactical_style: str,
    manager_doctrine: str,
    market_sentiment: str,
    recent_news_summary: str,
    key_nodes: List[str],
    tactical_logic: Dict[str, str],
    avg_xg: Any,
    conversion: Any,
    leakage: Any,
    entropy: Any,
    bias_type: str,
    matches: List[Dict[str, Any]],
) -> Dict[str, List[str]]:
    profile_lines: List[str] = []
    risk_lines: List[str] = []
    action_lines: List[str] = []

    identity_parts: List[str] = []
    if coach != "待补充":
        identity_parts.append(f"主帅为 `{coach}`")
    if base_formation != "待补充":
        identity_parts.append(f"常用阵型 `{base_formation}`")
    if tactical_style != "待补充":
        identity_parts.append(f"风格偏 `{tactical_style}`")
    elif manager_doctrine != "Unknown":
        identity_parts.append(f"战术画像为 `{manager_doctrine}`")
    if identity_parts:
        profile_lines.append("球队基础画像：" + "，".join(identity_parts) + "。")

    physical_sentence = _physical_signal_text(avg_xg, conversion, leakage, entropy)
    profile_lines.append(
        "物理面速读："
        f"`xG={_fmt_float(avg_xg)}`、`conv={_fmt_float(conversion)}`、`leakage={_fmt_float(leakage)}`、`entropy={_fmt_float(entropy)}`，{physical_sentence}。"
    )

    if key_nodes:
        focus_nodes = "、".join(key_nodes[:3])
        action_lines.append(f"赛前首先盯防/确认的核心节点：{focus_nodes}。")
    else:
        action_lines.append("当前缺少结构化核心节点名单，赛前需先确认首发核心、定位球主罚者和后防出球点。")

    tactical_parts: List[str] = []
    for key in ["P", "Space", "F", "H", "Set_Piece"]:
        value = str(tactical_logic.get(key) or "").strip()
        if value and value.lower() != "unknown":
            tactical_parts.append(f"{key}={value}")
    if tactical_parts:
        profile_lines.append("战术矩阵可直接作为对阵切入口：" + "，".join(tactical_parts) + "。")
    else:
        risk_lines.append("战术矩阵尚不完整，当前对空间利用、节奏控制和定位球路径的判断仍偏粗。")

    if recent_news_summary != "待补充":
        profile_lines.append(f"近期情报摘要：{recent_news_summary}")
    else:
        risk_lines.append("近期情报仍为空，赛前容易漏掉伤停、轮换、赛程密度和主帅表态带来的方向偏移。")

    if str(market_sentiment).strip() and market_sentiment != "Neutral":
        risk_lines.append(f"市场侧初步判断：{market_sentiment}")
    elif bias_type != "Aligned":
        risk_lines.append(f"市场偏差标签为 `{bias_type}`，可优先检查盘口是否存在系统性高估/低估。")
    else:
        risk_lines.append("市场侧暂未形成明确偏差标签，当前更适合作为基础底稿而非直接给强方向。")

    if isinstance(matches, list) and matches:
        opponents = " / ".join(str(match.get("english") or "").strip() for match in matches[:2] if str(match.get("english") or "").strip())
        if opponents:
            action_lines.append(f"本期关联场次：{opponents}。可直接把以上画像投射到该对阵。")

    avg_xg_num = _normalize_float(avg_xg)
    leakage_num = _normalize_float(leakage)
    conversion_num = _normalize_float(conversion)
    if avg_xg_num is not None and leakage_num is not None:
        if avg_xg_num >= 1.4 and leakage_num <= 0.45:
            risk_lines.append("这类球队在 prematch 上更怕被市场高估稳定性，尤其在强势题材下容易低估临场波动。")
        elif avg_xg_num < 1.1 and leakage_num >= 0.55:
            risk_lines.append("当前物理面偏脆，若赛前再出现伤停或轮换，比赛下限会明显被拉低。")
    if conversion_num is not None and conversion_num >= 1.25:
        risk_lines.append("近期终结效率偏热，赛前要警惕市场把短样本火力当作长期稳定输出。")
    elif conversion_num is not None and conversion_num <= 0.8:
        risk_lines.append("近期终结效率偏冷，若盘口明显下修，需分辨是运气回归还是进攻质量真实转弱。")

    action_lines.append("赛前操作上优先把这支队和 `最新伤停 + 临场盘口行为 + 对手风格克制关系` 交叉读取，再决定是否给方向。")

    if not profile_lines:
        profile_lines.append("当前仍以基础骨架为主，但已具备最小队伍画像，可先用于赛程锚定和对阵背景理解。")
    if not risk_lines:
        risk_lines.append("目前最大的不确定性仍来自赛前增量情报缺口，而不是基础画像缺失。")
    return {
        "profile": profile_lines,
        "risk": risk_lines,
        "action": action_lines,
    }


def _merge_intel_into_frontmatter(frontmatter: Dict[str, Any], intel: Dict[str, Any]) -> Dict[str, Any]:
    merged_frontmatter = merge_frontmatter_defaults(frontmatter, DEFAULT_FRONTMATTER)

    intel_base = dict(merged_frontmatter.get("intel_base") or {})
    if intel.get("manager_doctrine"):
        intel_base["manager_doctrine"] = intel["manager_doctrine"]
    if intel.get("market_sentiment"):
        intel_base["market_sentiment"] = intel["market_sentiment"]
    if intel.get("recent_news_summary"):
        intel_base["recent_news_summary"] = intel["recent_news_summary"]
    if intel.get("key_node_dependency"):
        intel_base["key_node_dependency"] = intel["key_node_dependency"]
    merged_frontmatter["intel_base"] = merge_frontmatter_defaults(intel_base, DEFAULT_FRONTMATTER["intel_base"])

    market_osint = dict(merged_frontmatter.get("market_osint") or {})
    if intel.get("market_external_notes"):
        market_osint["market_external_notes"] = intel["market_external_notes"]
    if intel.get("youtube_tactical_briefs"):
        market_osint["youtube_tactical_briefs"] = intel["youtube_tactical_briefs"]
    merged_frontmatter["market_osint"] = merge_frontmatter_defaults(market_osint, DEFAULT_FRONTMATTER["market_osint"])

    physical_reality = dict(merged_frontmatter.get("physical_reality") or {})
    for key in ["avg_xG_last_5", "conversion_efficiency", "defensive_leakage", "actual_tactical_entropy"]:
        if key in intel:
            physical_reality[key] = intel[key]
    merged_frontmatter["physical_reality"] = merge_frontmatter_defaults(physical_reality, DEFAULT_FRONTMATTER["physical_reality"])

    reality_gap = dict(merged_frontmatter.get("reality_gap") or {})
    if intel.get("bias_type"):
        reality_gap["bias_type"] = intel["bias_type"]
    if "S_dynamic_modifier" in intel:
        reality_gap["S_dynamic_modifier"] = intel["S_dynamic_modifier"]
    merged_frontmatter["reality_gap"] = merge_frontmatter_defaults(reality_gap, DEFAULT_FRONTMATTER["reality_gap"])

    existing_tactical_logic = merged_frontmatter.get("tactical_logic")
    tactical_logic = dict(existing_tactical_logic) if isinstance(existing_tactical_logic, dict) else {}
    if intel.get("tactical_logic"):
        tactical_logic.update(intel["tactical_logic"])
    if tactical_logic:
        merged_frontmatter["tactical_logic"] = tactical_logic

    if isinstance(intel.get("source_items"), list):
        merged_frontmatter["intel_source_items"] = intel.get("source_items", [])
    if isinstance(intel.get("absences"), list):
        merged_frontmatter["absences"] = intel.get("absences", [])
        injured_nodes = [
            str(item.get("player")).strip()
            for item in intel.get("absences", [])
            if isinstance(item, dict)
            and str(item.get("player") or "").strip()
            and str(item.get("status") or "").upper() in {"OUT", "DOUBTFUL", "RETURNING"}
        ]
        suspended_nodes = [
            str(item.get("player")).strip()
            for item in intel.get("absences", [])
            if isinstance(item, dict)
            and str(item.get("player") or "").strip()
            and str(item.get("status") or "").upper() == "SUSPENDED"
        ]
        merged_frontmatter["injured_nodes"] = sorted(set(injured_nodes))
        merged_frontmatter["suspended_nodes"] = sorted(set(suspended_nodes))
    if isinstance(intel.get("lineup_risk_profile"), dict):
        merged_frontmatter["lineup_risk_profile"] = intel.get("lineup_risk_profile", {})
    if isinstance(intel.get("last_match_lineup_snapshot"), dict):
        merged_frontmatter["last_match_lineup_snapshot"] = intel.get("last_match_lineup_snapshot", {})
    if str(intel.get("lineup_snapshot_status") or "").strip():
        merged_frontmatter["lineup_snapshot_status"] = str(intel.get("lineup_snapshot_status") or "UNKNOWN").upper()
    if isinstance(intel.get("lineup_rotation_signals"), dict):
        merged_frontmatter["lineup_rotation_signals"] = intel.get("lineup_rotation_signals", {})

    resilience_core = dict(merged_frontmatter.get("resilience_core") or {})
    resilience_core = merge_frontmatter_defaults(resilience_core, DEFAULT_RESILIENCE_CORE)
    if isinstance(intel.get("resilience_core"), dict):
        for key in DEFAULT_RESILIENCE_CORE:
            value = _normalize_float(intel["resilience_core"].get(key))
            if value is not None:
                resilience_core[key] = value
    merged_frontmatter["resilience_core"] = resilience_core

    market_behavior_core = dict(merged_frontmatter.get("market_behavior_core") or {})
    market_behavior_core = merge_frontmatter_defaults(market_behavior_core, DEFAULT_MARKET_BEHAVIOR_CORE)
    if isinstance(intel.get("market_behavior_core"), dict):
        direction = str(intel["market_behavior_core"].get("opening_to_live_direction") or "").strip()
        if direction:
            market_behavior_core["opening_to_live_direction"] = direction
        for key in ("water_level_slope", "bookmaker_divergence_index"):
            value = _normalize_float(intel["market_behavior_core"].get(key))
            if value is not None:
                market_behavior_core[key] = value
    merged_frontmatter["market_behavior_core"] = market_behavior_core

    return merged_frontmatter


def _update_manifest_context_flags_from_intel(manifest: Dict[str, Any], intel_lookup: Dict[str, Dict[str, Any]]) -> None:
    if not isinstance(manifest.get("matches"), list):
        return
    for match in manifest["matches"]:
        if not isinstance(match, dict):
            continue
        english = str(match.get("english") or "").strip()
        home, away = _split_match_english(english)
        home_intel = intel_lookup.get(home, {})
        away_intel = intel_lookup.get(away, {})
        home_profile = home_intel.get("lineup_risk_profile") if isinstance(home_intel.get("lineup_risk_profile"), dict) else {}
        away_profile = away_intel.get("lineup_risk_profile") if isinstance(away_intel.get("lineup_risk_profile"), dict) else {}

        flags = match.get("match_context_flags") if isinstance(match.get("match_context_flags"), dict) else {}
        flags["expected_core_availability"] = {
            "home": str(home_profile.get("expected_core_availability") or "UNKNOWN"),
            "away": str(away_profile.get("expected_core_availability") or "UNKNOWN"),
        }
        flags["lineup_stability_precheck"] = {
            "home": str(home_profile.get("lineup_stability_precheck") or "UNKNOWN"),
            "away": str(away_profile.get("lineup_stability_precheck") or "UNKNOWN"),
        }
        key_risk_home = str(home_profile.get("key_node_absence_risk") or "UNKNOWN")
        key_risk_away = str(away_profile.get("key_node_absence_risk") or "UNKNOWN")
        risk_rank = {"UNKNOWN": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
        dominant = key_risk_home if risk_rank.get(key_risk_home, 0) >= risk_rank.get(key_risk_away, 0) else key_risk_away
        flags["key_node_absence_risk"] = dominant
        home_rotation = (
            (home_intel.get("lineup_rotation_signals") or {}).get("rotation_intensity")
            if isinstance(home_intel.get("lineup_rotation_signals"), dict)
            else "UNKNOWN"
        )
        away_rotation = (
            (away_intel.get("lineup_rotation_signals") or {}).get("rotation_intensity")
            if isinstance(away_intel.get("lineup_rotation_signals"), dict)
            else "UNKNOWN"
        )
        rr = {"UNKNOWN": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3}
        home_rotation = str(home_rotation or "UNKNOWN").upper()
        away_rotation = str(away_rotation or "UNKNOWN").upper()
        flags["rotation_intensity"] = home_rotation if rr.get(home_rotation, 0) >= rr.get(away_rotation, 0) else away_rotation
        hs = str(home_intel.get("lineup_snapshot_status") or "UNKNOWN").upper()
        as_ = str(away_intel.get("lineup_snapshot_status") or "UNKNOWN").upper()
        sr = {"UNKNOWN": 0, "LIVE_BLOCKED": 1, "SEEDED": 2, "LIVE_OK": 3}
        flags["lineup_snapshot_status"] = hs if sr.get(hs, 0) >= sr.get(as_, 0) else as_
        match["match_context_flags"] = flags


def _render_body(
    team: str,
    league: str,
    issue: str,
    matches: List[Dict[str, Any]],
    intel: Optional[Dict[str, Any]] = None,
    frontmatter: Optional[Dict[str, Any]] = None,
) -> str:
    intel = intel or {}
    frontmatter = frontmatter or {}
    substantive = _has_substantive_intel(intel)
    archive_quality = _derive_archive_quality(intel, substantive)
    coach = _frontmatter_text(frontmatter, "coach", "待补充")
    base_formation = _frontmatter_text(frontmatter, "base_formation", "待补充")
    tactical_style = _frontmatter_text(frontmatter, "tactical_style", "待补充")
    manager_doctrine = str(
        intel.get("manager_doctrine")
        or ((frontmatter.get("intel_base") or {}).get("manager_doctrine"))
        or "Unknown"
    )
    market_sentiment = str(
        intel.get("market_sentiment")
        or ((frontmatter.get("intel_base") or {}).get("market_sentiment"))
        or "Neutral"
    )
    recent_news_summary = str(
        intel.get("recent_news_summary")
        or ((frontmatter.get("intel_base") or {}).get("recent_news_summary"))
        or "待补充"
    )
    key_nodes = _normalize_string_list(intel.get("key_node_dependency")) or _frontmatter_list(frontmatter, "key_node_dependency")
    prematch_focus_items = _normalize_string_list(intel.get("prematch_focus_items"))
    prematch_focus = str(intel.get("prematch_focus") or "")
    market_external_notes = _normalize_string_list(intel.get("market_external_notes"))
    youtube_tactical_briefs = _normalize_string_list(intel.get("youtube_tactical_briefs"))
    tactical_logic = _normalize_tactical_logic(intel.get("tactical_logic")) or _normalize_tactical_logic(frontmatter.get("tactical_logic"))
    bias_type = str(intel.get("bias_type") or ((frontmatter.get("reality_gap") or {}).get("bias_type")) or "Aligned")
    physical_reality = frontmatter.get("physical_reality") if isinstance(frontmatter.get("physical_reality"), dict) else {}
    avg_xg = intel.get("avg_xG_last_5", physical_reality.get("avg_xG_last_5", "默认占位"))
    conversion = intel.get("conversion_efficiency", physical_reality.get("conversion_efficiency", "默认占位"))
    leakage = intel.get("defensive_leakage", physical_reality.get("defensive_leakage", "默认占位"))
    entropy = intel.get("actual_tactical_entropy", physical_reality.get("actual_tactical_entropy", "默认占位"))
    resilience_core = frontmatter.get("resilience_core") if isinstance(frontmatter.get("resilience_core"), dict) else {}
    market_behavior_core = frontmatter.get("market_behavior_core") if isinstance(frontmatter.get("market_behavior_core"), dict) else {}
    known_signals = 0
    for value in [coach, base_formation, tactical_style]:
        if value != "待补充":
            known_signals += 1
    if manager_doctrine != "Unknown":
        known_signals += 1
    if key_nodes:
        known_signals += 1
    if recent_news_summary != "待补充":
        known_signals += 1
    if tactical_logic:
        known_signals += 1

    lines: List[str] = []
    lines.append(f"# {team}")
    lines.append("")
    lines.append("## 1. 档案状态")
    lines.append("")
    lines.append(f"- 当前状态：`{archive_quality}`")
    lines.append(f"- 回填来源：`team_archive_backfill.py --issue {issue}`")
    lines.append(f"- 所属联赛：`{league}`")
    lines.append(f"- 当前可用层级：`{'基础可用' if known_signals >= 4 else '低样本可用'}`")
    lines.append("")

    lines.append("## 2. 当前已知信息")
    lines.append("")
    lines.append(f"- 主教练：`{coach}`")
    lines.append(f"- 常用阵型：`{base_formation}`")
    lines.append(f"- 风格标签：`{tactical_style}`")
    lines.append(f"- Manager Doctrine：`{manager_doctrine}`")
    lines.append(f"- 市场情绪：`{market_sentiment}`")
    lines.append("")

    summary_sections = _build_prematch_summary(
        team=team,
        coach=coach,
        base_formation=base_formation,
        tactical_style=tactical_style,
        manager_doctrine=manager_doctrine,
        market_sentiment=market_sentiment,
        recent_news_summary=recent_news_summary,
        key_nodes=key_nodes,
        tactical_logic=tactical_logic,
        avg_xg=avg_xg,
        conversion=conversion,
        leakage=leakage,
        entropy=entropy,
        bias_type=bias_type,
        matches=matches,
    )
    lines.append("## 3. Prematch 摘要")
    lines.append("")
    lines.append("### 3.1 本队画像")
    lines.append("")
    for bullet in summary_sections["profile"]:
        lines.append(f"- {bullet}")
    lines.append("")
    lines.append("### 3.2 本场风险点")
    lines.append("")
    for bullet in summary_sections["risk"]:
        lines.append(f"- {bullet}")
    lines.append("")
    lines.append("### 3.3 赛前操作提示")
    lines.append("")
    for bullet in summary_sections["action"]:
        lines.append(f"- {bullet}")
    lines.append("")

    lines.append("## 4. Prematch 快照")
    lines.append("")
    lines.append("| 维度 | 当前值 | 说明 |")
    lines.append("| --- | --- | --- |")
    lines.append(f"| 核心节点依赖 | `{json.dumps(key_nodes, ensure_ascii=False)}` | {'可用于赛前关注' if key_nodes else '仍需补充关键人员依赖'} |")
    lines.append(f"| 近期新闻与舆论 | `{recent_news_summary}` | {'已有基础摘要' if recent_news_summary != '待补充' else '尚缺赛前新闻摘要'} |")
    lines.append(f"| 物理指标 | `xG={avg_xg}, conv={conversion}, leakage={leakage}, entropy={entropy}` | {'已有基础物理面' if str(avg_xg) != '默认占位' else '仍缺近 5 场核心物理指标'} |")
    lines.append(f"| Reality Gap | `{bias_type}` | {'可作为市场偏差起点' if bias_type != 'Aligned' else '尚未识别明显市场偏差'} |")
    lines.append(
        f"| 韧性三件套 | `comeback={resilience_core.get('concede_first_comeback_rate')}, lead={resilience_core.get('lead_protection_rate')}, late={resilience_core.get('late_phase_resilience')}` | "
        + ("结构完整 |" if all(_normalize_float(resilience_core.get(key)) is not None for key in DEFAULT_RESILIENCE_CORE) else "仍缺赛果韧性结构化数据 |")
    )
    lines.append(
        f"| 市场行为三件套 | `dir={market_behavior_core.get('opening_to_live_direction')}, slope={market_behavior_core.get('water_level_slope')}, div={market_behavior_core.get('bookmaker_divergence_index')}` | "
        + ("结构完整 |" if str(market_behavior_core.get('opening_to_live_direction') or '').strip() and all(_normalize_float(market_behavior_core.get(key)) is not None for key in ('water_level_slope', 'bookmaker_divergence_index')) else "仍缺盘口行为结构化数据 |")
    )
    lines.append("")

    lines.append("## 5. 本期关联比赛")
    lines.append("")
    if matches:
        lines.append("| Issue 场次 | 对阵 | Mapping Source |")
        lines.append("| --- | --- | --- |")
        for match in matches:
            lines.append(
                f"| `{match['index']:02d}` | `{match['english']}` | `{match['mapping_source']}` |"
            )
    else:
        lines.append("- 本期 manifest 未找到该队关联比赛。")
    lines.append("")

    lines.append("## 6. Prematch 关注项")
    lines.append("")
    lines.append(f"- 伤停与核心节点：{', '.join(key_nodes) if key_nodes else '待补充'}")
    if tactical_logic:
        lines.append(
            "- 战术矩阵（P / Space / F / H / Set Piece）："
            f"P={tactical_logic.get('P', '待补充')}, "
            f"Space={tactical_logic.get('Space', '待补充')}, "
            f"F={tactical_logic.get('F', '待补充')}, "
            f"H={tactical_logic.get('H', '待补充')}, "
            f"Set Piece={tactical_logic.get('Set_Piece', '待补充')}"
        )
    else:
        lines.append("- 战术矩阵（P / Space / F / H / Set Piece）：待补充")
    lines.append(f"- 近期新闻与情绪：{recent_news_summary}")
    if prematch_focus_items:
        for item in prematch_focus_items:
            lines.append(f"- Prematch Focus：{item}")
    elif prematch_focus:
        lines.append(f"- Prematch Focus：{prematch_focus}")
    else:
        lines.append("- 市场常见误判点：待补充")
    lines.append("")

    lines.append("## 7. 可用性判断")
    lines.append("")
    if substantive:
        lines.append("- 已具备最小实质情报，可进入下一轮 RAG 同步与 prematch 验证。")
        lines.append("- 仍建议继续补充更细的伤停、轮换、战术细节，避免样本过薄。")
    else:
        lines.append("- 当前队档可作为 `基础底稿` 使用，适合做赛程锚定、队伍画像和初步风险提醒。")
        lines.append("- 现阶段主要缺的是 `赛前增量情报`，而不是整份档案完全不可用。")
        lines.append("- 在没有补入新闻、伤停、盘口行为、韧性样本前，应按 `低样本 RAG` 理解，避免直接下强结论。")
    lines.append("")

    lines.append("## 8. 优先补强项")
    lines.append("")
    if not key_nodes:
        lines.append("- 优先补 `核心节点依赖`：首发核心、定位球主罚者、后防出球点。")
    if recent_news_summary == "待补充":
        lines.append("- 优先补 `赛前新闻摘要`：伤停、轮换、赛程密度、主帅表态。")
    if str(avg_xg) == "默认占位":
        lines.append("- 优先补 `近 5 场物理指标`：xG、转化率、防守泄漏、节奏变化。")
    if not tactical_logic:
        lines.append("- 优先补 `战术矩阵`：P / Space / F / H / Set Piece。")
    if not any(_normalize_float(resilience_core.get(key)) is not None for key in DEFAULT_RESILIENCE_CORE):
        lines.append("- 优先补 `韧性结构`：先丢球回追率、领先守成率、末段抗压。")
    if not str(market_behavior_core.get("opening_to_live_direction") or "").strip():
        lines.append("- 优先补 `盘口行为`：开盘到临场方向、水位斜率、分歧度。")
    if lines[-1] == "":
        lines.pop()
    if lines[-1] == "## 8. 优先补强项":
        lines.append("- 当前基础骨架完整，下一步优先补充赛前增量情报。")
    if lines[-1] != "":
        lines.append("")

    lines.append("## 9. 市面深度情报（外部观点）")
    lines.append("")
    lines.append("- 用途：沉淀盘口观点、媒体观点、以及后续 YouTube 大V 的技战术解析。")
    if market_external_notes:
        lines.append("- 市面观点摘要：")
        for item in market_external_notes:
            lines.append(f"  - {item}")
    else:
        lines.append("- 市面观点摘要：待补充")
    if youtube_tactical_briefs:
        lines.append("- YouTube 技战术要点：")
        for item in youtube_tactical_briefs:
            lines.append(f"  - {item}")
    else:
        lines.append("- YouTube 技战术要点：待补充")
    lines.append("- 建议格式：`来源/作者 + 观点 + 时间戳 + 与本队战术相关性`。")
    lines.append("")

    lines.append("## Next Actions")
    if substantive:
        lines.append("1. 将本队补料后的档案重新同步进 RAG。")
        lines.append("2. 补充更细的伤停、轮换与 tactical context，防止仍被 thin docs 卡死。")
        lines.append("3. 完成一轮补料后重新运行 `prematch_preflight.py --issue <issue>`。")
    else:
        lines.append("1. 补充主教练风格、核心节点依赖与近期新闻摘要。")
        lines.append("2. 补充至少近 5 场的物理特征与战术表现。")
        lines.append("3. 回填后重新运行 `prematch_preflight.py --issue <issue>`。")
    lines.append("")
    return "\n".join(lines)


def _backfill_one_team(
    *,
    vault_root: Path,
    issue: str,
    team: str,
    league: str,
    team_matches: List[Dict[str, Any]],
    intel: Optional[Dict[str, Any]],
    preflight_diagnostics: Optional[Dict[str, Any]],
    force: bool = False,
) -> Dict[str, Any]:
    # Always target canonical team path under the league directory to avoid
    # updating only an alias copy in 99_Alias_Archive.
    archive_path = ensure_team_archive(vault_root, team=team, league=league)
    if not archive_path.exists():
        merged_frontmatter = merge_frontmatter_defaults({}, DEFAULT_FRONTMATTER)
        content = build_markdown(
            merged_frontmatter,
            _render_body(team=team, league=league, issue=issue, matches=team_matches, frontmatter=merged_frontmatter),
        )
        write_markdown_safely(archive_path, content)
    frontmatter, body = read_existing_content(archive_path)
    original_text = build_markdown(frontmatter, body)
    diagnostics = _inspect_placeholder(original_text)
    preflight_diagnostics = preflight_diagnostics or {}
    needs_enrichment = bool(preflight_diagnostics.get("needs_enrichment"))

    substantive_intel = _has_substantive_intel(intel or {})
    if (not force) and (not diagnostics["placeholder"]) and needs_enrichment and not substantive_intel:
        return {
            "team": team,
            "league": league,
            "path": str(archive_path),
            "status": "flagged_needs_enrichment",
            "markers": sorted(set(diagnostics["markers"] + list(preflight_diagnostics.get("markers", [])))),
        }
    if (not force) and (not diagnostics["placeholder"]) and (not needs_enrichment) and (not substantive_intel):
        return {
            "team": team,
            "league": league,
            "path": str(archive_path),
            "status": "skipped_usable",
            "markers": diagnostics["markers"],
        }

    merged_frontmatter = _merge_intel_into_frontmatter(frontmatter, intel or {})
    merged_frontmatter["tags"] = _merge_tags(merged_frontmatter)
    merged_frontmatter["status"] = "active"
    merged_frontmatter["version"] = merged_frontmatter.get("version", 0.1)
    merged_frontmatter["project"] = merged_frontmatter.get("project", "Ares-Matrix-DB")
    merged_frontmatter["owner"] = merged_frontmatter.get("owner", "Ares")
    merged_frontmatter["current_league"] = merged_frontmatter.get("current_league", league)
    merged_frontmatter["archive_quality"] = _derive_archive_quality(intel or {}, substantive_intel)
    merged_frontmatter["last_modified_date"] = datetime.utcnow().strftime("%Y-%m-%d")
    merged_frontmatter["backfill_context"] = {
        "issue": issue,
        "updated_at_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ"),
        "script": "team_archive_backfill.py",
        "intel_enriched": substantive_intel,
    }
    if "creation_date" not in merged_frontmatter:
        merged_frontmatter["creation_date"] = datetime.utcnow().strftime("%Y-%m-%d")

    content = build_markdown(
        merged_frontmatter,
        _render_body(
            team=team,
            league=league,
            issue=issue,
            matches=team_matches,
            intel=intel,
            frontmatter=merged_frontmatter,
        ),
    )
    write_markdown_safely(archive_path, content)
    return {
        "team": team,
        "league": league,
        "path": str(archive_path),
        "status": "enriched_usable" if substantive_intel else "backfilled_placeholder",
        "markers": sorted(set(diagnostics["markers"] + list(preflight_diagnostics.get("markers", [])))),
    }


def _write_review_report(vault_root: Path, issue: str, results: List[Dict[str, Any]], intel_file_path: Optional[Path]) -> Path:
    router = AuditRouter(base_dir=Path(__file__).resolve().parent.parent.parent, vault_path=str(vault_root))
    issue_dirs = router._ensure_issue_dirs(issue)
    lines: List[str] = []
    lines.append(f"# Review {issue} - Team Archive Backfill")
    lines.append("")
    lines.append(f"- Updated At: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%SZ')}")
    lines.append(f"- Backfilled Placeholder Teams: {sum(1 for item in results if item['status'] == 'backfilled_placeholder')}")
    lines.append(f"- Enriched Usable Teams: {sum(1 for item in results if item['status'] == 'enriched_usable')}")
    lines.append(f"- Flagged Needs Enrichment Teams: {sum(1 for item in results if item['status'] == 'flagged_needs_enrichment')}")
    lines.append(f"- Skipped Usable Teams: {sum(1 for item in results if item['status'] == 'skipped_usable')}")
    lines.append(f"- Intel Input File: `{intel_file_path}`" if intel_file_path else "- Intel Input File: `None`")
    lines.append("")
    lines.append("## Results")
    for item in results:
        lines.append(
            f"- `{item['team']}` ({item['league']}) | status=`{item['status']}` | path=`{item['path']}`"
        )
    if not results:
        lines.append("- None")
    lines.append("")
    target = issue_dirs["review_dir"] / f"REVIEW-{issue}-Team_Archive_Backfill.md"
    target.write_text("\n".join(lines), encoding="utf-8")
    return target


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill placeholder Team Archives for an issue.")
    parser.add_argument("--issue", required=True, help="中国体彩期号，如 26066")
    parser.add_argument("--intel-file", required=False, help="结构化批量情报 JSON 文件；不传时自动尝试 issue 目录下的 TEAM-INTEL-<issue>.json")
    parser.add_argument("--force", action="store_true", help="强制回填所有本期球队档案（即使当前已判定 usable）。")
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent.parent.parent
    load_dotenv_into_env(base_dir)

    vault_env = os.getenv("ARES_VAULT_PATH")
    if not vault_env:
        raise EnvironmentError("未检测到 ARES_VAULT_PATH，无法执行 Team Archive 回填。")
    vault_root = Path(normalize_vault_path(vault_env)).expanduser()

    manifest = _load_manifest(vault_root, args.issue)
    team_match_lookup = _build_team_match_lookup(manifest)
    intel_lookup, intel_file_path = _load_issue_intel_payload(vault_root, args.issue, args.intel_file)
    preflight_lookup = _load_preflight_diagnostics(vault_root, args.issue)
    results: List[Dict[str, Any]] = []
    for team, league in iter_issue_teams(base_dir, vault_root, args.issue):
        result = _backfill_one_team(
            vault_root=vault_root,
            issue=args.issue,
            team=team,
            league=league,
            team_matches=team_match_lookup.get(team, []),
            intel=intel_lookup.get(team),
            preflight_diagnostics=preflight_lookup.get(team),
            force=args.force,
        )
        results.append(result)

    _update_manifest_context_flags_from_intel(manifest, intel_lookup)
    manifest_path = vault_root / "04_RAG_Raw_Data" / "Cold_Data_Lake" / f"{args.issue}_dispatch_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    report_path = _write_review_report(vault_root, args.issue, results, intel_file_path)
    logger.info(
        "Team Archive 回填完成 issue=%s, backfilled=%s, enriched=%s, skipped=%s, report=%s",
        args.issue,
        sum(1 for item in results if item["status"] == "backfilled_placeholder"),
        sum(1 for item in results if item["status"] == "enriched_usable"),
        sum(1 for item in results if item["status"] in {"skipped_usable", "flagged_needs_enrichment"}),
        report_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
