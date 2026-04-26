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
    build_archive_path,
    build_markdown,
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
    return False


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

    return merged_frontmatter


def _render_body(team: str, league: str, issue: str, matches: List[Dict[str, Any]], intel: Optional[Dict[str, Any]] = None) -> str:
    intel = intel or {}
    substantive = _has_substantive_intel(intel)
    archive_quality = "usable" if substantive else "placeholder_backfilled"
    manager_doctrine = str(intel.get("manager_doctrine") or "Unknown")
    market_sentiment = str(intel.get("market_sentiment") or "Neutral")
    recent_news_summary = str(intel.get("recent_news_summary") or "待补充")
    key_nodes = _normalize_string_list(intel.get("key_node_dependency"))
    prematch_focus_items = _normalize_string_list(intel.get("prematch_focus_items"))
    prematch_focus = str(intel.get("prematch_focus") or "")
    market_external_notes = _normalize_string_list(intel.get("market_external_notes"))
    youtube_tactical_briefs = _normalize_string_list(intel.get("youtube_tactical_briefs"))
    tactical_logic = _normalize_tactical_logic(intel.get("tactical_logic"))
    bias_type = str(intel.get("bias_type") or "Aligned")
    avg_xg = intel.get("avg_xG_last_5", "默认占位")
    conversion = intel.get("conversion_efficiency", "默认占位")
    leakage = intel.get("defensive_leakage", "默认占位")
    entropy = intel.get("actual_tactical_entropy", "默认占位")

    lines: List[str] = []
    lines.append(f"# {team}")
    lines.append("")
    lines.append("## 1. 档案状态")
    lines.append("")
    lines.append(f"- 当前状态：`{archive_quality}`")
    lines.append(f"- 回填来源：`team_archive_backfill.py --issue {issue}`")
    lines.append(f"- 所属联赛：`{league}`")
    lines.append("")

    lines.append("## 2. 基础信息待补")
    lines.append("")
    lines.append("| 维度 | 当前值 | 说明 |")
    lines.append("| --- | --- | --- |")
    lines.append(f"| 主教练风格 | `{manager_doctrine}` | {'已补充' if manager_doctrine != 'Unknown' else '需要补充 manager doctrine / 节奏偏好 / 轮换习惯'} |")
    lines.append(f"| 核心节点依赖 | `{json.dumps(key_nodes, ensure_ascii=False)}` | {'已补充' if key_nodes else '需要补充 key players / dependency'} |")
    lines.append(f"| 市场情绪 | `{market_sentiment}` | {'已补充近期新闻方向' if recent_news_summary != '待补充' else '需要补充近期新闻与舆论方向'} |")
    lines.append(f"| 物理指标 | `xG={avg_xg}, conv={conversion}, leakage={leakage}, entropy={entropy}` | {'已补充核心物理指标' if substantive and 'avg_xG_last_5' in intel else '需要补充近 5 场 xG、转化率、防守泄漏'} |")
    lines.append(f"| Reality Gap | `{bias_type}` | {'已补充市场偏差方向' if 'bias_type' in intel else '需要补充常见高估/低估偏差'} |")
    lines.append("")

    lines.append("## 3. 本期关联比赛")
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

    lines.append("## 4. Prematch 关注项")
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

    lines.append("## 5. 数据缺口")
    lines.append("")
    if substantive:
        lines.append("- 已具备最小实质情报，可进入下一轮 RAG 同步与 prematch 验证。")
        lines.append("- 仍建议继续补充更细的伤停、轮换、战术细节，避免样本过薄。")
    else:
        lines.append("- 缺少可直接用于 prematch 的实质情报内容。")
        lines.append("- 当前档案仍不应视为高质量 RAG 样本。")
    lines.append("")

    lines.append("## 6. 市面深度情报（外部观点）")
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
) -> Dict[str, Any]:
    archive_path = build_archive_path(vault_root, team=team, league=league)
    if not archive_path.exists():
        merged_frontmatter = merge_frontmatter_defaults({}, DEFAULT_FRONTMATTER)
        content = build_markdown(merged_frontmatter, _render_body(team=team, league=league, issue=issue, matches=team_matches))
        write_markdown_safely(archive_path, content)
    frontmatter, body = read_existing_content(archive_path)
    original_text = build_markdown(frontmatter, body)
    diagnostics = _inspect_placeholder(original_text)
    preflight_diagnostics = preflight_diagnostics or {}
    needs_enrichment = bool(preflight_diagnostics.get("needs_enrichment"))

    substantive_intel = _has_substantive_intel(intel or {})
    if not diagnostics["placeholder"] and needs_enrichment and not substantive_intel:
        return {
            "team": team,
            "league": league,
            "path": str(archive_path),
            "status": "flagged_needs_enrichment",
            "markers": sorted(set(diagnostics["markers"] + list(preflight_diagnostics.get("markers", [])))),
        }
    if not diagnostics["placeholder"] and not needs_enrichment and not substantive_intel:
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
    merged_frontmatter["archive_quality"] = "usable" if substantive_intel else "placeholder_backfilled"
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
        _render_body(team=team, league=league, issue=issue, matches=team_matches, intel=intel),
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
        )
        results.append(result)

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
