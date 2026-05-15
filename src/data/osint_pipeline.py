import argparse
import hashlib
import json
import logging
import math
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from audit_router import AuditRouter, load_dotenv_into_env
from osint_crawler import AresOsintCrawler
from osint_postmatch import MatchTelemetryPipeline
from postmatch_cleanup import cleanup_issue_postmatch
from team_forge import (
    build_archive_path,
    ensure_team_archive,
    infer_league,
    iter_issue_teams,
    load_team_alias_map,
    resolve_team_name,
    split_pair_text,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("AresTelemetry.Pipeline")


def _normalize_team_key(value: str) -> str:
    ascii_name = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
    normalized = "".join(ch for ch in ascii_name.lower() if ch.isalnum())
    if normalized:
        return normalized
    return "".join(ch for ch in str(value).strip().lower() if ch.isalnum())


def _split_match_english(english: str) -> tuple[str, str]:
    if " vs " in english:
        home, away = english.split(" vs ", 1)
        return home.strip(), away.strip()
    if " VS " in english:
        home, away = english.split(" VS ", 1)
        return home.strip(), away.strip()
    return english.strip(), ""


def _normalize_match_basic_payload(match: Dict[str, Any]) -> Dict[str, Any]:
    defaults = {
        "match": None,
        "league": None,
        "kickoff_time": None,
        "round": None,
        "home_rank_points": {"rank": None, "points": None},
        "away_rank_points": {"rank": None, "points": None},
        "remaining_matches": {"home": None, "away": None},
        "table_context": {"home_objective": None, "away_objective": None},
    }
    payload = match.get("match_basic") if isinstance(match.get("match_basic"), dict) else {}
    merged = dict(defaults)
    merged.update({k: v for k, v in payload.items() if v is not None})

    home, away = _split_match_english(str(match.get("english") or ""))
    if not merged.get("match"):
        merged["match"] = f"{home} vs {away}".strip() if home or away else None
    if not merged.get("league"):
        merged["league"] = str(match.get("league") or "").strip() or None
    kickoff_candidates = [
        match.get("understat_date"),
        match.get("football_data_date"),
        (((match.get("titan_prematch") or {}).get("pages") or {}).get("analysis") or {}).get("kickoff_time"),
    ]
    if not merged.get("kickoff_time"):
        for candidate in kickoff_candidates:
            text = str(candidate or "").strip()
            if text:
                merged["kickoff_time"] = text
                break

    home_rank_points = merged.get("home_rank_points") if isinstance(merged.get("home_rank_points"), dict) else {}
    away_rank_points = merged.get("away_rank_points") if isinstance(merged.get("away_rank_points"), dict) else {}
    remaining_matches = merged.get("remaining_matches") if isinstance(merged.get("remaining_matches"), dict) else {}
    table_context = merged.get("table_context") if isinstance(merged.get("table_context"), dict) else {}
    merged["home_rank_points"] = {
        "rank": home_rank_points.get("rank"),
        "points": home_rank_points.get("points"),
    }
    merged["away_rank_points"] = {
        "rank": away_rank_points.get("rank"),
        "points": away_rank_points.get("points"),
    }
    merged["remaining_matches"] = {
        "home": remaining_matches.get("home"),
        "away": remaining_matches.get("away"),
    }
    merged["table_context"] = {
        "home_objective": table_context.get("home_objective"),
        "away_objective": table_context.get("away_objective"),
    }
    return merged


def _required_match_basic_missing_fields(match_basic: Dict[str, Any]) -> List[str]:
    missing: List[str] = []
    required_paths = [
        ("match", match_basic.get("match")),
        ("league", match_basic.get("league")),
        ("kickoff_time", match_basic.get("kickoff_time")),
        ("home_rank_points.rank", (match_basic.get("home_rank_points") or {}).get("rank")),
        ("home_rank_points.points", (match_basic.get("home_rank_points") or {}).get("points")),
        ("away_rank_points.rank", (match_basic.get("away_rank_points") or {}).get("rank")),
        ("away_rank_points.points", (match_basic.get("away_rank_points") or {}).get("points")),
        ("remaining_matches.home", (match_basic.get("remaining_matches") or {}).get("home")),
        ("remaining_matches.away", (match_basic.get("remaining_matches") or {}).get("away")),
    ]
    for field, value in required_paths:
        if value is None or (isinstance(value, str) and not value.strip()):
            missing.append(field)
    return missing


def _yaml_safe_text(value: Any) -> str:
    text = str(value or "")
    text = text.replace('"', "'")
    return text


def _build_prematch_frontmatter_block(*, issue: str, match: Dict[str, Any], match_basic: Dict[str, Any]) -> str:
    home_rank_points = match_basic.get("home_rank_points") if isinstance(match_basic.get("home_rank_points"), dict) else {}
    away_rank_points = match_basic.get("away_rank_points") if isinstance(match_basic.get("away_rank_points"), dict) else {}
    remaining = match_basic.get("remaining_matches") if isinstance(match_basic.get("remaining_matches"), dict) else {}
    table_context = match_basic.get("table_context") if isinstance(match_basic.get("table_context"), dict) else {}
    lines = [
        "---",
        f'issue: "{_yaml_safe_text(issue)}"',
        f'match_index: {int(match.get("index", 0) or 0)}',
        f'chinese: "{_yaml_safe_text(match.get("chinese"))}"',
        f'english: "{_yaml_safe_text(match.get("english"))}"',
        f'league: "{_yaml_safe_text(match.get("league"))}"',
        f'mapping_source: "{_yaml_safe_text(match.get("mapping_source"))}"',
        f'understat_id: "{_yaml_safe_text(match.get("understat_id"))}"',
        f'football_data_match_id: "{_yaml_safe_text(match.get("football_data_match_id"))}"',
        "match_basic:",
        f'  match: "{_yaml_safe_text(match_basic.get("match"))}"',
        f'  league: "{_yaml_safe_text(match_basic.get("league"))}"',
        f'  kickoff_time: "{_yaml_safe_text(match_basic.get("kickoff_time"))}"',
        f"  round: {json.dumps(match_basic.get('round'))}",
        "  home_rank_points:",
        f"    rank: {json.dumps(home_rank_points.get('rank'))}",
        f"    points: {json.dumps(home_rank_points.get('points'))}",
        "  away_rank_points:",
        f"    rank: {json.dumps(away_rank_points.get('rank'))}",
        f"    points: {json.dumps(away_rank_points.get('points'))}",
        "  remaining_matches:",
        f"    home: {json.dumps(remaining.get('home'))}",
        f"    away: {json.dumps(remaining.get('away'))}",
        "  table_context:",
        f'    home_objective: "{_yaml_safe_text(table_context.get("home_objective"))}"',
        f'    away_objective: "{_yaml_safe_text(table_context.get("away_objective"))}"',
        "---",
        "",
    ]
    return "\n".join(lines)


def sync_prematch_audit_frontmatter(*, issue: str, manifest: Dict[str, Any], vault_root: Path) -> Dict[str, int]:
    issue_dir = vault_root / "03_Match_Audits" / str(issue) / "01_Prematch_Audits"
    if not issue_dir.exists():
        return {"updated": 0, "skipped": 0}

    by_index: Dict[int, Dict[str, Any]] = {}
    for row in manifest.get("matches", []) if isinstance(manifest.get("matches"), list) else []:
        try:
            idx = int(row.get("index"))
        except Exception:
            continue
        normalized = _normalize_match_basic_payload(row)
        row["match_basic"] = normalized
        by_index[idx] = row

    updated = 0
    skipped = 0
    for path in sorted(issue_dir.glob(f"Audit-{issue}-*.md")):
        m = re.search(rf"^Audit-{re.escape(issue)}-(\d+)-", path.name)
        if not m:
            skipped += 1
            continue
        idx = int(m.group(1))
        match = by_index.get(idx)
        if not isinstance(match, dict):
            skipped += 1
            continue
        content = path.read_text(encoding="utf-8")
        body = content
        if content.startswith("---\n"):
            closing = content.find("\n---\n", 4)
            if closing != -1:
                body = content[closing + len("\n---\n") :].lstrip("\n")
        frontmatter = _build_prematch_frontmatter_block(
            issue=issue,
            match=match,
            match_basic=match.get("match_basic") if isinstance(match.get("match_basic"), dict) else {},
        )
        rewritten = frontmatter + body
        if rewritten != content:
            path.write_text(rewritten, encoding="utf-8")
            updated += 1
    return {"updated": updated, "skipped": skipped}


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("环境变量 %s=%r 不是合法浮点数，回退默认值 %s", name, raw, default)
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("环境变量 %s=%r 不是合法整数，回退默认值 %s", name, raw, default)
        return default


def _resolve_engine_dir(explicit_engine_dir: Optional[str] = None) -> Path:
    """定位 20-engine 仓库根目录。"""
    current_repo = Path(__file__).resolve().parents[2]
    sibling = current_repo.parent / "20-ares-v4-engine"
    raw_path = explicit_engine_dir or os.getenv("ARES_ENGINE_DIR", str(sibling))
    return Path(raw_path).expanduser().resolve()


def preflight_checks(engine_dir: Path) -> list[str]:
    """对跨仓库串联所需的关键依赖做 fail-fast 检查。"""
    errors: list[str] = []

    if not engine_dir.exists():
        errors.append(f"找不到 20-engine 仓库目录: {engine_dir}")
        return errors

    engine_main = engine_dir / "main.py"
    if not engine_main.exists():
        errors.append(f"20-engine 缺少主入口: {engine_main}")

    engine_python = engine_dir / ".venv" / "bin" / "python"
    if not engine_python.exists():
        errors.append(f"20-engine 缺少虚拟环境解释器: {engine_python}")

    vault_path = os.getenv("ARES_VAULT_PATH")
    if not vault_path:
        errors.append("未配置 ARES_VAULT_PATH，无法写入统一 Vault。")
        return errors

    normalized_vault = Path(normalize_vault_path(vault_path))
    if not normalized_vault.exists():
        errors.append(f"ARES_VAULT_PATH 不存在: {normalized_vault}")
        return errors

    audit_root = normalized_vault / "03_Match_Audits"
    try:
        audit_root.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        errors.append(f"无法创建或访问审计目录 {audit_root}: {exc}")
        return errors

    probe_path = audit_root / "_ARES_PIPELINE_WRITE_PROBE.tmp"
    try:
        probe_path.write_text("ok", encoding="utf-8")
        probe_path.unlink(missing_ok=True)
    except OSError as exc:
        logger.warning("审计目录写入探针失败，继续交由实际流程验证: %s: %s", audit_root, exc)

    return errors


def inspect_rag_readiness(engine_dir: Path, manifest: Dict[str, Any]) -> Dict[str, Any]:
    chroma_db = engine_dir / "chromadb" / "chroma.sqlite3"
    diagnostics: Dict[str, Any] = {
        "ok": True,
        "blocker_type": None,
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
            blocker_type="rag_missing_database",
            summary=f"找不到 RAG 数据库: {chroma_db}",
            details=[f"Prematch 未执行；20-engine 缺少 `{chroma_db}`。"],
        )
        return diagnostics

    issue_team_map: Dict[str, str] = {}
    for match in manifest.get("matches", []):
        english = str(match.get("english", "")).strip()
        if not english:
            continue
        home, away = _split_match_english(english)
        for team in [home, away]:
            if not team:
                continue
            issue_team_map.setdefault(_normalize_team_key(team), team)

    diagnostics["issue_teams"] = sorted(issue_team_map.values())

    try:
        conn = sqlite3.connect(f"file:{chroma_db}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        diagnostics.update(
            ok=False,
            blocker_type="rag_unreadable_database",
            summary=f"无法读取 RAG 数据库: {exc}",
            details=[f"Prematch 未执行；Chroma sqlite 只读检查失败: {exc}"],
        )
        return diagnostics

    with conn:
        try:
            row = conn.execute("select count(*) from embeddings").fetchone()
            doc_count = int(row[0]) if row and row[0] is not None else 0
            team_rows = conn.execute(
                "select distinct string_value from embedding_metadata where key='team' and string_value is not null"
            ).fetchall()
        except sqlite3.Error as exc:
            diagnostics.update(
                ok=False,
                blocker_type="rag_query_failed",
                summary=f"RAG 元数据检查失败: {exc}",
                details=[f"Prematch 未执行；检查 embeddings / embedding_metadata 失败: {exc}"],
            )
            return diagnostics

    team_values = {
        _normalize_team_key(row[0]): str(row[0])
        for row in team_rows
        if row and row[0]
    }
    covered_keys = sorted(set(issue_team_map) & set(team_values))
    missing_keys = sorted(set(issue_team_map) - set(team_values))

    diagnostics["doc_count"] = doc_count
    diagnostics["covered_teams"] = [issue_team_map[key] for key in covered_keys]
    diagnostics["missing_teams"] = [issue_team_map[key] for key in missing_keys]

    min_doc_count = _env_int("ARES_PREMATCH_RAG_MIN_DOC_COUNT", 3)
    min_team_coverage_ratio = max(0.0, min(1.0, _env_float("ARES_PREMATCH_RAG_MIN_TEAM_COVERAGE_RATIO", 0.75)))
    max_missing_teams = max(0, _env_int("ARES_PREMATCH_RAG_MAX_MISSING_TEAMS", 4))
    required_team_coverage = math.ceil(len(issue_team_map) * min_team_coverage_ratio) if issue_team_map else 0

    blockers: List[str] = []
    if doc_count < min_doc_count:
        blockers.append(
            f"RAG 总文档数仅 `{doc_count}`，低于 Prematch 最低阈值 `{min_doc_count}`。"
        )
    if issue_team_map and len(covered_keys) < required_team_coverage:
        blockers.append(
            "Issue 球队覆盖不足："
            f"`{len(covered_keys)}/{len(issue_team_map)}` 支球队在 RAG metadata 中可见，"
            f"低于阈值 `{required_team_coverage}/{len(issue_team_map)}`"
            f"（coverage ratio >= `{min_team_coverage_ratio:.0%}`）。"
        )
    if issue_team_map and len(missing_keys) > max_missing_teams:
        blockers.append(
            f"Issue 球队缺口过大：缺失 `{len(missing_keys)}` 支球队，高于允许上限 `{max_missing_teams}`。"
        )

    if blockers:
        diagnostics.update(
            ok=False,
            blocker_type="rag_undercoverage",
            summary="Prematch 已被上游 RAG 覆盖不足阻断。",
            details=blockers
            + [
                f"当前 issue 球队: {', '.join(diagnostics['issue_teams']) or 'None'}",
                f"RAG 已覆盖球队: {', '.join(diagnostics['covered_teams']) or 'None'}",
                f"RAG 缺失球队: {', '.join(diagnostics['missing_teams']) or 'None'}",
                "这不是路径映射故障，而是战术逆境样本库供给不足；继续执行只会产出整批 REJECTED 报告。",
            ],
        )
        return diagnostics

    diagnostics["summary"] = (
        f"RAG readiness OK: docs={doc_count}, covered={len(covered_keys)}/{len(issue_team_map)}"
    )
    diagnostics["details"] = [
        f"RAG 总文档数: {doc_count}",
        f"Issue 球队覆盖: {len(covered_keys)}/{len(issue_team_map)}",
        f"Coverage 阈值: {required_team_coverage}/{len(issue_team_map) if issue_team_map else 0}",
        f"Missing 上限: {max_missing_teams}",
    ]
    return diagnostics


def normalize_vault_path(path_text: str) -> str:
    """标准化 .env 中的 Vault 路径写法。"""
    return str(path_text).replace("\\ ", " ").replace("\\~", "~")


def _build_child_env(force_vault: bool = False) -> Dict[str, str]:
    env = os.environ.copy()
    if force_vault:
        vault_path = str(os.getenv("ARES_VAULT_PATH") or "").strip()
        if vault_path:
            env["ARES_VAULT_PATH"] = str(Path(normalize_vault_path(vault_path)).expanduser())
    return env


def run_prematch_engine(
    *,
    issue: str,
    manifest_path: Path,
    engine_dir: Path,
    limit: Optional[int] = None,
) -> Dict[str, int]:
    """调用 20-engine 的 audit-issue 入口执行 Prematch 批量推演。"""
    python_bin = engine_dir / ".venv" / "bin" / "python"
    if not python_bin.exists():
        raise FileNotFoundError(f"找不到 engine Python 解释器: {python_bin}")

    cmd = [
        str(python_bin),
        "main.py",
        "audit-issue",
        "--issue",
        issue,
        "--manifest",
        str(manifest_path),
    ]
    if limit is not None and limit > 0:
        cmd.extend(["--limit", str(limit)])
    logger.info("==> 一键流程 Prematch 推演: %s", " ".join(cmd))
    timeout_sec = _env_int("ARES_PREMATCH_ENGINE_TIMEOUT_SEC", 1800)
    try:
        result = subprocess.run(
            cmd,
            cwd=engine_dir,
            env=_build_child_env(force_vault=True),
            text=True,
            capture_output=True,
            timeout=timeout_sec,
        )
    except subprocess.TimeoutExpired as exc:
        logger.error("Prematch 推演超时，timeout=%ss, cmd=%s", timeout_sec, " ".join(cmd))
        if exc.stdout:
            for line in str(exc.stdout).splitlines():
                logger.info("[20-engine][partial] %s", line)
        if exc.stderr:
            for line in str(exc.stderr).splitlines():
                logger.warning("[20-engine][partial-stderr] %s", line)
        return {"success": 0, "failed": limit or 1}
    if result.stdout:
        for line in result.stdout.splitlines():
            logger.info("[20-engine] %s", line)
    if result.stderr:
        for line in result.stderr.splitlines():
            logger.warning("[20-engine][stderr] %s", line)

    summary = {"success": 0, "failed": 0}
    for line in result.stdout.splitlines() if result.stdout else []:
        if not line.startswith("AUDIT_ISSUE_SUMMARY "):
            continue
        try:
            payload = json.loads(line.split(" ", 1)[1])
            summary["success"] = int(payload.get("processed", 0))
            summary["failed"] = int(payload.get("failed", 0))
        except (IndexError, json.JSONDecodeError, TypeError, ValueError) as exc:
            logger.warning("解析 audit-issue summary 失败: %s", exc)

    if result.returncode != 0:
        logger.error("Prematch 推演失败，exit_code=%s", result.returncode)
        if summary["success"] == 0 and summary["failed"] == 0:
            return {"success": 0, "failed": 1}
        return summary
    return summary


def resolve_manifest_path(base_dir: Path, run_id: str) -> Path:
    vault_path = os.getenv("ARES_VAULT_PATH")
    if vault_path:
        normalized = MatchTelemetryPipeline._normalize_vault_path(vault_path)
        primary = Path(normalized) / "04_RAG_Raw_Data" / "Cold_Data_Lake" / f"{run_id}_dispatch_manifest.json"
        if primary.exists():
            return primary
    return base_dir / "raw_reports" / f"{run_id}_dispatch_manifest.json"


def load_manifest(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _extract_markdown_section(text: str, heading: str) -> str:
    pattern = rf"^{re.escape(heading)}\n+(.*?)(?=^## |\Z)"
    match = re.search(pattern, text, flags=re.MULTILINE | re.DOTALL)
    return match.group(1).strip() if match else ""


def _extract_markdown_subsection(text: str, heading: str) -> List[str]:
    pattern = rf"^{re.escape(heading)}\n+(.*?)(?=^### |\Z)"
    match = re.search(pattern, text, flags=re.MULTILINE | re.DOTALL)
    if not match:
        return []
    lines: List[str] = []
    for raw in match.group(1).splitlines():
        line = raw.strip()
        if line.startswith("- "):
            lines.append(line[2:].strip())
    return lines


def _parse_team_archive_context(archive_path: Path) -> Dict[str, Any]:
    try:
        text = archive_path.read_text(encoding="utf-8")
    except OSError:
        return {}

    archive_quality_match = re.search(r"^archive_quality:\s*([^\n]+)$", text, flags=re.MULTILINE)
    usable_level_match = re.search(r"- 当前可用层级：`([^`]+)`", text)
    summary_section = _extract_markdown_section(text, "## 3. Prematch 摘要")
    profile_lines = _extract_markdown_subsection(summary_section, "### 3.1 本队画像")
    risk_lines = _extract_markdown_subsection(summary_section, "### 3.2 本场风险点")
    action_lines = _extract_markdown_subsection(summary_section, "### 3.3 赛前操作提示")

    summary_text_parts: List[str] = []
    for block in [profile_lines, risk_lines, action_lines]:
        if block:
            summary_text_parts.append(" ".join(block))

    return {
        "archive_path": str(archive_path),
        "archive_quality": (archive_quality_match.group(1).strip().strip("'\"") if archive_quality_match else ""),
        "usable_level": usable_level_match.group(1).strip() if usable_level_match else "",
        "prematch_summary": {
            "profile": profile_lines,
            "risk": risk_lines,
            "action": action_lines,
        },
        "prematch_summary_text": " ".join(part for part in summary_text_parts if part).strip(),
    }


def enrich_manifest_with_team_archive_context(
    *,
    manifest: Dict[str, Any],
    manifest_path: Path,
    base_dir: Path,
    vault_root: Path,
) -> Dict[str, int]:
    alias_map = load_team_alias_map(base_dir)
    updated_matches = 0
    cache: Dict[str, Dict[str, Any]] = {}

    matches = manifest.get("matches", [])
    if not isinstance(matches, list):
        return {"updated_matches": 0}

    for match in matches:
        english = str(match.get("english") or "").strip()
        home, away = _split_match_english(english)
        if not home or not away:
            continue
        resolved_home = resolve_team_name(home, alias_map)
        resolved_away = resolve_team_name(away, alias_map)
        league = str(match.get("league") or infer_league(resolved_home, resolved_away) or "").strip()
        if not league:
            continue

        side_contexts: Dict[str, Any] = {}
        for side, team_name in (("home", resolved_home), ("away", resolved_away)):
            cache_key = f"{league}:{team_name}"
            if cache_key not in cache:
                archive_path = build_archive_path(vault_root, team=team_name, league=league)
                cache[cache_key] = _parse_team_archive_context(archive_path) if archive_path.exists() else {}
            side_contexts[side] = cache.get(cache_key) or {}

        compact_context = {
            "home_summary": ((side_contexts.get("home") or {}).get("prematch_summary_text") or "").strip(),
            "away_summary": ((side_contexts.get("away") or {}).get("prematch_summary_text") or "").strip(),
        }

        match["team_archive_context"] = side_contexts
        match["prematch_context"] = compact_context
        updated_matches += 1

    if updated_matches:
        try:
            manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info("Manifest 已注入 Team Archive 摘要上下文: matches=%s -> %s", updated_matches, manifest_path)
        except OSError as exc:
            logger.warning("Manifest 注入 Team Archive 摘要失败（保留内存态继续执行）: %s", exc)
    return {"updated_matches": updated_matches}


def _is_prematch_mapped_match(match: Dict[str, Any]) -> bool:
    mapping_source = str(match.get("mapping_source") or "").strip().lower()
    has_anchor = bool(match.get("understat_id") or match.get("fbref_url") or match.get("football_data_match_id"))
    has_understat_time_anchor = bool(str(match.get("understat_date") or "").strip())
    understat_policy = (
        match.get("understat_prematch_id_policy")
        if isinstance(match.get("understat_prematch_id_policy"), dict)
        else {}
    )
    understat_upcoming_no_block = (
        str(understat_policy.get("fixture_status") or "").strip().lower() == "upcoming"
        and understat_policy.get("target_match_id_required") is False
    )
    titan_snapshot = match.get("titan_prematch") if isinstance(match.get("titan_prematch"), dict) else {}
    titan_signals = titan_snapshot.get("signals") if isinstance(titan_snapshot.get("signals"), dict) else {}
    titan_ready = (
        bool(match.get("cn_match_id"))
        and str(titan_signals.get("coverage") or "none").strip().lower() in {"full", "partial"}
    )
    if mapping_source == "unmapped":
        return False
    if mapping_source == "titan":
        return titan_ready
    if mapping_source == "understat":
        # Prematch policy: upcoming fixture does not require target Understat match id.
        return has_anchor or has_understat_time_anchor or understat_upcoming_no_block
    if mapping_source in {"fbref", "football-data"}:
        return has_anchor
    return has_anchor or titan_ready


def build_prematch_manifest(
    *,
    manifest: Dict[str, Any],
    mapped_only: bool,
) -> Dict[str, Any]:
    matches = manifest.get("matches", [])
    if not isinstance(matches, list):
        matches = []
    if not mapped_only:
        return {
            "manifest": manifest,
            "total_matches": len(matches),
            "selected_matches": len(matches),
            "skipped_unmapped": 0,
        }

    selected = [match for match in matches if _is_prematch_mapped_match(match)]
    skipped = len(matches) - len(selected)
    filtered_manifest = dict(manifest)
    filtered_manifest["matches"] = selected
    return {
        "manifest": filtered_manifest,
        "total_matches": len(matches),
        "selected_matches": len(selected),
        "skipped_unmapped": skipped,
    }


def _load_issue_team_diagnostics(vault_root: Path, issue: str) -> Dict[str, Any]:
    path = vault_root / "03_Match_Audits" / str(issue) / f"Audit-{issue}-team-diagnostics.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_prematch_input_gate_report(
    *,
    vault_root: Path,
    issue: str,
    rows: List[Dict[str, Any]],
    selected: int,
    total: int,
    min_team_docs: int,
) -> Dict[str, Any]:
    review_dir = vault_root / "03_Match_Audits" / str(issue) / "03_Review_Reports"
    review_dir.mkdir(parents=True, exist_ok=True)
    md_target = review_dir / f"REVIEW-{issue}-Prematch_Input_Gate.md"
    json_target = review_dir / f"REVIEW-{issue}-Prematch_Input_Gate.json"
    filtered = max(0, total - selected)
    has_hard_blocked_rows = any(bool(row.get("hard_blockers")) for row in rows)
    if selected <= 0:
        issue_status = "BLOCKED" if has_hard_blocked_rows else "HOLD"
    elif filtered > 0:
        issue_status = "HOLD"
    else:
        issue_status = "READY"
    payload = {
        "issue": str(issue),
        "updated_at_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ"),
        "issue_status": issue_status,
        "total_matches": int(total),
        "selected_matches": int(selected),
        "filtered_matches": int(filtered),
        "min_team_rag_docs": int(min_team_docs),
        "selected_match_indices": [
            int(row.get("index"))
            for row in rows
            if str(row.get("ready") or "").strip().lower() == "yes" and row.get("index") is not None
        ],
        "rows": rows,
    }
    lines: List[str] = []
    lines.append(f"# Review {issue} - Prematch Input Gate")
    lines.append("")
    lines.append(f"- Updated At: {payload['updated_at_utc']}")
    lines.append(f"- Issue Status: {issue_status}")
    lines.append(f"- Total Matches: {total}")
    lines.append(f"- Selected Matches: {selected}")
    lines.append(f"- Filtered Matches: {filtered}")
    lines.append(f"- Hard Blocked Rows: {sum(1 for row in rows if row.get('hard_blockers'))}")
    lines.append(f"- Min Team RAG Docs: {min_team_docs}")
    lines.append(f"- Gate JSON: `{json_target.name}`")
    lines.append("")
    lines.append("| # | Match | Quality Tag | Ready | Reasons |")
    lines.append("| --- | --- | --- | --- | --- |")
    for row in rows:
        reasons = "<br>".join(row.get("reasons") or []) or "-"
        lines.append(
            f"| {row.get('index')} | {row.get('match')} | `{row.get('quality_tag')}` | `{row.get('ready')}` | {reasons} |"
        )
    md_target.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    json_target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def _parse_gate_reason(reason: str) -> Tuple[str, str, Optional[str]]:
    raw = str(reason or "").strip()
    if not raw:
        return "", "", None
    parts = raw.split(":")
    if len(parts) >= 3:
        return parts[0], parts[1], ":".join(parts[2:])
    if len(parts) == 2:
        return parts[0], parts[1], None
    return parts[0], "", None


def _write_team_enrichment_queue_report(
    *,
    vault_root: Path,
    issue: str,
    rows: List[Dict[str, Any]],
    team_map: Dict[str, Dict[str, Any]],
    min_team_docs: int,
) -> None:
    review_dir = vault_root / "03_Match_Audits" / str(issue) / "03_Review_Reports"
    review_dir.mkdir(parents=True, exist_ok=True)

    queue_map: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        match_label = str(row.get("match") or "Unknown")
        match_index = str(row.get("index") or "")
        for raw_reason in row.get("reasons") or []:
            reason_type, reason_team, reason_value = _parse_gate_reason(str(raw_reason))
            if not reason_type or not reason_team:
                continue
            team_key = _normalize_team_key(reason_team)
            team_diag = team_map.get(team_key) if team_key else None
            display_team = str((team_diag or {}).get("team") or reason_team)
            display_league = str((team_diag or {}).get("league") or "")
            archive_status = str((team_diag or {}).get("archive_status") or "unknown")
            rag_docs = int((team_diag or {}).get("rag_doc_count") or 0)
            needs_enrichment = bool((team_diag or {}).get("needs_enrichment"))
            node = queue_map.setdefault(
                team_key or _normalize_team_key(display_team),
                {
                    "team": display_team,
                    "league": display_league,
                    "archive_status": archive_status,
                    "rag_doc_count": rag_docs,
                    "needs_enrichment": needs_enrichment,
                    "blocker_types": set(),
                    "blocker_reasons": set(),
                    "blocked_matches": set(),
                },
            )
            node["blocker_types"].add(reason_type)
            node["blocker_reasons"].add(str(raw_reason))
            node["blocked_matches"].add(f"{match_index}:{match_label}" if match_index else match_label)
            if reason_type == "non_usable_archive":
                node["archive_status"] = str(reason_value or node["archive_status"] or "unknown")
            elif reason_type == "low_rag_docs":
                try:
                    node["rag_doc_count"] = int(reason_value) if reason_value is not None else node["rag_doc_count"]
                except ValueError:
                    pass
            elif reason_type == "needs_enrichment":
                node["needs_enrichment"] = True

    queue_rows: List[Dict[str, Any]] = []
    for node in queue_map.values():
        score = 0
        archive_status = str(node.get("archive_status") or "")
        rag_docs = int(node.get("rag_doc_count") or 0)
        needs_enrichment = bool(node.get("needs_enrichment"))
        blockers = sorted(str(item) for item in node.get("blocker_reasons") or [])
        blocked_matches = sorted(str(item) for item in node.get("blocked_matches") or [])

        if archive_status != "usable":
            score += 50
        if "missing_team_diagnostics" in node.get("blocker_types", set()):
            score += 40
        if rag_docs < min_team_docs:
            score += (min_team_docs - rag_docs) * 10
        if needs_enrichment:
            score += 20
        score += len(blocked_matches)

        if score >= 80:
            priority = "P0"
        elif score >= 50:
            priority = "P1"
        else:
            priority = "P2"

        queue_rows.append(
            {
                "team": node.get("team"),
                "league": node.get("league"),
                "priority": priority,
                "priority_score": score,
                "archive_status": archive_status or "unknown",
                "rag_doc_count": rag_docs,
                "needs_enrichment": needs_enrichment,
                "blocked_match_count": len(blocked_matches),
                "blocked_matches": blocked_matches,
                "blocker_types": sorted(str(item) for item in node.get("blocker_types") or []),
                "blocker_reasons": blockers,
            }
        )

    queue_rows.sort(
        key=lambda item: (
            0 if item.get("priority") == "P0" else (1 if item.get("priority") == "P1" else 2),
            -int(item.get("priority_score") or 0),
            str(item.get("team") or ""),
        )
    )

    json_target = review_dir / f"TEAM-ENRICHMENT-QUEUE-{issue}.json"
    json_payload = {
        "issue": str(issue),
        "updated_at_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ"),
        "min_team_rag_docs": int(min_team_docs),
        "total_blocked_teams": len(queue_rows),
        "teams": queue_rows,
    }
    json_target.write_text(json.dumps(json_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    md_target = review_dir / f"REVIEW-{issue}-Team_Enrichment_Queue.md"
    lines: List[str] = []
    lines.append(f"# Review {issue} - Team Enrichment Queue")
    lines.append("")
    lines.append(f"- Updated At: {json_payload['updated_at_utc']}")
    lines.append(f"- Min Team RAG Docs: {min_team_docs}")
    lines.append(f"- Blocked Teams: {len(queue_rows)}")
    lines.append(f"- Queue JSON: `{json_target.name}`")
    lines.append("")
    lines.append("| Priority | Team | League | Archive | RAG Docs | Needs Enrichment | Blocked Matches | Blockers |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for item in queue_rows:
        blockers = "<br>".join(item.get("blocker_reasons") or []) or "-"
        lines.append(
            f"| `{item['priority']}` | `{item['team']}` | `{item['league'] or '-'}"
            f"` | `{item['archive_status']}` | `{item['rag_doc_count']}` | "
            f"`{'yes' if item['needs_enrichment'] else 'no'}` | `{item['blocked_match_count']}` | {blockers} |"
        )
    if not queue_rows:
        lines.append("| `P2` | `None` | `-` | `usable` | `-` | `no` | `0` | - |")
    md_target.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def build_prematch_ready_manifest(
    *,
    issue: str,
    manifest: Dict[str, Any],
    base_dir: Path,
    min_team_docs: int,
) -> Dict[str, Any]:
    vault_path = os.getenv("ARES_VAULT_PATH")
    if not vault_path:
        return {
            "manifest": manifest,
            "total_matches": len(manifest.get("matches", []) if isinstance(manifest.get("matches"), list) else []),
            "selected_matches": len(manifest.get("matches", []) if isinstance(manifest.get("matches"), list) else []),
            "filtered_matches": 0,
            "rows": [],
        }
    vault_root = Path(normalize_vault_path(vault_path)).expanduser()
    diagnostics = _load_issue_team_diagnostics(vault_root, issue)
    teams = diagnostics.get("teams") if isinstance(diagnostics.get("teams"), list) else []
    if not teams:
        matches = manifest.get("matches", []) if isinstance(manifest.get("matches"), list) else []
        return {
            "manifest": manifest,
            "total_matches": len(matches),
            "selected_matches": len(matches),
            "filtered_matches": 0,
            "rows": [],
        }
    alias_map = load_team_alias_map(base_dir)
    team_map: Dict[str, Dict[str, Any]] = {}
    for team_row in teams:
        team = str(team_row.get("team") or "").strip()
        if not team:
            continue
        key = _normalize_team_key(resolve_team_name(team, alias_map))
        team_map[key] = team_row

    matches = manifest.get("matches", []) if isinstance(manifest.get("matches"), list) else []
    selected_matches: List[Dict[str, Any]] = []
    rows: List[Dict[str, Any]] = []
    strong_statuses = {"usable", "usable_strong"}
    weak_statuses = {"usable_weak"}
    blocked_statuses = {"missing", "placeholder", "placeholder_backfilled"}
    non_blocking_enrichment_gaps = {
        "default_value_contamination:defensive_leakage",
        "conversion_efficiency_source_missing_do_not_use",
    }
    for match in matches:
        match_basic = _normalize_match_basic_payload(match)
        match["match_basic"] = match_basic
        english = str(match.get("english") or "").strip()
        home, away = _split_match_english(english)
        resolved_home = resolve_team_name(home, alias_map)
        resolved_away = resolve_team_name(away, alias_map)
        home_row = team_map.get(_normalize_team_key(resolved_home))
        away_row = team_map.get(_normalize_team_key(resolved_away))
        reasons: List[str] = []
        hard_blockers: List[str] = []
        soft_blockers: List[str] = []
        team_checks: List[Dict[str, Any]] = []
        match_basic_missing = _required_match_basic_missing_fields(match_basic)
        for field in match_basic_missing:
            marker = f"missing_match_basic:{field}"
            reasons.append(marker)
            soft_blockers.append(marker)
        for side_name, row in ((resolved_home, home_row), (resolved_away, away_row)):
            if not isinstance(row, dict):
                marker = f"missing_team_diagnostics:{side_name}"
                reasons.append(marker)
                hard_blockers.append(marker)
                continue
            archive_status = str(row.get("archive_status") or "").strip().lower()
            rag_docs = int(row.get("rag_doc_count") or 0)
            needs_enrichment = bool(row.get("needs_enrichment"))
            gaps = [str(item) for item in (row.get("gaps") or []) if str(item).strip()]
            missing_resilience_keys = [str(item) for item in (row.get("missing_resilience_keys") or []) if str(item).strip()]
            missing_market_behavior_keys = [
                str(item) for item in (row.get("missing_market_behavior_keys") or []) if str(item).strip()
            ]
            if archive_status in blocked_statuses or not archive_status:
                marker = f"non_usable_archive:{side_name}:{archive_status or 'unknown'}"
                reasons.append(marker)
                hard_blockers.append(marker)
            elif archive_status in weak_statuses:
                marker = f"weak_archive:{side_name}:{archive_status}"
                reasons.append(marker)
                soft_blockers.append(marker)
            elif archive_status not in strong_statuses:
                marker = f"non_usable_archive:{side_name}:{archive_status or 'unknown'}"
                reasons.append(marker)
                soft_blockers.append(marker)
            if rag_docs < min_team_docs:
                marker = f"low_rag_docs:{side_name}:{rag_docs}"
                reasons.append(marker)
                hard_blockers.append(marker)
            blocking_gaps = [item for item in gaps if item not in non_blocking_enrichment_gaps]
            if needs_enrichment and blocking_gaps:
                marker = f"needs_enrichment:{side_name}"
                reasons.append(marker)
                soft_blockers.append(marker)
            elif needs_enrichment:
                marker = f"metric_prohibited_as_directional_evidence:{side_name}"
                reasons.append(marker)
            if missing_resilience_keys:
                marker = f"missing_resilience_core:{side_name}:{','.join(missing_resilience_keys)}"
                reasons.append(marker)
                soft_blockers.append(marker)
            if missing_market_behavior_keys:
                marker = f"missing_market_behavior_core:{side_name}:{','.join(missing_market_behavior_keys)}"
                reasons.append(marker)
                soft_blockers.append(marker)
            team_checks.append(
                {
                    "team": side_name,
                    "archive_status": archive_status,
                    "rag_doc_count": rag_docs,
                    "needs_enrichment": needs_enrichment,
                    "missing_resilience_keys": missing_resilience_keys,
                    "missing_market_behavior_keys": missing_market_behavior_keys,
                }
            )

        if hard_blockers:
            readiness_level = "BLOCKED"
        elif soft_blockers:
            readiness_level = "HOLD"
        else:
            readiness_level = "READY"

        if not reasons:
            quality_tag = "ACTIONABLE"
        elif hard_blockers:
            quality_tag = "HALT_DRIVEN"
        else:
            quality_tag = "DATA_WEAK"
        ready = readiness_level == "READY"
        if ready:
            selected_matches.append(match)
        has_resilience_gap = any(bool(item.get("missing_resilience_keys")) for item in team_checks)
        has_market_behavior_gap = any(bool(item.get("missing_market_behavior_keys")) for item in team_checks)
        rows.append(
            {
                "index": match.get("index"),
                "match": english or str(match.get("chinese") or "Unknown"),
                "ready": "yes" if ready else "no",
                "prematch_readiness_level": readiness_level,
                "quality_tag": quality_tag,
                "reasons": reasons,
                "hard_blockers": hard_blockers,
                "soft_blockers": soft_blockers,
                "team_checks": team_checks,
                "match_basic_missing_fields": match_basic_missing,
                "has_match_basic_gap": bool(match_basic_missing),
                "has_resilience_gap": has_resilience_gap,
                "has_market_behavior_gap": has_market_behavior_gap,
                "has_structural_data_gap": bool(has_resilience_gap or has_market_behavior_gap or match_basic_missing),
            }
        )
        match["prematch_input_quality"] = {
            "quality_tag": quality_tag,
            "reasons": reasons,
            "ready": ready,
            "prematch_readiness_level": readiness_level,
            "hard_blockers": hard_blockers,
            "soft_blockers": soft_blockers,
            "match_basic_missing_fields": match_basic_missing,
            "has_match_basic_gap": bool(match_basic_missing),
            "has_resilience_gap": has_resilience_gap,
            "has_market_behavior_gap": has_market_behavior_gap,
            "has_structural_data_gap": bool(has_resilience_gap or has_market_behavior_gap or match_basic_missing),
        }

    filtered_manifest = dict(manifest)
    filtered_manifest["matches"] = selected_matches
    gate_payload = _write_prematch_input_gate_report(
        vault_root=vault_root,
        issue=issue,
        rows=rows,
        selected=len(selected_matches),
        total=len(matches),
        min_team_docs=min_team_docs,
    )
    _write_team_enrichment_queue_report(
        vault_root=vault_root,
        issue=issue,
        rows=rows,
        team_map=team_map,
        min_team_docs=min_team_docs,
    )
    return {
        "manifest": filtered_manifest,
        "total_matches": len(matches),
        "selected_matches": len(selected_matches),
        "filtered_matches": max(0, len(matches) - len(selected_matches)),
        "issue_status": gate_payload.get("issue_status", "READY"),
        "gate_payload": gate_payload,
        "rows": rows,
    }


def write_temp_manifest(*, issue: str, manifest: Dict[str, Any]) -> Path:
    with tempfile.NamedTemporaryFile(
        mode="w",
        prefix=f"ares_{issue}_prematch_",
        suffix=".json",
        delete=False,
        encoding="utf-8",
    ) as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
        return Path(f.name)


def normalize_manifest_team_names(
    *,
    manifest: Dict[str, Any],
    manifest_path: Path,
    base_dir: Path,
) -> Dict[str, int]:
    """用本地别名字典修补 manifest 中仍是中文缩写的 english 字段。"""
    alias_map = load_team_alias_map(base_dir)
    updated = 0
    for match in manifest.get("matches", []):
        chinese_home, chinese_away = split_pair_text(match.get("chinese", ""))
        english_home, english_away = split_pair_text(match.get("english", ""))
        resolved_home = resolve_team_name(english_home or chinese_home, alias_map)
        resolved_away = resolve_team_name(english_away or chinese_away, alias_map)

        if chinese_home:
            resolved_home = resolve_team_name(chinese_home, alias_map) if resolved_home == english_home else resolved_home
        if chinese_away:
            resolved_away = resolve_team_name(chinese_away, alias_map) if resolved_away == english_away else resolved_away

        if resolved_home and resolved_away:
            new_english = f"{resolved_home} vs {resolved_away}"
            if new_english != match.get("english"):
                match["english"] = new_english
                updated += 1

        if not match.get("league"):
            league = infer_league(resolved_home, resolved_away)
            if league:
                match["league"] = league
                updated += 1

    if updated:
        try:
            manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info("Manifest 队名/联赛归一化完成: updated_fields=%s -> %s", updated, manifest_path)
        except OSError as exc:
            logger.warning("Manifest 回写失败（保留内存态继续执行）: %s", exc)
    return {"updated_fields": updated}


def run_issue_team_forge(*, issue: str, base_dir: Path) -> Dict[str, int]:
    vault_path = os.getenv("ARES_VAULT_PATH")
    if not vault_path:
        logger.warning("未配置 ARES_VAULT_PATH，跳过 Team Forge 批量补档。")
        return {"created_or_updated": 0, "failed": 1}

    vault_root = Path(normalize_vault_path(vault_path)).expanduser()
    created_or_updated = 0
    failed = 0
    for team, league in iter_issue_teams(base_dir, vault_root, issue):
        try:
            ensure_team_archive(vault_root, team=team, league=league)
            created_or_updated += 1
        except Exception as exc:
            logger.error("Team Forge 补档失败 issue=%s team=%s league=%s: %s", issue, team, league, exc)
            failed += 1
    logger.info(
        "Team Forge issue 批量补档完成 issue=%s, created_or_updated=%s, failed=%s",
        issue,
        created_or_updated,
        failed,
    )
    return {"created_or_updated": created_or_updated, "failed": failed}


def run_issue_team_archive_backfill(
    *,
    issue: str,
    base_dir: Path,
    intel_file: Optional[str] = None,
    force: bool = False,
) -> Dict[str, int]:
    """在 prematch 前统一回填 issue 球队档案（可选注入 TEAM-INTEL 文件）。"""
    script_path = base_dir / "src" / "data" / "team_archive_backfill.py"
    if not script_path.exists():
        logger.warning("找不到 team_archive_backfill.py，跳过队档回填。")
        return {"backfilled": 0, "enriched": 0, "skipped": 1, "failed": 1}

    cmd = [sys.executable, str(script_path), "--issue", str(issue)]
    if intel_file:
        cmd.extend(["--intel-file", str(Path(intel_file).expanduser())])
    if force:
        cmd.append("--force")

    result = subprocess.run(
        cmd,
        cwd=base_dir,
        env=_build_child_env(force_vault=True),
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        logger.warning(
            "Team Archive 回填失败 issue=%s exit=%s stdout=%s stderr=%s",
            issue,
            result.returncode,
            (result.stdout or "").strip(),
            (result.stderr or "").strip(),
        )
        return {"backfilled": 0, "enriched": 0, "skipped": 0, "failed": 1}

    summary = {"backfilled": 0, "enriched": 0, "skipped": 0, "failed": 0}
    log_text = f"{result.stdout}\n{result.stderr}"
    m = re.search(
        r"backfilled=(\d+), enriched=(\d+), skipped=(\d+)",
        log_text,
    )
    if m:
        summary["backfilled"] = int(m.group(1))
        summary["enriched"] = int(m.group(2))
        summary["skipped"] = int(m.group(3))
    logger.info(
        "Team Archive 回填完成 issue=%s, backfilled=%s, enriched=%s, skipped=%s",
        issue,
        summary["backfilled"],
        summary["enriched"],
        summary["skipped"],
    )
    return summary


def sync_issue_team_archives_to_rag(
    *,
    issue: str,
    base_dir: Path,
    engine_dir: Path,
) -> Dict[str, int]:
    """导入本期球队档案到 20-engine RAG，确保 readiness gate 看到最新补档。"""
    vault_path = os.getenv("ARES_VAULT_PATH")
    if not vault_path:
        logger.warning("未配置 ARES_VAULT_PATH，跳过 issue 球队 RAG 同步。")
        return {"synced": 0, "skipped": 0, "failed": 1}

    python_bin = engine_dir / ".venv" / "bin" / "python"
    if not python_bin.exists():
        logger.warning("找不到 engine Python 解释器，跳过 issue 球队 RAG 同步: %s", python_bin)
        return {"synced": 0, "skipped": 0, "failed": 1}

    def _split_archive_chunks(content: str) -> List[Tuple[str, str]]:
        max_sections = max(1, _env_int("ARES_TEAM_ARCHIVE_RAG_MAX_SECTIONS", 4))
        min_chars = max(80, _env_int("ARES_TEAM_ARCHIVE_RAG_MIN_SECTION_CHARS", 180))
        body = content
        if content.startswith("---\n"):
            closing = content.find("\n---\n", 4)
            if closing != -1:
                body = content[closing + len("\n---\n") :].lstrip("\n")
        sections: List[Tuple[str, str]] = [("full", content)]
        current_title = "intro"
        current_lines: List[str] = []

        def _flush() -> None:
            nonlocal current_title, current_lines
            section_text = "\n".join(current_lines).strip()
            if len(section_text) >= min_chars:
                section_key = "".join(ch.lower() if ch.isalnum() else "-" for ch in current_title).strip("-")
                section_key = section_key or f"section-{len(sections)}"
                sections.append((section_key[:36], section_text))
            current_lines = []

        for line in body.splitlines():
            if line.startswith("## "):
                _flush()
                current_title = line[3:].strip() or "section"
                current_lines = [line]
                continue
            current_lines.append(line)
        _flush()

        dedup: List[Tuple[str, str]] = []
        seen = set()
        for suffix, text in sections:
            key = (suffix, text.strip())
            if key in seen:
                continue
            seen.add(key)
            dedup.append((suffix, text))
            if len(dedup) >= 1 + max_sections:
                break
        return dedup

    def _add_doc(
        *,
        team: str,
        source_file: Path,
        doc_id: str,
    ) -> subprocess.CompletedProcess:
        cmd = [
            str(python_bin),
            "main.py",
            "add-doc",
            "--file",
            str(source_file),
            "--team",
            team,
            "--source-level",
            "B",
            "--doc-id",
            doc_id,
        ]
        return subprocess.run(
            cmd,
            cwd=engine_dir,
            env=_build_child_env(force_vault=True),
            text=True,
            capture_output=True,
        )

    vault_root = Path(normalize_vault_path(vault_path)).expanduser()
    synced = 0
    skipped = 0
    failed = 0
    seen: set[tuple[str, str]] = set()
    for team, league in iter_issue_teams(base_dir, vault_root, issue):
        key = (team, league)
        if key in seen:
            skipped += 1
            continue
        seen.add(key)
        try:
            archive_path = build_archive_path(vault_root, team=team, league=league)
            if not archive_path.exists():
                archive_path = ensure_team_archive(vault_root, team=team, league=league)
        except Exception as exc:
            logger.warning("RAG 同步前定位球队档案失败 team=%s league=%s: %s", team, league, exc)
            failed += 1
            continue
        if not archive_path.exists():
            skipped += 1
            continue

        content = archive_path.read_text(encoding="utf-8")
        chunks = _split_archive_chunks(content)
        doc_key = hashlib.md5(f"issue-team-archive:{team}:{archive_path}".encode("utf-8")).hexdigest()[:16]
        team_failed = False
        with tempfile.TemporaryDirectory(prefix=f"ares_team_rag_{_normalize_team_key(team)}_") as temp_dir:
            temp_root = Path(temp_dir)
            for idx, (suffix, chunk_text) in enumerate(chunks, start=1):
                if idx == 1 and suffix == "full":
                    source_file = archive_path
                else:
                    source_file = temp_root / f"{_normalize_team_key(team)}_{idx:02d}_{suffix}.md"
                    source_file.write_text(chunk_text.strip() + "\n", encoding="utf-8")
                result = _add_doc(
                    team=team,
                    source_file=source_file,
                    doc_id=f"team-{doc_key}-{idx:02d}-{suffix}",
                )
                if result.returncode != 0:
                    team_failed = True
                    logger.warning(
                        "issue 球队档案导入 RAG 失败 team=%s doc=%s exit=%s stdout=%s stderr=%s",
                        team,
                        source_file,
                        result.returncode,
                        (result.stdout or "").strip(),
                        (result.stderr or "").strip(),
                    )
                    break
        if team_failed:
            failed += 1
            continue
        synced += 1

    logger.info("Issue 球队档案 RAG 同步完成 issue=%s, synced=%s, skipped=%s, failed=%s", issue, synced, skipped, failed)
    return {"synced": synced, "skipped": skipped, "failed": failed}


def run_batch_postmatch(
    *,
    issue: str,
    manifest: Dict[str, Any],
    source: str,
    league: Optional[str],
) -> Dict[str, int]:
    success = 0
    skipped = 0
    failed = 0
    for match in manifest.get("matches", []):
        fbref_url = match.get("fbref_url")
        uid = match.get("understat_id") or fbref_url
        official_score = match.get("official_score") or match.get("result_score")
        if not uid:
            skipped += 1
            continue

        logger.info("==> 一键流程赛后复盘: %s (Ref: %s)", match.get("chinese"), uid)
        pipeline = MatchTelemetryPipeline(
            issue=issue,
            match_id=str(uid),
            source=source,
            fbref_url=fbref_url,
            official_score=official_score,
            league=(
                match.get("league")
                or match.get("competition")
                or manifest.get("league")
                or league
            ),
        )
        try:
            pipeline.run()
            success += 1
        except Exception as e:
            logger.error("一键流程赛后复盘失败 %s: %s", match.get("chinese"), e)
            failed += 1

    return {"success": success, "skipped": skipped, "failed": failed}


def inspect_postmatch_readiness(manifest: Dict[str, Any]) -> Dict[str, Any]:
    matches = manifest.get("matches", [])
    if not isinstance(matches, list):
        matches = []

    total_matches = len(matches)
    scored_matches = 0
    missing_score_matches: List[str] = []
    for match in matches:
        official_score = match.get("official_score") or match.get("result_score")
        if official_score:
            scored_matches += 1
        else:
            missing_score_matches.append(str(match.get("english") or match.get("chinese") or "Unknown Match"))

    min_ready_matches = max(1, _env_int("ARES_POSTMATCH_MIN_READY_MATCHES", total_matches if total_matches else 1))
    min_ready_ratio = max(0.0, min(1.0, _env_float("ARES_POSTMATCH_MIN_READY_RATIO", 1.0)))
    required_by_ratio = math.ceil(total_matches * min_ready_ratio) if total_matches else 0
    required_ready_matches = max(min_ready_matches, required_by_ratio)

    ok = scored_matches >= required_ready_matches
    summary = (
        f"Postmatch readiness OK: scores={scored_matches}/{total_matches}"
        if ok
        else "Postmatch 已被官方比分门禁阻断。"
    )
    details = [
        f"Manifest 比赛数: {total_matches}",
        f"已具备 official_score/result_score 的比赛数: {scored_matches}",
        f"Postmatch 最低要求: {required_ready_matches}/{total_matches if total_matches else 0}",
    ]
    if missing_score_matches:
        details.append("缺少官方比分的比赛: " + ", ".join(missing_score_matches))

    return {
        "ok": ok,
        "summary": summary,
        "details": details,
        "total_matches": total_matches,
        "scored_matches": scored_matches,
        "missing_score_matches": missing_score_matches,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ares One-Command OSINT Pipeline")
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--issue", type=str, help="中国体彩期号，如 26064")
    mode_group.add_argument("--date", type=str, help="按日期分析，格式 YYYYMMDD，如 20260502")
    parser.add_argument("--scope", type=str, default="top5", help="date 模式范围，当前支持 top5")
    parser.add_argument(
        "--date-source",
        type=str,
        default="understat",
        choices=["understat", "football-data"],
        help="date 模式赛程口径来源，默认 understat（严格按 understat 比赛日）。",
    )
    parser.add_argument(
        "--date-enable-prematch",
        action="store_true",
        help="允许 date 模式执行 prematch（默认仅赛程清单流程）。",
    )
    parser.add_argument(
        "--source",
        type=str,
        default="auto",
        choices=["auto", "understat", "fbref"],
        help="赛后数据源策略: auto(先 Understat 后 FBref) | understat | fbref",
    )
    parser.add_argument("--league", type=str, required=False, help="联赛名，赛后定位 Team_Archives 时可用")
    parser.add_argument("--engine-dir", type=str, required=False, help="显式指定 20-engine 仓库路径")
    parser.add_argument("--skip-crawler", action="store_true", help="跳过 crawler，仅消费已有 dispatch_manifest")
    parser.add_argument("--skip-prematch", action="store_true", help="跳过 Prematch 推演，仅跑 crawler/路由/postmatch")
    parser.add_argument("--skip-postmatch", action="store_true", help="只跑 crawler 与目录路由，不跑赛后复盘")
    parser.add_argument("--skip-team-forge", action="store_true", help="跳过 Team Archives 批量补档")
    parser.add_argument("--skip-team-backfill", action="store_true", help="跳过 prematch 前 Team Archive 回填")
    parser.add_argument("--team-backfill-force", action="store_true", help="强制回填本期球队档案（覆盖 skipped_usable）")
    parser.add_argument("--team-intel-file", required=False, help="prematch 前回填使用的 TEAM-INTEL JSON 文件")
    parser.add_argument(
        "--sync-team-rag-only",
        action="store_true",
        help="仅执行 issue 球队档案 RAG 同步（含 Team Forge），不进入 prematch/postmatch",
    )
    parser.add_argument(
        "--prematch-mapped-only",
        action="store_true",
        help="Prematch 仅执行已映射场次（跳过 mapping_source=unmapped 的比赛）",
    )
    parser.add_argument(
        "--no-prematch-ready-gate",
        action="store_true",
        help="关闭按场次输入质量门槛（默认开启，筛除低质量输入场次）",
    )
    parser.add_argument("--prematch-limit", type=int, required=False, help="仅执行前 N 场 Prematch，用于串联调试")
    parser.add_argument("--no-prematch-stubs", action="store_true", help="不生成 Prematch 骨架文档")
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent.parent.parent
    load_dotenv_into_env(base_dir)
    run_id = args.issue if args.issue else f"DATE-{args.date}-{str(args.scope or 'top5').strip().lower()}"
    is_date_mode = bool(args.date)
    if is_date_mode:
        if args.sync_team_rag_only:
            logger.warning("date 模式不支持 --sync-team-rag-only，已忽略。")
            args.sync_team_rag_only = False
        if (not args.date_enable_prematch) and (not args.skip_prematch):
            logger.info("date 模式 v1 暂不执行 prematch，已自动启用 --skip-prematch。")
            args.skip_prematch = True
        if not args.skip_postmatch:
            logger.info("date 模式 v1 暂不执行 postmatch，已自动启用 --skip-postmatch。")
            args.skip_postmatch = True
        if not args.skip_team_forge:
            logger.info("date 模式 v1 暂不执行 Team Forge，已自动启用 --skip-team-forge。")
            args.skip_team_forge = True
        if not args.skip_team_backfill:
            logger.info("date 模式将执行 Team Archive backfill（默认强制回填）。")
        if not args.team_backfill_force:
            logger.info("date 模式默认启用 --team-backfill-force。")
            args.team_backfill_force = True
    engine_dir: Optional[Path] = None
    if (not args.skip_prematch) or args.sync_team_rag_only:
        engine_dir = _resolve_engine_dir(args.engine_dir)
        preflight_errors = preflight_checks(engine_dir)
        if preflight_errors:
            for err in preflight_errors:
                logger.error("Preflight 失败: %s", err)
            raise SystemExit(1)

    router = AuditRouter(base_dir=base_dir)
    manifest_path: Optional[Path] = None

    if not args.skip_crawler:
        crawler = AresOsintCrawler(
            issue=args.issue,
            analysis_date=args.date,
            scope=args.scope,
            date_source=args.date_source,
        )
        manifest_path = crawler.scan_and_map()
    else:
        manifest_path = resolve_manifest_path(base_dir, run_id)

    if not manifest_path or not manifest_path.exists():
        logger.error("找不到 dispatch_manifest: %s", manifest_path)
        raise SystemExit(1)

    manifest = load_manifest(manifest_path)
    manifest_matches = manifest.get("matches", []) if isinstance(manifest.get("matches"), list) else []
    if is_date_mode and not manifest_matches:
        logger.info("Date 模式当日无可分析比赛，流程结束。run_id=%s", run_id)
        raise SystemExit(0)
    normalize_manifest_team_names(
        manifest=manifest,
        manifest_path=manifest_path,
        base_dir=base_dir,
    )

    if not args.skip_team_forge:
        try:
            run_issue_team_forge(issue=run_id, base_dir=base_dir)
        except Exception as e:
            logger.warning("Team Forge 批量补档失败（不影响主流程）: %s", e)
    should_run_team_backfill = (not args.skip_team_backfill) and (
        is_date_mode or (not args.skip_prematch) or args.sync_team_rag_only
    )
    if should_run_team_backfill:
        try:
            run_issue_team_archive_backfill(
                issue=run_id,
                base_dir=base_dir,
                intel_file=args.team_intel_file,
                force=args.team_backfill_force,
            )
        except Exception as e:
            logger.warning("Team Archive 回填失败（不影响主流程）: %s", e)
    vault_path = os.getenv("ARES_VAULT_PATH")
    if vault_path:
        try:
            enrich_manifest_with_team_archive_context(
                manifest=manifest,
                manifest_path=manifest_path,
                base_dir=base_dir,
                vault_root=Path(normalize_vault_path(vault_path)).expanduser(),
            )
        except Exception as e:
            logger.warning("Manifest 注入 Team Archive 摘要失败（不影响主流程）: %s", e)
    if ((not args.skip_prematch) or args.sync_team_rag_only) and engine_dir is not None:
        try:
            sync_issue_team_archives_to_rag(
                issue=run_id,
                base_dir=base_dir,
                engine_dir=engine_dir,
            )
        except Exception as e:
            logger.warning("Issue 球队档案 RAG 同步失败（不影响主流程）: %s", e)
    if args.sync_team_rag_only:
        logger.info("sync-team-rag-only 已完成，流程提前结束。")
        raise SystemExit(0)
    prematch_manifest_path = manifest_path
    prematch_temp_manifest_path: Optional[Path] = None
    prematch_selection = build_prematch_manifest(
        manifest=manifest,
        mapped_only=args.prematch_mapped_only,
    )
    prematch_manifest = prematch_selection["manifest"]
    if args.prematch_mapped_only:
        logger.info(
            "Prematch mapped-only 已启用: selected=%s/%s, skipped_unmapped=%s",
            prematch_selection["selected_matches"],
            prematch_selection["total_matches"],
            prematch_selection["skipped_unmapped"],
        )
        if prematch_selection["selected_matches"] > 0:
            if prematch_temp_manifest_path is not None:
                try:
                    prematch_temp_manifest_path.unlink(missing_ok=True)
                except Exception:
                    pass
            prematch_temp_manifest_path = write_temp_manifest(
                issue=run_id,
                manifest=prematch_manifest,
            )
            prematch_manifest_path = prematch_temp_manifest_path
            logger.info("Prematch 将使用过滤 manifest: %s", prematch_manifest_path)
        else:
            logger.warning("Prematch mapped-only 下无可执行场次（全部为 unmapped 或缺锚点）。")

    if not args.no_prematch_ready_gate:
        min_team_docs = max(1, _env_int("ARES_PREMATCH_READY_MIN_TEAM_DOCS", 3))
        gate_selection = build_prematch_ready_manifest(
            issue=run_id,
            manifest=prematch_manifest,
            base_dir=base_dir,
            min_team_docs=min_team_docs,
        )
        prematch_manifest = gate_selection["manifest"]
        logger.info(
            "Prematch ready-gate 已启用: status=%s, selected=%s/%s, filtered=%s, min_team_docs=%s",
            gate_selection.get("issue_status"),
            gate_selection["selected_matches"],
            gate_selection["total_matches"],
            gate_selection["filtered_matches"],
            min_team_docs,
        )
        if gate_selection["selected_matches"] > 0:
            if prematch_temp_manifest_path is not None:
                try:
                    prematch_temp_manifest_path.unlink(missing_ok=True)
                except Exception:
                    pass
            prematch_temp_manifest_path = write_temp_manifest(
                issue=run_id,
                manifest=prematch_manifest,
            )
            prematch_manifest_path = prematch_temp_manifest_path
            logger.info("Prematch ready-gate 将使用过滤 manifest: %s", prematch_manifest_path)
        else:
            if is_date_mode and gate_selection["total_matches"] == 0:
                logger.info("Prematch ready-gate: date 模式无比赛日，selected=0。")
            else:
                logger.warning("Prematch ready-gate 下无可执行场次（输入质量不达标）。")

    if vault_path:
        try:
            sync_result = sync_prematch_audit_frontmatter(
                issue=run_id,
                manifest=manifest,
                vault_root=Path(normalize_vault_path(vault_path)).expanduser(),
            )
            logger.info(
                "Prematch audit frontmatter 同步完成: updated=%s skipped=%s",
                sync_result.get("updated", 0),
                sync_result.get("skipped", 0),
            )
        except Exception as exc:
            logger.warning("Prematch audit frontmatter 同步失败（不影响主流程）: %s", exc)

    prematch_summary = {"success": 0, "failed": 0}
    if not args.skip_prematch:
        rag_readiness = inspect_rag_readiness(engine_dir, prematch_manifest)
        if router.enabled and rag_readiness["ok"]:
            try:
                router.clear_prematch_blocker_report(run_id)
            except Exception as e:
                logger.warning("Prematch blocker 清理失败（不影响主流程）: %s", e)
        if router.enabled:
            try:
                router.ensure_issue_governance(
                    issue=run_id,
                    manifest=manifest,
                    create_prematch_stubs=rag_readiness["ok"] and not args.no_prematch_stubs,
                )
            except Exception as e:
                logger.warning("AuditRouter 预处理失败（不影响主流程）: %s", e)

        if not rag_readiness["ok"]:
            logger.error("Prematch 熔断: %s", rag_readiness["summary"])
            for detail in rag_readiness.get("details", []):
                logger.error("Prematch 熔断详情: %s", detail)
            if router.enabled:
                try:
                    router.write_prematch_blocker_report(
                        issue=run_id,
                        blocker_type=str(rag_readiness.get("blocker_type") or "unknown"),
                        summary=str(rag_readiness.get("summary") or "Prematch blocked"),
                        details=[str(item) for item in rag_readiness.get("details", [])],
                    )
                    router.ensure_issue_governance(
                        issue=run_id,
                        manifest=manifest,
                        create_prematch_stubs=False,
                    )
                except Exception as e:
                    logger.warning("Prematch blocker report 写入失败（不影响主流程）: %s", e)
            prematch_summary = {"success": 0, "failed": len(prematch_manifest.get("matches", []))}
        else:
            try:
                if not prematch_manifest.get("matches"):
                    prematch_summary = {"success": 0, "failed": 0}
                    if args.prematch_mapped_only and not args.no_prematch_ready_gate:
                        logger.warning("Prematch 已跳过：mapped-only + ready-gate 后没有可执行场次。")
                    elif args.prematch_mapped_only:
                        logger.warning("Prematch 已跳过：mapped-only 模式下没有可执行场次。")
                    elif not args.no_prematch_ready_gate:
                        logger.warning("Prematch 已跳过：ready-gate 过滤后没有可执行场次。")
                    else:
                        logger.warning("Prematch 已跳过：当前 manifest 没有可执行场次。")
                    if router.enabled:
                        try:
                            router.write_prematch_blocker_report(
                                issue=run_id,
                                blocker_type="prematch_input_gate_blocked",
                                summary="Prematch 被输入质量门禁阻断。",
                                details=[
                                    "selected_matches=0，当前 issue 不应继续进入 prematch 推演。",
                                    "请先查看 REVIEW-<issue>-Prematch_Input_Gate.json 与 Team Enrichment Queue。",
                                ],
                            )
                        except Exception as e:
                            logger.warning("Prematch input gate blocker 写入失败（不影响主流程）: %s", e)
                else:
                    prematch_summary = run_prematch_engine(
                        issue=run_id,
                        manifest_path=prematch_manifest_path,
                        engine_dir=engine_dir,
                        limit=args.prematch_limit,
                    )
                if router.enabled:
                    try:
                        router.ensure_issue_governance(
                            issue=run_id,
                            manifest=manifest,
                            create_prematch_stubs=False,
                        )
                    except Exception as e:
                        logger.warning("Prematch 质量闸门收口失败（不影响主流程）: %s", e)
            except Exception as e:
                logger.error("Prematch orchestration 失败: %s", e)
                prematch_summary = {"success": 0, "failed": 1}
    elif router.enabled:
        try:
            router.ensure_issue_governance(
                issue=run_id,
                manifest=manifest,
                create_prematch_stubs=not args.no_prematch_stubs,
            )
        except Exception as e:
            logger.warning("AuditRouter 预处理失败（不影响主流程）: %s", e)

    if args.skip_postmatch:
        if router.enabled:
            try:
                router.ensure_issue_governance(
                    issue=run_id,
                    manifest=manifest,
                    create_prematch_stubs=False,
                )
            except Exception as e:
                logger.warning("AuditRouter 收尾失败（不影响主流程）: %s", e)
        logger.info(
            "一键流程结束（已跳过赛后复盘） issue=%s, prematch_success=%s, prematch_failed=%s",
            run_id,
            prematch_summary["success"],
            prematch_summary["failed"],
        )
        if prematch_temp_manifest_path is not None:
            try:
                prematch_temp_manifest_path.unlink(missing_ok=True)
            except Exception as e:
                logger.warning("清理临时 prematch manifest 失败（不影响结果）: %s", e)
        raise SystemExit(0)

    postmatch_readiness = inspect_postmatch_readiness(manifest)
    if not postmatch_readiness["ok"]:
        logger.warning("Postmatch 熔断: %s", postmatch_readiness["summary"])
        for detail in postmatch_readiness.get("details", []):
            logger.warning("Postmatch 熔断详情: %s", detail)
        if router.enabled:
            try:
                cleanup_summary = cleanup_issue_postmatch(run_id, vault_path=str(router.vault_root))
                logger.info(
                    "Postmatch 存量清理完成: before=%s, after=%s, new_stale=%s, new_pending=%s, report=%s",
                    len(cleanup_summary["before_main"]),
                    len(cleanup_summary["after_main"]),
                    len(cleanup_summary["new_stale"]),
                    len(cleanup_summary["new_pending"]),
                    cleanup_summary["report_path"],
                )
            except Exception as e:
                logger.warning("Postmatch 存量清理失败（不影响主流程熔断）: %s", e)
        summary = {
            "success": 0,
            "skipped": len(manifest.get("matches", [])),
            "failed": 0,
        }
    else:
        summary = run_batch_postmatch(
            issue=run_id,
            manifest=manifest,
            source=args.source,
            league=args.league,
        )

    if router.enabled:
        try:
            router.ensure_issue_governance(
                issue=run_id,
                manifest=manifest,
                create_prematch_stubs=False,
            )
        except Exception as e:
            logger.warning("AuditRouter 收尾失败（不影响主流程）: %s", e)

    logger.info(
        "一键流程完成 issue=%s, prematch_success=%s, prematch_failed=%s, postmatch_success=%s, postmatch_skipped=%s, postmatch_failed=%s",
        run_id,
        prematch_summary["success"],
        prematch_summary["failed"],
        summary["success"],
        summary["skipped"],
        summary["failed"],
    )
    if prematch_temp_manifest_path is not None:
        try:
            prematch_temp_manifest_path.unlink(missing_ok=True)
        except Exception as e:
            logger.warning("清理临时 prematch manifest 失败（不影响结果）: %s", e)
