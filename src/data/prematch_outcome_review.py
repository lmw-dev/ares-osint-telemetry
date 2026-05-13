import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

from audit_router import load_dotenv_into_env, normalize_vault_path


TOP5_LEAGUES: Set[str] = {"EPL", "La_liga", "Bundesliga", "Serie_A", "Ligue_1"}


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_score(score_text: str) -> Optional[Tuple[int, int]]:
    txt = _safe_text(score_text).replace(":", "-")
    m = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s*$", txt)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def _outcome_code(home_goals: int, away_goals: int) -> str:
    if home_goals > away_goals:
        return "3"
    if home_goals == away_goals:
        return "1"
    return "0"


def _suggestion_set(suggestion: str) -> Set[str]:
    txt = _safe_text(suggestion).lower()
    if txt == "skip":
        return set()
    parts = [p.strip() for p in txt.split("/") if p.strip()]
    return {p for p in parts if p in {"3", "1", "0"}}


def _resolve_result_code(match: Dict[str, Any]) -> Optional[str]:
    for key in ("official_score", "result_score", "score"):
        parsed = _parse_score(_safe_text(match.get(key)))
        if parsed:
            return _outcome_code(parsed[0], parsed[1])
    result_flag = _safe_text(match.get("result")).lower()
    if result_flag in {"3", "1", "0"}:
        return result_flag
    return None


def _collect_postmatch_result_by_understat_id(issue_dir: Path, issue: str) -> Dict[str, str]:
    postmatch_dir = issue_dir / "04_Postmatch_Telemetry"
    if not postmatch_dir.exists():
        return {}
    out: Dict[str, str] = {}
    for path in sorted(postmatch_dir.glob(f"{issue}_*_postmatch.md")):
        m = re.search(rf"{re.escape(issue)}_(\d+)_postmatch\.md$", path.name)
        if not m:
            continue
        understat_id = m.group(1)
        text = path.read_text(encoding="utf-8")
        if not text.startswith("---"):
            continue
        parts = text.split("---", 2)
        if len(parts) < 3:
            continue
        frontmatter_raw = parts[1]
        frontmatter = yaml.safe_load(frontmatter_raw) or {}
        score_text = _safe_text((frontmatter.get("result") or {}).get("score"))
        if not _parse_score(score_text):
            m_score = re.search(r"(?mi)^\s*score:\s*([0-9]+\s*-\s*[0-9]+)\s*$", frontmatter_raw)
            if m_score:
                score_text = _safe_text(m_score.group(1))
        parsed = _parse_score(score_text)
        if parsed:
            out[understat_id] = _outcome_code(parsed[0], parsed[1])
    return out


def _collect_postmatch_process_by_understat_id(issue_dir: Path, issue: str) -> Dict[str, Dict[str, Any]]:
    postmatch_dir = issue_dir / "04_Postmatch_Telemetry"
    if not postmatch_dir.exists():
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for path in sorted(postmatch_dir.glob(f"{issue}_*_postmatch.md")):
        m = re.search(rf"{re.escape(issue)}_(\d+)_postmatch\.md$", path.name)
        if not m:
            continue
        understat_id = m.group(1)
        text = path.read_text(encoding="utf-8")
        if not text.startswith("---"):
            continue
        parts = text.split("---", 2)
        if len(parts) < 3:
            continue
        frontmatter = yaml.safe_load(parts[1]) or {}
        metrics = frontmatter.get("physical_metrics") or {}
        home_xg = metrics.get("home_xG")
        away_xg = metrics.get("away_xG")
        variance = (frontmatter.get("system_evaluation") or {}).get("variance_flag")
        try:
            hxg = float(home_xg)
            axg = float(away_xg)
        except Exception:
            continue
        out[understat_id] = {
            "home_xg": hxg,
            "away_xg": axg,
            "xg_gap_abs": abs(hxg - axg),
            "xg_better_side": "3" if hxg > axg else "0" if axg > hxg else "1",
            "variance_flag": bool(variance),
        }
    return out


def _collect_manifest_matches(manifest: Dict[str, Any], top5_only: bool) -> Dict[int, Dict[str, Any]]:
    rows: Dict[int, Dict[str, Any]] = {}
    for row in manifest.get("matches") or []:
        try:
            idx = int(row.get("index"))
        except Exception:
            continue
        if top5_only and _safe_text(row.get("league")) not in TOP5_LEAGUES:
            continue
        rows[idx] = row
    return rows


def _parse_synthesis_table(md_text: str) -> List[Dict[str, Any]]:
    lines = md_text.splitlines()
    items: List[Dict[str, Any]] = []
    in_table = False
    for line in lines:
        normalized = re.sub(r"\s+", " ", line.strip())
        if normalized.startswith("| Match ") and ("| 建议 " in normalized or "| Suggestion " in normalized):
            in_table = True
            continue
        if in_table and line.strip().startswith("| ---"):
            continue
        if in_table:
            if not line.strip().startswith("|"):
                break
            cols = [c.strip() for c in line.strip().strip("|").split("|")]
            if len(cols) < 5:
                continue
            match_name = cols[0]
            suggestion = cols[2].strip("` ")
            confidence = cols[3].strip("` ")
            reason = cols[4]
            items.append(
                {
                    "match": match_name,
                    "suggestion": suggestion,
                    "confidence": confidence,
                    "reason": reason,
                }
            )
    return items


def _load_synthesis_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    rows = result.get("match_verdicts") if isinstance(result.get("match_verdicts"), list) else []
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized.append(
            {
                "match": _safe_text(row.get("match")),
                "suggestion": _safe_text(row.get("final_suggestion") or row.get("suggestion") or "skip"),
                "analysis_suggestion": _safe_text(row.get("analysis_suggestion") or row.get("suggestion") or "skip"),
                "confidence": _safe_text(row.get("confidence") or "low"),
                "candidate_tier": _safe_text(row.get("candidate_tier") or ""),
                "non_actionable": bool(row.get("non_actionable")),
                "match_index": row.get("match_index"),
            }
        )
    return normalized


def _idx_from_match_name(match_name: str) -> Optional[int]:
    m = re.search(r"Audit-\d+-(\d+)-", match_name)
    if m:
        return int(m.group(1))
    return None


def _build_issue_match_lookup(issue_dir: Path) -> Dict[str, int]:
    lookup: Dict[str, int] = {}
    prematch_dir = issue_dir / "01_Prematch_Audits"
    for path in sorted(prematch_dir.glob("Audit-*.md")):
        name = path.name
        m = re.search(r"Audit-\d+-(\d+)-(.+)\.md$", name)
        if not m:
            continue
        idx = int(m.group(1))
        key = m.group(2).replace("_", " ").strip().lower()
        lookup[key] = idx
    return lookup


def _match_idx(row_match: str, lookup: Dict[str, int]) -> Optional[int]:
    key = _safe_text(row_match).lower()
    if key in lookup:
        return lookup[key]
    key = key.replace("  ", " ")
    return lookup.get(key)


def _normalize_team_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", _safe_text(text).lower())


def _manifest_pair_lookup(manifest: Dict[str, Any], top5_only: bool) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for row in manifest.get("matches") or []:
        try:
            idx = int(row.get("index"))
        except Exception:
            continue
        if top5_only and _safe_text(row.get("league")) not in TOP5_LEAGUES:
            continue
        english = _safe_text(row.get("english"))
        if " vs " not in english:
            continue
        home, away = [x.strip() for x in english.split(" vs ", 1)]
        key = f"{_normalize_team_key(home)}vs{_normalize_team_key(away)}"
        out[key] = idx
    return out


def _classify_review_label(
    suggestion_set: Set[str],
    result_code: Optional[str],
    process_payload: Optional[Dict[str, Any]],
) -> str:
    if not result_code:
        return "PENDING"
    result_hit = result_code in suggestion_set if suggestion_set else False
    if not process_payload:
        return "RESULT_HIT_PROCESS_UNKNOWN" if result_hit else "RESULT_MISS_PROCESS_UNKNOWN"

    xg_better = _safe_text(process_payload.get("xg_better_side"))
    xg_gap_abs = float(process_payload.get("xg_gap_abs") or 0.0)
    # 过程标签：用 xG 胜负方向 + 差值阈值来刻画 process quality。
    # New review taxonomy from 0510+0511 lessons:
    # - HIGH_QUALITY_HIT
    # - RESULT_HIT_PROCESS_WARNING
    # - PROTECTED_STRUCTURE_HIT
    # - CLEAR_MISS
    # - RESULT_MISS_PROCESS_CLOSE
    single_pick_like = len(suggestion_set) == 1
    if result_hit:
        if single_pick_like and xg_better == result_code and xg_gap_abs >= 0.35:
            return "HIGH_QUALITY_HIT"
        if (not single_pick_like) and xg_better == result_code and xg_gap_abs >= 0.35:
            return "PROTECTED_STRUCTURE_HIT"
        if xg_better and xg_better != result_code and xg_gap_abs >= 0.35:
            return "RESULT_HIT_PROCESS_WARNING"
        if not single_pick_like:
            return "PROTECTED_STRUCTURE_HIT"
        return "RESULT_HIT_PROCESS_NEUTRAL"

    if xg_better and xg_better != result_code and xg_gap_abs >= 0.35:
        return "CLEAR_MISS"
    return "RESULT_MISS_PROCESS_CLOSE"


def main() -> int:
    parser = argparse.ArgumentParser(description="Prematch 推演赛后回测（命中率 review）")
    parser.add_argument("--issue", required=True)
    parser.add_argument("--top5-only", action="store_true")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent.parent
    load_dotenv_into_env(repo_root)
    vault_env = _safe_text(os.getenv("ARES_VAULT_PATH"))
    if not vault_env:
        raise EnvironmentError("未检测到 ARES_VAULT_PATH。")
    vault_root = Path(normalize_vault_path(vault_env)).expanduser()
    issue_dir = vault_root / "03_Match_Audits" / str(args.issue)
    review_dir = issue_dir / "03_Review_Reports"
    analysis_dir = issue_dir / "02_Special_Analyses"
    manifest_path = vault_root / "04_RAG_Raw_Data" / "Cold_Data_Lake" / f"{args.issue}_dispatch_manifest.json"

    suffix = "-Top5" if args.top5_only else ""
    synthesis_md_path = analysis_dir / f"FINAL-{args.issue}-Prematch_Synthesis{suffix}.md"
    synthesis_json_path = analysis_dir / f"FINAL-{args.issue}-Prematch_Synthesis{suffix}.json"
    if not synthesis_md_path.exists():
        raise FileNotFoundError(f"找不到综合文件: {synthesis_md_path}")
    if not synthesis_json_path.exists():
        raise FileNotFoundError(f"找不到综合 JSON: {synthesis_json_path}")
    if not manifest_path.exists():
        raise FileNotFoundError(f"找不到 manifest: {manifest_path}")

    synthesis_payload = _load_json(synthesis_json_path)
    manifest = _load_json(manifest_path)
    match_rows = _load_synthesis_rows(synthesis_payload)
    manifest_by_idx = _collect_manifest_matches(manifest, top5_only=args.top5_only)
    pair_lookup = _manifest_pair_lookup(manifest, args.top5_only)
    manifest_by_understat = {
        _safe_text(row.get("understat_id")): row
        for row in (manifest.get("matches") or [])
        if _safe_text(row.get("understat_id"))
    }
    postmatch_results = _collect_postmatch_result_by_understat_id(issue_dir, str(args.issue))
    postmatch_process = _collect_postmatch_process_by_understat_id(issue_dir, str(args.issue))
    name_lookup = _build_issue_match_lookup(issue_dir)

    resolved: List[Dict[str, Any]] = []
    for row in match_rows:
        idx = _match_idx(row["match"], name_lookup)
        if idx is None and " vs " in _safe_text(row.get("match")):
            home, away = [x.strip() for x in _safe_text(row.get("match")).split(" vs ", 1)]
            key = f"{_normalize_team_key(home)}vs{_normalize_team_key(away)}"
            idx = pair_lookup.get(key)
        manifest_row = manifest_by_idx.get(idx or -1, {})
        result = _resolve_result_code(manifest_row) if manifest_row else None
        if not result and manifest_row:
            uid = _safe_text(manifest_row.get("understat_id"))
            if uid:
                result = postmatch_results.get(uid)
        if not result and idx:
            # 兜底：按 index 去 manifest 找 understat id 再查 postmatch
            fallback_row = manifest_by_idx.get(idx)
            uid = _safe_text((fallback_row or {}).get("understat_id"))
            if uid:
                result = postmatch_results.get(uid)
        uid = _safe_text((manifest_row or {}).get("understat_id"))
        suggestion = _safe_text(row.get("suggestion")).lower()
        picks = _suggestion_set(suggestion)
        is_actionable = (
            not bool(row.get("non_actionable"))
            and suggestion != "skip"
            and _safe_text(row.get("candidate_tier")) in {"稳胆", "博弈"}
        )
        if not is_actionable or suggestion == "skip":
            status = "skip"
        elif not result:
            uid = _safe_text((manifest_row or {}).get("understat_id"))
            if uid and uid not in postmatch_results:
                status = "missing_postmatch_artifact"
            else:
                status = "pending_match_result"
        elif result in picks:
            status = "hit"
        else:
            status = "miss"
        review_label = _classify_review_label(picks, result, postmatch_process.get(uid))
        resolved.append(
            {
                "idx": idx,
                "match": row["match"],
                "suggestion": row["suggestion"],
                "analysis_suggestion": row.get("analysis_suggestion") or row["suggestion"],
                "confidence": row["confidence"],
                "result": result or "-",
                "status": status,
                "review_label": review_label,
                "candidate_tier": row.get("candidate_tier") or "",
            }
        )

    actionable = [r for r in resolved if _safe_text(r["suggestion"]).lower() != "skip"]
    actionable = [r for r in actionable if r["status"] not in {"skip"}]
    settled = [r for r in actionable if r["status"] in {"hit", "miss"}]
    hits = sum(1 for r in settled if r["status"] == "hit")
    hit_rate = (hits / len(settled) * 100.0) if settled else 0.0
    pending = sum(1 for r in actionable if r["status"] in {"pending_match_result", "missing_postmatch_artifact"})
    skipped = sum(1 for r in resolved if r["status"] == "skip")

    lines: List[str] = []
    lines.append(f"# REVIEW-{args.issue}-Prematch_Outcome{'-Top5' if args.top5_only else ''}")
    lines.append("")
    lines.append(f"- Updated At: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%SZ')}")
    lines.append(f"- Scope: `{'Top5 Only' if args.top5_only else 'All Matches'}`")
    lines.append(f"- Synthesis Source: `{synthesis_md_path}`")
    lines.append(f"- Total Rows: `{len(resolved)}`")
    lines.append(f"- Actionable Picks: `{len(actionable)}`")
    lines.append(f"- Settled Picks: `{len(settled)}`")
    lines.append(f"- Hits: `{hits}`")
    lines.append(f"- Hit Rate: `{hit_rate:.1f}%`")
    lines.append(f"- Pending Results: `{pending}`")
    lines.append(f"- Skipped: `{skipped}`")
    lines.append("")
    lines.append("| # | Match | Suggestion | Confidence | Tier | Result | Status | ReviewLabel |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in resolved:
        lines.append(
            f"| {row.get('idx') or '-'} | {row['match']} | `{row['suggestion']}` | `{row['confidence']}` | `{row['candidate_tier'] or '-'}` | `{row['result']}` | `{row['status']}` | `{row['review_label']}` |"
        )

    lines.append("")
    lines.append("## 错误归因表")
    lines.append("| # | Match | 归因 | 说明 | 修正动作 |")
    lines.append("| --- | --- | --- | --- | --- |")
    for row in resolved:
        if row["status"] == "hit":
            cause = "命中"
            detail = f"建议方向覆盖赛果（{row['review_label']}）。"
            action = "保持现有规则。"
        elif row["status"] == "miss":
            if _safe_text(row["confidence"]).lower() == "low":
                cause = "低置信误判"
                detail = "低置信建议未命中，属于高噪音场次误入。"
                action = "低置信默认降级为观察，不进入执行池。"
            else:
                cause = "方向错误"
                detail = "建议方向与赛果不一致。"
                action = "回看该场 xG/盘口偏差阈值，修正边际门槛。"
        elif row["status"] == "missing_postmatch_artifact":
            cause = "缺少 postmatch 产物"
            detail = "比赛可能已结束，但当前 issue 目录下未找到对应 postmatch 落盘。"
            action = "补跑 postmatch 并重跑 outcome_review。"
        elif row["status"] == "pending_match_result":
            cause = "赛果未结算"
            detail = "暂未匹配到可结算赛果。"
            action = "待赛果入库后重跑 outcome_review。"
        else:
            cause = "主动回避"
            detail = "策略主动跳过。"
            action = "无需修正。"
        lines.append(
            f"| {row.get('idx') or '-'} | {row['match']} | {cause} | {detail} | {action} |"
        )

    review_dir.mkdir(parents=True, exist_ok=True)
    out_path = review_dir / f"REVIEW-{args.issue}-Prematch_Outcome{'-Top5' if args.top5_only else ''}.md"
    out_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")

    print("[summary]")
    print(f"issue={args.issue}")
    print(f"scope={'top5' if args.top5_only else 'all'}")
    print(f"rows={len(resolved)} actionable={len(actionable)} settled={len(settled)} hits={hits} pending={pending}")
    print(f"hit_rate={hit_rate:.1f}%")
    print(f"output={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
