import argparse
import json
import os
import re
from dataclasses import dataclass
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


def _parse_score(score_text: str) -> Optional[Tuple[int, int]]:
    txt = _safe_text(score_text).replace(":", "-")
    m = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s*$", txt)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def _winner_by_score(home_goals: int, away_goals: int) -> str:
    if home_goals > away_goals:
        return "home"
    if home_goals == away_goals:
        return "draw"
    return "away"


def _split_match_name(name: str) -> Tuple[str, str]:
    txt = _safe_text(name)
    if " vs " in txt:
        home, away = txt.split(" vs ", 1)
        return home.strip(), away.strip()
    if " VS " in txt:
        home, away = txt.split(" VS ", 1)
        return home.strip(), away.strip()
    return txt, ""


def _manifest_lookup(manifest: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    mapping: Dict[str, Dict[str, Any]] = {}
    for row in manifest.get("matches") or []:
        understat_id = _safe_text(row.get("understat_id"))
        if understat_id:
            mapping[understat_id] = row
    return mapping


@dataclass
class MatchTelemetry:
    match_id: str
    match_name: str
    home_team: str
    away_team: str
    score: str
    home_goals: int
    away_goals: int
    winner: str
    home_xg: float
    away_xg: float
    xg_gap: float
    xg_better_side: str
    xg_better_team: str
    variance_flag: bool
    pass_home: int
    pass_away: int
    pass_gap: int
    pass_better_team: str
    league: str


def _parse_postmatch_file(path: Path, league: str) -> Optional[MatchTelemetry]:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---"):
        return None

    parts = text.split("---", 2)
    if len(parts) < 3:
        return None

    frontmatter_raw = parts[1]
    frontmatter = yaml.safe_load(frontmatter_raw) or {}
    match_id = _safe_text(frontmatter.get("match_id") or "")
    match_name = _safe_text(frontmatter.get("match_name") or path.stem)
    home_team, away_team = _split_match_name(match_name)

    score_text = _safe_text(frontmatter.get("result", {}).get("score"))
    if not _parse_score(score_text):
        # 兼容 YAML 将 `1-1` 这类未加引号比分误解析为日期对象的情况：
        # 优先回退到 frontmatter 原文直接抽取 score 行。
        m_score = re.search(r"(?mi)^\s*score:\s*([0-9]+\s*-\s*[0-9]+)\s*$", frontmatter_raw)
        if m_score:
            score_text = _safe_text(m_score.group(1))
    score = _parse_score(score_text)
    if not score:
        return None
    home_goals, away_goals = score

    metrics = frontmatter.get("physical_metrics") or {}
    home_xg = float(metrics.get("home_xG") or 0.0)
    away_xg = float(metrics.get("away_xG") or 0.0)
    pass_home = int(metrics.get("passes_attacking_third_home") or 0)
    pass_away = int(metrics.get("passes_attacking_third_away") or 0)

    xg_gap = abs(home_xg - away_xg)
    xg_better_side = "draw"
    if home_xg > away_xg:
        xg_better_side = "home"
    elif away_xg > home_xg:
        xg_better_side = "away"

    return MatchTelemetry(
        match_id=match_id or path.stem,
        match_name=match_name,
        home_team=home_team,
        away_team=away_team,
        score=score_text,
        home_goals=home_goals,
        away_goals=away_goals,
        winner=_winner_by_score(home_goals, away_goals),
        home_xg=home_xg,
        away_xg=away_xg,
        xg_gap=xg_gap,
        xg_better_side=xg_better_side,
        xg_better_team=home_team if xg_better_side == "home" else away_team if xg_better_side == "away" else "draw",
        variance_flag=bool((frontmatter.get("system_evaluation") or {}).get("variance_flag")),
        pass_home=pass_home,
        pass_away=pass_away,
        pass_gap=abs(pass_home - pass_away),
        pass_better_team=home_team if pass_home >= pass_away else away_team,
        league=league,
    )


def _expected_points_by_xg(row: MatchTelemetry) -> Dict[str, int]:
    # 简化规则：xG 优势 >= 0.35 视为应拿 3 分；否则视为均势 1 分。
    if row.home_xg - row.away_xg >= 0.35:
        return {row.home_team: 3, row.away_team: 0}
    if row.away_xg - row.home_xg >= 0.35:
        return {row.home_team: 0, row.away_team: 3}
    return {row.home_team: 1, row.away_team: 1}


def _actual_points(row: MatchTelemetry) -> Dict[str, int]:
    if row.winner == "home":
        return {row.home_team: 3, row.away_team: 0}
    if row.winner == "away":
        return {row.home_team: 0, row.away_team: 3}
    return {row.home_team: 1, row.away_team: 1}


def _fmt_match_line(row: MatchTelemetry) -> str:
    return (
        f"- **[{row.league}] {row.match_name} {row.score}**："
        f"xG `{row.home_xg:.2f}-{row.away_xg:.2f}`，"
        f"进攻三区高危传球 `{row.pass_home}-{row.pass_away}`。"
    )


def _classify_review_case(row: MatchTelemetry, manifest_row: Dict[str, Any]) -> Dict[str, str]:
    flags = manifest_row.get("match_context_flags") if isinstance(manifest_row.get("match_context_flags"), dict) else {}
    market = manifest_row.get("market_behavior") if isinstance(manifest_row.get("market_behavior"), dict) else {}
    survival_level = _safe_text(flags.get("survival_pressure_level")).lower()
    new_manager_sample = int(flags.get("new_manager_sample_matches") or 0)
    manager_change_recent = bool(flags.get("manager_change_recent"))
    structural_crisis_context = bool(flags.get("structural_crisis_context"))
    favorite_overprice_risk = bool(market.get("favorite_overprice_risk"))

    case_type = "ALIGNED"
    if row.variance_flag:
        case_type = "VARIANCE_ALERT"
    elif (
        row.xg_better_side == "draw"
        and row.winner != "draw"
    ) or (
        row.xg_better_side in {"home", "away"} and row.winner != row.xg_better_side
    ):
        case_type = "SUSPICIOUS"

    error_type = "NONE"
    if manager_change_recent and new_manager_sample <= 1 and row.winner != "home":
        error_type = "NEW_MANAGER_REBOUND_OVERWEIGHTED"
    elif structural_crisis_context and survival_level in {"high", "extreme"} and row.winner == "home":
        error_type = "STRUCTURAL_CRISIS_OVERDOWNGRADE"
    elif survival_level in {"high", "extreme"} and row.xg_better_side == row.winner and row.winner in {"home", "away"}:
        error_type = "SURVIVAL_WIN_PATH_UNDERWEIGHTED"
    elif survival_level in {"high", "extreme"} and row.winner != row.xg_better_side and row.variance_flag:
        error_type = "STRUCTURAL_WEAK_SURVIVAL_TEAM_LIMIT"
    elif row.pass_gap >= 6 and row.winner != ("home" if row.pass_home > row.pass_away else "away"):
        error_type = "OVERPRICED_DOMINANCE_HANDICAP_FAILURE"
    elif favorite_overprice_risk and row.pass_gap >= 6 and row.winner != row.xg_better_side:
        error_type = "TERRITORY_NOT_EQUAL_XG_GATE_TRIGGERED"
    elif row.winner == "home" and (row.home_xg - row.away_xg) >= 0.8:
        error_type = "HIGH_QUALITY_HOME_FAVORITE_HIT"

    hit_type = "MAIN_HIT"
    if case_type == "VARIANCE_ALERT":
        hit_type = "PHYSICAL_DOMINANCE_VARIANCE"
    elif case_type == "SUSPICIOUS":
        hit_type = "RESULT_HIT_PROCESS_WARNING"

    return {"case_type": case_type, "error_type": error_type, "hit_type": hit_type}


def build_report(
    issue: str,
    rows: List[MatchTelemetry],
    top5_only: bool,
    expected_rows: Optional[List[Dict[str, Any]]] = None,
    manifest_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Tuple[str, Dict[str, Any]]:
    total = len(rows)
    variance_rows = [r for r in rows if r.variance_flag]
    aligned_rows = [
        r
        for r in rows
        if (not r.variance_flag)
        and (
            (r.xg_better_side == "draw" and r.winner == "draw")
            or (r.xg_better_side in {"home", "away"} and r.winner == r.xg_better_side)
        )
    ]
    suspicious_rows = [r for r in rows if r not in variance_rows and r not in aligned_rows]

    pass_dom_not_win = [
        r
        for r in rows
        if r.pass_gap >= 6
        and (
            (r.pass_home > r.pass_away and r.winner != "home")
            or (r.pass_away > r.pass_home and r.winner != "away")
        )
    ]

    team_delta: Dict[str, float] = {}
    for r in rows:
        expected = _expected_points_by_xg(r)
        actual = _actual_points(r)
        for team, exp_pt in expected.items():
            delta = float(exp_pt - actual.get(team, 0))
            team_delta[team] = team_delta.get(team, 0.0) + delta

    up_teams = sorted([(k, v) for k, v in team_delta.items() if v >= 2.0], key=lambda x: x[1], reverse=True)
    down_teams = sorted([(k, v) for k, v in team_delta.items() if v <= -2.0], key=lambda x: x[1])

    expected_rows = expected_rows or []
    actual_match_ids = {row.match_id for row in rows}
    missing_matches: List[Dict[str, Any]] = []
    for match in expected_rows:
        understat_id = _safe_text(match.get("understat_id"))
        if understat_id and understat_id in actual_match_ids:
            continue
        mapping_source = _safe_text(match.get("mapping_source")).lower()
        reason = "result_pending"
        if not understat_id:
            if mapping_source == "titan":
                reason = "titan_only_no_understat_id"
            else:
                reason = "no_understat_id"
        missing_matches.append(
            {
                "index": match.get("index"),
                "english": _safe_text(match.get("english")) or _safe_text(match.get("chinese")) or "Unknown",
                "reason": reason,
            }
        )

    manifest_lookup = manifest_lookup or {}
    review_cases: List[Dict[str, Any]] = []
    for row in rows:
        review_case = _classify_review_case(row, manifest_lookup.get(row.match_id, {}))
        error_type = review_case["error_type"]
        review_cases.append(
            {
                "match_id": row.match_id,
                "match_name": row.match_name,
                "case_type": review_case["case_type"],
                "error_type": error_type,
                "hit_type": review_case["hit_type"],
                "archive_patch_required": error_type != "NONE",
                "physical_validation": {
                    "xG": f"{row.home_xg:.2f}-{row.away_xg:.2f}",
                    "shots_on_target": None,
                    "attacking_third_passes": f"{row.pass_home}-{row.pass_away}",
                    "variance_flag": row.variance_flag,
                },
            }
        )

    summary_json: Dict[str, Any] = {
        "issue": issue,
        "scope": "top5" if top5_only else "all",
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ"),
        "total_matches": total,
        "expected_matches": len(expected_rows),
        "missing_matches": missing_matches,
        "variance_matches": len(variance_rows),
        "aligned_matches": len(aligned_rows),
        "suspicious_matches": len(suspicious_rows),
        "pass_dom_not_win_matches": len(pass_dom_not_win),
        "promote_teams": [{"team": t, "delta": round(d, 2)} for t, d in up_teams],
        "downgrade_teams": [{"team": t, "delta": round(d, 2)} for t, d in down_teams],
        "review_cases": review_cases,
    }

    lines: List[str] = []
    lines.append(f"# FINAL-{issue}-Postmatch_Synthesis{'-Top5' if top5_only else ''}")
    lines.append("")
    lines.append(f"- Updated At: {summary_json['updated_at']}")
    lines.append(f"- Issue: `{issue}`")
    lines.append(f"- Scope: `{'Top5 Only' if top5_only else 'All'}`")
    lines.append(f"- Total Matches: `{total}`")
    lines.append(f"- Expected Matches: `{len(expected_rows)}`")
    lines.append(f"- Variance Alerts: `{len(variance_rows)}`")
    lines.append(f"- Pass-Dominance But Not Win: `{len(pass_dom_not_win)}`")
    lines.append("")
    lines.append("## 0) Coverage")
    if missing_matches:
        lines.append(f"- Postmatch 覆盖 `{total}/{len(expected_rows)}`，仍缺 `{len(missing_matches)}` 场。")
        for item in missing_matches:
            lines.append(f"- `{item['index']}` `{item['english']}`: `{item['reason']}`")
    else:
        lines.append(f"- Postmatch 覆盖 `{total}/{len(expected_rows)}`，本期无缺口。")
    lines.append("")
    lines.append("## 0.5) Review 标准码")
    lines.append("- `case_type`: `ALIGNED` / `SUSPICIOUS` / `VARIANCE_ALERT`")
    lines.append("- `hit_type`: `MAIN_HIT` / `RESULT_HIT_PROCESS_WARNING` / `PHYSICAL_DOMINANCE_VARIANCE`")
    lines.append(
        "- `error_type`: `NONE` / `NEW_MANAGER_REBOUND_OVERWEIGHTED` / `SURVIVAL_WIN_PATH_UNDERWEIGHTED` / `OVERPRICED_DOMINANCE_HANDICAP_FAILURE` / `STRUCTURAL_CRISIS_OVERDOWNGRADE` / `STRUCTURAL_WEAK_SURVIVAL_TEAM_LIMIT` / `HIGH_QUALITY_HOME_FAVORITE_HIT` / `TERRITORY_NOT_EQUAL_XG_GATE_TRIGGERED`"
    )
    for item in review_cases:
        lines.append(
            f"- `{item['match_id']}` `{item['match_name']}`: case=`{item['case_type']}` hit=`{item['hit_type']}` error=`{item['error_type']}` archive_patch_required=`{item['archive_patch_required']}`"
        )
    lines.append("")
    lines.append("## ⚙️ 一、系统架构调整评估")
    lines.append("")
    lines.append("### 1) 代码层（Code Level）")
    lines.append(
        f"- 现有 `variance_flag` 机制有效：本期共识别 `{len(variance_rows)}/{total}` 场明显“比分与物理事实偏离”的比赛，核心逻辑无需重构。"
    )
    if pass_dom_not_win:
        lines.append(
            f"- 建议微调 `S_dynamic`：本期有 `{len(pass_dom_not_win)}` 场出现“高危传球显著占优但未赢球”，可上调进攻三区高危传球权重，用于提前预警‘得势不得分’型风险。"
        )
    else:
        lines.append("- 本期未出现显著的“高危传球压制却不胜”聚集特征，`S_dynamic` 可暂维持。")
    lines.append("")
    lines.append("### 2) 档案库层（Archive / Knowledge Base）")
    lines.append("- 建议对“xG 预期分 - 实际积分”偏差做强制回写：")
    if up_teams:
        lines.append(
            "- 强制提升评级（表现被低估）：" + "、".join([f"`{team}`(delta={delta:.1f})" for team, delta in up_teams])
        )
    else:
        lines.append("- 强制提升评级：暂无达到阈值的球队。")
    if down_teams:
        lines.append(
            "- 强制下调评级（结果高于表现）：" + "、".join([f"`{team}`(delta={delta:.1f})" for team, delta in down_teams])
        )
    else:
        lines.append("- 强制下调评级：暂无达到阈值的球队。")
    lines.append("")
    lines.append("## 📊 二、本期赛后物理分析")
    lines.append("")
    lines.append("### 🚨 1) 严重方差倒挂（建议下期降低比分权重、优先采信物理面）")
    if variance_rows:
        for row in variance_rows:
            lines.append(_fmt_match_line(row))
    else:
        lines.append("- 无。")
    lines.append("")
    lines.append("### ✅ 2) 逻辑闭环场次（物理优势兑现为赛果）")
    if aligned_rows:
        for row in aligned_rows:
            lines.append(_fmt_match_line(row))
    else:
        lines.append("- 无。")
    lines.append("")
    lines.append("### 🔍 3) 诡异/互啄场次（需谨慎解释）")
    if suspicious_rows:
        for row in suspicious_rows:
            lines.append(_fmt_match_line(row))
    else:
        lines.append("- 无。")
    lines.append("")
    lines.append("## Next Actions")
    lines.append("- 将“提升/下调评级”名单同步写入队档更新队列，下一期 prematch 前先完成回写。")
    lines.append("- 对“方差倒挂”场次在 prematch 检索层降低比分权重，优先读取 xG 与纵深传球证据。")
    lines.append("- 若继续使用 Top5 口径，请固定只消费 Top5 对阵，避免跨口径误判。")
    lines.append("")
    return "\n".join(lines).strip() + "\n", summary_json


def main() -> int:
    parser = argparse.ArgumentParser(description="Postmatch 赛后综合分析（系统调整 + 分组复盘）")
    parser.add_argument("--issue", required=True, help="issue 编号")
    parser.add_argument("--top5-only", action="store_true", help="仅统计五大联赛")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent.parent
    load_dotenv_into_env(repo_root)
    vault_env = _safe_text(os.getenv("ARES_VAULT_PATH"))
    if not vault_env:
        raise EnvironmentError("未检测到 ARES_VAULT_PATH。")
    vault_root = Path(normalize_vault_path(vault_env)).expanduser()

    issue_dir = vault_root / "03_Match_Audits" / str(args.issue)
    postmatch_dir = issue_dir / "04_Postmatch_Telemetry"
    analysis_dir = issue_dir / "02_Special_Analyses"
    analysis_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = vault_root / "04_RAG_Raw_Data" / "Cold_Data_Lake" / f"{args.issue}_dispatch_manifest.json"

    if not postmatch_dir.exists():
        raise FileNotFoundError(f"找不到 postmatch 目录: {postmatch_dir}")
    if not manifest_path.exists():
        raise FileNotFoundError(f"找不到 manifest: {manifest_path}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    mid_lookup = _manifest_lookup(manifest)

    rows: List[MatchTelemetry] = []
    for path in sorted(postmatch_dir.glob(f"{args.issue}_*_postmatch.md")):
        m = re.search(rf"{re.escape(args.issue)}_(\d+)_postmatch\.md$", path.name)
        match_id = m.group(1) if m else ""
        manifest_row = mid_lookup.get(match_id, {})
        league = _safe_text(manifest_row.get("league")) or "Unknown"
        if args.top5_only and league not in TOP5_LEAGUES:
            continue
        row = _parse_postmatch_file(path, league)
        if row:
            rows.append(row)

    if not rows:
        raise RuntimeError("无可用 postmatch 数据，无法生成综合分析。")

    expected_rows = [
        row
        for row in (manifest.get("matches") or [])
        if (not args.top5_only) or _safe_text(row.get("league")) in TOP5_LEAGUES
    ]
    md_text, payload = build_report(
        args.issue,
        rows,
        args.top5_only,
        expected_rows=expected_rows,
        manifest_lookup=mid_lookup,
    )
    suffix = "-Top5" if args.top5_only else ""
    md_path = analysis_dir / f"FINAL-{args.issue}-Postmatch_Synthesis{suffix}.md"
    json_path = analysis_dir / f"FINAL-{args.issue}-Postmatch_Synthesis{suffix}.json"
    md_path.write_text(md_text, encoding="utf-8")
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print("[summary]")
    print(f"issue={args.issue}")
    print(f"scope={'top5' if args.top5_only else 'all'}")
    print(f"matches={len(rows)} variance={payload['variance_matches']} aligned={payload['aligned_matches']}")
    print(f"output_md={md_path}")
    print(f"output_json={json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
