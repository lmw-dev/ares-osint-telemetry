import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

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
    synthesis_path = analysis_dir / f"FINAL-{args.issue}-Prematch_Synthesis{suffix}.md"
    if not synthesis_path.exists():
        raise FileNotFoundError(f"找不到综合文件: {synthesis_path}")
    if not manifest_path.exists():
        raise FileNotFoundError(f"找不到 manifest: {manifest_path}")

    synthesis_md = synthesis_path.read_text(encoding="utf-8")
    manifest = _load_json(manifest_path)
    match_rows = _parse_synthesis_table(synthesis_md)
    manifest_by_idx = _collect_manifest_matches(manifest, top5_only=args.top5_only)
    name_lookup = _build_issue_match_lookup(issue_dir)

    resolved: List[Dict[str, Any]] = []
    for row in match_rows:
        idx = _match_idx(row["match"], name_lookup)
        manifest_row = manifest_by_idx.get(idx or -1, {})
        result = _resolve_result_code(manifest_row) if manifest_row else None
        picks = _suggestion_set(row.get("suggestion"))
        if row.get("suggestion", "").lower() == "skip":
            status = "skip"
        elif not result:
            status = "pending_result"
        elif result in picks:
            status = "hit"
        else:
            status = "miss"
        resolved.append(
            {
                "idx": idx,
                "match": row["match"],
                "suggestion": row["suggestion"],
                "confidence": row["confidence"],
                "result": result or "-",
                "status": status,
            }
        )

    actionable = [r for r in resolved if _safe_text(r["suggestion"]).lower() != "skip"]
    settled = [r for r in actionable if r["status"] in {"hit", "miss"}]
    hits = sum(1 for r in settled if r["status"] == "hit")
    hit_rate = (hits / len(settled) * 100.0) if settled else 0.0
    pending = sum(1 for r in actionable if r["status"] == "pending_result")
    skipped = sum(1 for r in resolved if r["status"] == "skip")

    lines: List[str] = []
    lines.append(f"# REVIEW-{args.issue}-Prematch_Outcome{'-Top5' if args.top5_only else ''}")
    lines.append("")
    lines.append(f"- Updated At: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%SZ')}")
    lines.append(f"- Scope: `{'Top5 Only' if args.top5_only else 'All Matches'}`")
    lines.append(f"- Synthesis Source: `{synthesis_path}`")
    lines.append(f"- Total Rows: `{len(resolved)}`")
    lines.append(f"- Actionable Picks: `{len(actionable)}`")
    lines.append(f"- Settled Picks: `{len(settled)}`")
    lines.append(f"- Hits: `{hits}`")
    lines.append(f"- Hit Rate: `{hit_rate:.1f}%`")
    lines.append(f"- Pending Results: `{pending}`")
    lines.append(f"- Skipped: `{skipped}`")
    lines.append("")
    lines.append("| # | Match | Suggestion | Confidence | Result | Status |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for row in resolved:
        lines.append(
            f"| {row.get('idx') or '-'} | {row['match']} | `{row['suggestion']}` | `{row['confidence']}` | `{row['result']}` | `{row['status']}` |"
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
