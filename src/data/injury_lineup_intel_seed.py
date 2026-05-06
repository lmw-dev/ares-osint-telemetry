import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from audit_router import load_dotenv_into_env, normalize_vault_path
from team_forge import iter_issue_teams


def _load_manifest(vault_root: Path, issue: str) -> Dict[str, Any]:
    path = vault_root / "04_RAG_Raw_Data" / "Cold_Data_Lake" / f"{issue}_dispatch_manifest.json"
    if not path.exists():
        raise FileNotFoundError(f"找不到 manifest: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _split_match(english: str) -> Tuple[str, str]:
    if " vs " in english:
        home, away = english.split(" vs ", 1)
        return home.strip(), away.strip()
    if " VS " in english:
        home, away = english.split(" VS ", 1)
        return home.strip(), away.strip()
    return english.strip(), ""


def _build_issue_team_map(base_dir: Path, vault_root: Path, issue: str, manifest: Dict[str, Any]) -> Dict[str, str]:
    # Prefer iterator (already has alias normalization), fallback to manifest teams.
    out: Dict[str, str] = {}
    for team, league in iter_issue_teams(base_dir, vault_root, issue):
        if team and league:
            out[team] = league
    if out:
        return out
    for match in manifest.get("matches") or []:
        english = str(match.get("english") or "").strip()
        league = str(match.get("league") or "").strip() or "Unknown"
        home, away = _split_match(english)
        if home:
            out.setdefault(home, league)
        if away:
            out.setdefault(away, league)
    return out


def _default_team_payload(team: str, league: str) -> Dict[str, Any]:
    return {
        "team": team,
        "league": league,
        "source_items": [
            {
                "source_name": "Nowscore",
                "url": "",
                "fetched_at": "",
                "reliability": "UNKNOWN",
                "raw_status": "PENDING",
            },
            {
                "source_name": "Titan007",
                "url": "",
                "fetched_at": "",
                "reliability": "UNKNOWN",
                "raw_status": "PENDING",
            },
            {
                "source_name": "Transfermarkt",
                "url": "",
                "fetched_at": "",
                "reliability": "UNKNOWN",
                "raw_status": "PENDING",
            },
        ],
        "absences": [],
        "expected_core_availability": "UNKNOWN",
        "lineup_stability_precheck": "UNKNOWN",
        "key_node_absence_risk": "UNKNOWN",
        "recent_news_summary": "",
        "key_node_dependency": [],
        "prematch_focus_items": [],
        "market_external_notes": [],
        "youtube_tactical_briefs": [],
    }


def _load_existing(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    teams = payload.get("teams") if isinstance(payload.get("teams"), list) else []
    out: Dict[str, Dict[str, Any]] = {}
    for item in teams:
        if not isinstance(item, dict):
            continue
        team = str(item.get("team") or "").strip()
        if team:
            out[team] = item
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed TEAM-INTEL injury/lineup skeleton for one issue.")
    parser.add_argument("--issue", required=True, help="Issue id, e.g. DATE-20260504-top5")
    parser.add_argument("--merge", action="store_true", help="Merge into existing TEAM-INTEL-<issue>.json if exists.")
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent.parent.parent
    load_dotenv_into_env(base_dir)
    vault_env = os.getenv("ARES_VAULT_PATH")
    if not vault_env:
        raise EnvironmentError("未检测到 ARES_VAULT_PATH。")
    vault_root = Path(normalize_vault_path(vault_env)).expanduser()

    manifest = _load_manifest(vault_root, args.issue)
    teams = _build_issue_team_map(base_dir, vault_root, args.issue, manifest)
    review_dir = vault_root / "03_Match_Audits" / str(args.issue) / "03_Review_Reports"
    review_dir.mkdir(parents=True, exist_ok=True)
    target = review_dir / f"TEAM-INTEL-{args.issue}.json"

    existing = _load_existing(target) if args.merge else {}
    merged_teams: List[Dict[str, Any]] = []
    for team in sorted(teams):
        seeded = _default_team_payload(team, teams[team])
        if team in existing:
            # Keep user-maintained content first, fill missing keys from seed.
            payload = dict(seeded)
            payload.update(existing[team])
            for k in ("source_items", "absences", "key_node_dependency", "prematch_focus_items", "market_external_notes", "youtube_tactical_briefs"):
                if k in existing[team]:
                    payload[k] = existing[team][k]
            merged_teams.append(payload)
        else:
            merged_teams.append(seeded)

    payload = {
        "issue": args.issue,
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ"),
        "source": "injury_lineup_intel_seed.py",
        "description": "P0.3.1 seed for injury/expected lineup ingestion. Fill source_items and absences, then run team_archive_backfill.py.",
        "teams": merged_teams,
    }
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print("[summary]")
    print(f"issue={args.issue}")
    print(f"teams={len(merged_teams)}")
    print(f"output={target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

