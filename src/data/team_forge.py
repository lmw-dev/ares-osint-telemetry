import argparse
import logging
import os
import re
import json
from pathlib import Path
from typing import Dict, Any, Iterable, List, Optional, Tuple

import yaml
from team_archive_paths import candidate_team_filenames, league_archive_dir
from team_archive_paths import canonical_team_filename


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("AresTelemetry.TeamForge")


DEFAULT_FRONTMATTER: Dict[str, Any] = {
    "intel_base": {
        "manager_doctrine": "Unknown",
        "market_sentiment": "Neutral",
        "key_node_dependency": [],
        "recent_news_summary": "",
    },
    "market_osint": {
        "market_external_notes": [],
        "youtube_tactical_briefs": [],
    },
    "physical_reality": {
        "avg_xG_last_5": 1.0,
        "conversion_efficiency": 0.05,
        "defensive_leakage": 0.5,
        "variance_history": [],
        "actual_tactical_entropy": 0.40,
    },
    "reality_gap": {
        "bias_type": "Aligned",
        "S_dynamic_modifier": 0.0,
    },
}


DEFAULT_BODY = (
    "## Team Notes\n\n"
    "- Baseline profile initialized by `team_forge.py`.\n"
    "- Add tactical observations, injury patterns, and review snapshots below.\n"
    "\n## Market & YouTube Intel\n\n"
    "- Market external notes: pending.\n"
    "- YouTube tactical briefs: pending.\n"
)

_INVALID_SEGMENT_PATTERN = re.compile(r'[<>:"/\\|?*\x00-\x1F]+')
_PAIR_SPLIT_TOKENS = (" vs ", " VS ", "vs", "VS")

TEAM_LEAGUE_HINTS: Dict[str, str] = {
    "burnley": "EPL",
    "manchestercity": "EPL",
    "bournemouth": "EPL",
    "leeds": "EPL",
    "bayerleverkusen": "Bundesliga",
    "bayernmunich": "Bundesliga",
    "rbleipzig": "Bundesliga",
    "unionberlin": "Bundesliga",
    "vfbstuttgart": "Bundesliga",
    "freiburg": "Bundesliga",
    "fortunadusseldorf": "Bundesliga_2",
    "dynamodresden": "Bundesliga_2",
    "kaiserslautern": "Bundesliga_2",
    "eintrachtbraunschweig": "Bundesliga_2",
    "atalanta": "Serie_A",
    "inter": "Serie_A",
    "torino": "Serie_A",
    "lazio": "Serie_A",
    "napoli": "Serie_A",
    "cremonese": "Serie_A",
    "elche": "La_liga",
    "atleticomadrid": "La_liga",
    "realsociedad": "La_liga",
    "getafe": "La_liga",
    "barcelona": "La_liga",
    "celtavigo": "La_liga",
    "levante": "La_liga",
    "sevilla": "La_liga",
    "rayovallecano": "La_liga",
    "espanyol": "La_liga",
    "realoviedo": "La_liga",
    "villarreal": "La_liga",
    "parissaintgermain": "Ligue_1",
    "lehavre": "Ligue_1",
    "metz": "Ligue_1",
    "parisfc": "Ligue_1",
    "lille": "Ligue_1",
    "nantes": "Ligue_1",
    "brest": "Ligue_1",
    "lens": "Ligue_1",
    "amiens": "Ligue_2",
    "montpellier": "Ligue_2",
    "annecy": "Ligue_2",
    "paufc": "Ligue_2",
    "redstarfc": "Ligue_2",
    "guingamp": "Ligue_2",
    "laval": "Ligue_2",
    "rodez": "Ligue_2",
    "goaheadeagles": "Eredivisie",
    "azalkmaar": "Eredivisie",
    "psv": "Eredivisie",
    "zwolle": "Eredivisie",
    "denbosch": "Eerste_Divisie",
    "adodenhaag": "Eerste_Divisie",
    "dordrecht": "Eerste_Divisie",
    "willemii": "Eerste_Divisie",
    "avs": "Primeira_Liga",
    "arouca": "Primeira_Liga",
}


def load_dotenv_into_env(base_dir: Path) -> None:
    env_path = base_dir / ".env"
    if not env_path.exists():
        return

    with open(env_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                continue
            if value and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]
            os.environ.setdefault(key, value)


def normalize_vault_path(path_text: str) -> Path:
    normalized = str(path_text).replace("\\ ", " ").replace("\\~", "~")
    return Path(normalized).expanduser()


def sanitize_segment(value: str, field_name: str) -> str:
    cleaned = value.strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = _INVALID_SEGMENT_PATTERN.sub("_", cleaned)
    cleaned = cleaned.strip(" .")

    if not cleaned:
        raise ValueError(f"{field_name} 不能为空或仅包含非法字符")
    if cleaned in {".", ".."}:
        raise ValueError(f"{field_name} 非法：不能为 . 或 ..")
    return cleaned


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


def split_pair_text(value: str) -> Tuple[str, str]:
    txt = str(value or "").strip()
    for token in _PAIR_SPLIT_TOKENS:
        if token in txt:
            home, away = txt.split(token, 1)
            return home.strip(), away.strip()
    return txt, ""


def load_team_alias_map(base_dir: Path) -> Dict[str, str]:
    alias_path = base_dir / "src" / "data" / "team_alias_map.json"
    try:
        return json.loads(alias_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def resolve_team_name(raw_team: str, alias_map: Dict[str, str]) -> str:
    team = str(raw_team or "").strip()
    return str(alias_map.get(team, team)).strip()


def infer_league(*teams: str, explicit_league: Optional[str] = None) -> Optional[str]:
    if explicit_league:
        return explicit_league
    for team in teams:
        league = TEAM_LEAGUE_HINTS.get(normalize_key(team))
        if league:
            return league
    return None


def split_frontmatter(content: str) -> Tuple[Dict[str, Any], str]:
    if content.startswith("---\n"):
        closing_marker_index = content.find("\n---\n", 4)
        if closing_marker_index != -1:
            frontmatter_raw = content[4:closing_marker_index]
            body = content[closing_marker_index + len("\n---\n") :].lstrip("\n")
            try:
                frontmatter = yaml.safe_load(frontmatter_raw) or {}
            except Exception:
                frontmatter = {}
            if not isinstance(frontmatter, dict):
                frontmatter = {}
            return frontmatter, body
    return {}, content


def read_existing_content(target_path: Path) -> Tuple[Dict[str, Any], str]:
    if not target_path.exists():
        return {}, DEFAULT_BODY

    content = target_path.read_text(encoding="utf-8")
    if not content.strip():
        return {}, DEFAULT_BODY

    frontmatter, body = split_frontmatter(content)
    return frontmatter, body or DEFAULT_BODY


def merge_frontmatter_defaults(existing: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(existing or {})
    for key, default_value in defaults.items():
        existing_value = merged.get(key)
        if isinstance(default_value, dict):
            child_existing = existing_value if isinstance(existing_value, dict) else {}
            merged[key] = merge_frontmatter_defaults(child_existing, default_value)
            continue
        if key not in merged:
            merged[key] = default_value
    return merged


def build_markdown(frontmatter: Dict[str, Any], body: str) -> str:
    yaml_text = yaml.safe_dump(frontmatter, allow_unicode=True, sort_keys=False)
    body_text = body.rstrip() + "\n"
    return f"---\n{yaml_text}---\n\n{body_text}"


def write_markdown_safely(target_path: Path, content: str) -> None:
    temp_path = target_path.with_suffix(target_path.suffix + ".tmp")
    temp_path.write_text(content, encoding="utf-8")
    temp_path.replace(target_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Initialize or upgrade team archive Markdown with v4.2 schema."
    )
    parser.add_argument("--team", help="Team name, e.g. Arsenal")
    parser.add_argument("--league", help="League name, e.g. EPL")
    parser.add_argument("--issue", help="Dispatch manifest issue, e.g. 26065")
    return parser.parse_args()


def build_archive_path(vault_root: Path, team: str, league: str) -> Path:
    team_name = sanitize_segment(team, "team")
    league_name = sanitize_segment(league, "league")
    canonical_name = canonical_team_filename(team_name)
    candidates = candidate_team_filenames(team_name)
    seen = set()
    archive_root = vault_root / "02_Team_Archives"
    recursive_hits = []
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        recursive_hits.extend(archive_root.glob(f"**/{candidate}.md"))
    unique_recursive_hits = []
    seen_paths = set()
    for path in recursive_hits:
        resolved = str(path.resolve())
        if resolved in seen_paths:
            continue
        seen_paths.add(resolved)
        unique_recursive_hits.append(path)
    if len(unique_recursive_hits) == 1:
        return unique_recursive_hits[0]

    archive_dir = league_archive_dir(archive_root, league_name)
    archive_dir.mkdir(parents=True, exist_ok=True)
    seen.clear()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        existing = archive_dir / f"{candidate}.md"
        if existing.exists():
            return existing
    return archive_dir / f"{canonical_name}.md"


def ensure_team_archive(vault_root: Path, *, team: str, league: str) -> Path:
    team_name = sanitize_segment(team, "team")
    league_name = sanitize_segment(league, "league")
    archive_root = vault_root / "02_Team_Archives"
    archive_dir = league_archive_dir(archive_root, league_name)
    archive_dir.mkdir(parents=True, exist_ok=True)

    canonical_name = canonical_team_filename(team_name)
    canonical_path = archive_dir / f"{canonical_name}.md"
    candidates = candidate_team_filenames(team_name)

    existing: List[Path] = []
    seen = set()
    for candidate in [canonical_name, *candidates, team_name]:
        txt = str(candidate).strip()
        if not txt or txt in seen:
            continue
        seen.add(txt)
        path = archive_dir / f"{txt}.md"
        if path.exists():
            existing.append(path)

    target_path = canonical_path
    if not canonical_path.exists() and existing:
        # Prefer keeping the richest existing content when canonical file absent.
        best = max(existing, key=lambda p: p.stat().st_size)
        if best != canonical_path:
            best.replace(canonical_path)
        target_path = canonical_path
    elif canonical_path.exists():
        target_path = canonical_path

    # Move duplicate alias files to archive to avoid future split-brain updates.
    alias_archive_root = archive_root / "99_Alias_Archive" / "auto_name_cleanup" / archive_dir.name
    for path in existing:
        if path == target_path or not path.exists():
            continue
        alias_archive_root.mkdir(parents=True, exist_ok=True)
        archived = alias_archive_root / path.name
        if archived.exists():
            archived.unlink()
        path.replace(archived)

    frontmatter, body = read_existing_content(target_path)
    merged_frontmatter = merge_frontmatter_defaults(frontmatter, DEFAULT_FRONTMATTER)
    markdown_content = build_markdown(merged_frontmatter, body)
    write_markdown_safely(target_path, markdown_content)
    logger.info("球队档案写入完成 -> %s", target_path)
    return target_path


def iter_issue_teams(base_dir: Path, vault_root: Path, issue: str) -> Iterable[Tuple[str, str]]:
    manifest_path = vault_root / "04_RAG_Raw_Data" / "Cold_Data_Lake" / f"{issue}_dispatch_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"找不到 dispatch manifest: {manifest_path}")

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    alias_map = load_team_alias_map(base_dir)
    yielded = set()
    for match in payload.get("matches", []):
        home, away = split_pair_text(match.get("english", ""))
        if not home or not away:
            home_zh, away_zh = split_pair_text(match.get("chinese", ""))
            home = home or resolve_team_name(home_zh, alias_map)
            away = away or resolve_team_name(away_zh, alias_map)
        else:
            home = resolve_team_name(home, alias_map)
            away = resolve_team_name(away, alias_map)

        league = infer_league(home, away, explicit_league=match.get("league"))
        if not league:
            logger.warning("无法推断联赛，跳过 issue=%s match=%s", issue, match.get("english") or match.get("chinese"))
            continue

        for team in (home, away):
            key = (league, team)
            if key in yielded:
                continue
            yielded.add(key)
            yield team, league


def main() -> int:
    args = parse_args()
    base_dir = Path(__file__).resolve().parent.parent.parent
    load_dotenv_into_env(base_dir)

    vault_env = os.getenv("ARES_VAULT_PATH")
    if not vault_env:
        raise EnvironmentError(
            "未检测到环境变量 ARES_VAULT_PATH，请先在 shell 或 .env 中配置后再执行。"
        )

    vault_root = normalize_vault_path(vault_env)
    if args.issue:
        created = 0
        for team, league in iter_issue_teams(base_dir, vault_root, args.issue):
            ensure_team_archive(vault_root, team=team, league=league)
            created += 1
        logger.info("issue=%s 球队档案批量补齐完成，共处理 %s 支球队", args.issue, created)
        return 0

    if not args.team or not args.league:
        raise ValueError("单队模式必须同时提供 --team 和 --league，或改用 --issue 批量模式。")

    ensure_team_archive(vault_root, team=args.team, league=args.league)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
