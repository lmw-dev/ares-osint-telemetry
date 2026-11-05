#!/usr/bin/env python3
"""
Understat Completed-Match Client (Ares OSINT Postmatch Pipeline)
Extracts xG, shot map, and xG timeline from Understat completed-match pages.
Fails cleanly if match mapping is unavailable or schema changes.
"""

import urllib.request
import ssl
import json
import re
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple, List

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
}

UNDERSTAT_LEAGUE_MAP = {
    "epl": "EPL",
    "premier league": "EPL",
    "la liga": "La_liga",
    "laliga": "La_liga",
    "serie a": "Serie_A",
    "seriea": "Serie_A",
    "bundesliga": "Bundesliga",
    "ligue 1": "Ligue_1",
    "ligue1": "Ligue_1"
}

def decode_understat_json(escaped_str: str) -> Optional[Any]:
    """Decodes hex-escaped string from Understat script tags."""
    try:
        raw_json = escaped_str.encode("utf-8").decode("unicode_escape")
        return json.loads(raw_json)
    except Exception as e:
        try:
            # Fallback direct unescape
            unescaped = re.sub(r'\\x([0-9a-fA-F]{2})', lambda m: chr(int(m.group(1), 16)), escaped_str)
            return json.loads(unescaped)
        except Exception:
            return None

def parse_understat_match_page(html: str, match_url: str) -> Dict[str, Any]:
    """Parses Understat completed match HTML for shots, xG, and teams."""
    retrieved_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    shots_match = re.search(r"var shotsData\s*=\s*JSON\.parse\('([^']+)'\);", html)
    roster_match = re.search(r"var rostersData\s*=\s*JSON\.parse\('([^']+)'\);", html)
    match_info_match = re.search(r"var match_info\s*=\s*JSON\.parse\('([^']+)'\);", html)

    if not shots_match:
        return {
            "status": "PAGE_SCHEMA_CHANGED",
            "error": "shotsData script variable not found in Understat page",
            "canonical_url": match_url,
            "retrieved_at_utc": retrieved_at_utc
        }

    shots_data = decode_understat_json(shots_match.group(1))
    if shots_data is None:
        return {
            "status": "PAGE_SCHEMA_CHANGED",
            "error": "Failed to decode Understat shotsData JSON",
            "canonical_url": match_url,
            "retrieved_at_utc": retrieved_at_utc
        }

    home_shots_raw = shots_data.get("h", [])
    away_shots_raw = shots_data.get("a", [])

    def _normalize_shot(s: Dict[str, Any], team_side: str) -> Dict[str, Any]:
        return {
            "id": s.get("id"),
            "minute": int(s.get("minute", 0)),
            "team_side": team_side,
            "player": s.get("player"),
            "player_id": s.get("player_id"),
            "x": float(s.get("X", 0.0)),
            "y": float(s.get("Y", 0.0)),
            "xg": float(s.get("xG", 0.0)),
            "result": s.get("result"), # Goal, SavedShot, MissedShots, BlockedShot, ShotOnPost
            "situation": s.get("situation"), # OpenPlay, SetPiece, FromCorner, Penalty
            "shot_type": s.get("shotType"), # RightFoot, LeftFoot, Head
            "assist_player": s.get("player_assisted"),
            "last_action": s.get("lastAction")
        }

    home_shots = [_normalize_shot(s, "home") for s in home_shots_raw]
    away_shots = [_normalize_shot(s, "away") for s in away_shots_raw]

    # Calculate cumulative xG
    home_xg = sum(s["xg"] for s in home_shots)
    away_xg = sum(s["xg"] for s in away_shots)

    # Sort chronological
    all_shots = sorted(home_shots + away_shots, key=lambda x: (x["minute"], -x["xg"]))

    # Construct xG timeline
    timeline = []
    c_h, c_a = 0.0, 0.0
    for s in all_shots:
        if s["team_side"] == "home":
            c_h += s["xg"]
        else:
            c_a += s["xg"]
        timeline.append({
            "minute": s["minute"],
            "team_side": s["team_side"],
            "player": s["player"],
            "xg": s["xg"],
            "result": s["result"],
            "cumulative_xg_home": round(c_h, 4),
            "cumulative_xg_away": round(c_a, 4)
        })

    # Match info if available
    match_info = decode_understat_json(match_info_match.group(1)) if match_info_match else {}
    score_h = None
    score_a = None
    if match_info:
        try:
            goals = match_info.get("goals", {})
            score_h = int(goals.get("h")) if goals.get("h") is not None else None
            score_a = int(goals.get("a")) if goals.get("a") is not None else None
        except Exception:
            pass

    return {
        "status": "PASS",
        "canonical_url": match_url,
        "retrieved_at_utc": retrieved_at_utc,
        "xg_home": round(home_xg, 2),
        "xg_away": round(away_xg, 2),
        "shots_count_home": len(home_shots),
        "shots_count_away": len(away_shots),
        "home_score": score_h,
        "away_score": score_a,
        "home_shots": home_shots,
        "away_shots": away_shots,
        "all_shots_chronological": all_shots,
        "xg_timeline": timeline
    }

def fetch_understat_match_data(
    league: str,
    home_team: str,
    away_team: str,
    kickoff_utc: str,
    explicit_match_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Attempts to fetch and parse Understat match data.
    If unavailable, returns clean failure status without endless fallback.
    """
    retrieved_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if explicit_match_id:
        match_url = f"https://understat.com/match/{explicit_match_id}"
        req = urllib.request.Request(match_url, headers=HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=10, context=CTX) as r:
                html = r.read().decode("utf-8", errors="ignore")
            parsed = parse_understat_match_page(html, match_url)
            parsed["understat_match_id"] = str(explicit_match_id)
            return parsed
        except Exception as e:
            return {
                "status": "SOURCE_UNAVAILABLE",
                "understat_match_id": str(explicit_match_id),
                "error": str(e),
                "retrieved_at_utc": retrieved_at_utc
            }

    # If no explicit ID, Understat mapping is currently data_not_available for 2026/27
    return {
        "status": "DATA_NOT_AVAILABLE",
        "understat_match_id": None,
        "error": "Understat season schedule mapping currently unavailable for 2026/27",
        "retrieved_at_utc": retrieved_at_utc
    }
