#!/usr/bin/env python3
"""
FotMob Completed Match Client (Ares OSINT Postmatch Pipeline)
Extracts official completed match evidence:
- Result evidence (final score, halftime score)
- Lineup & events (confirmed XI, formations, subs, goals, cards, penalties)
- Process stats (shots, SOT, possession, corners, big chances, fouls)
- xG & Shot Map (minute-by-minute shot map, cumulative xG, game momentum)
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

def fetch_fotmob_completed_page(match_id: str, canonical_url: Optional[str] = None) -> Tuple[Optional[Dict[str, Any]], str, str]:
    """
    Fetches FotMob completed match pageProps via /match/<real_id> following redirects.
    Returns: (pageProps, final_url, status)
    """
    url = f"https://www.fotmob.com/match/{match_id}"
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=12, context=CTX) as r:
            final_url = r.geturl()
            html = r.read().decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"Error fetching FotMob match {match_id}: {e}")
        return None, url, "SOURCE_UNAVAILABLE"

    m = re.search(r"<script id=\"__NEXT_DATA__\" type=\"application/json\">(.*?)</script>", html)
    if not m:
        return None, final_url, "PAGE_SCHEMA_CHANGED"

    try:
        data = json.loads(m.group(1))
        page_props = data.get("props", {}).get("pageProps", {})
        return page_props, final_url, "PASS"
    except Exception as e:
        print(f"Error decoding FotMob JSON {match_id}: {e}")
        return None, final_url, "PAGE_SCHEMA_CHANGED"

def parse_fotmob_completed_match(page_props: Dict[str, Any], canonical_url: str, bound_match_id: str) -> Dict[str, Any]:
    """
    Extracts deterministic postmatch evidence from pageProps.
    Enforces match ID hard assertion.
    """
    retrieved_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1. Identity assertion
    general = page_props.get("general", {})
    header = page_props.get("header", {})
    content = page_props.get("content", {})
    
    parsed_mid = str(general.get("matchId") or page_props.get("id") or "")
    if parsed_mid != str(bound_match_id):
        return {
            "status": "SOURCE_IDENTITY_MISMATCH",
            "error": f"Parsed match ID ({parsed_mid}) != bound match ID ({bound_match_id})",
            "retrieved_at_utc": retrieved_at_utc
        }

    # Match lifecycle
    status_info = header.get("status", {})
    finished = status_info.get("finished", False)
    started = status_info.get("started", False)
    reason = status_info.get("reason", {}).get("long", "Unknown")

    if not finished:
        return {
            "status": "NON_FINAL_MATCH",
            "lifecycle": "LIVE" if started else "SCHEDULED",
            "reason": reason,
            "retrieved_at_utc": retrieved_at_utc
        }

    # 2. Result Evidence
    teams = header.get("teams", [])
    home_score = None
    away_score = None
    home_name = general.get("homeTeam", {}).get("name", "")
    away_name = general.get("awayTeam", {}).get("name", "")

    if len(teams) >= 2:
        try:
            home_score = int(teams[0].get("score"))
            away_score = int(teams[1].get("score"))
        except (ValueError, TypeError):
            pass

    # Halftime score from events
    ht_home = None
    ht_away = None
    raw_events = content.get("matchFacts", {}).get("events", {}).get("events", [])
    for ev in raw_events:
        if ev.get("type") == "Half" and (ev.get("halfStrShort") == "HT" or ev.get("halfStrKey") == "halftime_short"):
            ht_home = ev.get("homeScore")
            ht_away = ev.get("awayScore")
            break

    result_evidence = {
        "final_score_home": home_score,
        "final_score_away": away_score,
        "final_score_str": f"{home_score}-{away_score}" if home_score is not None and away_score is not None else "UNKNOWN",
        "halftime_score_home": ht_home,
        "halftime_score_away": ht_away,
        "halftime_score_str": f"{ht_home}-{ht_away}" if ht_home is not None and ht_away is not None else "UNKNOWN",
        "finished": finished,
        "match_reason": reason,
        "source_url": canonical_url,
        "retrieved_at_utc": retrieved_at_utc
    }

    # 3. Lineup & Events Evidence
    lineup_data = content.get("lineup", {})
    home_lineup = lineup_data.get("homeTeam", {})
    away_lineup = lineup_data.get("awayTeam", {})
    
    home_starters_raw = home_lineup.get("starters", []) or []
    away_starters_raw = away_lineup.get("starters", []) or []

    def _normalize_player(p: Dict[str, Any]) -> Dict[str, Any]:
        p_name = p.get("name") or f"{p.get('firstName', '')} {p.get('lastName', '')}".strip()
        return {
            "player_id": p.get("id"),
            "name": p_name,
            "shirt": p.get("shirt"),
            "position": p.get("position"),
            "rating": p.get("rating", {}).get("num") if isinstance(p.get("rating"), dict) else None
        }

    home_xi = [_normalize_player(p) for p in home_starters_raw]
    away_xi = [_normalize_player(p) for p in away_starters_raw]
    home_bench = [_normalize_player(p) for p in (home_lineup.get("subs", []) or [])]
    away_bench = [_normalize_player(p) for p in (away_lineup.get("subs", []) or [])]

    is_confirmed = (len(home_xi) == 11 and len(away_xi) == 11)
    lineup_status = "CONFIRMED" if is_confirmed else "PARTIAL"

    # Events extraction
    goals = []
    red_cards = []
    penalties = []
    substitutions = []

    home_team_id = general.get("homeTeam", {}).get("id")
    away_team_id = general.get("awayTeam", {}).get("id")

    for ev in raw_events:
        ev_type = ev.get("type")
        m = ev.get("time", 0)
        p_name = ev.get("player", {}).get("name") if isinstance(ev.get("player"), dict) else None

        # Accurately resolve team_side: do not default to away! If unknown, mark UNKNOWN
        if ev.get("isHome") is True:
            team_side = "home"
        elif ev.get("isHome") is False:
            team_side = "away"
        else:
            # isHome is None or not boolean: check teamId
            ev_team_id = ev.get("shotmapEvent", {}).get("teamId") or ev.get("teamId")
            if ev_team_id is not None and home_team_id is not None and ev_team_id == home_team_id:
                team_side = "home"
            elif ev_team_id is not None and away_team_id is not None and ev_team_id == away_team_id:
                team_side = "away"
            else:
                team_side = "UNKNOWN"

        if ev_type == "Goal":
            sm = ev.get("shotmapEvent") or {}
            is_og = (
                ev.get("goalDescription") == "Own goal" or
                ev.get("isOwnGoal") is True or
                ev.get("ownGoal") is True or
                sm.get("isOwnGoal") is True or
                ev.get("suffix") == "OG"
            )
            is_pen = (
                ev.get("goalDescription") == "Penalty" or
                ev.get("suffix") == "Pen" or
                ev.get("shotType") == "Penalty" or
                sm.get("situation") == "Penalty"
            )
            goals.append({
                "minute": m,
                "player": p_name,
                "team_side": team_side,
                "is_own_goal": is_og,
                "is_penalty": is_pen
            })
            if is_pen:
                penalties.append({
                    "minute": m,
                    "player": p_name,
                    "team_side": team_side,
                    "result": "Scored"
                })
        elif ev_type == "MissedPenalty":
            penalties.append({
                "minute": m,
                "player": p_name,
                "team_side": team_side,
                "result": "Missed"
            })
        elif ev_type == "Card":
            c_type = ev.get("card")
            if c_type in ["Red", "YellowRed"]:
                red_cards.append({
                    "minute": m,
                    "player": p_name,
                    "team_side": team_side,
                    "card_type": c_type
                })
        elif ev_type == "Substitution":
            sw = ev.get("swap", [])
            p_in = sw[0].get("name") if len(sw) > 0 else None
            p_out = sw[1].get("name") if len(sw) > 1 else None
            substitutions.append({
                "minute": m,
                "team_side": team_side,
                "player_in": p_in,
                "player_out": p_out
            })

    lineup_events_evidence = {
        "lineup_status": lineup_status,
        "home_formation": home_lineup.get("formation"),
        "away_formation": away_lineup.get("formation"),
        "home_xi": home_xi,
        "away_xi": away_xi,
        "home_bench": home_bench,
        "away_bench": away_bench,
        "goals": goals,
        "red_cards": red_cards,
        "penalties": penalties,
        "substitutions": substitutions,
        "source_url": canonical_url,
        "retrieved_at_utc": retrieved_at_utc
    }

    # 4. Process Stats Evidence
    raw_stats_groups = content.get("stats", {}).get("Periods", {}).get("All", {}).get("stats", [])
    flat_stats = {}
    for g in raw_stats_groups:
        for it in g.get("stats", []):
            title = it.get("title")
            val = it.get("stats")
            if title and val and isinstance(val, list) and len(val) >= 2:
                flat_stats[title] = val

    def _get_stat(title: str, default_val=None) -> Tuple[Any, Any]:
        val = flat_stats.get(title)
        if val and len(val) >= 2:
            return val[0], val[1]
        return default_val, default_val

    pos_h, pos_a = _get_stat("Ball possession")
    shots_h, shots_a = _get_stat("Total shots")
    sot_h, sot_a = _get_stat("Shots on target")
    corn_h, corn_a = _get_stat("Corners")
    bc_h, bc_a = _get_stat("Big chances")
    xg_stat_h, xg_stat_a = _get_stat("Expected goals (xG)")
    npxg_h, npxg_a = _get_stat("xG non-penalty")
    xg_open_h, xg_open_a = _get_stat("xG open play")
    xg_set_h, xg_set_a = _get_stat("xG set play")
    xgot_h, xgot_a = _get_stat("xG on target (xGOT)")
    fouls_h, fouls_a = _get_stat("Fouls committed")
    if fouls_h is None:
        fouls_h, fouls_a = _get_stat("Fouls")

    def _to_int(v):
        try:
            return int(v) if v is not None else None
        except (ValueError, TypeError):
            return None

    def _to_float(v):
        try:
            return float(v) if v is not None else None
        except (ValueError, TypeError):
            return None

    canonical_shots_home = _to_int(shots_h)
    canonical_shots_away = _to_int(shots_a)
    fotmob_aggregate_xg_home = _to_float(xg_stat_h)
    fotmob_aggregate_xg_away = _to_float(xg_stat_a)
    fotmob_aggregate_npxg_home = _to_float(npxg_h)
    fotmob_aggregate_npxg_away = _to_float(npxg_a)

    process_stats_evidence = {
        "possession_home": _to_int(pos_h),
        "possession_away": _to_int(pos_a),
        "shots_home": canonical_shots_home,
        "shots_away": canonical_shots_away,
        "canonical_shots_home": canonical_shots_home,
        "canonical_shots_away": canonical_shots_away,
        "shots_on_target_home": _to_int(sot_h),
        "shots_on_target_away": _to_int(sot_a),
        "corners_home": _to_int(corn_h),
        "corners_away": _to_int(corn_a),
        "fouls_home": _to_int(fouls_h),
        "fouls_away": _to_int(fouls_a),
        "big_chances_home": _to_int(bc_h),
        "big_chances_away": _to_int(bc_a),
        "xg_home": fotmob_aggregate_xg_home,
        "xg_away": fotmob_aggregate_xg_away,
        "fotmob_aggregate_xg_home": fotmob_aggregate_xg_home,
        "fotmob_aggregate_xg_away": fotmob_aggregate_xg_away,
        "fotmob_aggregate_npxg_home": fotmob_aggregate_npxg_home,
        "fotmob_aggregate_npxg_away": fotmob_aggregate_npxg_away,
        "xg_non_penalty_home": fotmob_aggregate_npxg_home,
        "xg_non_penalty_away": fotmob_aggregate_npxg_away,
        "xg_open_play_home": _to_float(xg_open_h),
        "xg_open_play_away": _to_float(xg_open_a),
        "xg_set_play_home": _to_float(xg_set_h),
        "xg_set_play_away": _to_float(xg_set_a),
        "xgot_home": _to_float(xgot_h),
        "xgot_away": _to_float(xgot_a),
        "raw_stat_groups_present": [g.get("title") for g in raw_stats_groups],
        "source_url": canonical_url,
        "retrieved_at_utc": retrieved_at_utc
    }

    # 5. xG & Shot Map Evidence
    raw_shots = content.get("shotmap", {}).get("shots", []) or []
    home_team_id = general.get("homeTeam", {}).get("id")
    away_team_id = general.get("awayTeam", {}).get("id")

    final_score_home = home_score
    final_score_away = away_score

    normalized_shots = []
    own_goal_shots = []
    shotmap_event_count_home = 0
    shotmap_event_count_away = 0
    c_xg_h, c_xg_a = 0.0, 0.0
    xg_timeline = []

    for s in raw_shots:
        # Check if own goal (own goals do not count towards conventional shots/xG)
        is_og = (s.get("isOwnGoal") is True)
        if is_og:
            own_goal_shots.append({
                "minute": int(s.get("min", 0)),
                "player": s.get("playerName"),
                "team_id": s.get("teamId"),
                "is_own_goal": True
            })
            continue

        team_id = s.get("teamId")
        if team_id == home_team_id:
            team_side = "home"
            shotmap_event_count_home += 1
        elif team_id == away_team_id:
            team_side = "away"
            shotmap_event_count_away += 1
        else:
            team_side = "UNKNOWN"

        xg_val = s.get("expectedGoals")
        xg_parsed = round(float(xg_val), 4) if xg_val is not None else None
        min_val = int(s.get("min", 0))

        sit = s.get("situation")
        st_type = s.get("shotType")
        if sit == "Penalty" or st_type == "Penalty":
            is_pen = True
            pen_class = "PENALTY"
        elif (sit is None and st_type is None) or sit == "UNKNOWN" or st_type == "UNKNOWN":
            is_pen = None
            pen_class = "UNKNOWN"
        else:
            is_pen = False
            pen_class = "NON_PENALTY"

        shot_entry = {
            "id": s.get("id"),
            "minute": min_val,
            "team_side": team_side,
            "player": s.get("playerName"),
            "player_id": s.get("playerId"),
            "x": float(s.get("x", 0.0)) if s.get("x") is not None else None,
            "y": float(s.get("y", 0.0)) if s.get("y") is not None else None,
            "xg": xg_parsed,
            "result": s.get("eventType"),
            "situation": sit,
            "shot_type": st_type,
            "is_penalty": is_pen,
            "penalty_classification": pen_class,
            "is_on_target": s.get("isOnTarget", False),
            "is_blocked": s.get("isBlocked", False)
        }
        normalized_shots.append(shot_entry)

    # Sort chronological
    normalized_shots.sort(key=lambda x: (x["minute"], -(x["xg"] if x["xg"] is not None else -1.0)))

    shots_with_xg_h = [s for s in normalized_shots if s["team_side"] == "home" and s["xg"] is not None]
    shots_with_xg_a = [s for s in normalized_shots if s["team_side"] == "away" and s["xg"] is not None]
    shots_missing_xg_h = [s for s in normalized_shots if s["team_side"] == "home" and s["xg"] is None]
    shots_missing_xg_a = [s for s in normalized_shots if s["team_side"] == "away" and s["xg"] is None]

    c_xg_h = sum(s["xg"] for s in shots_with_xg_h)
    c_xg_a = sum(s["xg"] for s in shots_with_xg_a)

    for s in normalized_shots:
        xg_timeline.append({
            "minute": s["minute"],
            "team_side": s["team_side"],
            "player": s["player"],
            "xg": s["xg"],
            "result": s["result"],
            "is_penalty": s["is_penalty"],
            "cumulative_known_xg_home": round(c_xg_h, 4),
            "cumulative_known_xg_away": round(c_xg_a, 4)
        })

    derived_shotmap_xg_home = round(c_xg_h, 2) if normalized_shots else None
    derived_shotmap_xg_away = round(c_xg_a, 2) if normalized_shots else None

    # Penalty vs Non-penalty breakdowns
    pen_shots_h = [s for s in normalized_shots if s["team_side"] == "home" and s["is_penalty"] is True]
    pen_shots_a = [s for s in normalized_shots if s["team_side"] == "away" and s["is_penalty"] is True]
    non_pen_shots_h = [s for s in normalized_shots if s["team_side"] == "home" and s["is_penalty"] is False]
    non_pen_shots_a = [s for s in normalized_shots if s["team_side"] == "away" and s["is_penalty"] is False]
    amb_pen_shots_h = [s for s in normalized_shots if s["team_side"] == "home" and s["is_penalty"] is None]
    amb_pen_shots_a = [s for s in normalized_shots if s["team_side"] == "away" and s["is_penalty"] is None]

    derived_npxg_home = round(sum(s["xg"] for s in non_pen_shots_h if s["xg"] is not None), 2) if normalized_shots else None
    derived_npxg_away = round(sum(s["xg"] for s in non_pen_shots_a if s["xg"] is not None), 2) if normalized_shots else None
    penalty_xg_home = round(sum(s["xg"] for s in pen_shots_h if s["xg"] is not None), 4) if pen_shots_h else 0.0
    penalty_xg_away = round(sum(s["xg"] for s in pen_shots_a if s["xg"] is not None), 4) if pen_shots_a else 0.0

    # Explicit completeness indicators
    shotmap_xg_completeness = {
        "home": {
            "is_complete": (len(shots_missing_xg_h) == 0),
            "missing_xg_count": len(shots_missing_xg_h),
            "known_shots_count": len(shots_with_xg_h),
            "known_xg": derived_shotmap_xg_home
        },
        "away": {
            "is_complete": (len(shots_missing_xg_a) == 0),
            "missing_xg_count": len(shots_missing_xg_a),
            "known_shots_count": len(shots_with_xg_a),
            "known_xg": derived_shotmap_xg_away
        }
    }
    npxg_completeness = {
        "home": {
            "is_complete": (len(amb_pen_shots_h) == 0 and len([s for s in non_pen_shots_h if s["xg"] is None]) == 0),
            "missing_penalty_type_count": len(amb_pen_shots_h),
            "missing_xg_count_in_non_pen": len([s for s in non_pen_shots_h if s["xg"] is None]),
            "known_npxg": derived_npxg_home
        },
        "away": {
            "is_complete": (len(amb_pen_shots_a) == 0 and len([s for s in non_pen_shots_a if s["xg"] is None]) == 0),
            "missing_penalty_type_count": len(amb_pen_shots_a),
            "missing_xg_count_in_non_pen": len([s for s in non_pen_shots_a if s["xg"] is None]),
            "known_npxg": derived_npxg_away
        }
    }

    # Red card split calculation (with explicit same-minute ambiguity handling)
    if not red_cards:
        red_card_split = {
            "status": "NO_RED_CARD",
            "effective_11v11": True,
            "red_card_count": 0
        }
    else:
        earliest_red = min(red_cards, key=lambda x: x["minute"])
        rmin = earliest_red["minute"]
        r_side = earliest_red["team_side"]
        r_player = earliest_red["player"]

        pre_shots = [s for s in normalized_shots if s["minute"] < rmin]
        same_min_red_shots = [s for s in normalized_shots if s["minute"] == rmin]
        post_shots = [s for s in normalized_shots if s["minute"] > rmin]

        red_card_split = {
            "status": "RED_CARD_SPLIT_AVAILABLE",
            "red_card_count": len(red_cards),
            "first_red_minute": rmin,
            "first_red_team": r_side,
            "first_red_player": r_player,
            "is_stoppage_time": (rmin >= 90),
            "pre_red_11v11": {
                "window": f"1-{rmin-1}m" if rmin > 1 else "none",
                "shots_home": len([s for s in pre_shots if s["team_side"] == "home"]),
                "shots_away": len([s for s in pre_shots if s["team_side"] == "away"]),
                "xg_home": round(sum(s["xg"] for s in pre_shots if s["team_side"] == "home" and s["xg"] is not None), 2),
                "xg_away": round(sum(s["xg"] for s in pre_shots if s["team_side"] == "away" and s["xg"] is not None), 2),
                "npxg_home": round(sum(s["xg"] for s in pre_shots if s["team_side"] == "home" and s["is_penalty"] is False and s["xg"] is not None), 2),
                "npxg_away": round(sum(s["xg"] for s in pre_shots if s["team_side"] == "away" and s["is_penalty"] is False and s["xg"] is not None), 2),
            },
            "same_minute_ambiguous": {
                "window": f"{rmin}m",
                "shots_count": len(same_min_red_shots),
                "shots": same_min_red_shots,
                "note": "ORDER_AMBIGUOUS: Shots occurring in the same minute as red card without second-precision timestamps cannot be safely partitioned."
            },
            "post_red": {
                "window": f"{rmin+1}-FTm",
                "shots_home": len([s for s in post_shots if s["team_side"] == "home"]),
                "shots_away": len([s for s in post_shots if s["team_side"] == "away"]),
                "xg_home": round(sum(s["xg"] for s in post_shots if s["team_side"] == "home" and s["xg"] is not None), 2),
                "xg_away": round(sum(s["xg"] for s in post_shots if s["team_side"] == "away" and s["xg"] is not None), 2),
                "npxg_home": round(sum(s["xg"] for s in post_shots if s["team_side"] == "home" and s["is_penalty"] is False and s["xg"] is not None), 2),
                "npxg_away": round(sum(s["xg"] for s in post_shots if s["team_side"] == "away" and s["is_penalty"] is False and s["xg"] is not None), 2),
            }
        }

    # Consistency check: Goals in events timeline vs final official score
    event_goals_home = sum(1 for g in goals if g["team_side"] == "home")
    event_goals_away = sum(1 for g in goals if g["team_side"] == "away")
    event_goals_unknown = sum(1 for g in goals if g["team_side"] == "UNKNOWN")
    has_unknown_team_goal = (event_goals_unknown > 0)
    has_own_goal = (len(own_goal_shots) > 0) or any(g.get("is_own_goal") for g in goals)

    score_consistent = (
        final_score_home is not None and 
        final_score_away is not None and 
        event_goals_home == final_score_home and 
        event_goals_away == final_score_away and
        not has_unknown_team_goal
    )

    # Check VAR events for cancellations or reviews
    var_events = [ev for ev in raw_events if ev.get("type") == "Var" or "disallowed" in str(ev).lower()]

    if not normalized_shots:
        game_state_split = {
            "status": "UNKNOWN",
            "reason": "SHOTMAP_UNAVAILABLE_CANNOT_SPLIT"
        }
    elif has_unknown_team_goal:
        game_state_split = {
            "status": "UNKNOWN",
            "reliable_split_available": False,
            "reason": f"UNKNOWN_GOAL_TEAM: {event_goals_unknown} goal event(s) have UNKNOWN team side. Deterministic partitioning aborted even if scores match.",
            "event_goals_home": event_goals_home,
            "event_goals_away": event_goals_away,
            "event_goals_unknown": event_goals_unknown,
            "final_score_home": final_score_home,
            "final_score_away": final_score_away,
            "segments": []
        }
    elif not score_consistent:
        game_state_split = {
            "status": "INCONSISTENT_SCORE_EVENTS",
            "reliable_split_available": False,
            "reason": f"Event goals ({event_goals_home}-{event_goals_away}) do not match final score ({final_score_home}-{final_score_away}) or contain UNKNOWN teams. Deterministic partitioning aborted.",
            "event_goals_home": event_goals_home,
            "event_goals_away": event_goals_away,
            "event_goals_unknown": event_goals_unknown,
            "final_score_home": final_score_home,
            "final_score_away": final_score_away
        }
    elif not goals:
        game_state_split = {
            "status": "LEVEL_FULL_MATCH",
            "summary": "全场0-0平局均势",
            "has_own_goal": False,
            "segments": [{
                "state": "level",
                "score_str": "0-0",
                "window": "1-FTm",
                "shots_home": len([s for s in normalized_shots if s["team_side"] == "home"]),
                "shots_away": len([s for s in normalized_shots if s["team_side"] == "away"]),
                "xg_home": derived_shotmap_xg_home,
                "xg_away": derived_shotmap_xg_away,
                "npxg_home": derived_npxg_home,
                "npxg_away": derived_npxg_away
            }],
            "order_ambiguous_shots": []
        }
    else:
        # Strict temporal partitioning:
        # Group goals by minute to prevent repeated shot matching, descending windows or fake intermediate states
        from collections import OrderedDict
        goals_by_minute = OrderedDict()
        for g in sorted(goals, key=lambda x: x["minute"]):
            goals_by_minute.setdefault(g["minute"], []).append(g)

        segments = []
        order_ambiguous_shots = []
        same_minute_multi_goal_events = []

        cur_score_h = 0
        cur_score_a = 0
        last_event_min = 0
        matched_goal_shot_ids = set()

        for g_min, min_goals in goals_by_minute.items():
            same_min_shots = [s for s in normalized_shots if s["minute"] == g_min]

            if len(min_goals) == 1:
                g = min_goals[0]
                g_side = g["team_side"]

                prior_shots = [s for s in normalized_shots if last_event_min < s["minute"] < g_min]

                goal_shot = None
                if not g.get("is_own_goal"):
                    for s in same_min_shots:
                        s_id = s.get("id") or id(s)
                        if s_id not in matched_goal_shot_ids and s["result"] == "Goal" and s["team_side"] == g_side:
                            goal_shot = s
                            matched_goal_shot_ids.add(s_id)
                            break

                # Prior state includes shots before goal minute + the goal shot itself
                state_shots = list(prior_shots)
                if goal_shot:
                    state_shots.append(goal_shot)

                # Other shots in the same minute cannot be ordered without second-level timestamps
                for s in same_min_shots:
                    if s != goal_shot:
                        order_ambiguous_shots.append({
                            "minute": s["minute"],
                            "team_side": s["team_side"],
                            "player": s["player"],
                            "xg": s["xg"],
                            "reason": f"Occurred at minute {g_min} alongside goal by {g.get('player')}; sequence unconfirmed."
                        })

                if cur_score_h == cur_score_a:
                    st = "level"
                elif cur_score_h > cur_score_a:
                    st = "home_lead"
                else:
                    st = "away_lead"

                win_str = f"{last_event_min+1}-{g_min}m (incl. goal shot at {g_min}m)" if goal_shot else (
                    f"{last_event_min+1}-{g_min-1}m" if g_min > last_event_min + 1 else f"{g_min}m (own goal)"
                )

                missing_xg_h = sum(1 for s in state_shots if s["team_side"] == "home" and s["xg"] is None)
                missing_xg_a = sum(1 for s in state_shots if s["team_side"] == "away" and s["xg"] is None)
                missing_pen_h = sum(1 for s in state_shots if s["team_side"] == "home" and s["is_penalty"] is None)
                missing_pen_a = sum(1 for s in state_shots if s["team_side"] == "away" and s["is_penalty"] is None)

                seg_entry = {
                    "state": st,
                    "score_str": f"{cur_score_h}-{cur_score_a}",
                    "score_before_goal": f"{cur_score_h}-{cur_score_a}",
                    "window": win_str,
                    "shots_home": len([s for s in state_shots if s["team_side"] == "home"]),
                    "shots_away": len([s for s in state_shots if s["team_side"] == "away"]),
                    "xg_home": round(sum(s["xg"] for s in state_shots if s["team_side"] == "home" and s["xg"] is not None), 2),
                    "xg_away": round(sum(s["xg"] for s in state_shots if s["team_side"] == "away" and s["xg"] is not None), 2),
                    "npxg_home": round(sum(s["xg"] for s in state_shots if s["team_side"] == "home" and s["is_penalty"] is False and s["xg"] is not None), 2),
                    "npxg_away": round(sum(s["xg"] for s in state_shots if s["team_side"] == "away" and s["is_penalty"] is False and s["xg"] is not None), 2),
                    "xg_complete_home": (missing_xg_h == 0),
                    "xg_complete_away": (missing_xg_a == 0),
                    "npxg_complete_home": (missing_pen_h == 0 and missing_xg_h == 0),
                    "npxg_complete_away": (missing_pen_a == 0 and missing_xg_a == 0)
                }
                if missing_xg_h > 0 or missing_xg_a > 0:
                    seg_entry["missing_xg_shots_home"] = missing_xg_h
                    seg_entry["missing_xg_shots_away"] = missing_xg_a
                if missing_pen_h > 0 or missing_pen_a > 0:
                    seg_entry["missing_penalty_type_shots_home"] = missing_pen_h
                    seg_entry["missing_penalty_type_shots_away"] = missing_pen_a

                segments.append(seg_entry)

                if g_side == "home":
                    cur_score_h += 1
                elif g_side == "away":
                    cur_score_a += 1
                last_event_min = g_min

            else:
                # Multiple goals in the same minute:
                # 1. Output prior segment if there is an elapsed interval before g_min
                prior_shots = [s for s in normalized_shots if last_event_min < s["minute"] < g_min]
                if g_min > last_event_min + 1 or len(prior_shots) > 0:
                    if cur_score_h == cur_score_a:
                        st = "level"
                    elif cur_score_h > cur_score_a:
                        st = "home_lead"
                    else:
                        st = "away_lead"

                    win_str = f"{last_event_min+1}-{g_min-1}m" if g_min > last_event_min + 1 else f"{last_event_min+1}m"
                    segments.append({
                        "state": st,
                        "score_str": f"{cur_score_h}-{cur_score_a}",
                        "score_before_goal": f"{cur_score_h}-{cur_score_a}",
                        "window": win_str,
                        "shots_home": len([s for s in prior_shots if s["team_side"] == "home"]),
                        "shots_away": len([s for s in prior_shots if s["team_side"] == "away"]),
                        "xg_home": round(sum(s["xg"] for s in prior_shots if s["team_side"] == "home" and s["xg"] is not None), 2),
                        "xg_away": round(sum(s["xg"] for s in prior_shots if s["team_side"] == "away" and s["xg"] is not None), 2),
                        "npxg_home": round(sum(s["xg"] for s in prior_shots if s["team_side"] == "home" and s["is_penalty"] is False and s["xg"] is not None), 2),
                        "npxg_away": round(sum(s["xg"] for s in prior_shots if s["team_side"] == "away" and s["is_penalty"] is False and s["xg"] is not None), 2),
                        "xg_complete_home": all(s["xg"] is not None for s in prior_shots if s["team_side"] == "home"),
                        "xg_complete_away": all(s["xg"] is not None for s in prior_shots if s["team_side"] == "away"),
                        "npxg_complete_home": all(s["is_penalty"] is not None and s["xg"] is not None for s in prior_shots if s["team_side"] == "home"),
                        "npxg_complete_away": all(s["is_penalty"] is not None and s["xg"] is not None for s in prior_shots if s["team_side"] == "away")
                    })

                # 2. Match goal shots without reusing the same shot
                matched_in_min = []
                for g in min_goals:
                    g_side = g["team_side"]
                    g_shot = None
                    if not g.get("is_own_goal"):
                        for s in same_min_shots:
                            s_id = s.get("id") or id(s)
                            if s_id not in matched_goal_shot_ids and s["result"] == "Goal" and s["team_side"] == g_side:
                                g_shot = s
                                matched_goal_shot_ids.add(s_id)
                                matched_in_min.append(s)
                                break

                # 3. All shots in this minute cannot be safely partitioned into intermediate scores!
                # Mark as order ambiguous without outputting descending windows or fake intermediate scores
                for s in same_min_shots:
                    order_ambiguous_shots.append({
                        "minute": s["minute"],
                        "team_side": s["team_side"],
                        "player": s["player"],
                        "xg": s["xg"],
                        "result": s.get("result"),
                        "reason": f"SAME_MINUTE_MULTIPLE_GOALS: Occurred at minute {g_min} during {len(min_goals)} simultaneous goals; intermediate sequence unconfirmed."
                    })

                delta_h = sum(1 for g in min_goals if g["team_side"] == "home")
                delta_a = sum(1 for g in min_goals if g["team_side"] == "away")

                same_minute_multi_goal_events.append({
                    "minute": g_min,
                    "goals_count": len(min_goals),
                    "goals": min_goals,
                    "score_before": f"{cur_score_h}-{cur_score_a}",
                    "score_after": f"{cur_score_h + delta_h}-{cur_score_a + delta_a}",
                    "intermediate_score_states_omitted": True,
                    "reason": f"Multiple goals ({len(min_goals)}) in minute {g_min}. Sequence unconfirmed; deterministic intermediate state omitted."
                })

                cur_score_h += delta_h
                cur_score_a += delta_a
                last_event_min = g_min

        # Post-final-goal segment
        post_shots = [s for s in normalized_shots if s["minute"] > last_event_min]
        if cur_score_h == cur_score_a:
            st = "level"
        elif cur_score_h > cur_score_a:
            st = "home_lead"
        else:
            st = "away_lead"

        missing_post_xg_h = sum(1 for s in post_shots if s["team_side"] == "home" and s["xg"] is None)
        missing_post_xg_a = sum(1 for s in post_shots if s["team_side"] == "away" and s["xg"] is None)
        missing_post_pen_h = sum(1 for s in post_shots if s["team_side"] == "home" and s["is_penalty"] is None)
        missing_post_pen_a = sum(1 for s in post_shots if s["team_side"] == "away" and s["is_penalty"] is None)

        segments.append({
            "state": st,
            "score_str": f"{cur_score_h}-{cur_score_a}",
            "score": f"{cur_score_h}-{cur_score_a}",
            "window": f"{last_event_min+1}-FTm",
            "shots_home": len([s for s in post_shots if s["team_side"] == "home"]),
            "shots_away": len([s for s in post_shots if s["team_side"] == "away"]),
            "xg_home": round(sum(s["xg"] for s in post_shots if s["team_side"] == "home" and s["xg"] is not None), 2),
            "xg_away": round(sum(s["xg"] for s in post_shots if s["team_side"] == "away" and s["xg"] is not None), 2),
            "npxg_home": round(sum(s["xg"] for s in post_shots if s["team_side"] == "home" and s["is_penalty"] is False and s["xg"] is not None), 2),
            "npxg_away": round(sum(s["xg"] for s in post_shots if s["team_side"] == "away" and s["is_penalty"] is False and s["xg"] is not None), 2),
            "xg_complete_home": (missing_post_xg_h == 0),
            "xg_complete_away": (missing_post_xg_a == 0),
            "npxg_complete_home": (missing_post_pen_h == 0 and missing_post_xg_h == 0),
            "npxg_complete_away": (missing_post_pen_a == 0 and missing_post_xg_a == 0)
        })

        game_state_split = {
            "status": "GAME_STATE_SPLIT_AVAILABLE",
            "has_own_goal": has_own_goal,
            "score_consistent": True,
            "segments": segments,
            "order_ambiguous_shots": order_ambiguous_shots
        }
        if same_minute_multi_goal_events:
            game_state_split["same_minute_multi_goal_events"] = same_minute_multi_goal_events

    # Boundary limitations declaration
    boundary_limitations = {
        "discrepancy_explanation_policy": "DISCREPANCY_UNEXPLAINED",
        "open_play_vs_npxg_rule": "NON_PENALTY_XG_NOT_EQUAL_OPEN_PLAY_XG",
        "temporal_ambiguity_policy": "Goal shots belong to prior score state; non-goal shots occurring in the same minute as events are partitioned strictly into order_ambiguous_shots.",
        "own_goal_handling": "Own goals have no assigned conventional xG, do not count as team shots, but update match score timeline.",
        "var_cancellation_count": len(var_events)
    }

    # Momentum data
    momentum_raw = content.get("momentum") or content.get("matchFacts", {}).get("momentum") or {}
    momentum_available = bool(momentum_raw.get("main", {}).get("data")) if isinstance(momentum_raw, dict) else False

    process_stats_evidence["shotmap_event_count_home"] = shotmap_event_count_home
    process_stats_evidence["shotmap_event_count_away"] = shotmap_event_count_away

    xg_process_evidence = {
        "xg_source": "FotMob-Aggregate" if fotmob_aggregate_xg_home is not None else ("FotMob-Shotmap" if derived_shotmap_xg_home is not None else "UNKNOWN"),
        "xg_home": fotmob_aggregate_xg_home if fotmob_aggregate_xg_home is not None else derived_shotmap_xg_home,
        "xg_away": fotmob_aggregate_xg_away if fotmob_aggregate_xg_away is not None else derived_shotmap_xg_away,
        "fotmob_aggregate_xg_home": fotmob_aggregate_xg_home,
        "fotmob_aggregate_xg_away": fotmob_aggregate_xg_away,
        "fotmob_aggregate_npxg_home": fotmob_aggregate_npxg_home,
        "fotmob_aggregate_npxg_away": fotmob_aggregate_npxg_away,
        "derived_shotmap_xg_home": derived_shotmap_xg_home,
        "derived_shotmap_xg_away": derived_shotmap_xg_away,
        "derived_npxg_home": derived_npxg_home,
        "derived_npxg_away": derived_npxg_away,
        "shotmap_xg_completeness": shotmap_xg_completeness,
        "npxg_completeness": npxg_completeness,
        "penalty_shots_home": len(pen_shots_h),
        "penalty_shots_away": len(pen_shots_a),
        "penalty_xg_home": penalty_xg_home,
        "penalty_xg_away": penalty_xg_away,
        "canonical_shots_home": canonical_shots_home,
        "canonical_shots_away": canonical_shots_away,
        "shotmap_event_count_home": shotmap_event_count_home,
        "shotmap_event_count_away": shotmap_event_count_away,
        "shots_count": len(normalized_shots),
        "shot_map_available": bool(normalized_shots),
        "all_shots_chronological": normalized_shots,
        "xg_timeline": xg_timeline,
        "red_card_split": red_card_split,
        "game_state_split": game_state_split,
        "boundary_limitations": boundary_limitations,
        "momentum_available": momentum_available,
        "source_url": canonical_url,
        "retrieved_at_utc": retrieved_at_utc
    }

    return {
        "status": "PASS",
        "canonical_url": canonical_url,
        "fotmob_match_id": str(bound_match_id),
        "parsed_page_match_id": parsed_mid,
        "identity_assertion": "PASS",
        "home_team": home_name,
        "away_team": away_name,
        "result_evidence": result_evidence,
        "lineup_events_evidence": lineup_events_evidence,
        "process_stats_evidence": process_stats_evidence,
        "xg_process_evidence": xg_process_evidence,
        "retrieved_at_utc": retrieved_at_utc
    }
