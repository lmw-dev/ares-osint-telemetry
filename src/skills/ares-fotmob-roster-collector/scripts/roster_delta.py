#!/usr/bin/env python3
"""
FotMob Roster Delta Calculator (Ares OSINT Telemetry Pipeline)
Computes deterministic delta between Prematch roster snapshot and Final Refresh roster snapshot.
"""

from typing import Dict, Any, List, Set, Optional

def _extract_player_names(player_list: List[Dict[str, Any]]) -> Set[str]:
    names = set()
    for p in player_list:
        if isinstance(p, dict):
            name = p.get("player_name") or p.get("name")
            if name:
                names.add(name.strip())
        elif isinstance(p, str):
            names.add(p.strip())
    return names

def compute_roster_delta(
    prev_snapshot: Dict[str, Any],
    curr_snapshot: Dict[str, Any],
    prev_news: Optional[Dict[str, Any]] = None,
    curr_news: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Computes a deterministic roster delta comparing analysis-time Prematch snapshot
    with the latest Final Refresh snapshot.
    """
    prev_utc = prev_snapshot.get("retrieved_at_utc")
    curr_utc = curr_snapshot.get("retrieved_at_utc")
    
    status_before = prev_snapshot.get("lineup_status", "NOT_AVAILABLE")
    status_now = curr_snapshot.get("lineup_status", "NOT_AVAILABLE")
    
    newly_confirmed = (status_before != "CONFIRMED" and status_now == "CONFIRMED")
    
    # XI changes
    prev_xi_h = _extract_player_names(prev_snapshot.get("confirmed_xi_home") or prev_snapshot.get("predicted_xi_home") or [])
    curr_xi_h = _extract_player_names(curr_snapshot.get("confirmed_xi_home") or curr_snapshot.get("predicted_xi_home") or [])
    
    prev_xi_a = _extract_player_names(prev_snapshot.get("confirmed_xi_away") or prev_snapshot.get("predicted_xi_away") or [])
    curr_xi_a = _extract_player_names(curr_snapshot.get("confirmed_xi_away") or curr_snapshot.get("predicted_xi_away") or [])
    
    xi_changes = {
        "home": {
            "added_starters": sorted(list(curr_xi_h - prev_xi_h)),
            "removed_starters": sorted(list(prev_xi_h - curr_xi_h))
        },
        "away": {
            "added_starters": sorted(list(curr_xi_a - prev_xi_a)),
            "removed_starters": sorted(list(prev_xi_a - curr_xi_a))
        }
    }
    
    # Injuries / Suspensions / Doubtful
    prev_inj_h = _extract_player_names(prev_snapshot.get("injured_home", []))
    curr_inj_h = _extract_player_names(curr_snapshot.get("injured_home", []))
    prev_inj_a = _extract_player_names(prev_snapshot.get("injured_away", []))
    curr_inj_a = _extract_player_names(curr_snapshot.get("injured_away", []))
    
    prev_dbt_h = _extract_player_names(prev_snapshot.get("doubtful_home", []))
    curr_dbt_h = _extract_player_names(curr_snapshot.get("doubtful_home", []))
    prev_dbt_a = _extract_player_names(prev_snapshot.get("doubtful_away", []))
    curr_dbt_a = _extract_player_names(curr_snapshot.get("doubtful_away", []))
    
    prev_susp_h = _extract_player_names(prev_snapshot.get("suspended_home", []))
    curr_susp_h = _extract_player_names(curr_snapshot.get("suspended_home", []))
    prev_susp_a = _extract_player_names(prev_snapshot.get("suspended_away", []))
    curr_susp_a = _extract_player_names(curr_snapshot.get("suspended_away", []))
    
    prev_unavail_h = _extract_player_names(prev_snapshot.get("unavailable_home", []))
    curr_unavail_h = _extract_player_names(curr_snapshot.get("unavailable_home", []))
    prev_unavail_a = _extract_player_names(prev_snapshot.get("unavailable_away", []))
    curr_unavail_a = _extract_player_names(curr_snapshot.get("unavailable_away", []))
    
    prev_bit_h = _extract_player_names(prev_snapshot.get("back_in_training_home", []))
    curr_bit_h = _extract_player_names(curr_snapshot.get("back_in_training_home", []))
    prev_bit_a = _extract_player_names(prev_snapshot.get("back_in_training_away", []))
    curr_bit_a = _extract_player_names(curr_snapshot.get("back_in_training_away", []))

    new_injuries = {
        "home": sorted(list(curr_inj_h - prev_inj_h)),
        "away": sorted(list(curr_inj_a - prev_inj_a))
    }
    new_doubtful = {
        "home": sorted(list(curr_dbt_h - prev_dbt_h)),
        "away": sorted(list(curr_dbt_a - prev_dbt_a))
    }
    new_suspensions = {
        "home": sorted(list(curr_susp_h - prev_susp_h)),
        "away": sorted(list(curr_susp_a - prev_susp_a))
    }
    newly_unavailable = {
        "home": sorted(list(curr_unavail_h - prev_unavail_h)),
        "away": sorted(list(curr_unavail_a - prev_unavail_a))
    }
    
    returned_available = {
        "home": sorted(list(prev_unavail_h - curr_unavail_h)),
        "away": sorted(list(prev_unavail_a - curr_unavail_a))
    }
    removed_from_injury = {
        "home": sorted(list(prev_inj_h - curr_inj_h)),
        "away": sorted(list(prev_inj_a - curr_inj_a))
    }
    newly_back_in_training = {
        "home": sorted(list(curr_bit_h - prev_bit_h)),
        "away": sorted(list(curr_bit_a - prev_bit_a))
    }
    
    # News delta
    material_news_added = []
    if curr_news:
        curr_items = curr_news.get("relevant_items", [])
        prev_headlines = set()
        if prev_news:
            for item in prev_news.get("relevant_items", []):
                prev_headlines.add(item.get("headline", "").strip().lower())
        for item in curr_items:
            hl = item.get("headline", "").strip()
            if hl.lower() not in prev_headlines:
                material_news_added.append(item)
                
    # Evaluate material change
    has_injury_change = bool(new_injuries["home"] or new_injuries["away"] or newly_unavailable["home"] or newly_unavailable["away"] or returned_available["home"] or returned_available["away"])
    has_xi_change = bool(xi_changes["home"]["added_starters"] or xi_changes["away"]["added_starters"])
    
    if has_injury_change or newly_confirmed or has_xi_change or len(material_news_added) >= 2:
        material_change = "YES"
    elif status_before == "NOT_AVAILABLE" and status_now == "NOT_AVAILABLE":
        material_change = "UNKNOWN"
    else:
        material_change = "NO"

    return {
        "previous_retrieved_at_utc": prev_utc,
        "current_retrieved_at_utc": curr_utc,
        "lineup_status_before": status_before,
        "lineup_status_now": status_now,
        "newly_confirmed_xi": newly_confirmed,
        "xi_changes": xi_changes,
        "new_injuries": new_injuries,
        "new_doubtful": new_doubtful,
        "new_suspensions": new_suspensions,
        "newly_unavailable": newly_unavailable,
        "returned_available": returned_available,
        "removed_from_injury_list": removed_from_injury,
        "newly_back_in_training": newly_back_in_training,
        "material_news_added": material_news_added,
        "material_change": material_change
    }
