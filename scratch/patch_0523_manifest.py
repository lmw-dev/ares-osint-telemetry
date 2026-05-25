import json
from pathlib import Path

manifest_path = Path("/Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/Cold_Data_Lake/DATE-20260523-top5_dispatch_manifest.json")

with open(manifest_path, "r", encoding="utf-8") as f:
    manifest = json.load(f)

# Check if already patched
existing_ids = {m.get("understat_id") for m in manifest.get("matches", [])}
if "30217" in existing_ids:
    print("Match 30217 already exists in manifest.")
else:
    new_match = {
        "index": 13,
        "chinese": "Lazio vs Pisa",
        "english": "Lazio vs Pisa",
        "cn_match_id": None,
        "cn_match_id_source": "missing",
        "understat_id": "30217",
        "understat_date": "2026-05-23 18:45:00",
        "understat_gap_days": 0,
        "fbref_url": None,
        "fbref_date": None,
        "fbref_gap_days": None,
        "football_data_match_id": None,
        "football_data_date": None,
        "football_data_gap_days": None,
        "football_data_competition": "SA",
        "mapping_source": "understat",
        "league": "Serie_A",
        "manual_anchor_applied": False,
        "manual_anchor_mode": None,
        "manual_anchor_notes": None,
        "manual_anchor_source": None,
        "understat_prematch_id_policy": {
            "fixture_status": "upcoming",
            "understat_target_match_id_required": False,
            "target_match_id_required": False,
            "understat_match_id_status": "pending_until_postmatch",
            "blocker": False,
            "user_supplied_match_ids_role": "not_provided",
            "user_supplied_match_links": []
        },
        "match_basic": {
            "match": "Lazio vs Pisa",
            "league": "Serie_A",
            "kickoff_time": "2026-05-23 18:45:00",
            "round": None,
            "home_rank_points": {
                "rank": None,
                "points": None
            },
            "away_rank_points": {
                "rank": None,
                "points": None
            },
            "remaining_matches": {
                "home": None,
                "away": None
            },
            "table_context": {
                "home_objective": None,
                "away_objective": None
            }
        },
        "titan_prematch": None,
        "euro_odds": {},
        "asian_handicap": {},
        "total_goals": {},
        "match_context_flags": {
            "primary_motivation_type": "NORMAL",
            "motivation_context_flags": [],
            "manager_change_recent": False,
            "new_manager_sample_matches": 0,
            "survival_pressure_level": "none",
            "opponent_survival_pressure_high": False,
            "elite_away_flag": False,
            "favorite_deep_handicap": 0.0,
            "structural_crisis_context": False,
            "europe_sandwich": False,
            "title_race_pressure": False,
            "relegation_zone_rank": None,
            "future_fixture_gap_days": None,
            "expected_core_availability": {
                "home": "FULL",
                "away": "FULL"
            },
            "lineup_stability_precheck": {
                "home": "GREEN",
                "away": "GREEN"
            },
            "key_node_absence_risk": "LOW",
            "rotation_intensity": "UNKNOWN",
            "lineup_snapshot_status": "UNKNOWN"
        },
        "market_behavior": {
            "handicap_retreat": False,
            "handicap_deepen": False,
            "favorite_odds_compressed": False,
            "favorite_overprice_risk": False,
            "shallow_home_support": False,
            "market_retreat_against_favorite": False
        },
        "market_odds_history": [],
        "team_archive_context": {
            "home": {
                "archive_path": "/Users/liumingwei/vaults/AresVault/02_Team_Archives/1_Top_Five_Europe/ITA_Italy/Lazio.md",
                "archive_quality": "usable",
                "usable_level": "基础可用"
            },
            "away": {
                "archive_path": "/Users/liumingwei/vaults/AresVault/02_Team_Archives/1_Top_Five_Europe/ITA_Italy/Pisa.md",
                "archive_quality": "usable",
                "usable_level": "基础可用"
            }
        }
    }
    manifest["matches"].append(new_match)
    
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print("Successfully patched manifest with Lazio vs Pisa match.")
