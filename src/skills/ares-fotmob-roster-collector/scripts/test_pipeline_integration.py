#!/usr/bin/env python3
"""
Pipeline Integration Test for Ares FotMob Roster Collector Skill v1.1
Executes real pipeline integration for fixture 3013703 (Madrid Derby):
1. Prematch pipeline execution:
   -> Generates 00-fixture-binding.json, 01-fotmob-prematch-roster.json, 02-fotmob-prematch-news.json
   -> Integrates into existing LALIGA_GW07_ANTIGRAVITY_MARKET_AND_NEWS_v1 package
2. Final Refresh pipeline execution:
   -> Reuses existing binding (binding_reused = True, fotmob_rediscovery = 0)
   -> Fetches fresh roster & calculates deterministic roster delta
   -> Generates 03-fotmob-final-roster.json, 04-fotmob-roster-delta.json
   -> Integrates into existing ARES_W39_FINAL_REFRESH_20260920_1800 directory
"""

import sys
import os
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
SKILL_SCRIPTS = Path(__file__).resolve().parent

sys.path.insert(0, str(SKILL_SCRIPTS))

from fotmob_roster_collector import collect_fixture_roster
import fotmob_binding

def run_integration():
    fixture_id = "3013703"
    league = "LaLiga_GW07"
    home_team = "马德里竞技"
    away_team = "皇家马德里"
    kickoff_utc = "2026-09-20T14:15:00Z"

    prematch_package_dir = REPO_ROOT / "LALIGA_GW07_ANTIGRAVITY_MARKET_AND_NEWS_v1" / "parsed" / f"{fixture_id}_fotmob"
    final_refresh_package_dir = REPO_ROOT / "ARES_W39_FINAL_REFRESH_20260920_1800" / f"{fixture_id}_fotmob"

    print("==================================================")
    print("STEP 1: PREMATCH PIPELINE EXECUTION")
    print("==================================================")

    prematch_input = {
        "project_fixture_id": fixture_id,
        "league": league,
        "home_team": home_team,
        "away_team": away_team,
        "kickoff_utc": kickoff_utc,
        "mode": "PREMATCH",
        "force_fetch_post_kickoff": True,
        "output_dir": str(prematch_package_dir)
    }

    prematch_res = collect_fixture_roster(prematch_input)

    binding = prematch_res["fotmob_binding"]
    print(f"Prematch Collector Status: {prematch_res['collector_status']}")
    print(f"FotMob Match ID Bound:   {binding['fotmob_match_id']}")
    print(f"Canonical URL:           {binding['canonical_url']}")
    print(f"Parsed Match ID:         {prematch_res['parsed_page_match_id']}")
    print(f"Identity Assertion:      {prematch_res['identity_assertion']}")

    binding_file = prematch_package_dir / "00-fixture-binding.json"
    prematch_roster_file = prematch_package_dir / "01-fotmob-prematch-roster.json"
    prematch_news_file = prematch_package_dir / "02-fotmob-prematch-news.json"

    assert binding_file.exists(), f"Missing {binding_file}"
    assert prematch_roster_file.exists(), f"Missing {prematch_roster_file}"
    assert prematch_news_file.exists(), f"Missing {prematch_news_file}"
    assert prematch_res["identity_assertion"] == "PASS"

    print("\nPrematch files successfully produced in existing package:")
    print(f" - {binding_file}")
    print(f" - {prematch_roster_file}")
    print(f" - {prematch_news_file}")

    print("\n==================================================")
    print("STEP 2: FINAL REFRESH PIPELINE EXECUTION")
    print("==================================================")

    # Instrument fotmob_binding to verify zero schedule rediscovery
    original_fetch = fotmob_binding.fetch_fotmob_league_schedule
    rediscovery_calls = []

    def mocked_fetch(*args, **kwargs):
        rediscovery_calls.append(args)
        return original_fetch(*args, **kwargs)

    fotmob_binding.fetch_fotmob_league_schedule = mocked_fetch

    with open(binding_file, "r", encoding="utf-8") as rf:
        saved_binding = json.load(rf)

    final_input = {
        "project_fixture_id": fixture_id,
        "league": league,
        "home_team": home_team,
        "away_team": away_team,
        "kickoff_utc": kickoff_utc,
        "mode": "FINAL_REFRESH",
        "existing_binding": saved_binding,
        "previous_roster_snapshot_path": str(prematch_roster_file),
        "force_fetch_post_kickoff": True,
        "output_dir": str(final_refresh_package_dir)
    }

    final_res = collect_fixture_roster(final_input)

    # Restore original fetch
    fotmob_binding.fetch_fotmob_league_schedule = original_fetch

    final_binding = final_res["fotmob_binding"]
    binding_reused = final_binding.get("reused_binding", False)
    fotmob_rediscovery = len(rediscovery_calls)

    print(f"Final Refresh Status:    {final_res['collector_status']}")
    print(f"Binding Reused:          {binding_reused}")
    print(f"FotMob Rediscovery Calls:{fotmob_rediscovery}")
    print(f"Identity Assertion:      {final_res['identity_assertion']}")

    final_binding_file = final_refresh_package_dir / "00-fixture-binding.json"
    final_roster_file = final_refresh_package_dir / "03-fotmob-final-roster.json"
    roster_delta_file = final_refresh_package_dir / "04-fotmob-roster-delta.json"

    assert final_binding_file.exists(), f"Missing {final_binding_file}"
    assert final_roster_file.exists(), f"Missing {final_roster_file}"
    assert roster_delta_file.exists(), f"Missing {roster_delta_file}"
    assert binding_reused is True, "Expected binding_reused == True"
    assert fotmob_rediscovery == 0, f"Expected 0 rediscovery calls, got {fotmob_rediscovery}"

    print("\nFinal Refresh files successfully produced in existing package:")
    print(f" - {final_binding_file}")
    print(f" - {final_roster_file}")
    print(f" - {roster_delta_file}")

    print("\n==================================================")
    print("INTEGRATION PROOF SUMMARY")
    print("==================================================")
    print(f"Fixture:               {fixture_id} ({home_team} vs {away_team})")
    print(f"binding_reused:        {binding_reused}")
    print(f"fotmob_rediscovery:    {fotmob_rediscovery}")
    print(f"Prematch Roster:       {prematch_roster_file}")
    print(f"Prematch News:         {prematch_news_file}")
    print(f"Final Roster:          {final_roster_file}")
    print(f"Roster Delta:          {roster_delta_file}")
    print("ALL INTEGRATION ASSERTIONS PASSED.")

if __name__ == "__main__":
    run_integration()
