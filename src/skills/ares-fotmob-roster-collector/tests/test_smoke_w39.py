#!/usr/bin/env python3
"""
Exact W39 Live Smoke Test Suite for Ares FotMob Roster Collector Skill v1.1:
Runs real live end-to-end collection for the 5 authoritative W39 fixtures:
1. 3013703 Atletico Madrid vs Real Madrid (LaLiga)
2. 3003889 Bournemouth vs Liverpool (EPL)
3. 3003895 Manchester City vs Sunderland (EPL)
4. 3018813 Leverkusen vs RB Leipzig (Bundesliga)
5. 2995349 Nice vs Lille (Ligue 1)

Proves:
- parsed_fotmob_match_id == bound_fotmob_match_id (identity assertion PASS)
- Post-kickoff smoke semantics: lifecycle = POST_KICKOFF_LOCKED, execution_usable = false
"""

import unittest
import sys
import tempfile
from pathlib import Path

SKILL_SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SKILL_SCRIPTS))

from fotmob_roster_collector import collect_fixture_roster

AUTHORITATIVE_W39_FIXTURES = [
    {
        "project_fixture_id": "3013703",
        "name": "Atletico Madrid vs Real Madrid",
        "league": "LaLiga_GW07",
        "home_team": "马德里竞技",
        "away_team": "皇家马德里",
        "kickoff_utc": "2026-09-20T14:15:00Z"
    },
    {
        "project_fixture_id": "3003889",
        "name": "Bournemouth vs Liverpool",
        "league": "EPL_GW05",
        "home_team": "伯恩茅斯",
        "away_team": "利物浦",
        "kickoff_utc": "2026-09-20T13:00:00Z"
    },
    {
        "project_fixture_id": "3003895",
        "name": "Manchester City vs Sunderland",
        "league": "EPL_GW05",
        "home_team": "曼城",
        "away_team": "桑德兰",
        "kickoff_utc": "2026-09-20T15:00:00Z"
    },
    {
        "project_fixture_id": "3018813",
        "name": "Leverkusen vs RB Leipzig",
        "league": "Bundesliga_GW04",
        "home_team": "勒沃库森",
        "away_team": "莱比锡红牛",
        "kickoff_utc": "2026-09-20T13:30:00Z"
    },
    {
        "project_fixture_id": "2995349",
        "name": "Nice vs Lille",
        "league": "Ligue1_GW05",
        "home_team": "尼斯",
        "away_team": "里尔",
        "kickoff_utc": "2026-09-20T15:00:00Z"
    }
]

class TestFotMobSmokeW39(unittest.TestCase):

    def test_live_smoke_5_authoritative_w39_fixtures(self):
        """Execute real end-to-end FotMob collection for the 5 authoritative W39 fixtures"""
        results_summary = []
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_base = Path(tmp_dir)

            for f in AUTHORITATIVE_W39_FIXTURES:
                fixture_dict = {
                    "project_fixture_id": f["project_fixture_id"],
                    "league": f["league"],
                    "home_team": f["home_team"],
                    "away_team": f["away_team"],
                    "kickoff_utc": f["kickoff_utc"],
                    "mode": "PREMATCH",
                    # Allow inspection of completed matches for smoke testing,
                    # while verifying that post-kickoff semantics mark execution_usable = false
                    "force_fetch_post_kickoff": True,
                    "output_dir": str(out_base / f["project_fixture_id"])
                }
                res = collect_fixture_roster(fixture_dict)

                binding = res.get("fotmob_binding", {})
                roster = res.get("roster_snapshot", {})

                b_status = binding.get("status")
                f_mid = binding.get("fotmob_match_id")
                c_url = binding.get("canonical_url", "")
                parsed_mid = res.get("parsed_page_match_id")
                parsed_home = res.get("parsed_page_home")
                parsed_away = res.get("parsed_page_away")
                parsed_ko = res.get("parsed_page_kickoff")
                identity_assert = res.get("identity_assertion")

                l_status = roster.get("lineup_status") if roster else "NOT_AVAILABLE"
                inj_h_cnt = len(roster.get("injured_home", [])) if roster else 0
                inj_a_cnt = len(roster.get("injured_away", [])) if roster else 0
                susp_h_cnt = len(roster.get("suspended_home", [])) if roster else 0
                susp_a_cnt = len(roster.get("suspended_away", [])) if roster else 0

                lifecycle = res.get("lifecycle")
                usable = res.get("execution_usable")
                retrieved_utc = binding.get("retrieved_at_utc")

                summary_item = {
                    "project_fixture_id": f["project_fixture_id"],
                    "fixture_name": f["name"],
                    "input_match": f"{f['home_team']} vs {f['away_team']}",
                    "input_kickoff": f["kickoff_utc"],
                    "resolved_fotmob_match_id": f_mid,
                    "canonical_url": c_url,
                    "parsed_page_match_id": parsed_mid,
                    "parsed_page_teams": f"{parsed_home} vs {parsed_away}",
                    "parsed_page_kickoff": parsed_ko,
                    "identity_assertion": identity_assert,
                    "lineup_status": l_status,
                    "injury_counts": f"H:{inj_h_cnt} / A:{inj_a_cnt}",
                    "suspension_counts": f"H:{susp_h_cnt} / A:{susp_a_cnt}",
                    "lifecycle": lifecycle,
                    "execution_usable": usable,
                    "retrieved_at_utc": retrieved_utc,
                    "binding_status": b_status
                }
                results_summary.append(summary_item)

                # Core Assertions
                self.assertIn(b_status, ["PASS", "CAUTION"], f"Binding failed for {f['name']}")
                self.assertIsNotNone(f_mid, f"FotMob Match ID missing for {f['name']}")
                self.assertNotEqual(str(f_mid), f["project_fixture_id"], "FotMob ID must NOT equal internal project ID")
                self.assertFalse(c_url.endswith(f"/match/{f['project_fixture_id']}"), "URL mechanically forged!")
                
                # Hard Identity Assertion
                self.assertEqual(identity_assert, "PASS", f"Identity assertion failed for {f['name']}")
                self.assertEqual(str(parsed_mid), str(f_mid), f"Parsed match ID mismatch for {f['name']}")

                # Post-Kickoff Semantics Assertion
                self.assertEqual(lifecycle, "POST_KICKOFF_LOCKED", f"Expected POST_KICKOFF_LOCKED for past match {f['name']}")
                self.assertFalse(usable, f"execution_usable must be False for post-kickoff match {f['name']}")

            print("\n================ EXACT W39 SMOKE TEST RESULTS ================")
            for s in results_summary:
                print(f"[{s['binding_status']}] {s['fixture_name']} (Proj ID: {s['project_fixture_id']})")
                print(f"   Input:        {s['input_match']} @ {s['input_kickoff']}")
                print(f"   FotMob Bound: ID {s['resolved_fotmob_match_id']} | URL {s['canonical_url']}")
                print(f"   Parsed Page:  ID {s['parsed_page_match_id']} | Teams: {s['parsed_page_teams']} | KO: {s['parsed_page_kickoff']}")
                print(f"   Assertion:    {s['identity_assertion']} (parsed == bound)")
                print(f"   Lineup:       {s['lineup_status']} | Inj: {s['injury_counts']} | Susp: {s['suspension_counts']}")
                print(f"   Semantics:    lifecycle={s['lifecycle']} | execution_usable={s['execution_usable']}")
                print(f"   Retrieved:    {s['retrieved_at_utc']}")
            print("==============================================================")

if __name__ == "__main__":
    unittest.main()
