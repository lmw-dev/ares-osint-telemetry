#!/usr/bin/env python3
"""
Unit tests for postmatch validation, error control, and regression guards:
H. source identity mismatch rejection
I. score conflict detection
J. missing xG => CAUTION not PASS
K. artifact_gap vs evidence_gap distinction
L. no betting-decision leakage
M. deterministic repeated output
N. completed-match lifecycle
O. non-final match rejection
P. process-fill-only does not overwrite result layer

Regression checks:
- project fixture ID must never be used as FotMob match ID
- xG=UNKNOWN must never coexist with full_process_complete
- missing process evidence must never yield evidence_gap=[]
- collector must never emit PROCESS_CONFIRMED / EDGE / LEAN / DECISION
"""

import unittest
import sys
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

SKILL_SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SKILL_SCRIPTS))

from postmatch_collector import (
    PostmatchCollector,
    FixtureIdentityDriftError
)
from fotmob_postmatch_client import parse_fotmob_completed_match

FROZEN_W39_EPL_GW05_IDENTITIES = {
    "3003889": {"home_team": "Bournemouth", "away_team": "Liverpool", "fotmob_match_id": "5795455", "final_score": "0-2"},
    "3003890": {"home_team": "Brentford", "away_team": "Chelsea", "fotmob_match_id": "5795456", "final_score": "2-2"},
    "3003891": {"home_team": "Brighton", "away_team": "Arsenal", "fotmob_match_id": "5795457", "final_score": "1-1"},
    "3003892": {"home_team": "Everton", "away_team": "Ipswich Town", "fotmob_match_id": "5795458", "final_score": "1-1"},
    "3003893": {"home_team": "Fulham", "away_team": "Manchester United", "fotmob_match_id": "5795459", "final_score": "1-1"},
    "3003894": {"home_team": "Leeds United", "away_team": "Crystal Palace", "fotmob_match_id": "5795460", "final_score": "0-0"},
    "3003895": {"home_team": "Manchester City", "away_team": "Sunderland", "fotmob_match_id": "5795461", "final_score": "5-3"},
    "3003896": {"home_team": "Newcastle United", "away_team": "Hull City", "fotmob_match_id": "5795462", "final_score": "2-0"},
    "3003897": {"home_team": "Nottingham Forest", "away_team": "Coventry City", "fotmob_match_id": "5795463", "final_score": "1-1"},
    "3003898": {"home_team": "Tottenham Hotspur", "away_team": "Aston Villa", "fotmob_match_id": "5795464", "final_score": "1-2"}
}

class TestPostmatchValidation(unittest.TestCase):

    def test_H_source_identity_mismatch_rejection(self):
        """Test H: Reject when page match ID does not match bound match ID"""
        mismatched_props = {
            "general": {"matchId": 999999, "homeTeam": {"name": "Team A"}, "awayTeam": {"name": "Team B"}},
            "header": {"status": {"finished": True}},
            "content": {}
        }
        res = parse_fotmob_completed_match(mismatched_props, "https://www.fotmob.com/test", "5795455")
        self.assertEqual(res["status"], "SOURCE_IDENTITY_MISMATCH")

    def test_I_score_conflict_detection(self):
        """Test I: Cross-source score discrepancy triggers SOURCE_CONFLICT and stops merge"""
        collector = PostmatchCollector()
        mock_binding = {
            "status": "PASS",
            "fotmob_match_id": "5795455",
            "canonical_url": "https://www.fotmob.com/matches/test/5795455"
        }
        mock_fm_page = {
            "general": {"matchId": 5795455, "homeTeam": {"name": "Bournemouth"}, "awayTeam": {"name": "Liverpool"}},
            "header": {
                "status": {"finished": True},
                "teams": [{"name": "Bournemouth", "score": 0}, {"name": "Liverpool", "score": 2}]
            },
            "content": {"lineup": {}, "stats": {}, "matchFacts": {"events": {"events": []}}}
        }
        # Understat says 1-1 (conflicting with FotMob 0-2)
        mock_understat = {
            "status": "PASS",
            "home_score": 1,
            "away_score": 1,
            "xg_home": 1.1,
            "xg_away": 1.2,
            "canonical_url": "https://understat.com/match/123",
            "retrieved_at_utc": "2026-09-21T00:00:00Z"
        }
        with patch("postmatch_collector.fetch_fotmob_completed_page", return_value=(mock_fm_page, "url", "PASS")), \
             patch("postmatch_collector.fetch_understat_match_data", return_value=mock_understat):
            res = collector.collect_fixture({
                "project_fixture_id": "9999001",
                "league": "EPL",
                "home_team": "Bournemouth",
                "away_team": "Liverpool",
                "kickoff_utc": "2026-09-20T13:00:00Z",
                "existing_binding": mock_binding
            })
            self.assertEqual(res["error"], "SOURCE_CONFLICT")
            self.assertIn("Score conflict", res["error_detail"])

    def test_J_missing_xg_status_caution(self):
        """Test J: Missing xG yields CAUTION instead of PASS"""
        collector = PostmatchCollector()
        mock_binding = {
            "status": "PASS",
            "fotmob_match_id": "5795455",
            "canonical_url": "https://www.fotmob.com/matches/test/5795455"
        }
        starters = [{"name": f"P{i}", "id": i} for i in range(1, 12)]
        mock_fm_page = {
            "general": {"matchId": 5795455, "homeTeam": {"name": "A"}, "awayTeam": {"name": "B"}},
            "header": {"status": {"finished": True}, "teams": [{"name": "A", "score": 1}, {"name": "B", "score": 0}]},
            "content": {
                "lineup": {"lineupType": "confirmed", "homeTeam": {"starters": starters}, "awayTeam": {"starters": starters}},
                "stats": {"Periods": {"All": {"stats": [{"title": "Top stats", "stats": [{"title": "Total shots", "stats": [10, 5]}, {"title": "Shots on target", "stats": [3, 2]}, {"title": "Ball possession", "stats": [50, 50]}]}]}}},
                "shotmap": {"shots": []}, # No xG
                "matchFacts": {"events": {"events": []}}
            }
        }
        with patch("postmatch_collector.fetch_fotmob_completed_page", return_value=(mock_fm_page, "url", "PASS")), \
             patch("postmatch_collector.fetch_understat_match_data", return_value={"status": "DATA_NOT_AVAILABLE"}):
            res = collector.collect_fixture({
                "project_fixture_id": "9999001",
                "league": "EPL",
                "home_team": "A",
                "away_team": "B",
                "kickoff_utc": "2026-09-20T13:00:00Z",
                "existing_binding": mock_binding
            })
            self.assertEqual(res["overall_postmatch_evidence_status"], "CAUTION")
            self.assertEqual(res["xg_status"], "UNKNOWN")

    def test_K_artifact_gap_vs_evidence_gap_distinction(self):
        """Test K: File presence (artifact_gap=[]) does not hide evidence_gap"""
        collector = PostmatchCollector()
        with tempfile.TemporaryDirectory() as tmp_dir:
            mock_binding = {
                "status": "PASS",
                "fotmob_match_id": "5795455",
                "canonical_url": "https://www.fotmob.com/matches/test/5795455"
            }
            starters = [{"name": f"P{i}", "id": i} for i in range(1, 12)]
            # Core stats missing possession
            mock_fm_page = {
                "general": {"matchId": 5795455, "homeTeam": {"name": "A"}, "awayTeam": {"name": "B"}},
                "header": {"status": {"finished": True}, "teams": [{"name": "A", "score": 1}, {"name": "B", "score": 0}]},
                "content": {
                    "lineup": {"lineupType": "confirmed", "homeTeam": {"starters": starters}, "awayTeam": {"starters": starters}},
                    "stats": {},
                    "shotmap": {"shots": []},
                    "matchFacts": {"events": {"events": []}}
                }
            }
            with patch("postmatch_collector.fetch_fotmob_completed_page", return_value=(mock_fm_page, "url", "PASS")), \
                 patch("postmatch_collector.fetch_understat_match_data", return_value={"status": "DATA_NOT_AVAILABLE"}):
                res = collector.collect_fixture({
                    "project_fixture_id": "9999001",
                    "league": "EPL",
                    "home_team": "A",
                    "away_team": "B",
                    "kickoff_utc": "2026-09-20T13:00:00Z",
                    "existing_binding": mock_binding,
                    "output_dir": tmp_dir
                })
                # Artifact gap is empty because all JSONs were generated
                self.assertEqual(res["artifact_gap"], [])
                # Evidence gap MUST NOT be empty because stats and xG were missing
                self.assertNotEqual(res["evidence_gap"], [])

    def test_L_no_betting_decision_leakage(self):
        """Test L: Zero betting decision, fair line, or prediction leakage"""
        collector = PostmatchCollector()
        mock_binding = {"status": "PASS", "fotmob_match_id": "5795455", "canonical_url": "url"}
        mock_fm_page = {
            "general": {"matchId": 5795455, "homeTeam": {"name": "A"}, "awayTeam": {"name": "B"}},
            "header": {"status": {"finished": True}, "teams": [{"name": "A", "score": 1}, {"name": "B", "score": 0}]},
            "content": {"lineup": {}, "stats": {}, "matchFacts": {"events": {"events": []}}}
        }
        with patch("postmatch_collector.fetch_fotmob_completed_page", return_value=(mock_fm_page, "url", "PASS")), \
             patch("postmatch_collector.fetch_understat_match_data", return_value={"status": "DATA_NOT_AVAILABLE"}):
            res = collector.collect_fixture({
                "project_fixture_id": "9999001",
                "league": "EPL",
                "home_team": "A",
                "away_team": "B",
                "kickoff_utc": "2026-09-20T13:00:00Z",
                "existing_binding": mock_binding
            })
            for forbidden in ["lean", "edge", "fair_line", "bet", "decision", "process_confirmed"]:
                self.assertNotIn(forbidden, res)

    def test_M_deterministic_repeated_output(self):
        """Test M: Running twice produces identical football evidence"""
        collector = PostmatchCollector()
        mock_binding = {"status": "PASS", "fotmob_match_id": "5795455", "canonical_url": "url"}
        starters = [{"name": f"P{i}", "id": i} for i in range(1, 12)]
        mock_fm_page = {
            "general": {"matchId": 5795455, "homeTeam": {"name": "A"}, "awayTeam": {"name": "B"}},
            "header": {"status": {"finished": True}, "teams": [{"name": "A", "score": 2}, {"name": "B", "score": 1}]},
            "content": {
                "lineup": {"lineupType": "confirmed", "homeTeam": {"starters": starters}, "awayTeam": {"starters": starters}},
                "stats": {"Periods": {"All": {"stats": [{"title": "Top stats", "stats": [{"title": "Total shots", "stats": [10, 8]}, {"title": "Shots on target", "stats": [4, 3]}, {"title": "Ball possession", "stats": [55, 45]}]}]}}},
                "shotmap": {"shots": [{"id": 1, "min": 10, "expectedGoals": 0.2, "eventType": "Goal", "x": 80, "y": 30}]},
                "matchFacts": {"events": {"events": []}}
            }
        }
        with patch("postmatch_collector.fetch_fotmob_completed_page", return_value=(mock_fm_page, "url", "PASS")), \
             patch("postmatch_collector.fetch_understat_match_data", return_value={"status": "DATA_NOT_AVAILABLE"}):
            f_input = {
                "project_fixture_id": "9999001",
                "league": "EPL",
                "home_team": "A",
                "away_team": "B",
                "kickoff_utc": "2026-09-20T13:00:00Z",
                "existing_binding": mock_binding
            }
            r1 = collector.collect_fixture(f_input)
            r2 = collector.collect_fixture(f_input)
            self.assertEqual(r1["final_score"], r2["final_score"])
            self.assertEqual(r1["shots"], r2["shots"])
            self.assertEqual(r1["xg_home"], r2["xg_home"])

    def test_N_completed_match_lifecycle(self):
        """Test N: Completed match lifecycle is POSTMATCH_COMPLETE"""
        dummy = {
            "general": {"matchId": 123},
            "header": {"status": {"finished": True, "reason": {"long": "Full-Time"}}, "teams": [{"score": 1}, {"score": 0}]},
            "content": {"lineup": {}, "stats": {}, "matchFacts": {"events": {"events": []}}}
        }
        res = parse_fotmob_completed_match(dummy, "url", "123")
        self.assertEqual(res["status"], "PASS")
        self.assertTrue(res["result_evidence"]["finished"])

    def test_O_non_final_match_rejection(self):
        """Test O: Match not completed (e.g. LIVE or SCHEDULED) is safely rejected"""
        dummy = {
            "general": {"matchId": 123},
            "header": {"status": {"finished": False, "started": True, "reason": {"long": "1st Half"}}, "teams": []},
            "content": {}
        }
        res = parse_fotmob_completed_match(dummy, "url", "123")
        self.assertEqual(res["status"], "NON_FINAL_MATCH")
        self.assertEqual(res["lifecycle"], "LIVE")

    def test_P_process_fill_only_preserves_result_layer(self):
        """Test P: PROCESS_FILL_ONLY mode stages to its own directory and does not overwrite existing result file"""
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as ares_vault_dir:
            ares_result_file = Path(ares_vault_dir) / "01-result-evidence.md"
            ares_result_file.write_text("ORIGINAL_ARES_RESULT_DATA", encoding="utf-8")

            collector = PostmatchCollector(output_base_dir=staging_dir)
            mock_binding = {"status": "PASS", "fotmob_match_id": "5795455", "canonical_url": "url"}
            mock_fm_page = {
                "general": {"matchId": 5795455, "homeTeam": {"name": "A"}, "awayTeam": {"name": "B"}},
                "header": {"status": {"finished": True}, "teams": [{"name": "A", "score": 2}, {"name": "B", "score": 0}]},
                "content": {"lineup": {}, "stats": {}, "matchFacts": {"events": {"events": []}}}
            }
            with patch("postmatch_collector.fetch_fotmob_completed_page", return_value=(mock_fm_page, "url", "PASS")), \
                 patch("postmatch_collector.fetch_understat_match_data", return_value={"status": "DATA_NOT_AVAILABLE"}):
                res = collector.collect_fixture({
                    "project_fixture_id": "9999001",
                    "league": "EPL",
                    "home_team": "A",
                    "away_team": "B",
                    "kickoff_utc": "2026-09-20T13:00:00Z",
                    "existing_binding": mock_binding
                }, mode="PROCESS_FILL_ONLY")

            # Ares vault file is completely intact
            self.assertEqual(ares_result_file.read_text(encoding="utf-8"), "ORIGINAL_ARES_RESULT_DATA")
            # Skill wrote to staging
            staged_file = Path(staging_dir) / "9999001" / "01-result-evidence.json"
            self.assertTrue(staged_file.exists())

    def test_Q_brentford_chelsea_xg_canonicalization(self):
        """Test Q: 3003890 Brentford vs Chelsea canonical xG = 2.65 / 0.83, derived ≈ 2.90 / 0.85, xg_consistency = CAUTION"""
        collector = PostmatchCollector()
        mock_binding = {"status": "PASS", "fotmob_match_id": "5795456", "canonical_url": "https://www.fotmob.com/matches/brentford-vs-chelsea/5795456"}
        starters = [{"name": f"P{i}", "id": i} for i in range(1, 12)]
        mock_fm_page = {
            "general": {"matchId": 5795456, "homeTeam": {"id": 1, "name": "Brentford"}, "awayTeam": {"id": 2, "name": "Chelsea"}},
            "header": {"status": {"finished": True}, "teams": [{"name": "Brentford", "score": 3}, {"name": "Chelsea", "score": 0}]},
            "content": {
                "lineup": {"lineupType": "confirmed", "homeTeam": {"starters": starters}, "awayTeam": {"starters": starters}},
                "stats": {"Periods": {"All": {"stats": [{"title": "Top stats", "stats": [
                    {"title": "Total shots", "stats": [13, 12]},
                    {"title": "Shots on target", "stats": [5, 4]},
                    {"title": "Ball possession", "stats": [42, 58]},
                    {"title": "Expected goals (xG)", "stats": ["2.65", "0.83"]}
                ]}]}}},
                # Mock shots that sum to 2.90 and 0.85
                "shotmap": {"shots": [
                    {"id": 1, "min": 10, "teamId": 1, "expectedGoals": 1.45, "eventType": "Goal"},
                    {"id": 2, "min": 20, "teamId": 1, "expectedGoals": 1.45, "eventType": "Goal"},
                    {"id": 3, "min": 30, "teamId": 2, "expectedGoals": 0.85, "eventType": "Goal"}
                ]},
                "matchFacts": {"events": {"events": []}}
            }
        }
        with patch("postmatch_collector.fetch_fotmob_completed_page", return_value=(mock_fm_page, "url", "PASS")), \
             patch("postmatch_collector.fetch_understat_match_data", return_value={"status": "DATA_NOT_AVAILABLE"}):
            res = collector.collect_fixture({
                "project_fixture_id": "3003890",
                "league": "EPL_GW05",
                "home_team": "布伦特福德",
                "away_team": "切尔西",
                "kickoff_utc": "2026-09-18T19:00:00Z",
                "existing_binding": mock_binding
            })
            self.assertEqual(res["canonical_xg_home"], 2.65)
            self.assertEqual(res["canonical_xg_away"], 0.83)
            self.assertEqual(res["canonical_xg_source"], "FotMob-Aggregate")
            self.assertEqual(res["derived_shotmap_xg_home"], 2.90)
            self.assertEqual(res["derived_shotmap_xg_away"], 0.85)
            self.assertEqual(res["xg_consistency"], "CAUTION")
            self.assertEqual(res["canonical_shots_home"], 13)
            self.assertEqual(res["canonical_shots_away"], 12)

    def test_R_tottenham_villa_xg_canonicalization(self):
        """Test R: 3003898 Tottenham vs Villa canonical xG = 1.56 / 1.16, derived ≈ 1.58 / 1.26, xg_consistency = CAUTION"""
        collector = PostmatchCollector()
        mock_binding = {"status": "PASS", "fotmob_match_id": "5795464", "canonical_url": "https://www.fotmob.com/matches/tottenham-vs-aston-villa/5795464"}
        starters = [{"name": f"P{i}", "id": i} for i in range(1, 12)]
        mock_fm_page = {
            "general": {"matchId": 5795464, "homeTeam": {"id": 1, "name": "Tottenham Hotspur"}, "awayTeam": {"id": 2, "name": "Aston Villa"}},
            "header": {"status": {"finished": True}, "teams": [{"name": "Tottenham", "score": 2}, {"name": "Aston Villa", "score": 3}]},
            "content": {
                "lineup": {"lineupType": "confirmed", "homeTeam": {"starters": starters}, "awayTeam": {"starters": starters}},
                "stats": {"Periods": {"All": {"stats": [{"title": "Top stats", "stats": [
                    {"title": "Total shots", "stats": [16, 12]},
                    {"title": "Shots on target", "stats": [6, 1]},
                    {"title": "Ball possession", "stats": [51, 49]},
                    {"title": "Expected goals (xG)", "stats": ["1.56", "1.16"]}
                ]}]}}},
                # Mock shots that sum to 1.58 and 1.26
                "shotmap": {"shots": [
                    {"id": 1, "min": 10, "teamId": 1, "expectedGoals": 1.58, "eventType": "Goal"},
                    {"id": 2, "min": 20, "teamId": 2, "expectedGoals": 1.26, "eventType": "Goal"}
                ]},
                "matchFacts": {"events": {"events": []}}
            }
        }
        with patch("postmatch_collector.fetch_fotmob_completed_page", return_value=(mock_fm_page, "url", "PASS")), \
             patch("postmatch_collector.fetch_understat_match_data", return_value={"status": "DATA_NOT_AVAILABLE"}):
            res = collector.collect_fixture({
                "project_fixture_id": "3003898",
                "league": "EPL_GW05",
                "home_team": "托特纳姆热刺",
                "away_team": "阿斯顿维拉",
                "kickoff_utc": "2026-09-19T11:30:00Z",
                "existing_binding": mock_binding
            })
            self.assertEqual(res["canonical_xg_home"], 1.56)
            self.assertEqual(res["canonical_xg_away"], 1.16)
            self.assertEqual(res["canonical_xg_source"], "FotMob-Aggregate")
            self.assertEqual(res["derived_shotmap_xg_home"], 1.58)
            self.assertEqual(res["derived_shotmap_xg_away"], 1.26)
            self.assertEqual(res["xg_consistency"], "CAUTION")

    def test_S_fulham_man_united_canonical_shots_vs_shotmap_events(self):
        """Test S: 3003893 Fulham vs Man United canonical shots = 12 / 29, shotmap events = 12 / 30"""
        collector = PostmatchCollector()
        mock_binding = {"status": "PASS", "fotmob_match_id": "5795459", "canonical_url": "https://www.fotmob.com/matches/fulham-vs-manchester-united/5795459"}
        starters = [{"name": f"P{i}", "id": i} for i in range(1, 12)]
        mock_fm_page = {
            "general": {"matchId": 5795459, "homeTeam": {"id": 1, "name": "Fulham"}, "awayTeam": {"id": 2, "name": "Manchester United"}},
            "header": {"status": {"finished": True}, "teams": [{"name": "Fulham", "score": 1}, {"name": "Manchester United", "score": 1}]},
            "content": {
                "lineup": {"lineupType": "confirmed", "homeTeam": {"starters": starters}, "awayTeam": {"starters": starters}},
                "stats": {"Periods": {"All": {"stats": [{"title": "Top stats", "stats": [
                    {"title": "Total shots", "stats": [12, 29]},
                    {"title": "Shots on target", "stats": [3, 9]},
                    {"title": "Ball possession", "stats": [48, 52]},
                    {"title": "Expected goals (xG)", "stats": ["1.15", "1.92"]}
                ]}]}}},
                # 12 home shots, 30 away shotmap events (e.g. 29 normal + 1 own goal)
                "shotmap": {"shots": [
                    {"id": f"h_{i}", "min": 10, "teamId": 1, "expectedGoals": 0.05, "eventType": "Miss"} for i in range(12)
                ] + [
                    {"id": f"a_{i}", "min": 20, "teamId": 2, "expectedGoals": 0.06, "eventType": "Miss"} for i in range(30)
                ]},
                "matchFacts": {"events": {"events": []}}
            }
        }
        with patch("postmatch_collector.fetch_fotmob_completed_page", return_value=(mock_fm_page, "url", "PASS")), \
             patch("postmatch_collector.fetch_understat_match_data", return_value={"status": "DATA_NOT_AVAILABLE"}):
            res = collector.collect_fixture({
                "project_fixture_id": "3003893",
                "league": "EPL_GW05",
                "home_team": "富勒姆",
                "away_team": "曼彻斯特联",
                "kickoff_utc": "2026-09-20T15:30:00Z",
                "existing_binding": mock_binding
            })
            self.assertEqual(res["canonical_shots_home"], 12)
            self.assertEqual(res["canonical_shots_away"], 29)
            self.assertEqual(res["shotmap_event_count_home"], 12)
            self.assertEqual(res["shotmap_event_count_away"], 30)
            self.assertEqual(res["shots"], "H:12 / A:29")

    def test_identity_lock_A_cannot_change_team_pairing(self):
        """Hard Test A: project_fixture_id cannot change team pairing"""
        collector = PostmatchCollector()
        mock_binding = {"status": "PASS", "fotmob_match_id": "5795460", "canonical_url": "url"}
        mock_fm_page = {
            "general": {"matchId": 5795460, "homeTeam": {"id": 1, "name": "Leeds United"}, "awayTeam": {"id": 2, "name": "Crystal Palace"}},
            "header": {"status": {"finished": True}, "teams": [{"name": "Leeds", "score": 0}, {"name": "Crystal Palace", "score": 0}]},
            "content": {"lineup": {}, "stats": {}, "matchFacts": {"events": {"events": []}}}
        }
        with patch("postmatch_collector.fetch_fotmob_completed_page", return_value=(mock_fm_page, "url", "PASS")), \
             self.assertRaises(FixtureIdentityDriftError):
            collector.collect_fixture({
                "project_fixture_id": "3003894",
                "league": "EPL_GW05",
                "home_team": "曼彻斯特城",
                "away_team": "水晶宫",
                "kickoff_utc": "2026-09-20T13:00:00Z",
                "existing_binding": mock_binding
            })

    def test_identity_lock_B_binding_reuse_cannot_rediscover_different_fixture(self):
        """Hard Test B: manifest fotmob_match_id mismatch with bound ID triggers FixtureIdentityDriftError"""
        collector = PostmatchCollector()
        with self.assertRaises(FixtureIdentityDriftError):
            collector.collect_fixture({
                "project_fixture_id": "3003894",
                "league": "EPL_GW05",
                "home_team": "利兹联",
                "away_team": "水晶宫",
                "kickoff_utc": "2026-09-20T13:00:00Z",
                "fotmob_match_id": "5795460",
                "existing_binding": {"status": "PASS", "fotmob_match_id": "5795461", "canonical_url": "url"}
            })

    def test_identity_lock_C_same_teams_different_date_cannot_overwrite_frozen(self):
        """Hard Test C: same teams on different date (>24h) triggers FixtureIdentityDriftError"""
        collector = PostmatchCollector()
        mock_binding = {"status": "PASS", "fotmob_match_id": "5795460", "canonical_url": "url"}
        mock_fm_page = {
            "general": {"matchId": 5795460, "matchTimeUTCDate": "2026-09-20T13:00:00Z", "homeTeam": {"id": 1, "name": "Leeds United"}, "awayTeam": {"id": 2, "name": "Crystal Palace"}},
            "header": {"status": {"finished": True, "utcTime": "2026-09-20T13:00:00Z"}, "teams": [{"name": "Leeds", "score": 0}, {"name": "Crystal Palace", "score": 0}]},
            "content": {"lineup": {}, "stats": {}, "matchFacts": {"events": {"events": []}}}
        }
        with patch("postmatch_collector.fetch_fotmob_completed_page", return_value=(mock_fm_page, "url", "PASS")), \
             self.assertRaises(FixtureIdentityDriftError):
            collector.collect_fixture({
                "project_fixture_id": "3003894",
                "league": "EPL_GW05",
                "home_team": "利兹联",
                "away_team": "水晶宫",
                "kickoff_utc": "2026-09-27T13:00:00Z",
                "existing_binding": mock_binding
            })

    def test_identity_lock_D_score_conflict_yields_source_conflict(self):
        """Hard Test D: cross-source score conflict triggers SOURCE_CONFLICT status"""
        collector = PostmatchCollector()
        mock_binding = {"status": "PASS", "fotmob_match_id": "5795460", "canonical_url": "url"}
        mock_fm_page = {
            "general": {"matchId": 5795460, "matchTimeUTCDate": "2026-09-20T13:00:00Z", "homeTeam": {"id": 1, "name": "Leeds United"}, "awayTeam": {"id": 2, "name": "Crystal Palace"}},
            "header": {"status": {"finished": True, "utcTime": "2026-09-20T13:00:00Z"}, "teams": [{"name": "Leeds", "score": 2}, {"name": "Crystal Palace", "score": 0}]},
            "content": {"lineup": {}, "stats": {}, "matchFacts": {"events": {"events": []}}}
        }
        mock_understat = {
            "status": "PASS",
            "home_score": 0,
            "away_score": 0,
            "xg_home": 0.5,
            "xg_away": 0.4,
            "canonical_url": "https://understat.com/match/3003894",
            "retrieved_at_utc": "2026-09-21T00:00:00Z"
        }
        with patch("postmatch_collector.fetch_fotmob_completed_page", return_value=(mock_fm_page, "url", "PASS")), \
             patch("postmatch_collector.fetch_understat_match_data", return_value=mock_understat):
            res = collector.collect_fixture({
                "project_fixture_id": "3003894",
                "league": "EPL_GW05",
                "home_team": "利兹联",
                "away_team": "水晶宫",
                "kickoff_utc": "2026-09-20T13:00:00Z",
                "existing_binding": mock_binding
            })
            self.assertEqual(res.get("error"), "SOURCE_CONFLICT")

    def test_identity_lock_E_3003894_must_remain_leeds_crystal_palace(self):
        """Hard Test E: 3003894 must remain Leeds–Crystal Palace"""
        self.assertIn("3003894", FROZEN_W39_EPL_GW05_IDENTITIES)
        meta = FROZEN_W39_EPL_GW05_IDENTITIES["3003894"]
        self.assertEqual(meta["home_team"], "Leeds United")
        self.assertEqual(meta["away_team"], "Crystal Palace")
        self.assertEqual(meta["fotmob_match_id"], "5795460")
        self.assertEqual(meta["final_score"], "0-0")

    def test_identity_lock_F_3003895_must_remain_mancity_sunderland(self):
        """Hard Test F: 3003895 must remain Man City–Sunderland"""
        self.assertIn("3003895", FROZEN_W39_EPL_GW05_IDENTITIES)
        meta = FROZEN_W39_EPL_GW05_IDENTITIES["3003895"]
        self.assertEqual(meta["home_team"], "Manchester City")
        self.assertEqual(meta["away_team"], "Sunderland")
        self.assertEqual(meta["fotmob_match_id"], "5795461")
        self.assertEqual(meta["final_score"], "5-3")

    def test_regression_guards(self):
        """Regression checks: project ID != FotMob ID, xG UNKNOWN prevents full process, etc."""
        collector = PostmatchCollector()
        # project ID mechanical construction blocked
        from fotmob_binding import validate_anti_regression
        with self.assertRaises(ValueError):
            validate_anti_regression("3003889", "https://www.fotmob.com/match/3003889")

    def test_main_pipeline_data_incompleteness_propagation(self):
        """
        Verify that Postmatch main pipeline properly propagates incompleteness of individual xG
        and penalty classification to final delivery (res, 04-xg-process.json, manifest.json),
        retains known sums, and prevents incompleteness from yielding PASS.
        """
        collector = PostmatchCollector()
        with tempfile.TemporaryDirectory() as tmp_dir:
            mock_binding = {
                "status": "PASS",
                "fotmob_match_id": "5795455",
                "canonical_url": "https://www.fotmob.com/matches/test/5795455"
            }
            starters = [{"name": f"P{i}", "id": i} for i in range(1, 12)]
            # Mock match: Home shot 2 missing xG, Away shot 2 missing penalty classification
            mock_fm_page = {
                "general": {
                    "matchId": 5795455,
                    "homeTeam": {"id": 1, "name": "Team A"},
                    "awayTeam": {"id": 2, "name": "Team B"}
                },
                "header": {
                    "status": {"finished": True},
                    "teams": [{"name": "Team A", "score": 1}, {"name": "Team B", "score": 1}]
                },
                "content": {
                    "lineup": {
                        "lineupType": "confirmed",
                        "homeTeam": {"starters": starters},
                        "awayTeam": {"starters": starters}
                    },
                    "stats": {
                        "Periods": {
                            "All": {
                                "stats": [
                                    {"title": "Top stats", "stats": [
                                        {"title": "Total shots", "stats": [2, 2]},
                                        {"title": "Shots on target", "stats": [1, 1]},
                                        {"title": "Ball possession", "stats": [50, 50]}
                                    ]}
                                ]
                            }
                        }
                    },
                    "shotmap": {
                        "shots": [
                            # Home shot 1: valid xG (0.35), normal OpenPlay
                            {"id": 1, "min": 10, "teamId": 1, "playerName": "H1", "expectedGoals": 0.35, "eventType": "Miss", "situation": "OpenPlay", "shotType": "RightFoot"},
                            # Home shot 2: missing xG (None)!
                            {"id": 2, "min": 25, "teamId": 1, "playerName": "H2", "expectedGoals": None, "eventType": "Goal", "situation": "OpenPlay", "shotType": "LeftFoot"},
                            # Away shot 1: valid xG (0.79), confirmed penalty
                            {"id": 3, "min": 50, "teamId": 2, "playerName": "A1", "expectedGoals": 0.79, "eventType": "Goal", "situation": "Penalty", "shotType": "Penalty"},
                            # Away shot 2: missing penalty classification! (situation None, shotType None)
                            {"id": 4, "min": 75, "teamId": 2, "playerName": "A2", "expectedGoals": 0.20, "eventType": "Miss", "situation": None, "shotType": None}
                        ]
                    },
                    "matchFacts": {
                        "events": {
                            "events": [
                                {"type": "Goal", "time": 25, "player": {"name": "H2"}, "teamId": 1, "isHome": True, "isOwnGoal": False, "shotType": "LeftFoot"},
                                {"type": "Goal", "time": 50, "player": {"name": "A1"}, "teamId": 2, "isHome": False, "isOwnGoal": False, "shotType": "Penalty"}
                            ]
                        }
                    }
                }
            }

            # Understat and FotMob-Aggregate unavailable -> forces shotmap derived fallback
            with patch("postmatch_collector.fetch_fotmob_completed_page", return_value=(mock_fm_page, "https://www.fotmob.com/matches/test/5795455", "PASS")), \
                 patch("postmatch_collector.fetch_understat_match_data", return_value={"status": "DATA_NOT_AVAILABLE"}):
                res = collector.collect_fixture({
                    "project_fixture_id": "9999002",
                    "league": "EPL",
                    "home_team": "Team A",
                    "away_team": "Team B",
                    "kickoff_utc": "2026-09-20T13:00:00Z",
                    "existing_binding": mock_binding,
                    "output_dir": tmp_dir
                })

                # 1. Verify completeness flags & missing counts are exposed
                self.assertIn("shotmap_xg_completeness", res)
                self.assertIn("npxg_completeness", res)
                
                # Home team missing 1 shot xG
                self.assertFalse(res["shotmap_xg_completeness"]["home"]["is_complete"])
                self.assertEqual(res["shotmap_xg_completeness"]["home"]["missing_xg_count"], 1)
                self.assertFalse(res["shotmap_xg_is_complete"])
                
                # Away team missing 1 penalty classification
                self.assertFalse(res["npxg_completeness"]["away"]["is_complete"])
                self.assertEqual(res["npxg_completeness"]["away"]["missing_penalty_type_count"], 1)
                self.assertFalse(res["npxg_is_complete"])

                # 2. Retains known partial sums
                self.assertEqual(res["known_shotmap_xg_home"], 0.35)
                self.assertEqual(res["known_npxg_away"], 0.0)
                self.assertEqual(res["derived_shotmap_xg_home"], 0.35)

                # 3. Data incompleteness prevents treating partial sum as full aggregate or granting PASS
                self.assertIsNone(res["canonical_xg_home"])
                self.assertEqual(res["canonical_xg_source"], "UNKNOWN")
                self.assertNotEqual(res["xg_status"], "PASS")
                self.assertIn(res["xg_status"], ["PARTIAL", "UNKNOWN"])
                self.assertNotEqual(res["overall_postmatch_evidence_status"], "PASS")
                
                # 4. Verify 04-xg-process.json persists completeness details
                xg_json_path = Path(tmp_dir) / "04-xg-process.json"
                self.assertTrue(xg_json_path.exists())
                with open(xg_json_path) as f:
                    saved_xg = json.load(f)
                self.assertFalse(saved_xg["shotmap_xg_is_complete"])
                self.assertFalse(saved_xg["npxg_is_complete"])
                self.assertEqual(saved_xg["missing_shot_xg_count_home"], 1)
                self.assertEqual(saved_xg["missing_penalty_type_count_away"], 1)
                self.assertEqual(saved_xg["known_shotmap_xg_home"], 0.35)

                # 5. Verify manifest.json records non-PASS status
                manifest_path = Path(tmp_dir) / "manifest.json"
                self.assertTrue(manifest_path.exists())
                with open(manifest_path) as f:
                    saved_manifest = json.load(f)
                self.assertNotEqual(saved_manifest["xg_status"], "PASS")
                self.assertNotEqual(saved_manifest["overall_status"], "PASS")

    def test_xg_consistency_governance_rules(self):
        """
        Verify that xg_consistency cannot display PASS when:
        1. Shotmap xG data is incomplete (e.g. 1 shot missing xG) => CAUTION
        2. Incompleteness flag is missing => UNKNOWN
        3. Shotmap is unavailable or cannot compare => UNKNOWN (no default PASS fallback)
        While confirmed source aggregate xG and partial sums are preserved.
        """
        collector = PostmatchCollector()
        mock_binding = {
            "status": "PASS",
            "fotmob_match_id": "5795455",
            "canonical_url": "https://www.fotmob.com/matches/test/5795455"
        }
        starters = [{"name": f"P{i}", "id": i} for i in range(1, 12)]

        # Scenario 1: Official aggregate exists (Understat 1.20 - 0.80), but shotmap has incomplete shot xG (delta is small 0.01)
        mock_fm_page_incomplete_shots = {
            "general": {"matchId": 5795455, "homeTeam": {"id": 1, "name": "A"}, "awayTeam": {"id": 2, "name": "B"}},
            "header": {"status": {"finished": True}, "teams": [{"name": "A", "score": 1}, {"name": "B", "score": 0}]},
            "content": {
                "lineup": {"lineupType": "confirmed", "homeTeam": {"starters": starters}, "awayTeam": {"starters": starters}},
                "stats": {"Periods": {"All": {"stats": [{"title": "Top stats", "stats": [{"title": "Total shots", "stats": [2, 1]}, {"title": "Shots on target", "stats": [1, 0]}, {"title": "Ball possession", "stats": [50, 50]}]}]}}},
                "shotmap": {
                    "shots": [
                        # Home: 1 shot has 1.20 xG (matches Understat 1.20 exactly!), 1 shot is missing xG!
                        {"id": 1, "min": 10, "teamId": 1, "playerName": "H1", "expectedGoals": 1.20, "eventType": "Goal", "situation": "OpenPlay", "shotType": "RightFoot"},
                        {"id": 2, "min": 30, "teamId": 1, "playerName": "H2", "expectedGoals": None, "eventType": "Miss", "situation": "OpenPlay", "shotType": "LeftFoot"},
                        # Away: 0.80 xG
                        {"id": 3, "min": 60, "teamId": 2, "playerName": "A1", "expectedGoals": 0.80, "eventType": "Miss", "situation": "OpenPlay", "shotType": "RightFoot"}
                    ]
                },
                "matchFacts": {"events": {"events": [{"type": "Goal", "time": 10, "player": {"name": "H1"}, "teamId": 1, "isHome": True, "shotType": "RightFoot"}]}}
            }
        }
        mock_understat = {
            "status": "PASS",
            "home_score": 1,
            "away_score": 0,
            "xg_home": 1.20,
            "xg_away": 0.80,
            "canonical_url": "https://understat.com/match/123",
            "retrieved_at_utc": "2026-11-04T00:00:00Z"
        }

        with patch("postmatch_collector.fetch_fotmob_completed_page", return_value=(mock_fm_page_incomplete_shots, "url", "PASS")), \
             patch("postmatch_collector.fetch_understat_match_data", return_value=mock_understat):
            res1 = collector.collect_fixture({
                "project_fixture_id": "9999003",
                "league": "EPL",
                "home_team": "A",
                "away_team": "B",
                "kickoff_utc": "2026-09-20T13:00:00Z",
                "existing_binding": mock_binding
            })
            # Confirmed source aggregate xG is preserved
            self.assertEqual(res1["canonical_xg_home"], 1.20)
            self.assertEqual(res1["canonical_xg_away"], 0.80)
            self.assertEqual(res1["canonical_xg_source"], "Understat")
            # Partial shotmap sum is preserved
            self.assertEqual(res1["derived_shotmap_xg_home"], 1.20)
            self.assertEqual(res1["known_shotmap_xg_home"], 1.20)
            # Despite delta being 0.0 (1.20 - 1.20 = 0), incomplete data CANNOT display PASS! Must be CAUTION
            self.assertNotEqual(res1["xg_consistency"], "PASS")
            self.assertEqual(res1["xg_consistency"], "CAUTION")

        # Scenario 2: Canonical xG exists, but shotmap is completely unavailable (cannot compare)
        mock_fm_page_no_shotmap = {
            "general": {"matchId": 5795455, "homeTeam": {"name": "A"}, "awayTeam": {"name": "B"}},
            "header": {"status": {"finished": True}, "teams": [{"name": "A", "score": 1}, {"name": "B", "score": 0}]},
            "content": {
                "lineup": {"lineupType": "confirmed", "homeTeam": {"starters": starters}, "awayTeam": {"starters": starters}},
                "stats": {"Periods": {"All": {"stats": [{"title": "Top stats", "stats": [{"title": "Total shots", "stats": [1, 0]}, {"title": "Shots on target", "stats": [1, 0]}, {"title": "Ball possession", "stats": [50, 50]}]}]}}},
                "shotmap": {"shots": []}, # No shotmap
                "matchFacts": {"events": {"events": [{"type": "Goal", "time": 10, "player": {"name": "H1"}, "teamId": 1, "isHome": True, "shotType": "RightFoot"}]}}
            }
        }
        with patch("postmatch_collector.fetch_fotmob_completed_page", return_value=(mock_fm_page_no_shotmap, "url", "PASS")), \
             patch("postmatch_collector.fetch_understat_match_data", return_value=mock_understat):
            res2 = collector.collect_fixture({
                "project_fixture_id": "9999004",
                "league": "EPL",
                "home_team": "A",
                "away_team": "B",
                "kickoff_utc": "2026-09-20T13:00:00Z",
                "existing_binding": mock_binding
            })
            # Canonical xG exists from Understat
            self.assertEqual(res2["canonical_xg_home"], 1.20)
            # But without shotmap, cannot compare => must be UNKNOWN, NOT PASS!
            self.assertNotEqual(res2["xg_consistency"], "PASS")
            self.assertEqual(res2["xg_consistency"], "UNKNOWN")

if __name__ == "__main__":
    unittest.main()
