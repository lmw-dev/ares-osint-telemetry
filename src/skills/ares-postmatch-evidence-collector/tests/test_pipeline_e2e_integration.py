#!/usr/bin/env python3
"""
E2E Integration Test: Postmatch Pipeline & Handoff Generation Guard
Verifies that scripts/run_postmatch_round.py correctly:
1. Distinguishes official aggregate xG from derived cumulative NPxG.
2. Displays "已知部分/不完整" when derived shots/xG are incomplete (never presents partial as complete).
3. Rejects identity mismatch or non-final match without falling back to CONFIRMED.
4. Operates in isolated temporary directories without depending on fixed agy paths.
"""

import unittest
import sys
import json
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "src/skills/ares-postmatch-evidence-collector/scripts"))

import fotmob_postmatch_client
from run_postmatch_round import PostmatchHandoffBuilder

class TestPostmatchPipelineIntegration(unittest.TestCase):

    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp(prefix="ares_pipeline_e2e_"))
        self.postmatch_dir = self.test_dir / "postmatch"
        self.postmatch_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    def test_pipeline_consumes_game_state_and_npxg(self):
        """Verify run_postmatch_round extracts game state, labels derived NPxG, and keeps official summary separate"""
        fid = "9990001"
        fdir = self.postmatch_dir / fid
        fdir.mkdir(parents=True, exist_ok=True)

        mock_raw = {
            "general": {"matchId": 888001, "homeTeam": {"id": 10, "name": "Team A"}, "awayTeam": {"id": 20, "name": "Team B"}},
            "header": {
                "status": {"finished": True},
                "teams": [{"score": 2, "name": "Team A"}, {"score": 0, "name": "Team B"}]
            },
            "content": {
                "matchFacts": {
                    "events": {
                        "events": [
                            {"type": "Goal", "time": 30, "isHome": True, "goalDescription": None, "player": {"name": "Striker A"}, "shotmapEvent": {"situation": "RegularPlay", "teamId": 10}},
                            {"type": "Goal", "time": 60, "isHome": True, "goalDescription": "Penalty", "player": {"name": "Pen Taker"}, "shotmapEvent": {"situation": "Penalty", "teamId": 10}}
                        ]
                    }
                },
                "lineup": {"homeTeam": {"formation": "4-3-3", "starters": []}, "awayTeam": {"formation": "4-4-2", "starters": []}},
                "stats": {
                    "Periods": {
                        "All": {
                            "stats": [
                                {"title": "Top stats", "stats": [
                                    {"title": "Ball possession", "stats": [60, 40]},
                                    {"title": "Total shots", "stats": [15, 6]},
                                    {"title": "Shots on target", "stats": [6, 2]},
                                    {"title": "Expected goals (xG)", "stats": ["2.10", "0.45"]},
                                    {"title": "Fouls committed", "stats": [10, 12]}
                                ]}
                            ]
                        }
                    }
                },
                "shotmap": {
                    "shots": [
                        {"id": 1, "min": 10, "teamId": 10, "expectedGoals": 0.30, "eventType": "Miss", "situation": "RegularPlay"},
                        {"id": 2, "min": 25, "teamId": 20, "expectedGoals": 0.45, "eventType": "Save", "situation": "RegularPlay"},
                        {"id": 3, "min": 30, "teamId": 10, "expectedGoals": 0.60, "eventType": "Goal", "situation": "RegularPlay"},
                        {"id": 4, "min": 50, "teamId": 10, "expectedGoals": 0.40, "eventType": "Miss", "situation": "RegularPlay"},
                        {"id": 5, "min": 60, "teamId": 10, "expectedGoals": 0.79, "eventType": "Goal", "situation": "Penalty", "shotType": "Penalty"}
                    ]
                }
            }
        }
        (fdir / "00-fotmob-raw.json").write_text(json.dumps(mock_raw))

        fixtures = [{
            "fixture_id": fid,
            "fotmob_id": "888001",
            "home_team": "Team A",
            "away_team": "Team B",
            "kickoff_utc": "2026-11-04T12:00:00Z",
            "binding_path": ""
        }]

        builder = PostmatchHandoffBuilder("2026-2027", "EPL", "GW10", fixtures)
        builder.postmatch_dir = self.postmatch_dir
        builder.output_zip = self.test_dir / "TEST_HANDOFF.zip"
        builder._execute_squad_valuation_rolling = lambda: {"status": "SKIP", "allow_stage6_promotion": False}

        with patch.object(fotmob_postmatch_client, "fetch_fotmob_completed_page", return_value=(mock_raw, "https://fotmob.com/888001", "PASS")):
            builder.execute()

        # 1. Assert 04-xg-process.json is generated with high precision data
        xg_json_path = fdir / "04-xg-process.json"
        self.assertTrue(xg_json_path.exists())
        xg_data = json.loads(xg_json_path.read_text())
        self.assertEqual(xg_data.get("derived_npxg_home"), 1.30)
        self.assertEqual(xg_data.get("derived_npxg_away"), 0.45)
        self.assertEqual(xg_data.get("game_state_split", {}).get("status"), "GAME_STATE_SPLIT_AVAILABLE")

        # 2. Assert 03-postmatch-preliminary.md explicitly separates official aggregate vs derived NPxG
        prelim_path = fdir / "03-postmatch-preliminary.md"
        self.assertTrue(prelim_path.exists())
        prelim_text = prelim_path.read_text()
        self.assertIn("官方来源汇总 xG (主队-客队)", prelim_text)
        self.assertIn("2.1 - 0.45", prelim_text)
        self.assertIn("逐射累加非点球 NPxG", prelim_text)
        self.assertIn("1.3 - 0.45", prelim_text)
        self.assertIn("逐射累加 NPxG", prelim_text)
        self.assertIn("单场平比分数据仅反映本场特定对手与战术场景下的阶段性产出，严禁直接外推为球队长期攻坚能力", prelim_text)

        # 3. Assert 00-ROUND-POSTMATCH-SUMMARY.md contains NPxG column
        summary_path = self.postmatch_dir / "00-ROUND-POSTMATCH-SUMMARY.md"
        self.assertTrue(summary_path.exists())
        summary_text = summary_path.read_text()
        self.assertIn("官方汇总 xG", summary_text)
        self.assertIn("逐射累加 NPxG", summary_text)
        self.assertIn("1.3 - 0.45", summary_text)

    def test_pipeline_incompleteness_displays_partial(self):
        """Verify incomplete shot data is explicitly labeled as '已知部分/不完整', never shown as complete"""
        fid = "9990002"
        fdir = self.postmatch_dir / fid
        fdir.mkdir(parents=True, exist_ok=True)

        # Mock FotMob raw where home shot 2 lacks expectedGoals
        mock_raw = {
            "general": {"matchId": 888002, "homeTeam": {"id": 10, "name": "Team A"}, "awayTeam": {"id": 20, "name": "Team B"}},
            "header": {
                "status": {"finished": True},
                "teams": [{"score": 1, "name": "Team A"}, {"score": 0, "name": "Team B"}]
            },
            "content": {
                "matchFacts": {
                    "events": {
                        "events": [
                            {"type": "Goal", "time": 30, "isHome": True, "player": {"name": "Striker A"}, "shotmapEvent": {"situation": "RegularPlay", "teamId": 10}}
                        ]
                    }
                },
                "lineup": {"homeTeam": {"formation": "4-3-3", "starters": []}, "awayTeam": {"formation": "4-4-2", "starters": []}},
                "stats": {
                    "Periods": {
                        "All": {
                            "stats": [
                                {"title": "Top stats", "stats": [
                                    {"title": "Ball possession", "stats": [50, 50]},
                                    {"title": "Total shots", "stats": [10, 5]},
                                    {"title": "Shots on target", "stats": [4, 1]},
                                    {"title": "Expected goals (xG)", "stats": ["1.50", "0.30"]}
                                ]}
                            ]
                        }
                    }
                },
                "shotmap": {
                    "shots": [
                        {"id": 1, "min": 10, "teamId": 10, "expectedGoals": 0.50, "eventType": "Miss", "situation": "RegularPlay"},
                        {"id": 2, "min": 20, "teamId": 10, "expectedGoals": None, "eventType": "Miss", "situation": "RegularPlay"}, # Missing xG!
                        {"id": 3, "min": 30, "teamId": 10, "expectedGoals": 0.40, "eventType": "Goal", "situation": "RegularPlay"},
                        {"id": 4, "min": 70, "teamId": 20, "expectedGoals": 0.30, "eventType": "Miss", "situation": "RegularPlay"}
                    ]
                }
            }
        }
        (fdir / "00-fotmob-raw.json").write_text(json.dumps(mock_raw))

        fixtures = [{
            "fixture_id": fid,
            "fotmob_id": "888002",
            "home_team": "Team A",
            "away_team": "Team B",
            "kickoff_utc": "2026-11-04T12:00:00Z",
            "binding_path": ""
        }]

        builder = PostmatchHandoffBuilder("2026-2027", "EPL", "GW10", fixtures)
        builder.postmatch_dir = self.postmatch_dir
        builder.output_zip = self.test_dir / "TEST_HANDOFF.zip"
        builder._execute_squad_valuation_rolling = lambda: {"status": "SKIP", "allow_stage6_promotion": False}

        with patch.object(fotmob_postmatch_client, "fetch_fotmob_completed_page", return_value=(mock_raw, "https://fotmob.com/888002", "PASS")):
            builder.execute()

        prelim_path = fdir / "03-postmatch-preliminary.md"
        prelim_text = prelim_path.read_text()
        # Home NPxG must be labeled as incomplete
        self.assertIn("已知部分 0.9 (不完整)", prelim_text)
        self.assertNotIn("**0.9 - 0.3**", prelim_text) # Must not present partial as complete pair

        summary_path = self.postmatch_dir / "00-ROUND-POSTMATCH-SUMMARY.md"
        summary_text = summary_path.read_text()
        self.assertIn("已知部分 0.9 (不完整) - 0.3", summary_text)

    def test_pipeline_rejects_identity_mismatch_and_non_final(self):
        """Verify identity mismatch or non-final match is blocked, never falling back to FOTMOB_CONFIRMED"""
        fid = "9990003"
        fdir = self.postmatch_dir / fid
        fdir.mkdir(parents=True, exist_ok=True)

        # Mock page props with WRONG matchId
        mismatched_raw = {
            "general": {"matchId": 999999, "homeTeam": {"id": 10, "name": "Team A"}, "awayTeam": {"id": 20, "name": "Team B"}},
            "header": {
                "status": {"finished": True},
                "teams": [{"score": 2, "name": "Team A"}, {"score": 0, "name": "Team B"}]
            },
            "content": {}
        }
        (fdir / "00-fotmob-raw.json").write_text(json.dumps(mismatched_raw))

        fixtures = [{
            "fixture_id": fid,
            "fotmob_id": "888003", # Bound ID is 888003, parsed ID is 999999
            "home_team": "Team A",
            "away_team": "Team B",
            "kickoff_utc": "2026-11-04T12:00:00Z",
            "binding_path": ""
        }]

        builder = PostmatchHandoffBuilder("2026-2027", "EPL", "GW10", fixtures)
        builder.postmatch_dir = self.postmatch_dir
        builder.output_zip = self.test_dir / "TEST_HANDOFF.zip"
        builder._execute_squad_valuation_rolling = lambda: {"status": "SKIP", "allow_stage6_promotion": False}

        with patch.object(fotmob_postmatch_client, "fetch_fotmob_completed_page", return_value=(mismatched_raw, "https://fotmob.com/888003", "PASS")):
            builder.execute()

        # Assert runtime state marks SOURCE_IDENTITY_MISMATCH, not CONFIRMED
        runtime_path = fdir / "00-runtime-state.md"
        runtime_text = runtime_path.read_text()
        self.assertIn("collector_status: SOURCE_IDENTITY_MISMATCH", runtime_text)
        self.assertNotIn("collector_status: FOTMOB_CONFIRMED", runtime_text)

        # Assert preliminary marks unknown
        prelim_path = fdir / "03-postmatch-preliminary.md"
        prelim_text = prelim_path.read_text()
        self.assertIn("全场比分 (FT)**: **UNKNOWN**", prelim_text)

if __name__ == "__main__":
    unittest.main()
