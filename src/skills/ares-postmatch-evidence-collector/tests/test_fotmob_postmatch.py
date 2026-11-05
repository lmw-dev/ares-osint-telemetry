#!/usr/bin/env python3
"""
Unit tests for FotMob completed-match evidence extraction:
A. FotMob completed-match parsing
B. confirmed XI parsing
C. event parsing
D. shots / SOT / possession / corners parsing
E. xG parsing
"""

import unittest
import sys
from pathlib import Path

SKILL_SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SKILL_SCRIPTS))

from fotmob_postmatch_client import parse_fotmob_completed_match

class TestFotMobPostmatch(unittest.TestCase):

    def setUp(self):
        starters_11 = [{"name": f"P{i}", "id": i, "shirt": str(i), "position": "MID"} for i in range(1, 12)]
        self.mock_page_props = {
            "general": {
                "matchId": 5795455,
                "homeTeam": {"id": 8678, "name": "Bournemouth"},
                "awayTeam": {"id": 8650, "name": "Liverpool"}
            },
            "header": {
                "status": {
                    "finished": True,
                    "started": True,
                    "reason": {"long": "Full-Time"}
                },
                "teams": [
                    {"name": "Bournemouth", "score": 0},
                    {"name": "Liverpool", "score": 2}
                ]
            },
            "content": {
                "matchFacts": {
                    "events": {
                        "events": [
                            {"type": "Goal", "time": 23, "player": {"name": "Luis Diaz"}, "teamId": 8650, "isOwnGoal": False, "shotType": "RightFoot"},
                            {"type": "Half", "time": 45, "homeScore": 0, "awayScore": 1, "halfStrShort": "HT"},
                            {"type": "Goal", "time": 65, "player": {"name": "Darwin Nunez"}, "teamId": 8650, "isOwnGoal": False, "shotType": "Header"},
                            {"type": "Card", "time": 72, "player": {"name": "Defender A"}, "teamId": 8678, "card": "Red"}
                        ]
                    }
                },
                "lineup": {
                    "lineupType": "confirmed",
                    "homeTeam": {"formation": "4-2-3-1", "starters": starters_11, "subs": []},
                    "awayTeam": {"formation": "4-3-3", "starters": starters_11, "subs": []}
                },
                "stats": {
                    "Periods": {
                        "All": {
                            "stats": [
                                {
                                    "title": "Top stats",
                                    "stats": [
                                        {"title": "Total shots", "stats": [9, 12]},
                                        {"title": "Shots on target", "stats": [2, 5]},
                                        {"title": "Ball possession", "stats": [45, 55]},
                                        {"title": "Corners", "stats": [3, 6]},
                                        {"title": "Big chances", "stats": [1, 3]},
                                        {"title": "Expected goals (xG)", "stats": ["0.75", "1.85"]}
                                    ]
                                }
                            ]
                        }
                    }
                },
                "shotmap": {
                    "shots": [
                        {"id": 1, "min": 23, "teamId": 8650, "playerName": "Luis Diaz", "expectedGoals": 0.45, "eventType": "Goal", "x": 88.0, "y": 34.0, "situation": "OpenPlay", "shotType": "RightFoot", "isOnTarget": True},
                        {"id": 2, "min": 65, "teamId": 8650, "playerName": "Darwin Nunez", "expectedGoals": 0.55, "eventType": "Goal", "x": 92.0, "y": 33.0, "situation": "Corner", "shotType": "Header", "isOnTarget": True}
                    ]
                }
            }
        }

    def test_A_fotmob_completed_match_parsing(self):
        """Test A: Parse score and status from completed match"""
        res = parse_fotmob_completed_match(self.mock_page_props, "https://www.fotmob.com/test", "5795455")
        self.assertEqual(res["status"], "PASS")
        self.assertEqual(res["result_evidence"]["final_score_str"], "0-2")
        self.assertEqual(res["result_evidence"]["halftime_score_str"], "0-1")
        self.assertTrue(res["result_evidence"]["finished"])

    def test_B_confirmed_xi_parsing(self):
        """Test B: Parse confirmed XI (11 home, 11 away) and formations"""
        res = parse_fotmob_completed_match(self.mock_page_props, "https://www.fotmob.com/test", "5795455")
        lineup = res["lineup_events_evidence"]
        self.assertEqual(lineup["lineup_status"], "CONFIRMED")
        self.assertEqual(len(lineup["home_xi"]), 11)
        self.assertEqual(len(lineup["away_xi"]), 11)
        self.assertEqual(lineup["home_formation"], "4-2-3-1")
        self.assertEqual(lineup["away_formation"], "4-3-3")

    def test_C_event_parsing(self):
        """Test C: Parse goals, cards, and halftime events"""
        res = parse_fotmob_completed_match(self.mock_page_props, "https://www.fotmob.com/test", "5795455")
        events = res["lineup_events_evidence"]
        self.assertEqual(len(events["goals"]), 2)
        self.assertEqual(events["goals"][0]["player"], "Luis Diaz")
        self.assertEqual(len(events["red_cards"]), 1)
        self.assertEqual(events["red_cards"][0]["player"], "Defender A")

    def test_D_stats_parsing(self):
        """Test D: Parse shots, shots on target, possession, corners, big chances"""
        res = parse_fotmob_completed_match(self.mock_page_props, "https://www.fotmob.com/test", "5795455")
        stats = res["process_stats_evidence"]
        self.assertEqual(stats["shots_home"], 9)
        self.assertEqual(stats["shots_away"], 12)
        self.assertEqual(stats["shots_on_target_home"], 2)
        self.assertEqual(stats["shots_on_target_away"], 5)
        self.assertEqual(stats["possession_home"], 45)
        self.assertEqual(stats["possession_away"], 55)
        self.assertEqual(stats["corners_home"], 3)
        self.assertEqual(stats["corners_away"], 6)
        self.assertEqual(stats["big_chances_home"], 1)
        self.assertEqual(stats["big_chances_away"], 3)

    def test_E_xg_parsing(self):
        """Test E: Parse xG and shot map data"""
        res = parse_fotmob_completed_match(self.mock_page_props, "https://www.fotmob.com/test", "5795455")
        xg_ev = res["xg_process_evidence"]
        self.assertIn(xg_ev["xg_source"], ["FotMob-Shotmap", "FotMob-Stats", "FotMob-Aggregate"])
        self.assertEqual(xg_ev["shots_count"], 2)
        self.assertTrue(xg_ev["shot_map_available"])
        self.assertGreater(xg_ev["xg_away"], 0.9)

    def test_F_game_state_and_red_card_split(self):
        """Test F: Parse game state and red card splits (LMW-382, LMW-386)"""
        res = parse_fotmob_completed_match(self.mock_page_props, "https://www.fotmob.com/test", "5795455")
        xg_ev = res["xg_process_evidence"]

        # Red card split test
        rc_split = xg_ev.get("red_card_split")
        self.assertIsNotNone(rc_split)
        self.assertEqual(rc_split["status"], "RED_CARD_SPLIT_AVAILABLE")
        self.assertEqual(rc_split["first_red_minute"], 72)
        self.assertEqual(rc_split["first_red_team"], "home")
        self.assertIn("pre_red_11v11", rc_split)
        self.assertIn("post_red", rc_split)

        # Game state split test
        gs_split = xg_ev.get("game_state_split")
        self.assertIsNotNone(gs_split)
        self.assertEqual(gs_split["status"], "GAME_STATE_SPLIT_AVAILABLE")
        self.assertGreaterEqual(len(gs_split["segments"]), 2)
        self.assertEqual(gs_split["segments"][0]["state"], "level")
        self.assertEqual(gs_split["segments"][0]["score_str"], "0-0")

    def test_G_non_penalty_and_boundary_limitations(self):
        """Test G: Verify npxG tracking and boundary limitations"""
        res = parse_fotmob_completed_match(self.mock_page_props, "https://www.fotmob.com/test", "5795455")
        xg_ev = res["xg_process_evidence"]
        self.assertIn("derived_npxg_home", xg_ev)
        self.assertIn("derived_npxg_away", xg_ev)
        self.assertIn("boundary_limitations", xg_ev)
        self.assertEqual(xg_ev["boundary_limitations"]["open_play_vs_npxg_rule"], "NON_PENALTY_XG_NOT_EQUAL_OPEN_PLAY_XG")

    def test_H_inconsistent_score_events_gate(self):
        """Test H: Abort deterministic split when event goals do not match final score"""
        corrupted_props = dict(self.mock_page_props)
        # Final score is 0-2, but remove all goals from events
        corrupted_props["content"] = dict(self.mock_page_props["content"])
        corrupted_props["content"]["matchFacts"] = {
            "events": {"events": []}
        }
        res = parse_fotmob_completed_match(corrupted_props, "https://www.fotmob.com/test", "5795455")
        gs_split = res["xg_process_evidence"]["game_state_split"]
        self.assertEqual(gs_split["status"], "INCONSISTENT_SCORE_EVENTS")
        self.assertFalse(gs_split["reliable_split_available"])

    def test_I_own_goal_and_same_minute_ambiguity(self):
        """Test I: Own goal excluded from conventional shots and same-minute ambiguity handled"""
        props = dict(self.mock_page_props)
        # Add a missed shot at minute 23 (same minute as Luis Diaz goal at 23)
        props["content"] = dict(self.mock_page_props["content"])
        props["content"]["shotmap"] = {
            "shots": [
                {"id": 1, "min": 23, "teamId": 8650, "playerName": "Luis Diaz", "expectedGoals": 0.45, "eventType": "Goal", "x": 88.0, "y": 34.0, "situation": "OpenPlay", "shotType": "RightFoot", "isOnTarget": True},
                {"id": 10, "min": 23, "teamId": 8678, "playerName": "Opponent Striker", "expectedGoals": 0.12, "eventType": "Miss", "x": 60.0, "y": 30.0, "situation": "OpenPlay", "shotType": "LeftFoot", "isOnTarget": False},
                {"id": 2, "min": 65, "teamId": 8650, "playerName": "Darwin Nunez", "expectedGoals": 0.55, "eventType": "Goal", "x": 92.0, "y": 33.0, "situation": "Corner", "shotType": "Header", "isOnTarget": True},
                {"id": 99, "min": 40, "teamId": 8678, "playerName": "Own Goal Guy", "expectedGoals": None, "eventType": "Goal", "isOwnGoal": True, "x": 10.0, "y": 30.0, "situation": "FromCorner", "shotType": "OtherBodyParts", "isOnTarget": True}
            ]
        }
        res = parse_fotmob_completed_match(props, "https://www.fotmob.com/test", "5795455")
        xg_ev = res["xg_process_evidence"]
        # Own goal should not count towards conventional shots count (3 regular shots)
        self.assertEqual(xg_ev["shots_count"], 3)
        gs_split = xg_ev["game_state_split"]
        self.assertTrue(gs_split["has_own_goal"])
        # The shot at minute 23 by Opponent Striker must be categorized as order_ambiguous_shots
        amb_shots = gs_split.get("order_ambiguous_shots", [])
        self.assertEqual(len(amb_shots), 1)
        self.assertEqual(amb_shots[0]["player"], "Opponent Striker")
        self.assertEqual(amb_shots[0]["minute"], 23)

    def test_J_unknown_goal_team_gate(self):
        """Test J: When goal team is unknown, do not default to away, and abort split even if score numbers match"""
        props = dict(self.mock_page_props)
        props["content"] = dict(self.mock_page_props["content"])
        props["header"] = dict(self.mock_page_props["header"])
        props["header"]["teams"] = [
            {"name": "Bournemouth", "score": 0},
            {"name": "Liverpool", "score": 1}
        ]
        # One goal with unknown team (teamId 999999, isHome None)
        props["content"]["matchFacts"] = {
            "events": {
                "events": [
                    {"type": "Goal", "time": 30, "player": {"name": "Mystery Scorer"}, "teamId": 999999, "isHome": None}
                ]
            }
        }
        res = parse_fotmob_completed_match(props, "https://www.fotmob.com/test", "5795455")
        gs_split = res["xg_process_evidence"]["game_state_split"]
        self.assertEqual(gs_split["status"], "UNKNOWN")
        self.assertFalse(gs_split["reliable_split_available"])
        self.assertIn("UNKNOWN_GOAL_TEAM", gs_split["reason"])

    def test_K_missing_xg_and_unknown_penalty_type(self):
        """Test K: Missing shot xG or penalty type must not silently default to 0 or non-penalty, and flag completeness"""
        props = dict(self.mock_page_props)
        props["content"] = dict(self.mock_page_props["content"])
        props["content"]["shotmap"] = {
            "shots": [
                # Shot 1: Home team normal open play shot with xG
                {"id": 1, "min": 10, "teamId": 8678, "playerName": "Home Player 1", "expectedGoals": 0.25, "eventType": "Miss", "situation": "OpenPlay", "shotType": "RightFoot"},
                # Shot 2: Home team missing xG! (None)
                {"id": 2, "min": 20, "teamId": 8678, "playerName": "Home Player 2", "expectedGoals": None, "eventType": "Miss", "situation": "OpenPlay", "shotType": "LeftFoot"},
                # Shot 3: Home team missing penalty classification (situation & shotType None)
                {"id": 3, "min": 35, "teamId": 8678, "playerName": "Home Player 3", "expectedGoals": 0.40, "eventType": "Miss", "situation": None, "shotType": None},
                # Shot 4: Away team confirmed penalty
                {"id": 4, "min": 65, "teamId": 8650, "playerName": "Away Pen Taker", "expectedGoals": 0.79, "eventType": "Goal", "situation": "Penalty", "shotType": "Penalty"}
            ]
        }
        res = parse_fotmob_completed_match(props, "https://www.fotmob.com/test", "5795455")
        xg_ev = res["xg_process_evidence"]
        
        # Check completeness flags
        shot_comp = xg_ev["shotmap_xg_completeness"]
        self.assertFalse(shot_comp["home"]["is_complete"])
        self.assertEqual(shot_comp["home"]["missing_xg_count"], 1)
        self.assertEqual(shot_comp["home"]["known_shots_count"], 2)
        # Retains known sum (0.25 + 0.40 = 0.65)
        self.assertEqual(shot_comp["home"]["known_xg"], 0.65)
        self.assertEqual(xg_ev["derived_shotmap_xg_home"], 0.65)

        # NPxG completeness
        npxg_comp = xg_ev["npxg_completeness"]
        self.assertFalse(npxg_comp["home"]["is_complete"])
        self.assertEqual(npxg_comp["home"]["missing_penalty_type_count"], 1)
        # Retains confirmed non-penalty sum with valid xG (only Shot 1: 0.25)
        self.assertEqual(npxg_comp["home"]["known_npxg"], 0.25)
        self.assertEqual(xg_ev["derived_npxg_home"], 0.25)

    def test_L_same_minute_multiple_goals(self):
        """Test L: Two goals in the same minute: no repeated shot matching, no descending window, and ambiguous sequence flagged"""
        props = dict(self.mock_page_props)
        props["content"] = dict(self.mock_page_props["content"])
        props["header"] = dict(self.mock_page_props["header"])
        props["header"]["teams"] = [
            {"name": "Bournemouth", "score": 1},
            {"name": "Liverpool", "score": 1}
        ]
        # Both goals happen in minute 45
        props["content"]["matchFacts"] = {
            "events": {
                "events": [
                    {"type": "Goal", "time": 45, "player": {"name": "Home Hero"}, "teamId": 8678, "isOwnGoal": False, "isHome": True, "shotType": "RightFoot"},
                    {"type": "Goal", "time": 45, "player": {"name": "Away Hero"}, "teamId": 8650, "isOwnGoal": False, "isHome": False, "shotType": "LeftFoot"}
                ]
            }
        }
        props["content"]["shotmap"] = {
            "shots": [
                {"id": 101, "min": 15, "teamId": 8678, "playerName": "Early Bird", "expectedGoals": 0.15, "eventType": "Miss", "situation": "OpenPlay", "shotType": "Header"},
                {"id": 102, "min": 45, "teamId": 8678, "playerName": "Home Hero", "expectedGoals": 0.42, "eventType": "Goal", "situation": "OpenPlay", "shotType": "RightFoot"},
                {"id": 103, "min": 45, "teamId": 8650, "playerName": "Away Hero", "expectedGoals": 0.38, "eventType": "Goal", "situation": "OpenPlay", "shotType": "LeftFoot"},
                {"id": 104, "min": 45, "teamId": 8678, "playerName": "Unlucky Third", "expectedGoals": 0.08, "eventType": "Miss", "situation": "OpenPlay", "shotType": "Header"},
                {"id": 105, "min": 80, "teamId": 8650, "playerName": "Late Chance", "expectedGoals": 0.20, "eventType": "Miss", "situation": "OpenPlay", "shotType": "RightFoot"}
            ]
        }
        res = parse_fotmob_completed_match(props, "https://www.fotmob.com/test", "5795455")
        gs_split = res["xg_process_evidence"]["game_state_split"]
        
        self.assertEqual(gs_split["status"], "GAME_STATE_SPLIT_AVAILABLE")
        self.assertTrue(gs_split["score_consistent"])
        
        # Check that same_minute_multi_goal_events is recorded
        multi_events = gs_split.get("same_minute_multi_goal_events", [])
        self.assertEqual(len(multi_events), 1)
        self.assertEqual(multi_events[0]["minute"], 45)
        self.assertEqual(multi_events[0]["goals_count"], 2)
        self.assertTrue(multi_events[0]["intermediate_score_states_omitted"])

        # Check all segments have non-descending windows
        windows = [seg["window"] for seg in gs_split["segments"]]
        scores = [seg["score_str"] for seg in gs_split["segments"]]
        for w in windows:
            self.assertNotIn("46-45m", w)
        # Intermediate states like 1-0 or 0-1 must NOT be output as deterministic segments
        self.assertNotIn("1-0", scores)
        self.assertNotIn("0-1", scores)

        # All 3 shots in minute 45 should be recorded in order_ambiguous_shots
        amb_shots = gs_split["order_ambiguous_shots"]
        self.assertEqual(len(amb_shots), 3)
        amb_ids = [s.get("player") for s in amb_shots]
        self.assertIn("Home Hero", amb_ids)
        self.assertIn("Away Hero", amb_ids)
        self.assertIn("Unlucky Third", amb_ids)

if __name__ == "__main__":
    unittest.main()
