#!/usr/bin/env python3
"""
Unit tests for FotMob Roster Delta computation:
K. new injury detected
L. player return detected
M. predicted to confirmed transition
"""

import unittest
import sys
from pathlib import Path

SKILL_SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SKILL_SCRIPTS))

from roster_delta import compute_roster_delta

class TestFotMobRosterDelta(unittest.TestCase):

    def test_K_new_injury_detected(self):
        """Test K: A player not previously listed as injured/unavailable appears in current injured list"""
        prev_snapshot = {
            "retrieved_at_utc": "2026-09-20T10:00:00Z",
            "lineup_status": "PREDICTED",
            "injured_home": [{"player_name": "Player A", "reason": "Knee"}],
            "injured_away": [],
            "unavailable_home": [{"player_name": "Player A", "reason": "Knee"}],
            "unavailable_away": []
        }
        curr_snapshot = {
            "retrieved_at_utc": "2026-09-20T17:00:00Z",
            "lineup_status": "PREDICTED",
            "injured_home": [
                {"player_name": "Player A", "reason": "Knee"},
                {"player_name": "Player B", "reason": "Hamstring"}
            ],
            "injured_away": [],
            "unavailable_home": [
                {"player_name": "Player A", "reason": "Knee"},
                {"player_name": "Player B", "reason": "Hamstring"}
            ],
            "unavailable_away": []
        }

        delta = compute_roster_delta(prev_snapshot, curr_snapshot)

        self.assertIn("Player B", delta["new_injuries"]["home"])
        self.assertIn("Player B", delta["newly_unavailable"]["home"])
        self.assertEqual(delta["material_change"], "YES")

    def test_L_player_return_detected(self):
        """Test L: A previously injured/unavailable player is removed from injured list and becomes available"""
        prev_snapshot = {
            "retrieved_at_utc": "2026-09-19T10:00:00Z",
            "lineup_status": "PREDICTED",
            "injured_home": [{"player_name": "Player A", "reason": "Ankle"}],
            "injured_away": [],
            "unavailable_home": [{"player_name": "Player A", "reason": "Ankle"}],
            "unavailable_away": [],
            "back_in_training_home": []
        }
        curr_snapshot = {
            "retrieved_at_utc": "2026-09-20T17:00:00Z",
            "lineup_status": "PREDICTED",
            "injured_home": [],
            "injured_away": [],
            "unavailable_home": [],
            "unavailable_away": [],
            "back_in_training_home": [{"player_name": "Player A"}]
        }

        delta = compute_roster_delta(prev_snapshot, curr_snapshot)

        self.assertIn("Player A", delta["returned_available"]["home"])
        self.assertIn("Player A", delta["removed_from_injury_list"]["home"])
        self.assertIn("Player A", delta["newly_back_in_training"]["home"])
        self.assertEqual(delta["material_change"], "YES")

    def test_M_predicted_to_confirmed_transition(self):
        """Test M: Lineup transitioning from PREDICTED to CONFIRMED with XI changes detected"""
        prev_snapshot = {
            "retrieved_at_utc": "2026-09-20T10:00:00Z",
            "lineup_status": "PREDICTED",
            "predicted_xi_home": ["GK1", "DEF1", "DEF2", "MID1", "FWD1"],
            "predicted_xi_away": ["GK2", "DEF3", "MID2", "FWD2"]
        }
        curr_snapshot = {
            "retrieved_at_utc": "2026-09-20T17:15:00Z",
            "lineup_status": "CONFIRMED",
            "confirmed_xi_home": ["GK1", "DEF1", "DEF2", "MID1", "FWD_SURPRISE"], # FWD1 dropped for FWD_SURPRISE
            "confirmed_xi_away": ["GK2", "DEF3", "MID2", "FWD2"]
        }

        delta = compute_roster_delta(prev_snapshot, curr_snapshot)

        self.assertTrue(delta["newly_confirmed_xi"])
        self.assertEqual(delta["lineup_status_before"], "PREDICTED")
        self.assertEqual(delta["lineup_status_now"], "CONFIRMED")
        self.assertIn("FWD_SURPRISE", delta["xi_changes"]["home"]["added_starters"])
        self.assertIn("FWD1", delta["xi_changes"]["home"]["removed_starters"])
        self.assertEqual(delta["material_change"], "YES")

if __name__ == "__main__":
    unittest.main()
