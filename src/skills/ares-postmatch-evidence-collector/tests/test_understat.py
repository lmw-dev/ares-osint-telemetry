#!/usr/bin/env python3
"""
Unit tests for Understat client:
F. Understat match binding
G. Understat shot parsing
"""

import unittest
import sys
from pathlib import Path

SKILL_SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SKILL_SCRIPTS))

from understat_client import parse_understat_match_page, fetch_understat_match_data

class TestUnderstatClient(unittest.TestCase):

    def setUp(self):
        # Sample Understat HTML snippet with hex-encoded JSON
        # Shots: home has 1 shot, away has 1 shot
        shots_json = '{"h":[{"id":"1","minute":"15","result":"Goal","X":"0.88","Y":"0.50","xG":"0.65","player":"Striker H","player_id":"11","situation":"OpenPlay","shotType":"RightFoot","player_assisted":"Mid H","lastAction":"Pass"}],"a":[{"id":"2","minute":"70","result":"SavedShot","X":"0.75","Y":"0.40","xG":"0.25","player":"Striker A","player_id":"22","situation":"OpenPlay","shotType":"LeftFoot","player_assisted":null,"lastAction":"None"}]}'
        escaped_shots = "".join(f"\\x{ord(c):02x}" for c in shots_json)
        match_info_json = '{"goals":{"h":"1","a":"0"}}'
        escaped_info = "".join(f"\\x{ord(c):02x}" for c in match_info_json)

        self.mock_html = f"""
        <html>
        <head><title>Match Test</title></head>
        <body>
        <script>var shotsData = JSON.parse('{escaped_shots}');</script>
        <script>var match_info = JSON.parse('{escaped_info}');</script>
        </body>
        </html>
        """

    def test_F_understat_match_binding(self):
        """Test F: Match binding handling when explicit ID is given or when source unavailable"""
        # When explicit ID is absent for 2026/27, it returns clean DATA_NOT_AVAILABLE without crashing
        res = fetch_understat_match_data("EPL", "Team A", "Team B", "2026-09-20T13:00:00Z")
        self.assertEqual(res["status"], "DATA_NOT_AVAILABLE")
        self.assertIsNone(res["understat_match_id"])

    def test_G_understat_shot_parsing(self):
        """Test G: Understat shot parsing, cumulative xG, and timeline extraction"""
        parsed = parse_understat_match_page(self.mock_html, "https://understat.com/match/12345")
        self.assertEqual(parsed["status"], "PASS")
        self.assertEqual(parsed["xg_home"], 0.65)
        self.assertEqual(parsed["xg_away"], 0.25)
        self.assertEqual(parsed["shots_count_home"], 1)
        self.assertEqual(parsed["shots_count_away"], 1)
        self.assertEqual(parsed["home_score"], 1)
        self.assertEqual(parsed["away_score"], 0)
        
        # Chronological timeline
        tl = parsed["xg_timeline"]
        self.assertEqual(len(tl), 2)
        self.assertEqual(tl[0]["minute"], 15)
        self.assertEqual(tl[0]["player"], "Striker H")
        self.assertEqual(tl[1]["minute"], 70)
        self.assertEqual(tl[1]["player"], "Striker A")

if __name__ == "__main__":
    unittest.main()
