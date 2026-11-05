#!/usr/bin/env python3
"""
Unit tests for FotMob roster & news parsing (v1.1 Acceptance):
G. predicted XI parsed when not confirmed
H. confirmed XI parsed when confirmed
I. injured / doubtful / suspended / back in training parsed
J. empty injury list handled safely
O. post-kickoff locked status logic
P. no betting decision leakage
Q. hard assertion: match ID mismatch triggers FOTMOB_PAGE_IDENTITY_MISMATCH
R. news evidence semantics: publisher, headline, source_url, published_at, official
"""

import unittest
import sys
from pathlib import Path
from unittest.mock import patch

SKILL_SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SKILL_SCRIPTS))

from fotmob_roster_collector import (
    parse_roster_snapshot,
    parse_news_snapshot,
    check_lifecycle,
    collect_fixture_roster
)

class TestRosterParsing(unittest.TestCase):

    def test_G_predicted_xi_parsing(self):
        """Test G: Predicted XI correctly extracted and status set to PREDICTED"""
        dummy_page_props = {
            "content": {
                "lineup": {
                    "lineupType": "predicted",
                    "homeTeam": {
                        "formation": "4-3-3",
                        "starters": [
                            {"name": "Player 1", "id": 101, "shirt": "1", "position": "GK"},
                            {"name": "Player 2", "id": 102, "shirt": "2", "position": "DEF"}
                        ],
                        "subs": [],
                        "unavailable": []
                    },
                    "awayTeam": {
                        "formation": "4-4-2",
                        "starters": [
                            {"name": "Away Player 1", "id": 201, "shirt": "1", "position": "GK"}
                        ],
                        "subs": [],
                        "unavailable": []
                    }
                }
            }
        }
        res = parse_roster_snapshot(dummy_page_props, "https://www.fotmob.com/test", "123", "2026-09-20T10:00:00Z", "2026-09-20 18:00:00 CST")
        self.assertEqual(res["lineup_status"], "PREDICTED")
        self.assertEqual(len(res["predicted_xi_home"]), 2)
        self.assertEqual(len(res["confirmed_xi_home"]), 0)
        self.assertEqual(res["home_formation"], "4-3-3")

    def test_H_confirmed_xi_parsing(self):
        """Test H: Confirmed XI correctly extracted and status set to CONFIRMED"""
        starters_11 = [{"name": f"P{i}", "id": i, "shirt": str(i)} for i in range(1, 12)]
        dummy_page_props = {
            "content": {
                "lineup": {
                    "lineupType": "confirmed",
                    "homeTeam": {
                        "formation": "4-2-3-1",
                        "starters": starters_11,
                        "subs": [],
                        "unavailable": []
                    },
                    "awayTeam": {
                        "formation": "4-3-3",
                        "starters": starters_11,
                        "subs": [],
                        "unavailable": []
                    }
                }
            }
        }
        res = parse_roster_snapshot(dummy_page_props, "https://www.fotmob.com/test", "123", "2026-09-20T10:00:00Z", "2026-09-20 18:00:00 CST")
        self.assertEqual(res["lineup_status"], "CONFIRMED")
        self.assertEqual(len(res["confirmed_xi_home"]), 11)
        self.assertEqual(len(res["predicted_xi_home"]), 0)

    def test_I_injured_suspended_parsing(self):
        """Test I: Injured, suspended, doubtful, and back in training categorization"""
        dummy_page_props = {
            "content": {
                "lineup": {
                    "lineupType": "standard",
                    "homeTeam": {
                        "formation": "4-3-3",
                        "starters": [],
                        "unavailable": [
                            {"name": "Injured Star", "id": 1, "unavailability": {"type": "injury", "expectedReturn": "October 2026"}},
                            {"name": "Suspended Mid", "id": 2, "unavailability": {"type": "suspension", "expectedReturn": "Next match"}},
                            {"name": "Returning Defender", "id": 3, "unavailability": {"type": "injury", "expectedReturn": "Back in training"}}
                        ]
                    },
                    "awayTeam": {"starters": [], "unavailable": []}
                }
            }
        }
        res = parse_roster_snapshot(dummy_page_props, "https://www.fotmob.com/test", "123", "2026-09-20T10:00:00Z", "2026-09-20 18:00:00 CST")
        self.assertEqual(len(res["injured_home"]), 2)
        self.assertEqual(len(res["suspended_home"]), 1)
        self.assertEqual(len(res["back_in_training_home"]), 1)
        self.assertEqual(len(res["unavailable_home"]), 3)
        self.assertEqual(res["injured_home"][0]["player_name"], "Injured Star")
        self.assertEqual(res["suspended_home"][0]["player_name"], "Suspended Mid")

    def test_J_empty_injury_list(self):
        """Test J: Empty injury list parses safely without crashing"""
        dummy_page_props = {
            "content": {
                "lineup": {
                    "lineupType": "standard",
                    "homeTeam": {"starters": [], "unavailable": []},
                    "awayTeam": {"starters": [], "unavailable": []}
                }
            }
        }
        res = parse_roster_snapshot(dummy_page_props, "https://www.fotmob.com/test", "123", "2026-09-20T10:00:00Z", "2026-09-20 18:00:00 CST")
        self.assertEqual(res["injured_home"], [])
        self.assertEqual(res["unavailable_home"], [])

    def test_O_post_kickoff_lock(self):
        """Test O: Past kickoff matches are marked POST_KICKOFF_LOCKED and not fetched"""
        past_ko = "2026-09-10T12:00:00Z"
        lifecycle, usable, mins = check_lifecycle(past_ko, "PREMATCH")
        self.assertEqual(lifecycle, "POST_KICKOFF_LOCKED")
        self.assertFalse(usable)
        self.assertLess(mins, 0)

    def test_P_no_market_judgment_leakage(self):
        """Test P: Roster snapshot outputs contain zero market or betting fields"""
        dummy_page_props = {
            "content": {
                "lineup": {
                    "lineupType": "standard",
                    "homeTeam": {"starters": [], "unavailable": []},
                    "awayTeam": {"starters": [], "unavailable": []}
                }
            }
        }
        res = parse_roster_snapshot(dummy_page_props, "https://www.fotmob.com/test", "123", "2026-09-20T10:00:00Z", "2026-09-20 18:00:00 CST")
        forbidden_keys = {"value", "bet", "lean", "edge", "odds", "fair_line", "handicap"}
        found_forbidden = [k for k in res.keys() if k.lower() in forbidden_keys]
        self.assertEqual(found_forbidden, [])

    def test_Q_match_id_hard_assertion_mismatch(self):
        """Test Q: Hard assertion blocks execution if parsed_fotmob_match_id != bound_fotmob_match_id"""
        existing_binding = {
            "status": "PASS",
            "canonical_url": "https://www.fotmob.com/matches/test/555555",
            "fotmob_match_id": "555555",
            "fotmob_slug": "test",
            "confidence": 1.0
        }
        # Mock fetch_match_page_with_fallback to return a mismatched matchId 999999
        mismatched_props = {
            "general": {"matchId": 999999, "homeTeam": {"name": "Team A"}, "awayTeam": {"name": "Team B"}},
            "content": {"lineup": {"lineupType": "standard"}}
        }
        with patch("fotmob_roster_collector.fetch_match_page_with_fallback", return_value=(mismatched_props, "https://www.fotmob.com/matches/test/555555", "PASS")):
            res = collect_fixture_roster({
                "project_fixture_id": "12345",
                "league": "EPL",
                "home_team": "Team A",
                "away_team": "Team B",
                "kickoff_utc": "2026-10-01T12:00:00Z",
                "force_fetch_post_kickoff": True,
                "existing_binding": existing_binding
            })
            self.assertEqual(res["collector_status"], "FAILED")
            self.assertEqual(res["error"], "FOTMOB_PAGE_IDENTITY_MISMATCH")
            self.assertEqual(res["identity_assertion"], "FAIL")

    def test_R_news_semantics_structure(self):
        """Test R: News evidence extracted with publisher, headline, source_url, published_at, official"""
        dummy_page_props = {
            "content": {
                "matchFacts": {
                    "preReview": [
                        {
                            "title": "Tactical Preview",
                            "description": "Short summary of the derby",
                            "source": "Opta",
                            "shareUrl": "https://www.fotmob.com/news/123",
                            "dateUpdated": "2026-09-20T08:00:00Z"
                        }
                    ]
                }
            }
        }
        news_res = parse_news_snapshot(dummy_page_props, "https://www.fotmob.com/matches/test/123", "123", "2026-09-20T10:00:00Z")
        self.assertTrue(news_res["preview_available"])
        self.assertEqual(len(news_res["relevant_items"]), 1)
        item = news_res["relevant_items"][0]
        self.assertEqual(item["publisher"], "Opta")
        self.assertEqual(item["headline"], "Tactical Preview")
        self.assertEqual(item["source_url"], "https://www.fotmob.com/news/123")
        self.assertEqual(item["published_at"], "2026-09-20T08:00:00Z")
        self.assertFalse(item["official"])

if __name__ == "__main__":
    unittest.main()
