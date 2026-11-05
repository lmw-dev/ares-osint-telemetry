#!/usr/bin/env python3
"""
Unit tests for FotMob identity binding (v1.1 Acceptance):
A. correct FotMob identity binding
B. project ID != FotMob ID
C. home/away naming normalization
C2. entity tokens preserved (e.g. city vs united)
D. kickoff timezone normalization
D2. kickoff matching tolerance (>12h rejection)
E. ambiguous match rejection
F. existing binding reuse
"""

import unittest
import sys
from pathlib import Path

SKILL_SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SKILL_SCRIPTS))

from fotmob_binding import normalize_name, team_name_similarity, resolve_fotmob_binding, evaluate_team_match

class TestFotMobBinding(unittest.TestCase):

    def test_A_correct_fotmob_identity_binding(self):
        """Test A: Correct FotMob canonical identity binding for Real Madrid vs Atletico Madrid"""
        res = resolve_fotmob_binding(
            project_fixture_id="3013703",
            league="LaLiga_GW07",
            home_team="马德里竞技",
            away_team="皇家马德里",
            kickoff_utc="2026-09-20T14:15:00Z"
        )
        self.assertIn(res["status"], ["PASS", "CAUTION"])
        self.assertEqual(str(res["fotmob_match_id"]), "5868072")
        self.assertIn("/matches/", res["canonical_url"])

    def test_B_project_id_not_fotmob_id(self):
        """Test B: Project fixture ID (Titan ID) is never used as FotMob match ID"""
        project_id = "3013703"
        res = resolve_fotmob_binding(
            project_fixture_id=project_id,
            league="LaLiga_GW07",
            home_team="马德里竞技",
            away_team="皇家马德里",
            kickoff_utc="2026-09-20T14:15:00Z"
        )
        self.assertNotEqual(str(res["fotmob_match_id"]), project_id)
        self.assertFalse(res["canonical_url"].endswith(f"/match/{project_id}"))

    def test_C_name_normalization(self):
        """Test C: Accent stripping and token extraction"""
        t1 = normalize_name("Atlético Madrid")
        t2 = normalize_name("Atletico")
        t3 = normalize_name("马德里竞技")
        self.assertIn("atletico", t1)
        self.assertIn("atletico", t2)
        self.assertIn("atletico", t3)
        self.assertGreaterEqual(team_name_similarity("Atlético Madrid", "马德里竞技"), 0.5)

    def test_C2_entity_tokens_preserved(self):
        """Test C2: Significant tokens like city and united are preserved, avoiding false merges"""
        m_city_tokens = normalize_name("Manchester City")
        m_utd_tokens = normalize_name("Manchester United")
        self.assertIn("city", m_city_tokens)
        self.assertIn("united", m_utd_tokens)
        self.assertNotEqual(m_city_tokens, m_utd_tokens)
        
        sim, prio = evaluate_team_match("Manchester City", "Manchester United")
        self.assertLess(sim, 0.90)

    def test_D_timezone_normalization(self):
        """Test D: Kickoff timezone string handling (UTC Z vs +00:00)"""
        res = resolve_fotmob_binding(
            project_fixture_id="3003889",
            league="EPL_GW05",
            home_team="伯恩茅斯",
            away_team="利物浦",
            kickoff_utc="2026-09-20T13:00:00+00:00"
        )
        self.assertIn(res["status"], ["PASS", "CAUTION"])
        self.assertEqual(str(res["fotmob_match_id"]), "5795455")

    def test_D2_kickoff_rejection_over_12h(self):
        """Test D2: Kickoff time difference over 12h is rejected unless reschedule is proven"""
        res = resolve_fotmob_binding(
            project_fixture_id="3003889",
            league="EPL_GW05",
            home_team="伯恩茅斯",
            away_team="利物浦",
            kickoff_utc="2026-09-22T13:00:00Z", # 48 hours off
            reschedule_proven=False
        )
        self.assertEqual(res["status"], "FAILED")
        self.assertEqual(res["error"], "FOTMOB_BINDING_FAILED")

    def test_E_ambiguous_match_rejection(self):
        """Test E: Non-existent or conflicting team names should fail gracefully"""
        res = resolve_fotmob_binding(
            project_fixture_id="9999999",
            league="EPL",
            home_team="NonExistentFC",
            away_team="AlienInvaders",
            kickoff_utc="2026-09-20T13:00:00Z"
        )
        self.assertEqual(res["status"], "FAILED")
        self.assertEqual(res["error"], "FOTMOB_BINDING_FAILED")

    def test_F_existing_binding_reuse(self):
        """Test F: Existing binding is reused without re-querying"""
        existing = {
            "status": "PASS",
            "canonical_url": "https://www.fotmob.com/matches/test/123",
            "fotmob_match_id": "123",
            "fotmob_slug": "test",
            "matched_home": "Team A",
            "matched_away": "Team B",
            "matched_kickoff": "2026-09-20T13:00:00Z",
            "confidence": 1.0
        }
        res = resolve_fotmob_binding(
            project_fixture_id="11111",
            league="EPL",
            home_team="Team A",
            away_team="Team B",
            kickoff_utc="2026-09-20T13:00:00Z",
            existing_binding=existing
        )
        self.assertTrue(res["reused_binding"])
        self.assertEqual(res["fotmob_match_id"], "123")
        self.assertEqual(res["canonical_url"], "https://www.fotmob.com/matches/test/123")

if __name__ == "__main__":
    unittest.main()
