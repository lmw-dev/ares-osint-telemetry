#!/usr/bin/env python3
"""
Anti-regression guard test:
N. Assert that mechanically constructing https://www.fotmob.com/match/{project_fixture_id}
   is blocked or fails assertions.
"""

import unittest
import sys
from pathlib import Path

SKILL_SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SKILL_SCRIPTS))

from fotmob_binding import validate_anti_regression, resolve_fotmob_binding

class TestAntiRegressionGuard(unittest.TestCase):

    def test_N1_mechanical_construction_blocked_by_validator(self):
        """Test N1: validate_anti_regression raises ValueError if URL is mechanically constructed"""
        project_id = "3013703"
        illegal_url = f"https://www.fotmob.com/match/{project_id}"
        
        with self.assertRaises(ValueError) as ctx:
            validate_anti_regression(project_id, illegal_url)
        
        self.assertIn("REGRESSION DEFECT", str(ctx.exception))
        self.assertIn(project_id, str(ctx.exception))

    def test_N2_mechanical_slug_blocked_by_validator(self):
        """Test N2: validate_anti_regression raises ValueError if URL ends with /match/{project_id}"""
        project_id = "3003889"
        illegal_suffix_url = f"https://www.fotmob.com/api/match/{project_id}"
        
        with self.assertRaises(ValueError) as ctx:
            validate_anti_regression(project_id, illegal_suffix_url)
        
        self.assertIn("REGRESSION DEFECT", str(ctx.exception))

    def test_N3_valid_url_passes(self):
        """Test N3: Canonical FotMob URL with real match ID passes validation"""
        project_id = "3013703"
        valid_canonical_url = "https://www.fotmob.com/matches/real-madrid-vs-atletico-madrid/2ubozo#5868072"
        
        # Should not raise exception
        try:
            validate_anti_regression(project_id, valid_canonical_url)
        except ValueError:
            self.fail("validate_anti_regression raised ValueError unexpectedly on a valid canonical URL!")

    def test_N4_binding_resolver_rejects_mechanical_reuse(self):
        """Test N4: Existing binding containing mechanically forged URL is blocked upon resolution"""
        project_id = "3013703"
        tampered_binding = {
            "status": "PASS",
            "canonical_url": f"https://www.fotmob.com/match/{project_id}", # Defective mechanical URL
            "fotmob_match_id": project_id,
            "fotmob_slug": "defective-slug"
        }
        
        with self.assertRaises(ValueError):
            resolve_fotmob_binding(
                project_fixture_id=project_id,
                league="LaLiga",
                home_team="马德里竞技",
                away_team="皇家马德里",
                kickoff_utc="2026-09-20T14:15:00Z",
                existing_binding=tampered_binding
            )

if __name__ == "__main__":
    unittest.main()
