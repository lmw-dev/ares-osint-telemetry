#!/usr/bin/env python3
"""
Ares Postmatch Evidence Collector Engine (v1.0)
Deterministic External Postmatch Collector for Ares OSINT Telemetry Pipeline.

Core Tenets:
- External evidence collection only, zero betting decisions or Fair Line output
- Support FULL_POSTMATCH and PROCESS_FILL_ONLY modes
- Multi-source priority: Understat (primary xG) -> FotMob (secondary xG fallback) -> UNKNOWN
- Cross-source validation (final score, teams, kickoff) with SOURCE_CONFLICT defense
- Strict distinction between artifact_gap and evidence_gap
"""

import os
import sys
import json
import time
import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List

SKILL_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = SKILL_DIR.parents[2]

# Ensure access to binding resolver
ROSTER_SKILL_SCRIPTS = REPO_ROOT / "src" / "skills" / "ares-fotmob-roster-collector" / "scripts"
if str(ROSTER_SKILL_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(ROSTER_SKILL_SCRIPTS))

CURRENT_SCRIPTS = SKILL_DIR / "scripts"
if str(CURRENT_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(CURRENT_SCRIPTS))

from fotmob_binding import (
    resolve_fotmob_binding,
    validate_anti_regression,
    ZH_NAME_MAP,
    normalize_name,
    team_name_similarity
)
from understat_client import fetch_understat_match_data
from fotmob_postmatch_client import fetch_fotmob_completed_page, parse_fotmob_completed_match

class FixtureIdentityDriftError(ValueError):
    """Raised when fixture identity drifts from frozen authoritative input manifest."""
    pass

def assert_fixture_identity_lock(
    fixture_input: Dict[str, Any],
    bound_fotmob_id: str,
    fm_parsed: Optional[Dict[str, Any]] = None,
    page_props: Optional[Dict[str, Any]] = None
) -> None:
    """
    Production-grade manifest-driven Identity Lock assertion:
    Asserts that the resolved/fetched FotMob match strictly corresponds to the
    immutable fixture identity defined in fixture_input.
    """
    proj_id = str(fixture_input.get("project_fixture_id", ""))
    in_home = fixture_input.get("home_team", "")
    in_away = fixture_input.get("away_team", "")
    in_kickoff = fixture_input.get("kickoff_utc", "")
    prebound_fotmob_id = fixture_input.get("fotmob_match_id")

    # 1. Pre-bound ID check (if provided in input manifest)
    if prebound_fotmob_id and str(bound_fotmob_id) != str(prebound_fotmob_id):
        raise FixtureIdentityDriftError(
            f"FIXTURE_IDENTITY_DRIFT: Fixture {proj_id} bound FotMob ID '{bound_fotmob_id}' does not match manifest '{prebound_fotmob_id}'"
        )

    # 2. Parsed match ID check
    if fm_parsed:
        parsed_mid = str(fm_parsed.get("fotmob_match_id") or "")
        if parsed_mid and parsed_mid != str(bound_fotmob_id):
            raise FixtureIdentityDriftError(
                f"FIXTURE_IDENTITY_DRIFT: Fixture {proj_id} parsed FotMob match ID '{parsed_mid}' != bound ID '{bound_fotmob_id}'"
            )

        # 3. Entity check: Home & Away team correspondence
        p_home = fm_parsed.get("home_team", "")
        p_away = fm_parsed.get("away_team", "")

        def _teams_match(target_zh_or_en: str, parsed_en: str) -> bool:
            if not target_zh_or_en or not parsed_en:
                return False
            sim = team_name_similarity(target_zh_or_en, parsed_en)
            return sim >= 0.5

        if p_home and not _teams_match(in_home, p_home):
            raise FixtureIdentityDriftError(
                f"FIXTURE_IDENTITY_DRIFT: Fixture {proj_id} parsed home '{p_home}' does not match frozen home '{in_home}'"
            )

        if p_away and not _teams_match(in_away, p_away):
            raise FixtureIdentityDriftError(
                f"FIXTURE_IDENTITY_DRIFT: Fixture {proj_id} parsed away '{p_away}' does not match frozen away '{in_away}'"
            )

    # 4. Kickoff date check (from page_props if present)
    if page_props and in_kickoff:
        general = page_props.get("general", {})
        header = page_props.get("header", {})
        utc_time_str = header.get("status", {}).get("utcTime") or general.get("matchTimeUTCDate")
        if utc_time_str:
            try:
                dt_in = datetime.fromisoformat(in_kickoff.replace("Z", "+00:00"))
                dt_parsed = datetime.fromisoformat(utc_time_str.replace("Z", "+00:00"))
                diff_hours = abs((dt_in - dt_parsed).total_seconds()) / 3600.0
                if diff_hours > 24.0:
                    raise FixtureIdentityDriftError(
                        f"FIXTURE_IDENTITY_DRIFT: Fixture {proj_id} kickoff '{utc_time_str}' differs by {diff_hours:.1f}h from frozen '{in_kickoff}'"
                    )
            except Exception as ex:
                if isinstance(ex, FixtureIdentityDriftError):
                    raise ex
                pass

class PostmatchCollector:

    def __init__(self, output_base_dir: Optional[str] = None):
        self.output_base_dir = Path(output_base_dir) if output_base_dir else None
        self.network_request_count = 0

    def collect_fixture(
        self,
        fixture_input: Dict[str, Any],
        mode: str = "FULL_POSTMATCH"
    ) -> Dict[str, Any]:
        """
        Collects deterministic postmatch evidence for a single fixture.
        Modes: FULL_POSTMATCH or PROCESS_FILL_ONLY.
        """
        start_time = time.time()
        proj_id = str(fixture_input["project_fixture_id"])
        league = fixture_input["league"]
        home_team = fixture_input["home_team"]
        away_team = fixture_input["away_team"]
        kickoff_utc = fixture_input["kickoff_utc"]
        existing_binding = fixture_input.get("existing_binding")
        explicit_understat_id = fixture_input.get("understat_match_id")

        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        out_dir = Path(fixture_input["output_dir"]) if fixture_input.get("output_dir") else (
            self.output_base_dir / proj_id if self.output_base_dir else None
        )

        sources_used = []
        evidence_gap = []

        # 1. Resolve FotMob identity binding
        if existing_binding and existing_binding.get("fotmob_match_id"):
            fotmob_binding_res = existing_binding
            reused_binding = True
        else:
            self.network_request_count += 1
            fotmob_binding_res = resolve_fotmob_binding(
                project_fixture_id=proj_id,
                league=league,
                home_team=home_team,
                away_team=away_team,
                kickoff_utc=kickoff_utc,
                reschedule_proven=fixture_input.get("reschedule_proven", False)
            )
            reused_binding = False

        if fotmob_binding_res.get("status") not in ["PASS", "CAUTION"]:
            return {
                "project_fixture_id": proj_id,
                "overall_postmatch_evidence_status": "UNKNOWN",
                "error": "FOTMOB_BINDING_FAILED",
                "error_detail": fotmob_binding_res.get("error_detail"),
                "artifact_gap": ["00-postmatch-identity.json"],
                "evidence_gap": ["FotMob identity binding failed"]
            }

        bound_fotmob_id = str(fotmob_binding_res["fotmob_match_id"])
        canonical_fotmob_url = fotmob_binding_res["canonical_url"]
        validate_anti_regression(proj_id, canonical_fotmob_url)

        # Assert Pre-Fetch Identity Lock
        assert_fixture_identity_lock(
            fixture_input=fixture_input,
            bound_fotmob_id=bound_fotmob_id
        )

        # 2. Fetch FotMob completed match data
        self.network_request_count += 1
        page_props, final_url, fm_fetch_status = fetch_fotmob_completed_page(bound_fotmob_id, canonical_fotmob_url)

        if not page_props:
            return {
                "project_fixture_id": proj_id,
                "overall_postmatch_evidence_status": "UNKNOWN",
                "error": "FOTMOB_SOURCE_UNAVAILABLE",
                "error_detail": f"Failed to fetch FotMob page for match {bound_fotmob_id}",
                "artifact_gap": ["01-result-evidence.json", "02-lineup-events.json", "03-process-stats.json"],
                "evidence_gap": ["FotMob completed match page unavailable"]
            }

        fm_parsed = parse_fotmob_completed_match(page_props, canonical_fotmob_url, bound_fotmob_id)

        if fm_parsed.get("status") == "SOURCE_IDENTITY_MISMATCH":
            return {
                "project_fixture_id": proj_id,
                "overall_postmatch_evidence_status": "UNKNOWN",
                "error": "SOURCE_IDENTITY_MISMATCH",
                "error_detail": fm_parsed.get("error"),
                "evidence_gap": ["FotMob parsed match ID does not match bound ID"]
            }

        if fm_parsed.get("status") == "NON_FINAL_MATCH":
            return {
                "project_fixture_id": proj_id,
                "overall_postmatch_evidence_status": "UNKNOWN",
                "lifecycle": fm_parsed.get("lifecycle"),
                "error": "MATCH_NOT_COMPLETED",
                "error_detail": f"Match status is {fm_parsed.get('lifecycle')} ({fm_parsed.get('reason')})",
                "evidence_gap": ["Match has not concluded final whistle"]
            }

        # Assert Post-Fetch Identity Lock (verifies parsed teams, date, and FotMob match ID)
        assert_fixture_identity_lock(
            fixture_input=fixture_input,
            bound_fotmob_id=bound_fotmob_id,
            fm_parsed=fm_parsed,
            page_props=page_props
        )

        sources_used.append({
            "source_name": "FotMob",
            "source_url": canonical_fotmob_url,
            "retrieved_at_utc": fm_parsed["retrieved_at_utc"]
        })

        res_ev = fm_parsed["result_evidence"]
        lineup_ev = fm_parsed["lineup_events_evidence"]
        stats_ev = fm_parsed["process_stats_evidence"]
        fm_xg_ev = fm_parsed["xg_process_evidence"]

        # 3. Fetch Understat completed match data (Primary xG Source)
        self.network_request_count += 1
        understat_res = fetch_understat_match_data(
            league=league,
            home_team=home_team,
            away_team=away_team,
            kickoff_utc=kickoff_utc,
            explicit_match_id=explicit_understat_id
        )

        canonical_xg_home = None
        canonical_xg_away = None
        canonical_xg_source = "UNKNOWN"
        understat_used = False

        fotmob_aggregate_xg_home = fm_xg_ev.get("fotmob_aggregate_xg_home")
        fotmob_aggregate_xg_away = fm_xg_ev.get("fotmob_aggregate_xg_away")
        derived_shotmap_xg_home = fm_xg_ev.get("derived_shotmap_xg_home")
        derived_shotmap_xg_away = fm_xg_ev.get("derived_shotmap_xg_away")

        shotmap_xg_completeness = fm_xg_ev.get("shotmap_xg_completeness") or {}
        npxg_completeness = fm_xg_ev.get("npxg_completeness") or {}

        shotmap_h_comp = shotmap_xg_completeness.get("home", {}).get("is_complete", True)
        shotmap_a_comp = shotmap_xg_completeness.get("away", {}).get("is_complete", True)
        shotmap_xg_is_complete = bool(shotmap_h_comp and shotmap_a_comp)

        npxg_h_comp = npxg_completeness.get("home", {}).get("is_complete", True)
        npxg_a_comp = npxg_completeness.get("away", {}).get("is_complete", True)
        npxg_is_complete = bool(npxg_h_comp and npxg_a_comp)

        xg_data_is_complete = bool(shotmap_xg_is_complete and npxg_is_complete)

        canonical_shots_home = stats_ev.get("canonical_shots_home") if stats_ev.get("canonical_shots_home") is not None else stats_ev.get("shots_home")
        canonical_shots_away = stats_ev.get("canonical_shots_away") if stats_ev.get("canonical_shots_away") is not None else stats_ev.get("shots_away")
        shotmap_event_count_home = fm_xg_ev.get("shotmap_event_count_home", 0)
        shotmap_event_count_away = fm_xg_ev.get("shotmap_event_count_away", 0)

        # Understat check
        understat_valid = (
            understat_res.get("status") == "PASS" and
            understat_res.get("xg_home") is not None and
            understat_res.get("xg_away") is not None
        )

        if understat_valid:
            # Cross-source score validation
            u_h_score = understat_res.get("home_score")
            u_a_score = understat_res.get("away_score")
            if u_h_score is not None and u_a_score is not None:
                if u_h_score != res_ev["final_score_home"] or u_a_score != res_ev["final_score_away"]:
                    return {
                        "project_fixture_id": proj_id,
                        "overall_postmatch_evidence_status": "UNKNOWN",
                        "error": "SOURCE_CONFLICT",
                        "error_detail": f"Score conflict: FotMob ({res_ev['final_score_str']}) vs Understat ({u_h_score}-{u_a_score})",
                        "evidence_gap": ["Cross-source score discrepancy detected"]
                    }

            sources_used.append({
                "source_name": "Understat",
                "source_url": understat_res["canonical_url"],
                "retrieved_at_utc": understat_res["retrieved_at_utc"]
            })
            canonical_xg_home = understat_res["xg_home"]
            canonical_xg_away = understat_res["xg_away"]
            canonical_xg_source = "Understat"
            understat_used = True
            all_shots = understat_res.get("all_shots_chronological", [])
            xg_timeline = understat_res.get("xg_timeline", [])
            shots_cnt = len(all_shots)
            shot_map_avail = bool(all_shots)
        elif fotmob_aggregate_xg_home is not None and fotmob_aggregate_xg_away is not None:
            # Rule 2: FotMob aggregate
            canonical_xg_home = fotmob_aggregate_xg_home
            canonical_xg_away = fotmob_aggregate_xg_away
            canonical_xg_source = "FotMob-Aggregate"
            all_shots = fm_xg_ev.get("all_shots_chronological", [])
            xg_timeline = fm_xg_ev.get("xg_timeline", [])
            shots_cnt = fm_xg_ev.get("shots_count", 0)
            shot_map_avail = fm_xg_ev.get("shot_map_available", False)
        elif derived_shotmap_xg_home is not None and derived_shotmap_xg_away is not None:
            # Rule 3: Derived shot-map sum fallback
            # 数据不完整时，不得把部分合计当成完整汇总！缺失则标 UNKNOWN
            if shotmap_xg_is_complete:
                canonical_xg_home = derived_shotmap_xg_home
                canonical_xg_away = derived_shotmap_xg_away
                canonical_xg_source = "FOTMOB_SHOTMAP_DERIVED_FALLBACK"
            else:
                canonical_xg_home = None
                canonical_xg_away = None
                canonical_xg_source = "UNKNOWN"
                evidence_gap.append("Source aggregate xG missing and derived shotmap xG is incomplete; cannot substitute partial sum as full aggregate")
            all_shots = fm_xg_ev.get("all_shots_chronological", [])
            xg_timeline = fm_xg_ev.get("xg_timeline", [])
            shots_cnt = fm_xg_ev.get("shots_count", 0)
            shot_map_avail = fm_xg_ev.get("shot_map_available", False)
        else:
            evidence_gap.append("xG and shot map data unavailable from both Understat and FotMob")
            all_shots = []
            xg_timeline = []
            shots_cnt = 0
            shot_map_avail = False

        # Calculate consistency delta against derived shot-map sum
        delta_h = None
        delta_a = None
        xg_consistency = "UNKNOWN"

        can_compare = (
            canonical_xg_home is not None and 
            canonical_xg_away is not None and 
            derived_shotmap_xg_home is not None and 
            derived_shotmap_xg_away is not None
        )

        if can_compare:
            delta_h = round(abs(canonical_xg_home - derived_shotmap_xg_home), 4)
            delta_a = round(abs(canonical_xg_away - derived_shotmap_xg_away), 4)

            has_explicit_completeness_flag = (
                bool(shotmap_xg_completeness) and 
                "home" in shotmap_xg_completeness and 
                "away" in shotmap_xg_completeness and
                "is_complete" in shotmap_xg_completeness["home"] and
                "is_complete" in shotmap_xg_completeness["away"]
            )

            if not has_explicit_completeness_flag:
                # 完整性标记缺失，无法进行有效比较，不得显示 PASS
                xg_consistency = "UNKNOWN"
            elif not shotmap_xg_is_complete:
                # 逐射数据不完整，不得显示 PASS；标为 CAUTION
                xg_consistency = "CAUTION"
            else:
                # 逐射数据完整且存在有效比较：根据误差阈值判定
                if max(delta_h, delta_a) <= 0.03:
                    xg_consistency = "PASS"
                else:
                    xg_consistency = "CAUTION"
        else:
            # 无法进行有效比较（缺少官方汇总或逐射数据），标为 UNKNOWN
            xg_consistency = "UNKNOWN"

        final_xg_data = {
            "canonical_xg_home": canonical_xg_home,
            "canonical_xg_away": canonical_xg_away,
            "canonical_xg_source": canonical_xg_source,
            "xg_home": canonical_xg_home,
            "xg_away": canonical_xg_away,
            "xg_source": canonical_xg_source,
            "fotmob_aggregate_xg_home": fotmob_aggregate_xg_home,
            "fotmob_aggregate_xg_away": fotmob_aggregate_xg_away,
            "fotmob_aggregate_npxg_home": fm_xg_ev.get("fotmob_aggregate_npxg_home"),
            "fotmob_aggregate_npxg_away": fm_xg_ev.get("fotmob_aggregate_npxg_away"),
            "derived_shotmap_xg_home": derived_shotmap_xg_home,
            "derived_shotmap_xg_away": derived_shotmap_xg_away,
            "derived_npxg_home": fm_xg_ev.get("derived_npxg_home"),
            "derived_npxg_away": fm_xg_ev.get("derived_npxg_away"),
            "shotmap_xg_completeness": shotmap_xg_completeness,
            "npxg_completeness": npxg_completeness,
            "shotmap_xg_is_complete": shotmap_xg_is_complete,
            "npxg_is_complete": npxg_is_complete,
            "known_shotmap_xg_home": derived_shotmap_xg_home,
            "known_shotmap_xg_away": derived_shotmap_xg_away,
            "known_npxg_home": fm_xg_ev.get("derived_npxg_home"),
            "known_npxg_away": fm_xg_ev.get("derived_npxg_away"),
            "missing_shot_xg_count_home": shotmap_xg_completeness.get("home", {}).get("missing_xg_count", 0),
            "missing_shot_xg_count_away": shotmap_xg_completeness.get("away", {}).get("missing_xg_count", 0),
            "missing_penalty_type_count_home": npxg_completeness.get("home", {}).get("missing_penalty_type_count", 0),
            "missing_penalty_type_count_away": npxg_completeness.get("away", {}).get("missing_penalty_type_count", 0),
            "penalty_shots_home": fm_xg_ev.get("penalty_shots_home", 0),
            "penalty_shots_away": fm_xg_ev.get("penalty_shots_away", 0),
            "penalty_xg_home": fm_xg_ev.get("penalty_xg_home", 0.0),
            "penalty_xg_away": fm_xg_ev.get("penalty_xg_away", 0.0),
            "xg_consistency_delta_home": delta_h,
            "xg_consistency_delta_away": delta_a,
            "xg_consistency": xg_consistency,
            "canonical_shots_home": canonical_shots_home,
            "canonical_shots_away": canonical_shots_away,
            "shotmap_event_count_home": shotmap_event_count_home,
            "shotmap_event_count_away": shotmap_event_count_away,
            "shots_count": shots_cnt,
            "shot_map_available": shot_map_avail,
            "all_shots_chronological": all_shots,
            "xg_timeline": xg_timeline,
            "red_card_split": fm_xg_ev.get("red_card_split"),
            "game_state_split": fm_xg_ev.get("game_state_split"),
            "boundary_limitations": fm_xg_ev.get("boundary_limitations"),
            "momentum_available": fm_xg_ev.get("momentum_available", False),
            "understat_status": understat_res.get("status"),
            "understat_fallback_reason": understat_res.get("error") if not understat_valid else None,
            "source_url": canonical_fotmob_url if canonical_xg_source != "Understat" else understat_res.get("canonical_url"),
            "retrieved_at_utc": now_utc
        }

        # 4. Evidence Status Assessment
        # Result status
        if res_ev.get("final_score_home") is not None and res_ev.get("final_score_away") is not None:
            result_status = "PASS"
        else:
            result_status = "UNKNOWN"
            evidence_gap.append("Final score incomplete")

        # Lineup status
        if lineup_ev.get("lineup_status") == "CONFIRMED" and len(lineup_ev["home_xi"]) == 11 and len(lineup_ev["away_xi"]) == 11:
            lineup_status = "PASS"
        elif len(lineup_ev["home_xi"]) > 0:
            lineup_status = "PARTIAL"
            evidence_gap.append("Confirmed 11 starters partially missing")
        else:
            lineup_status = "UNKNOWN"
            evidence_gap.append("Lineup completely unavailable")

        # Process stats status
        has_core_stats = (
            stats_ev.get("shots_home") is not None and
            stats_ev.get("shots_on_target_home") is not None and
            stats_ev.get("possession_home") is not None
        )
        if has_core_stats:
            stats_status = "PASS"
        else:
            stats_status = "PARTIAL"
            evidence_gap.append("Core process stats (shots/SOT/possession) incomplete")

        # Record xG completeness gaps
        if not shotmap_xg_is_complete:
            evidence_gap.append(f"Shotmap individual xG incomplete: home missing {shotmap_xg_completeness.get('home', {}).get('missing_xg_count', 0)}, away missing {shotmap_xg_completeness.get('away', {}).get('missing_xg_count', 0)}")
        if not npxg_is_complete:
            evidence_gap.append(f"Non-penalty xG incomplete: home missing pen type {npxg_completeness.get('home', {}).get('missing_penalty_type_count', 0)}, away missing pen type {npxg_completeness.get('away', {}).get('missing_penalty_type_count', 0)}")

        # xG status: 数据不完整时，不得把部分合计当成完整汇总或用于完整性 PASS
        if canonical_xg_home is not None and canonical_xg_away is not None:
            if xg_data_is_complete:
                xg_status = "PASS"
            else:
                xg_status = "PARTIAL"
        else:
            xg_status = "UNKNOWN"

        # Overall Status
        if result_status == "PASS" and lineup_status == "PASS" and stats_status == "PASS" and xg_status == "PASS":
            overall_status = "PASS"
        elif result_status == "PASS" and stats_status in ["PASS", "PARTIAL"]:
            overall_status = "CAUTION"
        else:
            overall_status = "UNKNOWN"

        # Construct identity record
        identity_record = {
            "project_fixture_id": proj_id,
            "league": league,
            "home_team": home_team,
            "away_team": away_team,
            "kickoff_utc": kickoff_utc,
            "fotmob_match_id": bound_fotmob_id,
            "fotmob_canonical_url": canonical_fotmob_url,
            "understat_match_id": understat_res.get("understat_match_id"),
            "binding_reused": reused_binding,
            "lifecycle": "POSTMATCH_COMPLETE",
            "execution_usable": True,
            "collector_mode": mode,
            "retrieved_at_utc": now_utc
        }

        # Lineage record
        lineage_record = {
            "project_fixture_id": proj_id,
            "retrieved_at_utc": now_utc,
            "sources_used": sources_used,
            "cross_source_validation": {
                "score_validated": True if understat_used else "SINGLE_SOURCE_FOTMOB",
                "identity_validated": True
            }
        }

        # Artifacts to stage
        artifacts = {
            "00-postmatch-identity.json": identity_record,
            "01-result-evidence.json": res_ev,
            "02-lineup-events.json": lineup_ev,
            "03-process-stats.json": stats_ev,
            "04-xg-process.json": final_xg_data,
            "05-source-lineage.json": lineage_record
        }

        artifact_gap = []
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
            for fname, payload in artifacts.items():
                target_f = out_dir / fname
                try:
                    with open(target_f, "w", encoding="utf-8") as wf:
                        json.dump(payload, wf, ensure_ascii=False, indent=2)
                except Exception as e:
                    artifact_gap.append(fname)

            # Write manifest
            manifest = {
                "project_fixture_id": proj_id,
                "collector_mode": mode,
                "identity_assertion": "PASS",
                "overall_status": overall_status,
                "result_status": result_status,
                "lineup_status": lineup_status,
                "stats_status": stats_status,
                "xg_status": xg_status,
                "artifact_gap": artifact_gap,
                "evidence_gap": evidence_gap,
                "retrieved_at_utc": now_utc
            }
            with open(out_dir / "manifest.json", "w", encoding="utf-8") as wf:
                json.dump(manifest, wf, ensure_ascii=False, indent=2)

        elapsed = round(time.time() - start_time, 2)

        return {
            "project_fixture_id": proj_id,
            "home_team": home_team,
            "away_team": away_team,
            "kickoff_utc": kickoff_utc,
            "final_score": res_ev.get("final_score_str"),
            "fotmob_match_id": bound_fotmob_id,
            "fotmob_url": canonical_fotmob_url,
            "identity_assertion": "PASS",
            "fotmob_status": "PASS",
            "confirmed_xi_status": lineup_status,
            "events_status": "PASS" if lineup_ev.get("goals") is not None else "PARTIAL",
            "canonical_shots_home": canonical_shots_home,
            "canonical_shots_away": canonical_shots_away,
            "shotmap_event_count_home": shotmap_event_count_home,
            "shotmap_event_count_away": shotmap_event_count_away,
            "shots": f"H:{canonical_shots_home} / A:{canonical_shots_away}",
            "shots_on_target": f"H:{stats_ev.get('shots_on_target_home')} / A:{stats_ev.get('shots_on_target_away')}",
            "possession": f"H:{stats_ev.get('possession_home')}% / A:{stats_ev.get('possession_away')}%",
            "corners": f"H:{stats_ev.get('corners_home')} / A:{stats_ev.get('corners_away')}",
            "big_chances": f"H:{stats_ev.get('big_chances_home')} / A:{stats_ev.get('big_chances_away')}",
            "understat_match_id": understat_res.get("understat_match_id"),
            "understat_url": understat_res.get("canonical_url"),
            "canonical_xg_home": canonical_xg_home,
            "canonical_xg_away": canonical_xg_away,
            "canonical_xg_source": canonical_xg_source,
            "xg_home": canonical_xg_home,
            "xg_away": canonical_xg_away,
            "fotmob_aggregate_xg_home": fotmob_aggregate_xg_home,
            "fotmob_aggregate_xg_away": fotmob_aggregate_xg_away,
            "derived_shotmap_xg_home": derived_shotmap_xg_home,
            "derived_shotmap_xg_away": derived_shotmap_xg_away,
            "derived_npxg_home": fm_xg_ev.get("derived_npxg_home"),
            "derived_npxg_away": fm_xg_ev.get("derived_npxg_away"),
            "shotmap_xg_completeness": shotmap_xg_completeness,
            "npxg_completeness": npxg_completeness,
            "shotmap_xg_is_complete": shotmap_xg_is_complete,
            "npxg_is_complete": npxg_is_complete,
            "known_shotmap_xg_home": derived_shotmap_xg_home,
            "known_shotmap_xg_away": derived_shotmap_xg_away,
            "known_npxg_home": fm_xg_ev.get("derived_npxg_home"),
            "known_npxg_away": fm_xg_ev.get("derived_npxg_away"),
            "missing_shot_xg_count_home": shotmap_xg_completeness.get("home", {}).get("missing_xg_count", 0),
            "missing_shot_xg_count_away": shotmap_xg_completeness.get("away", {}).get("missing_xg_count", 0),
            "missing_penalty_type_count_home": npxg_completeness.get("home", {}).get("missing_penalty_type_count", 0),
            "missing_penalty_type_count_away": npxg_completeness.get("away", {}).get("missing_penalty_type_count", 0),
            "xg_consistency_delta_home": delta_h,
            "xg_consistency_delta_away": delta_a,
            "xg_consistency": xg_consistency,
            "understat_status": understat_res.get("status"),
            "xg_source": canonical_xg_source,
            "result_evidence_status": result_status,
            "lineup_evidence_status": lineup_status,
            "process_stats_status": stats_status,
            "xg_status": xg_status,
            "overall_postmatch_evidence_status": overall_status,
            "artifact_gap": artifact_gap,
            "evidence_gap": evidence_gap,
            "elapsed_seconds": elapsed,
            "retrieved_at_utc": now_utc
        }

    def collect_round_batch(
        self,
        fixtures_list: List[Dict[str, Any]],
        mode: str = "FULL_POSTMATCH",
        staging_dir: Optional[str] = None,
        round_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Runs batch collection across an authoritative round denominator."""
        round_start = time.time()
        staging_path = Path(staging_dir) if staging_dir else self.output_base_dir
        if staging_path:
            os.makedirs(staging_path, exist_ok=True)

        round_name = round_id or (fixtures_list[0].get("league") if fixtures_list else "W39_ROUND")

        results = []
        for fix in fixtures_list:
            target_out = (staging_path / str(fix["project_fixture_id"])) if staging_path else None
            fix_input = dict(fix)
            if target_out:
                fix_input["output_dir"] = str(target_out)
            try:
                r = self.collect_fixture(fix_input, mode=mode)
                results.append(r)
            except FixtureIdentityDriftError as e:
                # Critical identity drift: STOP entire batch immediately
                print(f"CRITICAL DRIFT DETECTED: {e}")
                raise e

        total_wall_clock = round(time.time() - round_start, 2)
        avg_per_fixture = round(total_wall_clock / len(fixtures_list), 2) if fixtures_list else 0.0

        # Aggregations
        art_comp_cnt = sum(1 for r in results if not r.get("artifact_gap"))
        res_comp_cnt = sum(1 for r in results if r.get("result_evidence_status") == "PASS")
        lin_comp_cnt = sum(1 for r in results if r.get("lineup_evidence_status") == "PASS")
        core_comp_cnt = sum(1 for r in results if r.get("process_stats_status") == "PASS")
        xg_comp_cnt = sum(1 for r in results if r.get("xg_status") == "PASS")
        xg_cons_pass = sum(1 for r in results if r.get("xg_consistency") == "PASS")
        xg_cons_caution = sum(1 for r in results if r.get("xg_consistency") == "CAUTION")
        ident_pass_cnt = sum(1 for r in results if r.get("identity_assertion") == "PASS")
        full_proc_cnt = sum(1 for r in results if r.get("overall_postmatch_evidence_status") == "PASS")

        all_evidence_gaps = []
        all_artifact_gaps = []
        for r in results:
            p_id = r["project_fixture_id"]
            for eg in r.get("evidence_gap", []):
                all_evidence_gaps.append(f"Fixture {p_id}: {eg}")
            for ag in r.get("artifact_gap", []):
                all_artifact_gaps.append(f"Fixture {p_id}: {ag}")

        summary = {
            "round_id": round_name,
            "collector_mode": mode,
            "total_denominator": len(fixtures_list),
            "processed_count": len(results),
            "artifact_complete_count": art_comp_cnt,
            "result_complete_count": res_comp_cnt,
            "lineup_complete_count": lin_comp_cnt,
            "core_stats_complete_count": core_comp_cnt,
            "xg_complete_count": xg_comp_cnt,
            "xg_consistency_pass_count": xg_cons_pass,
            "xg_consistency_caution_count": xg_cons_caution,
            "identity_complete_count": ident_pass_cnt,
            "full_process_complete_count": full_proc_cnt,
            "wall_clock_seconds": total_wall_clock,
            "average_per_fixture_seconds": avg_per_fixture,
            "network_request_count": self.network_request_count,
            "artifact_gaps": all_artifact_gaps,
            "evidence_gaps": all_evidence_gaps,
            "fixtures": results,
            "retrieved_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        }

        if staging_path:
            with open(staging_path / "00-ROUND-POSTMATCH-COLLECTOR-SUMMARY.json", "w", encoding="utf-8") as wf:
                json.dump(summary, wf, ensure_ascii=False, indent=2)

            # Write SHA256SUMS.txt for all files in staging_path
            sha_lines = []
            for root, _, files in os.walk(staging_path):
                for f in sorted(files):
                    if f in ["SHA256SUMS.txt"]:
                        continue
                    full_p = Path(root) / f
                    rel_p = full_p.relative_to(staging_path)
                    h = hashlib.sha256(full_p.read_bytes()).hexdigest()
                    sha_lines.append(f"{h}  {rel_p}")
            
            with open(staging_path / "SHA256SUMS.txt", "w", encoding="utf-8") as wf:
                wf.write("\n".join(sorted(sha_lines)) + "\n")

        return summary
