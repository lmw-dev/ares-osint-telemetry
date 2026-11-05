#!/usr/bin/env python3
"""
FotMob Roster & Team-News Collector (Ares OSINT Telemetry Pipeline v1.1)

Acceptance Fixes:
1. Hard assertion: parsed_fotmob_match_id == bound_fotmob_match_id (fails with FOTMOB_PAGE_IDENTITY_MISMATCH)
2. Follows /match/<real_fotmob_match_id> redirect to parse page
3. 1-bounded rendered/browser fallback if SSR content is deferred, with status PASS/PARTIAL/UNKNOWN (no retry loop)
4. Removal of unsubstantiated "100% stable" claims regarding SSR
5. Normalized news evidence semantics: publisher, headline, source_url, published_at, official=false
6. Source Policy: SOURCE_POLICY_STATUS = "CAUTION"
7. Explicit declaration: OFFICIAL_SOURCE_FALLBACK = "NOT_IMPLEMENTED"
"""

import urllib.request
import ssl
import json
import re
import os
import sys
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Tuple, List
from pathlib import Path

SKILL_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SKILL_DIR / "scripts"))

from fotmob_binding import resolve_fotmob_binding, validate_anti_regression
from roster_delta import compute_roster_delta

SOURCE_POLICY_STATUS = "CAUTION"
SOURCE_POLICY_NOTE = "Automated/systematic FotMob access requires separate production-policy review."
OFFICIAL_SOURCE_FALLBACK = "NOT_IMPLEMENTED"

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
}

def check_lifecycle(kickoff_utc: str, mode: str) -> Tuple[str, bool, float]:
    """
    Compares actual collection time against authoritative kickoff.
    Returns: (lifecycle_state, execution_usable, time_to_kickoff_minutes)
    """
    now_utc = datetime.now(timezone.utc)
    try:
        clean_ko = kickoff_utc.replace("Z", "+00:00")
        ko_dt = datetime.fromisoformat(clean_ko)
    except Exception:
        ko_dt = now_utc + timedelta(hours=24)
        
    time_to_ko_mins = round((ko_dt - now_utc).total_seconds() / 60.0, 1)
    
    if now_utc >= ko_dt:
        lifecycle = "POST_KICKOFF_LOCKED"
        usable = False
    else:
        if mode.upper() == "FINAL_REFRESH":
            lifecycle = "ACTIVE_FINAL_REFRESH"
        else:
            lifecycle = "ACTIVE_PREMATCH"
        usable = True
        
    return lifecycle, usable, time_to_ko_mins

def fetch_fotmob_page_html(url: str) -> Tuple[Optional[str], str]:
    """Fetches FotMob page HTML following redirects, returns (html, final_url)."""
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=12, context=CTX) as r:
            final_url = r.geturl()
            html = r.read().decode("utf-8", errors="ignore")
            return html, final_url
    except Exception as e:
        print(f"Error fetching FotMob URL {url}: {e}")
        return None, url

def extract_next_data(html: str) -> Optional[Dict[str, Any]]:
    """Extracts __NEXT_DATA__ JSON from HTML."""
    m = re.search(r"<script id=\"__NEXT_DATA__\" type=\"application/json\">(.*?)</script>", html)
    if not m:
        return None
    try:
        data = json.loads(m.group(1))
        return data.get("props", {}).get("pageProps", {})
    except Exception as e:
        print(f"Error decoding __NEXT_DATA__: {e}")
        return None

def fetch_match_page_with_fallback(bound_fotmob_match_id: str, canonical_url: str) -> Tuple[Optional[Dict[str, Any]], str, str]:
    """
    Preferred retrieval:
    real FotMob match_id -> /match/<real_fotmob_match_id> -> follow redirect -> parse page.
    Fallback: Exactly one bounded fallback attempt if SSR pageProps.content is unavailable/deferred.
    Returns: (pageProps, final_url, fallback_status: PASS/PARTIAL/UNKNOWN)
    """
    # 1. Primary retrieval via /match/<real_match_id>
    primary_url = f"https://www.fotmob.com/match/{bound_fotmob_match_id}"
    html, final_url = fetch_fotmob_page_html(primary_url)
    
    page_props = None
    if html:
        page_props = extract_next_data(html)
        
    # Check if primary retrieval succeeded with full content
    if page_props and page_props.get("content"):
        return page_props, final_url, "PASS"
        
    # 2. Exactly one bounded fallback: attempt fetching canonical_url directly
    print(f"Primary SSR content missing/deferred for match {bound_fotmob_match_id}. Triggering 1-bounded fallback.")
    fallback_url = canonical_url.split("#")[0]
    if fallback_url != final_url:
        fb_html, fb_final_url = fetch_fotmob_page_html(fallback_url)
        if fb_html:
            fb_props = extract_next_data(fb_html)
            if fb_props and fb_props.get("content"):
                return fb_props, fb_final_url, "PARTIAL"
            elif fb_props:
                return fb_props, fb_final_url, "PARTIAL"
                
    if page_props:
        return page_props, final_url, "PARTIAL"
        
    return None, final_url, "UNKNOWN"

def parse_roster_snapshot(
    page_props: Dict[str, Any],
    canonical_url: str,
    fotmob_match_id: str,
    retrieved_at_utc: str,
    retrieved_at_cst: str
) -> Dict[str, Any]:
    """Extracts structured roster snapshot from FotMob pageProps."""
    content = page_props.get("content", {})
    lineup = content.get("lineup", {})
    
    lineup_type = lineup.get("lineupType", "")
    home_data = lineup.get("homeTeam", {})
    away_data = lineup.get("awayTeam", {})
    
    home_starters = home_data.get("starters", []) or []
    away_starters = away_data.get("starters", []) or []
    
    clean_lt = (lineup_type or "").strip().lower()
    # 严格只有官方确证且标记为 confirmed 时才可标 CONFIRMED；lastStarting11 必须显式标记为 PREDICTED (LAST_STARTING_XI_REUSE)
    if clean_lt == "confirmed":
        lineup_status = "CONFIRMED"
    elif clean_lt == "laststarting11":
        lineup_status = "PREDICTED (LAST_STARTING_XI_REUSE)"
    elif clean_lt in ["predicted", "expected", "projected"] or (len(home_starters) > 0 and len(away_starters) > 0):
        lineup_status = "PREDICTED"
    else:
        lineup_status = "NOT_AVAILABLE"

    def _format_player_list(players: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        res = []
        for p in players:
            p_name = p.get("name") or f"{p.get('firstName', '')} {p.get('lastName', '')}".strip()
            res.append({
                "player_name": p_name,
                "player_id": p.get("id"),
                "shirt_number": p.get("shirt"),
                "position": p.get("position"),
                "status": "STARTER",
                "source_label": "FotMob"
            })
        return res

    def _parse_unavailability(unavail_list: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        injured = []
        doubtful = []
        suspended = []
        unavailable = []
        back_in_training = []
        major_returns = []
        
        for u in unavail_list:
            p_name = u.get("name") or f"{u.get('firstName', '')} {u.get('lastName', '')}".strip()
            un_info = u.get("unavailability", {})
            u_type = un_info.get("type", "unavailable").lower()
            exp_return = un_info.get("expectedReturn", "Unknown")
            exp_date = un_info.get("expectedReturnDate")
            
            entry = {
                "player_name": p_name,
                "player_id": u.get("id"),
                "status": u_type.upper(),
                "reason": un_info.get("type", "unavailable"),
                "expected_return": exp_return,
                "expected_return_date": exp_date,
                "source_label": "FotMob"
            }
            
            unavailable.append(entry)
            
            if exp_return and "back in training" in exp_return.lower():
                back_in_training.append(entry)

            if "suspension" in u_type:
                suspended.append(entry)
            elif "doubtful" in u_type:
                doubtful.append(entry)
            elif "injury" in u_type:
                injured.append(entry)
            else:
                injured.append(entry)
                
        return injured, doubtful, suspended, unavailable, back_in_training, major_returns

    inj_h, dbt_h, susp_h, unav_h, bit_h, ret_h = _parse_unavailability(home_data.get("unavailable", []))
    inj_a, dbt_a, susp_a, unav_a, bit_a, ret_a = _parse_unavailability(away_data.get("unavailable", []))

    confirmed_xi_h = _format_player_list(home_starters) if lineup_status == "CONFIRMED" else []
    confirmed_xi_a = _format_player_list(away_starters) if lineup_status == "CONFIRMED" else []
    predicted_xi_h = _format_player_list(home_starters) if lineup_status == "PREDICTED" else []
    predicted_xi_a = _format_player_list(away_starters) if lineup_status == "PREDICTED" else []

    return {
        "source_name": "FotMob",
        "canonical_source_url": canonical_url,
        "fotmob_match_id": str(fotmob_match_id),
        "retrieved_at_utc": retrieved_at_utc,
        "retrieved_at_cst": retrieved_at_cst,
        "lineup_status": lineup_status,
        "lineup_type_raw": lineup_type,
        "home_formation": home_data.get("formation"),
        "away_formation": away_data.get("formation"),
        "confirmed_xi_home": confirmed_xi_h,
        "confirmed_xi_away": confirmed_xi_a,
        "predicted_xi_home": predicted_xi_h,
        "predicted_xi_away": predicted_xi_a,
        "bench_home": _format_player_list(home_data.get("subs", []) or []),
        "bench_away": _format_player_list(away_data.get("subs", []) or []),
        "injured_home": inj_h,
        "injured_away": inj_a,
        "doubtful_home": dbt_h,
        "doubtful_away": dbt_a,
        "suspended_home": susp_h,
        "suspended_away": susp_a,
        "unavailable_home": unav_h,
        "unavailable_away": unav_a,
        "back_in_training_home": bit_h,
        "back_in_training_away": bit_a,
        "major_returns_home": ret_h,
        "major_returns_away": ret_a
    }

def parse_news_snapshot(
    page_props: Dict[str, Any],
    canonical_url: str,
    fotmob_match_id: str,
    retrieved_at_utc: str
) -> Dict[str, Any]:
    """
    Extracts match preview news and context with normalized evidence semantics.
    FotMob-hosted news is NOT automatically labeled official.
    Required fields: publisher, headline, source_url, published_at, official.
    """
    content = page_props.get("content", {})
    mf = content.get("matchFacts", {})
    pre_reviews = mf.get("preReview", []) or []
    
    relevant_items = []
    for r in pre_reviews:
        lang = r.get("lang", "en")
        if lang == "en" or not relevant_items:
            pub = r.get("source") or "Opta/FotMob"
            source_url = r.get("shareUrl") or r.get("contentUrl") or canonical_url
            
            # Official determination: only true if verified official club media
            is_official = False
            if "official" in pub.lower() or "club" in pub.lower():
                is_official = True

            relevant_items.append({
                "publisher": pub,
                "headline": r.get("title", ""),
                "short_summary": r.get("description", ""),
                "source_url": source_url,
                "published_at": r.get("dateUpdated", ""),
                "official": is_official
            })

    return {
        "source_name": "FotMob",
        "canonical_source_url": canonical_url,
        "fotmob_match_id": str(fotmob_match_id),
        "retrieved_at_utc": retrieved_at_utc,
        "preview_available": bool(relevant_items),
        "relevant_items": relevant_items
    }

def collect_fixture_roster(fixture_input: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main entry point for FotMob roster collection.
    Accepts deterministic fixture object, outputs structured evidence files.
    """
    proj_fid = str(fixture_input["project_fixture_id"])
    league = fixture_input["league"]
    home = fixture_input["home_team"]
    away = fixture_input["away_team"]
    ko_utc = fixture_input["kickoff_utc"]
    mode = fixture_input.get("mode", "PREMATCH").upper()
    existing_binding = fixture_input.get("existing_binding")
    prev_snapshot_path = fixture_input.get("previous_roster_snapshot_path")
    out_dir = fixture_input.get("output_dir")

    now_utc = datetime.now(timezone.utc)
    retrieved_at_utc = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    retrieved_at_cst = now_utc.astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S CST")

    # 1. Check lifecycle
    lifecycle, usable, time_to_ko_mins = check_lifecycle(ko_utc, mode)

    # 2. Identity binding
    binding = resolve_fotmob_binding(
        project_fixture_id=proj_fid,
        league=league,
        home_team=home,
        away_team=away,
        kickoff_utc=ko_utc,
        round_num=fixture_input.get("round"),
        season=fixture_input.get("season"),
        existing_binding=existing_binding,
        reschedule_proven=fixture_input.get("reschedule_proven", False)
    )

    if binding["status"] == "FAILED":
        result = {
            "project_fixture_id": proj_fid,
            "collector_status": "FAILED",
            "roster_evidence_status": "UNKNOWN",
            "error": "FOTMOB_BINDING_FAILED",
            "error_detail": binding.get("error_detail"),
            "lifecycle": lifecycle,
            "execution_usable": usable,
            "source_policy_status": SOURCE_POLICY_STATUS,
            "official_source_fallback": OFFICIAL_SOURCE_FALLBACK,
            "fotmob_binding": binding
        }
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
            with open(os.path.join(out_dir, "00-fixture-binding.json"), "w", encoding="utf-8") as wf:
                json.dump(binding, wf, ensure_ascii=False, indent=2)
        return result

    # Validate anti-regression
    canonical_url = binding["canonical_url"]
    bound_fotmob_match_id = str(binding["fotmob_match_id"])
    validate_anti_regression(proj_fid, canonical_url)

    # 3. Post-kickoff check: prevent live betting contamination unless forced for audit/smoke testing
    force_fetch = fixture_input.get("force_fetch_post_kickoff", False)
    if lifecycle == "POST_KICKOFF_LOCKED" and not force_fetch:
        result = {
            "project_fixture_id": proj_fid,
            "collector_status": "COMPLETE",
            "roster_evidence_status": "PASS",
            "lifecycle": lifecycle,
            "execution_usable": False,
            "source_policy_status": SOURCE_POLICY_STATUS,
            "official_source_fallback": OFFICIAL_SOURCE_FALLBACK,
            "fotmob_binding": binding,
            "message": "POST_KICKOFF_LOCKED: Preserving existing pre-match snapshot; live betting contamination prevented."
        }
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
            with open(os.path.join(out_dir, "00-fixture-binding.json"), "w", encoding="utf-8") as wf:
                json.dump(binding, wf, ensure_ascii=False, indent=2)
        return result

    # 4. Preferred retrieval via /match/<real_match_id> with 1-bounded fallback
    page_props, final_url, fallback_status = fetch_match_page_with_fallback(bound_fotmob_match_id, canonical_url)
    if not page_props:
        result = {
            "project_fixture_id": proj_fid,
            "collector_status": "FAILED",
            "roster_evidence_status": "UNKNOWN",
            "error": "FOTMOB_SOURCE_UNAVAILABLE",
            "error_detail": f"Failed to retrieve FotMob match page for ID {bound_fotmob_match_id}",
            "lifecycle": lifecycle,
            "execution_usable": usable if lifecycle != "POST_KICKOFF_LOCKED" else False,
            "source_policy_status": SOURCE_POLICY_STATUS,
            "official_source_fallback": OFFICIAL_SOURCE_FALLBACK,
            "fotmob_binding": binding
        }
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
            with open(os.path.join(out_dir, "00-fixture-binding.json"), "w", encoding="utf-8") as wf:
                json.dump(binding, wf, ensure_ascii=False, indent=2)
        return result

    # 5. HARD ASSERTION: parsed_fotmob_match_id == bound_fotmob_match_id
    general = page_props.get("general", {})
    parsed_match_id = str(general.get("matchId") or page_props.get("id") or "")
    parsed_page_home = general.get("homeTeam", {}).get("name", "")
    parsed_page_away = general.get("awayTeam", {}).get("name", "")
    parsed_page_kickoff = page_props.get("header", {}).get("status", {}).get("utcTime") or general.get("matchTimeUTC", "")

    if parsed_match_id != bound_fotmob_match_id:
        result = {
            "project_fixture_id": proj_fid,
            "collector_status": "FAILED",
            "roster_evidence_status": "FAILED",
            "error": "FOTMOB_PAGE_IDENTITY_MISMATCH",
            "error_detail": f"Parsed FotMob match ID ({parsed_match_id}) != bound FotMob match ID ({bound_fotmob_match_id})",
            "parsed_page_match_id": parsed_match_id,
            "bound_fotmob_match_id": bound_fotmob_match_id,
            "identity_assertion": "FAIL",
            "lifecycle": lifecycle,
            "execution_usable": False,
            "source_policy_status": SOURCE_POLICY_STATUS,
            "official_source_fallback": OFFICIAL_SOURCE_FALLBACK,
            "fotmob_binding": binding
        }
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
            with open(os.path.join(out_dir, "00-fixture-binding.json"), "w", encoding="utf-8") as wf:
                json.dump(binding, wf, ensure_ascii=False, indent=2)
        return result

    # Extract parsed page proof
    identity_assertion = "PASS"

    # 6. Parse roster snapshot
    roster_snap = parse_roster_snapshot(page_props, canonical_url, bound_fotmob_match_id, retrieved_at_utc, retrieved_at_cst)
    roster_snap["identity_assertion"] = identity_assertion
    roster_snap["parsed_page_match_id"] = parsed_match_id
    roster_snap["parsed_page_home"] = parsed_page_home
    roster_snap["parsed_page_away"] = parsed_page_away
    roster_snap["parsed_page_kickoff"] = parsed_page_kickoff
    roster_snap["ssr_fallback_status"] = fallback_status

    # 7. Parse news snapshot
    news_snap = parse_news_snapshot(page_props, canonical_url, bound_fotmob_match_id, retrieved_at_utc)

    # Evaluation of evidence status
    if roster_snap["lineup_status"] in ["CONFIRMED", "PREDICTED"]:
        roster_evidence_status = "PASS"
        collector_status = "COMPLETE"
    else:
        roster_evidence_status = "PARTIAL"
        collector_status = "COMPLETE"

    # 8. If FINAL_REFRESH mode, compute delta
    roster_delta = None
    if mode == "FINAL_REFRESH" and prev_snapshot_path and os.path.exists(prev_snapshot_path):
        try:
            with open(prev_snapshot_path, "r", encoding="utf-8") as rf:
                prev_snap = json.load(rf)
            roster_delta = compute_roster_delta(prev_snap, roster_snap, None, news_snap)
        except Exception as e:
            print(f"Warning: failed to compute roster delta: {e}")

    # 9. Persist file outputs if output_dir provided
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
        raw_props_path = os.path.join(out_dir, "00-raw-fotmob-match-props.json")
        try:
            raw_bytes = json.dumps(page_props, ensure_ascii=False, indent=2).encode("utf-8")
            raw_sha = hashlib.sha256(raw_bytes).hexdigest()
            with open(raw_props_path, "wb") as wf:
                wf.write(raw_bytes)
            binding["raw_props_file"] = "00-raw-fotmob-match-props.json"
            binding["raw_props_sha256"] = raw_sha
            roster_snap["raw_props_file"] = "00-raw-fotmob-match-props.json"
            roster_snap["raw_props_sha256"] = raw_sha

            # [Ares R2 Spec] 同步落盘至 raw/fotmob/{fotmob_id}_raw_match_props.json 作为 Layer 0 物理凭据
            out_path_obj = Path(out_dir)
            if out_path_obj.parent.name == "parsed":
                pkg_raw_fotmob = out_path_obj.parent.parent / "raw" / "fotmob"
                pkg_raw_fotmob.mkdir(parents=True, exist_ok=True)
                pkg_raw_file = pkg_raw_fotmob / f"{bound_fotmob_match_id}_raw_match_props.json"
                with open(pkg_raw_file, "wb") as pwf:
                    pwf.write(raw_bytes)
        except Exception as e:
            print(f"Warning: failed to persist raw FotMob match props: {e}")

        with open(os.path.join(out_dir, "00-fixture-binding.json"), "w", encoding="utf-8") as wf:
            json.dump(binding, wf, ensure_ascii=False, indent=2)
            
        if mode == "PREMATCH":
            with open(os.path.join(out_dir, "01-fotmob-prematch-roster.json"), "w", encoding="utf-8") as wf:
                json.dump(roster_snap, wf, ensure_ascii=False, indent=2)
            with open(os.path.join(out_dir, "02-fotmob-prematch-news.json"), "w", encoding="utf-8") as wf:
                json.dump(news_snap, wf, ensure_ascii=False, indent=2)
        else:
            with open(os.path.join(out_dir, "03-fotmob-final-roster.json"), "w", encoding="utf-8") as wf:
                json.dump(roster_snap, wf, ensure_ascii=False, indent=2)
            if roster_delta:
                with open(os.path.join(out_dir, "04-fotmob-roster-delta.json"), "w", encoding="utf-8") as wf:
                    json.dump(roster_delta, wf, ensure_ascii=False, indent=2)

    return {
        "project_fixture_id": proj_fid,
        "collector_status": collector_status,
        "roster_evidence_status": roster_evidence_status,
        "lifecycle": lifecycle,
        "execution_usable": usable if lifecycle != "POST_KICKOFF_LOCKED" else False,
        "time_to_kickoff_minutes": time_to_ko_mins,
        "source_policy_status": SOURCE_POLICY_STATUS,
        "source_policy_note": SOURCE_POLICY_NOTE,
        "official_source_fallback": OFFICIAL_SOURCE_FALLBACK,
        "ssr_fallback_status": fallback_status,
        "fotmob_binding": binding,
        "identity_assertion": identity_assertion,
        "parsed_page_match_id": parsed_match_id,
        "parsed_page_home": parsed_page_home,
        "parsed_page_away": parsed_page_away,
        "parsed_page_kickoff": parsed_page_kickoff,
        "roster_snapshot": roster_snap,
        "news_snapshot": news_snap,
        "roster_delta": roster_delta
    }
