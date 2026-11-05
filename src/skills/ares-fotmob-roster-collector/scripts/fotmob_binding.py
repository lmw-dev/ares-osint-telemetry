#!/usr/bin/env python3
"""
FotMob Identity Binding Resolver (Ares OSINT Telemetry Pipeline v1.1)
Resolves canonical FotMob match identity from fixture attributes (league, home, away, kickoff).
NEVER assumes project_fixture_id == fotmob_match_id.

Binding Priorities:
1. Known alias / exact external identity match
2. Normalized full name exact match
3. Token similarity fallback (Jaccard)

Kickoff Time Tolerance:
<=3h  : PASS candidate
3-12h : CAUTION
>12h  : Reject (unless explicit reschedule_proven metadata is provided)
"""

import urllib.request
import ssl
import json
import re
import unicodedata
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Tuple, List

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
}

LEAGUE_MAP = {
    "epl": {"id": 47, "slug": "premier-league", "name": "Premier League"},
    "premier league": {"id": 47, "slug": "premier-league", "name": "Premier League"},
    "english premier league": {"id": 47, "slug": "premier-league", "name": "Premier League"},
    "英超": {"id": 47, "slug": "premier-league", "name": "Premier League"},
    "epl_gw05": {"id": 47, "slug": "premier-league", "name": "Premier League"},
    "epl_gw04": {"id": 47, "slug": "premier-league", "name": "Premier League"},

    "laliga": {"id": 87, "slug": "laliga", "name": "LaLiga"},
    "la liga": {"id": 87, "slug": "laliga", "name": "LaLiga"},
    "primera division": {"id": 87, "slug": "laliga", "name": "LaLiga"},
    "西甲": {"id": 87, "slug": "laliga", "name": "LaLiga"},
    "laliga_gw07": {"id": 87, "slug": "laliga", "name": "LaLiga"},
    "laliga_gw06": {"id": 87, "slug": "laliga", "name": "LaLiga"},

    "seriea": {"id": 55, "slug": "serie-a", "name": "Serie A"},
    "serie a": {"id": 55, "slug": "serie-a", "name": "Serie A"},
    "意甲": {"id": 55, "slug": "serie-a", "name": "Serie A"},
    "seriea_gw05": {"id": 55, "slug": "serie-a", "name": "Serie A"},
    "seriea_gw04": {"id": 55, "slug": "serie-a", "name": "Serie A"},

    "bundesliga": {"id": 54, "slug": "bundesliga", "name": "Bundesliga"},
    "德甲": {"id": 54, "slug": "bundesliga", "name": "Bundesliga"},
    "bundesliga_gw04": {"id": 54, "slug": "bundesliga", "name": "Bundesliga"},
    "bundesliga_gw03": {"id": 54, "slug": "bundesliga", "name": "Bundesliga"},

    "ligue1": {"id": 53, "slug": "ligue-1", "name": "Ligue 1"},
    "ligue 1": {"id": 53, "slug": "ligue-1", "name": "Ligue 1"},
    "法甲": {"id": 53, "slug": "ligue-1", "name": "Ligue 1"},
    "ligue1_gw05": {"id": 53, "slug": "ligue-1", "name": "Ligue 1"},
    "ligue1_gw04": {"id": 53, "slug": "ligue-1", "name": "Ligue 1"}
}

ZH_NAME_MAP = {
    # 英超
    "伯恩茅斯": "Bournemouth",
    "利物浦": "Liverpool",
    "利兹联": "Leeds United",
    "水晶宫": "Crystal Palace",
    "曼彻斯特城": "Manchester City",
    "曼城": "Manchester City",
    "桑德兰": "Sunderland",
    "富勒姆": "Fulham",
    "曼彻斯特联": "Manchester United",
    "曼联": "Manchester United",
    "托特纳姆热刺": "Tottenham",
    "热刺": "Tottenham",
    "阿斯顿维拉": "Aston Villa",
    "维拉": "Aston Villa",
    "布莱顿": "Brighton",
    "阿森纳": "Arsenal",
    "埃弗顿": "Everton",
    "伊普斯维奇": "Ipswich Town",
    "纽卡斯尔联": "Newcastle",
    "纽卡斯尔": "Newcastle",
    "赫尔城": "Hull City",
    "诺丁汉森林": "Nottingham Forest",
    "考文垂": "Coventry City",
    "切尔西": "Chelsea",
    "布伦特福德": "Brentford",
    "狼队": "Wolverhampton",
    "南安普顿": "Southampton",
    "西汉姆联": "West Ham",
    "莱斯特城": "Leicester City",

    # 西甲
    "马德里竞技": "Atletico Madrid",
    "马竞": "Atletico Madrid",
    "皇家马德里": "Real Madrid",
    "皇马": "Real Madrid",
    "拉科鲁尼亚": "Deportivo La Coruna",
    "皇家贝蒂斯": "Real Betis",
    "贝蒂斯": "Real Betis",
    "赫塔菲": "Getafe",
    "马拉加": "Malaga",
    "巴伦西亚": "Valencia",
    "皇家社会": "Real Sociedad",
    "皇社": "Real Sociedad",
    "比利亚雷亚尔": "Villarreal",
    "黄潜": "Villarreal",
    "莱万特": "Levante",
    "奥萨苏纳": "Osasuna",
    "巴列卡诺": "Rayo Vallecano",
    "毕尔巴鄂竞技": "Athletic Club",
    "毕尔巴鄂": "Athletic Club",
    "阿拉维斯": "Deportivo Alaves",
    "塞尔塔": "Celta Vigo",
    "桑坦德竞技": "Racing Santander",
    "塞维利亚": "Sevilla",
    "巴塞罗那": "Barcelona",
    "巴萨": "Barcelona",
    "西班牙人": "Espanyol",
    "埃尔切": "Elche",
    "赫罗纳": "Girona",
    "马略卡": "Mallorca",
    "拉斯帕尔马斯": "Las Palmas",
    "莱加内斯": "Leganes",
    "巴拉多利德": "Real Valladolid",

    # 意甲
    "佛罗伦萨": "Fiorentina",
    "那不勒斯": "Napoli",
    "弗洛西诺尼": "Frosinone",
    "科莫": "Como",
    "帕尔马": "Parma",
    "热那亚": "Genoa",
    "尤文图斯": "Juventus",
    "尤文": "Juventus",
    "亚特兰大": "Atalanta",
    "AC米兰": "AC Milan",
    "米兰": "AC Milan",
    "莱切": "Lecce",
    "博洛尼亚": "Bologna",
    "都灵": "Torino",
    "乌迪内斯": "Udinese",
    "卡利亚里": "Cagliari",
    "罗马": "Roma",
    "国际米兰": "Inter Milan",
    "国米": "Inter Milan",
    "威尼斯": "Venezia",
    "拉齐奥": "Lazio",
    "蒙扎": "Monza",
    "萨索洛": "Sassuolo",
    "维罗纳": "Verona",
    "恩波利": "Empoli",

    # 法甲
    "欧塞尔": "Auxerre",
    "布雷斯特": "Brest",
    "尼斯": "Nice",
    "里尔": "Lille",
    "昂热": "Angers",
    "巴黎圣日耳曼": "Paris Saint-Germain",
    "大巴黎": "Paris Saint-Germain",
    "雷恩": "Rennes",
    "圣埃蒂安": "Saint-Etienne",
    "兰斯": "Reims",
    "斯特拉斯堡": "Strasbourg",
    "南特": "Nantes",
    "梅斯": "Metz",
    "洛里昂": "Lorient",
    "里昂": "Lyon",
    "图卢兹": "Toulouse",
    "勒阿弗尔": "Le Havre",
    "摩纳哥": "Monaco",
    "朗斯": "Lens",
    "马赛": "Marseille",
    "蒙彼利埃": "Montpellier",
    "特鲁瓦": "Troyes",
    "巴黎FC": "Paris FC",
    "勒芒": "Le Mans",

    # 德甲
    "勒沃库森": "Bayer Leverkusen",
    "RB莱比锡": "RB Leipzig",
    "莱比锡红牛": "RB Leipzig",
    "莱比锡": "RB Leipzig",
    "沙尔克04": "Schalke 04",
    "沙尔克": "Schalke 04",
    "埃弗斯堡": "Elversberg",
    "帕德博恩": "Paderborn",
    "霍芬海姆": "Hoffenheim",
    "法兰克福": "Eintracht Frankfurt",
    "弗赖堡": "Freiburg",
    "门兴格拉德巴赫": "Borussia Monchengladbach",
    "门兴": "Borussia Monchengladbach",
    "美因茨": "Mainz 05",
    "汉堡": "Hamburger SV",
    "科隆": "FC Koln",
    "云达不莱梅": "Werder Bremen",
    "不莱梅": "Werder Bremen",
    "奥格斯堡": "Augsburg",
    "斯图加特": "VfB Stuttgart",
    "多特蒙德": "Borussia Dortmund",
    "多特": "Borussia Dortmund",
    "拜仁慕尼黑": "Bayern Munich",
    "拜仁": "Bayern Munich",
    "柏林联合": "Union Berlin",
    "海登海姆": "Heidenheim",
    "圣保利": "St. Pauli",
    "波鸿": "VfL Bochum",
    "沃尔夫斯堡": "Wolfsburg"
}

_SCHEDULE_CACHE = {}

# PURE STRUCTURAL STOP WORDS ONLY.
# NEVER globally discard entity-significant tokens like 'city', 'united', 'real', 'athletic', 'club', 'town'!
PURE_STOP_TOKENS = {
    "fc", "cf", "afc", "sv", "vfb", "vfl", "sc", "tsg", "cd", "ca", "de", "la", "le",
    "1", "04", "07", "05"
}

def normalize_name(name: str) -> List[str]:
    """Tokenize and normalize team name for accent-insensitive, pure structural stop-word filtering."""
    if not name:
        return []
    # If Chinese, translate via ZH_NAME_MAP if available
    name_str = ZH_NAME_MAP.get(name, name)
    # NFKD decompose
    n = unicodedata.normalize("NFKD", name_str).encode("ascii", "ignore").decode("utf-8").lower()
    # German / English city name variations
    n = re.sub(r"\b(munchen|muenchen)\b", "munich", n)
    n = re.sub(r"\b(koln|koeln)\b", "cologne", n)
    # remove non-alphanumeric
    n = re.sub(r"[^\w\s]", " ", n)
    tokens = [t for t in n.split() if t not in PURE_STOP_TOKENS and len(t) > 1]
    return tokens

def evaluate_team_match(input_name: str, cand_name: str) -> Tuple[float, int]:
    """
    Evaluates team name match and returns (similarity_score, priority_level).
    Priority 1: Known alias / exact external identity match
    Priority 2: Normalized full name exact match
    Priority 3: Token similarity fallback (Jaccard)
    """
    if not input_name or not cand_name:
        return 0.0, 99
        
    mapped_input = ZH_NAME_MAP.get(input_name, input_name)
    
    def _clean_str(s: str) -> str:
        de = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8").lower()
        de = re.sub(r"\b(munchen|muenchen)\b", "munich", de)
        de = re.sub(r"\b(koln|koeln)\b", "cologne", de)
        return re.sub(r"[^\w]", "", de)

    c_in = _clean_str(mapped_input)
    c_cand = _clean_str(cand_name)
    c_raw_in = _clean_str(input_name)
    
    # Priority 1: Known alias or exact case-insensitive match
    if mapped_input.lower() == cand_name.lower() or c_in == c_cand or c_raw_in == c_cand:
        return 1.0, 1
        
    # Priority 2: Normalized full token list match
    t_in = normalize_name(input_name)
    t_cand = normalize_name(cand_name)
    
    if t_in and t_cand and t_in == t_cand:
        return 0.98, 2
        
    # Priority 3: Token similarity fallback
    s1, s2 = set(t_in), set(t_cand)
    if not s1 or not s2:
        return 0.0, 3
        
    if s1 == s2:
        return 0.95, 3
    if s1.issubset(s2) or s2.issubset(s1):
        return 0.90, 3
        
    inter = s1.intersection(s2)
    union = s1.union(s2)
    sim = len(inter) / len(union) if union else 0.0
    return sim, 3

def team_name_similarity(name1: str, name2: str) -> float:
    """Calculate similarity score using evaluate_team_match."""
    sim, _ = evaluate_team_match(name1, name2)
    return sim

def fetch_fotmob_league_schedule(league_id: int, league_slug: str) -> List[Dict[str, Any]]:
    """Fetch and cache complete season fixture schedule from FotMob league page."""
    cache_key = f"{league_id}_{league_slug}"
    if cache_key in _SCHEDULE_CACHE:
        return _SCHEDULE_CACHE[cache_key]
        
    url = f"https://www.fotmob.com/leagues/{league_id}/matches/{league_slug}"
    req = urllib.request.Request(url, headers=HEADERS)
    html = ""
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(req, timeout=20, context=CTX) as r:
                html = r.read().decode("utf-8", errors="ignore")
            if html:
                break
        except Exception as e:
            if attempt == 3:
                print(f"Error fetching FotMob schedule for league {league_id}: {e}")
                return []
            time.sleep(1.5)
        
    m = re.search(r"<script id=\"__NEXT_DATA__\" type=\"application/json\">(.*?)</script>", html)
    if not m:
        return []
        
    try:
        data = json.loads(m.group(1))
        fixtures = data.get("props", {}).get("pageProps", {}).get("fixtures", {}).get("allMatches", [])
        if fixtures:
            _SCHEDULE_CACHE[cache_key] = fixtures
        return fixtures
    except Exception as e:
        print(f"Error parsing FotMob schedule JSON for league {league_id}: {e}")
        return []

def resolve_fotmob_binding(
    project_fixture_id: str,
    league: str,
    home_team: str,
    away_team: str,
    kickoff_utc: str,
    round_num: Optional[int] = None,
    season: Optional[str] = None,
    existing_binding: Optional[Dict[str, Any]] = None,
    reschedule_proven: bool = False
) -> Dict[str, Any]:
    """
    Resolves canonical FotMob match identity from fixture attributes.
    Enforces strict binding priorities:
    1. Known alias / external identity
    2. Normalized full name
    3. Token similarity fallback
    
    Kickoff matching:
    <=3h  : PASS candidate
    3-12h : CAUTION
    >12h  : Reject unless reschedule_proven == True
    """
    retrieved_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # 1. Check existing valid binding
    if existing_binding and existing_binding.get("status") in ["PASS", "CAUTION"]:
        canonical_url = existing_binding.get("canonical_url", "")
        f_mid = existing_binding.get("fotmob_match_id", "")
        if canonical_url and f_mid:
            # Hard anti-regression check
            validate_anti_regression(project_fixture_id, canonical_url)
            return {
                "status": existing_binding.get("status"),
                "reused_binding": True,
                "canonical_url": canonical_url,
                "fotmob_match_id": str(f_mid),
                "fotmob_slug": existing_binding.get("fotmob_slug", ""),
                "matched_home": existing_binding.get("matched_home", home_team),
                "matched_away": existing_binding.get("matched_away", away_team),
                "matched_kickoff": existing_binding.get("matched_kickoff", kickoff_utc),
                "identity_checks": {
                    "league_match": True,
                    "home_match": True,
                    "away_match": True,
                    "kickoff_match": True
                },
                "confidence": existing_binding.get("confidence", 1.0),
                "retrieved_at_utc": retrieved_at_utc
            }

    # 2. Resolve league config
    l_key = league.strip().lower()
    l_conf = LEAGUE_MAP.get(l_key)
    if not l_conf:
        for k, conf in LEAGUE_MAP.items():
            if k in l_key or l_key in k:
                l_conf = conf
                break
                
    if not l_conf:
        return {
            "status": "FAILED",
            "error": "FOTMOB_BINDING_FAILED",
            "error_detail": f"Unsupported or unmapped league: {league}",
            "canonical_url": None,
            "fotmob_match_id": None,
            "confidence": 0.0,
            "retrieved_at_utc": retrieved_at_utc
        }

    # 3. Retrieve schedule
    schedule = fetch_fotmob_league_schedule(l_conf["id"], l_conf["slug"])
    if not schedule:
        return {
            "status": "FAILED",
            "error": "FOTMOB_SOURCE_UNAVAILABLE",
            "error_detail": f"Failed to fetch FotMob schedule for {l_conf['name']}",
            "canonical_url": None,
            "fotmob_match_id": None,
            "confidence": 0.0,
            "retrieved_at_utc": retrieved_at_utc
        }

    # Parse target kickoff
    try:
        clean_ko = kickoff_utc.replace("Z", "+00:00")
        target_dt = datetime.fromisoformat(clean_ko)
    except Exception:
        target_dt = None

    candidates = []
    for m in schedule:
        cand_home = m.get("home", {}).get("name", "")
        cand_away = m.get("away", {}).get("name", "")
        cand_time_str = m.get("status", {}).get("utcTime", "")
        
        sim_h, prio_h = evaluate_team_match(home_team, cand_home)
        sim_a, prio_a = evaluate_team_match(away_team, cand_away)
        
        # Check team match threshold (must match both teams)
        if sim_h >= 0.5 and sim_a >= 0.5:
            time_diff_hours = None
            date_match = True
            if target_dt and cand_time_str:
                try:
                    cand_dt = datetime.fromisoformat(cand_time_str.replace("Z", "+00:00"))
                    time_diff_hours = abs((cand_dt - target_dt).total_seconds()) / 3600.0
                    # Strict kickoff rule: >12h reject unless explicit reschedule
                    if not reschedule_proven and time_diff_hours > 12.0:
                        date_match = False
                except Exception:
                    pass
                    
            if date_match:
                candidates.append({
                    "match": m,
                    "sim_h": sim_h,
                    "sim_a": sim_a,
                    "prio_h": prio_h,
                    "prio_a": prio_a,
                    "time_diff_hours": time_diff_hours
                })

    if not candidates:
        # Check if home and away were flipped in input
        for m in schedule:
            cand_home = m.get("home", {}).get("name", "")
            cand_away = m.get("away", {}).get("name", "")
            sim_h, _ = evaluate_team_match(away_team, cand_home)
            sim_a, _ = evaluate_team_match(home_team, cand_away)
            if sim_h >= 0.7 and sim_a >= 0.7:
                return {
                    "status": "FAILED",
                    "error": "FOTMOB_BINDING_FAILED",
                    "error_detail": f"Fixture home/away reversed on FotMob ({cand_home} vs {cand_away})",
                    "canonical_url": None,
                    "fotmob_match_id": None,
                    "confidence": 0.0,
                    "retrieved_at_utc": retrieved_at_utc
                }
                
        return {
            "status": "FAILED",
            "error": "FOTMOB_BINDING_FAILED",
            "error_detail": f"No candidate found for {home_team} vs {away_team} in {l_conf['name']}",
            "canonical_url": None,
            "fotmob_match_id": None,
            "confidence": 0.0,
            "retrieved_at_utc": retrieved_at_utc
        }

    # Sort candidates by:
    # 1. Combined priority (1+1=2 is best)
    # 2. Time difference (smallest is best)
    # 3. Combined similarity (highest is best)
    candidates.sort(key=lambda x: (
        x["prio_h"] + x["prio_a"],
        x["time_diff_hours"] if x["time_diff_hours"] is not None else 9999,
        -(x["sim_h"] + x["sim_a"])
    ))

    if len(candidates) > 1:
        best = candidates[0]
        second = candidates[1]
        best_prio = best["prio_h"] + best["prio_a"]
        second_prio = second["prio_h"] + second["prio_a"]
        
        # If priorities and similarities are tied within 3h, mark ambiguous
        if best_prio == second_prio and (best["sim_h"] + best["sim_a"]) == (second["sim_h"] + second["sim_a"]):
            if best["time_diff_hours"] is not None and second["time_diff_hours"] is not None:
                if abs(second["time_diff_hours"] - best["time_diff_hours"]) < 3.0:
                    return {
                        "status": "FAILED",
                        "error": "FOTMOB_BINDING_FAILED",
                        "error_detail": f"Multiple ambiguous matches found for {home_team} vs {away_team}",
                        "canonical_url": None,
                        "fotmob_match_id": None,
                        "confidence": 0.0,
                        "retrieved_at_utc": retrieved_at_utc
                    }
        chosen = best
    else:
        chosen = candidates[0]

    matched_m = chosen["match"]
    f_mid = str(matched_m.get("id"))
    page_url = matched_m.get("pageUrl", "")
    canonical_url = f"https://www.fotmob.com{page_url}" if page_url.startswith("/") else page_url
    
    # Extract slug
    m_slug = re.search(r"/matches/([^/#]+)", page_url)
    fotmob_slug = m_slug.group(1) if m_slug else ""

    # Identity checks
    home_match = (chosen["sim_h"] >= 0.7)
    away_match = (chosen["sim_a"] >= 0.7)
    time_diff = chosen["time_diff_hours"]
    
    is_time_pass = (time_diff is not None and time_diff <= 3.0)
    is_time_caution = (time_diff is not None and 3.0 < time_diff <= 12.0)

    # Determine status based on binding priority and time window
    if home_match and away_match:
        if (is_time_pass or (target_dt is None)) and chosen["sim_h"] >= 0.9 and chosen["sim_a"] >= 0.9:
            status = "PASS"
            confidence = 1.0
        elif is_time_caution or (chosen["sim_h"] >= 0.7 and chosen["sim_a"] >= 0.7):
            status = "CAUTION"
            confidence = round((chosen["sim_h"] + chosen["sim_a"]) / 2.0, 2)
        else:
            status = "FAILED"
            confidence = 0.0
    else:
        status = "FAILED"
        confidence = 0.0

    # Hard regression check
    validate_anti_regression(project_fixture_id, canonical_url)

    return {
        "status": status,
        "reused_binding": False,
        "canonical_url": canonical_url,
        "fotmob_match_id": f_mid,
        "fotmob_slug": fotmob_slug,
        "matched_home": matched_m.get("home", {}).get("name"),
        "matched_away": matched_m.get("away", {}).get("name"),
        "matched_kickoff": matched_m.get("status", {}).get("utcTime"),
        "time_difference_hours": time_diff,
        "identity_checks": {
            "league_match": True,
            "home_match": home_match,
            "away_match": away_match,
            "kickoff_match": (is_time_pass or is_time_caution)
        },
        "binding_priority": {
            "home_priority": chosen["prio_h"],
            "away_priority": chosen["prio_a"]
        },
        "confidence": confidence,
        "retrieved_at_utc": retrieved_at_utc
    }

def validate_anti_regression(project_fixture_id: str, canonical_url: str):
    """
    Enforces regression defense:
    Must never mechanically convert project_fixture_id into:
    https://www.fotmob.com/match/<project_fixture_id>
    """
    invalid_constructed = f"/match/{project_fixture_id}"
    invalid_url = f"https://www.fotmob.com/match/{project_fixture_id}"
    if canonical_url == invalid_url or canonical_url.endswith(invalid_constructed):
        raise ValueError(f"REGRESSION DEFECT: Detected direct mechanical construction of FotMob URL from project_fixture_id: {canonical_url}")
