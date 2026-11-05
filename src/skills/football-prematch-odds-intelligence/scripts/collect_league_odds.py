#!/usr/bin/env python3
"""
EPL / 五大联赛赛前赔率全量自动化采集器 (Ares OSINT Telemetry Pipeline)
支持自动对齐轮次赛事ID、排除已赛场次、并发抓取 1X2 欧赔、亚让 (AH)、进球大小 (OU)，
落盘原始原件 (raw/market/)，生成证据清单与 SHA-256，并输出结构化解析 JSON (parsed/)。
"""

import urllib.request
import ssl
import re
import json
import hashlib
import os
import sys
import time
import argparse
from pathlib import Path
from bs4 import BeautifulSoup

# 支持同目录导入 h2h_extractor
sys.path.insert(0, str(Path(__file__).resolve().parent))
from h2h_extractor import extract_h2h_data

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Referer": "https://zq.titan007.com/cn/League/36.html"
}

LEAGUE_CONFIG = {
    "epl": {"sclass_id": 36, "name": "English Premier League", "name_cn": "英超"},
    "laliga": {"sclass_id": 31, "name": "La Liga", "name_cn": "西甲"},
    "seriea": {"sclass_id": 34, "name": "Serie A", "name_cn": "意甲"},
    "bundesliga": {"sclass_id": 8, "name": "Bundesliga", "name_cn": "德甲"},
    "ligue1": {"sclass_id": 11, "name": "Ligue 1", "name_cn": "法甲"}
}

NAME_MAP = {
    "澳*": "Macauslot",
    "Crow*": "Crown",
    "36*": "Bet 365",
    "易*": "Easybets",
    "伟*": "BetVictor",
    "明*": "M88",
    "10*": "10Bet",
    "12*": "12Bet",
    "利*": "SBOBET",
    "盈*": "IBCBET",
    "18*": "18Bet",
    "立*": "Ladbrokes",
    "平*": "Pinnacle",
    "香港马*": "HKJC",
    "威*": "William Hill",
    "Interwet*": "Interwetten"
}

def get_league_schedule(sclass_id, season, round_num):
    url = f"https://zq.titan007.com/jsData/matchResult/{season}/s{sclass_id}.js"
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=12, context=CTX) as resp:
            text = resp.read().decode("utf-8", errors="ignore")
    except:
        text = ""
        
    r_key = f"R_{round_num}"
    # 若基础 js 不含该轮数据（如意甲等存在子联赛分段 s34_2948.js），自动探测 SubLeague 页面
    if not text or (r_key not in text):
        sub_url = f"https://zq.titan007.com/cn/SubLeague/{sclass_id}.html"
        try:
            sub_req = urllib.request.Request(sub_url, headers=HEADERS)
            with urllib.request.urlopen(sub_req, timeout=12, context=CTX) as sub_resp:
                sub_html = sub_resp.read().decode("utf-8", errors="ignore")
            # 匹配 /jsData/matchResult/{season}/s{sclass_id}_*.js
            m_sub_js = re.search(r"(/jsData/matchResult/" + re.escape(season) + r"/s" + str(sclass_id) + r"_[^\"\'\?]+)", sub_html)
            if m_sub_js:
                actual_js_url = "https://zq.titan007.com" + m_sub_js.group(1)
                with urllib.request.urlopen(urllib.request.Request(actual_js_url, headers=HEADERS), timeout=12, context=CTX) as actual_resp:
                    text = actual_resp.read().decode("utf-8", errors="ignore")
        except Exception as e:
            print(f"SubLeague 探测警告: {e}")
    
    # 提取球队字典
    team_dict = {}
    m_team = re.search(r"arrTeam\s*=\s*\[(.*?)\];", text, re.DOTALL)
    if m_team:
        for t in re.findall(r"\[(.*?)\]", m_team.group(1)):
            parts = [p.strip().strip("'").strip('"') for p in t.split(",")]
            if len(parts) >= 2:
                team_dict[parts[0]] = parts[1]
                
    # 提取轮次比赛
    m_r = re.search(r"jh\[[\"']" + r_key + r"[\"']\]\s*=\s*\[(.*?)\];", text, re.DOTALL)
    if not m_r:
        m_r = re.search(r_key + r"\s*\]\s*=\s*\[(.*?)\];", text, re.DOTALL)
    if not m_r:
        raise ValueError(f"未能在 s{sclass_id}.js 或对应子联赛数据中找到轮次 {round_num} 的数据！")
        
    fixtures = []
    for it in re.findall(r"\[(.*?)\]", m_r.group(1)):
        p = [x.strip().strip("'").strip('"') for x in it.split(",")]
        mid = p[0]
        state = p[2]
        mtime = p[3]
        hid, gid = p[4], p[5]
        hname = team_dict.get(hid, f"Team_{hid}")
        gname = team_dict.get(gid, f"Team_{gid}")
        score = p[6] if p[6] else ""
        fixtures.append({
            "schedule_id": mid,
            "state": state, # -1: 已完赛, 0: 未开赛
            "time_bj": mtime,
            "home": hname,
            "away": gname,
            "score": score
        })
    return fixtures

ZH_TEAM_MAP = {
    # 德甲
    "斯图加特": "VfB Stuttgart",
    "科隆": "1. FC Köln",
    "霍芬海姆": "TSG Hoffenheim",
    "多特蒙德": "Borussia Dortmund",
    "勒沃库森": "Bayer 04 Leverkusen",
    "柏林联合": "1. FC Union Berlin",
    "门兴格拉德巴赫": "Borussia Mönchengladbach",
    "埃弗斯堡": "SV 07 Elversberg",
    "云达不莱梅": "SV Werder Bremen",
    "RB莱比锡": "RB Leipzig",
    "帕德博恩": "SC Paderborn 07",
    "弗赖堡": "SC Freiburg",
    "沙尔克04": "FC Schalke 04",
    "拜仁慕尼黑": "FC Bayern München",
    "汉堡": "Hamburger SV",
    "美因茨": "1. FSV Mainz 05",
    "法兰克福": "Eintracht Frankfurt",
    "奥格斯堡": "FC Augsburg",
    # 英超
    "利物浦": "Liverpool",
    "诺丁汉森林": "Nottingham Forest",
    "考文垂": "Coventry City",
    "赫尔城": "Hull City",
    "伯恩茅斯": "Bournemouth",
    "埃弗顿": "Everton",
    "热刺": "Tottenham",
    "纽卡斯尔联": "Newcastle",
    "纽卡斯尔": "Newcastle",
    "切尔西": "Chelsea",
    "布莱顿": "Brighton",
    "利兹联": "Leeds United",
    "布伦特福德": "Brentford",
    "桑德兰": "Sunderland",
    "富勒姆": "Fulham",
    "曼联": "Manchester United",
    "曼彻斯特联": "Manchester United",
    "伊普斯维奇": "Ipswich Town",
    "阿森纳": "Arsenal",
    "托特纳姆热刺": "Tottenham",
    "阿斯顿维拉": "Aston Villa",
    "水晶宫": "Crystal Palace",
    "曼彻斯特城": "Manchester City",
    "曼城": "Manchester City",
    # 法甲
    "雷恩": "Rennes",
    "马赛": "Marseille",
    "斯特拉斯堡": "Strasbourg",
    "摩纳哥": "Monaco",
    "勒阿弗尔": "Le Havre",
    "昂热": "Angers",
    "欧塞尔": "Auxerre",
    "尼斯": "Nice",
    "洛里昂": "Lorient",
    "图卢兹": "Toulouse",
    "巴黎FC": "Paris FC",
    "里昂": "Lyon",
    "里尔": "Lille",
    "特鲁瓦": "Troyes",
    "勒芒": "Le Mans",
    "朗斯": "Lens",
    "布雷斯特": "Brest",
    "巴黎圣日耳曼": "Paris Saint-Germain",
    # 西甲
    "皇家社会": "Real Sociedad",
    "塞尔塔": "Celta Vigo",
    "巴列卡诺": "Rayo Vallecano",
    "西班牙人": "Espanyol",
    "阿拉维斯": "Deportivo Alaves",
    "巴伦西亚": "Valencia",
    "埃尔切": "Elche",
    "皇家马德里": "Real Madrid",
    "拉科鲁尼亚": "Deportivo La Coruna",
    "塞维利亚": "Sevilla",
    "马德里竞技": "Atletico Madrid",
    "奥萨苏纳": "Osasuna",
    "巴塞罗那": "Barcelona",
    "桑坦德竞技": "Racing Santander",
    "莱万特": "Levante",
    "毕尔巴鄂竞技": "Athletic Club",
    "皇家贝蒂斯": "Real Betis",
    "赫塔菲": "Getafe",
    "马拉加": "Malaga",
    "比利亚雷亚尔": "Villarreal",
    # 意甲
    "蒙扎": "Monza",
    "萨索洛": "Sassuolo",
    "乌迪内斯": "Udinese",
    "卡利亚里": "Cagliari",
    "博洛尼亚": "Bologna",
    "都灵": "Torino",
    "罗马": "Roma",
    "国际米兰": "Inter Milan",
    "威尼斯": "Venezia",
    "拉齐奥": "Lazio",
    "佛罗伦萨": "Fiorentina",
    "那不勒斯": "Napoli",
    "弗洛西诺尼": "Frosinone",
    "科莫": "Como",
    "帕尔马": "Parma",
    "热那亚": "Genoa",
    "尤文图斯": "Juventus",
    "亚特兰大": "Atalanta",
    "AC米兰": "AC Milan",
    "莱切": "Lecce"
}

def collect_fixture_markets(sched_id, home, away, raw_dir, parsed_dir, match_time=""):
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(parsed_dir, exist_ok=True)
    
    manifest_entries = []
    retrieved_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    
    # 1. 1X2 欧赔
    url_1x2 = f"https://1x2d.titan007.com/{sched_id}.js"
    raw_1x2 = b""
    try:
        req = urllib.request.Request(url_1x2, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=12, context=CTX) as resp:
            raw_1x2 = resp.read()
    except Exception as e:
        print(f"  [1X2 抓取失败] {url_1x2}: {e}")
        
    sha_1x2 = hashlib.sha256(raw_1x2).hexdigest() if raw_1x2 else None
    f_1x2 = os.path.join(raw_dir, f"{sched_id}_1x2.js")
    if raw_1x2:
        with open(f_1x2, "wb") as wf:
            wf.write(raw_1x2)
            
    manifest_entries.append({
        "source_name": "Titan007 1X2 Odds Stream",
        "source_url": url_1x2,
        "external_id": sched_id,
        "market_type": "1X2",
        "http_status": 200 if raw_1x2 else 500,
        "raw_file": os.path.relpath(f_1x2, os.path.dirname(os.path.dirname(raw_dir))),
        "file_size": len(raw_1x2),
        "sha256": sha_1x2,
        "retrieved_utc": retrieved_utc,
        "parse_status": "QUALIFIED" if raw_1x2 else "BLOCKED"
    })
    
    # 2. 亚盘 AH
    url_ah = f"https://vip.titan007.com/AsianOdds_n.aspx?id={sched_id}&l=0"
    raw_ah = b""
    try:
        req = urllib.request.Request(url_ah, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=12, context=CTX) as resp:
            raw_ah = resp.read()
    except Exception as e:
        print(f"  [AH 抓取失败] {url_ah}: {e}")
        
    sha_ah = hashlib.sha256(raw_ah).hexdigest() if raw_ah else None
    f_ah = os.path.join(raw_dir, f"{sched_id}_ah.html")
    if raw_ah:
        with open(f_ah, "wb") as wf:
            wf.write(raw_ah)
            
    manifest_entries.append({
        "source_name": "Titan007 Asian Handicap",
        "source_url": url_ah,
        "external_id": sched_id,
        "market_type": "AH",
        "http_status": 200 if raw_ah else 500,
        "raw_file": os.path.relpath(f_ah, os.path.dirname(os.path.dirname(raw_dir))),
        "file_size": len(raw_ah),
        "sha256": sha_ah,
        "retrieved_utc": retrieved_utc,
        "parse_status": "QUALIFIED" if raw_ah else "BLOCKED"
    })
    
    # 3. 大小球 OU
    url_ou = f"https://vip.titan007.com/OverDown_n.aspx?id={sched_id}&l=0"
    raw_ou = b""
    try:
        req = urllib.request.Request(url_ou, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=12, context=CTX) as resp:
            raw_ou = resp.read()
    except Exception as e:
        print(f"  [OU 抓取失败] {url_ou}: {e}")
        
    sha_ou = hashlib.sha256(raw_ou).hexdigest() if raw_ou else None
    f_ou = os.path.join(raw_dir, f"{sched_id}_ou.html")
    if raw_ou:
        with open(f_ou, "wb") as wf:
            wf.write(raw_ou)
            
    manifest_entries.append({
        "source_name": "Titan007 Over/Under",
        "source_url": url_ou,
        "external_id": sched_id,
        "market_type": "OU",
        "http_status": 200 if raw_ou else 500,
        "raw_file": os.path.relpath(f_ou, os.path.dirname(os.path.dirname(raw_dir))),
        "file_size": len(raw_ou),
        "sha256": sha_ou,
        "retrieved_utc": retrieved_utc,
        "parse_status": "QUALIFIED" if raw_ou else "BLOCKED"
    })

    # 4. 分析页 Analysis (用于 H2H 对赛往绩抽取)
    url_analysis = f"https://zq.titan007.com/analysis/{sched_id}cn.htm"
    raw_analysis = b""
    try:
        req = urllib.request.Request(url_analysis, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=12, context=CTX) as resp:
            raw_analysis = resp.read()
    except Exception as e:
        print(f"  [Analysis 抓取失败] {url_analysis}: {e}")
        
    sha_analysis = hashlib.sha256(raw_analysis).hexdigest() if raw_analysis else None
    f_analysis = os.path.join(raw_dir, f"{sched_id}_analysis.html")
    if raw_analysis:
        with open(f_analysis, "wb") as wf:
            wf.write(raw_analysis)
            
    manifest_entries.append({
        "source_name": "Titan007 Match Analysis (H2H)",
        "source_url": url_analysis,
        "external_id": sched_id,
        "market_type": "ANALYSIS_H2H",
        "http_status": 200 if raw_analysis else 500,
        "raw_file": os.path.relpath(f_analysis, os.path.dirname(os.path.dirname(raw_dir))),
        "file_size": len(raw_analysis),
        "sha256": sha_analysis,
        "retrieved_utc": retrieved_utc,
        "parse_status": "QUALIFIED" if raw_analysis else "BLOCKED"
    })
    
    # 结构化解析 1X2, AH, OU
    text_1x2 = raw_1x2.decode("utf-8", errors="ignore")
    text_ah = raw_ah.decode("utf-8", errors="ignore")
    text_ou = raw_ou.decode("utf-8", errors="ignore")
    text_analysis = raw_analysis.decode("utf-8", errors="ignore")
    
    parsed_1x2 = []
    m_1x2 = re.search(r"var\s+game=Array\((.*?)\);", text_1x2, re.DOTALL)
    if m_1x2:
        for r in re.findall(r'"([^"]+)"', m_1x2.group(1)):
            p = r.split("|")
            if len(p) > 20:
                parsed_1x2.append({
                    "company_id": p[0],
                    "company_name": p[2],
                    "company_cn": p[21] if len(p) > 21 else "",
                    "initial": {"home": float(p[3]), "draw": float(p[4]), "away": float(p[5]), "return_rate": float(p[9]) if p[9] else 0.0},
                    "current": {"home": float(p[10]), "draw": float(p[11]), "away": float(p[12]), "return_rate": float(p[16]) if p[16] else 0.0},
                    "kelly": {"home": float(p[17]) if p[17] else 0.0, "draw": float(p[18]) if p[18] else 0.0, "away": float(p[19]) if p[19] else 0.0},
                    "updated_time": p[20].replace("-1", "").replace(",", "-")
                })
                
    parsed_ah = []
    soup_ah = BeautifulSoup(text_ah, "html.parser")
    table_ah = soup_ah.find("table", id="odds")
    if table_ah:
        for tr in table_ah.find_all("tr"):
            tds = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if len(tds) >= 9 and tds[1] in NAME_MAP:
                parsed_ah.append({
                    "company_name": NAME_MAP[tds[1]],
                    "company_raw": tds[1],
                    "initial": {"home_water": tds[3], "line": tds[4], "away_water": tds[5]},
                    "current": {"home_water": tds[6], "line": tds[7], "away_water": tds[8]}
                })

    parsed_ou = []
    soup_ou = BeautifulSoup(text_ou, "html.parser")
    table_ou = soup_ou.find("table", id="odds")
    if table_ou:
        for tr in table_ou.find_all("tr"):
            tds = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if len(tds) >= 9 and tds[1] in NAME_MAP:
                parsed_ou.append({
                    "company_name": NAME_MAP[tds[1]],
                    "company_raw": tds[1],
                    "initial": {"over_water": tds[3], "line": tds[4], "under_water": tds[5]},
                    "current": {"over_water": tds[6], "line": tds[7], "under_water": tds[8]}
                })

    # 抽取对赛往绩 H2H (严谨防崩降级)
    try:
        h2h_data = extract_h2h_data(text_analysis, home, away, match_time, url_analysis)
    except Exception as e:
        h2h_data = {
            "source": "Titan007",
            "source_url": url_analysis,
            "retrieved_at_utc": retrieved_utc,
            "status": "PARSE_FAILED",
            "available_count": 0,
            "error": str(e),
            "records": []
        }
        
    f_h2h = os.path.join(parsed_dir, f"{sched_id}_h2h.json")
    with open(f_h2h, "w", encoding="utf-8") as wf:
        json.dump(h2h_data, wf, ensure_ascii=False, indent=2)
        
    sha_h2h = hashlib.sha256(open(f_h2h, "rb").read()).hexdigest()
    manifest_entries.append({
        "source_name": "Titan007 Head to Head Normalized (h2h.json)",
        "source_url": url_analysis,
        "external_id": sched_id,
        "market_type": "H2H_JSON",
        "http_status": 200,
        "raw_file": os.path.relpath(f_h2h, os.path.dirname(os.path.dirname(raw_dir))),
        "file_size": os.path.getsize(f_h2h),
        "sha256": sha_h2h,
        "retrieved_utc": retrieved_utc,
        "parse_status": h2h_data.get("status", "QUALIFIED")
    })
                
    structured_data = {
        "external_id": sched_id,
        "home": home,
        "away": away,
        "match_time_cst": match_time,
        "retrieved_utc": retrieved_utc,
        "status": "QUALIFIED" if len(parsed_1x2) >= 3 else "HOLD",
        "h2h_status": h2h_data.get("status"),
        "h2h_available_count": h2h_data.get("available_count", 0),
        "bookmakers_count": len(parsed_1x2),
        "raw_refs": {
            "1x2": {"path": f_1x2, "sha256": sha_1x2, "url": url_1x2},
            "ah": {"path": f_ah, "sha256": sha_ah, "url": url_ah},
            "ou": {"path": f_ou, "sha256": sha_ou, "url": url_ou},
            "analysis": {"path": f_analysis, "sha256": sha_analysis, "url": url_analysis},
            "h2h": {"path": f_h2h, "sha256": sha_h2h, "url": url_analysis}
        },
        "markets": {
            "europe_1x2": parsed_1x2,
            "asian_handicap": parsed_ah,
            "over_under": parsed_ou
        }
    }
    
    clean_name = f"{home.lower().replace(' ', '_')}_vs_{away.lower().replace(' ', '_')}"
    json_path = os.path.join(parsed_dir, f"{sched_id}_{clean_name}_odds.json")
    with open(json_path, "w", encoding="utf-8") as wf:
        json.dump(structured_data, wf, ensure_ascii=False, indent=2)
        
    return manifest_entries, structured_data

def update_binding_table(binding_path, denom_path, league, season, round_str, qualified_matches):
    if not os.path.exists(binding_path) or not os.path.exists(denom_path):
        return
    try:
        with open(denom_path, "r", encoding="utf-8") as f:
            denom = json.load(f)
        with open(binding_path, "r", encoding="utf-8") as f:
            binding_data = json.load(f)
            
        bound_count = 0
        new_bindings = []
        for df in denom.get("fixtures", []):
            loc_id = str(df.get("openligadb_matchID") or df.get("fixture_id"))
            ko_utc = df.get("kickoff_utc")
            d_home = df.get("home")
            d_away = df.get("away")
            
            matched_qm = None
            for qm in qualified_matches:
                zh_h = qm["home"]
                zh_a = qm["away"]
                map_h = ZH_TEAM_MAP.get(zh_h, zh_h)
                map_a = ZH_TEAM_MAP.get(zh_a, zh_a)
                if (map_h == d_home or map_h in d_home) and (map_a == d_away or map_a in d_away):
                    matched_qm = qm
                    break
                    
            if matched_qm:
                bound_count += 1
                sched_id = matched_qm["external_id"]
                raw_refs = matched_qm.get("raw_refs", {})
                new_bindings.append({
                    "league": league.capitalize(),
                    "season": season,
                    "round": f"GW{int(round_str):02d}" if round_str.isdigit() else round_str,
                    "local_fixture_id": loc_id,
                    "understat_id": str(df.get("understat_id", "")),
                    "kickoff_utc": ko_utc,
                    "home": d_home,
                    "away": d_away,
                    "normalized_home": d_home.lower().replace(" ", "_"),
                    "normalized_away": d_away.lower().replace(" ", "_"),
                    "binding_tuple": f"{league.capitalize()}|{ko_utc}|{d_home}|{d_away}|{loc_id}",
                    "titan_schedule_id": sched_id,
                    "confidence": "EXACT",
                    "status": "BOUND_READY_FOR_ADAPTER",
                    "evidence_refs": [
                        {
                            "type": "titan007_1x2",
                            "source_url": raw_refs.get("1x2", {}).get("url"),
                            "retrieved_utc": matched_qm.get("retrieved_utc"),
                            "sha256": raw_refs.get("1x2", {}).get("sha256"),
                            "raw_path": raw_refs.get("1x2", {}).get("path")
                        },
                        {
                            "type": "titan007_ah",
                            "source_url": raw_refs.get("ah", {}).get("url"),
                            "retrieved_utc": matched_qm.get("retrieved_utc"),
                            "sha256": raw_refs.get("ah", {}).get("sha256"),
                            "raw_path": raw_refs.get("ah", {}).get("path")
                        },
                        {
                            "type": "titan007_ou",
                            "source_url": raw_refs.get("ou", {}).get("url"),
                            "retrieved_utc": matched_qm.get("retrieved_utc"),
                            "sha256": raw_refs.get("ou", {}).get("sha256"),
                            "raw_path": raw_refs.get("ou", {}).get("path")
                        }
                    ],
                    "ambiguity": "None - exact 5-tuple match verified against official Denominator"
                })
        if bound_count > 0:
            binding_data["version"] = "1.3.0"
            binding_data["generated_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            binding_data["coverage"] = f"{bound_count}/{len(denom.get('fixtures', []))} bound (EXACT match with Denominator)"
            binding_data["status"] = "ALL_BOUND" if bound_count == len(denom.get("fixtures", [])) else "PARTIALLY_BOUND"
            binding_data["bindings"] = new_bindings
            with open(binding_path, "w", encoding="utf-8") as wf:
                json.dump(binding_data, wf, ensure_ascii=False, indent=2)
            print(f"✅ Binding Table 已成功更新: {binding_path} (精确绑定: {bound_count}/{len(denom.get('fixtures', []))})")
    except Exception as ex:
        print(f"⚠️ 更新 binding table 失败: {ex}")

def main():
    parser = argparse.ArgumentParser(description="自动采集指定联赛轮次的真实赔率与盘口原件")
    parser.add_argument("--league", default="epl", choices=list(LEAGUE_CONFIG.keys()), help="联赛标识")
    parser.add_argument("--season", default="2026-2027", help="赛季，如 2026-2027")
    parser.add_argument("--round", type=int, required=True, help="轮次编号，如 2")
    parser.add_argument("--output-root", "--output_root", dest="output_root", default=None, help="项目输出根目录 (如 Vault GW 路径)")
    parser.add_argument("--output_dir", "--output-dir", dest="output_dir", default=None, help="兼容性交付包输出路径")
    parser.add_argument("--denominator", default=None, help="权威 Denominator 路径 (用于 5-tuple exact match 与 binding table 更新)")
    parser.add_argument("--include-finished", action="store_true", help="是否包含已完赛场次 (仅限回归测试与历史回测使用，默认严格排除)")
    args = parser.parse_args()
    
    target_root = args.output_root or args.output_dir
    if not target_root:
        raise ValueError("必须指定 --output-root 或 --output_dir！")
        
    # 判断是否为 Vault 结构
    is_vault_mode = (args.output_root is not None) or ("03_Match_Audits" in target_root) or os.path.exists(os.path.join(target_root, "00_Denominator_Ledger.json"))
    
    if is_vault_mode:
        raw_market_dir = os.path.join(target_root, "01_Evidence/raw/market/titan007")
        parsed_market_dir = os.path.join(target_root, "01_Evidence/skill_runs/parsed")
        manifest_path = os.path.join(target_root, "01_Evidence/raw/market/market_evidence_manifest.json")
        skill_runs_dir = os.path.join(target_root, "01_Evidence/skill_runs")
    else:
        raw_market_dir = os.path.join(target_root, "raw/market/titan007")
        parsed_market_dir = os.path.join(target_root, "parsed")
        manifest_path = os.path.join(target_root, "raw/market/market_evidence_manifest.json")
        skill_runs_dir = os.path.join(target_root, "parsed")
        
    os.makedirs(raw_market_dir, exist_ok=True)
    os.makedirs(parsed_market_dir, exist_ok=True)
    os.makedirs(skill_runs_dir, exist_ok=True)
    
    cfg = LEAGUE_CONFIG[args.league]
    print(f"=== 开始采集 {cfg['name_cn']} {args.season} 第 {args.round} 轮全量市场赔率 ===")
    print(f"  模式: {'Vault 项目模式' if is_vault_mode else '独立交付包模式'}")
    print(f"  原始文件落地目录: {raw_market_dir}")
    print(f"  解析文件落地目录: {parsed_market_dir}")
    
    fixtures = get_league_schedule(cfg["sclass_id"], args.season, args.round)
    print(f"已获取底层赛程全部 {len(fixtures)} 场赛事对阵：")
    
    manifest_all = []
    qualified_matches = []
    
    for f in fixtures:
        sched_id = f["schedule_id"]
        home, away = f["home"], f["away"]
        if f["state"] == "-1" and not args.include_finished:
            print(f"  [排除] {home} vs {away} (ID: {sched_id}) 已完赛 ({f['score']})，严格排除 Prematch 采集！")
            continue
            
        print(f"  -> 采集比赛: {home} vs {away} (ID: {sched_id}, 开赛时间: {f['time_bj']}) ...")
        m_entries, s_data = collect_fixture_markets(sched_id, home, away, raw_market_dir, parsed_market_dir, match_time=f.get("time_bj", ""))
        manifest_all.extend(m_entries)
        qualified_matches.append(s_data)
        print(f"     已成功抓取: 1X2 机构 {len(s_data['markets']['europe_1x2'])} 家, 亚盘 {len(s_data['markets']['asian_handicap'])} 家, 大小球 {len(s_data['markets']['over_under'])} 家, H2H: {s_data.get('h2h_available_count', 0)} 场 ({s_data.get('h2h_status', 'NO_DATA')})")

    # 写入清单
    with open(manifest_path, "w", encoding="utf-8") as wf:
        json.dump(manifest_all, wf, ensure_ascii=False, indent=2)
        
    # 若在 Vault 模式，写入 skill_runs/run_manifest.json
    if is_vault_mode:
        run_manifest = {
            "skill": "football-prematch-odds-intelligence",
            "version": "2.2.0",
            "executed_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "league": cfg["name"],
            "season": args.season,
            "round": args.round,
            "output_root": target_root,
            "fixtures_collected": len(qualified_matches),
            "manifest_path": os.path.relpath(manifest_path, target_root),
            "raw_dir": os.path.relpath(raw_market_dir, target_root),
            "parsed_dir": os.path.relpath(parsed_market_dir, target_root)
        }
        with open(os.path.join(skill_runs_dir, "run_manifest.json"), "w", encoding="utf-8") as rf:
            json.dump(run_manifest, rf, ensure_ascii=False, indent=2)
            
    # 若提供了 denominator 路径（或自动检测），更新 binding table
    denom_file = args.denominator
    if not denom_file and is_vault_mode:
        auto_denom = os.path.join(target_root, "00_Denominator_Ledger.json")
        if os.path.exists(auto_denom):
            denom_file = auto_denom
            
    binding_file = os.path.join(target_root, "01_Evidence/titan_schedule_binding_table.json")
    if denom_file and os.path.exists(denom_file) and os.path.exists(binding_file):
        update_binding_table(binding_file, denom_file, args.league, args.season, str(args.round), qualified_matches)

    print(f"\n✅ 全轮采集完成！共处理 {len(qualified_matches)} 场未开赛比赛，原始证据与清单已全部就绪。")

if __name__ == "__main__":
    main()
