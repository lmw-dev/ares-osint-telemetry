#!/usr/bin/env python3
"""
历史对赛往绩与核心机构初终盘走势全自动采集器 (H2H Market History Collector)
Ares OSINT Telemetry Pipeline 核心模块

功能：
1. 抓取分析页 https://zq.titan007.com/analysis/{schedule_id}cn.htm
2. 解析 v_data，提取历史交锋明细：日期、赛事、主客队、赛前排名、比分、半场比分、角球比、盘路胜负、大小球胜负及历史 ScheduleID；
3. 对每场具有 ScheduleID 的历史赛事，自动请求 https://1x2d.titan007.com/{mid}.js；
4. 提取指定核心博彩公司（默认：William Hill 威廉希尔、Macauslot 澳门、Bet365、Ladbrokes 立博等）的开盘（初盘）和终盘（即时盘）数据；
5. 计算赔率升降走势与返还率，输出结构化 JSON，落盘原始原件并登记 SHA-256。
"""

import urllib.request
import ssl
import re
import json
import hashlib
import os
import time
import ast
import argparse
from bs4 import BeautifulSoup

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Referer": "https://zq.titan007.com/"
}

# 核心监控机构字典 (ID / 名称关键字)
TARGET_BOOKMAKERS = {
    "115": {"name": "William Hill", "cn": "威廉希尔"},
    "80":  {"name": "Macauslot", "cn": "澳门彩票"},
    "281": {"name": "Bet 365", "cn": "日博/365"},
    "82":  {"name": "Ladbrokes", "cn": "立博"},
    "545": {"name": "Pinnacle", "cn": "平博"},
    "454": {"name": "Interwetten", "cn": "因特维登"}
}

def parse_year(raw_str):
    parts = raw_str.split("-")
    yr = int(parts[0])
    full_year = f"20{yr:02d}" if yr <= 50 else f"19{yr:02d}"
    return f"{full_year}-{parts[1]}-{parts[2]}"

def fetch_h2h_matches(schedule_id, out_dir):
    url = f"https://zq.titan007.com/analysis/{schedule_id}cn.htm"
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=12, context=CTX) as resp:
        raw_html = resp.read()
        
    html = raw_html.decode("utf-8", errors="ignore")
    
    # 原始原件落盘
    raw_dir = os.path.join(out_dir, "raw/market/titan007")
    os.makedirs(raw_dir, exist_ok=True)
    raw_file = os.path.join(raw_dir, f"{schedule_id}_analysis.html")
    with open(raw_file, "wb") as wf:
        wf.write(raw_html)
    sha_html = hashlib.sha256(raw_html).hexdigest()
    
    m_home = re.search(r'var\s+hometeam\s*=\s*"([^"]+)";', html)
    m_away = re.search(r'var\s+guestteam\s*=\s*"([^"]+)";', html)
    home_team = m_home.group(1) if m_home else "主队"
    away_team = m_away.group(1) if m_away else "客队"
    
    m_v = re.search(r"var\s+v_data\s*=\s*(\[.*?\]);", html, re.DOTALL)
    if not m_v:
        raise ValueError(f"赛事 {schedule_id} 未在分析页找到 v_data 数组！")
        
    v_data = ast.literal_eval(m_v.group(1))
    
    parsed_matches = []
    for row in v_data:
        full_date = parse_year(row[0])
        league_name = row[2]
        
        soup_h = BeautifulSoup(row[5], "html.parser")
        soup_a = BeautifulSoup(row[7], "html.parser")
        h_rank = soup_h.find("span")["title"].strip() if soup_h.find("span") and "title" in soup_h.find("span").attrs else ""
        a_rank = soup_a.find("span")["title"].strip() if soup_a.find("span") and "title" in soup_a.find("span").attrs else ""
        h_name = soup_h.get_text(strip=True)
        a_name = soup_a.get_text(strip=True)
        
        score_h = int(row[8])
        score_a = int(row[9])
        half_score = row[10]
        
        corners_h = row[16] if len(row) > 16 else "-"
        corners_a = row[17] if len(row) > 17 else "-"
        corners_str = f"{corners_h}-{corners_a}" if corners_h != "-" and corners_a != "-" else "未记录"
        
        handicap_result = "赢" if row[12] == 1 else ("走" if row[12] == 0 else "输")
        ou_result = "大球" if row[14] == 1 else ("走" if row[14] == 0 else "小球")
        match_id = row[15] if len(row) > 15 else ""
        
        outcome = "主胜" if score_h > score_a else ("平局" if score_h == score_a else "客胜")
        
        parsed_matches.append({
            "match_id": str(match_id),
            "date": full_date,
            "league": league_name,
            "home": h_name,
            "home_rank": h_rank,
            "away": a_name,
            "away_rank": a_rank,
            "score": f"{score_h}-{score_a}",
            "half_score": half_score,
            "outcome": outcome,
            "corners": corners_str,
            "handicap_result": handicap_result,
            "ou_result": ou_result,
            "total_goals": score_h + score_a
        })
        
    return home_team, away_team, parsed_matches, raw_file, sha_html

def fetch_historical_odds(match_id, requested_bookmakers=None):
    if not match_id or int(match_id) < 10000:
        return {}
    url = f"https://1x2d.titan007.com/{match_id}.js"
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=10, context=CTX) as resp:
            text = resp.read().decode("utf-8", errors="ignore")
    except:
        return {}
        
    m = re.search(r"var\s+game=Array\((.*?)\);", text, re.DOTALL)
    if not m:
        return {}
        
    odds_map = {}
    for r in re.findall(r'"([^"]+)"', m.group(1)):
        p = r.split("|")
        if len(p) > 20:
            cid = p[0]
            cname = p[2]
            ccn = p[21] if len(p) > 21 else ""
            
            # 过滤指定公司
            matched_id = None
            if cid in TARGET_BOOKMAKERS:
                matched_id = cid
            else:
                for k, v in TARGET_BOOKMAKERS.items():
                    if v["name"].lower() in cname.lower() or v["cn"] in ccn:
                        matched_id = k
                        break
                        
            if matched_id:
                if requested_bookmakers and matched_id not in requested_bookmakers and TARGET_BOOKMAKERS[matched_id]["name"] not in requested_bookmakers:
                    continue
                def safe_float(val, default=0.0):
                    try:
                        return float(val) if val else default
                    except:
                        return default

                init_h = safe_float(p[3])
                init_d = safe_float(p[4])
                init_a = safe_float(p[5])
                curr_h = safe_float(p[10])
                curr_d = safe_float(p[11])
                curr_a = safe_float(p[12])
                init_ret = safe_float(p[9]) if len(p) > 9 else 0.0
                curr_ret = safe_float(p[16]) if len(p) > 16 else 0.0
                
                odds_map[TARGET_BOOKMAKERS[matched_id]["name"]] = {
                    "company_id": matched_id,
                    "company_cn": TARGET_BOOKMAKERS[matched_id]["cn"],
                    "initial": {"home": init_h, "draw": init_d, "away": init_a, "return_rate": init_ret},
                    "final": {"home": curr_h, "draw": curr_d, "away": curr_a, "return_rate": curr_ret},
                    "trend": {
                        "home": "↑ 升" if curr_h > init_h else ("↓ 降" if curr_h < init_h else "- 平"),
                        "draw": "↑ 升" if curr_d > init_d else ("↓ 降" if curr_d < init_d else "- 平"),
                        "away": "↑ 升" if curr_a > init_a else ("↓ 降" if curr_a < init_a else "- 平")
                    },
                    "final_time": p[20].replace("-1", "").replace(",", "-") if len(p) > 20 else ""
                }
    return odds_map

def collect_h2h_pipeline(schedule_id, out_dir, target_books=None):
    parsed_dir = os.path.join(out_dir, "parsed")
    os.makedirs(parsed_dir, exist_ok=True)
    
    print(f"=== 开始采集赛事 {schedule_id} 历史对战往绩与盘口走势 ===")
    home_team, away_team, matches, raw_file, sha_html = fetch_h2h_matches(schedule_id, out_dir)
    print(f"解析成功：{home_team} vs {away_team}，共获取 {len(matches)} 场历史交锋。")
    
    # 批量拉取赔率
    h2h_full_data = []
    for idx, m in enumerate(matches):
        mid = m["match_id"]
        print(f"  [{idx+1}/{len(matches)}] 获取交锋赛事 {m['date']} (ID: {mid}) 核心机构赔率...", end="\r")
        b_odds = fetch_historical_odds(mid, target_books)
        m_copy = dict(m)
        m_copy["bookmaker_odds"] = b_odds
        h2h_full_data.append(m_copy)
    print(f"\n全部 {len(matches)} 场历史盘口匹配完成！")
    
    # 汇总统计
    total_matches = len(matches)
    h_wins = sum(1 for m in matches if m["outcome"] == "主胜")
    draws = sum(1 for m in matches if m["outcome"] == "平局")
    a_wins = sum(1 for m in matches if m["outcome"] == "客胜")
    
    summary_pack = {
        "schedule_id": str(schedule_id),
        "fixture": f"{home_team} vs {away_team}",
        "home_team": home_team,
        "away_team": away_team,
        "total_h2h_records": total_matches,
        "statistics": {
            "home_wins": h_wins,
            "draws": draws,
            "away_wins": a_wins,
            "home_win_rate": f"{(h_wins/total_matches)*100:.1f}%" if total_matches else "0%"
        },
        "h2h_matches": h2h_full_data,
        "evidence_file": os.path.relpath(raw_file, out_dir),
        "evidence_sha256": sha_html
    }
    
    out_file = os.path.join(parsed_dir, f"{schedule_id}_h2h_odds_trajectory.json")
    with open(out_file, "w", encoding="utf-8") as wf:
        json.dump(summary_pack, wf, ensure_ascii=False, indent=2)
        
    print(f"✅ 历史交锋盘口全景数据已保存至: {out_file}")
    return summary_pack

def main():
    parser = argparse.ArgumentParser(description="历史交锋与核心机构初终盘走势自动化采集器")
    parser.add_argument("--schedule_id", required=True, help="球探赛事 ID，如 3003865")
    parser.add_argument("--output_dir", required=True, help="交付包输出根目录")
    parser.add_argument("--bookmakers", nargs="*", default=["William Hill", "Macauslot"], help="指定提取的博彩公司")
    args = parser.parse_args()
    
    collect_h2h_pipeline(args.schedule_id, args.output_dir, args.bookmakers)

if __name__ == "__main__":
    main()
