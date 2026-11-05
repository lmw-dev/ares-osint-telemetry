#!/usr/bin/env python3
"""
h2h_extractor.py
================
从 Titan007 单场分析页 (https://zq.titan007.com/analysis/{match_id}cn.htm)
中高稳定性抽取「对赛往绩」结构化数据。

遵循原则：
1. 稳定 > 完整 > 丰富；
2. 只解析页面实际存在的字段，不进行全网搜索，不调用其他数据源；
3. 不把球会友谊静默混入正式比赛统计；
4. 仅输出客观事实统计，绝不输出任何主观投注预测。
"""

import re
import ast
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

def safe_float(val, default=None):
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def safe_int(val, default=0):
    if val is None or val == "":
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default

def parse_full_date(raw_date_str):
    """
    将 Titan007 的 YY-MM-DD 转换为 YYYY-MM-DD
    """
    if not raw_date_str:
        return ""
    parts = raw_date_str.strip().split("-")
    if len(parts) == 3:
        yr = safe_int(parts[0], 0)
        full_yr = f"20{yr:02d}" if yr <= 50 else f"19{yr:02d}"
        return f"{full_yr}-{parts[1]}-{parts[2]}"
    return raw_date_str

def clean_team_name(raw_name_html):
    if not raw_name_html:
        return ""
    if "<" in raw_name_html:
        soup = BeautifulSoup(raw_name_html, "html.parser")
        return soup.get_text(strip=True)
    return raw_name_html.strip()

def is_friendly_competition(comp_name):
    if not comp_name:
        return False
    keywords = ["友谊", "球会友谊", "热身", "Friendly", "Club Friendly"]
    for kw in keywords:
        if kw in comp_name:
            return True
    return False

def calculate_h2h_stats(records, current_home, current_away):
    """
    只做轻量事实统计，绝不输出任何主观分析结论。
    """
    if not records:
        return {
            "matches": 0,
            "current_home_wins": 0,
            "draws": 0,
            "current_away_wins": 0,
            "current_home_goals": 0,
            "current_away_goals": 0,
            "avg_total_goals": 0.0,
            "over_2_5_count": 0,
            "btts_count": 0,
            "handicap_win_count": 0,
            "handicap_push_count": 0,
            "handicap_loss_count": 0,
            "handicap_unresolved_count": 0,
            "historical_ou_provenance": "NOT_AVAILABLE_IN_SOURCE"
        }

    c_h_wins = 0
    draws = 0
    c_a_wins = 0
    c_h_goals = 0
    c_a_goals = 0
    over_2_5 = 0
    btts = 0
    
    hk_win = 0
    hk_push = 0
    hk_loss = 0
    hk_unresolved = 0

    clean_c_home = clean_team_name(current_home)
    clean_c_away = clean_team_name(current_away)

    for r in records:
        h_score = safe_int(r.get("home_goals"), 0)
        a_score = safe_int(r.get("away_goals"), 0)
        tot_goals = h_score + a_score

        if tot_goals > 2.5:
            over_2_5 += 1
        if h_score > 0 and a_score > 0:
            btts += 1

        r_home = r.get("historical_home_team") or r.get("home_team", "")
        r_away = r.get("historical_away_team") or r.get("away_team", "")

        # 归属严格判定
        is_h_as_home = (clean_c_home in r_home or r_home in clean_c_home) if (clean_c_home and r_home) else False
        is_h_as_away = (clean_c_home in r_away or r_away in clean_c_home) if (clean_c_home and r_away) else False

        if is_h_as_home and not is_h_as_away:
            c_h_goals += h_score
            c_a_goals += a_score
            if h_score > a_score:
                c_h_wins += 1
            elif h_score == a_score:
                draws += 1
            else:
                c_a_wins += 1
        elif is_h_as_away and not is_h_as_home:
            # 当前主队在历史交锋中作为客队
            c_h_goals += a_score
            c_a_goals += h_score
            if a_score > h_score:
                c_h_wins += 1
            elif a_score == h_score:
                draws += 1
            else:
                c_a_wins += 1
        else:
            # 身份未能确定时（未匹配或双向模糊匹配），绝对不静默计入胜平负，避免数据污染
            pass

        # 盘路统计（严格以当前主队视角 current_home_handicap_result 为准）
        # 仅当明确解析为 WIN / PUSH / LOSS 时才计入，UNKNOWN 坚决不进入胜走负计数！
        hr = r.get("current_home_handicap_result") or r.get("handicap_result")
        if hr == "WIN":
            hk_win += 1
        elif hr == "PUSH":
            hk_push += 1
        elif hr == "LOSS":
            hk_loss += 1
        else:
            hk_unresolved += 1

    total_m = len(records)
    avg_goals = round((c_h_goals + c_a_goals) / total_m, 2) if total_m > 0 else 0.0

    return {
        "matches": total_m,
        "current_home_wins": c_h_wins,
        "draws": draws,
        "current_away_wins": c_a_wins,
        "current_home_goals": c_h_goals,
        "current_away_goals": c_a_goals,
        "avg_total_goals": avg_goals,
        "over_2_5_count": over_2_5,
        "btts_count": btts,
        "handicap_win_count": hk_win,
        "handicap_push_count": hk_push,
        "handicap_loss_count": hk_loss,
        "handicap_unresolved_count": hk_unresolved,
        "historical_ou_provenance": "NOT_AVAILABLE_IN_SOURCE"
    }

def extract_h2h_data(html_content: str, current_home: str = "", current_away: str = "", match_time_str: str = "", source_url: str = "") -> dict:
    """
    核心抽取函数：输入 HTML 文本，输出规范 h2h.json 字典。
    """
    retrieved_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    
    if not html_content or not html_content.strip():
        return {
            "source": "Titan007",
            "source_url": source_url,
            "retrieved_at_utc": retrieved_utc,
            "status": "NO_DATA",
            "available_count": 0,
            "records": [],
            "last_10": {"count": 0, "stats": calculate_h2h_stats([], current_home, current_away), "matches": []},
            "last_5_years": {"count": 0, "stats": calculate_h2h_stats([], current_home, current_away), "matches": []},
            "same_venue": {"same_venue_status": "NOT_AVAILABLE", "count": 0, "stats": calculate_h2h_stats([], current_home, current_away), "matches": []},
            "official_matches_count": 0,
            "friendly_matches_count": 0,
            "message": "HTML content empty"
        }

    # 1. 尝试解析 v_data
    try:
        m_v = re.search(r"var\s+v_data\s*=\s*(\[.*?\]);", html_content, re.DOTALL)
        if not m_v:
            return {
                "source": "Titan007",
                "source_url": source_url,
                "retrieved_at_utc": retrieved_utc,
                "status": "NO_DATA",
                "available_count": 0,
                "records": [],
                "last_10": {"count": 0, "stats": calculate_h2h_stats([], current_home, current_away), "matches": []},
                "last_5_years": {"count": 0, "stats": calculate_h2h_stats([], current_home, current_away), "matches": []},
                "same_venue": {"same_venue_status": "NOT_AVAILABLE", "count": 0, "stats": calculate_h2h_stats([], current_home, current_away), "matches": []},
                "official_matches_count": 0,
                "friendly_matches_count": 0,
                "message": "v_data not found in page (no historical head-to-head matches)"
            }
            
        v_data = ast.literal_eval(m_v.group(1))
    except Exception as e:
        return {
            "source": "Titan007",
            "source_url": source_url,
            "retrieved_at_utc": retrieved_utc,
            "status": "PARSE_FAILED",
            "available_count": 0,
            "records": [],
            "error": f"Failed to parse v_data AST: {str(e)}",
            "last_10": {"count": 0, "stats": calculate_h2h_stats([], current_home, current_away), "matches": []},
            "last_5_years": {"count": 0, "stats": calculate_h2h_stats([], current_home, current_away), "matches": []},
            "same_venue": {"same_venue_status": "NOT_AVAILABLE", "count": 0, "stats": calculate_h2h_stats([], current_home, current_away), "matches": []}
        }

    if not v_data or len(v_data) == 0:
        return {
            "source": "Titan007",
            "source_url": source_url,
            "retrieved_at_utc": retrieved_utc,
            "status": "NO_DATA",
            "available_count": 0,
            "records": [],
            "last_10": {"count": 0, "stats": calculate_h2h_stats([], current_home, current_away), "matches": []},
            "last_5_years": {"count": 0, "stats": calculate_h2h_stats([], current_home, current_away), "matches": []},
            "same_venue": {"same_venue_status": "NOT_AVAILABLE", "count": 0, "stats": calculate_h2h_stats([], current_home, current_away), "matches": []},
            "official_matches_count": 0,
            "friendly_matches_count": 0,
            "message": "v_data is empty"
        }

    # 2. 提取盘口与赔率字典 Vs_hOdds 和 Vs_eOdds
    h_map = {}
    try:
        m_h = re.search(r"var\s+Vs_hOdds\s*=\s*(\[.*?\]);", html_content, re.DOTALL)
        if m_h:
            arr_h = ast.literal_eval(m_h.group(1))
            for row in arr_h:
                mid = str(row[0])
                cid = safe_int(row[1], -1)
                if mid not in h_map:
                    h_map[mid] = {}
                h_map[mid][cid] = row
    except Exception:
        h_map = {}

    e_map = {}
    try:
        m_e = re.search(r"var\s+Vs_eOdds\s*=\s*(\[.*?\]);", html_content, re.DOTALL)
        if m_e:
            arr_e = ast.literal_eval(m_e.group(1))
            for row in arr_e:
                mid = str(row[0])
                cid = safe_int(row[1], -1)
                if mid not in e_map:
                    e_map[mid] = {}
                e_map[mid][cid] = row
    except Exception:
        e_map = {}

    # 3. 确定当前基准时间（向前 5 年）
    now_dt = datetime.utcnow()
    if match_time_str:
        for fmt in ["%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"]:
            try:
                now_dt = datetime.strptime(match_time_str.split(".")[0].replace("Z", ""), fmt)
                break
            except ValueError:
                pass
    cutoff_5_years = now_dt - timedelta(days=5 * 365.25)

    # 4. 遍历 v_data 解析每一条记录
    clean_c_home = clean_team_name(current_home)
    clean_c_away = clean_team_name(current_away)

    records = []
    has_partial = False

    for row in v_data:
        try:
            full_date = parse_full_date(row[0])
            comp = str(row[2])
            is_friendly = is_friendly_competition(comp)
            
            h_team = clean_team_name(str(row[5]))
            a_team = clean_team_name(str(row[7]))
            
            score_h = safe_int(row[8], 0)
            score_a = safe_int(row[9], 0)
            ft_score = f"{score_h}-{score_a}"
            ht_score = str(row[10]) if len(row) > 10 and row[10] else None
            
            # 角球
            corners_h = row[16] if len(row) > 16 else None
            corners_a = row[17] if len(row) > 17 else None
            corners_str = f"{corners_h}-{corners_a}" if (corners_h is not None and corners_a is not None and str(corners_h).isdigit() and str(corners_a).isdigit()) else None
            
            # 胜平负 (从比赛自身主客场视角)
            if score_h > score_a:
                wdl = "W"
            elif score_h == score_a:
                wdl = "D"
            else:
                wdl = "L"
                
            # 盘路结果: row[13]: 1=赢, 0=走, -1=输
            raw_pan = safe_int(row[13], None) if len(row) > 13 else None
            if raw_pan == 1:
                hk_res = "WIN"
            elif raw_pan == 0:
                hk_res = "PUSH"
            elif raw_pan == -1:
                hk_res = "LOSS"
            else:
                hk_res = None
                
            # 大小球结果: row[14]: 1=大球, 0=走, -1=小球
            raw_ou = safe_int(row[14], None) if len(row) > 14 else None
            if raw_ou == 1:
                tot_res = "OVER"
            elif raw_ou == 0:
                tot_res = "PUSH"
            elif raw_ou == -1:
                tot_res = "UNDER"
            else:
                tot_res = None
                
            mid = str(row[15]) if len(row) > 15 else ""
            
            # 从 Vs_hOdds 提取亚盘初盘 (优先 Macauslot=1, Crown=3, Bet365=8)
            h_cand = h_map.get(mid, {})
            ah_row = h_cand.get(1) or h_cand.get(3) or h_cand.get(8) or (list(h_cand.values())[0] if h_cand else None)
            
            ah_h_price = safe_float(ah_row[2]) if ah_row and len(ah_row) > 2 else None
            ah_line = safe_float(ah_row[3]) if ah_row and len(ah_row) > 3 else (ah_row[3] if ah_row and len(ah_row) > 3 else None)
            ah_a_price = safe_float(ah_row[4]) if ah_row and len(ah_row) > 4 else None

            # 从 Vs_eOdds 提取欧赔初盘 (优先 威廉希尔=115, 澳门=80, 均值=0)
            e_cand = e_map.get(mid, {})
            eu_row = e_cand.get(115) or e_cand.get(80) or e_cand.get(0) or (list(e_cand.values())[0] if e_cand else None)
            
            eu_h_odds = safe_float(eu_row[2]) if eu_row and len(eu_row) > 2 else None
            eu_d_odds = safe_float(eu_row[3]) if eu_row and len(eu_row) > 3 else None
            eu_a_odds = safe_float(eu_row[4]) if eu_row and len(eu_row) > 4 else None

            # 盘路计算与主客方向校正 (AH Orientation Normalization)
            # ah_line 来自 Vs_hOdds，其数值在 Titan007 中永远表示历史主队 (h_team) 的让球线
            hist_home_hk_res = None
            curr_home_hk_res = None
            orientation_status = "NOT_APPLICABLE"

            if ah_line is not None and isinstance(ah_line, (int, float)):
                diff_hist = (score_h - score_a) - ah_line
                if diff_hist > 0:
                    hist_home_hk_res = "WIN"
                elif diff_hist == 0:
                    hist_home_hk_res = "PUSH"
                else:
                    hist_home_hk_res = "LOSS"

                # 严格判定当前分析主队 (current_home) 在历史比赛中的归属
                is_curr_h_as_hist_h = (clean_c_home in h_team or h_team in clean_c_home) if (clean_c_home and h_team) else False
                is_curr_h_as_hist_a = (clean_c_home in a_team or a_team in clean_c_home) if (clean_c_home and a_team) else False

                if is_curr_h_as_hist_h and not is_curr_h_as_hist_a:
                    orientation_status = "MATCHED_AS_HOME"
                    curr_home_hk_res = hist_home_hk_res
                elif is_curr_h_as_hist_a and not is_curr_h_as_hist_h:
                    orientation_status = "MATCHED_AS_AWAY"
                    # 当前主队作为历史客队，其盘路与历史主队严格相反
                    if hist_home_hk_res == "WIN":
                        curr_home_hk_res = "LOSS"
                    elif hist_home_hk_res == "LOSS":
                        curr_home_hk_res = "WIN"
                    else:
                        curr_home_hk_res = "PUSH"
                else:
                    # 关键安全防线：若无法可靠确定归属（未匹配或双向模糊冲突），绝不默认继承历史主队方向！
                    # 显式置为 UNRESOLVED_TEAM_IDENTITY 与 UNKNOWN，杜绝 silent contamination
                    orientation_status = "UNRESOLVED_TEAM_IDENTITY"
                    curr_home_hk_res = "UNKNOWN"
            else:
                has_partial = True

            # 大小球结果核验与溯源闭环 (Historical OU Line Provenance)
            # 经过深度 DOM 与脚本审计，Titan007 分析页未提供历史各场的进球大小盘口数组 (无 Vs_oOdds)。
            # 页面内 v_data[14] 仅为 (score_h + score_a >= 3) 的硬编码 2.5 球标志位，绝非博彩机构大小球盘口。
            # 为严格遵守 Ares 零幻觉与证据闭环原则，total_line 标为 None，total_result 标为 "UNKNOWN"，
            # 仅保留纯客观进球数事实标志 total_goals_2_5_fact ("OVER" / "UNDER")。
            tot_goals = score_h + score_a
            tot_2_5_fact = "OVER" if tot_goals > 2.5 else "UNDER"

            # 时间判定
            in_5y = False
            try:
                m_dt = datetime.strptime(full_date, "%Y-%m-%d")
                in_5y = (m_dt >= cutoff_5_years)
            except Exception:
                pass
                
            # 主客相同判定 (本次主队在主场面对本次客队)
            is_same_venue = False
            if clean_c_home and clean_c_away:
                if (clean_c_home in h_team or h_team in clean_c_home) and (clean_c_away in a_team or a_team in clean_c_away):
                    is_same_venue = True

            records.append({
                "match_id": mid,
                "match_date": full_date,
                "competition": comp,
                "is_friendly": is_friendly,
                "historical_home_team": h_team,
                "historical_away_team": a_team,
                "home_team": h_team,
                "away_team": a_team,
                "home_goals": score_h,
                "away_goals": score_a,
                "full_time_score": ft_score,
                "half_time_score": ht_score,
                "corners": corners_str,
                "asian_handicap_line": ah_line,
                "asian_handicap_home_price": ah_h_price,
                "asian_handicap_away_price": ah_a_price,
                "orientation_status": orientation_status,
                "historical_home_handicap_result": hist_home_hk_res,
                "current_home_handicap_result": curr_home_hk_res,
                "handicap_result": curr_home_hk_res,  # 统一以当前主队视角为标准 (未解析则为 UNKNOWN)
                "european_home_odds": eu_h_odds,
                "european_draw_odds": eu_d_odds,
                "european_away_odds": eu_a_odds,
                "result_wdl": wdl,
                "total_line": None,
                "total_result": "UNKNOWN",
                "total_result_provenance": "NOT_AVAILABLE_IN_SOURCE (Titan007 analysis page provides no historical OU line array)",
                "total_goals_2_5_fact": tot_2_5_fact,
                "in_last_5_years": in_5y,
                "is_same_venue": is_same_venue
            })
        except Exception as err:
            has_partial = True
            continue

    if not records:
        return {
            "source": "Titan007",
            "source_url": source_url,
            "retrieved_at_utc": retrieved_utc,
            "status": "PARSE_FAILED",
            "field_coverage": {
                "fixture_records": "NO_DATA",
                "match_results": "NO_DATA",
                "asian_handicap": "NO_DATA",
                "european_odds": "NO_DATA",
                "total_goals_fact": "NO_DATA",
                "historical_ou_line": "NOT_AVAILABLE_IN_SOURCE",
                "total_result": "UNKNOWN"
            },
            "available_count": 0,
            "records": [],
            "error": "Failed to parse any match records from v_data",
            "last_10": {"count": 0, "stats": calculate_h2h_stats([], current_home, current_away), "matches": []},
            "last_5_years": {"count": 0, "stats": calculate_h2h_stats([], current_home, current_away), "matches": []},
            "same_venue": {"same_venue_status": "NOT_AVAILABLE", "count": 0, "stats": calculate_h2h_stats([], current_home, current_away), "matches": []}
        }

    # 5. 视图划分（比赛类型分离：统计以正式比赛为主，球会友谊不静默混入）
    official_records = [r for r in records if not r["is_friendly"]]
    friendly_records = [r for r in records if r["is_friendly"]]

    # (1) last_10
    l10_records = records[:10]
    l10_official = [r for r in l10_records if not r["is_friendly"]]
    l10_view = {
        "count": len(l10_records),
        "official_count": len(l10_official),
        "friendly_count": len(l10_records) - len(l10_official),
        "stats": calculate_h2h_stats(l10_official, current_home, current_away),
        "matches": l10_records
    }

    # (2) last_5_years
    l5y_records = [r for r in records if r["in_last_5_years"]]
    l5y_official = [r for r in l5y_records if not r["is_friendly"]]
    l5y_view = {
        "count": len(l5y_records),
        "official_count": len(l5y_official),
        "friendly_count": len(l5y_records) - len(l5y_official),
        "stats": calculate_h2h_stats(l5y_official, current_home, current_away),
        "matches": l5y_records
    }

    # (3) same_venue
    sv_records = [r for r in records if r["is_same_venue"]]
    sv_official = [r for r in sv_records if not r["is_friendly"]]
    same_venue_status = "AVAILABLE" if (clean_c_home and clean_c_away) else "NOT_AVAILABLE"
    same_venue_view = {
        "same_venue_status": same_venue_status,
        "count": len(sv_records),
        "official_count": len(sv_official),
        "friendly_count": len(sv_records) - len(sv_official),
        "stats": calculate_h2h_stats(sv_official, current_home, current_away),
        "matches": sv_records
    }

    # 6. 状态判定与字段覆盖度解耦 (Decoupled Status & Field Coverage)
    # 严格门禁：亚盘合格要求 1) 存在有效盘口 line 且 2) 主客 orientation 必须确凿解析 (MATCHED_AS_HOME 或 MATCHED_AS_AWAY)
    # 若存在任何一场 UNRESOLVED_TEAM_IDENTITY，asian_handicap 覆盖度自动降为 PARTIAL
    ah_valid_and_resolved = sum(
        1 for r in records
        if r.get("asian_handicap_line") is not None and r.get("orientation_status") in ("MATCHED_AS_HOME", "MATCHED_AS_AWAY")
    )
    eu_valid_count = sum(1 for r in records if r.get("european_home_odds") is not None)

    field_coverage = {
        "fixture_records": "COMPLETE",
        "match_results": "COMPLETE",
        "asian_handicap": "COMPLETE" if (ah_valid_and_resolved == len(records) and len(records) > 0) else ("PARTIAL" if ah_valid_and_resolved > 0 else "NO_DATA"),
        "european_odds": "COMPLETE" if eu_valid_count == len(records) else ("PARTIAL" if eu_valid_count > 0 else "NO_DATA"),
        "total_goals_fact": "COMPLETE",
        "historical_ou_line": "NOT_AVAILABLE_IN_SOURCE",
        "total_result": "UNKNOWN"
    }

    # status 代表 H2H 提取流水线成功执行与样本充足度 (满足10场样本即为 COMPLETE)
    status = "COMPLETE"
    if len(records) < 10:
        status = "PARTIAL"

    return {
        "source": "Titan007",
        "source_url": source_url or f"https://zq.titan007.com/analysis/unknowncn.htm",
        "retrieved_at_utc": retrieved_utc,
        "status": status,
        "field_coverage": field_coverage,
        "available_count": len(records),
        "official_matches_count": len(official_records),
        "friendly_matches_count": len(friendly_records),
        "records": records,
        "last_10": l10_view,
        "last_5_years": l5y_view,
        "same_venue": same_venue_view
    }

if __name__ == "__main__":
    import sys
    import json
    if len(sys.argv) > 1:
        with open(sys.argv[1], "r", encoding="utf-8") as f:
            content = f.read()
        res = extract_h2h_data(content, "巴伦西亚", "巴塞罗那", "2026-09-06 22:15")
        print(json.dumps(res, ensure_ascii=False, indent=2))
