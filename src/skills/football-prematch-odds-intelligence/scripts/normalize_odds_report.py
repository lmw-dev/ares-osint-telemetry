#!/usr/bin/env python3
"""Ares V2.1 赔率量化研判与清洗引擎 (Quant Engine)

本模块负责赛前公司级欧赔、亚盘、大小球赔率的统一清洗、精准平均值计算、
水位高低档量化、博弈信号推导及 market_tags 与 risk_tags 的自动化生成。
为上层大模型及 Ares 决策系统提供极致防错、完全结构化的比赛日赔率标准数据包。
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

CANONICAL_BOOKMAKERS = [
    "威廉", "澳门", "立博", "365", "易胜博", "伟德", "Pinnacle/平博", "Betfair/交易所类"
]

ALIASES = {
    "威廉希尔": "威廉", "William": "威廉", "William Hill": "威廉",
    "澳彩": "澳门", "Macauslot": "澳门", "澳门彩票": "澳门",
    "Ladbrokes": "立博", "利记": "立博", "立博国际": "立博",
    "Bet365": "365", "bet365": "365",
    "Easbet": "易胜博", "易胜": "易胜博",
    "BetVictor": "伟德", "Victor Chandler": "伟德", "伟德亚洲": "伟德",
    "Pinnacle": "Pinnacle/平博", "平博": "Pinnacle/平博", "Pinnacle Sports": "Pinnacle/平博",
    "Betfair": "Betfair/交易所类", "交易所": "Betfair/交易所类", "Exchange": "Betfair/交易所类",
}


def canon_bookmaker(name: str) -> str:
    """标准化博彩公司名称"""
    name_str = str(name).strip()
    return ALIASES.get(name_str, name_str)


def fnum(x: Any) -> Optional[float]:
    """安全转换为浮点数"""
    if x is None or x == "":
        return None
    try:
        v = float(x)
        if math.isnan(v):
            return None
        return v
    except (ValueError, TypeError):
        return None


def parse_handicap(h: Any) -> Optional[float]:
    """将亚盘让球盘口解析为标准的主队视角浮点数（负数代表主队让球，正数代表主队受让）"""
    if h is None:
        return None
    if isinstance(h, (int, float)):
        return float(h)
    
    s = str(h).strip().replace(" ", "")
    if not s:
        return None
    
    # 尝试直接解析
    try:
        return float(s)
    except ValueError:
        pass
    
    # 中文及特殊字符解析
    sign = -1.0  # 默认主让视角（负数）
    if "受" in s or "客让" in s or "+" in s:
        sign = 1.0
    
    # 清洗中文词汇
    s_clean = s.replace("主让", "").replace("客让", "").replace("受", "").replace("让", "").replace("+", "").replace("-", "")
    
    mapping = {
        "平手": 0.0,
        "平/半": 0.25, "平半": 0.25,
        "半球": 0.5,
        "半/一": 0.75, "半一": 0.75,
        "一球": 1.0,
        "一/球半": 1.25, "一球半": 1.25,
        "球半": 1.5,
        "球半/两": 1.75, "球半/二": 1.75, "球半两": 1.75,
        "两球": 2.0, "二球": 2.0,
        "两/两球半": 2.25, "两球/两球半": 2.25, "两球半": 2.5,
        "三球": 3.0
    }
    
    for k, v in mapping.items():
        if k in s_clean:
            return sign * v if v != 0.0 else 0.0
            
    # 尝试解析类似 -0.25 或 -0/0.5 或 -0.5/1 等斜杠格式
    for sep in ["/", "-", "或"]:
        if sep in s_clean:
            parts = s_clean.split(sep)
            try:
                p1 = abs(float(parts[0]))
                p2 = abs(float(parts[1]))
                val = (p1 + p2) / 2.0
                if s.startswith("-"):
                    return -val
                elif s.startswith("+"):
                    return val
                return sign * val
            except ValueError:
                pass
                
    return None


def parse_total_line(l: Any) -> Optional[float]:
    """将大小球盘口解析为标准浮点数"""
    if l is None:
        return None
    if isinstance(l, (int, float)):
        return float(l)
    
    s = str(l).strip().replace(" ", "")
    if not s:
        return None
    
    try:
        return float(s)
    except ValueError:
        pass
        
    s_clean = s.replace("球", "")
    for sep in ["/", "-", "或"]:
        if sep in s_clean:
            parts = s_clean.split(sep)
            try:
                p1 = float(parts[0])
                p2 = float(parts[1])
                return (p1 + p2) / 2.0
            except ValueError:
                pass
    return None


def calculate_average_euro(euro_dict: Dict[str, Any]) -> Tuple[Dict[str, Optional[float]], Dict[str, Optional[float]]]:
    """计算欧赔初盘与即时的均值"""
    init_home, init_draw, init_away = [], [], []
    curr_home, curr_draw, curr_away = [], [], []
    
    for bookmaker, data in euro_dict.items():
        if not isinstance(data, dict) or data.get("status") == "source_missing":
            continue
        init = data.get("initial")
        curr = data.get("current")
        
        if isinstance(init, list) and len(init) >= 3:
            h, d, a = fnum(init[0]), fnum(init[1]), fnum(init[2])
            if h is not None and d is not None and a is not None:
                init_home.append(h)
                init_draw.append(d)
                init_away.append(a)
                
        if isinstance(curr, list) and len(curr) >= 3:
            h, d, a = fnum(curr[0]), fnum(curr[1]), fnum(curr[2])
            if h is not None and d is not None and a is not None:
                curr_home.append(h)
                curr_draw.append(d)
                curr_away.append(a)
                
    def mean(lst: List[float]) -> Optional[float]:
        return round(sum(lst) / len(lst), 3) if lst else None
        
    return (
        {"home": mean(init_home), "draw": mean(init_draw), "away": mean(init_away)},
        {"home": mean(curr_home), "draw": mean(curr_draw), "away": mean(curr_away)}
    )


def calculate_average_asian(asian_dict: Dict[str, Any]) -> Tuple[Dict[str, Optional[float]], Dict[str, Optional[float]]]:
    """计算亚盘初盘与即时的均值"""
    init_hwater, init_handicap, init_awater = [], [], []
    curr_hwater, curr_handicap, curr_awater = [], [], []
    
    for bookmaker, data in asian_dict.items():
        if not isinstance(data, dict) or data.get("status") == "source_missing":
            continue
        init = data.get("initial")
        curr = data.get("current")
        
        if isinstance(init, list) and len(init) >= 3:
            hw, hc, aw = fnum(init[0]), parse_handicap(init[1]), fnum(init[2])
            if hw is not None and hc is not None and aw is not None:
                init_hwater.append(hw)
                init_handicap.append(hc)
                init_awater.append(aw)
                
        if isinstance(curr, list) and len(curr) >= 3:
            hw, hc, aw = fnum(curr[0]), parse_handicap(curr[1]), fnum(curr[2])
            if hw is not None and hc is not None and aw is not None:
                curr_hwater.append(hw)
                curr_handicap.append(hc)
                curr_awater.append(aw)
                
    def mean(lst: List[float]) -> Optional[float]:
        return round(sum(lst) / len(lst), 3) if lst else None
        
    return (
        {"home_water": mean(init_hwater), "handicap": mean(init_handicap), "away_water": mean(init_awater)},
        {"home_water": mean(curr_hwater), "handicap": mean(curr_handicap), "away_water": mean(curr_awater)}
    )


def calculate_average_total(total_dict: Dict[str, Any]) -> Tuple[Dict[str, Optional[float]], Dict[str, Optional[float]]]:
    """计算大小球初盘与即时的均值"""
    init_owater, init_line, init_uwater = [], [], []
    curr_owater, curr_line, curr_uwater = [], [], []
    
    for bookmaker, data in total_dict.items():
        if not isinstance(data, dict) or data.get("status") == "source_missing":
            continue
        init = data.get("initial")
        curr = data.get("current")
        
        if isinstance(init, list) and len(init) >= 3:
            ow, line, uw = fnum(init[0]), parse_total_line(init[1]), fnum(init[2])
            if ow is not None and line is not None and uw is not None:
                init_owater.append(ow)
                init_line.append(line)
                init_uwater.append(uw)
                
        if isinstance(curr, list) and len(curr) >= 3:
            ow, line, uw = fnum(curr[0]), parse_total_line(curr[1]), fnum(curr[2])
            if ow is not None and line is not None and uw is not None:
                curr_owater.append(ow)
                curr_line.append(line)
                curr_uwater.append(uw)
                
    def mean(lst: List[float]) -> Optional[float]:
        return round(sum(lst) / len(lst), 3) if lst else None
        
    return (
        {"over_water": mean(init_owater), "line": mean(init_line), "under_water": mean(init_uwater)},
        {"over_water": mean(curr_owater), "line": mean(curr_line), "under_water": mean(curr_uwater)}
    )


def fmt_delta(delta: Optional[float], label: str) -> Optional[str]:
    if delta is None or abs(delta) < 0.005:
        return None
    return f"{label}{'升' if delta > 0 else '降'}{abs(delta):.2f}"


def euro_change(initial: Iterable[Any], current: Iterable[Any]) -> str:
    init = [fnum(x) for x in initial]
    cur = [fnum(x) for x in current]
    if len(init) < 3 or len(cur) < 3 or any(v is None for v in init[:3] + cur[:3]):
        return "数据不足"
    labels = ["主胜", "平赔", "客胜"]
    deltas = [cur[i] - init[i] for i in range(3)]
    parts = [fmt_delta(d, labels[i]) for i, d in enumerate(deltas)]
    parts = [p for p in parts if p]
    return "；".join(parts) if parts else "无明显变化"


def handicap_change(initial: Iterable[Any], current: Iterable[Any]) -> str:
    init = [fnum(initial[0]), parse_handicap(initial[1]), fnum(initial[2])]
    cur = [fnum(current[0]), parse_handicap(current[1]), fnum(current[2])]
    if any(v is None for v in init + cur):
        return "数据不足"
    parts: List[str] = []
    if init[1] is not None and cur[1] is not None and abs(cur[1] - init[1]) >= 0.005:
        parts.append(f"盘口 {init[1]:g}→{cur[1]:g}")
    p = fmt_delta(cur[0] - init[0], "主水")
    if p:
        parts.append(p)
    p = fmt_delta(cur[2] - init[2], "客水")
    if p:
        parts.append(p)
    return "；".join(parts) if parts else "无明显变化"


def total_change(initial: Iterable[Any], current: Iterable[Any]) -> str:
    init = [fnum(initial[0]), parse_total_line(initial[1]), fnum(initial[2])]
    cur = [fnum(current[0]), parse_total_line(current[1]), fnum(current[2])]
    if any(v is None for v in init + cur):
        return "数据不足"
    parts: List[str] = []
    if init[1] is not None and cur[1] is not None and abs(cur[1] - init[1]) >= 0.005:
        parts.append(f"大小球 {init[1]:g}→{cur[1]:g}")
    p = fmt_delta(cur[0] - init[0], "大球水")
    if p:
        parts.append(p)
    p = fmt_delta(cur[2] - init[2], "小球水")
    if p:
        parts.append(p)
    return "；".join(parts) if parts else "无明显变化"


def normalize_bookmaker_dict(d: Dict[str, Any], canonical_keys: List[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {k: {"status": "source_missing"} for k in canonical_keys}
    for k, v in (d or {}).items():
        canon = canon_bookmaker(k)
        if canon in out:
            out[canon] = v
        else:
            out[canon] = v
    return out


def get_coverage_info(d: Dict[str, Any], expected_keys: List[str]) -> Dict[str, Any]:
    """生成 company_coverage 中细化的 active、expected、coverage_rate 和 missing 公司"""
    missing = []
    active_count = 0
    for k in expected_keys:
        val = d.get(k)
        if isinstance(val, dict) and val.get("status") != "source_missing" and val.get("initial") is not None:
            active_count += 1
        else:
            missing.append(k)
            
    expected = len(expected_keys)
    coverage_rate = round(active_count / expected, 3) if expected > 0 else 0.0
    return {
        "active": active_count,
        "expected": expected,
        "coverage_rate": coverage_rate,
        "missing": missing
    }


def evaluate_confidence(count: int, high_threshold: int, medium_threshold: int) -> str:
    if count >= high_threshold:
        return "high"
    elif count >= medium_threshold:
        return "medium"
    return "low"


def run_quant_engine(match: Dict[str, Any]) -> Dict[str, Any]:
    """核心 Ares V2.1 量化判研引擎。输入可以是原始格式，亦可以是半结构化格式"""
    m = dict(match)
    
    # 提取比赛基本字段
    match_no = m.get("match_no", "01")
    home = m.get("home")
    away = m.get("away")
    kickoff = m.get("kickoff", "")
    
    if not home or not away:
        # 兼容老版 "match": "热刺 vs 埃弗顿" 格式
        match_str = m.get("match", "")
        if " vs " in match_str:
            parts = match_str.split(" vs ")
            home = parts[0].strip()
            away = parts[1].strip()
        elif "-" in match_str:
            parts = match_str.split("-")
            home = parts[0].strip()
            away = parts[1].strip()
        else:
            home = home or "未知主队"
            away = away or "未知客队"
            
    # 构建 P0 级防错块
    sanity_check = {
        "match_no": match_no,
        "home": home,
        "away": away,
        "euro_order": "home/draw/away",
        "asian_format": "home_water / handicap_from_home_view / away_water",
        "handicap_sign_rule": {
            "negative": "home_gives_ball",
            "positive": "home_receives_ball"
        },
        "total_format": "over_water / goal_line / under_water"
    }
    
    # 整理并规范化 raw 赔率数据
    raw_euro = m.get("odds_raw", {}).get("euro", m.get("priority_euro", {}))
    raw_asian = m.get("odds_raw", {}).get("asian", m.get("asian_handicap", {}))
    raw_total = m.get("odds_raw", {}).get("total", m.get("total_goals", {}))
    
    norm_euro = normalize_bookmaker_dict(raw_euro, CANONICAL_BOOKMAKERS)
    norm_asian = normalize_bookmaker_dict(raw_asian, CANONICAL_BOOKMAKERS)
    norm_total = normalize_bookmaker_dict(raw_total, ["365", "Pinnacle/平博"])
    
    # 对每家公司计算 last_change/key_change 并更新 status
    for k, v in norm_euro.items():
        if isinstance(v, dict) and v.get("initial") is not None and v.get("current") is not None:
            v["status"] = "active"
            v["last_change"] = euro_change(v["initial"], v["current"])
            v.setdefault("update_time", None)
    for k, v in norm_asian.items():
        if isinstance(v, dict) and v.get("initial") is not None and v.get("current") is not None:
            v["status"] = "active"
            v["key_change"] = handicap_change(v["initial"], v["current"])
            v.setdefault("update_time", None)
    for k, v in norm_total.items():
        if isinstance(v, dict) and v.get("initial") is not None and v.get("current") is not None:
            v["status"] = "active"
            v["key_change"] = total_change(v["initial"], v["current"])
            v.setdefault("update_time", None)
            
    odds_raw = {
        "euro": norm_euro,
        "asian": norm_asian,
        "total": norm_total
    }
    
    # 精密计算均值 odds_avg
    avg_euro_init, avg_euro_curr = calculate_average_euro(norm_euro)
    avg_asian_init, avg_asian_curr = calculate_average_asian(norm_asian)
    avg_total_init, avg_total_curr = calculate_average_total(norm_total)
    
    odds_avg = {
        "euro": {"initial": avg_euro_init, "current": avg_euro_curr},
        "asian": {"initial": avg_asian_init, "current": avg_asian_curr},
        "total": {"initial": avg_total_init, "current": avg_total_curr}
    }
    
    # ---------------- 自动化博弈信号与标签研判引擎 ----------------
    euro_signal = "STABLE"
    asian_signal = "STABLE"
    total_signal = "STABLE"
    euro_asian_split = False
    
    market_tags = []
    risk_tags = []
    
    # 1. 欧赔方向信号与打标
    eh_init, ed_init, ea_init = avg_euro_init.get("home"), avg_euro_init.get("draw"), avg_euro_init.get("away")
    eh_curr, ed_curr, ea_curr = avg_euro_curr.get("home"), avg_euro_curr.get("draw"), avg_euro_curr.get("away")
    
    if eh_init is not None and eh_curr is not None:
        delta_home = eh_curr - eh_init
        if delta_home <= -0.05:
            euro_signal = "HOME_STRENGTHENED"
            market_tags.append("HOME_EURO_STRENGTHENED")
            
    if ea_init is not None and ea_curr is not None:
        delta_away = ea_curr - ea_init
        if delta_away <= -0.08:
            euro_signal = "AWAY_STRENGTHENED"
            market_tags.append("AWAY_EURO_STRENGTHENED")
            
    if ed_init is not None and ed_curr is not None:
        delta_draw = ed_curr - ed_init
        if delta_draw <= -0.15:
            market_tags.append("DRAW_EURO_COMPRESSED")
            if euro_signal == "STABLE":
                euro_signal = "DRAW_COMPRESSED"
                
    # 2. 亚盘方向信号与打标
    ah_init = avg_asian_init.get("handicap")
    ah_curr = avg_asian_curr.get("handicap")
    aw_init_h = avg_asian_init.get("home_water")
    aw_curr_h = avg_asian_curr.get("home_water")
    aw_init_a = avg_asian_init.get("away_water")
    aw_curr_a = avg_asian_curr.get("away_water")
    
    # 判定让球盘退盘或加深
    if ah_init is not None and ah_curr is not None:
        # 判定让球强队退让 (退盘)
        if (ah_init < 0 and ah_curr > ah_init) or (ah_init > 0 and ah_curr < ah_init):
            asian_signal = "FAVORITE_RETREAT"
            risk_tags.append("FAVORITE_RETREAT")
        # 主让盘加深 (h < 0) 且盘口变小
        elif ah_init < 0 and ah_curr < ah_init:
            asian_signal = "HOME_HANDICAP_DEEPENED"
        # 客让盘加深 (h > 0) 且盘口变大
        elif ah_init > 0 and ah_curr > ah_init:
            asian_signal = "AWAY_HANDICAP_DEEPENED"
        # 盘口维持一致
        elif abs(ah_curr - ah_init) < 0.01:
            if aw_init_h is not None and aw_curr_h is not None and (aw_curr_h - aw_init_h) <= -0.08 and aw_curr_h < 0.88:
                asian_signal = "HOME_WATER_SUPPORT"
                market_tags.append("HOME_HANDICAP_WATER_SUPPORT")
            elif aw_init_a is not None and aw_curr_a is not None and (aw_curr_a - aw_init_a) <= -0.08 and aw_curr_a < 0.88:
                asian_signal = "AWAY_WATER_SUPPORT"
                market_tags.append("AWAY_HANDICAP_WATER_SUPPORT")
                
        # 让步盘区间打标
        curr_handicap_abs = abs(ah_curr)
        if 0.0 <= curr_handicap_abs <= 0.5:
            market_tags.append("LOW_TO_MEDIUM_HANDICAP_FAVORITE")
        elif curr_handicap_abs >= 0.75:
            market_tags.append("DEEP_HANDICAP_FAVORITE")
            
        # 强队水位超载判定
        if ah_curr < 0 and aw_curr_h is not None and aw_curr_h >= 1.03:
            risk_tags.append("FAVORITE_NEAR_UPPER_BOUND")
        elif ah_curr > 0 and aw_curr_a is not None and aw_curr_a >= 1.03:
            risk_tags.append("FAVORITE_NEAR_UPPER_BOUND")
            
    # 3. 大小球方向信号与打标
    th_init = avg_total_init.get("line")
    th_curr = avg_total_curr.get("line")
    tw_init_o = avg_total_init.get("over_water")
    tw_curr_o = avg_total_curr.get("over_water")
    tw_init_u = avg_total_init.get("under_water")
    tw_curr_u = avg_total_curr.get("under_water")
    
    if th_init is not None and th_curr is not None:
        if th_curr > th_init:
            total_signal = "LINE_DEEPENED"
        elif th_curr < th_init:
            total_signal = "LINE_SHRINK"
        elif abs(th_curr - th_init) < 0.01:
            if tw_init_o is not None and tw_curr_o is not None and ((tw_curr_o - tw_init_o) <= -0.10 or tw_curr_o < 0.80):
                total_signal = "OVER_WATER_COMPRESSED"
                market_tags.append("OVER_WATER_COMPRESSED")
            elif tw_init_u is not None and tw_curr_u is not None and (tw_curr_u - tw_init_u) <= -0.10:
                total_signal = "UNDER_WATER_SUPPORT"
                market_tags.append("UNDER_WATER_COMPRESSED")
                
    # 4. 欧亚分裂与对齐研判
    if eh_init is not None and eh_curr is not None and aw_init_h is not None and aw_curr_h is not None:
        delta_eh = eh_curr - eh_init
        delta_ah_w = aw_curr_h - aw_init_h
        
        if delta_eh <= -0.05 and (delta_ah_w >= 0.06 or asian_signal == "FAVORITE_RETREAT"):
            euro_asian_split = True
        if delta_eh >= 0.05 and delta_ah_w <= -0.06:
            euro_asian_split = True
            
    if ea_init is not None and ea_curr is not None and aw_init_a is not None and aw_curr_a is not None:
        delta_ea = ea_curr - ea_init
        delta_ah_aw = aw_curr_a - aw_init_a
        
        if delta_ea <= -0.08 and (delta_ah_aw >= 0.06 or asian_signal == "FAVORITE_RETREAT"):
            euro_asian_split = True
        if delta_ea >= 0.08 and delta_ah_aw <= -0.06:
            euro_asian_split = True
            
    if euro_asian_split:
        risk_tags.append("EURO_ASIAN_SPLIT")
        
    # 判定双盘对齐（EURO_ASIAN_ALIGNED）
    if (euro_signal == "HOME_STRENGTHENED" and (asian_signal in ["HOME_WATER_SUPPORT", "HOME_HANDICAP_DEEPENED"])) or \
       (euro_signal == "AWAY_STRENGTHENED" and (asian_signal in ["AWAY_WATER_SUPPORT", "AWAY_HANDICAP_DEEPENED"])):
        market_tags.append("EURO_ASIAN_ALIGNED")
        
    # 5. 异常平局防守判定
    draw_drop_count = 0
    for bookmaker, data in norm_euro.items():
        if not isinstance(data, dict) or data.get("status") == "source_missing":
            continue
        init = data.get("initial")
        curr = data.get("current")
        if isinstance(init, list) and len(init) >= 3 and isinstance(curr, list) and len(curr) >= 3:
            i_draw = fnum(init[1])
            c_draw = fnum(curr[1])
            if i_draw is not None and c_draw is not None:
                if (c_draw - i_draw) <= -0.20 and c_draw < 3.10:
                    draw_drop_count += 1
                    
    if draw_drop_count >= 3:
        risk_tags.append("DRAW_COMPRESSED")
        
    # 6. 新增：收官轮次战意与轮换轻度风险自动判定 (END_OF_SEASON_MOTIVATION_CHECK_REQUIRED)
    # 检测联赛末轮（通常为第37、38轮），或者在 kickoff 为五月且是收官期
    is_end_of_season = False
    round_str = str(m.get("round", "")).strip()
    if "37" in round_str or "38" in round_str or "第37轮" in match_no or "第38轮" in match_no:
        is_end_of_season = True
    if kickoff and "2026-05" in kickoff:
        is_end_of_season = True
    # 兼容老数据中的标记
    if m.get("is_end_of_season") is True:
        is_end_of_season = True
        
    if is_end_of_season:
        risk_tags.append("END_OF_SEASON_MOTIVATION_CHECK_REQUIRED")
        
    # 数组去重
    market_tags = sorted(list(set(market_tags)))
    risk_tags = sorted(list(set(risk_tags)))
    
    market_move = {
        "euro_signal": euro_signal,
        "asian_signal": asian_signal,
        "total_signal": total_signal,
        "euro_asian_split": euro_asian_split
    }
    
    # ---------------- 新增：计算 market_move_detail 数值 delta ----------------
    def get_delta(curr: Optional[float], init: Optional[float]) -> Optional[float]:
        if curr is None or init is None:
            return None
        return round(curr - init, 2)
        
    market_move_detail = {
        "euro": {
            "home_delta": get_delta(eh_curr, eh_init),
            "draw_delta": get_delta(ed_curr, ed_init),
            "away_delta": get_delta(ea_curr, ea_init)
        },
        "asian": {
            "handicap_delta": get_delta(ah_curr, ah_init),
            "home_water_delta": get_delta(aw_curr_h, aw_init_h),
            "away_water_delta": get_delta(aw_curr_a, aw_init_a)
        },
        "total": {
            "line_delta": get_delta(th_curr, th_init),
            "over_water_delta": get_delta(tw_curr_o, tw_init_o),
            "under_water_delta": get_delta(tw_curr_u, tw_init_u)
        }
    }
    
    # ---------------- 置信度与覆盖率评估 (分母与 Missing 公司追踪) ----------------
    cov_euro = get_coverage_info(norm_euro, CANONICAL_BOOKMAKERS)
    cov_asian = get_coverage_info(norm_asian, CANONICAL_BOOKMAKERS)
    cov_total = get_coverage_info(norm_total, ["365", "Pinnacle/平博"])
    
    company_coverage = {
        "euro": cov_euro,
        "asian": cov_asian,
        "total": cov_total
    }
    
    data_confidence = {
        "euro": evaluate_confidence(cov_euro["active"], 6, 4),
        "asian": evaluate_confidence(cov_asian["active"], 5, 3),
        "total": evaluate_confidence(cov_total["active"], 2, 1)
    }
    
    # 拆分 market_time_logic 为 initial_read, movement_read, split_check, ares_warning
    old_time_logic = m.get("market_time_logic", {})
    if isinstance(old_time_logic, dict):
        initial_read = old_time_logic.get("initial_read", old_time_logic.get("初盘多空博弈", "未分析"))
        movement_read = old_time_logic.get("movement_read", old_time_logic.get("即时资金盘面移动", "未分析"))
        split_check = old_time_logic.get("split_check", old_time_logic.get("双盘同向裂痕校验", "未分析"))
        ares_warning = old_time_logic.get("ares_warning", old_time_logic.get("Ares风控防冷建议", "未分析"))
    else:
        initial_read, movement_read, split_check, ares_warning = "未分析", "未分析", "未分析", "未分析"
        
    market_time_logic = {
        "initial_read": initial_read,
        "movement_read": movement_read,
        "split_check": split_check,
        "ares_warning": ares_warning
    }
    
    # 客观结论客观化：移除强投注推荐
    market_conclusion = m.get("market_conclusion", [])
    if not market_conclusion:
        market_conclusion = []
        if euro_signal == "HOME_STRENGTHENED":
            market_conclusion.append(f"市场主方向偏向{home}一方")
        elif euro_signal == "AWAY_STRENGTHENED":
            market_conclusion.append(f"市场主方向偏向{away}一方")
            
        if "EURO_ASIAN_ALIGNED" in market_tags:
            market_conclusion.append("主胜欧赔下调与亚盘主队低水调整同向，欧亚信号一致。")
        if "OVER_WATER_COMPRESSED" in market_tags:
            market_conclusion.append("大球水位被明显压低，市场对较高进球区间的赔付保护增强。")
        elif "UNDER_WATER_COMPRESSED" in market_tags:
            market_conclusion.append("大小球小球水受到压缩，庄家防范少球防守态势")
            
        if euro_asian_split:
            market_conclusion.append("⚠️ 警惕：该场对阵触发欧亚大裂痕，存在严重风控异常信号")
        if "DRAW_COMPRESSED" in risk_tags:
            market_conclusion.append("⚠️ 警惕：多博彩公司平赔异常大幅拉低，平局风险显著增加")
        if "END_OF_SEASON_MOTIVATION_CHECK_REQUIRED" in risk_tags:
            market_conclusion.append("⚠️ 警惕：此场为收官轮次，各方战意与替补轮换需要 Prematch 引擎二次确认。")
            
        market_conclusion.append("请结合 Prematch 引擎的伤停、硬核战意与临场阵容进行综合表决")
        
    # 重组顶级 JSON 对象
    return {
        "match_no": match_no,
        "home": home,
        "away": away,
        "kickoff": kickoff,
        "sanity_check": sanity_check,
        "odds_raw": odds_raw,
        "odds_avg": odds_avg,
        "market_move": market_move,
        "market_move_detail": market_move_detail,
        "market_tags": market_tags,
        "risk_tags": risk_tags,
        "data_confidence": data_confidence,
        "company_coverage": company_coverage,
        "market_time_logic": market_time_logic,
        "market_conclusion": market_conclusion
    }


def main() -> int:
    if len(sys.argv) < 3:
        print("Usage: normalize_odds_report.py input.json output.json", file=sys.stderr)
        return 2
    src = Path(sys.argv[1])
    dst = Path(sys.argv[2])
    data = json.loads(src.read_text(encoding="utf-8"))
    
    matches = []
    if isinstance(data, dict):
        if "matches" in data:
            matches = data["matches"]
        else:
            # 兼容单场顶级 JSON
            matches = [data]
    elif isinstance(data, list):
        matches = data
        
    normalized_matches = [run_quant_engine(m) for m in matches]
    
    # 保持和输入相同的包装层次，如果是单场则输出单场，多场则包装在 matches 数组中
    out = {"matches": normalized_matches} if len(normalized_matches) > 1 or "matches" in data else normalized_matches[0]
    
    dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Quant Engine successfully normalized record. Result saved to: {dst}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
