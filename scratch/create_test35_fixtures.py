#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
create_test35_fixtures.py
===========================
在 AresMatchday_Test20_20260520 基础上，追加 21~35 场，
生成全量 35 场模拟包 AresMatchday_Test35_20260520。

新增 15 场（五大联赛全覆盖）：
  EPL (5场):
    21 - Arsenal vs Chelsea              : 伦敦德比，欧战争夺，市场中性偏主
    22 - Liverpool vs Manchester City    : 标题级大战，大盘高波动
    23 - Aston Villa vs Brighton         : 欧战边缘，过程与市场同向
    24 - West Ham vs Fulham              : 中游稳定对决，低风险
    25 - Newcastle vs Nottingham Forest  : 欧战争夺，主队强势

  La Liga (3场):
    26 - Atletico Madrid vs Valencia     : 强防守主队 vs 低迷客队
    27 - Girona vs Athletic Club         : 冷门风险，盘口偏紧
    28 - Real Madrid vs Mallorca         : 已锁冠轮换降权

  Serie A (2场):
    29 - Napoli vs AC Milan              : 意甲欧冠争夺战

  Bundesliga (4场):
    30 - Bayern Munich vs Borussia Dortmund  : 德国超级德比
    31 - Bayer Leverkusen vs RB Leipzig      : 欧冠席位争夺
    32 - Eintracht Frankfurt vs Hoffenheim   : 主场优势明显
    33 - Freiburg vs VfB Stuttgart           : 南德稳定对决

  Ligue 1 (1场):
    34 - Marseille vs Lille                  : 法甲欧战争夺

  Mixed 补充 (1场):
    35 - Toulouse vs Auxerre                 : 法甲安全收官普通场
"""

import json
from pathlib import Path


def main():
    vault_root = Path("/Users/liumingwei/vaults/AresVault")
    src_matchday_dir = vault_root / "03_Match_Audits" / "AresMatchday_Test20_20260520"
    dst_matchday_dir = vault_root / "03_Match_Audits" / "AresMatchday_Test35_20260520"
    dst_matchday_dir.mkdir(parents=True, exist_ok=True)

    # ──────────────────────────────────────────────────────────────
    # 1. 扩充 market.json：继承 20 场，追加 21~35 场
    # ──────────────────────────────────────────────────────────────
    market_src_path = src_matchday_dir / "market.json"
    if not market_src_path.exists():
        print("Error: 原 20 场 market.json 不存在！")
        return

    with open(market_src_path, "r", encoding="utf-8") as f:
        market_data = json.load(f)

    # 注意：Test20 根目录 market.json 已经是富化版（含 breakdown 等字段）
    # 但我们的脚本生成的是输入格式（不含 breakdown），引擎跑批时会重新计算
    # 所以从富化版中提取基础字段，或者直接构建干净输入
    # → 策略：直接构建新 15 场的纯输入格式，聚合写入；引擎跑批时统一富化
    # 但由于 20 场已是富化版，继承会导致重复字段；引擎读取时会优先用输入字段
    # → 最佳实践：从原始 Test20 input market.json 中读取（不用富化版）

    # 重新构造干净的 matches 列表（剥离引擎注入字段）
    engine_fields = {"market_move_detail", "risk_tags", "prematch_mode", "deep_queue_score", "deep_queue_breakdown"}
    clean_matches = []
    for m in market_data.get("matches", []):
        clean_m = {k: v for k, v in m.items() if k not in engine_fields}
        # 保留 market_tags（输入字段），其余引擎注入字段剥离
        clean_matches.append(clean_m)

    extra_market = [
        # ============================================================
        # EPL 5 场
        # ============================================================
        # 21: Arsenal vs Chelsea — 伦敦德比，欧战争夺，主队过程略优
        # 预期 Mode: STANDARD（市场同向，无大分裂）
        {
            "match_no": "21", "match_id": "21",
            "home": "Arsenal", "away": "Chelsea",
            "home_team": "阿森纳", "away_team": "切尔西",
            "league": "EPL", "kickoff": "2026-05-25 19:45:00",
            "data_source": "Understat EPL 2025/2026",
            "sanity_check": {"euro_order": "home/draw/away", "asian_format": "home_water / handicap_from_home_view / away_water"},
            "odds_avg": {
                "euro": {"initial": {"home": 2.00, "draw": 3.40, "away": 3.80},
                         "current": {"home": 1.90, "draw": 3.50, "away": 4.00}},
                "asian": {"initial": {"home_water": 0.90, "handicap": -0.50, "away_water": 0.96},
                          "current": {"home_water": 0.88, "handicap": -0.50, "away_water": 0.98}},
                "total": {"initial": {"over_water": 0.90, "line": 2.75, "under_water": 0.90},
                          "current": {"over_water": 0.88, "line": 2.75, "under_water": 0.92}}
            },
            "market_move": {"euro_signal": "HOME_STRENGTHENED", "asian_signal": "HANDICAP_STABLE",
                            "total_signal": "TOTAL_STABLE", "euro_asian_split": False},
            "market_tags": ["EURO_ASIAN_ALIGNED"],
            "risk_tags": ["STRONG_FAVORITE_VARIANCE_GUARD"]
        },
        # 22: Liverpool vs Manchester City — 标题级大战，大盘高波动，欧亚可能分裂
        # 预期 Mode: DEEP（多重风险叠加）
        {
            "match_no": "22", "match_id": "22",
            "home": "Liverpool", "away": "Manchester City",
            "home_team": "利物浦", "away_team": "曼城",
            "league": "EPL", "kickoff": "2026-05-25 21:00:00",
            "data_source": "Understat EPL 2025/2026",
            "sanity_check": {"euro_order": "home/draw/away", "asian_format": "home_water / handicap_from_home_view / away_water"},
            "odds_avg": {
                "euro": {"initial": {"home": 2.20, "draw": 3.40, "away": 3.20},
                         "current": {"home": 2.50, "draw": 3.30, "away": 2.80}},
                "asian": {"initial": {"home_water": 0.88, "handicap": -0.25, "away_water": 0.98},
                          "current": {"home_water": 0.95, "handicap": 0.00, "away_water": 0.90}},
                "total": {"initial": {"over_water": 0.90, "line": 3.00, "under_water": 0.90},
                          "current": {"over_water": 0.90, "line": 3.00, "under_water": 0.90}}
            },
            "market_move": {"euro_signal": "AWAY_STRENGTHENED", "asian_signal": "HOME_SHALLOW",
                            "total_signal": "TOTAL_STABLE", "euro_asian_split": True},
            "market_tags": ["AWAY_EURO_STRENGTHENED", "HOME_HANDICAP_COMPRESSED", "EURO_ASIAN_SPLIT"],
            "risk_tags": ["MARKET_PROCESS_CONFLICT", "EURO_ASIAN_SPLIT", "PICKEM_COMPRESSION"]
        },
        # 23: Aston Villa vs Brighton — 欧战争夺，过程与市场同向，风险中等
        # 预期 Mode: STANDARD
        {
            "match_no": "23", "match_id": "23",
            "home": "Aston Villa", "away": "Brighton",
            "home_team": "阿斯顿维拉", "away_team": "布莱顿",
            "league": "EPL", "kickoff": "2026-05-26 19:45:00",
            "data_source": "Understat EPL 2025/2026",
            "sanity_check": {"euro_order": "home/draw/away", "asian_format": "home_water / handicap_from_home_view / away_water"},
            "odds_avg": {
                "euro": {"initial": {"home": 2.10, "draw": 3.40, "away": 3.60},
                         "current": {"home": 2.00, "draw": 3.45, "away": 3.80}},
                "asian": {"initial": {"home_water": 0.90, "handicap": -0.50, "away_water": 0.96},
                          "current": {"home_water": 0.88, "handicap": -0.50, "away_water": 0.98}},
                "total": {"initial": {"over_water": 0.90, "line": 2.75, "under_water": 0.90},
                          "current": {"over_water": 0.90, "line": 2.75, "under_water": 0.90}}
            },
            "market_move": {"euro_signal": "HOME_STRENGTHENED", "asian_signal": "HANDICAP_STABLE",
                            "total_signal": "TOTAL_STABLE", "euro_asian_split": False},
            "market_tags": ["EURO_ASIAN_ALIGNED", "PROCESS_AND_MARKET_ALIGNED"],
            "risk_tags": ["CLEAN_STRONG_FAVORITE"]
        },
        # 24: West Ham vs Fulham — 中游伦敦德比，稳定无异常，低风险
        # 预期 Mode: LIGHT
        {
            "match_no": "24", "match_id": "24",
            "home": "West Ham United", "away": "Fulham",
            "home_team": "西汉姆联", "away_team": "富勒姆",
            "league": "EPL", "kickoff": "2026-05-26 21:00:00",
            "data_source": "Understat EPL 2025/2026",
            "sanity_check": {"euro_order": "home/draw/away", "asian_format": "home_water / handicap_from_home_view / away_water"},
            "odds_avg": {
                "euro": {"initial": {"home": 2.40, "draw": 3.20, "away": 3.10},
                         "current": {"home": 2.40, "draw": 3.20, "away": 3.10}},
                "asian": {"initial": {"home_water": 0.92, "handicap": -0.25, "away_water": 0.94},
                          "current": {"home_water": 0.92, "handicap": -0.25, "away_water": 0.94}},
                "total": {"initial": {"over_water": 0.90, "line": 2.50, "under_water": 0.90},
                          "current": {"over_water": 0.90, "line": 2.50, "under_water": 0.90}}
            },
            "market_move": {"euro_signal": "HOME_STABLE", "asian_signal": "HANDICAP_STABLE",
                            "total_signal": "TOTAL_STABLE", "euro_asian_split": False},
            "market_tags": [],
            "risk_tags": []
        },
        # 25: Newcastle vs Nottingham Forest — 纽卡欧战争夺，主场强势，深盘让步
        # 预期 Mode: STANDARD（深盘保护触发）
        {
            "match_no": "25", "match_id": "25",
            "home": "Newcastle United", "away": "Nottingham Forest",
            "home_team": "纽卡斯尔联", "away_team": "诺丁汉森林",
            "league": "EPL", "kickoff": "2026-05-27 19:45:00",
            "data_source": "Understat EPL 2025/2026",
            "sanity_check": {"euro_order": "home/draw/away", "asian_format": "home_water / handicap_from_home_view / away_water"},
            "odds_avg": {
                "euro": {"initial": {"home": 1.65, "draw": 3.80, "away": 5.50},
                         "current": {"home": 1.60, "draw": 3.90, "away": 5.80}},
                "asian": {"initial": {"home_water": 0.88, "handicap": -0.75, "away_water": 0.98},
                          "current": {"home_water": 0.85, "handicap": -1.00, "away_water": 1.01}},
                "total": {"initial": {"over_water": 0.90, "line": 2.75, "under_water": 0.90},
                          "current": {"over_water": 0.90, "line": 3.00, "under_water": 0.90}}
            },
            "market_move": {"euro_signal": "HOME_STRENGTHENED", "asian_signal": "HOME_DEEPENED",
                            "total_signal": "TOTAL_UP", "euro_asian_split": False},
            "market_tags": ["HOME_EURO_STRENGTHENED", "FAVORITE_DEEPENED"],
            "risk_tags": ["STRONG_FAVORITE_VARIANCE_GUARD", "FAVORITE_DEEP_HANDICAP_CAUTION", "WIN_DIRECTION_HANDICAP_OVERPRICED"]
        },
        # ============================================================
        # La Liga 3 场
        # ============================================================
        # 26: Atletico Madrid vs Valencia — 强防守主队，客队低迷，大盘稳定偏主
        # 预期 Mode: STANDARD
        {
            "match_no": "26", "match_id": "26",
            "home": "Atletico Madrid", "away": "Valencia",
            "home_team": "马德里竞技", "away_team": "瓦伦西亚",
            "league": "ESP_Spain", "kickoff": "2026-05-25 22:00:00",
            "data_source": "Understat La Liga 2025/2026",
            "sanity_check": {"euro_order": "home/draw/away", "asian_format": "home_water / handicap_from_home_view / away_water"},
            "odds_avg": {
                "euro": {"initial": {"home": 1.55, "draw": 4.00, "away": 7.00},
                         "current": {"home": 1.50, "draw": 4.10, "away": 7.50}},
                "asian": {"initial": {"home_water": 0.88, "handicap": -0.75, "away_water": 0.98},
                          "current": {"home_water": 0.86, "handicap": -1.00, "away_water": 1.00}},
                "total": {"initial": {"over_water": 0.88, "line": 2.25, "under_water": 0.92},
                          "current": {"over_water": 0.86, "line": 2.00, "under_water": 0.94}}
            },
            "market_move": {"euro_signal": "HOME_STRENGTHENED", "asian_signal": "HOME_DEEPENED",
                            "total_signal": "TOTAL_DOWN", "euro_asian_split": False},
            "market_tags": ["HOME_DEEPENED", "LOW_TOTAL_REPAIR"],
            "risk_tags": ["STRONG_FAVORITE_VARIANCE_GUARD", "FAVORITE_DEEP_HANDICAP_CAUTION", "WIN_DIRECTION_HANDICAP_OVERPRICED"]
        },
        # 27: Girona vs Athletic Club — 双方欧战争夺，冷门风险，盘口偏紧平手
        # 预期 Mode: STANDARD
        {
            "match_no": "27", "match_id": "27",
            "home": "Girona", "away": "Athletic Club",
            "home_team": "赫罗纳", "away_team": "毕尔巴鄂竞技",
            "league": "ESP_Spain", "kickoff": "2026-05-26 22:00:00",
            "data_source": "Understat La Liga 2025/2026",
            "sanity_check": {"euro_order": "home/draw/away", "asian_format": "home_water / handicap_from_home_view / away_water"},
            "odds_avg": {
                "euro": {"initial": {"home": 2.50, "draw": 3.20, "away": 2.90},
                         "current": {"home": 2.60, "draw": 3.20, "away": 2.80}},
                "asian": {"initial": {"home_water": 0.92, "handicap": 0.00, "away_water": 0.94},
                          "current": {"home_water": 0.94, "handicap": 0.25, "away_water": 0.90}},
                "total": {"initial": {"over_water": 0.90, "line": 2.50, "under_water": 0.90},
                          "current": {"over_water": 0.90, "line": 2.50, "under_water": 0.90}}
            },
            "market_move": {"euro_signal": "AWAY_STRENGTHENED", "asian_signal": "AWAY_DEEPENED",
                            "total_signal": "TOTAL_STABLE", "euro_asian_split": False},
            "market_tags": ["AWAY_REPAIR", "PICKEM_COMPRESSION"],
            "risk_tags": ["MARKET_PROCESS_CONFLICT", "PICKEM_COMPRESSION", "UNDERDOG_WIN_LIVE"]
        },
        # 28: Real Madrid vs Mallorca — 已锁冠，战意降级，大幅让步高水
        # 预期 Mode: DEEP（锁冠轮换 + 大让保护叠加）
        {
            "match_no": "28", "match_id": "28",
            "home": "Real Madrid", "away": "Mallorca",
            "home_team": "皇家马德里", "away_team": "马略卡",
            "league": "ESP_Spain", "kickoff": "2026-05-27 22:00:00",
            "data_source": "Understat La Liga 2025/2026",
            "sanity_check": {"euro_order": "home/draw/away", "asian_format": "home_water / handicap_from_home_view / away_water"},
            "odds_avg": {
                "euro": {"initial": {"home": 1.22, "draw": 6.50, "away": 15.00},
                         "current": {"home": 1.35, "draw": 5.50, "away": 11.00}},
                "asian": {"initial": {"home_water": 0.85, "handicap": -1.75, "away_water": 1.01},
                          "current": {"home_water": 0.95, "handicap": -1.25, "away_water": 0.90}},
                "total": {"initial": {"over_water": 0.88, "line": 3.50, "under_water": 0.92},
                          "current": {"over_water": 0.92, "line": 3.00, "under_water": 0.88}}
            },
            "market_move": {"euro_signal": "AWAY_STRENGTHENED", "asian_signal": "AWAY_SHALLOW",
                            "total_signal": "TOTAL_DOWN", "euro_asian_split": False},
            "market_tags": ["AWAY_EURO_REPAIR", "HOME_HANDICAP_COMPRESSED", "DRAW_REPAIR"],
            "risk_tags": ["LOCKED_TARGET_DEFLATION", "BRAND_FAVORITE_DOWNGRADE", "FAVORITE_RETREAT",
                          "FAVORITE_DEEP_HANDICAP_CAUTION", "WIN_DIRECTION_HANDICAP_OVERPRICED"]
        },
        # ============================================================
        # Serie A 1 场
        # ============================================================
        # 29: Napoli vs AC Milan — 意甲欧冠席位争夺，双方战意高，市场欧亚分裂
        # 预期 Mode: DEEP
        {
            "match_no": "29", "match_id": "29",
            "home": "Napoli", "away": "AC Milan",
            "home_team": "那不勒斯", "away_team": "AC米兰",
            "league": "Serie_A", "kickoff": "2026-05-26 19:45:00",
            "data_source": "Understat Serie A 2025/2026",
            "sanity_check": {"euro_order": "home/draw/away", "asian_format": "home_water / handicap_from_home_view / away_water"},
            "odds_avg": {
                "euro": {"initial": {"home": 2.10, "draw": 3.30, "away": 3.60},
                         "current": {"home": 2.30, "draw": 3.20, "away": 3.20}},
                "asian": {"initial": {"home_water": 0.88, "handicap": -0.25, "away_water": 0.98},
                          "current": {"home_water": 0.95, "handicap": 0.00, "away_water": 0.90}},
                "total": {"initial": {"over_water": 0.90, "line": 2.75, "under_water": 0.90},
                          "current": {"over_water": 0.90, "line": 2.75, "under_water": 0.90}}
            },
            "market_move": {"euro_signal": "AWAY_STRENGTHENED", "asian_signal": "HOME_SHALLOW",
                            "total_signal": "TOTAL_STABLE", "euro_asian_split": True},
            "market_tags": ["AWAY_EURO_STRENGTHENED", "HOME_HANDICAP_COMPRESSED", "EURO_ASIAN_SPLIT"],
            "risk_tags": ["EURO_ASIAN_SPLIT", "MARKET_PROCESS_CONFLICT", "PICKEM_COMPRESSION"]
        },
        # ============================================================
        # Bundesliga 4 场
        # ============================================================
        # 30: Bayern Munich vs Borussia Dortmund — 德国超级德比，大盘高让+退水风险
        # 预期 Mode: DEEP（深盘大让 + 退水风险 + 品牌热门）
        {
            "match_no": "30", "match_id": "30",
            "home": "Bayern Munich", "away": "Borussia Dortmund",
            "home_team": "拜仁慕尼黑", "away_team": "多特蒙德",
            "league": "GER_Germany", "kickoff": "2026-05-25 20:30:00",
            "data_source": "Understat Bundesliga 2025/2026",
            "sanity_check": {"euro_order": "home/draw/away", "asian_format": "home_water / handicap_from_home_view / away_water"},
            "odds_avg": {
                "euro": {"initial": {"home": 1.55, "draw": 4.20, "away": 6.00},
                         "current": {"home": 1.70, "draw": 3.90, "away": 5.00}},
                "asian": {"initial": {"home_water": 0.85, "handicap": -1.00, "away_water": 1.01},
                          "current": {"home_water": 0.95, "handicap": -0.75, "away_water": 0.90}},
                "total": {"initial": {"over_water": 0.88, "line": 3.25, "under_water": 0.92},
                          "current": {"over_water": 0.90, "line": 3.00, "under_water": 0.90}}
            },
            "market_move": {"euro_signal": "AWAY_STRENGTHENED", "asian_signal": "AWAY_SHALLOW",
                            "total_signal": "TOTAL_DOWN", "euro_asian_split": False},
            "market_tags": ["HOME_HANDICAP_COMPRESSED", "AWAY_EURO_STRENGTHENED"],
            "risk_tags": ["FAVORITE_RETREAT", "FAVORITE_DEEP_HANDICAP_CAUTION", "WIN_DIRECTION_HANDICAP_OVERPRICED", "BRAND_FAVORITE_DOWNGRADE"]
        },
        # 31: Bayer Leverkusen vs RB Leipzig — 欧冠席位争夺，过程vs market方向有分歧
        # 预期 Mode: STANDARD~DEEP
        {
            "match_no": "31", "match_id": "31",
            "home": "Bayer Leverkusen", "away": "Rasen Ballsport Leipzig",
            "home_team": "勒沃库森", "away_team": "莱比锡红牛",
            "league": "GER_Germany", "kickoff": "2026-05-26 20:30:00",
            "data_source": "Understat Bundesliga 2025/2026",
            "sanity_check": {"euro_order": "home/draw/away", "asian_format": "home_water / handicap_from_home_view / away_water"},
            "odds_avg": {
                "euro": {"initial": {"home": 1.85, "draw": 3.50, "away": 4.40},
                         "current": {"home": 1.90, "draw": 3.50, "away": 4.20}},
                "asian": {"initial": {"home_water": 0.90, "handicap": -0.50, "away_water": 0.96},
                          "current": {"home_water": 0.90, "handicap": -0.50, "away_water": 0.96}},
                "total": {"initial": {"over_water": 0.90, "line": 2.75, "under_water": 0.90},
                          "current": {"over_water": 0.90, "line": 2.75, "under_water": 0.90}}
            },
            "market_move": {"euro_signal": "HOME_STABLE", "asian_signal": "HANDICAP_STABLE",
                            "total_signal": "TOTAL_STABLE", "euro_asian_split": False},
            "market_tags": ["EURO_ASIAN_ALIGNED"],
            "risk_tags": ["STRONG_FAVORITE_VARIANCE_GUARD"]
        },
        # 32: Eintracht Frankfurt vs Hoffenheim — 主场有力优势，客队无大压力
        # 预期 Mode: LIGHT~STANDARD
        {
            "match_no": "32", "match_id": "32",
            "home": "Eintracht Frankfurt", "away": "Hoffenheim",
            "home_team": "法兰克福", "away_team": "霍芬海姆",
            "league": "GER_Germany", "kickoff": "2026-05-27 20:30:00",
            "data_source": "Understat Bundesliga 2025/2026",
            "sanity_check": {"euro_order": "home/draw/away", "asian_format": "home_water / handicap_from_home_view / away_water"},
            "odds_avg": {
                "euro": {"initial": {"home": 1.80, "draw": 3.60, "away": 4.80},
                         "current": {"home": 1.75, "draw": 3.65, "away": 5.00}},
                "asian": {"initial": {"home_water": 0.90, "handicap": -0.50, "away_water": 0.96},
                          "current": {"home_water": 0.88, "handicap": -0.50, "away_water": 0.98}},
                "total": {"initial": {"over_water": 0.90, "line": 2.75, "under_water": 0.90},
                          "current": {"over_water": 0.90, "line": 2.75, "under_water": 0.90}}
            },
            "market_move": {"euro_signal": "HOME_STRENGTHENED", "asian_signal": "HANDICAP_STABLE",
                            "total_signal": "TOTAL_STABLE", "euro_asian_split": False},
            "market_tags": ["EURO_ASIAN_ALIGNED"],
            "risk_tags": ["CLEAN_STRONG_FAVORITE"]
        },
        # 33: Freiburg vs VfB Stuttgart — 南德稳定对决，盘口平稳，低风险
        # 预期 Mode: LIGHT
        {
            "match_no": "33", "match_id": "33",
            "home": "Freiburg", "away": "VfB Stuttgart",
            "home_team": "弗赖堡", "away_team": "斯图加特",
            "league": "GER_Germany", "kickoff": "2026-05-27 20:30:00",
            "data_source": "Understat Bundesliga 2025/2026",
            "sanity_check": {"euro_order": "home/draw/away", "asian_format": "home_water / handicap_from_home_view / away_water"},
            "odds_avg": {
                "euro": {"initial": {"home": 2.40, "draw": 3.20, "away": 3.10},
                         "current": {"home": 2.40, "draw": 3.20, "away": 3.10}},
                "asian": {"initial": {"home_water": 0.92, "handicap": -0.25, "away_water": 0.94},
                          "current": {"home_water": 0.92, "handicap": -0.25, "away_water": 0.94}},
                "total": {"initial": {"over_water": 0.90, "line": 2.75, "under_water": 0.90},
                          "current": {"over_water": 0.90, "line": 2.75, "under_water": 0.90}}
            },
            "market_move": {"euro_signal": "HOME_STABLE", "asian_signal": "HANDICAP_STABLE",
                            "total_signal": "TOTAL_STABLE", "euro_asian_split": False},
            "market_tags": [],
            "risk_tags": []
        },
        # ============================================================
        # Ligue 1 2 场
        # ============================================================
        # 34: Marseille vs Lille — 法甲欧战争夺，双方战意高，市场稳定偏主
        # 预期 Mode: STANDARD
        {
            "match_no": "34", "match_id": "34",
            "home": "Marseille", "away": "Lille",
            "home_team": "马赛", "away_team": "里尔",
            "league": "FRA_France", "kickoff": "2026-05-26 21:00:00",
            "data_source": "Understat Ligue 1 2025/2026",
            "sanity_check": {"euro_order": "home/draw/away", "asian_format": "home_water / handicap_from_home_view / away_water"},
            "odds_avg": {
                "euro": {"initial": {"home": 1.95, "draw": 3.50, "away": 4.00},
                         "current": {"home": 1.90, "draw": 3.55, "away": 4.20}},
                "asian": {"initial": {"home_water": 0.90, "handicap": -0.50, "away_water": 0.96},
                          "current": {"home_water": 0.88, "handicap": -0.50, "away_water": 0.98}},
                "total": {"initial": {"over_water": 0.90, "line": 2.75, "under_water": 0.90},
                          "current": {"over_water": 0.90, "line": 2.75, "under_water": 0.90}}
            },
            "market_move": {"euro_signal": "HOME_STRENGTHENED", "asian_signal": "HANDICAP_STABLE",
                            "total_signal": "TOTAL_STABLE", "euro_asian_split": False},
            "market_tags": ["EURO_ASIAN_ALIGNED", "PROCESS_AND_MARKET_ALIGNED"],
            "risk_tags": ["CLEAN_STRONG_FAVORITE"]
        },
        # 35: Toulouse vs Auxerre — 法甲安全收官，双方无压力，完全低风险
        # 预期 Mode: LIGHT（置底）
        {
            "match_no": "35", "match_id": "35",
            "home": "Toulouse", "away": "Auxerre",
            "home_team": "图卢兹", "away_team": "欧塞尔",
            "league": "FRA_France", "kickoff": "2026-05-27 21:00:00",
            "data_source": "Understat Ligue 1 2025/2026",
            "sanity_check": {"euro_order": "home/draw/away", "asian_format": "home_water / handicap_from_home_view / away_water"},
            "odds_avg": {
                "euro": {"initial": {"home": 2.30, "draw": 3.20, "away": 3.20},
                         "current": {"home": 2.30, "draw": 3.20, "away": 3.20}},
                "asian": {"initial": {"home_water": 0.92, "handicap": 0.00, "away_water": 0.94},
                          "current": {"home_water": 0.92, "handicap": 0.00, "away_water": 0.94}},
                "total": {"initial": {"over_water": 0.90, "line": 2.50, "under_water": 0.90},
                          "current": {"over_water": 0.90, "line": 2.50, "under_water": 0.90}}
            },
            "market_move": {"euro_signal": "HOME_STABLE", "asian_signal": "HANDICAP_STABLE",
                            "total_signal": "TOTAL_STABLE", "euro_asian_split": False},
            "market_tags": [],
            "risk_tags": []
        }
    ]

    clean_matches.extend(extra_market)
    market_data["matches"] = clean_matches

    with open(dst_matchday_dir / "market.json", "w", encoding="utf-8") as f:
        json.dump(market_data, f, ensure_ascii=False, indent=2)
    print(f"✅ market.json 35 场写入完成 → {dst_matchday_dir / 'market.json'}")

    # ──────────────────────────────────────────────────────────────
    # 2. 扩充 abnormal.json：继承 20 场，追加 21~35 场
    # ──────────────────────────────────────────────────────────────
    abnormal_src_path = src_matchday_dir / "abnormal.json"
    with open(abnormal_src_path, "r", encoding="utf-8") as f:
        abnormal_data = json.load(f)

    matches_dict = abnormal_data.get("matches", {})
    # 清理 key，统一为 2 位字符串
    cleaned_dict = {}
    for k, v in matches_dict.items():
        try:
            k_str = f"{int(k):02d}"
        except ValueError:
            k_str = k
        v["match_no"] = k_str
        cleaned_dict[k_str] = v

    def make_partial_pass(match_no, home, away, home_label="normal", away_label="normal"):
        """生成标准 PARTIAL_PASS 伤停条目（无确认缺阵，正常赛前状态）。"""
        return {
            "match_no": match_no,
            "home": home, "away": away,
            "fact_gate": {
                "status": "PARTIAL_PASS",
                "final_confidence": "medium_low",
                "reason": ["no_confirmed_absences", "player_availability_requires_latest_team_news",
                           "abnormal_info_usable_only_as_context"]
            },
            "teams": [
                {"team": home, "side": "home",
                 "target_status": {"label": home_label, "motivation_level": "medium",
                                   "confidence": "medium_low", "source": "abnormal_inference"},
                 "confirmed": [], "supported": [], "needs_latest_confirmation": [], "affected_units": {}},
                {"team": away, "side": "away",
                 "target_status": {"label": away_label, "motivation_level": "medium",
                                   "confidence": "medium_low", "source": "abnormal_inference"},
                 "confirmed": [], "supported": [], "needs_latest_confirmation": [], "affected_units": {}}
            ]
        }

    extra_abnormal = {
        # EPL
        "21": make_partial_pass("21", "Arsenal", "Chelsea", "europaleague_contention", "europaleague_contention"),
        "22": {
            "match_no": "22", "home": "Liverpool", "away": "Manchester City",
            "fact_gate": {"status": "PARTIAL_PASS", "final_confidence": "medium_low",
                          "reason": ["no_confirmed_absences", "player_availability_requires_latest_team_news",
                                     "abnormal_info_usable_only_as_context"]},
            "teams": [
                {"team": "Liverpool", "side": "home",
                 "target_status": {"label": "title_race_motivation", "motivation_level": "high",
                                   "confidence": "medium_low", "source": "abnormal_inference"},
                 "confirmed": [], "supported": [],
                 "needs_latest_confirmation": [
                     {"player": "Mohamed Salah", "unit": "attack", "status": "fatigue_management",
                      "confidence": "low_to_medium",
                      "reason": "赛季末疲劳管理，主帅暗示可能轮休，实际出战概率仍偏高。"}
                 ],
                 "affected_units": {"attack": "AMBER"}},
                {"team": "Manchester City", "side": "away",
                 "target_status": {"label": "title_race_motivation", "motivation_level": "high",
                                   "confidence": "medium_low", "source": "abnormal_inference"},
                 "confirmed": [], "supported": [],
                 "needs_latest_confirmation": [
                     {"player": "Kevin De Bruyne", "unit": "midfield", "status": "knock_doubt",
                      "confidence": "low_to_medium",
                      "reason": "De Bruyne 轻微撞伤，赛前做体能评估，出战概率约 70%。"}
                 ],
                 "affected_units": {"midfield": "AMBER"}}
            ]
        },
        "23": make_partial_pass("23", "Aston Villa", "Brighton", "europaleague_contention", "normal"),
        "24": make_partial_pass("24", "West Ham United", "Fulham", "normal", "normal"),
        "25": make_partial_pass("25", "Newcastle United", "Nottingham Forest", "europaleague_contention", "normal"),
        # La Liga
        "26": make_partial_pass("26", "Atletico Madrid", "Valencia", "championshiprace", "normal"),
        "27": make_partial_pass("27", "Girona", "Athletic Club", "europaleague_contention", "europaleague_contention"),
        "28": {
            "match_no": "28", "home": "Real Madrid", "away": "Mallorca",
            "fact_gate": {"status": "PARTIAL_PASS", "final_confidence": "medium_low",
                          "reason": ["no_confirmed_absences", "player_availability_requires_latest_team_news",
                                     "locked_target_confirmed"]},
            "teams": [
                {"team": "Real Madrid", "side": "home",
                 "target_status": {"label": "target_deflation", "motivation_level": "low",
                                   "confidence": "medium", "source": "abnormal_inference"},
                 "confirmed": [], "supported": [],
                 "needs_latest_confirmation": [
                     {"player": "Jude Bellingham", "unit": "midfield", "status": "rotation_rest",
                      "confidence": "medium", "reason": "皇马已锁冠，贝林厄姆欧冠决赛前高度可能轮休。"},
                     {"player": "Kylian Mbappé", "unit": "attack", "status": "rotation_rest",
                      "confidence": "medium", "reason": "姆巴佩同理，主帅明示轮换战略。"}
                 ],
                 "affected_units": {"midfield": "AMBER", "attack": "AMBER"}},
                {"team": "Mallorca", "side": "away",
                 "target_status": {"label": "survival_safe", "motivation_level": "medium",
                                   "confidence": "medium_low", "source": "abnormal_inference"},
                 "confirmed": [], "supported": [], "needs_latest_confirmation": [], "affected_units": {}}
            ]
        },
        # Serie A
        "29": make_partial_pass("29", "Napoli", "AC Milan", "championshiprace", "europaleague_contention"),
        # Bundesliga
        "30": {
            "match_no": "30", "home": "Bayern Munich", "away": "Borussia Dortmund",
            "fact_gate": {"status": "PARTIAL_PASS", "final_confidence": "medium_low",
                          "reason": ["no_confirmed_absences", "player_availability_requires_latest_team_news",
                                     "abnormal_info_usable_only_as_context"]},
            "teams": [
                {"team": "Bayern Munich", "side": "home",
                 "target_status": {"label": "title_race_motivation", "motivation_level": "high",
                                   "confidence": "medium_low", "source": "abnormal_inference"},
                 "confirmed": [], "supported": [],
                 "needs_latest_confirmation": [
                     {"player": "Harry Kane", "unit": "attack", "status": "knock_doubt",
                      "confidence": "low_to_medium", "reason": "凯恩轻微撞伤，赛前体能评估，预计出战。"}
                 ],
                 "affected_units": {"attack": "AMBER"}},
                {"team": "Borussia Dortmund", "side": "away",
                 "target_status": {"label": "europaleague_contention", "motivation_level": "high",
                                   "confidence": "medium_low", "source": "abnormal_inference"},
                 "confirmed": [], "supported": [], "needs_latest_confirmation": [], "affected_units": {}}
            ]
        },
        "31": make_partial_pass("31", "Bayer Leverkusen", "Rasen Ballsport Leipzig",
                               "europaleague_contention", "europaleague_contention"),
        "32": make_partial_pass("32", "Eintracht Frankfurt", "Hoffenheim", "europaleague_contention", "normal"),
        "33": make_partial_pass("33", "Freiburg", "VfB Stuttgart", "normal", "normal"),
        # Ligue 1
        "34": make_partial_pass("34", "Marseille", "Lille", "europaleague_contention", "europaleague_contention"),
        "35": make_partial_pass("35", "Toulouse", "Auxerre", "normal", "normal"),
    }

    cleaned_dict.update(extra_abnormal)
    abnormal_data["matches"] = cleaned_dict

    with open(dst_matchday_dir / "abnormal.json", "w", encoding="utf-8") as f:
        json.dump(abnormal_data, f, ensure_ascii=False, indent=2)
    print(f"✅ abnormal.json 35 场写入完成 → {dst_matchday_dir / 'abnormal.json'}")
    print("🚀 AresMatchday_Test35_20260520 数据库准备完成，可立即跑批！")


if __name__ == "__main__":
    main()
