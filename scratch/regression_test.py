#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
regression_test.py
==================
Ares Prematch V2.3 自动回归测试脚本。
用于校验全量 35 场跑批后，5 个核心校准样本是否正确识别了其专属的 must-have 门禁标签，
且 prematch_mode 均符合预设的 min_mode，不会发生 regression。
"""

import json
import sys
from pathlib import Path

REGRESSION_CASES = {
    "06": {
        "name": "Juventus vs Fiorentina",
        "must_have": ["STRONG_FAVORITE_VARIANCE_GUARD", "PROCESS_RIGHT_RESULT_RISK"],
        "min_mode": "STANDARD",
        "desc": "强热门方差保护门禁"
    },
    "02": {
        "name": "Roma vs Lazio",
        "must_have": ["CLEAN_STRONG_FAVORITE", "PROCESS_AND_MOTIVATION_ALIGNED"],
        "min_mode": "STANDARD",
        "desc": "强热门同向正样本门禁"
    },
    "05": {
        "name": "Lorient vs Le Havre",
        "must_have": ["SURVIVAL_WIN_CONVERSION_GATE", "UNDERDOG_WIN_LIVE"],
        "min_mode": "STANDARD",
        "desc": "保级客胜转换门禁"
    },
    "08": {
        "name": "Cagliari vs Torino",
        "must_have": ["MARKET_OVERPRICES_MOTIVATION_SIDE", "SURVIVAL_PRICE_OVERCOMPRESSION"],
        "min_mode": "STANDARD",
        "desc": "市场过度定价战意门禁"
    },
    "14": {
        "name": "Udinese vs Cremonese",
        "must_have": ["MARKET_REPAIRS_SURVIVAL_SIDE", "DRAW_PROTECTION"],
        "min_mode": "STANDARD",
        "desc": "保级客队修复门禁"
    }
}

def get_mode_level(mode: str) -> int:
    levels = {"LIGHT": 1, "STANDARD": 2, "DEEP": 3}
    return levels.get(mode, 0)

def main():
    vault_root = Path("/Users/liumingwei/vaults/AresVault")
    market_json_path = vault_root / "03_Match_Audits" / "AresMatchday_Test35_20260520" / "market.json"
    
    print("=" * 60)
    print("🚀 Ares Prematch V2.3 门禁规则自动回归测试")
    print(f"📂 聚合文件路径: {market_json_path}")
    print("=" * 60)
    
    if not market_json_path.exists():
        print(f"❌ 错误: 跑批生成的根目录 market.json 不存在！请先跑批。")
        print("regression_status: FAIL")
        sys.exit(1)
        
    with open(market_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    matches = data.get("matches", [])
    matches_by_no = {}
    for m in matches:
        m_no = m.get("match_no")
        try:
            m_no_str = f"{int(m_no):02d}"
        except (ValueError, TypeError):
            m_no_str = m_no
        matches_by_no[m_no_str] = m
    
    all_passed = True
    print("\n--- 开始逐场校验回归用例 ---")
    for m_no, config in REGRESSION_CASES.items():
        m_no_str = f"{int(m_no):02d}"
        match = matches_by_no.get(m_no_str)
        if not match:
            print(f"❌ Match {m_no_str} ({config['name']}) 缺失！")
            all_passed = False
            continue
            
        m_name = f"{match.get('home')} vs {match.get('away')}"
        m_mode = match.get("prematch_mode", "UNKNOWN")
        m_score = match.get("deep_queue_score", 0)
        m_tags = match.get("risk_tags", [])
        
        print(f"\n👉 场次 {m_no_str} | {m_name} ({config['desc']}):")
        print(f"   [运行值] mode: {m_mode} (score: {m_score}) | risk_tags: {m_tags}")
        print(f"   [预期值] min_mode: {config['min_mode']} | must_have: {config['must_have']}")
        
        # 1. 模式门禁校验
        if get_mode_level(m_mode) < get_mode_level(config["min_mode"]):
            print(f"   ❌ [FAIL] 模式不达标！当前 {m_mode} < 最低期望 {config['min_mode']}")
            all_passed = False
        else:
            print(f"   ✅ [PASS] 模式符合要求.")
            
        # 2. Must-have 标签校验
        missing_tags = [tag for tag in config["must_have"] if tag not in m_tags]
        if missing_tags:
            print(f"   ❌ [FAIL] 缺失关键博弈标签: {missing_tags}")
            all_passed = False
        else:
            print(f"   ✅ [PASS] 专属博弈标签完整.")
            
    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 恭喜！5 场核心校准样本全部成功通过回归测试！")
        print("regression_status: PASS")
        sys.exit(0)
    else:
        print("⚠️ 警告: 回归测试失败，仍有关键样本门禁回退！")
        print("regression_status: FAIL")
        sys.exit(1)

if __name__ == "__main__":
    main()
