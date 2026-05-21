#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
generate_material_pack.py
==========================
Ares Prematch 资料包一键打包生成引擎 V2.3 (BATCH_READY + DEEP_QUEUE)
1. 完整 v2.2 结构：market.json / abnormal.json / audit_input.md 三件套。
2. 新增 market_move_detail：自动从 initial/current 计算 delta 字段。
3. abnormal 三分层：confirmed / supported / needs_latest_confirmation。
4. deep_queue_score + prematch_mode：自动计算每场是否进入深度队列。
5. Game Script Seed 变量式推演种子（非战术论文）。
"""

import os
import sys
import csv
import json
import yaml
import re
import argparse
import shutil
import logging
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ────────────────────────────────────────────────────────────────
# 1. 基础配置与路径初始化
# ────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("football-prematch-material-pack")

def get_project_root() -> Path:
    return Path(__file__).parent.parent.parent.parent.parent

def get_vault_path() -> Path:
    vault_env = os.getenv("ARES_VAULT_PATH")
    if vault_env:
        return Path(vault_env)
    return Path("/Users/liumingwei/vaults/AresVault")

# ────────────────────────────────────────────────────────────────
# 2. 队名强解析与底座模糊检索
# ────────────────────────────────────────────────────────────────

def load_alias_map(root: Path) -> Dict[str, str]:
    alias_path = root / "src" / "data" / "team_alias_map.json"
    if alias_path.exists():
        try:
            return json.loads(alias_path.read_text(encoding="utf-8"))
        except Exception as e:
            logger.error(f"加载队名别名映射失败: {e}")
    return {}

def resolve_team_name(name: str, alias_map: Dict[str, str]) -> str:
    return alias_map.get(name.strip(), name.strip())

def is_team_match(name_a: str, name_b: str) -> bool:
    if not name_a or not name_b:
        return False
    norm_a = re.sub(r"[^a-zA-Z0-9]", "", name_a).lower()
    norm_b = re.sub(r"[^a-zA-Z0-9]", "", name_b).lower()
    if norm_a == norm_b:
        return True
    # 特例防卫映射，例如 PSG 与 Paris Saint Germain 的无损对齐
    special_cases = {
        "psg": "parissaintgermain",
        "parissaintgermain": "psg"
    }
    if special_cases.get(norm_a) == norm_b:
        return True
    if len(norm_a) >= 4 and len(norm_b) >= 4:
        if norm_a in norm_b or norm_b in norm_a:
            return True
    return False

def fuzzy_find_archive_path(vault_root: Path, resolved_en_name: str) -> Optional[Path]:
    archives_dir = vault_root / "02_Team_Archives" / "1_Top_Five_Europe"
    if not archives_dir.exists():
        return None
    for league_dir in archives_dir.iterdir():
        if not league_dir.is_dir():
            continue
        for md_file in league_dir.glob("*.md"):
            if md_file.name.startswith("_"):
                continue
            if is_team_match(md_file.stem, resolved_en_name):
                return md_file
    return None

def split_frontmatter(content: str) -> Tuple[Dict[str, Any], str]:
    if content.startswith("---\n"):
        closing = content.find("\n---\n", 4)
        if closing != -1:
            try:
                fm = yaml.safe_load(content[4:closing]) or {}
                body = content[closing + len("\n---\n"):].lstrip("\n")
                return fm, body
            except Exception as e:
                logger.error(f"解析 Frontmatter YAML 失败: {e}")
    return {}, content

# ────────────────────────────────────────────────────────────────
# 3. 智能数据格式化提取器 (V2.3)
# ────────────────────────────────────────────────────────────────

def calculate_market_move_detail(odds_avg: Dict[str, Any]) -> Dict[str, Any]:
    """
    从 initial/current 自动计算各盘口 delta 字段。
    delta = current - initial（保留2位小数）。
    """
    detail = {}
    try:
        euro = odds_avg.get("euro", {})
        ei, ec = euro.get("initial", {}), euro.get("current", {})
        if ei and ec:
            detail["euro"] = {
                "home_delta": round(float(ec.get("home", 0)) - float(ei.get("home", 0)), 2),
                "draw_delta": round(float(ec.get("draw", 0)) - float(ei.get("draw", 0)), 2),
                "away_delta": round(float(ec.get("away", 0)) - float(ei.get("away", 0)), 2),
            }
    except Exception:
        pass
    try:
        asian = odds_avg.get("asian", {})
        ai, ac = asian.get("initial", {}), asian.get("current", {})
        if ai and ac:
            detail["asian"] = {
                "handicap_delta": round(float(ac.get("handicap", 0)) - float(ai.get("handicap", 0)), 2),
                "home_water_delta": round(float(ac.get("home_water", 0)) - float(ai.get("home_water", 0)), 2),
                "away_water_delta": round(float(ac.get("away_water", 0)) - float(ai.get("away_water", 0)), 2),
            }
    except Exception:
        pass
    try:
        total = odds_avg.get("total", {})
        ti, tc = total.get("initial", {}), total.get("current", {})
        if ti and tc:
            detail["total"] = {
                "line_delta": round(float(tc.get("line", 0)) - float(ti.get("line", 0)), 2),
                "over_water_delta": round(float(tc.get("over_water", 0)) - float(ti.get("over_water", 0)), 2),
                "under_water_delta": round(float(tc.get("under_water", 0)) - float(ti.get("under_water", 0)), 2),
            }
    except Exception:
        pass
    return detail


def format_market_data(odds: Dict[str, Any], market_move: Dict[str, Any],
                        market_move_detail: Dict[str, Any], status: str) -> str:
    """
    格式化赔率数据，支持新式 odds_avg (current/initial) 与旧式扁平格式。
    新增 delta 渲染。
    """
    if status == "MISSING" or not odds:
        return (
            "> [!CAUTION]\n"
            "> **MARKET_DATA_STATUS: MISSING**\n"
            "> **MARKET_GATE: HALT_FOR_MARKET** (赔率大表缺失，禁止填充模板假数据，博弈分析强挂起)"
        )

    lines = []
    euro_raw = odds.get("euro", {})
    is_new_format = isinstance(euro_raw, dict) and "current" in euro_raw

    if is_new_format:
        euro = odds.get("euro", {})
        if euro:
            curr, init = euro.get("current", {}), euro.get("initial", {})
            delta = market_move_detail.get("euro", {})
            lines.append("- **欧洲独赢盘 (欧赔平均值)**:")
            lines.append(f"  * 当前: 主胜 `{curr.get('home','N/A')}` | 平 `{curr.get('draw','N/A')}` | 客胜 `{curr.get('away','N/A')}`")
            lines.append(f"  * 初始: 主胜 `{init.get('home','N/A')}` | 平 `{init.get('draw','N/A')}` | 客胜 `{init.get('away','N/A')}`")
            if delta:
                lines.append(f"  * **变动 Δ**: 主胜 `{delta.get('home_delta',0):+.2f}` | 平 `{delta.get('draw_delta',0):+.2f}` | 客胜 `{delta.get('away_delta',0):+.2f}`")

        asian = odds.get("asian", {})
        if asian:
            curr, init = asian.get("current", {}), asian.get("initial", {})
            delta = market_move_detail.get("asian", {})
            lines.append("- **亚洲让球盘 (让手与贴水)**:")
            lines.append(f"  * 当前: 主贴 `{curr.get('home_water','N/A')}` | 让步 `{curr.get('handicap','N/A')}` | 客贴 `{curr.get('away_water','N/A')}`")
            lines.append(f"  * 初始: 主贴 `{init.get('home_water','N/A')}` | 让步 `{init.get('handicap','N/A')}` | 客贴 `{init.get('away_water','N/A')}`")
            if delta:
                lines.append(f"  * **变动 Δ**: 让步 `{delta.get('handicap_delta',0):+.2f}` | 主贴 `{delta.get('home_water_delta',0):+.2f}` | 客贴 `{delta.get('away_water_delta',0):+.2f}`")

        total = odds.get("total", {})
        if total:
            curr, init = total.get("current", {}), total.get("initial", {})
            delta = market_move_detail.get("total", {})
            lines.append("- **进球大小盘**:")
            lines.append(f"  * 当前: 大球贴 `{curr.get('over_water','N/A')}` | 界限 `{curr.get('line','N/A')}` | 小球贴 `{curr.get('under_water','N/A')}`")
            lines.append(f"  * 初始: 大球贴 `{init.get('over_water','N/A')}` | 界限 `{init.get('line','N/A')}` | 小球贴 `{init.get('under_water','N/A')}`")
            if delta:
                lines.append(f"  * **变动 Δ**: 界限 `{delta.get('line_delta',0):+.2f}` | 大球贴 `{delta.get('over_water_delta',0):+.2f}` | 小球贴 `{delta.get('under_water_delta',0):+.2f}`")

        if market_move:
            lines.append("- **大盘变动信号**:")
            if "euro_signal" in market_move:
                lines.append(f"  * 独赢盘: `{market_move.get('euro_signal','N/A')}`")
                lines.append(f"  * 让球盘: `{market_move.get('asian_signal','N/A')}`")
                lines.append(f"  * 大小球: `{market_move.get('total_signal','N/A')}`")
            else:
                lines.append(f"  * 独赢盘趋势: `{market_move.get('home_win_trend','N/A')}`")
                lines.append(f"  * 让球盘变动: `{market_move.get('handicap_move','N/A')}`")
    else:
        # 旧格式兼容
        euro = odds.get("euro", {})
        if euro:
            lines.append(f"- **欧赔**: 主胜 `{euro.get('h','N/A')}` | 平 `{euro.get('d','N/A')}` | 客胜 `{euro.get('a','N/A')}`")
        asian = odds.get("asian", {})
        if asian:
            lines.append(f"- **亚盘**: 主贴 `{asian.get('home_water','N/A')}` | 让步 `{asian.get('handicap','N/A')}` | 客贴 `{asian.get('away_water','N/A')}`")
        ou = odds.get("over_under", {})
        if ou:
            lines.append(f"- **大小球**: 大球贴 `{ou.get('over_water','N/A')}` | 界限 `{ou.get('line','N/A')}` | 小球贴 `{ou.get('under_water','N/A')}`")
        if market_move:
            lines.append(f"- **独赢盘趋势**: `{market_move.get('home_win_trend','N/A')}`")
            lines.append(f"- **让球盘变动**: `{market_move.get('handicap_move','N/A')}`")

    return "\n".join(lines) if lines else "* 暂无赔率明细记录。"


def extract_abnormal_blocks(
    abnormal_match: Dict[str, Any], status: str
) -> Tuple[str, str, str, str, List[str]]:
    """
    提取存疑伤停和受灾单元 (V2.3 三分层)。
    返回: (suspicious_block, rotation_block, fg_status, fg_confidence, fg_reasons)
    三分层: confirmed_absences / supported_absences / needs_latest_confirmation
    """
    if status == "MISSING" or not abnormal_match:
        return (
            "> [!WARNING]\n"
            "> **NEEDS_VERIFICATION** (缺乏临场伤停 abnormal.json 输入源，置信度降级)",
            "* 门禁检测：因数据缺失未发现事实，受灾分析暂未激活。",
            "NEEDS_VERIFICATION",
            "medium_low",
            ["abnormal_json_missing", "source_evidence_missing"]
        )

    # 旧格式（suspicious_items + unit_impact）自动兼容转化
    teams = abnormal_match.get("teams", [])
    if not teams and "suspicious_items" in abnormal_match:
        susp = abnormal_match.get("suspicious_items", [])
        converted = []
        for item in susp:
            if isinstance(item, dict):
                converted.append({
                    "player": item.get("player", "未知"),
                    "unit": "unknown",
                    "status": "needs_confirmation",
                    "confidence": item.get("confidence", "low_to_medium"),
                    "reason": item.get("reason", "情况待确认")
                })
        rot = abnormal_match.get("unit_impact", {})
        affected = {}
        for unit, score in rot.items():
            uk = "attack"
            if "防线" in unit or "后卫" in unit:
                uk = "defense"
            elif "中场" in unit or "中前卫" in unit:
                uk = "midfield"
            elif "门将" in unit:
                uk = "goalkeeper"
            affected[uk] = "RED_IF_ABSENT" if "RED" in str(score) else "AMBER"
        teams = [{
            "team": abnormal_match.get("home", "home_team"),
            "side": "home",
            "needs_latest_confirmation": converted,
            "affected_units": affected
        }]

    fg = abnormal_match.get("fact_gate", {})
    fg_status = fg.get("status", "PARTIAL_PASS")
    fg_confidence = fg.get("final_confidence", "medium_low")
    fg_reasons = fg.get("reason", ["abnormal_parsed_clean"])

    susp_lines = []
    has_players = False
    has_needs_confirm_only = False

    # 三分层渲染
    layer_map = [
        ("confirmed",                 "✅ 已确诊缺阵",    "high"),
        ("supported",                 "🟡 消息支持缺/复", "medium"),
        ("needs_latest_confirmation", "⚠️ 临场需最终确认", "low_to_medium"),
    ]

    for t_data in teams:
        side_zh = "主队" if t_data.get("side", "home") == "home" else "客队"
        team_name = t_data.get("team", "未知")

        has_any_confirmed = bool(t_data.get("confirmed"))
        has_any_needs = bool(t_data.get("needs_latest_confirmation"))
        if has_any_needs and not has_any_confirmed:
            has_needs_confirm_only = True

        for key, label, default_conf in layer_map:
            for p in t_data.get(key, []):
                has_players = True
                p_name = p.get("player", "未知")
                p_unit = p.get("unit", "unknown")
                p_conf = p.get("confidence", default_conf)
                p_reason = p.get("reason", p.get("status", "暂无详情"))
                susp_lines.append(
                    f"- **{p_name}** [{side_zh} / `{p_unit}`] "
                    f"({label}, 置信度: `{p_conf}`): {p_reason}"
                )

    abnormal_suspicious = (
        "\n".join(susp_lines) if has_players
        else "* 事实门禁：当前无已确认或存疑伤停，阵容事实基础清洁。"
    )

    rot_lines = []
    has_rot = False
    for t_data in teams:
        side_zh = "主队" if t_data.get("side", "home") == "home" else "客队"
        team_name = t_data.get("team", "未知")
        for unit, rating in t_data.get("affected_units", {}).items():
            has_rot = True
            clean = "RED_IF_ABSENT" if "RED" in str(rating) else ("AMBER" if "AMBER" in str(rating) else str(rating))
            rot_lines.append(f"- **{team_name} ({side_zh}) `{unit}`单元**: `{clean}`")

    abnormal_rotation = (
        "\n".join(rot_lines) if has_rot
        else "* 事实门禁：阵容基本面稳定，未激活受灾单元减震评级。"
    )

    # 级联降级：所有球员仅在 needs_latest_confirmation → PARTIAL_PASS
    if has_needs_confirm_only:
        fg_status = "PARTIAL_PASS"
        fg_confidence = "medium_low"
        fg_reasons = [
            "no_confirmed_absences",
            "player_availability_requires_latest_team_news",
            "abnormal_info_usable_only_as_context"
        ]

    return abnormal_suspicious, abnormal_rotation, fg_status, fg_confidence, fg_reasons


def calculate_auto_risk_tags(
    home_avg_xg: float, home_defensive_leakage: float,
    home_strength: str, home_contaminated: bool,
    away_avg_xg: float, away_defensive_leakage: float,
    away_strength: str, away_contaminated: bool,
    handicap: float, euro_h: float, euro_a: float,
    market_status: str, abnormal_status: str,
    market_move_raw: Dict[str, Any]
) -> Tuple[List[str], str, str, str, Dict[str, Any], Dict[str, Any]]:
    """
    V2.3 Auto Risk Tags + 双轨大裂缝研判。
    返回: (tags, p_side, p_conf, p_reason, market_internal_div, market_process_div)
    """
    tags = []

    if market_status == "MISSING":
        tags.append("MARKET_DATA_MISSING")
    if abnormal_status == "MISSING":
        tags.append("ABNORMAL_DATA_MISSING")
    if home_strength == "weak" or away_strength == "weak":
        tags.append("TEAM_ARCHIVE_WEAK")
    if home_contaminated or away_contaminated:
        tags.append("CONTAMINATION_HISTORY")

    if home_defensive_leakage <= 0.8:
        tags.append("HOME_DEFENSIVE_FLOOR_HIGH")
    if away_defensive_leakage <= 0.8:
        tags.append("AWAY_DEFENSIVE_FLOOR_HIGH")
    if home_defensive_leakage > 1.8:
        tags.append("HOME_DEFENSIVE_LEAKAGE_HIGH")
    if away_defensive_leakage > 1.8:
        tags.append("AWAY_DEFENSIVE_LEAKAGE_HIGH")

    is_home_fav = (handicap <= -0.75) or (0.0 < euro_h < 1.70)
    is_away_fav = (handicap >= 0.75) or (0.0 < euro_a < 1.70)

    home_net = home_avg_xg - home_defensive_leakage
    away_net = away_avg_xg - away_defensive_leakage

    p_side, p_reason_parts = "Equal", []
    if home_net > away_net + 0.3:
        p_side = "Home"
        p_reason_parts.append(f"主队过程净期望 ({home_net:.4f}) 明显优于客队 ({away_net:.4f})")
    elif away_net > home_net + 0.3:
        p_side = "Away"
        p_reason_parts.append(f"客队过程净期望 ({away_net:.4f}) 明显优于主队 ({home_net:.4f})")
    else:
        p_reason_parts.append(f"主客净期望差异不显著 (主 {home_net:.4f} vs 客 {away_net:.4f})，势均力敌")

    p_conf = "high"
    if home_strength == "weak" or away_strength == "weak":
        p_conf = "medium_low"
        weak = "主客两队" if (home_strength == "weak" and away_strength == "weak") else ("主队" if home_strength == "weak" else "客队")
        p_reason_parts.append(f"注意：{weak}底座物理档案为 weak，置信度受限降级")

    # 欧亚内部裂缝
    has_split = bool(market_move_raw.get("euro_asian_split", False))
    if has_split:
        tags.append("EURO_ASIAN_SPLIT")
    market_internal_div = {
        "status": has_split,
        "note": (
            "欧赔与亚盘方向不一致，盘口信号分裂；让球方向置信度需降级。"
            if has_split else
            "欧赔与亚盘同向移动，未发现内部水位异常裂痕。"
        )
    }

    # 市场-底座大裂缝
    market_process_div = {
        "status": False,
        "severity": "none",
        "note": "市场让球盘口与主客常态物理画像优势基本吻合，处于合理波动区间。"
    }

    if market_status == "MISSING":
        market_process_div["note"] = "MARKET_JSON_MISSING: 博弈大裂缝研判强挂起。"
    else:
        euro_sig = str(market_move_raw.get("euro_signal", "")).upper()
        asian_sig = str(market_move_raw.get("asian_signal", "")).upper()

        # 1. 强热门方差保护 (STRONG_FAVORITE_VARIANCE_GUARD) 物理规则 (尤文场)
        # 主队是深盘强热门，客队防守下限极高且主队过程优势并非压倒性时
        if is_home_fav and handicap <= -0.75 and away_defensive_leakage <= 0.8:
            tags.append("STRONG_FAVORITE_VARIANCE_GUARD")
            tags.append("PROCESS_RIGHT_RESULT_RISK")

        # 2. 亚盘深但欧赔主胜修复冲突 (ASIAN_DEEP_EURO_REPAIR_CONFLICT) 物理规则 (Nice场)
        if handicap <= -1.0 and ("HOME_WEAKENED" in euro_sig or "AWAY_STRENGTHENED" in euro_sig):
            tags.append("ASIAN_DEEP_EURO_REPAIR_CONFLICT")
            market_process_div = {
                "status": True,
                "severity": "medium",
                "note": "亚盘深让但欧赔主胜指数呈现弱化修复趋势，形成深盘与欧赔反向冲突。"
            }

        # 3. 平局过度压缩 (DRAW_COMPRESSED & LOW_EVENT_GAME) 物理规则 (奥萨苏纳场)
        # 通过 market_move 的 total_signal 判断大小球界限
        total_sig = str(market_move_raw.get("total_signal", "")).upper()
        if "TOTAL_DOWN" in total_sig and abs(handicap) <= 0.25:
            tags.append("DRAW_COMPRESSED")
            tags.append("LOW_TOTAL_REPAIR")
            tags.append("LOW_EVENT_GAME")

        # 逻辑 1：过程偏强方但市场退水/大让步降权 (PROCESS_EDGE_FAVORITE_BUT_MARKET_RETREAT)
        fav_retreat_detected = False
        if p_side == "Away" and ("AWAY_WEAKENED" in euro_sig or "AWAY_SHALLOW" in asian_sig):
            fav_retreat_detected = True
        elif p_side == "Home" and ("HOME_WEAKENED" in euro_sig or "HOME_SHALLOW" in asian_sig):
            fav_retreat_detected = True

        # 逻辑 2：欧亚分裂，且过程与欧赔同向、但亚盘反向 (PROCESS_AND_EURO_SUPPORT_FAVORITE_ASIAN_SUPPORTS_UNDERDOG)
        euro_asian_process_split = False
        if has_split:
            if p_side == "Away" and ("AWAY_DOWN" in euro_sig or "AWAY_STRENGTHENED" in euro_sig) and ("HOME_DEEPENED" in asian_sig or "HOME_SUPPORTED" in asian_sig):
                euro_asian_process_split = True
                tags.append("PROCESS_AND_EURO_SUPPORT_AWAY_ASIAN_SUPPORTS_HOME")
                market_process_div = {
                    "status": True,
                    "severity": "medium",
                    "note": "过程与欧赔均偏客队，但亚盘仍加深主队，形成 process/euro vs asian 分裂。"
                }
            elif p_side == "Home" and ("HOME_DOWN" in euro_sig or "HOME_STRENGTHENED" in euro_sig) and ("AWAY_DEEPENED" in asian_sig or "AWAY_SUPPORTED" in asian_sig):
                euro_asian_process_split = True
                tags.append("PROCESS_AND_EURO_SUPPORT_HOME_ASIAN_SUPPORTS_AWAY")
                market_process_div = {
                    "status": True,
                    "severity": "medium",
                    "note": "过程与欧赔均偏主队，但亚盘仍加深客队，形成 process/euro vs asian 分裂。"
                }

        # 逻辑 3：若不是上述特殊分裂，再进入常规主/客强过程冲突判定
        if not euro_asian_process_split and not market_process_div.get("status"):
            if fav_retreat_detected:
                tags.append("PROCESS_EDGE_FAVORITE_BUT_MARKET_RETREAT")
                market_process_div = {
                    "status": True,
                    "severity": "medium",
                    "note": f"过程仍偏{p_side}队，但市场显著削弱{p_side}队，修复对手；属于过程强方被市场降权。"
                }
            else:
                if is_home_fav:
                    if home_avg_xg < 1.1 or home_defensive_leakage > 1.8:
                        tags.append("MARKET_OVERPRICES_HOME")
                    if away_defensive_leakage <= 0.8:
                        tags.append("FAVORITE_DEEP_HANDICAP_CAUTION")
                    if p_side == "Away":
                        tags.append("MARKET_PROCESS_CONFLICT")
                        market_process_div = {
                            "status": True, "severity": "high",
                            "note": f"【强警告】市场深让主队（让 `{handicap}`），但底座过程优势在客队，市场与物理过程严重大分裂！"
                        }
                elif is_away_fav:
                    if away_avg_xg < 1.1 or away_defensive_leakage > 1.8:
                        tags.append("MARKET_OVERPRICES_AWAY")
                    if home_defensive_leakage <= 0.8:
                        tags.append("FAVORITE_DEEP_HANDICAP_CAUTION")
                    if p_side == "Home":
                        tags.append("MARKET_PROCESS_CONFLICT")
                        market_process_div = {
                            "status": True, "severity": "high",
                            "note": f"【强警告】市场深让客队（让 `{handicap}`），但底座过程优势在主队，市场与物理过程严重大分裂！"
                        }

    return (
        sorted(list(set(tags))),
        p_side, p_conf, "; ".join(p_reason_parts),
        market_internal_div, market_process_div
    )


def calculate_prematch_mode(
    risk_tags: List[str],
    fg_status: str,
    market_process_div: Dict[str, Any],
    market_internal_div: Dict[str, Any],
    market_status: str,
    ab_status: str
) -> Tuple[str, int]:
    """
    根据风险标签与裂缝信号，计算 prematch_mode 和 deep_queue_score。
    HALT(0-1) / LIGHT(2-4) / STANDARD(5-7) / DEEP(8+)
    """
    if market_status == "MISSING" and ab_status == "MISSING":
        return "HALT", 0

    score = 0

    HIGH_TAGS   = {
        "MARKET_PROCESS_CONFLICT", "MARKET_OVERPRICES_HOME", "MARKET_OVERPRICES_AWAY",
        "FAVORITE_RETREAT", "EURO_ASIAN_SPLIT", "SURVIVAL_WIN_CONVERSION_GATE",
        "PROCESS_EDGE_FAVORITE_BUT_MARKET_RETREAT",
        "PROCESS_AND_EURO_SUPPORT_AWAY_ASIAN_SUPPORTS_HOME",
        "PROCESS_AND_EURO_SUPPORT_HOME_ASIAN_SUPPORTS_AWAY",
        # V2.3 扩容 10 场测试集新增高危博弈大门禁
        "STRONG_FAVORITE_VARIANCE_GUARD",
        "ROTATION_RISK",
        "MARKET_OVERPRICES_MOTIVATION_SIDE",
        "SURVIVAL_PRICE_OVERCOMPRESSION",
        "DRAW_COMPRESSED",
        "ASIAN_DEEP_EURO_REPAIR_CONFLICT",
        "LOCKED_TARGET_DEFLATION"
    }
    MEDIUM_TAGS = {
        "FAVORITE_DEEP_HANDICAP_CAUTION", "HOME_DEFENSIVE_LEAKAGE_HIGH",
        "AWAY_DEFENSIVE_LEAKAGE_HIGH", "HOME_DEFENSIVE_FLOOR_HIGH", "AWAY_DEFENSIVE_FLOOR_HIGH",
        "UNDERDOG_WIN_LIVE", "EURO_ASIAN_SPLIT_PRIORITY",
        "AWAY_REPAIR", "HANDICAP_CONFIDENCE_DOWNGRADE", "DRAW_PROTECTION",
        # V2.3 扩容中危标签
        "PROCESS_RIGHT_RESULT_RISK",
        "LOW_TOTAL_REPAIR",
        "LOW_EVENT_GAME"
    }
    LOW_TAGS    = {
        "TEAM_ARCHIVE_WEAK", "CONTAMINATION_HISTORY",
        "ABNORMAL_DATA_MISSING", "MARKET_DATA_MISSING",
        "CLEAN_STRONG_FAVORITE", "PROCESS_AND_MOTIVATION_ALIGNED", "MARKET_SUPPORTS_STRONG_HOME",
        # V2.3 扩容低危标签
        "STRONG_HOME_DIRECTION"
    }

    for t in risk_tags:
        if t in HIGH_TAGS:
            score += 3
        elif t in MEDIUM_TAGS:
            score += 2
        elif t in LOW_TAGS:
            score += 1

    # 大裂缝加权
    if market_process_div.get("status"):
        if market_process_div.get("severity") == "high":
            score += 3
        elif market_process_div.get("severity") == "medium":
            score += 2
    if market_internal_div.get("status"):
        score += 2

    # 事实门禁不完整
    if fg_status == "PARTIAL_PASS":
        score += 1
    elif fg_status == "NEEDS_VERIFICATION":
        score += 2

    # 单边缺失
    if market_status == "MISSING" or ab_status == "MISSING":
        score += 1

    if score >= 8:
        mode = "DEEP"
    elif score >= 5:
        mode = "STANDARD"
    else:
        mode = "LIGHT"

    return mode, score


def _extract_handicap_euro(odds: Dict[str, Any]) -> Tuple[float, float, float]:
    """从 odds/odds_avg 中提取 handicap/euro_h/euro_a 浮点数，兼容新旧格式。"""
    h, eh, ea = 0.0, 0.0, 0.0
    if not odds:
        return h, eh, ea
    try:
        euro_raw = odds.get("euro", {})
        is_new = isinstance(euro_raw, dict) and "current" in euro_raw
        if is_new:
            h  = float(odds.get("asian", {}).get("current", {}).get("handicap", 0.0))
            eh = float(euro_raw.get("current", {}).get("home", 0.0))
            ea = float(euro_raw.get("current", {}).get("away", 0.0))
        else:
            h  = float(odds.get("asian", {}).get("handicap", 0.0))
            eh = float(euro_raw.get("h", 0.0))
            ea = float(euro_raw.get("a", 0.0))
    except (TypeError, ValueError):
        pass
    return h, eh, ea


# ────────────────────────────────────────────────────────────────
# 4. 一键打包核心流程
# ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Ares Prematch 资料包引擎 V2.3")
    parser.add_argument("--date", required=True, help="比赛日日期 (YYYYMMDD)")
    args = parser.parse_args()

    project_root = get_project_root()
    vault_root   = get_vault_path()

    matchday_dir = vault_root / "03_Match_Audits" / f"AresMatchday_{args.date}"
    matchday_dir.mkdir(parents=True, exist_ok=True)

    alias_map = load_alias_map(project_root)

    # 1. 读取 market.json
    market_path = matchday_dir / "market.json"
    market_data_present, market_matches = False, []
    if market_path.exists():
        try:
            raw = json.loads(market_path.read_text(encoding="utf-8"))
            market_matches = raw if isinstance(raw, list) else raw.get("matches", [])
            market_data_present = True
            logger.info(f"成功加载 market.json，包含 {len(market_matches)} 场记录。")
        except Exception as e:
            logger.error(f"解析 market.json 失败: {e}")
    else:
        logger.warning(f"market.json 缺失: {market_path}，将启动 Demo 数据。")

    # 2. 读取 abnormal.json
    abnormal_path = matchday_dir / "abnormal.json"
    abnormal_data_present, abnormal_matches_dict = False, {}
    if abnormal_path.exists():
        try:
            raw = json.loads(abnormal_path.read_text(encoding="utf-8"))
            abnormal_matches_dict = raw.get("matches", raw)
            abnormal_data_present = True
            logger.info("成功加载 abnormal.json。")
        except Exception as e:
            logger.error(f"解析 abnormal.json 失败: {e}")

    # 3. Demo 回退注入 (Ares v2.3 标准 Schema)
    if not market_data_present and abnormal_data_present:
        logger.warning("market.json 缺失但 abnormal.json 存在，注入 Demo 比赛列表（MISSING 门禁激活）。")
        market_matches = [{
            "match_no": "01", "match_id": "01",
            "home": "Tottenham Hotspur", "away": "Everton",
            "home_team": "热刺", "away_team": "埃弗顿",
            "league": "EPL", "kickoff": "2026-05-23 15:00:00",
            "data_source": "Understat EPL 2025/2026"
        }]
    elif market_data_present and not abnormal_data_present:
        logger.warning("market.json 存在但 abnormal.json 缺失，保持伤停事实缺失状态。")
    elif not market_data_present and not abnormal_data_present:
        logger.info("双文件均缺失，注入热刺 vs 埃弗顿 Ares v2.3 Demo 演练数据...")
        market_matches = [{
            "match_no": "01", "match_id": "01",
            "home": "Tottenham Hotspur", "away": "Everton",
            "home_team": "热刺", "away_team": "埃弗顿",
            "league": "EPL", "kickoff": "2026-05-23 15:00:00",
            "data_source": "Understat EPL 2025/2026",
            "sanity_check": {
                "euro_order": "home/draw/away",
                "asian_format": "home_water / handicap_from_home_view / away_water",
                "handicap_sign_rule": {"negative": "home_gives_ball", "positive": "home_receives_ball"}
            },
            "odds_avg": {
                "euro":  {"initial": {"home": 1.75, "draw": 4.00, "away": 4.60},
                           "current": {"home": 1.55, "draw": 4.20, "away": 5.50}},
                "asian": {"initial": {"home_water": 0.92, "handicap": -0.75, "away_water": 0.96},
                           "current": {"home_water": 0.85, "handicap": -1.0,  "away_water": 1.05}},
                "total": {"initial": {"over_water": 0.92, "line": 2.5,  "under_water": 0.96},
                           "current": {"over_water": 0.90, "line": 2.75, "under_water": 0.98}}
            },
            "market_move": {
                "euro_signal": "HOME_STRENGTHENED", "asian_signal": "HOME_DEEPENED",
                "total_signal": "TOTAL_UP", "euro_asian_split": False
            },
            "market_tags": ["HOME_EURO_STRENGTHENED", "FAVORITE_DEEPENED", "EURO_ASIAN_ALIGNED"],
            "risk_tags": ["FAVORITE_DEEP_HANDICAP_CAUTION"]
        }]
        abnormal_matches_dict = {
            "01": {
                "match_no": "01", "home": "Tottenham Hotspur", "away": "Everton",
                "fact_gate": {
                    "status": "PARTIAL_PASS", "final_confidence": "medium_low",
                    "reason": ["only_suspicious_items", "latest_availability_not_confirmed"]
                },
                "teams": [
                    {
                        "team": "Tottenham Hotspur", "side": "home",
                        "target_status": {"label": "needs_verification", "motivation_level": "unknown", "confidence": "low"},
                        "confirmed": [],
                        "supported": [],
                        "needs_latest_confirmation": [
                            {"player": "Dominic Solanke", "unit": "attack",
                             "status": "late_check", "confidence": "low_to_medium",
                             "reason": "周中脚踝扭伤，主帅表示临场 late check，出场率约 40%。"}
                        ],
                        "affected_units": {"attack": "AMBER"}
                    },
                    {
                        "team": "Everton", "side": "away",
                        "target_status": {"label": "mid_table_pressure", "motivation_level": "medium", "confidence": "low"},
                        "confirmed": [],
                        "supported": [
                            {"player": "Dominic Calvert-Lewin", "unit": "attack",
                             "status": "returning_from_injury", "confidence": "medium",
                             "reason": "本周全训，消息来源称极大概率可出战（70%+）。"}
                        ],
                        "needs_latest_confirmation": [
                            {"player": "Jarrad Branthwaite", "unit": "defense",
                             "status": "illness_doubt", "confidence": "low_to_medium",
                             "reason": "未参加赛前发布会，患流感，临场上阵成疑。"}
                        ],
                        "affected_units": {"defense": "RED_IF_ABSENT"}
                    }
                ]
            }
        }
        market_data_present = True
        abnormal_data_present = True

    # 读取审计卡模板
    template_path = Path(__file__).parent.parent / "templates" / "audit_input_template.md"
    if not template_path.exists():
        logger.error(f"缺失推演卡模板: {template_path}")
        sys.exit(1)
    audit_template = template_path.read_text(encoding="utf-8")

    csv_rows = []

    # 4. 遍历比赛打包
    for idx, m in enumerate(market_matches):
        m_id = m.get("match_id", m.get("match_no", f"{idx+1:02d}"))
        try:
            m_id_str = f"{int(m_id):02d}"
        except Exception:
            m_id_str = str(m_id)

        home = m.get("home_team", m.get("home", "HomeTeam"))
        away = m.get("away_team", m.get("away", "AwayTeam"))
        kickoff = m.get("kickoff", f"{args.date[:4]}-{args.date[4:6]}-{args.date[6:8]} 00:00:00")
        ds = m.get("data_source", "Understat")

        home_en = resolve_team_name(home, alias_map)
        away_en = resolve_team_name(away, alias_map)

        home_dir_part = re.sub(r"\s+", "_", home_en)
        away_dir_part = re.sub(r"\s+", "_", away_en)
        match_folder = f"{m_id_str}_{home_dir_part}_{away_dir_part}"
        match_dir = matchday_dir / match_folder
        match_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"===> [{m_id_str}] {home} vs {away}")

        # 4.1 底座拷贝
        home_coach, home_formation, home_tactical_style = "未知", "4-3-3", "进攻足球"
        home_avg_xg, home_def_leak, home_conv_eff, home_tact_entropy = 1.0, 1.0, 0.05, 0.40
        home_strength, home_contaminated = "unknown", False

        away_coach, away_formation, away_tactical_style = "未知", "4-3-3", "防守反击"
        away_avg_xg, away_def_leak, away_conv_eff, away_tact_entropy = 1.0, 1.0, 0.05, 0.40
        away_strength, away_contaminated = "unknown", False

        for side, en_name, prefix, slot in [
            ("home", home_en, f"{m_id_str}_home", "home"),
            ("away", away_en, f"{m_id_str}_away", "away")
        ]:
            archive = fuzzy_find_archive_path(vault_root, en_name)
            if archive:
                shutil.copy2(archive, match_dir / f"{prefix}.md")
                logger.info(f"     [只读拷贝] {archive.name} -> {prefix}.md")
                fm, _ = split_frontmatter(archive.read_text(encoding="utf-8"))
                pr = fm.get("physical_reality", {})
                if side == "home":
                    home_coach = fm.get("coach", "未知")
                    home_formation = fm.get("base_formation", "4-3-3")
                    home_tactical_style = fm.get("tactical_style", "进攻足球")
                    home_strength = fm.get("archive_strength", "unknown")
                    home_contaminated = "CONTAMINATED" in str(fm.get("contamination_status", "")) or "contamination_note" in fm
                    if isinstance(pr, dict):
                        home_avg_xg  = float(pr.get("avg_xG_last_5", 1.0))
                        home_def_leak = float(pr.get("defensive_leakage", 1.0))
                        home_conv_eff = float(pr.get("conversion_efficiency", 0.05))
                        home_tact_entropy = float(pr.get("actual_tactical_entropy", 0.40))
                else:
                    away_coach = fm.get("coach", "未知")
                    away_formation = fm.get("base_formation", "4-3-3")
                    away_tactical_style = fm.get("tactical_style", "防守反击")
                    away_strength = fm.get("archive_strength", "unknown")
                    away_contaminated = "CONTAMINATED" in str(fm.get("contamination_status", "")) or "contamination_note" in fm
                    if isinstance(pr, dict):
                        away_avg_xg  = float(pr.get("avg_xG_last_5", 1.0))
                        away_def_leak = float(pr.get("defensive_leakage", 1.0))
                        away_conv_eff = float(pr.get("conversion_efficiency", 0.05))
                        away_tact_entropy = float(pr.get("actual_tactical_entropy", 0.40))
            else:
                logger.warning(f"     [!] 缺失{'主' if side=='home' else '客'}队底座: {en_name}")
                (match_dir / f"{prefix}.md").write_text(
                    f"# {home if side=='home' else away}\n\n底座档案缺失，请运行 team_forge 初始化。",
                    encoding="utf-8"
                )

        # 4.2 赔率处理
        odds = m.get("odds_avg", m.get("odds", {}))
        market_move = m.get("market_move", {})
        m_status = "READY" if market_data_present else "MISSING"

        # 计算 market_move_detail
        is_new_odds = isinstance(odds.get("euro", {}), dict) and "current" in odds.get("euro", {})
        market_move_detail = calculate_market_move_detail(odds) if (m_status == "READY" and is_new_odds) else {}

        if m_status == "READY":
            # 写 market.json（附加 market_move_detail）
            market_out = dict(m)
            market_out["market_move_detail"] = market_move_detail
            (match_dir / f"{m_id_str}_market.json").write_text(
                json.dumps(market_out, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            md_details = format_market_data(odds, market_move, market_move_detail, "READY")
            market_md = (
                f"# 赔率市场速览卡 (Market Reality Card)\n\n"
                f"- **本场对阵**: {home} vs {away}\n"
                f"- **物理映射**: {home_en} vs {away_en}\n"
                f"- **数据状态**: READY\n\n"
                f"## 欧赔与亚盘让手细目\n{md_details}\n"
            )
            (match_dir / f"{m_id_str}_market.md").write_text(market_md, encoding="utf-8")
            logger.info(f"     [market] {m_id_str}_market.json / .md")
        else:
            odds, market_move, market_move_detail = {}, {}, {}
            (match_dir / f"{m_id_str}_market.md").write_text(
                f"# 赔率市场速览卡\n\n- **数据状态**: MISSING (HALT_FOR_MARKET)\n\n"
                f"> [!CAUTION]\n> **博弈分析强挂起：无真实 market.json！**\n", encoding="utf-8"
            )
            logger.warning(f"     [!] market.json MISSING")

        # 4.3 伤停处理
        ab_match = abnormal_matches_dict.get(m_id, abnormal_matches_dict.get(m_id_str, {}))
        ab_status = "READY" if (abnormal_data_present and ab_match) else "MISSING"

        if ab_status == "READY":
            # 4.3.1 战意物理防卫降级：非 standings_context 的异常模块推导强制限制置信度为 medium_low
            if isinstance(ab_match, dict) and "teams" in ab_match:
                for t_data in ab_match.get("teams", []):
                    t_status = t_data.get("target_status")
                    if isinstance(t_status, dict):
                        t_status["source"] = "abnormal_inference"
                        if t_status.get("confidence") in ["high", "medium"]:
                            t_status["confidence"] = "medium_low"

            (match_dir / f"{m_id_str}_abnormal.json").write_text(
                json.dumps(ab_match, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            ab_susp, ab_rot, fg_status, fg_conf, fg_reasons = extract_abnormal_blocks(ab_match, "READY")
            abnormal_md = (
                f"# 事实门禁与伤停异动卡 (Team Abnormalities Card)\n\n"
                f"- **本场对阵**: {home} vs {away}\n"
                f"- **事实门禁状态**: `{fg_status}` (置信度: `{fg_conf}`)\n\n"
                f"## 🚨 存疑伤停异动 (三分层)\n{ab_susp}\n\n"
                f"## 💥 受灾单元与阵容残缺评级\n{ab_rot}\n"
            )
            (match_dir / f"{m_id_str}_abnormal.md").write_text(abnormal_md, encoding="utf-8")
            logger.info(f"     [abnormal] {m_id_str}_abnormal.json / .md | fact_gate: {fg_status}")
        else:
            ab_match = {}
            ab_susp = "> [!WARNING]\n> **NEEDS_VERIFICATION** (abnormal.json 缺失，置信度降级)"
            ab_rot  = "* 门禁检测：因数据缺失未发现事实，受灾分析暂未激活。"
            fg_status, fg_conf = "NEEDS_VERIFICATION", "medium_low"
            fg_reasons = ["abnormal_json_missing", "source_evidence_missing"]
            (match_dir / f"{m_id_str}_abnormal.md").write_text(
                f"# 事实门禁与伤停异动卡\n\n- **数据状态**: NEEDS_VERIFICATION\n\n"
                f"> [!WARNING]\n> **降级防卫已激活：abnormal.json 缺失！**\n", encoding="utf-8"
            )

        # 4.4 提取盘口浮点
        handicap_val, euro_h_val, euro_a_val = _extract_handicap_euro(odds) if m_status == "READY" else (0.0, 0.0, 0.0)

        # 4.5 博弈冲突标签 + 双轨大裂缝
        risk_tags, p_side, p_conf, p_reason, m_int_div, m_proc_div = calculate_auto_risk_tags(
            home_avg_xg, home_def_leak, home_strength, home_contaminated,
            away_avg_xg, away_def_leak, away_strength, away_contaminated,
            handicap_val, euro_h_val, euro_a_val,
            m_status, ab_status, market_move
        )

        # 合并 market.json 里的自定义 risk_tags，保障保级/强热门/冷门等语义标签弹性融合
        extra_tags = m.get("risk_tags", [])
        if extra_tags:
            risk_tags = sorted(list(set(risk_tags + extra_tags)))

        # 4.6 Prematch Mode + Deep Queue Score
        prematch_mode, deep_queue_score = calculate_prematch_mode(
            risk_tags, fg_status, m_proc_div, m_int_div, m_status, ab_status
        )
        logger.info(f"     [auto_tags] {risk_tags}")
        logger.info(f"     [prematch_mode] {prematch_mode} (score={deep_queue_score}) | process_edge={p_side}({p_conf})")

        # 4.7 格式化模板参数
        auto_risk_tags_fmt = " | ".join([f"`{t}`" for t in risk_tags]) if risk_tags else "`Aligned`"
        fg_reason_yaml = json.dumps(fg_reasons, ensure_ascii=False)
        fg_reason_fmt  = "\n".join([f"  - {r}" for r in fg_reasons])

        if m_status == "MISSING":
            int_div_fmt  = "`MARKET_JSON_MISSING` (欧亚大裂缝研判挂起)"
            proc_div_fmt = "`MARKET_JSON_MISSING` (博弈大裂缝研判挂起)"
        else:
            int_div_fmt  = (f"`SPLIT_DETECTED` {m_int_div.get('note','')}"
                           if m_int_div.get("status") else
                           f"`ALIGNED` {m_int_div.get('note','')}")
            proc_div_fmt = (f"`CONFLICT_SEVERITY:{m_proc_div.get('severity','none').upper()}` {m_proc_div.get('note','')}"
                           if m_proc_div.get("status") else
                           f"`ALIGNED` {m_proc_div.get('note','')}")

        market_data_block = format_market_data(odds, market_move, market_move_detail, m_status)

        # 生成 Game Script Seed 变量块（变量式推演）
        game_script_seed_block = _build_game_script_seed(
            home, away, home_avg_xg, home_def_leak, away_avg_xg, away_def_leak,
            handicap_val, p_side, risk_tags
        )

        filled_audit = audit_template.format(
            match_no=m_id_str,
            home_team=home, away_team=away,
            league=m.get("league", "EPL"), kickoff=kickoff,
            market_json_status=m_status, abnormal_json_status=ab_status,
            home_card_status=f"READY_{home_strength.upper()}",
            away_card_status=f"READY_{away_strength.upper()}",
            home_resolved=home_en, away_resolved=away_en,
            process_edge_side=p_side, process_edge_confidence=p_conf,
            process_edge_reason=json.dumps(p_reason, ensure_ascii=False),
            process_edge_reason_formatted=p_reason,
            market_internal_divergence_status=json.dumps(m_int_div.get("status", False)),
            market_internal_divergence_note=m_int_div.get("note", ""),
            market_internal_divergence_formatted=int_div_fmt,
            market_process_divergence_status=json.dumps(m_proc_div.get("status", False)),
            market_process_divergence_severity=m_proc_div.get("severity", "none"),
            market_process_divergence_note=m_proc_div.get("note", ""),
            market_process_divergence_formatted=proc_div_fmt,
            auto_risk_tags=json.dumps(risk_tags),
            auto_risk_tags_formatted=auto_risk_tags_fmt,
            prematch_mode=prematch_mode,
            deep_queue_score=deep_queue_score,
            market_data_block=market_data_block,
            fact_gate_status=fg_status, fact_gate_confidence=fg_conf,
            fact_gate_reason_yaml=fg_reason_yaml,
            fact_gate_reason_formatted=fg_reason_fmt,
            abnormal_suspicious_block=ab_susp,
            abnormal_rotation_block=ab_rot,
            home_coach=home_coach, home_formation=home_formation,
            home_tactical_style=home_tactical_style,
            home_avg_xg=f"{home_avg_xg:.4f}", home_defensive_leakage=f"{home_def_leak:.4f}",
            home_conversion_efficiency=f"{home_conv_eff:.4f}",
            home_actual_tactical_entropy=f"{home_tact_entropy:.4f}",
            away_coach=away_coach, away_formation=away_formation,
            away_tactical_style=away_tactical_style,
            away_avg_xg=f"{away_avg_xg:.4f}", away_defensive_leakage=f"{away_def_leak:.4f}",
            away_conversion_efficiency=f"{away_conv_eff:.4f}",
            away_actual_tactical_entropy=f"{away_tact_entropy:.4f}",
            game_script_seed_block=game_script_seed_block,
            data_source=ds
        )

        audit_path = match_dir / f"{m_id_str}_audit_input.md"
        audit_path.write_text(filled_audit, encoding="utf-8")
        logger.info(f"     [audit_input] {m_id_str}_audit_input.md")

        # 4.8 CSV 行
        status_code = "READY"
        if m_status == "MISSING" and ab_status == "MISSING":
            status_code = "HALT"
        elif m_status == "MISSING":
            status_code = "MISSING_MARKET"
        elif ab_status == "MISSING":
            status_code = "MISSING_ABNORMAL"
        elif fg_status == "PARTIAL_PASS":
            status_code = "PARTIAL_PASS"

        csv_rows.append({
            "match_no": m_id_str,
            "match_id": m.get("match_id", m_id_str),
            "league": m.get("league", "EPL"),
            "kickoff": kickoff,
            "home": home_en, "away": away_en,
            "audit_file": f"{match_folder}/{m_id_str}_audit_input.md",
            "home_card": f"{match_folder}/{m_id_str}_home.md",
            "away_card": f"{match_folder}/{m_id_str}_away.md",
            "market_json": f"{match_folder}/{m_id_str}_market.json",
            "abnormal_json": f"{match_folder}/{m_id_str}_abnormal.json",
            "fact_gate_status": fg_status,
            "process_edge": p_side,
            "risk_tags": "|".join(risk_tags),
            "prematch_mode": prematch_mode,
            "deep_queue_score": deep_queue_score,
            "status": status_code
        })

    # 5. 输出 00_match_list.csv
    csv_path = matchday_dir / "00_match_list.csv"
    csv_headers = [
        "match_no", "match_id", "league", "kickoff", "home", "away",
        "audit_file", "home_card", "away_card", "market_json", "abnormal_json",
        "fact_gate_status", "process_edge", "risk_tags",
        "prematch_mode", "deep_queue_score", "status"
    ]
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_headers)
        writer.writeheader()
        writer.writerows(csv_rows)

    logger.info("=" * 60)
    logger.info("Ares Prematch 引擎 V2.3 (BATCH_READY + DEEP_QUEUE) 运行完成！")
    logger.info(f"拉链大表: {csv_path}")
    logger.info("=" * 60)


def _build_game_script_seed(
    home: str, away: str,
    home_xg: float, home_leak: float,
    away_xg: float, away_leak: float,
    handicap: float,
    process_edge: str,
    risk_tags: List[str]
) -> str:
    """
    生成变量式 Game Script Seed（非战术论文，直接给推演变量）。
    """
    home_lead_stable = home_xg >= 1.2 and home_leak <= 1.2
    away_comeback = away_xg >= 1.2
    high_scoring = (home_xg + away_leak > 2.5) or (away_xg + home_leak > 2.5)
    conflict = "MARKET_PROCESS_CONFLICT" in risk_tags

    lines = []
    lines.append("### Scenario A — 主队先破门")
    lines.append("```yaml")
    lines.append(f"trigger: {home} 率先进球")
    lines.append(f"home_lead_stability: {'HIGH' if home_lead_stable else 'LOW'}")
    lines.append(f"  reason: home_defensive_leakage={home_leak:.2f} ({'稳固' if home_leak <= 1.2 else '偏高，反扑风险大'})")
    lines.append(f"away_comeback_threat: {'HIGH' if away_comeback else 'LOW'}")
    lines.append(f"  reason: away_avg_xg={away_xg:.2f}")
    if conflict:
        lines.append(f"conflict_note: MARKET_PROCESS_CONFLICT 存在 — 市场看好主队但过程优势在{process_edge}，领先后谨慎")
    lines.append(f"set_piece_risk: {'YES' if home_leak > 1.5 else 'LOW'}")
    lines.append("```")
    lines.append("")

    lines.append("### Scenario B — 客队先破门")
    lines.append("```yaml")
    lines.append(f"trigger: {away} 率先进球")
    lines.append(f"away_lead_stability: {'HIGH' if away_xg >= 1.2 and away_leak <= 1.2 else 'MEDIUM'}")
    lines.append(f"  reason: away_defensive_leakage={away_leak:.2f}")
    lines.append(f"home_pressure_level: {'HIGH' if home_xg >= 1.2 else 'LOW'} (home_avg_xg={home_xg:.2f})")
    lines.append(f"handicap_bust_risk: {'YES' if abs(handicap) >= 0.75 else 'LOW'} (market_handicap={handicap:+.2f})")
    lines.append(f"likely_open_game: {'YES' if high_scoring else 'NO'}")
    lines.append("```")
    lines.append("")

    lines.append("### Scenario C — 僵局焦灼 (0-0 至 60min+)")
    lines.append("```yaml")
    lines.append(f"trigger: 双方前60分钟未开分")
    lines.append(f"total_market_line: {2.75 if handicap <= -0.75 else 2.5} (参考大小球界限)")
    lines.append(f"set_piece_decider: YES  # 体能消耗后定位球是核心破局手段")
    lines.append(f"entropy_escalation: home_entropy={0.40:.2f}, away_entropy={0.40:.2f}")
    lines.append(f"late_substitution_impact: HIGH  # 换人后阵型熵值上升，进球概率提升")
    if "AWAY_DEFENSIVE_FLOOR_HIGH" in risk_tags:
        lines.append(f"note: AWAY_DEFENSIVE_FLOOR_HIGH — 客队防线下限极高，焦灼场景客胜/平局价值更大")
    lines.append("```")

    return "\n".join(lines)


if __name__ == "__main__":
    main()
