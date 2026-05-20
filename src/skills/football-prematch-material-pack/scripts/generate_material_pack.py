#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
generate_material_pack.py
==========================
Ares Prematch 资料包一键打包生成引擎 V2.2 (BATCH_READY 架构重构版)
1. 彻底派生单场材料：子目录下分发 xx_market.json / xx_market.md / xx_abnormal.json / xx_abnormal.md。
2. 强置信度防御：在缺失赔率或异动数据时进行 MISSING/NEEDS_VERIFICATION 降级阻断，拒绝假模板数据。
3. 首创博弈冲突标签识别 (Auto Risk Tags)：交叉对比底座常态过程指标 vs 市场深盘让格，自动暴露博弈大裂缝。
4. 双轨大裂缝预警：欧亚内部水位偏离 (market_internal_divergence) + 市场-底座优势偏离 (market_process_divergence)。
5. SOP 字段更新：00_match_list.csv 全新对齐。
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
# 2. 队名强解析与底座模糊检索 (对齐物理画像)
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
    cleaned = name.strip()
    return alias_map.get(cleaned, cleaned)

def is_team_match(name_a: str, name_b: str) -> bool:
    """
    智能队名比对，支持精确匹配与健壮的子串模糊匹配（长度>=4，防御极短噪点）。
    """
    if not name_a or not name_b:
        return False
    norm_a = re.sub(r"[^a-zA-Z0-9]", "", name_a).lower()
    norm_b = re.sub(r"[^a-zA-Z0-9]", "", name_b).lower()
    if norm_a == norm_b:
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
        closing_marker_index = content.find("\n---\n", 4)
        if closing_marker_index != -1:
            frontmatter_raw = content[4:closing_marker_index]
            body = content[closing_marker_index + len("\n---\n") :].lstrip("\n")
            try:
                frontmatter = yaml.safe_load(frontmatter_raw) or {}
                return frontmatter, body
            except Exception as e:
                logger.error(f"解析 Frontmatter YAML 失败: {e}")
    return {}, content

# ────────────────────────────────────────────────────────────────
# 3. 智能数据格式化提取器与博弈冲突交叉筛选 (V2.2 全面升级)
# ────────────────────────────────────────────────────────────────

def format_market_data(odds: Dict[str, Any], market_move: Dict[str, Any], status: str) -> str:
    """
    格式化赔率数据。若缺失则输出强警告，坚决阻断模板脏数据。
    支持兼容旧式 odds/market_move 以及新式 odds_avg/market_move。
    """
    if status == "MISSING" or not odds:
        return (
            "> [!CAUTION]\n"
            "> **MARKET_DATA_STATUS: MISSING**\n"
            "> **MARKET_GATE: HALT_FOR_MARKET** (由于赔率大表缺失或当场无匹配，禁止填充模板假数据，博弈分析强挂起)"
        )

    lines = []

    # 检测是否为新版 odds_avg 格式（有 current/initial 子键）
    euro_raw = odds.get("euro", {})
    is_new_format = isinstance(euro_raw, dict) and "current" in euro_raw

    if is_new_format:
        # 新格式：odds 本身就是 odds_avg 结构
        odds_avg = odds
        euro = odds_avg.get("euro", {})
        if euro:
            curr = euro.get("current", {})
            init = euro.get("initial", {})
            lines.append("- **欧洲独赢盘 (欧赔平均值)**:")
            lines.append(f"  * 当前平均: 主胜 `{curr.get('home', 'N/A')}` | 平局 `{curr.get('draw', 'N/A')}` | 客胜 `{curr.get('away', 'N/A')}`")
            lines.append(f"  * 初始平均: 主胜 `{init.get('home', 'N/A')}` | 平局 `{init.get('draw', 'N/A')}` | 客胜 `{init.get('away', 'N/A')}`")

        asian = odds_avg.get("asian", {})
        if asian:
            curr = asian.get("current", {})
            init = asian.get("initial", {})
            lines.append("- **亚洲让球盘 (让手与贴水)**:")
            lines.append(f"  * 当前平均: 主贴 `{curr.get('home_water', 'N/A')}` | 让步 `{curr.get('handicap', 'N/A')}` | 客贴 `{curr.get('away_water', 'N/A')}`")
            lines.append(f"  * 初始平均: 主贴 `{init.get('home_water', 'N/A')}` | 让步 `{init.get('handicap', 'N/A')}` | 客贴 `{init.get('away_water', 'N/A')}`")

        total = odds_avg.get("total", {})
        if total:
            curr = total.get("current", {})
            init = total.get("initial", {})
            lines.append("- **进球大小盘 (大小球界限)**:")
            lines.append(f"  * 当前平均: 大球贴 `{curr.get('over_water', 'N/A')}` | 界限 `{curr.get('line', 'N/A')}` | 小球贴 `{curr.get('under_water', 'N/A')}`")
            lines.append(f"  * 初始平均: 大球贴 `{init.get('over_water', 'N/A')}` | 界限 `{init.get('line', 'N/A')}` | 小球贴 `{init.get('under_water', 'N/A')}`")

        if market_move:
            lines.append("- **大盘赔率变动信号**:")
            if "euro_signal" in market_move:
                lines.append(f"  * 独赢盘变动: `{market_move.get('euro_signal', 'N/A')}`")
                lines.append(f"  * 让球盘变动: `{market_move.get('asian_signal', 'N/A')}`")
                lines.append(f"  * 大小球变动: `{market_move.get('total_signal', 'N/A')}`")
            else:
                lines.append(f"  * 独赢盘趋势: `{market_move.get('home_win_trend', 'N/A')}`")
                lines.append(f"  * 让球盘变动: `{market_move.get('handicap_move', 'N/A')}`")
    else:
        # 旧格式：odds 是扁平的 euro/asian/over_under 结构
        euro = odds.get("euro", {})
        if euro:
            lines.append(f"- **欧洲独赢盘 (欧赔)**: 主胜 `{euro.get('h', 'N/A')}` | 平局 `{euro.get('d', 'N/A')}` | 客胜 `{euro.get('a', 'N/A')}`")

        asian = odds.get("asian", {})
        if asian:
            lines.append(f"- **亚洲让球盘**: 主贴 `{asian.get('home_water', 'N/A')}` | 让步 `{asian.get('handicap', 'N/A')}` | 客贴 `{asian.get('away_water', 'N/A')}`")

        ou = odds.get("over_under", {})
        if ou:
            lines.append(f"- **进球大小盘**: 大球贴 `{ou.get('over_water', 'N/A')}` | 界限 `{ou.get('line', 'N/A')}` | 小球贴 `{ou.get('under_water', 'N/A')}`")

        if market_move:
            lines.append("- **大盘赔率变动信号**:")
            lines.append(f"  * 独赢盘趋势: `{market_move.get('home_win_trend', 'N/A')}`")
            lines.append(f"  * 让球盘变动: `{market_move.get('handicap_move', 'N/A')}`")

    return "\n".join(lines) if lines else "* 暂无赔率明细记录。"


def extract_abnormal_blocks(
    abnormal_match: Dict[str, Any], status: str
) -> Tuple[str, str, str, str, List[str]]:
    """
    提取存疑伤停和受灾单元，基于全新的 fact_gate schema。
    返回: (abnormal_suspicious, abnormal_rotation, fact_gate_status, fact_gate_confidence, fact_gate_reasons)
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

    # 如果是老格式（suspicious_items + unit_impact），自动兼容转化
    teams = abnormal_match.get("teams", [])
    if not teams and "suspicious_items" in abnormal_match:
        susp = abnormal_match.get("suspicious_items", [])
        converted_items = []
        for item in susp:
            if isinstance(item, dict):
                converted_items.append({
                    "player": item.get("player", "未知"),
                    "unit": "unknown",
                    "status": "needs_confirmation",
                    "confidence": item.get("confidence", "low_to_medium"),
                    "reason": item.get("reason", "情况待确认")
                })

        rot = abnormal_match.get("unit_impact", {})
        affected_units = {}
        for unit, score in rot.items():
            unit_en = "attack"
            if "防线" in unit or "后卫" in unit:
                unit_en = "defense"
            elif "中场" in unit or "中前卫" in unit:
                unit_en = "midfield"
            elif "门将" in unit:
                unit_en = "goalkeeper"
            affected_units[unit_en] = "RED_IF_ABSENT" if "RED" in str(score) else "AMBER"

        teams = [
            {
                "team": abnormal_match.get("home", "home_team"),
                "side": "home",
                "needs_latest_confirmation": converted_items,
                "affected_units": affected_units
            }
        ]

    # 读取 fact_gate 字段（新格式）
    fg = abnormal_match.get("fact_gate", {})
    fg_status = fg.get("status", "PARTIAL_PASS")
    fg_confidence = fg.get("final_confidence", "medium_low")
    fg_reasons = fg.get("reason", ["abnormal_parsed_clean"])

    susp_lines = []
    has_players = False

    for t_data in teams:
        side = t_data.get("side", "home")
        side_zh = "主队" if side == "home" else "客队"

        needs_confirm = t_data.get("needs_latest_confirmation", [])
        confirmed = t_data.get("confirmed", [])
        supported = t_data.get("supported", [])

        for player_list, label in [
            (confirmed, "已确诊缺阵"),
            (supported, "消息支持复出/上阵"),
            (needs_confirm, "临场需确认")
        ]:
            for p in player_list:
                has_players = True
                p_name = p.get("player", "未知")
                p_unit = p.get("unit", "unknown")
                p_conf = p.get("confidence", "low_to_medium")
                p_reason = p.get("reason", p.get("status", "暂无详情说明"))
                susp_lines.append(
                    f"- **{p_name}** [{side_zh} / 战术单元: `{p_unit}`] "
                    f"(置信度: `{p_conf}`, 分类: `{label}`): {p_reason}"
                )

    abnormal_suspicious = (
        "\n".join(susp_lines) if has_players
        else "* 事实门禁：当前无确认或受灾伤停事件报道，阵容事实基础清洁。"
    )

    rot_lines = []
    has_rotations = False
    for t_data in teams:
        team_name = t_data.get("team", "未知")
        side = t_data.get("side", "home")
        side_zh = "主队" if side == "home" else "客队"
        affected = t_data.get("affected_units", {})
        if affected:
            for unit, rating in affected.items():
                has_rotations = True
                clean_rating = str(rating).strip()
                if "RED" in clean_rating:
                    clean_rating = "RED_IF_ABSENT"
                elif "AMBER" in clean_rating:
                    clean_rating = "AMBER"
                rot_lines.append(f"- **{team_name} ({side_zh}) {unit}单元**: `{clean_rating}`")

    abnormal_rotation = (
        "\n".join(rot_lines) if has_rotations
        else "* 事实门禁：阵容基本面稳定，未激活受灾单元减震评级。"
    )

    # 额外防线评估：只要有 needs_latest_confirmation 且无 confirmed，就自动降级
    has_needs_confirm_only = any(
        t_data.get("needs_latest_confirmation") and not t_data.get("confirmed")
        for t_data in teams
    )
    if has_needs_confirm_only:
        fg_status = "PARTIAL_PASS"
        fg_confidence = "medium_low"
        fg_reasons = ["abnormal_json_present", "but_all_player_items_needs_latest_confirmation"]

    return abnormal_suspicious, abnormal_rotation, fg_status, fg_confidence, fg_reasons


def calculate_auto_risk_tags(
    home_avg_xg: float,
    home_defensive_leakage: float,
    home_strength: str,
    home_contaminated: bool,
    away_avg_xg: float,
    away_defensive_leakage: float,
    away_strength: str,
    away_contaminated: bool,
    handicap: float,
    euro_h: float,
    euro_a: float,
    market_status: str,
    abnormal_status: str,
    market_move_raw: Dict[str, Any]
) -> Tuple[List[str], str, str, str, Dict[str, Any], Dict[str, Any]]:
    """
    智能博弈冲突识别算法 (Auto Risk Tags) & 底座优势判断 & 欧亚与大裂缝研判 V2.2。
    返回: (tags, process_edge_side, process_edge_confidence, process_edge_reason,
            market_internal_div, market_process_div)
    handicap: 主队让格，主让一球为 -1.0，客让一球为 1.0
    """
    tags = []

    # 1. 数据缺失预警
    if market_status == "MISSING":
        tags.append("MARKET_DATA_MISSING")
    if abnormal_status == "MISSING":
        tags.append("ABNORMAL_DATA_MISSING")

    # 2. 档案品质预警
    if home_strength == "weak" or away_strength == "weak":
        tags.append("TEAM_ARCHIVE_WEAK")
    if home_contaminated or away_contaminated:
        tags.append("CONTAMINATION_HISTORY")

    # 3. 防守下限判定
    if home_defensive_leakage <= 0.8:
        tags.append("HOME_DEFENSIVE_FLOOR_HIGH")
    if away_defensive_leakage <= 0.8:
        tags.append("AWAY_DEFENSIVE_FLOOR_HIGH")
    if home_defensive_leakage > 1.8:
        tags.append("HOME_DEFENSIVE_LEAKAGE_HIGH")
    if away_defensive_leakage > 1.8:
        tags.append("AWAY_DEFENSIVE_LEAKAGE_HIGH")

    # 4. 交叉对比：市场看好主队/客队
    is_home_favorite = (handicap <= -0.75) or (0.0 < euro_h < 1.70)
    is_away_favorite = (handicap >= 0.75) or (0.0 < euro_a < 1.70)

    # 过程优势研判 (xg - leakage)
    home_net = home_avg_xg - home_defensive_leakage
    away_net = away_avg_xg - away_defensive_leakage

    process_edge_side = "Equal"
    process_edge_reason = []

    if home_net > away_net + 0.3:
        process_edge_side = "Home"
        process_edge_reason.append(f"主队过程净期望 ({home_net:.4f}) 明显优于客队 ({away_net:.4f})")
    elif away_net > home_net + 0.3:
        process_edge_side = "Away"
        process_edge_reason.append(f"客队过程净期望 ({away_net:.4f}) 明显优于主队 ({home_net:.4f})")
    else:
        process_edge_reason.append(
            f"主队净期望 ({home_net:.4f}) 与客队 ({away_net:.4f}) 无明显代际差，底座势均力敌"
        )

    # 计算 process_edge_confidence
    process_edge_confidence = "high"
    if home_strength == "weak" or away_strength == "weak":
        process_edge_confidence = "medium_low"
        if home_strength == "weak" and away_strength == "weak":
            process_edge_reason.append("注意：主客两队底座物理档案均为 weak，过程期望推演置信度降级")
        else:
            weak_team = "主队" if home_strength == "weak" else "客队"
            process_edge_reason.append(f"注意：{weak_team}底座物理档案为 weak，过程期望推演置信度受限降级")

    # 5. 欧亚内部大裂缝判定 (market_internal_divergence)
    has_internal_split = bool(market_move_raw.get("euro_asian_split", False))
    market_internal_div = {
        "status": has_internal_split,
        "note": (
            "欧赔与亚盘同向移动，未发现欧亚内部水位异常裂痕。"
            if not has_internal_split
            else "检测到欧亚板块内部走向分裂，存在操盘手欧亚反向引导痕迹！"
        )
    }

    # 6. 市场与物理过程底座大裂缝判定 (market_process_divergence)
    market_process_div = {
        "status": False,
        "severity": "none",
        "note": "市场让球盘口与主客两队常态物理画像优势基本吻合，处于合理波动区间。"
    }

    if market_status == "MISSING":
        market_process_div = {
            "status": False,
            "severity": "none",
            "note": "MARKET_JSON_MISSING: 博弈大裂缝研判强挂起。"
        }
    else:
        if is_home_favorite:
            if home_avg_xg < 1.1 or home_defensive_leakage > 1.8:
                tags.append("MARKET_OVERPRICES_HOME")
            if away_defensive_leakage <= 0.8:
                tags.append("FAVORITE_DEEP_HANDICAP_CAUTION")
            if process_edge_side == "Away":
                tags.append("MARKET_PROCESS_CONFLICT")
                market_process_div = {
                    "status": True,
                    "severity": "high",
                    "note": (
                        f"【强警告】市场盘口深让极力看好主队（主让 `{handicap}`），"
                        f"但主队底座常态极差且客队过程物理优势明显（优势方为 {process_edge_side}），"
                        "市场与物理过程发生严重大分裂！"
                    )
                }
        elif is_away_favorite:
            if away_avg_xg < 1.1 or away_defensive_leakage > 1.8:
                tags.append("MARKET_OVERPRICES_AWAY")
            if home_defensive_leakage <= 0.8:
                tags.append("FAVORITE_DEEP_HANDICAP_CAUTION")
            if process_edge_side == "Home":
                tags.append("MARKET_PROCESS_CONFLICT")
                market_process_div = {
                    "status": True,
                    "severity": "high",
                    "note": (
                        f"【强警告】市场盘口深让极力看好客队（客让 `{handicap}`），"
                        f"但客队底座常态极差且主队过程物理优势明显（优势方为 {process_edge_side}），"
                        "市场与物理过程发生严重大分裂！"
                    )
                }

    return (
        sorted(list(set(tags))),
        process_edge_side,
        process_edge_confidence,
        "; ".join(process_edge_reason),
        market_internal_div,
        market_process_div
    )


def _extract_handicap_euro(odds: Dict[str, Any]) -> Tuple[float, float, float]:
    """从 odds 或 odds_avg 结构中提取 handicap / euro_h / euro_a 浮点数。"""
    handicap_val = 0.0
    euro_h_val = 0.0
    euro_a_val = 0.0
    if not odds:
        return handicap_val, euro_h_val, euro_a_val

    # 检测新格式
    euro_raw = odds.get("euro", {})
    is_new_format = isinstance(euro_raw, dict) and "current" in euro_raw

    try:
        if is_new_format:
            asian_curr = odds.get("asian", {}).get("current", {})
            handicap_val = float(asian_curr.get("handicap", 0.0))
            euro_curr = euro_raw.get("current", {})
            euro_h_val = float(euro_curr.get("home", 0.0))
            euro_a_val = float(euro_curr.get("away", 0.0))
        else:
            handicap_val = float(odds.get("asian", {}).get("handicap", 0.0))
            euro_h_val = float(euro_raw.get("h", 0.0))
            euro_a_val = float(euro_raw.get("a", 0.0))
    except (TypeError, ValueError):
        pass

    return handicap_val, euro_h_val, euro_a_val


# ────────────────────────────────────────────────────────────────
# 4. 一键打包核心流程
# ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Ares Prematch 资料包一键打包生成引擎 V2.2 (BATCH_READY)")
    parser.add_argument("--date", required=True, help="比赛日日期 (格式: YYYYMMDD)")
    args = parser.parse_args()

    project_root = get_project_root()
    vault_root = get_vault_path()

    # 比赛日根路径
    matchday_dir = vault_root / "03_Match_Audits" / f"AresMatchday_{args.date}"
    if not matchday_dir.exists():
        logger.info(f"正在自动创建比赛日目录: {matchday_dir}")
        matchday_dir.mkdir(parents=True, exist_ok=True)

    alias_map = load_alias_map(project_root)

    # 1. 读取 market.json (带强状态检测)
    market_path = matchday_dir / "market.json"
    market_data_present = False
    market_matches = []

    if market_path.exists():
        try:
            market_raw = json.loads(market_path.read_text(encoding="utf-8"))
            market_matches = market_raw if isinstance(market_raw, list) else market_raw.get("matches", [])
            market_data_present = True
            logger.info(f"成功加载大盘 market.json，包含 {len(market_matches)} 场赔率记录。")
        except Exception as e:
            logger.error(f"解析大盘 market.json 失败: {e}")
    else:
        logger.warning(f"大盘 market.json 缺失: {market_path}。将启动 Demo 演练数据。")

    # 2. 读取 abnormal.json (带强状态检测)
    abnormal_path = matchday_dir / "abnormal.json"
    abnormal_data_present = False
    abnormal_matches_dict = {}

    if abnormal_path.exists():
        try:
            abnormal_raw = json.loads(abnormal_path.read_text(encoding="utf-8"))
            abnormal_matches_dict = abnormal_raw.get("matches", abnormal_raw)
            abnormal_data_present = True
            logger.info("成功加载大盘 abnormal.json。")
        except Exception as e:
            logger.error(f"解析大盘 abnormal.json 失败: {e}")

    # 3. Demo 回退注入与单边缺失优雅降级机制
    # - 若 market.json 缺失，注入占位列表但保持 market_data_present=False，以触发 MISSING 门禁。
    # - 若 abnormal.json 缺失，保持 abnormal_data_present=False。
    if not market_data_present and abnormal_data_present:
        logger.warning("触发特定降级测试：大盘 market.json 缺失，但 abnormal.json 存在。")
        market_matches = [{
            "match_no": "01",
            "match_id": "01",
            "home": "Tottenham Hotspur",
            "away": "Everton",
            "home_team": "热刺",
            "away_team": "埃弗顿",
            "kickoff": "2026-05-23 15:00:00",
            "data_source": "Understat EPL 2025/2026"
        }]
    elif market_data_present and not abnormal_data_present:
        logger.warning("触发特定降级测试：大盘 market.json 存在，但 abnormal.json 缺失。")
    elif not market_data_present and not abnormal_data_present:
        logger.info("未检测到任何大盘物理文件，自动注入热刺 vs 埃弗顿 Demo 演练数据 (Ares v2.2 Schema)...")
        market_matches = [{
            "match_no": "01",
            "match_id": "01",
            "home": "Tottenham Hotspur",
            "away": "Everton",
            "home_team": "热刺",
            "away_team": "埃弗顿",
            "league": "EPL",
            "kickoff": "2026-05-23 15:00:00",
            "data_source": "Understat EPL 2025/2026",
            "sanity_check": {
                "euro_order": "home/draw/away",
                "asian_format": "home_water / handicap_from_home_view / away_water",
                "handicap_sign_rule": {
                    "negative": "home_gives_ball",
                    "positive": "home_receives_ball"
                }
            },
            "odds_avg": {
                "euro": {
                    "initial": {"home": 1.75, "draw": 4.00, "away": 4.60},
                    "current": {"home": 1.55, "draw": 4.20, "away": 5.50}
                },
                "asian": {
                    "initial": {"home_water": 0.92, "handicap": -0.75, "away_water": 0.96},
                    "current": {"home_water": 0.85, "handicap": -1.0, "away_water": 1.05}
                },
                "total": {
                    "initial": {"over_water": 0.92, "line": 2.5, "under_water": 0.96},
                    "current": {"over_water": 0.90, "line": 2.75, "under_water": 0.98}
                }
            },
            "market_move": {
                "euro_signal": "HOME_STRENGTHENED",
                "asian_signal": "HOME_DEEPENED",
                "total_signal": "TOTAL_UP",
                "euro_asian_split": False
            },
            "market_tags": ["HOME_EURO_STRENGTHENED", "FAVORITE_DEEPENED", "EURO_ASIAN_ALIGNED"],
            "risk_tags": ["FAVORITE_DEEP_HANDICAP_CAUTION"]
        }]
        abnormal_matches_dict = {
            "01": {
                "match_no": "01",
                "home": "Tottenham Hotspur",
                "away": "Everton",
                "fact_gate": {
                    "status": "PARTIAL_PASS",
                    "final_confidence": "medium_low",
                    "reason": [
                        "only_suspicious_items_extracted",
                        "latest_availability_not_confirmed"
                    ]
                },
                "teams": [
                    {
                        "team": "Tottenham Hotspur",
                        "side": "home",
                        "needs_latest_confirmation": [
                            {
                                "player": "Dominic Solanke",
                                "unit": "attack",
                                "status": "late_check",
                                "confidence": "low_to_medium"
                            }
                        ],
                        "affected_units": {"attack": "AMBER"}
                    },
                    {
                        "team": "Everton",
                        "side": "away",
                        "needs_latest_confirmation": [
                            {
                                "player": "Jarrad Branthwaite",
                                "unit": "defense",
                                "status": "illness_doubt",
                                "confidence": "low_to_medium"
                            }
                        ],
                        "affected_units": {"defense": "RED_IF_ABSENT"}
                    }
                ]
            }
        }
        market_data_present = True
        abnormal_data_present = True

    # 读取重构后的审计卡模板
    template_path = Path(__file__).parent.parent / "templates" / "audit_input_template.md"
    if not template_path.exists():
        logger.error(f"缺失推演卡模板 audit_input_template.md: {template_path}")
        sys.exit(1)
    audit_template = template_path.read_text(encoding="utf-8")

    csv_rows = []

    # 4. 遍历比赛打包 (BATCH_READY)
    for idx, m in enumerate(market_matches):
        m_id = m.get("match_id", m.get("match_no", f"{idx+1:02d}"))
        try:
            m_id_int = int(m_id)
            m_id_str = f"{m_id_int:02d}"
        except Exception:
            m_id_str = str(m_id)

        # 兼容 home/home_team 两种字段命名
        home = m.get("home_team", m.get("home", "HomeTeam"))
        away = m.get("away_team", m.get("away", "AwayTeam"))
        kickoff = m.get("kickoff", f"{args.date[:4]}-{args.date[4:6]}-{args.date[6:8]} 00:00:00")
        ds = m.get("data_source", "Understat")

        home_en = resolve_team_name(home, alias_map)
        away_en = resolve_team_name(away, alias_map)

        home_dir_part = re.sub(r"\s+", "_", home_en)
        away_dir_part = re.sub(r"\s+", "_", away_en)
        match_folder_name = f"{m_id_str}_{home_dir_part}_{away_dir_part}"

        match_output_dir = matchday_dir / match_folder_name
        match_output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"===> 开始处理第 {m_id_str} 场: {home} vs {away}...")
        logger.info(f"     专属比赛目录: {match_output_dir}")

        # 4.1 查找主客底座 (只读拷贝)
        home_archive = fuzzy_find_archive_path(vault_root, home_en)
        away_archive = fuzzy_find_archive_path(vault_root, away_en)

        home_coach, home_formation, home_tactical_style = "未知", "4-3-3", "进攻足球"
        home_avg_xg, home_defensive_leakage, home_conversion_efficiency, home_actual_tactical_entropy = 1.0, 1.0, 0.05, 0.40
        home_strength, home_contaminated = "unknown", False

        away_coach, away_formation, away_tactical_style = "未知", "4-3-3", "防守反击"
        away_avg_xg, away_defensive_leakage, away_conversion_efficiency, away_actual_tactical_entropy = 1.0, 1.0, 0.05, 0.40
        away_strength, away_contaminated = "unknown", False

        if home_archive:
            shutil.copy2(home_archive, match_output_dir / f"{m_id_str}_home.md")
            logger.info(f"     [只读拷贝] 主队底座 {home_archive.name} -> {m_id_str}_home.md")
            fm, _ = split_frontmatter(home_archive.read_text(encoding="utf-8"))
            home_coach = fm.get("coach", "未知")
            home_formation = fm.get("base_formation", "4-3-3")
            home_tactical_style = fm.get("tactical_style", "进攻足球")
            home_strength = fm.get("archive_strength", "unknown")
            home_contaminated = (
                "CONTAMINATED" in str(fm.get("contamination_status", ""))
                or "contamination_note" in fm
            )
            pr = fm.get("physical_reality", {})
            if isinstance(pr, dict):
                home_avg_xg = float(pr.get("avg_xG_last_5", 1.0))
                home_defensive_leakage = float(pr.get("defensive_leakage", 1.0))
                home_conversion_efficiency = float(pr.get("conversion_efficiency", 0.05))
                home_actual_tactical_entropy = float(pr.get("actual_tactical_entropy", 0.40))
        else:
            logger.warning(f"     [!] 缺失主队底座 {home_en}")
            (match_output_dir / f"{m_id_str}_home.md").write_text(
                f"# {home}\n\n底座档案缺失，请运行 team_forge 初始化。", encoding="utf-8"
            )

        if away_archive:
            shutil.copy2(away_archive, match_output_dir / f"{m_id_str}_away.md")
            logger.info(f"     [只读拷贝] 客队底座 {away_archive.name} -> {m_id_str}_away.md")
            fm, _ = split_frontmatter(away_archive.read_text(encoding="utf-8"))
            away_coach = fm.get("coach", "未知")
            away_formation = fm.get("base_formation", "4-3-3")
            away_tactical_style = fm.get("tactical_style", "防守反击")
            away_strength = fm.get("archive_strength", "unknown")
            away_contaminated = (
                "CONTAMINATED" in str(fm.get("contamination_status", ""))
                or "contamination_note" in fm
            )
            pr = fm.get("physical_reality", {})
            if isinstance(pr, dict):
                away_avg_xg = float(pr.get("avg_xG_last_5", 1.0))
                away_defensive_leakage = float(pr.get("defensive_leakage", 1.0))
                away_conversion_efficiency = float(pr.get("conversion_efficiency", 0.05))
                away_actual_tactical_entropy = float(pr.get("actual_tactical_entropy", 0.40))
        else:
            logger.warning(f"     [!] 缺失客队底座 {away_en}")
            (match_output_dir / f"{m_id_str}_away.md").write_text(
                f"# {away}\n\n底座档案缺失，请运行 team_forge 初始化。", encoding="utf-8"
            )

        # 4.2 赔率门禁与当场提炼 (market.json -> xx_market.json / xx_market.md)
        # 兼容 odds 和 odds_avg 两种字段
        odds = m.get("odds_avg", m.get("odds", {}))
        market_move = m.get("market_move", {})

        m_status = "READY" if market_data_present else "MISSING"

        if m_status == "READY":
            # 写入 xx_market.json
            market_single_path = match_output_dir / f"{m_id_str}_market.json"
            market_single_path.write_text(json.dumps(m, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info(f"     [分发物料] 派生专属赔率卡 -> {m_id_str}_market.json")

            # 写入 xx_market.md
            md_details = format_market_data(odds, market_move, "READY")
            market_md_content = (
                f"# 赔率市场速览卡 (Market Reality Card)\n\n"
                f"- **本场对阵**: {home} vs {away}\n"
                f"- **物理映射**: {home_en} vs {away_en}\n"
                f"- **数据状态**: READY\n\n"
                f"## 欧赔与亚盘让手细目\n"
                f"{md_details}\n"
            )
            (match_output_dir / f"{m_id_str}_market.md").write_text(market_md_content, encoding="utf-8")
            logger.info(f"     [生成说明卡] -> {m_id_str}_market.md")
        else:
            odds = {}
            market_move = {}
            market_md_content = (
                f"# 赔率市场速览卡 (Market Reality Card)\n\n"
                f"- **本场对阵**: {home} vs {away}\n"
                f"- **数据状态**: MISSING (HALT_FOR_MARKET)\n\n"
                f"> [!CAUTION]\n"
                f"> **博弈分析强挂起：未发现真实 market.json 数据输入！禁止用虚构数字研判！**\n"
            )
            (match_output_dir / f"{m_id_str}_market.md").write_text(market_md_content, encoding="utf-8")
            logger.warning(f"     [!] 缺失当场赔率大表数据，锁定 market_md 说明卡。")

        # 4.3 伤停事实门禁与当场提炼 (abnormal.json -> xx_abnormal.json / xx_abnormal.md)
        ab_match = abnormal_matches_dict.get(m_id, abnormal_matches_dict.get(m_id_str, {}))
        ab_status = "READY" if (abnormal_data_present and ab_match) else "MISSING"

        if ab_status == "READY":
            # 写入 xx_abnormal.json
            abnormal_single_path = match_output_dir / f"{m_id_str}_abnormal.json"
            abnormal_single_path.write_text(json.dumps(ab_match, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info(f"     [分发物料] 派生专属伤停卡 -> {m_id_str}_abnormal.json")

            # 提取门禁数据（V2.2 五元组）
            ab_suspicious, ab_rotation, fg_status, fg_confidence, fg_reasons = extract_abnormal_blocks(ab_match, "READY")

            # 写入 xx_abnormal.md
            abnormal_md_content = (
                f"# 事实门禁与伤停异动卡 (Team Abnormalities Card)\n\n"
                f"- **本场对阵**: {home} vs {away}\n"
                f"- **事实门禁状态**: `{fg_status}` (置信度: `{fg_confidence}`)\n\n"
                f"## 🚨 存疑伤停异动\n"
                f"{ab_suspicious}\n\n"
                f"## 💥 受灾单元与阵容残缺评级\n"
                f"{ab_rotation}\n"
            )
            (match_output_dir / f"{m_id_str}_abnormal.md").write_text(abnormal_md_content, encoding="utf-8")
            logger.info(f"     [生成说明卡] -> {m_id_str}_abnormal.md")
        else:
            ab_match = {}
            ab_suspicious = (
                "> [!WARNING]\n"
                "> **NEEDS_VERIFICATION** (缺乏临场伤停 abnormal.json 输入源，置信度降级)"
            )
            ab_rotation = "* 门禁检测：因数据缺失未发现事实，受灾分析暂未激活。"
            fg_status = "NEEDS_VERIFICATION"
            fg_confidence = "medium_low"
            fg_reasons = ["abnormal_json_missing", "source_evidence_missing"]

            abnormal_md_content = (
                f"# 事实门禁与伤停异动卡 (Team Abnormalities Card)\n\n"
                f"- **本场对阵**: {home} vs {away}\n"
                f"- **事实置信度**: NEEDS_VERIFICATION\n\n"
                f"> [!WARNING]\n"
                f"> **降级防卫已激活：abnormal.json 缺失，临场变动事实未确认！**\n"
            )
            (match_output_dir / f"{m_id_str}_abnormal.md").write_text(abnormal_md_content, encoding="utf-8")
            logger.warning(f"     [!] 缺失当场伤停数据，锁定 abnormal_md 说明卡。")

        # 4.4 提取 Handicap/Euro 浮点数（兼容新旧格式）
        handicap_val, euro_h_val, euro_a_val = _extract_handicap_euro(odds) if m_status == "READY" else (0.0, 0.0, 0.0)

        # 4.5 智能计算博弈风险标签与双轨大裂缝优势判定 (V2.2)
        risk_tags, p_side, p_conf, p_reason, m_internal_div, m_process_div = calculate_auto_risk_tags(
            home_avg_xg, home_defensive_leakage, home_strength, home_contaminated,
            away_avg_xg, away_defensive_leakage, away_strength, away_contaminated,
            handicap_val, euro_h_val, euro_a_val,
            m_status, ab_status,
            market_move
        )

        logger.info(f"     [博弈冲突标签检测] -> {risk_tags}")
        logger.info(f"     [底座优势判定方] -> {p_side} (置信度: {p_conf}) | 原因: {p_reason}")

        # 4.6 拼装并输出重塑后的审计输入前瞻卡
        market_data_block = format_market_data(odds, market_move, m_status)
        auto_risk_tags_formatted = " | ".join([f"`{t}`" for t in risk_tags]) if risk_tags else "`Aligned` (无博弈物理偏离)"
        fact_gate_reason_yaml = json.dumps(fg_reasons, ensure_ascii=False)
        fact_gate_reason_formatted = "\n".join([f"  - {r}" for r in fg_reasons])

        # 双轨大裂缝格式化
        if m_status == "MISSING":
            market_internal_divergence_formatted = "`MARKET_JSON_MISSING` (欧亚大裂缝研判强挂起)"
            market_process_divergence_formatted = "`MARKET_JSON_MISSING` (博弈大裂缝研判强挂起)"
        else:
            int_split = m_internal_div.get("status", False)
            market_internal_divergence_formatted = (
                f"`SPLIT_DETECTED` {m_internal_div.get('note', '')}"
                if int_split
                else f"`ALIGNED` {m_internal_div.get('note', '')}"
            )
            proc_split = m_process_div.get("status", False)
            proc_sev = m_process_div.get("severity", "none")
            market_process_divergence_formatted = (
                f"`CONFLICT_SEVERITY:{proc_sev.upper()}` {m_process_div.get('note', '')}"
                if proc_split
                else f"`ALIGNED` {m_process_div.get('note', '')}"
            )

        filled_audit = audit_template.format(
            match_no=m_id_str,
            home_team=home,
            away_team=away,
            league=m.get("league", "EPL"),
            kickoff=kickoff,
            market_json_status=m_status,
            abnormal_json_status=ab_status,
            home_card_status=f"READY_{home_strength.upper()}",
            away_card_status=f"READY_{away_strength.upper()}",
            home_resolved=home_en,
            away_resolved=away_en,
            process_edge_side=p_side,
            process_edge_confidence=p_conf,
            process_edge_reason=json.dumps(p_reason, ensure_ascii=False),
            process_edge_reason_formatted=p_reason,
            market_internal_divergence_status=json.dumps(m_internal_div.get("status", False)),
            market_internal_divergence_note=m_internal_div.get("note", ""),
            market_internal_divergence_formatted=market_internal_divergence_formatted,
            market_process_divergence_status=json.dumps(m_process_div.get("status", False)),
            market_process_divergence_severity=m_process_div.get("severity", "none"),
            market_process_divergence_note=m_process_div.get("note", ""),
            market_process_divergence_formatted=market_process_divergence_formatted,
            auto_risk_tags=json.dumps(risk_tags),
            auto_risk_tags_formatted=auto_risk_tags_formatted,
            market_data_block=market_data_block,
            fact_gate_status=fg_status,
            fact_gate_confidence=fg_confidence,
            fact_gate_reason_yaml=fact_gate_reason_yaml,
            fact_gate_reason_formatted=fact_gate_reason_formatted,
            abnormal_suspicious_block=ab_suspicious,
            abnormal_rotation_block=ab_rotation,
            home_coach=home_coach,
            home_formation=home_formation,
            home_tactical_style=home_tactical_style,
            home_avg_xg=f"{home_avg_xg:.4f}",
            home_defensive_leakage=f"{home_defensive_leakage:.4f}",
            home_conversion_efficiency=f"{home_conversion_efficiency:.4f}",
            home_actual_tactical_entropy=f"{home_actual_tactical_entropy:.4f}",
            away_coach=away_coach,
            away_formation=away_formation,
            away_tactical_style=away_tactical_style,
            away_avg_xg=f"{away_avg_xg:.4f}",
            away_defensive_leakage=f"{away_defensive_leakage:.4f}",
            away_conversion_efficiency=f"{away_conversion_efficiency:.4f}",
            away_actual_tactical_entropy=f"{away_actual_tactical_entropy:.4f}",
            data_source=ds
        )

        audit_file_name = f"{m_id_str}_audit_input.md"
        audit_path = match_output_dir / audit_file_name
        audit_path.write_text(filled_audit, encoding="utf-8")
        logger.info(f"     [生成推演前瞻输入卡] -> {audit_file_name}")

        # 4.7 整理拉链表一行记录 (对齐 SOP 标准字段)
        status_code = "READY"
        if m_status == "MISSING" and ab_status == "MISSING":
            status_code = "MISSING_MARKET_ABNORMAL"
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
            "home": home_en,
            "away": away_en,
            "audit_file": f"{match_folder_name}/{m_id_str}_audit_input.md",
            "home_card": f"{match_folder_name}/{m_id_str}_home.md",
            "away_card": f"{match_folder_name}/{m_id_str}_away.md",
            "market_json": f"{match_folder_name}/{m_id_str}_market.json",
            "abnormal_json": f"{match_folder_name}/{m_id_str}_abnormal.json",
            "fact_gate_status": fg_status,
            "process_edge": p_side,
            "risk_tags": "|".join(risk_tags),
            "status": status_code
        })

    # 5. 输出 00_match_list.csv (SOP 字段对齐)
    csv_file_path = matchday_dir / "00_match_list.csv"
    csv_headers = [
        "match_no", "match_id", "league", "kickoff", "home", "away",
        "audit_file", "home_card", "away_card", "market_json", "abnormal_json",
        "fact_gate_status", "process_edge", "risk_tags", "status"
    ]

    with open(csv_file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_headers)
        writer.writeheader()
        writer.writerows(csv_rows)

    logger.info("==================================================================")
    logger.info("Ares Prematch 一键打包引擎升级重构版 V2.2 运行成功 (BATCH_READY)！")
    logger.info(f"生成的拉链大表索引: {csv_file_path}")
    logger.info("==================================================================")

if __name__ == "__main__":
    main()
