#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
generate_physical_profile.py
=============================
物理画像提炼与底座无损 Soft Update 脚本。
1. 消费 Understat 隐藏 XHR 接口，无时序断层拉取最近 5 场已踢完比赛的历史 raw 指标。
2. 引入大模型进行“智能噪点修剪 (Noise Pruning)”，平滑红牌罚下、主力大轮换等带来的数据污染。
3. 自动提供物理算术平均值作为高可用 fallback。
4. 无损回填 Soft Update 至 02_Team_Archives/{league}/{team}.md 球队档案。
"""

import os
import sys
import re
import json
import yaml
import argparse
import logging
import requests
import time
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

try:
    from dotenv import load_dotenv
    # 自动递归向上寻找并加载 .env 文件
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
logger = logging.getLogger("football-physical-profile")

# 五大联赛映射
LEAGUE_MAP = {
    "EPL": "ENG_England",
    "La_liga": "ESP_Spain",
    "Bundesliga": "GER_Germany",
    "Serie_A": "ITA_Italy",
    "Ligue_1": "FRA_France",
    "英超": "ENG_England",
    "西甲": "ESP_Spain",
    "德甲": "GER_Germany",
    "意甲": "ITA_Italy",
    "法甲": "FRA_France"
}

UNDERSTAT_LEAGUE_MAP = {
    "ENG_England": "EPL",
    "ESP_Spain": "La_liga",
    "GER_Germany": "Bundesliga",
    "ITA_Italy": "Serie_A",
    "FRA_France": "Ligue_1"
}

def get_project_root() -> Path:
    return Path(__file__).parent.parent.parent.parent.parent

def get_vault_path() -> Path:
    # 优先从环境变量获取，其次回退到用户的主目录路径
    vault_env = os.getenv("ARES_VAULT_PATH")
    if vault_env:
        return Path(vault_env)
    return Path("/Users/liumingwei/vaults/AresVault")

# ────────────────────────────────────────────────────────────────
# 2. 球队名称别名强解析
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

def fuzzy_find_archive_path(vault_root: Path, resolved_en_name: str) -> Tuple[Optional[Path], Optional[str]]:
    """
    模糊匹配底座中的球队档案 markdown 路径，并返回联赛代号。
    """
    archives_dir = vault_root / "02_Team_Archives" / "1_Top_Five_Europe"
    if not archives_dir.exists():
        logger.error(f"底座档案目录不存在: {archives_dir}")
        return None, None

    # 标准化搜索目标
    target = re.sub(r"[^a-zA-Z0-9]", "", resolved_en_name).lower()
    
    best_match = None
    best_league_dir = None
    
    for league_dir in archives_dir.iterdir():
        if not league_dir.is_dir():
            continue
        for md_file in league_dir.glob("*.md"):
            if md_file.name.startswith("_"):
                continue
            # 标准化文件名
            stem_norm = re.sub(r"[^a-zA-Z0-9]", "", md_file.stem).lower()
            # 完全匹配或包含关系
            if stem_norm == target or target in stem_norm or stem_norm in target:
                return md_file, league_dir.name

    return None, None

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

# ────────────────────────────────────────────────────────────────
# 3. Understat XHR 数据拉取
# ────────────────────────────────────────────────────────────────

def fetch_understat_matches(league_code: str, year: str) -> List[Dict[str, Any]]:
    """
    通过 XHR 隐藏接口拉取联赛指定年份的所有比赛。
    """
    url = f"https://understat.com/getLeagueData/{league_code}/{year}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }
    
    logger.info(f"正在从 Understat 获取联赛 {league_code} {year} 数据: {url} ...")
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if "dates" in data:
                return data["dates"]
        else:
            logger.error(f"HTTP 请求异常，状态码: {resp.status_code}")
    except Exception as e:
        logger.error(f"获取 Understat 数据失败: {e}")
    return []

def extract_recent_played_matches(
    matches: List[Dict[str, Any]], 
    team_en_title: str, 
    limit: int = 5
) -> List[Dict[str, Any]]:
    """
    提取某支球队最近踢完的比赛列表，并按时间倒序排列。
    """
    played = []
    
    for m in matches:
        if not m.get("isResult"):
            continue
        h_title = m.get("h", {}).get("title", "")
        a_title = m.get("a", {}).get("title", "")
        
        if is_team_match(h_title, team_en_title) or is_team_match(a_title, team_en_title):
            played.append(m)
            
    # 按时间降序排列
    played.sort(key=lambda x: x.get("datetime", ""), reverse=True)
    return played[:limit]

# ────────────────────────────────────────────────────────────────
# 4. 大模型智能修剪与算术 fallback
# ────────────────────────────────────────────────────────────────

def compute_arithmetic_fallback(
    recent_matches: List[Dict[str, Any]], 
    team_en_title: str
) -> Dict[str, Any]:
    """
    物理算术均值 fallback 算法。当 LLM 无法连接时启动，保障系统高可用。
    """
    if not recent_matches:
        return {
            "avg_xG_last_5": 1.0,
            "conversion_efficiency": 0.05,
            "defensive_leakage": 1.0,
            "actual_tactical_entropy": 0.40,
            "xg_history_last_5": []
        }
        
    xG_history = []
    xGA_history = []
    goals_for = 0
    
    for m in recent_matches:
        h_title = m.get("h", {}).get("title", "")
        is_home = is_team_match(h_title, team_en_title)
        
        # 提取进球与期望进球
        m_goals = m.get("goals", {})
        m_xg = m.get("xG", {})
        
        try:
            g_h = int(m_goals.get("h", 0))
            g_a = int(m_goals.get("a", 0))
            xg_h = float(m_xg.get("h", 1.0))
            xg_a = float(m_xg.get("a", 1.0))
        except Exception:
            g_h, g_a, xg_h, xg_a = 0, 0, 1.0, 1.0
            
        if is_home:
            xG_history.append(xg_h)
            xGA_history.append(xg_a)
            goals_for += g_h
        else:
            xG_history.append(xg_a)
            xGA_history.append(xg_h)
            goals_for += g_a
            
    avg_xG = round(sum(xG_history) / len(xG_history), 4)
    avg_xGA = round(sum(xGA_history) / len(xGA_history), 4)
    
    # 转换率：总进球 / 总 xG（安全防除以 0）
    sum_xg = max(sum(xG_history), 1e-6)
    conversion = round(goals_for / sum_xg, 4)
    
    return {
        "avg_xG_last_5": avg_xG,
        "conversion_efficiency": conversion,
        "defensive_leakage": avg_xGA,
        "actual_tactical_entropy": 0.40,
        "xg_history_last_5": [round(v, 4) for v in xG_history]
    }

def call_llm_noise_pruning(
    recent_matches: List[Dict[str, Any]], 
    team_en_title: str,
    fallback_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    调用外部大模型进行智能噪点修剪 (Noise Pruning)。
    """
    provider = str(os.getenv("ARES_LLM_PROVIDER", "openai")).strip().lower()
    api_key = str(os.getenv("ARES_LLM_API_KEY", "")).strip() or \
              str(os.getenv("OPENAI_API_KEY", "")).strip() or \
              str(os.getenv("GEMINI_API_KEY", "")).strip() or \
              str(os.getenv("DEEPSEEK_API_KEY", "")).strip()
              
    if not api_key:
        logger.info("未检测到 LLM API Key，自动回退物理算术均值算法。")
        return fallback_data
        
    base_url = str(os.getenv("ARES_LLM_BASE_URL", "")).strip() or "https://api.openai.com/v1"
    model = str(os.getenv("ARES_LLM_MODEL", "gpt-4o-mini")).strip()
    timeout = int(os.getenv("ARES_LLM_TIMEOUT_SEC", "20"))
    
    matches_summary = []
    
    for i, m in enumerate(recent_matches):
        h = m.get("h", {}).get("title", "")
        a = m.get("a", {}).get("title", "")
        gh = m.get("goals", {}).get("h", "0")
        ga = m.get("goals", {}).get("a", "0")
        xgh = m.get("xG", {}).get("h", "1.0")
        xga = m.get("xG", {}).get("a", "1.0")
        dt = m.get("datetime", "")
        
        is_home = is_team_match(h, team_en_title)
        self_xg = xgh if is_home else xga
        opp_xg = xga if is_home else xgh
        self_goals = gh if is_home else ga
        opp_goals = ga if is_home else gh
        opponent = a if is_home else h
        venue = "主场" if is_home else "客场"
        
        matches_summary.append(
            f"场次 {i+1}: 时间: {dt} | {venue} vs {opponent} | 比分: {self_goals}-{opp_goals} | 自身 raw xG: {self_xg} | 对方 raw xG: {opp_xg}"
        )
        
    matches_context = "\n".join(matches_summary)
    
    system_prompt = (
        "你是一个足球期望数据智能研判与战术分析专家。\n"
        "你的任务是评估某支球队最近 5 场比赛的 raw 物理期望数据，识别其中的特殊物理噪点并进行合理的修正和代偿拟合。\n\n"
        "物理噪点包括但不限于：\n"
        "1. 早退红牌：如果球队在比赛前段被罚下人导致完全死守，导致进攻 xG 异常暴跌（如低于 0.3），你应该对这场的 xG 进行向上代偿修正，恢复为代表其健康水准的数值（如 1.0 - 1.3）；若多打人疯狂刷了大量 xG，也可以进行适当的平滑调低。\n"
        "2. 提前夺冠/保级上岸后的无意义大轮换或青年队练兵。\n"
        "3. 恶劣暴雨雪天气或伤停流感爆发影响了正常传控。\n"
        "如果不存在任何噪点，请直接采用普通的算术平均值进行输出，严禁无故扭曲数据。\n\n"
        "请务必输出一个标准的 JSON 对象，不要包含 markdown 格式标记，包含以下字段：\n"
        "- avg_xG_last_5 (float): 智能修正平滑后最近 5 场的进攻 xG 均值（保留 4 位小数）。\n"
        "- conversion_efficiency (float): 基于修正后 xG 均值和 5 场总进球计算的终结率（进球数 / 修正后 xG 均值，保留 4 位小数）。\n"
        "- defensive_leakage (float): 智能修正平滑后最近 5 场的防守 xGA 均值（即 Expected Goals Against 期望失球均值，保留 4 位小数）。\n"
        "- actual_tactical_entropy (float): 基于平滑度偏离评估出的实际战术熵（常态在 0.3 - 0.6 之间，波动越强烈该值越高）。\n"
        "- noise_identified (list of strings): 识别到并做出修正的噪点场次说明。如果没有修正，写空列表。"
    )
    
    user_payload = {
        "team_name": team_en_title,
        "matches_context": matches_context,
        "arithmetic_baseline": fallback_data
    }
    
    logger.info(f"正在调用 LLM ({provider}) 进行智能噪点分析与修正...")
    endpoint = f"{base_url.rstrip('/')}/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    request_payload = {
        "model": model,
        "temperature": 0.1,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ],
        "response_format": {"type": "json_object"},
    }
    
    try:
        resp = requests.post(endpoint, headers=headers, json=request_payload, timeout=timeout)
        resp.raise_for_status()
        res_data = resp.json()
        content = res_data.get("choices", [{}])[0].get("message", {}).get("content", "")
        parsed = json.loads(content)
        
        # 做基本数值校验防御
        if not isinstance(parsed.get("avg_xG_last_5"), (int, float)):
            raise ValueError("avg_xG_last_5 格式异常")
            
        logger.info(f"LLM 噪点修剪执行成功！")
        logger.info(f"  > 识别到噪点: {parsed.get('noise_identified')}")
        logger.info(f"  > 修正前 xG 均值: {fallback_data['avg_xG_last_5']} | 修正后 xG 均值: {parsed['avg_xG_last_5']}")
        logger.info(f"  > 修正前 xGA 均值: {fallback_data['defensive_leakage']} | 修正后 xGA 均值: {parsed['defensive_leakage']}")
        
        # 补齐 xg_history_last_5 物理字段
        parsed["xg_history_last_5"] = fallback_data["xg_history_last_5"]
        return parsed
        
    except Exception as e:
        logger.warning(f"LLM 噪点修正调用异常: {e}，自动回退为物理算术平均 Baseline")
        return fallback_data

# ────────────────────────────────────────────────────────────────
# 5. Frontmatter Soft Update
# ────────────────────────────────────────────────────────────────

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

def build_markdown(frontmatter: Dict[str, Any], body: str) -> str:
    # 限制以规范的 markdown 格式输出 frontmatter
    yaml_text = yaml.safe_dump(frontmatter, allow_unicode=True, sort_keys=False)
    return f"---\n{yaml_text}---\n\n{body}"

def soft_update_team_archive(
    archive_path: Path, 
    physical_reality: Dict[str, Any]
) -> None:
    """
    Soft Update 底座档案，仅合并更新 physical_reality 块，保留 Body 100% 完整。
    """
    logger.info(f"开始对底座档案进行无损 Soft Update: {archive_path.name}")
    content = archive_path.read_text(encoding="utf-8")
    frontmatter, body = split_frontmatter(content)
    
    # 获取已有的 physical_reality 部分进行合并
    existing_reality = frontmatter.get("physical_reality", {})
    if not isinstance(existing_reality, dict):
        existing_reality = {}
        
    # 合并新算出的物理画像
    merged_reality = {**existing_reality, **physical_reality}
    
    frontmatter["physical_reality"] = merged_reality
    
    # 回写文件
    updated_content = build_markdown(frontmatter, body)
    archive_path.write_text(updated_content, encoding="utf-8")
    logger.info(f"Soft Update 完成！100% 保留了 Body 文本。")

# ────────────────────────────────────────────────────────────────
# 6. 命令行入口与主流程串联
# ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Ares 球队物理画像提炼与底座 Soft Update 工具")
    parser.add_argument("--team-name", required=True, help="要分析的球队名称")
    parser.add_argument("--league", required=False, help="可选，指定联赛")
    parser.add_argument("--year", default="2025", help="赛季年份 (默认 2025，即 2025/26 赛季)")
    args = parser.parse_args()

    project_root = get_project_root()
    vault_root = get_vault_path()

    # 1. 队名映射
    alias_map = load_alias_map(project_root)
    resolved_en = resolve_team_name(args.team_name, alias_map)
    logger.info(f"输入球队名: {args.team_name} | 映射英文名: {resolved_en}")

    # 2. 匹配底座档案路径
    archive_path, archive_league_dir = fuzzy_find_archive_path(vault_root, resolved_en)
    if not archive_path:
        logger.error(f"无法在 02_Team_Archives 中匹配到球队 '{args.team_name}' 或 '{resolved_en}' 的档案！")
        sys.exit(1)

    logger.info(f"匹配到对应底座档案: {archive_path}")
    logger.info(f"所属底座联赛目录: {archive_league_dir}")

    # 3. 确定 understat 联赛编码
    understat_league = args.league or UNDERSTAT_LEAGUE_MAP.get(archive_league_dir)
    if not understat_league:
        logger.error(f"无法为底座联赛目录 {archive_league_dir} 解析对应的 Understat 联赛编码。")
        sys.exit(1)

    logger.info(f"对接 Understat 联赛编码: {understat_league} | 年份: {args.year}")

    # 4. 抓取 Understat 赛程与数据
    matches = fetch_understat_matches(understat_league, args.year)
    if not matches:
        # 尝试使用前一年作为回退（比如 2025 赛季刚开踢没有数据，可以用 2024 年的数据作为参考）
        prev_year = str(int(args.year) - 1)
        logger.warning(f"未能获取到 {args.year} 赛季的比赛数据，尝试获取前一赛季 {prev_year} 数据...")
        matches = fetch_understat_matches(understat_league, prev_year)
        if not matches:
            logger.error("无法获取 Understat 数据，流程熔断！")
            sys.exit(1)

    # 5. 提取最近 5 场已踢完比赛的 raw 物理数据
    recent = extract_recent_played_matches(matches, resolved_en, limit=5)
    if not recent:
        logger.error(f"在 Understat 赛程中未找到任何关于 '{resolved_en}' 已踢完的比赛！")
        sys.exit(1)

    logger.info(f"成功提取 '{resolved_en}' 最近已赛的 {len(recent)} 场比赛 raw 记录。")

    # 6. 计算物理均值 Baseline (Fallback)
    baseline = compute_arithmetic_fallback(recent, resolved_en)

    # 7. 调用大模型进行智能剪枝与代偿修正
    final_reality = call_llm_noise_pruning(recent, resolved_en, baseline)

    # 8. Soft Update 无损回填底座
    soft_update_team_archive(archive_path, final_reality)

if __name__ == "__main__":
    main()
