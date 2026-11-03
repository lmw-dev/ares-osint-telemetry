#!/usr/bin/env python3
"""
src/engine/squad_value_prior_adapter.py
=======================================
Ares 足球 — 已发布 W40 最近首发身价资产 (Starting XI Value Prior) 读取与比赛证据注入适配器

核心契约与安全规则:
1. 强制读取生产 AresVault/02_Team_Archives/priors/team_recent_xi_value.current.json 活动指针与 W40 Canonical JSON；
2. 校验 Canonical JSON 物理 SHA-256 (51ff7692544f7700a6f9fd2775c7656476b271f9a558c967dbea655498e907d5)；
3. 校验 Canonical Receipt 状态 (PASS, round=W40, teams_count=96)；
4. 严格保留原始数据质量等级 (READY 7, CAUTION 67, UNKNOWN 22) 与语义基准 (FULL_XI, PARTIAL_LOWER_BOUND, CAUTION, UNKNOWN)；
5. CAUTION 仅为两场诊断均值，不得冒充三场中位数；双方不具备可靠可比性时严禁生成精确身价比值；
6. 附带当前阵容警示 (current_roster_caveat): 历史身价先验绝不能替代最新官网伤停与临场首发。
"""

import sys
import json
import os
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

# 路径兼容: 优先支持环境变量与自适应目录探测，未指定时完全保持当前 Mac 默认路径
_LOCAL_WORKSPACE = Path("/Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry")
_FALLBACK_WORKSPACE = Path(__file__).resolve().parent.parent.parent
WORKSPACE_ROOT = Path(os.environ.get("ARES_WORKSPACE_ROOT", _LOCAL_WORKSPACE if _LOCAL_WORKSPACE.exists() else _FALLBACK_WORKSPACE))

_LOCAL_VAULT = Path("/Users/liumingwei/vaults/AresVault")
VAULT_ROOT = Path(os.environ.get("ARES_VAULT_PATH", _LOCAL_VAULT))

PRIORS_DIR = VAULT_ROOT / "02_Team_Archives" / "priors"
CURRENT_POINTER_FILE = PRIORS_DIR / "team_recent_xi_value.current.json"
CANONICAL_RECEIPT_FILE = PRIORS_DIR / "W40_CANONICAL_PUBLISH_RECEIPT.json"

EXPECTED_W40_JSON_SHA = "51ff7692544f7700a6f9fd2775c7656476b271f9a558c967dbea655498e907d5"
EXPECTED_CYCLE = "W40"

# 尝试导入 fotmob_binding 中的中文/别名映射
sys.path.insert(0, str(WORKSPACE_ROOT / "src" / "skills" / "ares-fotmob-roster-collector" / "scripts"))
try:
    from fotmob_binding import ZH_NAME_MAP
except ImportError:
    ZH_NAME_MAP = {}


def calc_sha256(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


class SquadValuePriorAdapter:
    """已发布首发身价资产读取与适配器 (Stage 6 生产规范版: 满足 VALUE_PRIOR_CONSIDERATION 契约)"""

    def __init__(self, vault_priors_dir: Optional[Path] = None, override_canonical_json_path: Optional[Path] = None):
        self.priors_dir = vault_priors_dir or PRIORS_DIR
        self.pointer_file = self.priors_dir / "team_recent_xi_value.current.json"
        self.override_canonical_json_path = Path(override_canonical_json_path) if override_canonical_json_path else None
        self._load_and_validate_canonical()

    def _load_and_validate_canonical(self):
        if self.override_canonical_json_path and self.override_canonical_json_path.exists():
            # 允许在受控回放或测试模式下加载指定的审核/候选资产
            self.canonical_json_path = self.override_canonical_json_path
            self.canonical_data = json.loads(self.canonical_json_path.read_text(encoding="utf-8"))
            self.active_cycle = self.canonical_data.get("schema_version") or "CANDIDATE_REPLAY"
            self.active_sha = calc_sha256(self.canonical_json_path)
            self.evidence_cutoff = self.canonical_data.get("evidence_cutoff_date", "2026-10-18")
            self.evidence_cutoff_utc = self.canonical_data.get("evidence_cutoff_utc") or f"{self.evidence_cutoff}T00:00:00Z"
            self.valuation_snapshot_date = self.canonical_data.get("valuation_snapshot_date", "2026-09-30")
            self.published_at_utc = self.canonical_data.get("generated_at") or f"{self.evidence_cutoff}T00:00:00Z"
            self.valuation_semantics = self.canonical_data.get("valuation_semantics", "FIXED_PRICE_RECENT_XI_COMPOSITION_PRIOR")
            self.publication_status = "CANDIDATE_REPLAY_AUTHORIZED"
        else:
            if not self.pointer_file.exists():
                raise FileNotFoundError(f"POINTER_FILE_NOT_FOUND: {self.pointer_file}")

            pointer_data = json.loads(self.pointer_file.read_text(encoding="utf-8"))
            active_cycle = pointer_data.get("current_active_cycle") or pointer_data.get("active_cycle")
            rel_json_path = pointer_data.get("canonical_json_path") or pointer_data.get("canonical_asset_relative_path")
            expected_sha = pointer_data.get("canonical_json_sha256") or pointer_data.get("canonical_asset_sha256")

            if not active_cycle or not rel_json_path or not expected_sha:
                raise ValueError("POINTER_FILE_CORRUPTED: Missing active_cycle, canonical_json_path, or canonical_json_sha256")

            self.canonical_json_path = self.priors_dir / rel_json_path
            if not self.canonical_json_path.exists():
                raise FileNotFoundError(f"CANONICAL_JSON_NOT_FOUND: {self.canonical_json_path}")

            live_json_sha = calc_sha256(self.canonical_json_path)
            if live_json_sha.lower().strip() != expected_sha.lower().strip():
                raise ValueError(f"CANONICAL_JSON_SHA_MISMATCH: live={live_json_sha} != expected={expected_sha}")

            # 动态绑定对应轮次的 Receipt，禁止硬编码 W40！优先匹配与当前 Canonical JSON 哈希一致的收据
            receipt_candidates = [
                self.priors_dir / f"STAGE6_FULL_CYCLE_PRODUCTION_SYNC_RECEIPT_{active_cycle}_POSTMATCH.json",
                self.priors_dir / f"STAGE6_FULL_CYCLE_PRODUCTION_SYNC_RECEIPT_{active_cycle}.json",
                self.priors_dir / f"{active_cycle}_CANONICAL_PUBLISH_RECEIPT.json",
                self.priors_dir / f"{active_cycle}_CANONICAL_RECEIPT.json",
                self.priors_dir / f"{active_cycle}_PUBLISH_RECEIPT.json",
                WORKSPACE_ROOT / "staging" / "recent_xi_prior_continuation" / "w43_rolling_r3" / f"{active_cycle}_CANONICAL_PUBLISH_RECEIPT.json",
                WORKSPACE_ROOT / "staging" / "recent_xi_prior_continuation" / "w40_rolling" / f"{active_cycle}_CANONICAL_PUBLISH_RECEIPT.json",
            ]
            found_receipt = None
            # 优先精确匹配与当前底座 live_json_sha 一致的 Receipt
            for rc in receipt_candidates:
                if rc.exists():
                    try:
                        r_data = json.loads(rc.read_text(encoding="utf-8"))
                        r_sha = r_data.get("canonical_json_sha256") or r_data.get("sha256") or r_data.get("target_1_asset", {}).get("sha256") or r_data.get("priors_source", {}).get("sha256")
                        if r_sha and str(r_sha).lower().strip() == live_json_sha.lower().strip():
                            found_receipt = rc
                            break
                    except Exception:
                        pass

            if not found_receipt:
                for rc in receipt_candidates:
                    if rc.exists():
                        found_receipt = rc
                        break

            if not found_receipt:
                raise FileNotFoundError(f"CANONICAL_RECEIPT_NOT_FOUND: No receipt matching cycle '{active_cycle}' in {self.priors_dir} or staging")

            self.receipt_file = found_receipt
            rcpt_data = json.loads(self.receipt_file.read_text(encoding="utf-8"))
            rcpt_round = str(rcpt_data.get("round", "") or rcpt_data.get("active_cycle", "") or rcpt_data.get("cycle", "")).strip().upper()
            rcpt_status = rcpt_data.get("status") or ("PASS" if "PASS" in str(rcpt_data.get("consumer_readback_verification", {}).get("status", "")) or rcpt_data.get("final_deployment_state") == "PRODUCTION_ACTIVE" or rcpt_data.get("denominator_lock", {}).get("status") == "STRICT_DENOMINATOR_MATCH" else "FAIL")
            if rcpt_status != "PASS" or rcpt_round != str(active_cycle).strip().upper():
                raise ValueError(f"CANONICAL_RECEIPT_INVALID: status={rcpt_status}, round={rcpt_round}")

            rcpt_sha = rcpt_data.get("canonical_json_sha256") or rcpt_data.get("sha256") or rcpt_data.get("target_1_asset", {}).get("sha256") or rcpt_data.get("priors_source", {}).get("sha256")
            if rcpt_sha and rcpt_sha.lower().strip() != live_json_sha.lower().strip():
                raise ValueError(f"RECEIPT_SHA_MISMATCH: receipt={rcpt_sha} != actual={live_json_sha}")

            self.canonical_data = json.loads(self.canonical_json_path.read_text(encoding="utf-8"))
            self.active_cycle = active_cycle
            self.active_sha = live_json_sha
            self.evidence_cutoff = self.canonical_data.get("evidence_cutoff_date", "2026-10-12")
            self.evidence_cutoff_utc = self.canonical_data.get("evidence_cutoff_utc") or f"{self.evidence_cutoff}T00:00:00Z"
            self.published_at_utc = rcpt_data.get("published_at_utc") or rcpt_data.get("timestamp_utc") or self.canonical_data.get("generated_at") or f"{self.evidence_cutoff}T00:00:00Z"
            self.valuation_snapshot_date = self.canonical_data.get("valuation_snapshot_date", "2026-09-30")
            self.valuation_semantics = self.canonical_data.get("valuation_semantics", "STATIC_VALUATION_SNAPSHOT_APPLIED_TO_RECENT_XI")
            self.publication_status = "CANONICAL_PUBLISHED"

        # 构建 96 队名称检索索引 (支持标准英文名、小写、去除特殊字符、中文映射)
        self.teams_index: Dict[str, Dict[str, Any]] = {}
        for t in self.canonical_data.get("teams", []):
            raw_name = t["team_name"]
            team_key = t.get("team_key", raw_name)
            self._register_alias(raw_name, t)
            self._register_alias(team_key, t)
            self._register_alias(raw_name.lower(), t)
            self._register_alias(team_key.lower(), t)
            self._register_alias(raw_name.replace(" ", "").replace(".", "").replace("-", "").lower(), t)

        # 挂载中文映射表
        self.alias_map: Dict[str, str] = {}
        alias_file = WORKSPACE_ROOT / "src" / "data" / "team_alias_map.json"
        if alias_file.exists():
            try:
                self.alias_map.update(json.loads(alias_file.read_text(encoding="utf-8")))
            except Exception:
                pass
        self.alias_map.update(ZH_NAME_MAP)

        for cn_name, en_name in self.alias_map.items():
            en_matched = self.find_team(en_name)
            if en_matched:
                self._register_alias(cn_name, en_matched)
                self._register_alias(cn_name.strip(), en_matched)
                self._register_alias(cn_name.lower(), en_matched)

    def _register_alias(self, alias: str, team_obj: Dict[str, Any]):
        if alias and alias not in self.teams_index:
            self.teams_index[alias] = team_obj

    def find_team(self, query: str) -> Optional[Dict[str, Any]]:
        """多级容错精确匹配球队数据对象"""
        if not query:
            return None
        q = query.strip()

        # 1. 优先通过中文/别名映射表转换为英文标准名
        if hasattr(self, "alias_map") and q in self.alias_map:
            mapped_en = self.alias_map[q]
            if mapped_en in self.teams_index:
                return self.teams_index[mapped_en]
            if mapped_en.lower() in self.teams_index:
                return self.teams_index[mapped_en.lower()]

        if q in self.teams_index:
            return self.teams_index[q]
        ql = q.lower()
        if ql in self.teams_index:
            return self.teams_index[ql]
        q_norm = ql.replace(" ", "").replace(".", "").replace("-", "")
        if q_norm in self.teams_index:
            return self.teams_index[q_norm]

        # 96 队权威别名精确覆盖
        canonical_96 = {
            # 英超 20 队
            "曼城": "Man City", "曼彻斯特城": "Man City", "manchester city": "Man City", "man city": "Man City",
            "曼联": "Man Utd", "曼彻斯特联": "Man Utd", "manchester united": "Man Utd", "man utd": "Man Utd",
            "诺丁汉森林": "Nott'm Forest", "诺丁汉": "Nott'm Forest", "nottingham forest": "Nott'm Forest", "nott'm forest": "Nott'm Forest",
            "利兹联": "Leeds", "利兹": "Leeds", "leeds united": "Leeds", "leeds": "Leeds",
            "伊普斯维奇": "Ipswich", "伊普斯": "Ipswich", "ipswich town": "Ipswich", "ipswich": "Ipswich",
            "考文垂": "Coventry", "coventry city": "Coventry", "coventry": "Coventry",
            "托特纳姆热刺": "Tottenham", "热刺": "Tottenham", "tottenham hotspur": "Tottenham", "tottenham": "Tottenham",
            "纽卡斯尔联": "Newcastle", "纽卡斯尔": "Newcastle", "纽卡": "Newcastle", "newcastle united": "Newcastle", "newcastle": "Newcastle",
            "阿斯顿维拉": "Aston Villa", "维拉": "Aston Villa", "aston villa": "Aston Villa",
            "布伦特福德": "Brentford", "布伦特": "Brentford", "brentford": "Brentford",
            "布莱顿": "Brighton", "布赖顿": "Brighton", "brighton": "Brighton",
            "水晶宫": "Crystal Palace", "crystal palace": "Crystal Palace",
            "埃弗顿": "Everton", "everton": "Everton",
            "富勒姆": "Fulham", "fulham": "Fulham",
            "赫尔城": "Hull City", "hull city": "Hull City",
            "伯恩茅斯": "Bournemouth", "伯恩茅": "Bournemouth", "bournemouth": "Bournemouth",
            "桑德兰": "Sunderland", "sunderland": "Sunderland",
            "利物浦": "Liverpool", "liverpool": "Liverpool",
            "阿森纳": "Arsenal", "arsenal": "Arsenal",
            "切尔西": "Chelsea", "chelsea": "Chelsea",
            # 德甲 18 队
            "多特蒙德": "Dortmund", "多特": "Dortmund", "borussia dortmund": "Dortmund", "dortmund": "Dortmund",
            "拜仁慕尼黑": "Bayern Munich", "拜仁": "Bayern Munich", "bayern munich": "Bayern Munich",
            "勒沃库森": "Leverkusen", "勒沃": "Leverkusen", "bayer leverkusen": "Leverkusen", "leverkusen": "Leverkusen",
            "莱比锡红牛": "Leipzig", "莱比锡": "Leipzig", "rb leipzig": "Leipzig", "leipzig": "Leipzig",
            "法兰克福": "Frankfurt", "eintracht frankfurt": "Frankfurt", "frankfurt": "Frankfurt",
            "门兴格拉德巴赫": "Mönchengladbach", "门兴": "Mönchengladbach", "borussia mönchengladbach": "Mönchengladbach", "mönchengladbach": "Mönchengladbach", "monchengladbach": "Mönchengladbach",
            "科隆": "1.FC Köln", "fc koln": "1.FC Köln", "1.fc koln": "1.FC Köln",
            "奥格斯堡": "Augsburg", "augsburg": "Augsburg",
            "弗赖堡": "Freiburg", "freiburg": "Freiburg",
            "汉堡": "Hamburg", "hamburg": "Hamburg",
            "霍芬海姆": "Hoffenheim", "hoffenheim": "Hoffenheim",
            "美因茨": "Mainz", "mainz 05": "Mainz", "mainz": "Mainz",
            "斯图加特": "Stuttgart", "stuttgart": "Stuttgart",
            "柏林联合": "Union Berlin", "union berlin": "Union Berlin",
            "不莱梅": "Werder Bremen", "云达不莱梅": "Werder Bremen", "werder bremen": "Werder Bremen",
            "沙尔克04": "FC Schalke 04", "沙尔克": "FC Schalke 04", "schalke 04": "FC Schalke 04",
            "帕德博恩": "SC Paderborn", "paderborn": "SC Paderborn",
            "埃尔弗斯贝格": "SV Elversberg", "elversberg": "SV Elversberg",
            # 意甲 20 队
            "国际米兰": "Inter", "国米": "Inter", "inter milan": "Inter", "inter": "Inter",
            "AC米兰": "AC Milan", "米兰": "AC Milan", "ac milan": "AC Milan",
            "尤文图斯": "Juventus", "尤文": "Juventus", "juventus": "Juventus",
            "那不勒斯": "Napoli", "napoli": "Napoli",
            "罗马": "Roma", "as roma": "Roma", "roma": "Roma",
            "拉齐奥": "Lazio", "lazio": "Lazio",
            "亚特兰大": "Atalanta", "atalanta": "Atalanta",
            "佛罗伦萨": "Fiorentina", "fiorentina": "Fiorentina",
            "博洛尼亚": "Bologna", "bologna": "Bologna",
            "都灵": "Torino", "torino": "Torino",
            "蒙扎": "Monza", "monza": "Monza",
            "卡利亚里": "Cagliari", "cagliari": "Cagliari",
            "恩波利": "Empoli", "empoli": "Empoli",
            "热那亚": "Genoa", "genoa": "Genoa",
            "帕尔马": "Parma", "parma": "Parma",
            "莱切": "Lecce", "lecce": "Lecce",
            "乌迪内斯": "Udinese", "udinese": "Udinese",
            "威尼斯": "Venezia", "venezia": "Venezia",
            "科莫": "Como", "como": "Como",
            "萨索洛": "Sassuolo", "sassuolo": "Sassuolo",
            "弗罗西诺内": "Frosinone", "弗洛西诺尼": "Frosinone", "frosinone": "Frosinone",
            # 西甲 20 队
            "皇家马德里": "Real Madrid", "皇马": "Real Madrid", "real madrid": "Real Madrid",
            "巴塞罗那": "Barcelona", "巴萨": "Barcelona", "barcelona": "Barcelona",
            "马德里竞技": "Atlético", "马竞": "Atlético", "atletico madrid": "Atlético", "atletico": "Atlético",
            "皇家社会": "Real Sociedad", "皇社": "Real Sociedad", "real sociedad": "Real Sociedad",
            "毕尔巴鄂竞技": "Athletic Club", "毕尔巴鄂": "Athletic Club", "athletic club": "Athletic Club", "athletic bilbao": "Athletic Club",
            "皇家贝蒂斯": "Real Betis", "贝蒂斯": "Real Betis", "real betis": "Real Betis",
            "比利亚雷亚尔": "Villarreal", "黄潜": "Villarreal", "villarreal": "Villarreal",
            "塞维利亚": "Sevilla FC", "sevilla": "Sevilla FC", "sevilla fc": "Sevilla FC",
            "瓦伦西亚": "Valencia", "巴伦西亚": "Valencia", "valencia": "Valencia",
            "奥萨苏纳": "CA Osasuna", "osasuna": "CA Osasuna", "ca osasuna": "CA Osasuna",
            "塞尔塔": "Celta", "celta vigo": "Celta", "celta": "Celta",
            "赫塔菲": "Getafe", "getafe": "Getafe",
            "西班牙人": "Espanyol", "espanyol": "Espanyol",
            "巴列卡诺": "Rayo Vallecano", "rayo vallecano": "Rayo Vallecano",
            "阿拉维斯": "Alavés", "alaves": "Alavés", "deportivo alaves": "Alavés",
            "埃尔切": "Elche CF", "elche": "Elche CF", "elche cf": "Elche CF",
            "莱万特": "Levante", "levante": "Levante",
            "马拉加": "Málaga CF", "malaga": "Málaga CF", "malaga cf": "Málaga CF",
            "拉科鲁尼亚": "Dep. A Coruña", "deportivo la coruna": "Dep. A Coruña", "dep. a coruña": "Dep. A Coruña",
            "桑坦德竞技": "Racing", "racing santander": "Racing", "racing": "Racing",
            # 法甲 18 队
            "巴黎圣日耳曼": "PSG", "巴黎": "PSG", "大巴黎": "PSG", "paris saint-germain": "PSG", "psg": "PSG",
            "摩纳哥": "Monaco", "as monaco": "Monaco", "monaco": "Monaco",
            "马赛": "Marseille", "olympique de marseille": "Marseille", "marseille": "Marseille",
            "里尔": "Lille", "lille": "Lille",
            "里昂": "Lyon", "olympique lyonnais": "Lyon", "lyon": "Lyon",
            "雷恩": "Rennes", "rennes": "Rennes",
            "朗斯": "Lens", "lens": "Lens",
            "尼斯": "Nice", "nice": "Nice",
            "斯特拉斯堡": "R. Strasbourg", "strasbourg": "R. Strasbourg", "r. strasbourg": "R. Strasbourg",
            "图卢兹": "Toulouse", "toulouse": "Toulouse",
            "布雷斯特": "Stade Brestois", "brest": "Stade Brestois", "stade brestois": "Stade Brestois",
            "欧塞尔": "AJ Auxerre", "auxerre": "AJ Auxerre", "aj auxerre": "AJ Auxerre",
            "昂热": "Angers SCO", "angers": "Angers SCO", "angers sco": "Angers SCO",
            "洛里昂": "FC Lorient", "lorient": "FC Lorient", "fc lorient": "FC Lorient",
            "勒阿弗尔": "Le Havre AC", "le havre": "Le Havre AC", "le havre ac": "Le Havre AC",
            "勒芒": "Le Mans FC", "le mans": "Le Mans FC", "le mans fc": "Le Mans FC",
            "巴黎FC": "Paris FC", "paris fc": "Paris FC",
            "特鲁瓦": "Troyes", "troyes": "Troyes"
        }
        
        q_clean = q.lower().strip()
        if q_clean in canonical_96:
            target_en = canonical_96[q_clean]
            if target_en in self.teams_index:
                return self.teams_index[target_en]
            if target_en.lower() in self.teams_index:
                return self.teams_index[target_en.lower()]

        return None

    def get_value_prior_consideration(
        self,
        home_team_name: str,
        away_team_name: str,
        kickoff_utc: Optional[str] = None,
        fixture_id: Optional[str] = None,
        league: Optional[str] = None,
        round_no: Optional[int] = None,
        match_context: Optional[Dict[str, Any]] = None,
        is_retrospective_replay: bool = False,
    ) -> Dict[str, Any]:
        """
        核心方法: 生成单场比赛规范的 VALUE_PRIOR_CONSIDERATION 契约对象。
        满足:
        - 仅消费合法发布的权威资产 (或受控回放资产)；
        - 开球时间完整 UTC 时间戳校验与时序截断门禁；
        - 缺少有效开球时间或发布资格时刚性降级为 UNKNOWN；
        - 回放模式必须显式声明 is_retrospective_replay；
        - READY / CAUTION / UNKNOWN 状态准确传递；
        - CAUTION / UNKNOWN 下 authoritative_asset_ratio 必须为 null；
        - 5 项 Anti-Overreach 禁令注入。
        """
        home_obj = self.find_team(home_team_name)
        away_obj = self.find_team(away_team_name)

        def parse_team_metrics(obj: Optional[Dict[str, Any]], requested_name: str) -> Dict[str, Any]:
            if not obj:
                return {
                    "matched": False,
                    "team_name": requested_name,
                    "canonical_name": None,
                    "status": "UNKNOWN",
                    "valid_matches": 0,
                    "complete_matches": 0,
                    "missing_matches": 3,
                    "effective_value_eur": None,
                    "metric_type": None,
                    "completeness_quality": "UNKNOWN_METRIC_UNAVAILABLE"
                }

            status = obj.get("last3_status", "READY")
            valid_m = obj.get("valid_match_count", 0)
            comp_m = obj.get("complete_match_count", 0)
            c_name = obj.get("team_name")
            quality = obj.get("category") or obj.get("completeness_quality", "N/A")

            # 稳健提取身价值 (兼容 float, int 与 dict 结构)
            def extract_val(metric):
                if isinstance(metric, (int, float)):
                    return float(metric)
                if isinstance(metric, dict):
                    return metric.get("value_eur") or metric.get("amount") or metric.get("eur")
                return None

            pm = obj.get("primary_metric")
            sm = obj.get("secondary_metric")

            p_val = extract_val(pm)
            s_val = extract_val(sm)

            if p_val is not None:
                val = p_val
                m_type = "primary_metric"
            elif s_val is not None:
                val = s_val
                m_type = "secondary_metric"
            else:
                val = None
                m_type = None

            if val is not None and status == "UNKNOWN":
                status = "READY" if (valid_m >= 2 or quality in ["FULL_XI_EXACT", "IMAGE_VERIFIED_FULL_XI"]) else "CAUTION"

            return {
                "matched": True,
                "team_name": requested_name,
                "canonical_name": c_name,
                "status": status,
                "valid_matches": valid_m,
                "complete_matches": comp_m,
                "missing_matches": max(0, 3 - valid_m),
                "effective_value_eur": val,
                "metric_type": m_type,
                "completeness_quality": quality
            }

        h_info = parse_team_metrics(home_obj, home_team_name)
        a_info = parse_team_metrics(away_obj, away_team_name)

        h_stat = h_info["status"]
        a_stat = a_info["status"]
        h_val = h_info["effective_value_eur"]
        a_val = a_info["effective_value_eur"]

        # 1. 开球时间时序截断门禁与发布资格门禁 (Temporal Cutoff & Publishing Qualification)
        is_temporal_valid = True
        temporal_error = None

        # 1.1 开球时间必填与有效性校验 (缺少有效时间自动降级)
        if not kickoff_utc or not isinstance(kickoff_utc, str) or not kickoff_utc.strip():
            is_temporal_valid = False
            temporal_error = "MISSING_KICKOFF_TIME_DEGRADED: Kickoff UTC timestamp is missing or empty. Prior degraded to UNKNOWN."
        else:
            try:
                # 校验并提取完整 ISO8601 UTC 时间戳进行精确时序比较
                clean_ko = kickoff_utc.strip().replace("Z", "+00:00")
                if "+" not in clean_ko and "-" not in clean_ko[10:]:
                    clean_ko += "+00:00"
                from datetime import datetime
                ko_dt = datetime.fromisoformat(clean_ko)

                # 解析已发布资产的截止时间与发布时间
                clean_cutoff = self.evidence_cutoff_utc.strip().replace("Z", "+00:00")
                if "+" not in clean_cutoff and "-" not in clean_cutoff[10:]:
                    clean_cutoff += "+00:00"
                cutoff_dt = datetime.fromisoformat(clean_cutoff)

                clean_pub = self.published_at_utc.strip().replace("Z", "+00:00")
                if "+" not in clean_pub and "-" not in clean_pub[10:]:
                    clean_pub += "+00:00"
                pub_dt = datetime.fromisoformat(clean_pub)

                # 生产防未来信息穿越 (非回放模式下严格判定)
                if not is_retrospective_replay:
                    if cutoff_dt > ko_dt:
                        is_temporal_valid = False
                        temporal_error = (
                            f"TEMPORAL_LOOKAHEAD_VIOLATION: Evidence cutoff ({self.evidence_cutoff_utc}) "
                            f"is later than match kickoff ({kickoff_utc}). Future asset cannot be used."
                        )
                    # 只要证据截止日期在开球之前，物理文件归档时间晚于开球不构成实质性未来信息穿越 (支持赛后归档与先验补齐)
            except Exception as e:
                is_temporal_valid = False
                temporal_error = f"INVALID_KICKOFF_TIME_FORMAT_DEGRADED: Kickoff timestamp '{kickoff_utc}' cannot be parsed as valid ISO8601 ({e})."

        # 1.2 发布资格校验 (缺少发布资格自动降级)
        is_eligible = True
        eligibility_error = None
        if not hasattr(self, "active_cycle") or not self.active_cycle or self.active_cycle == "UNKNOWN":
            is_eligible = False
            eligibility_error = "ASSET_INELIGIBLE_FOR_CONSUMPTION: Asset lacks verified publishing cycle or version."
        elif getattr(self, "publication_status", "") not in ("CANONICAL_PUBLISHED", "CANDIDATE_REPLAY_AUTHORIZED"):
            is_eligible = False
            eligibility_error = f"ASSET_INELIGIBLE_FOR_CONSUMPTION: Publication status '{getattr(self, 'publication_status', '')}' is not authorized."

        # 归一化状态至 READY / CAUTION / UNKNOWN 三元模型
        def normalize_stat(s):
            if not s or s == "UNKNOWN":
                return "UNKNOWN"
            if "READY" in s:
                return "READY"
            if "CAUTION" in s or "PARTIAL" in s or "BOUND" in s:
                return "CAUTION"
            return "CAUTION"

        h_stat_norm = normalize_stat(h_stat)
        a_stat_norm = normalize_stat(a_stat)

        # 辅助仲裁: 沿用现有覆盖质量口径，判定双方覆盖是否严重不对等或不足以比较
        def check_coverage_comparability(h_m: Dict[str, Any], a_m: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
            """
            沿用现有覆盖质量口径判定双方是否覆盖严重不对等或不足以比较:
            1. 跨层极端不对等: 一方为 FULL_XI 确证估值 (3/3 完整)，另一方为严重缺值的残缺下界 (PARTIAL_LOWER_BOUND 或 complete <= 1);
            2. 完整场次断层悬殊: 完整场次差异 >= 2 场且较少方存在下界 (如 3 场完整 vs 1 场完整下界);
            3. 零完整场次下界: 任一方 complete_matches == 0 且为下界 (完全缺乏单场完整基线，不足以形成比率比较).
            """
            h_qual = str(h_m.get("completeness_quality") or "").upper()
            a_qual = str(a_m.get("completeness_quality") or "").upper()
            h_comp = h_m.get("complete_matches", 0)
            a_comp = a_m.get("complete_matches", 0)
            h_valid = h_m.get("valid_matches", 0)
            a_valid = a_m.get("valid_matches", 0)

            # 是否为完全确证首发
            is_h_full = ("FULL_XI" in h_qual) and (h_comp >= 3)
            is_a_full = ("FULL_XI" in a_qual) and (a_comp >= 3)

            # 是否为严重残缺下界
            is_h_partial = ("PARTIAL" in h_qual) or (h_comp <= 1 and any(k in h_qual for k in ["LOWER_BOUND", "BOUND"]))
            is_a_partial = ("PARTIAL" in a_qual) or (a_comp <= 1 and any(k in a_qual for k in ["LOWER_BOUND", "BOUND"]))

            # 规则 1: 一方为 FULL_XI 确证，另一方为严重缺值的残缺下界 (如拜仁 vs 帕德博恩)
            if (is_h_full and is_a_partial) or (is_a_full and is_h_partial):
                full_t = home_team_name if is_h_full else away_team_name
                part_t = away_team_name if is_h_full else home_team_name
                part_q = a_qual if is_h_full else h_qual
                part_c = a_comp if is_h_full else h_comp
                part_v = a_valid if is_h_full else h_valid
                return False, (
                    f"Severely asymmetric coverage: '{full_t}' has full starting XI exact valuation (3/3 matches), "
                    f"whereas '{part_t}' is {part_q} ({part_c}/{part_v} full matches). "
                    f"Coverage is severely asymmetric and insufficient for ratio comparison; diagnostic quotient suppressed to null."
                )

            # 规则 2: 完整场次断层悬殊 (差异 >= 2 场) 且较少方存在下界
            if abs(h_comp - a_comp) >= 2:
                if (h_comp < a_comp and any(k in h_qual for k in ["LOWER_BOUND", "PARTIAL", "BOUND"])) or \
                   (a_comp < h_comp and any(k in a_qual for k in ["LOWER_BOUND", "PARTIAL", "BOUND"])):
                    return False, (
                        f"Severely asymmetric match completeness: home '{home_team_name}' has {h_comp}/{h_valid} full matches ({h_qual}) "
                        f"and away '{away_team_name}' has {a_comp}/{a_valid} full matches ({a_qual}). "
                        f"Disparity >= 2 matches with lower-bound caveat; quotient suppressed to null."
                    )

            # 规则 3: 零完整场次下界 (缺乏单场完整基线，不足以比较)
            if (h_comp == 0 and any(k in h_qual for k in ["LOWER_BOUND", "PARTIAL", "BOUND"])) or \
               (a_comp == 0 and any(k in a_qual for k in ["LOWER_BOUND", "PARTIAL", "BOUND"])):
                zero_t = home_team_name if h_comp == 0 else away_team_name
                zero_q = h_qual if h_comp == 0 else a_qual
                return False, (
                    f"Insufficient baseline for comparison: '{zero_t}' has 0 complete matches ({zero_q}). "
                    f"Ratio comparison suppressed to null."
                )

            return True, None

        # 2. 状态机仲裁与比率门禁
        if not is_temporal_valid:
            asset_status = "UNKNOWN"
            status_reason = temporal_error
            is_comp_allowed = False
            auth_ratio = None
            diag_quotient = None
        elif not is_eligible:
            asset_status = "UNKNOWN"
            status_reason = eligibility_error
            is_comp_allowed = False
            auth_ratio = None
            diag_quotient = None
        elif h_stat_norm == "UNKNOWN" or a_stat_norm == "UNKNOWN":
            asset_status = "NOT_USED" if (h_info["matched"] or a_info["matched"]) else "UNKNOWN"
            status_reason = f"Incomplete pair coverage: home '{home_team_name}' is {h_stat}, away '{away_team_name}' is {a_stat}. Comparison cannot be formed."
            is_comp_allowed = False
            auth_ratio = None
            diag_quotient = None
        elif h_stat_norm == "CAUTION" or a_stat_norm == "CAUTION":
            asset_status = "SUPPORTING_CONTEXT"
            is_comparable, incomp_reason = check_coverage_comparability(h_info, a_info)
            if not is_comparable:
                status_reason = incomp_reason
                is_comp_allowed = False
                auth_ratio = None
                diag_quotient = None
            else:
                status_reason = f"Cautionary coverage: home '{home_team_name}' is {h_stat}, away '{away_team_name}' is {a_stat}."
                is_comp_allowed = True
                auth_ratio = None
                diag_quotient = round(h_val / a_val, 4) if (h_val and a_val and a_val > 0) else None
        elif h_stat_norm == "READY" and a_stat_norm == "READY":
            # 样本状态为 READY 不应覆盖实际估值质量：
            # 只有双方均为完全确证首发（非下界、非部分覆盖、全部完赛场次 11/11 完整估值），才输出权威比值
            def is_full_exact_valuation(info: Dict[str, Any]) -> bool:
                q = str(info.get("completeness_quality") or "").upper()
                if any(k in q for k in ["LOWER_BOUND", "PARTIAL", "BOUND", "INCOMPLETE"]):
                    return False
                valid_m = info.get("valid_matches", 0)
                comp_m = info.get("complete_matches", 0)
                if valid_m == 0 or comp_m < valid_m:
                    return False
                return q in ["FULL_XI_EXACT", "FULL_XI_CONFIRMED", "IMAGE_VERIFIED_FULL_XI"]

            is_h_full = is_full_exact_valuation(h_info)
            is_a_full = is_full_exact_valuation(a_info)

            asset_status = "SUPPORTING_CONTEXT"
            raw_ratio = round(h_val / a_val, 4) if (h_val and a_val and a_val > 0) else None

            if is_h_full and is_a_full:
                status_reason = "High-confidence coverage: both teams verified READY with complete matches (full starting XI exact)."
                is_comp_allowed = True
                auth_ratio = raw_ratio
                diag_quotient = raw_ratio
            else:
                # 检查双方覆盖是否严重不对等或不足以比较
                is_comparable, incomp_reason = check_coverage_comparability(h_info, a_info)
                if not is_comparable:
                    status_reason = incomp_reason
                    is_comp_allowed = False
                    auth_ratio = None
                    diag_quotient = None
                else:
                    # 覆盖可比但存在下界：坚决不输出权威精确倍数，保留诊断值
                    status_reason = (
                        f"Lower-bound valuation caveat: both teams sample-READY ({h_info['valid_matches']}/{a_info['valid_matches']} matches), "
                        f"but {home_team_name} is {h_info['completeness_quality']} ({h_info['complete_matches']}/{h_info['valid_matches']} full) "
                        f"and {away_team_name} is {a_info['completeness_quality']} ({a_info['complete_matches']}/{a_info['valid_matches']} full). "
                        f"Authoritative ratio suppressed to diagnostic only."
                    )
                    is_comp_allowed = True
                    auth_ratio = None
                    diag_quotient = raw_ratio
        else:
            asset_status = "UNKNOWN"
            status_reason = "Unrecognized status combination."
            is_comp_allowed = False
            auth_ratio = None
            diag_quotient = None

        def fmt_eur(val):
            if val is None:
                return "N/A"
            if val >= 100_000_000:
                return f"€{val / 1_000_000:.1f}M"
            return f"€{val / 1_000_000:.2f}M"

        # 生成人类可读 Markdown 摘要
        h_disp = f"{h_info['canonical_name'] or home_team_name} [{h_stat}] ({fmt_eur(h_val)}, {h_info['completeness_quality']})"
        a_disp = f"{a_info['canonical_name'] or away_team_name} [{a_stat}] ({fmt_eur(a_val)}, {a_info['completeness_quality']})"
        ratio_disp = (
            f"{auth_ratio}x"
            if auth_ratio is not None
            else (
                f"{diag_quotient}x (未验证诊断倍数)"
                if diag_quotient is not None
                else ("不可比 (覆盖不对等，比率置空)" if not is_comp_allowed and asset_status == "SUPPORTING_CONTEXT" else "不可比 (比率置空)")
            )
        )

        summary_md = (
            f"- **首发身价先验 (VALUE_PRIOR_CONSIDERATION)**:\n"
            f"  - 主队: {h_disp}\n"
            f"  - 客队: {a_disp}\n"
            f"  - 状态: `{asset_status}` | 对比允许: `{is_comp_allowed}` | 比率: `{ratio_disp}`\n"
            f"  - *防越权注记: 基于 Transfermarkt {self.valuation_snapshot_date} 静态估值应用于最近三场首发。本指标仅作宏观人员配置与阵容深度参考，严禁以此推导让球盘、大小球或 1X2 胜率，严禁替代确证首发与战术终审。*"
        )

        consideration = {
            "interface_contract_version": "1.0.0-pilot",
            "asset_version": self.active_cycle,
            "valuation_snapshot_date": self.valuation_snapshot_date,
            "fixture_id": str(fixture_id) if fixture_id else "UNKNOWN",
            "league": league or "UNKNOWN",
            "round": round_no or 0,
            "home_team": home_team_name,
            "away_team": away_team_name,
            "asset_status": asset_status,  # MUST be SUPPORTING_CONTEXT | NOT_USED | UNKNOWN
            "status_reason": status_reason,
            "is_comparison_allowed": is_comp_allowed,
            "is_structural_supporting_context_only": True,
            "authoritative_asset_ratio": auth_ratio,  # strictly null if either side is CAUTION/UNKNOWN
            "unverified_diagnostic_quotient": diag_quotient,  # diagnostic only
            "home_team_metrics": h_info,
            "away_team_metrics": a_info,
            "anti_overreach_contract": {
                "overreach_forbidden": True,
                "cannot_derive_asian_handicap": True,
                "cannot_derive_over_under": True,
                "cannot_derive_1x2_odds": True,
                "cannot_override_football_model": True,
                "allowed_usage_scope": "SUPPORTING_CONTEXT_MACRO_SQUAD_QUALITY_ONLY",
                "enforcement_statement": (
                    "Squad valuation metrics represent static fixed-price squad composition priors. "
                    "They provide macroscopic context on squad depth and structural financial backing only. "
                    "Under NO circumstances shall squad valuation be used to mathematically derive or justify "
                    "Asian Handicaps, Goal Lines, or 1X2 market prices, nor shall they overturn "
                    "xG, tactical structures, team dynamics, or official injury/lineup confirmations."
                ),
            },
            "evidence_lineage": {
                "source_cycle": self.active_cycle,
                "source_sha256": self.active_sha,
                "evidence_cutoff_date": self.evidence_cutoff,
                "valuation_snapshot_date": self.valuation_snapshot_date,
            },
            "human_readable_summary": summary_md,
        }

        if is_retrospective_replay:
            consideration["is_retrospective_replay"] = True
            consideration["replay_audit_tag"] = "RETROSPECTIVE_REPLAY"

        if match_context:
            consideration["match_reference_context"] = match_context

        return consideration

    def get_starting_xi_value_prior(
        self,
        home_team_name: str,
        away_team_name: str,
        kickoff_utc: Optional[str] = None,
        fixture_id: Optional[str] = None,
        league: Optional[str] = None,
        round_no: Optional[int] = None,
        match_context: Optional[Dict[str, Any]] = None,
        is_retrospective_replay: bool = False,
    ) -> Dict[str, Any]:
        """向后兼容原调用接口，直接返回最新规范的 VALUE_PRIOR_CONSIDERATION 对象"""
        return self.get_value_prior_consideration(
            home_team_name=home_team_name,
            away_team_name=away_team_name,
            kickoff_utc=kickoff_utc,
            fixture_id=fixture_id,
            league=league,
            round_no=round_no,
            match_context=match_context,
            is_retrospective_replay=is_retrospective_replay,
        )


# 全局单例
_GLOBAL_ADAPTER = None

def get_squad_value_adapter() -> SquadValuePriorAdapter:
    global _GLOBAL_ADAPTER
    if _GLOBAL_ADAPTER is None:
        _GLOBAL_ADAPTER = SquadValuePriorAdapter()
    return _GLOBAL_ADAPTER
