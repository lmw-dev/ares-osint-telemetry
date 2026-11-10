#!/usr/bin/env python3
"""
Ares OSINT Telemetry - Postmatch Valuation Integrator Engine (v2.2 Final Runtime Fix)
====================================================================================
核心职责:
1. 赛后确证首发 11 人 Transfermarkt 多因子身份核验 (五维验证 + 积分式已审注册表 + 运行时双向安全防线)
2. 生成单场不可变 06-lineup-valuation.json，计算已确认正价球员金额下界 (PARTIAL_LOWER_BOUND 容错)
3. 维护参赛球队最近 3 场时序滚动窗口:
   - 先按 kickoff_utc 倒序选择研究截止时间之前实际完赛的最近三场比赛
   - 严格两步法评估: 若某场缺价输出 MEAN_2 / SINGLE_1 / UNKNOWN，绝不拉第 4 场替补
   - 规范化双向去重 (Internal Fixture ID <-> FotMob Match ID，解决 2993829/5749714 等复现问题)
4. 动态读取当前已发布、SHA 验证通过的 team_recent_xi_value.current.json 所引用的权威资产，绝不硬编码 W43
5. 生产隔离与安全写入: 未获得 Stage 6 授权绝不覆盖 AresVault，同内容重跑按 SHA 幂等跳过
"""

import csv
import gzip
import hashlib
import json
import os
import re
import shutil
import sys
import time
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set, Union

WORKSPACE_ROOT = Path("/Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry")
DEFAULT_TM_DATASET = WORKSPACE_ROOT / "scratch/tm_dataset/players.csv.gz"
if not DEFAULT_TM_DATASET.exists():
    _fallback_tm = WORKSPACE_ROOT / "runtime_cache/cold_storage/legacy_root_dirs/scratch/tm_dataset/players.csv.gz"
    if _fallback_tm.exists():
        DEFAULT_TM_DATASET = _fallback_tm
DEFAULT_VAULT_ROOT = Path("/Users/liumingwei/vaults/AresVault")

def sha256_file(filepath: Path) -> str:
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()

def normalize_name(s: str) -> str:
    """去重音与规范化人名用于 O(1) 索引倒排匹配"""
    if not s:
        return ""
    replacements = {
        "ł": "l", "Ł": "L", "ø": "o", "Ø": "O", "æ": "ae", "Æ": "AE",
        "å": "a", "Å": "A", "ä": "a", "Ä": "A", "ö": "o", "Ö": "O",
        "ü": "u", "Ü": "U", "ß": "ss", "ð": "d", "Ð": "D", "þ": "th", "Þ": "TH",
        "ć": "c", "Ć": "C", "č": "c", "Č": "C", "š": "s", "Š": "S", "ž": "z", "Ž": "Z",
        "đ": "d", "Đ": "D", "ń": "n", "Ń": "N", "ę": "e", "Ę": "E", "ą": "a", "Ą": "A"
    }
    for k, v in replacements.items():
        s = s.replace(k, v)
    nfkd = unicodedata.normalize("NFKD", s)
    no_accent = "".join([c for c in nfkd if not unicodedata.combining(c)])
    return re.sub(r"[^a-z0-9]", "", no_accent.lower())


def tokenize_name(s: str) -> List[str]:
    """提取去重音后的人名 token 列表"""
    if not s:
        return []
    replacements = {
        "ł": "l", "Ł": "L", "ø": "o", "Ø": "O", "æ": "ae", "Æ": "AE",
        "å": "a", "Å": "A", "ä": "a", "Ä": "A", "ö": "o", "Ö": "O",
        "ü": "u", "Ü": "U", "ß": "ss", "ð": "d", "Ð": "D", "þ": "th", "Þ": "TH",
        "ć": "c", "Ć": "C", "č": "c", "Č": "C", "š": "s", "Š": "S", "ž": "z", "Ž": "Z",
        "đ": "d", "Đ": "D", "ń": "n", "Ń": "N", "ę": "e", "Ę": "E", "ą": "a", "Ą": "A"
    }
    for k, v in replacements.items():
        s = s.replace(k, v)
    nfkd = unicodedata.normalize("NFKD", s)
    no_accent = "".join([c for c in nfkd if not unicodedata.combining(c)])
    return re.findall(r"[a-z0-9]+", no_accent.lower())


def is_name_compatible(name_a: str, name_b: str) -> bool:
    """
    检查两名人名是否实质兼容（防范恶意冒名，同时容许全名/简称/别名合理差异）
    1. 规范化完全相等（如 Lautaro Martínez vs Lautaro Martinez）
    2. 词袋子集/超集关系（如 Yann Aurel Bisseck vs Yann Bisseck, Pape Sarr vs Pape Matar Sarr）
    3. 姓氏相同且名具有前缀缩写兼容（如 Nicolás González vs Nico González）
    """
    if not name_a or not name_b:
        return False
    norm_a = normalize_name(name_a)
    norm_b = normalize_name(name_b)
    if norm_a == norm_b or norm_a in norm_b or norm_b in norm_a:
        return True
    toks_a = set(tokenize_name(name_a))
    toks_b = set(tokenize_name(name_b))
    if not toks_a or not toks_b:
        return False
    if toks_a.issubset(toks_b) or toks_b.issubset(toks_a):
        return True
    list_a = tokenize_name(name_a)
    list_b = tokenize_name(name_b)
    if list_a and list_b and list_a[-1] == list_b[-1]:
        first_a, first_b = list_a[0], list_b[0]
        if first_a.startswith(first_b) or first_b.startswith(first_a):
            return True
    return False

# Standard Country Alias Table
COUNTRY_ALIASES = {
    "england": "england", "eng": "england", "spain": "spain", "esp": "spain",
    "italy": "italy", "ita": "italy", "france": "france", "fra": "france",
    "germany": "germany", "ger": "germany", "portugal": "portugal", "por": "portugal",
    "netherlands": "netherlands", "ned": "netherlands", "argentina": "argentina", "arg": "argentina",
    "brazil": "brazil", "bra": "brazil", "switzerland": "switzerland", "sui": "switzerland",
    "denmark": "denmark", "den": "denmark", "angola": "angola", "ang": "angola",
    "ivory coast": "cote d'ivoire", "côte d'ivoire": "cote d'ivoire", "cote d'ivoire": "cote d'ivoire", "civ": "cote d'ivoire",
    "dr congo": "dr congo", "congo dr": "dr congo", "drc": "dr congo", "cod": "dr congo",
    "algeria": "algeria", "alg": "algeria", "dza": "algeria", "sweden": "sweden", "swe": "sweden",
    "scotland": "scotland", "sco": "scotland", "poland": "poland", "pol": "poland",
    "belgium": "belgium", "bel": "belgium", "romania": "romania", "rou": "romania",
    "guinea-bissau": "guinea-bissau", "gnb": "guinea-bissau", "north macedonia": "north macedonia", "mkd": "north macedonia",
    "montenegro": "montenegro", "mne": "montenegro", "croatia": "croatia", "cro": "croatia",
    "greece": "greece", "gre": "greece", "grc": "greece", "austria": "austria", "aut": "austria",
    "serbia": "serbia", "srb": "serbia", "morocco": "morocco", "mar": "morocco",
    "colombia": "colombia", "col": "colombia", "chile": "chile", "chi": "chile",
    "uruguay": "uruguay", "uru": "uruguay", "slovenia": "slovenia", "svn": "slovenia",
    "turkey": "turkey", "tur": "turkey", "türkiye": "turkey", "turkiye": "turkey",
    "wales": "wales", "wal": "wales", "norway": "norway", "nor": "norway",
    "united states": "united states", "usa": "united states", "us": "united states",
    "bosnia and herzegovina": "bosnia-herzegovina", "bosnia-herzegovina": "bosnia-herzegovina", "bih": "bosnia-herzegovina",
    "czech republic": "czech republic", "czechia": "czech republic", "cze": "czech republic",
    "finland": "finland", "fin": "finland", "japan": "japan", "jpn": "japan",
    "mexico": "mexico", "mex": "mexico", "nigeria": "nigeria", "nga": "nigeria",
    "senegal": "senegal", "sen": "senegal", "cameroon": "cameroon", "cmr": "cameroon",
    "ghana": "ghana", "gha": "ghana", "mali": "mali", "mli": "mali",
    "paraguay": "paraguay", "par": "paraguay", "peru": "peru", "per": "peru",
    "ecuador": "ecuador", "ecu": "ecuador", "venezuela": "venezuela", "ven": "venezuela",
    "georgia": "georgia", "geo": "georgia", "albania": "albania", "alb": "albania",
    "kosovo": "kosovo", "kvx": "kosovo", "slovakia": "slovakia", "svk": "slovakia",
    "hungary": "hungary", "hun": "hungary", "ireland": "ireland", "irl": "ireland",
    "northern ireland": "northern ireland", "nir": "northern ireland", "iceland": "iceland", "isl": "iceland",
    "australia": "australia", "aus": "australia", "new zealand": "new zealand", "nzl": "new zealand",
    "south korea": "south korea", "korea, south": "south korea", "kor": "south korea",
    "canada": "canada", "can": "canada", "guinea": "guinea", "gui": "guinea", "gin": "guinea",
    "jamaica": "jamaica", "jam": "jamaica"
}

# Known conflict/homonym players strictly degraded
DEGRADED_CONFLICT_FIDS = {
    1705649: "Alessandro Romano (FotMob: Italy vs TM: Switzerland)",
    1344550: "Willy Kambwala (FotMob: DR Congo vs TM: France)",
    1417101: "Paco Esteban (FotMob youth player vs Chilean veteran 1990 Esteban Pavez)"
}

class CanonicalMatchResolver:
    """内部赛事 ID (Internal Fixture ID) 与 FotMob Match ID 双向规范化绑定与去重解析器"""
    def __init__(self, vault_root: Optional[Path] = None):
        self.internal_to_fotmob = {}
        self.fotmob_to_internal = {}
        self._load_bindings(vault_root or DEFAULT_VAULT_ROOT)

    def _load_bindings(self, vault_root: Path):
        # 1. 扫描 03_Match_Audits 绑定的 00-fixture-binding.json 或 00-fotmob-raw.json
        audits_root = vault_root / "03_Match_Audits/2026-2027"
        if audits_root.exists():
            for b_file in audits_root.glob("**/00-fixture-binding.json"):
                try:
                    b_data = json.loads(b_file.read_text("utf-8"))
                    items = b_data if isinstance(b_data, list) else [b_data]
                    for b in items:
                        int_id = str(b.get("fixture_id", "")).strip()
                        f_id = str(b.get("fotmob_id") or b.get("fotmob_match_id", "")).strip()
                        if int_id and f_id:
                            self.internal_to_fotmob[int_id] = f_id
                            self.fotmob_to_internal[f_id] = int_id
                except Exception:
                    pass
        # 显式登记已确证的双向绑定 (含 2993829 <-> 5749714)
        self.internal_to_fotmob["2993829"] = "5749714"
        self.fotmob_to_internal["5749714"] = "2993829"

    def register_binding(self, internal_id: str, fotmob_id: str):
        if internal_id and fotmob_id:
            s_int = str(internal_id).strip()
            s_fot = str(fotmob_id).strip()
            self.internal_to_fotmob[s_int] = s_fot
            self.fotmob_to_internal[s_fot] = s_int

    def get_canonical_id(self, fixture_id: Optional[str], match_id: Optional[str] = None) -> str:
        """统一返回主规范 ID (优先返回内部 fixture_id)"""
        s_fix = str(fixture_id).strip() if fixture_id else ""
        s_mid = str(match_id).strip() if match_id else ""

        if s_fix and s_fix in self.internal_to_fotmob:
            return s_fix
        if s_mid and s_mid in self.fotmob_to_internal:
            return self.fotmob_to_internal[s_mid]
        if s_fix:
            return s_fix
        if s_mid:
            return s_mid
        return "UNKNOWN_MATCH"

    def are_same_match(self, m1: Dict[str, Any], m2: Dict[str, Any]) -> bool:
        """判断两条比赛记录是否为同一场物理比赛 (支持跨 ID 绑定与实体复合指纹)"""
        c1 = self.get_canonical_id(m1.get("fixture_id"), m1.get("match_id"))
        c2 = self.get_canonical_id(m2.get("fixture_id"), m2.get("match_id"))
        if c1 != "UNKNOWN_MATCH" and c2 != "UNKNOWN_MATCH" and c1 == c2:
            return True

        # 实体指纹判定: 对阵双方 + 比赛日期 (前 10 位 YYYY-MM-DD)
        t1 = (m1.get("team_name") or "").lower().strip()
        opp1 = (m1.get("opponent") or "").lower().strip()
        d1 = str(m1.get("match_date") or m1.get("kickoff_utc") or m1.get("match_kickoff_utc") or "")[:10]

        t2 = (m2.get("team_name") or "").lower().strip()
        opp2 = (m2.get("opponent") or "").lower().strip()
        d2 = str(m2.get("match_date") or m2.get("kickoff_utc") or m2.get("match_kickoff_utc") or "")[:10]

        if t1 and opp1 and d1 and t2 and opp2 and d2:
            if d1 == d2 and t1 == t2 and opp1 == opp2:
                return True
        return False

def load_active_production_priors(
    vault_root: Optional[Path] = None,
    explicit_path: Optional[Path] = None
) -> Tuple[Dict[str, Any], Path, str]:
    """
    Task A: 动态读取当前已发布、SHA 验证通过的 team_recent_xi_value.current.json 所引用的权威资产。
    绝不硬编码 W43 或任何周次，支持发布后下一轮自适应继承。
    返回: (candidate_doc, resolved_canonical_file, active_cycle)
    """
    v_root = Path(vault_root or DEFAULT_VAULT_ROOT)
    if explicit_path and Path(explicit_path).exists():
        p = Path(explicit_path)
        data = json.loads(p.read_text("utf-8"))
        cycle = data.get("archive_cycle") or p.stem.split(".")[-1]
        return data, p, cycle

    pointer_file = v_root / "02_Team_Archives/priors/team_recent_xi_value.current.json"
    if not pointer_file.exists():
        raise FileNotFoundError(f"Production active pointer missing: {pointer_file}")

    ptr_data = json.loads(pointer_file.read_text("utf-8"))
    active_cycle = ptr_data.get("active_cycle") or ptr_data.get("current_active_cycle") or "UNKNOWN"
    canonical_rel = ptr_data.get("canonical_asset_relative_path") or ptr_data.get("canonical_json_path")
    expected_sha = ptr_data.get("canonical_asset_sha256") or ptr_data.get("canonical_json_sha256")

    canonical_target = pointer_file.parent / canonical_rel if canonical_rel else (pointer_file.parent / f"team_recent_xi_value.{active_cycle}.json")
    if not canonical_target.exists():
        raise FileNotFoundError(f"Canonical asset {canonical_target} referenced by pointer does not exist")

    # 物理 SHA-256 完整性回读校验
    actual_sha = sha256_file(canonical_target)
    if expected_sha and actual_sha != expected_sha:
        raise ValueError(f"CRITICAL: Pointer SHA mismatch for {canonical_target}! Expected {expected_sha}, got {actual_sha}")

    data = json.loads(canonical_target.read_text("utf-8"))
    return data, canonical_target, active_cycle

class PostmatchValuationEngine:
    """赛后单场首发身价计算与滚动维护核心引擎"""

    def __init__(self, tm_dataset_path: Optional[Path] = None, vault_root: Optional[Path] = None):
        self.tm_dataset_path = Path(tm_dataset_path or DEFAULT_TM_DATASET)
        self.vault_root = Path(vault_root or DEFAULT_VAULT_ROOT)
        self.all_tm_players = {}
        self.tm_name_index = defaultdict(list)
        self.fotmob_to_canonical = {}
        self.audited_identity_registry = {}
        self.team_to_club = {}
        self.tm_sha256 = ""
        self.match_resolver = CanonicalMatchResolver(self.vault_root)

        self._load_tm_dataset()
        self._load_team_mapping()
        self._load_audited_identity_registry()
        self._load_club_mapping()

    def _load_team_mapping(self):
        m_path = WORKSPACE_ROOT / "staging/recent_xi_prior_continuation/w40_rolling/sources/TOP_FIVE_96_FOTMOB_IDENTITY_MAPPING.json"
        if m_path.exists():
            try:
                data = json.loads(m_path.read_text("utf-8"))
                for t in data.get("teams", []):
                    fmid = str(t.get("fotmob_team_id", ""))
                    cname = t.get("canonical_team_name")
                    if fmid and cname:
                        self.fotmob_to_canonical[fmid] = cname
            except Exception:
                pass

    def _load_audited_identity_registry(self):
        """加载经过多因子 5 维核验的球员身份注册表 (包含 R1+R2.2 确证 126 名球员)"""
        reg_path = Path(__file__).parent / "audited_identity_registry.json"
        if reg_path.exists():
            try:
                data = json.loads(reg_path.read_text("utf-8"))
                for fid_str, rec in data.items():
                    self.audited_identity_registry[int(fid_str)] = rec
            except Exception as e:
                print(f"[WARN] Failed to load audited_identity_registry: {e}")

    def _load_club_mapping(self):
        """加载球队到 Transfermarkt Club ID 的映射字典"""
        reg_path = Path(__file__).parent / "team_club_id_registry.json"
        if reg_path.exists():
            try:
                self.team_to_club = json.loads(reg_path.read_text("utf-8"))
            except Exception as e:
                print(f"[WARN] Failed to load team_club_id_registry: {e}")

    def _load_tm_dataset(self):
        if not self.tm_dataset_path.exists():
            raise FileNotFoundError(f"Transfermarkt dataset missing at: {self.tm_dataset_path}")
        self.tm_sha256 = sha256_file(self.tm_dataset_path)
        with gzip.open(self.tm_dataset_path, "rt", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                val = int(row.get("market_value_in_eur") or 0) if row.get("market_value_in_eur") else None
                dob = row.get("date_of_birth", "").strip()
                by = int(dob[:4]) if len(dob) >= 4 and dob[:4].isdigit() else None
                pid = row["player_id"].strip()
                p_name = row.get("name", "").strip()
                rec = {
                    "player_id": pid,
                    "name": p_name,
                    "current_club_id": str(row.get("current_club_id", "")).strip(),
                    "current_club_name": row.get("current_club_name", "").strip(),
                    "valuation_eur": val,
                    "dob": dob,
                    "birth_year": by,
                    "country": row.get("country_of_citizenship", "").strip(),
                    "position": row.get("position", "").strip(),
                    "sub_position": row.get("sub_position", "").strip()
                }
                self.all_tm_players[pid] = rec
                norm_n = normalize_name(p_name)
                self.tm_name_index[norm_n].append(rec)

    @staticmethod
    def is_country_match(f_cname: Optional[str], f_ccode: Optional[str], tm_c: Optional[str]) -> Tuple[bool, str]:
        if not tm_c:
            return False, "Missing TM country"
        if not f_cname and not f_ccode:
            return False, "Missing FotMob country"
        t_norm = tm_c.lower().strip()
        f_name_norm = COUNTRY_ALIASES.get(f_cname.lower().strip() if f_cname else "", f_cname.lower().strip() if f_cname else "")
        f_code_norm = COUNTRY_ALIASES.get(f_ccode.lower().strip() if f_ccode else "", f_ccode.lower().strip() if f_ccode else "")
        t_target = COUNTRY_ALIASES.get(t_norm, t_norm)
        if f_name_norm == t_target or f_code_norm == t_target:
            return True, "MATCH"
        dual_map = {
            ("jamaica", "england"): True,
            ("england", "jamaica"): True,
            ("france", "guinea"): True,
            ("guinea", "france"): True,
            ("portugal", "cape verde"): True,
            ("cape verde", "portugal"): True,
        }
        if (f_name_norm, t_target) in dual_map or (f_code_norm, t_target) in dual_map:
            return True, "MATCH_DUAL_CITIZENSHIP"
        return False, f"Mismatch: FotMob '{f_cname}/{f_ccode}' vs TM '{tm_c}'"

    @staticmethod
    def is_position_match(usual_pos: Optional[int], pos_id: Optional[int], tm_pos: Optional[str], tm_sub: Optional[str]) -> Tuple[bool, str]:
        if not tm_pos:
            return False, "Missing TM position"
        tm_text = (tm_pos + " " + (tm_sub or "")).lower()
        if usual_pos == 0:
            if "keeper" in tm_text or "goalkeeper" in tm_text:
                return True, "MATCH"
        elif usual_pos == 1:
            if "def" in tm_text or "back" in tm_text:
                return True, "MATCH"
            if "mid" in tm_text and ("right" in tm_text or "left" in tm_text):
                return True, "MATCH_WINGBACK"
        elif usual_pos == 2:
            if "mid" in tm_text:
                return True, "MATCH"
            if "wing" in tm_text or "back" in tm_text:
                return True, "MATCH_WIDE_OVERLAP"
            if "striker" in tm_text or "second striker" in tm_text or "att" in tm_text:
                return True, "MATCH_ATTACKING_MID"
        elif usual_pos == 3:
            if "att" in tm_text or "forw" in tm_text or "wing" in tm_text or "striker" in tm_text:
                return True, "MATCH"
            if "mid" in tm_text and "attacking" in tm_text:
                return True, "MATCH_ATTACKING_MID"

        if pos_id:
            if pos_id == 11 and "keeper" in tm_text:
                return True, "MATCH_BY_POSID"
            if 30 <= pos_id <= 39 and ("def" in tm_text or "back" in tm_text):
                return True, "MATCH_BY_POSID"
            if 70 <= pos_id <= 79 and "mid" in tm_text:
                return True, "MATCH_BY_POSID"
            if 80 <= pos_id <= 89 and ("mid" in tm_text or "att" in tm_text or "striker" in tm_text):
                return True, "MATCH_BY_POSID"
            if 100 <= pos_id <= 109 and ("att" in tm_text or "forw" in tm_text or "wing" in tm_text):
                return True, "MATCH_BY_POSID"

        return False, f"Mismatch: FotMob UsualPos={usual_pos}, PosID={pos_id} vs TM '{tm_pos}/{tm_sub}'"

    @staticmethod
    def is_age_match(f_age: Optional[int], tm_byear: Optional[int], tm_dob: Optional[str]) -> Tuple[bool, str]:
        if f_age is None or tm_byear is None:
            return False, "Missing Age/DOB"
        calc_age = 2026 - tm_byear
        if abs(calc_age - int(f_age)) <= 1:
            return True, "MATCH"
        return False, f"Age mismatch: FotMob {f_age} vs TM birth_year {tm_byear}"

    def resolve_starter(self, starter_raw: Dict[str, Any], team_club_id: Optional[str] = None) -> Tuple[Optional[int], str, Optional[Dict[str, Any]], str]:
        """
        统一多因子身份核验入口 (含 Task C 运行时双向强安全防线)
        返回 (valuation_eur, status, tm_player_obj, audit_note)
        status: IDENTITY_VERIFIED / IDENTITY_UNRESOLVED
        """
        fid = starter_raw.get("id")
        f_name = starter_raw.get("name", "").strip()
        norm_p = normalize_name(f_name)

        # 1. 严格排查降级冲突名单 (Level 0)
        if fid and int(fid) in DEGRADED_CONFLICT_FIDS:
            return None, "IDENTITY_UNRESOLVED", None, f"DEGRADED_CONFLICT: {DEGRADED_CONFLICT_FIDS[int(fid)]}"

        # 2. 检查多因子已审核身份注册表 (Task C: 运行时身份与快照版本校验防线)
        if fid and int(fid) in self.audited_identity_registry:
            audited = self.audited_identity_registry[int(fid)]
            tm_id = audited.get("tm_player_id")

            # 防线 C1: 检查姓名一致性 (严禁 FotMob ID 姓名严重碰撞仍自动通过)
            reg_f_name = audited.get("fotmob_name", "")
            reg_tm_name = audited.get("tm_name", "")

            name_matched = is_name_compatible(f_name, reg_f_name) or is_name_compatible(f_name, reg_tm_name)
            if not name_matched:
                return None, "IDENTITY_UNRESOLVED", None, f"SECURITY_ALERT: FotMob ID {fid} name collision! Input '{f_name}' != Registered '{reg_f_name}'"

            # 防线 C2: 检查 TM ID 是否在当前快照中 (若快照未收录但注册表有实证核验正价，允许采纳实证)
            tm_p = self.all_tm_players.get(tm_id)
            if not tm_p:
                if audited.get("valuation_eur") and audited["valuation_eur"] > 0:
                    return audited["valuation_eur"], "IDENTITY_VERIFIED", {
                        "player_id": tm_id,
                        "name": reg_tm_name or f_name,
                        "valuation_eur": audited["valuation_eur"],
                        "country": audited.get("country") or starter_raw.get("countryName"),
                        "position": audited.get("position") or starter_raw.get("usualPlayingPositionId")
                    }, f"AUDITED_REGISTRY_VERIFIED: {audited.get('audit_note', '')}"
                return None, "IDENTITY_UNRESOLVED", None, f"SNAPSHOT_MISMATCH: TM ID {tm_id} not found in 2026-09-30 snapshot"

            # 防线 C3: 快照估值一致性 (优先物理快照现值，快照缺值时若注册表有核验正价则采纳)
            val = tm_p.get("valuation_eur")
            if val is None or val <= 0:
                if audited.get("valuation_eur") and audited["valuation_eur"] > 0:
                    val = audited["valuation_eur"]
                else:
                    return None, "FOUND_IN_TM_BUT_NO_VALUATION", tm_p, "TM snapshot market value is null or zero"

            # 防线 C4: 多因子运行时冲突排查 (国籍/年龄明显冲突降级)
            f_cname = starter_raw.get("countryName")
            f_ccode = starter_raw.get("countryCode")
            if f_cname or f_ccode:
                c_ok, c_msg = self.is_country_match(f_cname, f_ccode, tm_p["country"])
                if not c_ok and "Mismatch" in c_msg:
                    return None, "IDENTITY_UNRESOLVED", tm_p, f"RUNTIME_CONFLICT: Nationality {c_msg}"

            f_age = starter_raw.get("age")
            if f_age is not None and tm_p["birth_year"]:
                a_ok, a_msg = self.is_age_match(f_age, tm_p["birth_year"], tm_p["dob"])
                if not a_ok and abs(2026 - tm_p["birth_year"] - int(f_age)) > 2:
                    return None, "IDENTITY_UNRESOLVED", tm_p, f"RUNTIME_CONFLICT: Age difference > 2 years ({f_age} vs {2026 - tm_p['birth_year']})"

            note = f"{audited.get('audit_source', 'AUDITED')}: {audited.get('audit_note', 'Verified')}"
            return val, "IDENTITY_VERIFIED", tm_p, note

        # 3. 检查同俱乐部球员匹配 + 动态五维校验
        if team_club_id:
            cands_c = [c for c in self.all_tm_players.values() if c["current_club_id"] == str(team_club_id) and normalize_name(c["name"]) == norm_p]
            if len(cands_c) == 1:
                cand = cands_c[0]
                c_ok, c_msg = self.is_country_match(starter_raw.get("countryName"), starter_raw.get("countryCode"), cand["country"])
                a_ok, a_msg = self.is_age_match(starter_raw.get("age"), cand["birth_year"], cand["dob"])
                p_ok, p_msg = self.is_position_match(starter_raw.get("usualPlayingPositionId"), starter_raw.get("positionId"), cand["position"], cand["sub_position"])
                v_ok = cand["valuation_eur"] is not None and cand["valuation_eur"] > 0

                if c_ok and a_ok and p_ok and v_ok:
                    return cand["valuation_eur"], "IDENTITY_VERIFIED", cand, f"CLUB_EXACT_AND_FACTORS_VERIFIED: TM {cand['player_id']}"
                elif cand["valuation_eur"] is None or cand["valuation_eur"] == 0:
                    return None, "FOUND_IN_TM_BUT_NO_VALUATION", cand, "TM market value is null or zero in snapshot"
                else:
                    reasons = [m for ok, m in [(c_ok, c_msg), (a_ok, a_msg), (p_ok, p_msg)] if not ok]
                    return None, "IDENTITY_UNRESOLVED", cand, f"FACTORS_FAILED: {'; '.join(reasons)}"

        # 4. 全局名称索引查找 (单候选 + 动态五维校验)
        candidates = list(self.tm_name_index.get(norm_p, []))
        if len(candidates) == 1:
            cand = candidates[0]
            c_ok, c_msg = self.is_country_match(starter_raw.get("countryName"), starter_raw.get("countryCode"), cand["country"])
            a_ok, a_msg = self.is_age_match(starter_raw.get("age"), cand["birth_year"], cand["dob"])
            p_ok, p_msg = self.is_position_match(starter_raw.get("usualPlayingPositionId"), starter_raw.get("positionId"), cand["position"], cand["sub_position"])
            v_ok = cand["valuation_eur"] is not None and cand["valuation_eur"] > 0

            if c_ok and a_ok and p_ok and v_ok:
                return cand["valuation_eur"], "IDENTITY_VERIFIED", cand, f"ALL_FACTORS_VERIFIED: TM {cand['player_id']}"
            elif cand["valuation_eur"] is None or cand["valuation_eur"] == 0:
                return None, "FOUND_IN_TM_BUT_NO_VALUATION", cand, "TM market value is null or zero in snapshot"
            else:
                reasons = [m for ok, m in [(c_ok, c_msg), (a_ok, a_msg), (p_ok, p_msg)] if not ok]
                return None, "IDENTITY_UNRESOLVED", cand, f"FACTORS_FAILED: {'; '.join(reasons)}"

        return None, "IDENTITY_UNRESOLVED", None, "Player not found in TM snapshot or ambiguous name"

    def generate_single_match_lineup_valuation(
        self,
        raw_match_data: Dict[str, Any],
        league: str,
        season: str,
        round_id: str,
        fixture_id: str,
        home_club_id: Optional[str] = None,
        away_club_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Task B: 生成单场首发身价标准资产 06-lineup-valuation.json
        """
        gen = raw_match_data.get("general", {})
        ko_utc = gen.get("matchTimeUTCDate", "")
        h_info = gen.get("homeTeam", {})
        a_info = gen.get("awayTeam", {})
        mid = str(gen.get("matchId", fixture_id))

        # 注册双向绑定
        self.match_resolver.register_binding(fixture_id, mid)

        lineup_content = raw_match_data.get("content", {}).get("lineup", {})
        teams_valuation = {}

        for side, t_info, cid in [("homeTeam", h_info, home_club_id), ("awayTeam", a_info, away_club_id)]:
            tid_str = str(t_info.get("id", ""))
            raw_tname = t_info.get("name", "Unknown")
            tname = self.fotmob_to_canonical.get(tid_str, raw_tname)
            starters_raw = lineup_content.get(side, {}).get("starters", [])
            if not starters_raw and "lineup" in lineup_content and isinstance(lineup_content["lineup"], list):
                idx = 0 if side == "homeTeam" else 1
                if idx < len(lineup_content["lineup"]):
                    starters_raw = lineup_content["lineup"][idx].get("starters", [])

            # 自动推导 TM club_id
            target_cid = cid or self.team_to_club.get(tname)

            evaluated_starters = []
            known_sum_eur = 0
            valued_count = 0
            missing_players = []

            for s in starters_raw:
                s_name = s.get("name", "Unknown")
                fid = s.get("id")
                shirt = s.get("shirtNumber", "")
                val_eur, status, tm_p, note = self.resolve_starter(s, team_club_id=target_cid)

                record = {
                    "player_name": s_name,
                    "fotmob_id": fid,
                    "shirt_number": shirt,
                    "tm_player_id": tm_p.get("player_id") if tm_p else None,
                    "valuation_eur": val_eur,
                    "status": status,
                    "audit_note": note,
                    "country": (tm_p.get("country") if tm_p else None) or s.get("countryName"),
                    "position": (tm_p.get("position") if tm_p else None) or s.get("usualPlayingPositionId")
                }
                evaluated_starters.append(record)

                if val_eur is not None and val_eur > 0:
                    known_sum_eur += val_eur
                    valued_count += 1
                else:
                    missing_players.append({
                        "player_name": s_name,
                        "fotmob_id": fid,
                        "shirt_number": shirt,
                        "reason": note
                    })

            is_full = (valued_count == 11 and len(starters_raw) == 11)
            basis = "FULL_XI" if is_full else "PARTIAL_LOWER_BOUND"

            teams_valuation[side] = {
                "team_name": tname,
                "team_id": str(t_info.get("id", "")),
                "lineup_status": "CONFIRMED_XI" if len(starters_raw) == 11 else "PENDING_LINEUP_EVIDENCE",
                "valuation_basis": basis,
                "valued_starter_count": valued_count,
                "total_starters_count": len(starters_raw),
                "coverage_ratio": f"{valued_count}/{len(starters_raw)}",
                "known_xi_value_eur": known_sum_eur,
                "missing_players_count": len(missing_players),
                "missing_players": missing_players,
                "starters": evaluated_starters
            }

        now_iso = datetime.now(timezone.utc).isoformat()
        doc = {
            "schema_version": "1.0.0",
            "artifact_name": "06-lineup-valuation.json",
            "match_metadata": {
                "league": league,
                "season": season,
                "round": round_id,
                "fixture_id": str(fixture_id),
                "fotmob_match_id": mid,
                "canonical_match_id": self.match_resolver.get_canonical_id(fixture_id, mid),
                "match_kickoff_utc": ko_utc,
                "home_team": {"name": teams_valuation["homeTeam"]["team_name"], "id": str(h_info.get("id", "")), "raw_name": h_info.get("name")},
                "away_team": {"name": teams_valuation["awayTeam"]["team_name"], "id": str(a_info.get("id", "")), "raw_name": a_info.get("name")}
            },
            "teams_valuation": teams_valuation,
            "provenance": {
                "lineup_provider": "FotMob",
                "valuation_dataset": "Transfermarkt",
                "valuation_snapshot_date": "2026-09-30",
                "valuation_snapshot_sha256": self.tm_sha256,
                "source_retrieved_at": "UNKNOWN",
                "processed_at": now_iso,
                "producer_script": "src/engine/postmatch_valuation_integrator.py"
            },
            "postmatch_gate": {
                "lineup_evidence": "CONFIRMED" if all(tv["total_starters_count"] == 11 for tv in teams_valuation.values()) else "PENDING_LINEUP_EVIDENCE",
                "valuation_persistence": "READY_FOR_STAGE6_AUTHORIZATION"
            }
        }
        return doc

    def compute_team_rolling_window(
        self,
        team_name: str,
        historical_matches: List[Dict[str, Any]],
        as_of_cutoff_utc: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Task D: 严格两步时序规则
        1. 先选择研究截止时间 (as_of_cutoff_utc) 之前最近三场已完赛且首发证据合格的比赛
        2. 严格两步法: 若某场缺价输出 MEAN_2 / SINGLE_1 / UNKNOWN，绝不拉第 4 场替补！
        """
        # 第一步: 过滤不合格比赛 (未完赛、未来比赛) 并进行规范化去重
        eligible_matches = []
        seen_matches = []

        for m in historical_matches:
            ko = m.get("kickoff_utc") or m.get("match_kickoff_utc") or ""
            # 规则 D1: 排除未来比赛 (kickoff_utc > as_of_cutoff_utc)
            if as_of_cutoff_utc and ko and ko > as_of_cutoff_utc:
                continue

            # 规则 D2: 排除未完赛比赛 (未确认首发或未开赛)
            status = m.get("lineup_status") or m.get("status")
            if status == "PENDING_LINEUP_EVIDENCE":
                continue

            # 规则 D4: 规范化双向去重 (Internal ID <-> FotMob ID 映射去重)
            is_dup = False
            for prev in seen_matches:
                if self.match_resolver.are_same_match(prev, m):
                    is_dup = True
                    break
            if is_dup:
                continue

            seen_matches.append(m)
            eligible_matches.append(m)

        # 规则 D5: 补赛按照真实 kickoff_utc 严格时序倒序排序
        sorted_matches = sorted(
            eligible_matches,
            key=lambda x: (x.get("kickoff_utc") or x.get("match_kickoff_utc", ""), str(x.get("fixture_id") or x.get("match_id", ""))),
            reverse=True
        )

        # 严格截取截止时间前最近 3 场比赛 (固定窗口，绝不从第 4 场抓替补)
        fixed_window = sorted_matches[:3]

        # 第二步: 评估这 3 场比赛的金额可用性
        valid_valued_matches = [m for m in fixed_window if m.get("known_xi_value_eur") is not None and float(m["known_xi_value_eur"]) > 0]
        n_valid = len(valid_valued_matches)

        if len(fixed_window) == 3 and n_valid == 3:
            sample_code = "MEAN_3"
            status = "READY"
        elif n_valid == 2:
            sample_code = "MEAN_2"
            status = "READY"
        elif n_valid == 1:
            sample_code = "SINGLE_1"
            status = "CAUTION"
        else:
            sample_code = "UNKNOWN"
            status = "UNKNOWN"

        if n_valid > 0:
            sums = [float(m["known_xi_value_eur"]) for m in valid_valued_matches]
            mean_eur = round(sum(sums) / float(n_valid), 2)
            sorted_sums = sorted(sums)
            if n_valid % 2 == 1:
                median_eur = sorted_sums[n_valid // 2]
            else:
                median_eur = round((sorted_sums[0] + sorted_sums[1]) / 2.0, 2)
            all_full = (n_valid == 3 and all(m.get("valuation_basis") == "FULL_XI" for m in valid_valued_matches))
            basis = "FULL_XI" if all_full else "PARTIAL_LOWER_BOUND"
        else:
            mean_eur = None
            median_eur = None
            basis = "UNKNOWN"

        return {
            "team_name": team_name,
            "status": status,
            "sample_count": n_valid,
            "sample_code": sample_code,
            "valuation_basis": basis,
            "mean_recent_xi_eur": mean_eur,
            "median_recent_xi_eur": median_eur,
            "matches_used": [str(m.get("fixture_id") or m.get("match_id")) for m in fixed_window],
            "matches_detail": fixed_window,  # 完整 matches 历史明细
            "coverages": [m.get("coverage_ratio", "UNKNOWN") for m in fixed_window],
            "cutoff_kickoff_utc": (fixed_window[0].get("kickoff_utc") or fixed_window[0].get("match_kickoff_utc")) if fixed_window else None
        }

    @staticmethod
    def render_team_archive_markdown_block(rolling_data: Dict[str, Any], cycle_name: str) -> str:
        """
        Task D: 生成球队档案 Markdown 侧车轻量引用块
        """
        tname = rolling_data["team_name"]
        mean_str = f"€{rolling_data['mean_recent_xi_eur']:,.2f}" if rolling_data["mean_recent_xi_eur"] else "null"
        med_str = f"€{rolling_data['median_recent_xi_eur']:,.2f}" if rolling_data["median_recent_xi_eur"] else "null"

        block = f"""<!-- ARES_SQUAD_VALUE_PRIOR_V1:{cycle_name}_RECENT_XI_VALUE_ROLLING_V1:START -->
## [{tname}] {cycle_name} 滚动首发身价先验侧车 ({cycle_name}_RECENT_XI_VALUE_ROLLING_V1)
```yaml
team_name: "{tname}"
rolling_cycle: "{cycle_name}"
status: "{rolling_data['status']}"
sample_code: "{rolling_data['sample_code']}"
sample_count: {rolling_data['sample_count']}
valuation_basis: "{rolling_data['valuation_basis']}"
mean_recent_xi_eur: {rolling_data['mean_recent_xi_eur']}
median_recent_xi_eur: {rolling_data['median_recent_xi_eur']}
matches_used: {rolling_data['matches_used']}
coverages: {rolling_data['coverages']}
cutoff_kickoff_utc: "{rolling_data['cutoff_kickoff_utc']}"
canonical_reference_snapshot: "2026-09-30"
```
> **业务释义**: 基于最近 {rolling_data['sample_count']} 场完赛确证首发 11 人 Transfermarkt 静态快照（2026-09-30）计算。均值：{mean_str}，中位数：{med_str}。状态：{rolling_data['valuation_basis']}。
<!-- ARES_SQUAD_VALUE_PRIOR_V1:{cycle_name}_RECENT_XI_VALUE_ROLLING_V1:END -->"""
        return block

def safe_write_single_match_valuation(
    target_path: Path,
    doc: Dict[str, Any],
    is_production_authorized: bool = False
) -> Dict[str, Any]:
    """
    Task E: 生产隔离与幂等发布防线
    1. 未获得 Stage 6 授权前，严禁写入 AresVault 生产目录
    2. 目标已存在且授权时，比对 SHA 实行幂等跳过
    """
    is_in_production = ("AresVault" in str(target_path) and "03_Match_Audits" in str(target_path))

    if is_in_production and not is_production_authorized:
        # 生产未授权，阻断写入生产目录
        return {
            "status": "BLOCKED_UNAUTHORIZED",
            "message": f"Write to production path {target_path} blocked: Stage 6 authorization required",
            "action": "SKIPPED"
        }

    target_path.parent.mkdir(parents=True, exist_ok=True)
    new_bytes = json.dumps(doc, indent=2, ensure_ascii=False).encode("utf-8")
    new_sha = hashlib.sha256(new_bytes).hexdigest()

    if target_path.exists():
        existing_bytes = target_path.read_bytes()
        existing_sha = hashlib.sha256(existing_bytes).hexdigest()
        if existing_sha == new_sha:
            return {
                "status": "PASS",
                "action": "IDEMPOTENT_UNCHANGED",
                "file_path": str(target_path),
                "sha256": existing_sha
            }
        else:
            if is_in_production and not is_production_authorized:
                raise PermissionError(f"Cannot overwrite existing production asset {target_path} without Stage 6 authorization!")

    with open(target_path, "wb") as wf:
        wf.write(new_bytes)

    return {
        "status": "PASS",
        "action": "WRITTEN",
        "file_path": str(target_path),
        "sha256": new_sha
    }

def run_round_postmatch_valuation_pipeline(
    league: str,
    season: str,
    round_id: str,
    fixtures: List[Dict[str, Any]],
    postmatch_base_dir: Path,
    staging_output_dir: Path,
    canonical_priors_path: Optional[Path] = None,
    is_production_authorized: bool = False
) -> Dict[str, Any]:
    """
    调度入口: 赛后单场身价计算与滚动维护全流程 (v2.2 最终版)
    """
    engine = PostmatchValuationEngine()
    staging_output_dir.mkdir(parents=True, exist_ok=True)

    audits_by_fixture = {}
    team_new_matches = defaultdict(list)

    print(f"\n[Postmatch Valuation] 启动 {league} {round_id} 全轮首发身价计算流水线...")

    for fix in fixtures:
        fid = str(fix["fixture_id"])
        fdir = postmatch_base_dir / fid
        raw_f = fdir / "00-fotmob-raw.json"
        if not raw_f.exists():
            continue

        try:
            raw_data = json.loads(raw_f.read_text("utf-8"))
            val_doc = engine.generate_single_match_lineup_valuation(
                raw_match_data=raw_data,
                league=league,
                season=season,
                round_id=round_id,
                fixture_id=fid
            )

            # Task E: 安全受控写入单场资产 (未授权时写入 staging 候选隔离区)
            if is_production_authorized:
                out_file = fdir / "06-lineup-valuation.json"
            else:
                out_file = staging_output_dir / "single_matches" / fid / "06-lineup-valuation.json"

            write_res = safe_write_single_match_valuation(
                target_path=out_file,
                doc=val_doc,
                is_production_authorized=is_production_authorized
            )

            audits_by_fixture[fid] = val_doc

            # 记录双方出场
            for side in ["homeTeam", "awayTeam"]:
                tv = val_doc["teams_valuation"][side]
                opp_side = "awayTeam" if side == "homeTeam" else "homeTeam"
                opp_name = val_doc["teams_valuation"][opp_side]["team_name"]
                team_new_matches[tv["team_name"]].append({
                    "fixture_id": fid,
                    "match_id": fid,
                    "match_date": val_doc["match_metadata"]["match_kickoff_utc"][:10],
                    "kickoff_utc": val_doc["match_metadata"]["match_kickoff_utc"],
                    "match_kickoff_utc": val_doc["match_metadata"]["match_kickoff_utc"],
                    "opponent": opp_name,
                    "home_away": "home" if side == "homeTeam" else "away",
                    "known_xi_value_eur": tv["known_xi_value_eur"],
                    "coverage_ratio": tv["coverage_ratio"],
                    "valuation_basis": tv["valuation_basis"],
                    "missing_players_count": tv["missing_players_count"]
                })
        except Exception as e:
            print(f"  [ERROR] 处理比赛 {fid} 身价失败: {e}")
            return {
                "status": "FAILED",
                "error": str(e),
                "failed_fixture": fid,
                "pipeline_status": "PIPELINE_FAILED"
            }

    # 门禁前置防线 (Fail-Closed): 确认至少有一场有效比赛产出阵容身价数据
    # 若零匹配或全部缺失原始数据，受控降级为 DEGRADED，严禁虚假放行 (Prevent Fail-Open)
    if len(audits_by_fixture) == 0 or len(team_new_matches) == 0:
        degraded_reason = f"No valid match lineup valuation generated for {len(fixtures)} input fixtures (missing 00-fotmob-raw.json or zero matches extracted)"
        print(f"  [WARN] 赛后身价流水线受控降级: {degraded_reason}")
        return {
            "status": "DEGRADED",
            "pipeline_status": "DEGRADED_NO_VALID_FIXTURES",
            "reason": degraded_reason,
            "error": degraded_reason,
            "single_match_artifacts_count": 0,
            "participating_teams_count": 0,
            "coverage_rate": f"0/{len(fixtures)}",
            "canonical_persistence_status": "BLOCKED_NO_DATA",
            "allow_stage6_promotion": False
        }

    # Task A: 动态装载当前生效的权威生产底座，绝不硬编码 W43
    candidate_doc, priors_path, active_cycle = load_active_production_priors(
        vault_root=engine.vault_root,
        explicit_path=canonical_priors_path
    )
    print(f"  [Active Priors Loaded] Cycle: {active_cycle}, File: {priors_path.name}")

    updated_teams = []
    markdown_blocks = {}

    for t_entry in candidate_doc.get("teams", []):
        tname = t_entry.get("team_name")
        if tname in team_new_matches:
            # 增量合并并重新计算最近 3 场窗口
            existing_matches = []
            for m in t_entry.get("matches", []):
                existing_matches.append({
                    "fixture_id": m.get("match_id") or m.get("fixture_id"),
                    "match_id": m.get("match_id") or m.get("fixture_id"),
                    "match_date": m.get("match_date") or (m.get("match_kickoff_utc", "")[:10]),
                    "kickoff_utc": m.get("match_kickoff_utc") or m.get("kickoff_utc") or m.get("match_date", ""),
                    "match_kickoff_utc": m.get("match_kickoff_utc") or m.get("kickoff_utc") or m.get("match_date", ""),
                    "opponent": m.get("opponent", "UNKNOWN"),
                    "home_away": m.get("home_away", "UNKNOWN"),
                    "known_xi_value_eur": m.get("known_value_sum_eur") or m.get("starting_xi_market_value_eur") or m.get("known_xi_value_eur", 0),
                    "coverage_ratio": f"{m.get('valued_starter_count', 11)}/11" if "valued_starter_count" in m else m.get("coverage_ratio", "11/11"),
                    "valuation_basis": "FULL_XI" if (m.get("valued_starter_count", 11) == 11 or m.get("valuation_basis") == "FULL_XI") else "PARTIAL_LOWER_BOUND",
                    "missing_players_count": m.get("missing_players_count", 0)
                })
            combined = existing_matches + team_new_matches[tname]
            rolling_res = engine.compute_team_rolling_window(tname, combined)

            # Build candidate entry (Task 2: 完整保存 matches 明细)
            new_t_entry = dict(t_entry)
            new_t_entry["matches"] = rolling_res["matches_detail"]
            new_t_entry["valid_match_count"] = rolling_res["sample_count"]
            new_t_entry["last3_status"] = rolling_res["status"]
            new_t_entry["primary_metric"] = {
                "metric_name": f"recent_xi_value_mean_{rolling_res['sample_count']}_eur",
                "value_eur": rolling_res["mean_recent_xi_eur"],
                "n_matches_used": rolling_res["sample_count"],
                "completeness": rolling_res["valuation_basis"],
                "semantic_basis": rolling_res["valuation_basis"],
                "derived_from_match_ids": rolling_res["matches_used"]
            }
            if rolling_res["median_recent_xi_eur"]:
                new_t_entry["secondary_metric"] = {
                    "metric_name": f"median_{rolling_res['sample_count']}",
                    "value_eur": rolling_res["median_recent_xi_eur"],
                    "n_matches_used": rolling_res["sample_count"],
                    "completeness": rolling_res["valuation_basis"],
                    "semantic_basis": rolling_res["valuation_basis"]
                }
            updated_teams.append(new_t_entry)
            markdown_blocks[tname] = engine.render_team_archive_markdown_block(rolling_res, f"{league.upper()}_{round_id.upper()}")
        else:
            updated_teams.append(t_entry)

    # 完整覆盖判定: 区分部分 N 匹配 vs 单侧缺失 vs 全部完整匹配
    # 规则 1: 比赛数量必须覆盖全量 fixtures
    # 规则 2: 每场比赛必须双侧均具备确证首发证据 (lineup_evidence == "CONFIRMED")
    all_fixtures_confirmed = (
        len(audits_by_fixture) == len(fixtures) and
        all(
            vdoc.get("postmatch_gate", {}).get("lineup_evidence") == "CONFIRMED"
            for vdoc in audits_by_fixture.values()
        )
    )

    if not all_fixtures_confirmed:
        status_code = "DEGRADED"
        pipe_status = "DEGRADED_PARTIAL"
        canon_status = "BLOCKED_PARTIAL_SQUAD_DATA"
        allow_stage6 = False
        if len(audits_by_fixture) < len(fixtures):
            degraded_msg = f"Partial coverage: only {len(audits_by_fixture)}/{len(fixtures)} fixtures processed; Stage 6 Promotion BLOCKED"
        else:
            degraded_msg = f"Incomplete lineup evidence in processed fixtures (single-sided missing or unconfirmed XI); Stage 6 Promotion BLOCKED"
        print(f"  [WARN] 赛后身价覆盖不全/单侧缺失降级: {degraded_msg}")
    else:
        status_code = "PASS"
        pipe_status = "STAGING_PASS"
        canon_status = "STAGING_READY_AWAITING_STAGE6_AUTHORIZATION"
        allow_stage6 = True
        degraded_msg = None

    candidate_doc["teams"] = updated_teams
    candidate_doc["pipeline_status"] = pipe_status
    candidate_doc["canonical_persistence_status"] = canon_status
    candidate_doc["active_cycle_inherited"] = active_cycle
    candidate_doc["allow_stage6_promotion"] = allow_stage6
    m_round = re.search(r"(?:GW|W)(?P<week>\d+)", round_id, re.IGNORECASE)
    if m_round:
        c_str = f"W{int(m_round.group('week'))}"
        candidate_doc["cycle_name"] = c_str
        candidate_doc["target_cycle"] = c_str
        candidate_doc["cycle"] = c_str
    if degraded_msg:
        candidate_doc["degraded_reason"] = degraded_msg

    # Write staging deliverables
    candidate_file = staging_output_dir / f"team_recent_xi_value.{round_id}_POSTMATCH_CANDIDATE.json"
    with open(candidate_file, "w", encoding="utf-8") as f:
        json.dump(candidate_doc, f, indent=2, ensure_ascii=False)

    cand_sha = sha256_file(candidate_file)
    with open(f"{candidate_file}.sha256", "w", encoding="utf-8") as f:
        f.write(f"{cand_sha}  {candidate_file.name}\n")

    audits_file = staging_output_dir / "postmatch_lineup_valuation_audits.json"
    with open(audits_file, "w", encoding="utf-8") as f:
        json.dump(audits_by_fixture, f, indent=2, ensure_ascii=False)

    # Write team archive markdown preview
    md_preview_file = staging_output_dir / "TEAM_ARCHIVE_VALUATION_BLOCKS_PREVIEW.md"
    with open(md_preview_file, "w", encoding="utf-8") as f:
        f.write("# 球队档案首发身价侧车更新预览 (Stage 6 归档候选)\n\n")
        for tname, block in sorted(markdown_blocks.items()):
            f.write(f"{block}\n\n---\n\n")

    # Generate CANDIDATE_MANIFEST.tsv
    manifest_entries = []
    for p in sorted(staging_output_dir.rglob("*")):
        if p.is_file() and p.name not in ("CANDIDATE_MANIFEST.tsv", ".DS_Store"):
            rel_name = str(p.relative_to(staging_output_dir))
            manifest_entries.append((rel_name, p.stat().st_size, sha256_file(p)))

    manifest_file = staging_output_dir / "CANDIDATE_MANIFEST.tsv"
    with open(manifest_file, "w", encoding="utf-8") as mf:
        mf.write("filename\tsize_bytes\tsha256\n")
        for name, size, sha in manifest_entries:
            mf.write(f"{name}\t{size}\t{sha}\n")

    res_dict = {
        "status": status_code,
        "pipeline_status": pipe_status,
        "coverage_rate": f"{len(audits_by_fixture)}/{len(fixtures)}",
        "allow_stage6_promotion": allow_stage6,
        "canonical_persistence_status": canon_status,
        "candidate_file": str(candidate_file),
        "candidate_sha256": cand_sha,
        "audits_file": str(audits_file),
        "manifest_file": str(manifest_file),
        "single_match_artifacts_count": len(audits_by_fixture),
        "participating_teams_count": len(team_new_matches),
        "markdown_preview": str(md_preview_file)
    }
    if degraded_msg:
        res_dict["reason"] = degraded_msg
    return res_dict


# 权威历史身价资产指纹白名单 (仅限历史已冻结批准的基线，严禁未授权新文件静默冒充)
HISTORICAL_SQUAD_VALUATION_APPROVED_WHITELIST = {
    # W40 Canonical 生产底座
    "51ff7692544f7700a6f9fd2775c7656476b271f9a558c967dbea655498e907d5": "W40_CANONICAL_BASELINE",
    # W43 Canonical 生产底座
    "29c693cc3f5e69ec1a31c04ed016661dba7f53108173331b821db4db6096a946": "W43_CANONICAL_BASELINE",
    # W43 R3 候选原件
    "c32f81ce3d8956a72621a4089da608fbf5181c456948edcc98f50314004256e8": "W43_R3_APPROVED_CANDIDATE",
    # W44 意甲发布已批准资产
    "e95926d2fd81dc2c7d853eac62301d1dc927a433eea05f55bb1592ef0fb33470": "W44_STAGE6_APPROVED_CANONICAL"
}


def verify_squad_valuation_promotion_gate(
    candidate_doc_or_path: Union[Dict[str, Any], Path, str],
    expected_manifest_sha: Optional[str] = None
) -> Tuple[bool, str]:
    """
    Stage 6 身价资产生产发布入口语义门禁 (LMW-413 Fail-Closed Gate)
    
    守卫规则:
    1. 物理存在与文件读取:
       - 候选文件必须物理存在且可读取；
       - 若指定 expected_manifest_sha，文件真实物理 SHA 必须 100% 严格一致，严禁伪造或内容漂移。
    2. JSON 解析与载荷结构检查:
       - 必须是合法 JSON 字典，必须包含 "teams" 且包含 96 支球队数据 (全五大联赛法定分母)。
    3. 新版赛后流水线候选 (New Pipeline Candidate):
       - 必须显式声明 allow_stage6_promotion: True；
       - 若缺少该字段，或其值为 False，坚决拒绝；
       - pipeline_status 严禁包含 DEGRADED (如 DEGRADED_PARTIAL, DEGRADED_NO_VALID_FIXTURES)；
       - canonical_persistence_status 严禁包含 BLOCKED (如 BLOCKED_PARTIAL_SQUAD_DATA, BLOCKED_NO_DATA)；
       - 严禁存在 degraded_reason 或 reason 降级原因；
       - 单侧球队数据缺失、部分比赛缺失或 0 比赛提取一律阻断。
    4. 历史资产权威白名单校验 (Historical Baseline Compatibility):
       - 对于不含 allow_stage6_promotion 字段的历史资产，绝对禁止静默兼容或无条件放行；
       - 必须且仅能匹配 HISTORICAL_SQUAD_VALUATION_APPROVED_WHITELIST 中的已审物理 SHA；
       - 任何不认识或未列入白名单的版本一律抛出 UNKNOWN_SCHEMA_OR_UNAPPROVED_VERSION 坚决拒绝。
    """
    live_sha = None
    if isinstance(candidate_doc_or_path, (str, Path)):
        cand_path = Path(candidate_doc_or_path)
        if not cand_path.exists():
            return False, f"CANDIDATE_FILE_MISSING: Path {cand_path} does not exist"
        live_bytes = cand_path.read_bytes()
        live_sha = hashlib.sha256(live_bytes).hexdigest()
        try:
            doc = json.loads(live_bytes.decode("utf-8"))
        except Exception as e:
            return False, f"INVALID_JSON_PAYLOAD: JSON decode error: {e}"
    elif isinstance(candidate_doc_or_path, dict):
        if "candidate_file" in candidate_doc_or_path and "teams" not in candidate_doc_or_path:
            cf_path = Path(candidate_doc_or_path["candidate_file"])
            if cf_path.exists():
                return verify_squad_valuation_promotion_gate(cf_path, expected_manifest_sha)
        doc = candidate_doc_or_path
        live_bytes = json.dumps(doc, indent=2, ensure_ascii=False).encode("utf-8")
        live_sha = hashlib.sha256(live_bytes).hexdigest()
    else:
        return False, "INVALID_INPUT_TYPE: Input must be Path, str, or dict"

    # 1. Manifest 哈希一致性校验
    if expected_manifest_sha and live_sha != expected_manifest_sha:
        return False, f"MANIFEST_SHA_MISMATCH: Live SHA {live_sha} != Manifest {expected_manifest_sha}"

    if not isinstance(doc, dict):
        return False, "INVALID_SCHEMA_ROOT: Root must be a JSON dictionary"

    # 2. 球队分母核验: 必须包含 96 支球队
    teams = doc.get("teams", [])
    if not isinstance(teams, list) or len(teams) != 96:
        return False, f"INVALID_TEAM_COUNT: Expected 96 teams, got {len(teams)}"

    # 3. 判定资产类型与门禁核验
    has_promotion_field = ("allow_stage6_promotion" in doc)

    if has_promotion_field:
        # 新版赛后候选分支 (Strict New Candidate Gate)
        allow_stage6 = doc.get("allow_stage6_promotion")
        pipe_status = str(doc.get("pipeline_status", "UNKNOWN"))
        canon_status = str(doc.get("canonical_persistence_status", "UNKNOWN"))
        degraded_reason = doc.get("degraded_reason") or doc.get("reason")

        if allow_stage6 is not True:
            return False, f"PROMOTION_DISALLOWED: allow_stage6_promotion is {allow_stage6} (must be strictly True)"

        if "DEGRADED" in pipe_status:
            return False, f"DEGRADED_PIPELINE_STATUS: pipeline_status='{pipe_status}' is degraded; Stage 6 Promotion blocked"

        if "BLOCKED" in canon_status:
            return False, f"BLOCKED_CANONICAL_STATUS: canonical_persistence_status='{canon_status}'; Stage 6 Promotion blocked"

        if degraded_reason:
            return False, f"DEGRADED_REASON_PRESENT: Candidate contains degraded_reason='{degraded_reason}'"

        return True, "NEW_PIPELINE_CANDIDATE_APPROVED"

    else:
        # 历史资产兼容分支 (Strict Historical Whitelist Only)
        if live_sha in HISTORICAL_SQUAD_VALUATION_APPROVED_WHITELIST:
            label = HISTORICAL_SQUAD_VALUATION_APPROVED_WHITELIST[live_sha]
            return True, f"HISTORICAL_APPROVED_BASELINE: {label}"
        else:
            return False, (
                f"UNKNOWN_SCHEMA_OR_UNAPPROVED_VERSION: Asset lacks 'allow_stage6_promotion' "
                f"and SHA-256 '{live_sha}' is not in approved historical baseline whitelist! "
                f"Default-deny applied."
            )



VOLATILE_PROMOTION_METADATA_KEYS = {
    "production_status",
    "pipeline_status",
    "canonical_persistence_status",
    "promoted_at_utc",
    "published_at_utc",
    "operator_approval_token",
    "candidate_staged_at_utc",
    "generator_script",
    "note",
    "allow_stage6_promotion",
    "degraded_reason",
}


def extract_valuation_business_content(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    提取身价资产的核心业务内容，排除发布阶段与流水线追加的易变元数据字段。
    同时归一化周期字段表示，确保语义比对精确。
    """
    biz = {k: v for k, v in doc.items() if k not in VOLATILE_PROMOTION_METADATA_KEYS}
    c_val = doc.get("cycle_name") or doc.get("target_cycle") or doc.get("cycle")
    if c_val:
        c_tuple = parse_cycle_id(c_val)
        biz["normalized_cycle"] = (
            f"{c_tuple[0]}-W{c_tuple[1]}"
            if c_tuple and c_tuple[0] > 0
            else f"W{c_tuple[1]}"
            if c_tuple
            else str(c_val)
        )
    biz.pop("cycle_name", None)
    biz.pop("target_cycle", None)
    biz.pop("cycle", None)
    return biz


class CycleGateResult(tuple):
    """
    周期门禁校验返回对象，同时支持 2 元组解包 (allowed, message) 和富属性/上下文访问。
    """
    def __new__(cls, allowed: bool, message: str, context: Optional[Dict[str, Any]] = None):
        inst = super().__new__(cls, (allowed, message))
        inst.allowed = allowed
        inst.message = message
        inst.context = context or {}
        return inst

    @property
    def validated_cycle_str(self) -> Optional[str]:
        return self.context.get("validated_cycle_str")

    @property
    def validated_cycle_tuple(self) -> Optional[Tuple[int, int]]:
        return self.context.get("validated_cycle_tuple")

    @property
    def active_pointer_file(self) -> Optional[Path]:
        return self.context.get("active_pointer_file")

    @property
    def is_idempotent_applied(self) -> bool:
        return self.context.get("is_idempotent_applied", False)

    @property
    def authoritative_pointer_write_path(self) -> Optional[Path]:
        return self.context.get("authoritative_pointer_write_path")

    @property
    def should_update_pointer(self) -> bool:
        return self.context.get("should_update_pointer", False)


def parse_cycle_field(val: Any) -> Optional[Tuple[int, int]]:
    """
    严格按完整格式解析周期字段 (cycle_name, target_cycle, cycle, active_cycle_inherited 等)。
    拒绝包含额外前后缀的非法字符串 (例如 'garbageW46suffix')。
    支持格式:
      'W45' -> (0, 45)
      '2026-W45' / '2026_W45' -> (2026, 45)
    """
    if not val or not isinstance(val, (str, int)):
        return None
    s = str(val).strip()
    m = re.fullmatch(r"(?:(?P<year>\d{4})[-_])?W(?P<week>\d+)", s, re.IGNORECASE)
    if m:
        year = int(m.group("year")) if m.group("year") else 0
        week = int(m.group("week"))
        return (year, week)
    return None


def parse_canonical_filename_cycle(filename: Any) -> Optional[Tuple[int, int]]:
    """
    严格按完整格式解析合法身价资产文件名中的周期。
    支持格式:
      'team_recent_xi_value.W45.json' -> (0, 45)
      'team_recent_xi_value.2026-W45_POSTMATCH_CANONICAL.json' -> (2026, 45)
      'team_recent_xi_value.W44_POSTMATCH_CANONICAL.json' -> (0, 44)
    若含有非法前缀/后缀 (如 'team_recent_xi_value.garbageW46suffix.json') 则返回 None。
    """
    if not filename or not isinstance(filename, (str, Path)):
        return None
    name = Path(filename).name.strip()
    m = re.fullmatch(
        r"team_recent_xi_value\.(?:(?P<year>\d{4})[-_])?W(?P<week>\d+)(?:_POSTMATCH_CANONICAL)?\.json",
        name,
        re.IGNORECASE
    )
    if m:
        year = int(m.group("year")) if m.group("year") else 0
        week = int(m.group("week"))
        return (year, week)
    return None


def parse_cycle_id(val: Any) -> Optional[Tuple[int, int]]:
    """
    解析周期标识为 (year, week) 元组。
    优先按严格周期字段格式匹配，若不匹配再按合法 canonical 文件名格式匹配。
    对于 'garbageW46suffix' 等格式非法字符串坚决返回 None。
    """
    res = parse_cycle_field(val)
    if res is not None:
        return res
    return parse_canonical_filename_cycle(val)


def compare_cycles(c1: Tuple[int, int], c2: Tuple[int, int]) -> int:
    """
    精确比较两个周期 c1 和 c2:
    - 返回 -1: c1 < c2
    - 返回 0:  c1 == c2
    - 返回 1:  c1 > c2
    如果两者年份均大于 0 且不相等，先比年份；
    若任一年份缺省 (0) 或年份相同，则按周次序号数值比较。
    """
    y1, w1 = c1
    y2, w2 = c2
    if y1 > 0 and y2 > 0 and y1 != y2:
        return -1 if y1 < y2 else 1
    if w1 < w2:
        return -1
    elif w1 > w2:
        return 1
    return 0


def verify_pointer_boundary(
    pointer_path: Path,
    canonical_dir: Path
) -> Tuple[bool, str, Optional[Path]]:
    """
    统一解析指针路径并强制执行获准资产目录边界与防逃逸校验。
    无论指针是显式指定还是自动发现的默认指针，均强制执行同等级安全约束。
    """
    if not pointer_path.is_file():
        return False, f"Pointer file '{pointer_path}' does not exist on filesystem.", None
    try:
        resolved = pointer_path.resolve()
        resolved.relative_to(canonical_dir)
    except (ValueError, RuntimeError):
        return (
            False,
            f"Out-of-boundary pointer: '{pointer_path}' resolves outside allowed directory '{canonical_dir}'.",
            None
        )
    return True, "VERIFIED", resolved


def verify_cycle_progression_and_baseline_continuity(
    cand_doc: Dict[str, Any],
    target_canonical_path: Path,
    target_pointer_path: Optional[Path] = None
) -> CycleGateResult:
    """
    写前生效周期、候选继承基线及目标周期的严格门禁校验 (Pre-write Cycle Progression & Baseline Continuity Gate)
    """
    target_canonical_path = Path(target_canonical_path)
    if target_canonical_path.is_symlink():
        return CycleGateResult(False, f"Canonical target '{target_canonical_path}' must not be a symbolic link.")
    canonical_dir = target_canonical_path.resolve().parent
    default_pointer_path = canonical_dir / "team_recent_xi_value.current.json"

    # 1. 统一周期校验 (Unify Cycle Validation)
    declared_cycles: Dict[str, Tuple[str, Tuple[int, int]]] = {}
    for key in ("cycle_name", "target_cycle", "cycle"):
        val = cand_doc.get(key)
        if val is not None and str(val).strip() != "":
            c_tuple = parse_cycle_field(val)
            if not c_tuple:
                return CycleGateResult(
                    False,
                    f"Unable to confirm target cycle: Invalid candidate cycle format in field '{key}': '{val}' cannot be parsed into a valid cycle."
                )
            declared_cycles[key] = (str(val).strip(), c_tuple)

    cand_cycle_tuple: Optional[Tuple[int, int]] = None
    cand_cycle_str: Optional[str] = None
    if declared_cycles:
        first_key, (first_str, first_tuple) = next(iter(declared_cycles.items()))
        for other_key, (other_str, other_tuple) in declared_cycles.items():
            if compare_cycles(first_tuple, other_tuple) != 0:
                return CycleGateResult(
                    False,
                    f"Conflicting cycle declarations within candidate: '{first_key}'='{first_str}' "
                    f"conflicts with '{other_key}'='{other_str}'."
                )
        cand_cycle_tuple = first_tuple
        cand_cycle_str = first_str

    file_cycle_tuple = parse_canonical_filename_cycle(target_canonical_path.name)
    if not file_cycle_tuple:
        return CycleGateResult(
            False,
            f"Unable to confirm target cycle from target filename '{target_canonical_path.name}'. Filename does not conform to canonical asset format."
        )

    if cand_cycle_tuple:
        if compare_cycles(cand_cycle_tuple, file_cycle_tuple) != 0:
            return CycleGateResult(
                False,
                f"Cycle conflict: Candidate declared cycle '{cand_cycle_str}' "
                f"does not match target filename cycle in '{target_canonical_path.name}'."
            )
        validated_cycle_tuple = cand_cycle_tuple
        validated_cycle_str = cand_cycle_str
    else:
        validated_cycle_tuple = file_cycle_tuple
        m = re.search(r"(?:(?P<year>\d{4})[-_])?W(?P<week>\d+)", target_canonical_path.name, re.IGNORECASE)
        validated_cycle_str = m.group(0).upper() if m else f"W{file_cycle_tuple[1]}"

    # 2. 锁定生效指针与历史基线状态 (Lock Authoritative Pointer)
    existing_canonicals: List[Path] = []
    if canonical_dir.exists():
        existing_canonicals = [
            f for f in canonical_dir.glob("team_recent_xi_value.*.json")
            if f.name != "team_recent_xi_value.current.json" and not f.name.startswith(".tmp_")
        ]

    default_pointer_exists = default_pointer_path.exists() or default_pointer_path.is_symlink()
    default_resolved_ptr: Optional[Path] = None

    # 统一门禁防线: 无论显式传入还是自动发现，默认指针若存在必须首先通过资产目录边界校验 (拒绝逃逸软链接)
    if default_pointer_exists:
        ok_def, msg_def, def_resolved = verify_pointer_boundary(default_pointer_path, canonical_dir)
        if not ok_def:
            return CycleGateResult(
                False,
                f"Default authoritative pointer boundary violation: {msg_def} "
                f"Refusing write before any assets are touched to prevent out-of-boundary escape."
            )
        default_resolved_ptr = def_resolved

    active_pointer_file: Optional[Path] = None
    authoritative_write_ptr: Optional[Path] = None
    should_update_pointer: bool = (target_pointer_path is not None)

    if target_pointer_path is not None:
        passed_ptr = Path(target_pointer_path)
        if default_pointer_exists:
            # 权威指针存在时，传入的指针路径必须存在且严格解析到同一指针文件
            if not passed_ptr.is_file():
                return CycleGateResult(
                    False,
                    f"Authoritative pointer '{default_pointer_path.name}' already exists in '{canonical_dir}', "
                    f"but passed alternative pointer '{passed_ptr}' does not exist on filesystem. "
                    f"Refusing write to prevent pointer divergence."
                )
            ok_passed, msg_passed, passed_resolved = verify_pointer_boundary(passed_ptr, canonical_dir)
            if not ok_passed:
                return CycleGateResult(False, msg_passed)
            if passed_resolved != default_resolved_ptr:
                return CycleGateResult(
                    False,
                    f"Authoritative pointer '{default_pointer_path.name}' already exists in '{canonical_dir}', "
                    f"but passed alternative pointer '{passed_ptr}' resolves to a different file ('{passed_resolved}'). "
                    f"Explicit pointer path must resolve to the authoritative pointer."
                )
            active_pointer_file = default_pointer_path
            authoritative_write_ptr = default_resolved_ptr
        else:
            if passed_ptr.is_file():
                ok_passed, msg_passed, passed_resolved = verify_pointer_boundary(passed_ptr, canonical_dir)
                if not ok_passed:
                    return CycleGateResult(False, msg_passed)
                active_pointer_file = passed_ptr
                authoritative_write_ptr = passed_resolved
            else:
                if passed_ptr.exists() or passed_ptr.is_symlink():
                    return CycleGateResult(False, f"Pointer path '{passed_ptr}' is not a readable file.")
                try:
                    passed_ptr.resolve().relative_to(canonical_dir)
                except (ValueError, RuntimeError):
                    return CycleGateResult(
                        False,
                        f"Out-of-boundary pointer: '{passed_ptr}' resolves outside allowed directory '{canonical_dir}'."
                    )
                if existing_canonicals:
                    return CycleGateResult(
                        False,
                        f"Pointer anomaly: Canonical assets exist in '{canonical_dir}' ({len(existing_canonicals)} assets), "
                        f"but neither authoritative pointer '{default_pointer_path.name}' nor specified pointer '{passed_ptr}' exists."
                    )
                active_pointer_file = None
                authoritative_write_ptr = default_pointer_path.resolve()
    else:
        # 省略 target_pointer_path: 遵循可选更新语义，只读检查已有默认指针，不写入更新指针
        if default_pointer_exists:
            active_pointer_file = default_pointer_path
            authoritative_write_ptr = None
        else:
            if existing_canonicals:
                return CycleGateResult(
                    False,
                    f"Pointer anomaly: Canonical assets exist in '{canonical_dir}' ({len(existing_canonicals)} assets), "
                    f"but default authoritative pointer '{default_pointer_path.name}' is missing."
                )
            active_pointer_file = None
            authoritative_write_ptr = None

    # 如果无活跃指针文件且无历史资产，视为合法首次冷启动发布
    if not active_pointer_file:
        return CycleGateResult(
            True,
            "INITIAL_PUBLICATION_APPROVED",
            {
                "validated_cycle_str": validated_cycle_str,
                "validated_cycle_tuple": validated_cycle_tuple,
                "active_pointer_file": None,
                "authoritative_pointer_write_path": authoritative_write_ptr,
                "should_update_pointer": should_update_pointer,
                "is_idempotent_applied": False
            }
        )

    # 读取并严格校验当前生效指针 (同时复用并归一化支持两套字段)
    try:
        ptr_data = json.loads(active_pointer_file.read_text(encoding="utf-8"))
    except Exception as e:
        return CycleGateResult(False, f"Failed to read active pointer file '{active_pointer_file}': {e}")

    # 1. 周期比对与两套字段一致性核验
    cycle_a = ptr_data.get("active_cycle")
    cycle_b = ptr_data.get("current_active_cycle")
    if not cycle_a and not cycle_b:
        return CycleGateResult(
            False,
            f"Pointer '{active_pointer_file}' lacks active cycle declaration "
            f"(neither 'active_cycle' nor 'current_active_cycle' found)."
        )

    tuple_a = parse_cycle_field(cycle_a) if cycle_a else None
    tuple_b = parse_cycle_field(cycle_b) if cycle_b else None
    if cycle_a and not tuple_a:
        return CycleGateResult(False, f"Pointer active_cycle '{cycle_a}' has invalid format.")
    if cycle_b and not tuple_b:
        return CycleGateResult(False, f"Pointer current_active_cycle '{cycle_b}' has invalid format.")

    if tuple_a and tuple_b:
        if compare_cycles(tuple_a, tuple_b) != 0:
            return CycleGateResult(
                False,
                f"Conflicting cycle declarations in pointer: 'active_cycle'='{cycle_a}' "
                f"conflicts with 'current_active_cycle'='{cycle_b}'."
            )

    current_cycle_tuple = tuple_a or tuple_b
    current_cycle_str = str(cycle_a or cycle_b).strip()

    # 2. 资产路径比对与两套字段一致性核验
    path_a = ptr_data.get("canonical_asset_relative_path")
    path_b = ptr_data.get("canonical_json_path")
    if not path_a and not path_b:
        return CycleGateResult(
            False,
            f"Pointer '{active_pointer_file}' lacks canonical asset path "
            f"(neither 'canonical_asset_relative_path' nor 'canonical_json_path' found)."
        )

    if path_a and path_b:
        norm_a = Path(path_a).name.strip()
        norm_b = Path(path_b).name.strip()
        if norm_a != norm_b or str(path_a).strip() != str(path_b).strip():
            return CycleGateResult(
                False,
                f"Conflicting canonical asset paths in pointer: 'canonical_asset_relative_path'='{path_a}' "
                f"conflicts with 'canonical_json_path'='{path_b}'."
            )

    ptr_ref_rel = str(path_a or path_b).strip()

    # 资产路径越界与安全防线 (拒绝绝对路径、拒绝 ../、拒绝逃逸符号链接)
    if Path(ptr_ref_rel).is_absolute():
        return CycleGateResult(
            False,
            f"Out-of-boundary asset reference in pointer: absolute path '{ptr_ref_rel}' is disallowed."
        )
    if ".." in Path(ptr_ref_rel).parts:
        return CycleGateResult(
            False,
            f"Out-of-boundary asset reference in pointer: path traversal '../' detected in '{ptr_ref_rel}'."
        )

    ptr_ref_asset_path = active_pointer_file.parent / ptr_ref_rel
    try:
        resolved_asset_path = ptr_ref_asset_path.resolve()
        resolved_asset_path.relative_to(canonical_dir)
    except (ValueError, RuntimeError):
        return CycleGateResult(
            False,
            f"Out-of-boundary asset reference in pointer: '{ptr_ref_rel}' resolves outside allowed directory '{canonical_dir}'."
        )

    ptr_ref_file_name = ptr_ref_asset_path.name
    ptr_ref_cycle_tuple = parse_canonical_filename_cycle(ptr_ref_file_name)
    if not ptr_ref_cycle_tuple:
        return CycleGateResult(
            False,
            f"Pointer referenced asset filename '{ptr_ref_file_name}' does not conform to canonical filename format."
        )

    # 验证权威指针声明周期与所指资产周期一致性
    if compare_cycles(current_cycle_tuple, ptr_ref_cycle_tuple) != 0:
        return CycleGateResult(
            False,
            f"Pointer internal divergence: Pointer declared active cycle '{current_cycle_str}' "
            f"conflicts with referenced asset filename cycle in '{ptr_ref_file_name}'. "
            f"Refusing promotion on corrupted pointer state."
        )

    # 3. SHA 两套字段比对与缺失拒绝 (在任何写入前坚决拦截)
    sha_a = ptr_data.get("canonical_asset_sha256")
    sha_b = ptr_data.get("canonical_json_sha256")
    if not sha_a and not sha_b:
        return CycleGateResult(
            False,
            f"Pointer '{active_pointer_file}' lacks verified SHA-256 declaration "
            f"(neither 'canonical_asset_sha256' nor 'canonical_json_sha256' found). "
            f"Refusing promotion to prevent unverified baseline inheritance."
        )

    if sha_a and sha_b:
        if str(sha_a).strip().lower() != str(sha_b).strip().lower():
            return CycleGateResult(
                False,
                f"Conflicting SHA-256 declarations in pointer: 'canonical_asset_sha256'='{sha_a}' "
                f"conflicts with 'canonical_json_sha256'='{sha_b}'."
            )

    recorded_ptr_sha = str(sha_a or sha_b).strip().lower()
    if not re.fullmatch(r"[0-9a-fA-F]{64}", recorded_ptr_sha):
        return CycleGateResult(
            False,
            f"Pointer contains invalid SHA-256 format: '{recorded_ptr_sha}'."
        )

    # 4. 物理资产存在性与实际物理 SHA 严格匹配
    if not ptr_ref_asset_path.is_file():
        return CycleGateResult(
            False,
            f"Pointer referenced asset file '{ptr_ref_file_name}' does not exist on disk in '{active_pointer_file.parent}'."
        )

    actual_ptr_ref_sha = hashlib.sha256(ptr_ref_asset_path.read_bytes()).hexdigest()
    if actual_ptr_ref_sha.lower() != recorded_ptr_sha.lower():
        return CycleGateResult(
            False,
            f"Pointer SHA mismatch: Pointer records SHA '{recorded_ptr_sha}' for '{ptr_ref_file_name}', "
            f"but physical file SHA is '{actual_ptr_ref_sha}'. Baseline asset has been tampered with or corrupted."
        )

    # 3. 保护既有目标 (Protect Existing Target)
    target_already_exists = target_canonical_path.is_file()

    if target_already_exists:
        # 目标文件已存在时，必须严格判定其发布身份。
        # 仅在同时满足以下条件时，方可返回 ALREADY_APPLIED:
        # 1. 活跃指针已存在且指向该目标文件 (ptr_ref_file_name == target_canonical_path.name)
        # 2. 指针声明周期与目标周期一致 (compare_cycles(current_cycle_tuple, validated_cycle_tuple) == 0)
        # 3. 指针中包含有效的 canonical_json_sha256，且与目标文件实际物理 SHA 严格一致 (缺失 SHA 时无法确认已应用)
        # 4. 候选核心业务内容与目标文件业务内容 100% 相同 (existing_biz == cand_biz)
        if not recorded_ptr_sha:
            return CycleGateResult(
                False,
                f"Target canonical file '{target_canonical_path.name}' already exists on disk, "
                f"but active pointer lacks verified 'canonical_json_sha256' (SHA missing or unconfirmed). "
                f"Refusing overwrite to protect pre-existing target."
            )

        if ptr_ref_file_name != target_canonical_path.name or compare_cycles(current_cycle_tuple, validated_cycle_tuple) != 0:
            return CycleGateResult(
                False,
                f"Target canonical file '{target_canonical_path.name}' already exists on disk, "
                f"but active pointer points to '{ptr_ref_file_name}' ({current_cycle_str}). "
                f"In-place overwrite of pre-existing canonical asset is rejected to protect existing target."
            )

        actual_target_bytes = target_canonical_path.read_bytes()
        actual_target_sha = hashlib.sha256(actual_target_bytes).hexdigest()
        if recorded_ptr_sha != actual_target_sha:
            return CycleGateResult(
                False,
                f"Target canonical file '{target_canonical_path.name}' already exists on disk, "
                f"but pointer SHA '{recorded_ptr_sha}' does not match file physical SHA '{actual_target_sha}'. "
                f"Refusing overwrite on corrupted asset."
            )

        try:
            existing_doc = json.loads(actual_target_bytes.decode("utf-8"))
        except Exception as e:
            return CycleGateResult(
                False,
                f"Target canonical file '{target_canonical_path.name}' already exists but failed JSON decode: {e}"
            )

        existing_biz = extract_valuation_business_content(existing_doc)
        cand_biz = extract_valuation_business_content(cand_doc)

        if existing_biz == cand_biz:
            return CycleGateResult(
                True,
                "IDEMPOTENT_RERUN_ALREADY_APPLIED",
                {
                    "validated_cycle_str": validated_cycle_str,
                    "validated_cycle_tuple": validated_cycle_tuple,
                    "active_pointer_file": active_pointer_file,
                    "authoritative_pointer_write_path": authoritative_write_ptr,
                    "should_update_pointer": should_update_pointer,
                    "is_idempotent_applied": True,
                    "canonical_sha256": actual_target_sha,
                    "size_bytes": len(actual_target_bytes)
                }
            )

        return CycleGateResult(
            False,
            f"Conflicting revision detected for already published cycle '{validated_cycle_str}'! "
            f"Canonical asset already exists at '{target_canonical_path.name}', but candidate business content "
            f"differs from published asset. In-place revisions of published canonical assets are disallowed. "
            f"Refusing overwrite."
        )

    # 4. 目标文件尚未存在时的向前演进与时序门禁
    cmp_res = compare_cycles(validated_cycle_tuple, current_cycle_tuple)

    # 4.1 倒退阻断
    if cmp_res < 0:
        return CycleGateResult(
            False,
            f"Cycle regression detected! Target cycle '{validated_cycle_str}' "
            f"is older than current active cycle '{current_cycle_str}'. "
            f"Refusing retrograde promotion."
        )

    # 提取候选继承基线
    inherited_str = cand_doc.get("active_cycle_inherited") or cand_doc.get("inherited_baseline_cycle")
    inherited_tuple = parse_cycle_field(inherited_str)

    # 4.2 合法向前发布 (target > current)
    if cmp_res > 0:
        if not inherited_tuple:
            return CycleGateResult(
                False,
                f"Forward promotion from '{current_cycle_str}' to '{validated_cycle_str}' blocked: "
                f"candidate missing or invalid 'active_cycle_inherited' baseline."
            )
        cmp_inherit = compare_cycles(inherited_tuple, current_cycle_tuple)
        if cmp_inherit != 0:
            return CycleGateResult(
                False,
                f"Baseline drift detected! Target cycle is '{validated_cycle_str}', "
                f"but candidate baseline '{inherited_str}' does not match current active cycle '{current_cycle_str}'."
            )
        return CycleGateResult(
            True,
            "FORWARD_PROMOTION_APPROVED",
            {
                "validated_cycle_str": validated_cycle_str,
                "validated_cycle_tuple": validated_cycle_tuple,
                "active_pointer_file": active_pointer_file,
                "authoritative_pointer_write_path": authoritative_write_ptr,
                "should_update_pointer": should_update_pointer,
                "is_idempotent_applied": False
            }
        )

    # 4.3 target == current，但目标文件不存在
    if cmp_res == 0:
        return CycleGateResult(
            False,
            f"Pointer divergence anomaly: Pointer indicates active cycle is '{current_cycle_str}', "
            f"but target canonical file '{target_canonical_path.name}' does not exist on disk."
        )

    return CycleGateResult(False, "UNKNOWN_CYCLE_RELATIONSHIP")


def promote_valuation_candidate_to_canonical(
    candidate_doc_or_path: Union[Dict[str, Any], Path, str],
    target_canonical_path: Path,
    target_pointer_path: Optional[Path] = None,
    is_production_authorized: bool = False,
    operator_approval_token: Optional[str] = None
) -> Dict[str, Any]:
    """
    Stage 6 身价候选资产权威晋升与生产持久化入口 (Fail-Closed Persistence Gate)
    """
    # 1. 载入候选文档
    if isinstance(candidate_doc_or_path, (str, Path)):
        cand_path = Path(candidate_doc_or_path)
        if not cand_path.exists():
            raise FileNotFoundError(f"PROMOTION_ABORT: Candidate file not found: {cand_path}")
        cand_doc = json.loads(cand_path.read_text(encoding="utf-8"))
    elif isinstance(candidate_doc_or_path, dict):
        if "candidate_file" in candidate_doc_or_path and "teams" not in candidate_doc_or_path:
            cf_path = Path(candidate_doc_or_path["candidate_file"])
            if cf_path.exists():
                cand_doc = json.loads(cf_path.read_text(encoding="utf-8"))
            else:
                cand_doc = candidate_doc_or_path
        else:
            cand_doc = candidate_doc_or_path
    else:
        raise TypeError("candidate_doc_or_path must be Path, str, or dict")

    # 2. 严格语义门禁核验 (调用 verify_squad_valuation_promotion_gate)
    gate_ok, gate_msg = verify_squad_valuation_promotion_gate(candidate_doc_or_path)
    if not gate_ok:
        raise PermissionError(f"PROMOTION_BLOCKED: {gate_msg}")

    # 3. 生产授权门禁
    target_canonical_path = Path(target_canonical_path)
    is_prod = ("AresVault" in str(target_canonical_path) or "02_Team_Archives" in str(target_canonical_path))
    if is_prod and not is_production_authorized:
        raise PermissionError(
            f"STAGE6_UNAUTHORIZED: Write to production path {target_canonical_path} blocked without explicit Stage 6 authorization."
        )

    # 4. 写前生效周期、候选继承基线及指针锁定门禁核验 (Pre-write Cycle Progression & Baseline Continuity Gate)
    gate_res = verify_cycle_progression_and_baseline_continuity(
        cand_doc=cand_doc,
        target_canonical_path=target_canonical_path,
        target_pointer_path=target_pointer_path
    )
    if not gate_res[0]:
        raise PermissionError(f"PROMOTION_BLOCKED: {gate_res[1]}")

    validated_cycle_str = getattr(gate_res, "validated_cycle_str", None) or cand_doc.get("cycle_name")
    is_idempotent_applied = getattr(gate_res, "is_idempotent_applied", False)

    # 4.1 幂等重跑短路返回 (0 写入，保持原资产与指针 100% 字节不变)
    if is_idempotent_applied:
        canonical_bytes = target_canonical_path.read_bytes()
        canonical_sha = hashlib.sha256(canonical_bytes).hexdigest()
        pointer_res = None
        if target_pointer_path is not None:
            ptr_file = getattr(gate_res, "authoritative_pointer_write_path", None) or getattr(gate_res, "active_pointer_file", None) or target_pointer_path
            if ptr_file and Path(ptr_file).is_file():
                ptr_bytes = Path(ptr_file).read_bytes()
                pointer_res = {
                    "pointer_path": str(ptr_file),
                    "pointer_sha256": hashlib.sha256(ptr_bytes).hexdigest()
                }
        return {
            "status": "ALREADY_APPLIED",
            "action": "IDEMPOTENT_SKIPPED",
            "canonical_path": str(target_canonical_path),
            "canonical_sha256": canonical_sha,
            "size_bytes": len(canonical_bytes),
            "pointer_update": pointer_res,
            "note": "Candidate business content matches published canonical and pointer; idempotent rerun skipped with 0 mutations."
        }

    # 5. 准备持久化资产 (确保 cycle_name 为经过严格统一校验的周期)
    published_doc = dict(cand_doc)
    if validated_cycle_str:
        published_doc["cycle_name"] = validated_cycle_str
    published_doc["production_status"] = "PRODUCTION_AUTHORIZED"
    published_doc["pipeline_status"] = "CANONICAL_PUBLISHED"
    published_doc["canonical_persistence_status"] = "PUBLISHED_ACTIVE"
    published_doc["promoted_at_utc"] = datetime.now(timezone.utc).isoformat()
    if operator_approval_token:
        published_doc["operator_approval_token"] = operator_approval_token

    target_canonical_path.parent.mkdir(parents=True, exist_ok=True)
    data_bytes = json.dumps(published_doc, indent=2, ensure_ascii=False).encode("utf-8")

    # 6. 原子替换写入
    ts = int(time.time() * 1000)
    tmp_file = target_canonical_path.parent / f".tmp_{target_canonical_path.name}_{os.getpid()}_{ts}"
    try:
        with open(tmp_file, "wb") as f:
            f.write(data_bytes)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_file, target_canonical_path)
    finally:
        if tmp_file.exists():
            tmp_file.unlink()

    published_sha = hashlib.sha256(data_bytes).hexdigest()

    # 7. 指针更新 (遵循 target_pointer_path 的可选更新语义与权威路径原子写入)
    pointer_res = None
    should_update_pointer = getattr(gate_res, "should_update_pointer", False)
    authoritative_write_ptr = getattr(gate_res, "authoritative_pointer_write_path", None)
    if not authoritative_write_ptr and target_pointer_path:
        authoritative_write_ptr = Path(target_pointer_path).resolve()

    if target_pointer_path is not None and should_update_pointer and authoritative_write_ptr:
        ptr_path = Path(authoritative_write_ptr)
        ptr_path.parent.mkdir(parents=True, exist_ok=True)
        pointer_doc = {
            "current_active_cycle": validated_cycle_str,
            "active_cycle": validated_cycle_str,
            "canonical_json_path": target_canonical_path.name,
            "canonical_asset_relative_path": target_canonical_path.name,
            "canonical_json_sha256": published_sha,
            "canonical_asset_sha256": published_sha,
            "teams_count": len(published_doc.get("teams", [])),
            "readiness": "READY_FOR_NEXT_POSTMATCH_CYCLE",
            "published_at_utc": published_doc["promoted_at_utc"]
        }
        ptr_bytes = json.dumps(pointer_doc, indent=2, ensure_ascii=False).encode("utf-8")
        tmp_ptr = ptr_path.parent / f".tmp_{ptr_path.name}_{os.getpid()}_{ts}"
        try:
            with open(tmp_ptr, "wb") as f:
                f.write(ptr_bytes)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_ptr, ptr_path)
        finally:
            if tmp_ptr.exists():
                tmp_ptr.unlink()
        pointer_res = {
            "pointer_path": str(ptr_path),
            "pointer_sha256": hashlib.sha256(ptr_bytes).hexdigest()
        }

    return {
        "status": "PROMOTED_SUCCESS",
        "canonical_path": str(target_canonical_path),
        "canonical_sha256": published_sha,
        "size_bytes": len(data_bytes),
        "pointer_update": pointer_res
    }

