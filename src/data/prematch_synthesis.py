import argparse
import json
import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from audit_router import load_dotenv_into_env, normalize_vault_path


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("AresTelemetry.PrematchSynthesis")

SUPPORTED_LLM_PROVIDERS = {"openai", "gemini", "deepseek"}
RIVALRY_TEAM_PAIRS = {
    ("manchesterunited", "liverpool"),
    ("liverpool", "manchesterunited"),
    ("arsenal", "tottenhamhotspur"),
    ("tottenhamhotspur", "arsenal"),
    ("realmadrid", "barcelona"),
    ("barcelona", "realmadrid"),
    ("atleticomadrid", "realmadrid"),
    ("realmadrid", "atleticomadrid"),
    ("intermilan", "acmilan"),
    ("acmilan", "intermilan"),
}

BRAND_POWER_LEVEL = {
    "low": 1,
    "medium": 2,
    "medium_high": 3,
    "high": 4,
    "elite": 5,
}

EXPECTED_HOME_PRICE_ANCHOR = {
    "elite": {"default": (1.25, 1.40), "defensive_mid_table": (1.35, 1.48)},
    "high": {"default": (1.35, 1.48), "defensive_mid_table": (1.42, 1.55)},
    "medium_high": {"default": (1.45, 1.65), "defensive_mid_table": (1.55, 1.75)},
    "medium": {"default": (1.65, 1.90), "defensive_mid_table": (1.70, 1.95)},
    "low": {"default": (1.85, 2.20), "defensive_mid_table": (1.95, 2.30)},
}


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    txt = _safe_text(text)
    if not txt:
        return None
    try:
        parsed = json.loads(txt)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    start = txt.find("{")
    end = txt.rfind("}")
    if start >= 0 and end > start:
        snippet = txt[start : end + 1]
        try:
            parsed = json.loads(snippet)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return None
    return None


def _extract_section_bullets(markdown_text: str, heading: str) -> List[str]:
    marker = f"## {heading}"
    idx = markdown_text.find(marker)
    if idx < 0:
        return []
    tail = markdown_text[idx + len(marker) :]
    next_heading = tail.find("\n## ")
    body = tail if next_heading < 0 else tail[:next_heading]
    lines = [_safe_text(line) for line in body.splitlines()]
    values: List[str] = []
    for line in lines:
        if line.startswith("- `") and line.endswith("`"):
            values.append(line[3:-1])
        elif line.startswith("- "):
            values.append(line[2:].strip("` "))
    return [value for value in values if value and value.lower() != "none"]


def _parse_first_float(text: str) -> Optional[float]:
    m = re.search(r"-?\d+(?:\.\d+)?", _safe_text(text))
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_gate_lookup(gate_snapshot: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    rows = gate_snapshot.get("rows") if isinstance(gate_snapshot.get("rows"), list) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        index = row.get("index")
        match_name = _safe_text(row.get("match"))
        if index is not None:
            lookup[f"idx:{index}"] = row
        if match_name:
            lookup[f"match:{match_name}"] = row
    return lookup


def _safe_gap(edge_a: Any, edge_b: Any) -> Optional[float]:
    if not isinstance(edge_a, (int, float)) or not isinstance(edge_b, (int, float)):
        return None
    return abs(float(edge_a) - float(edge_b))


def _normalize_match_key(value: str) -> str:
    return re.sub(r"\s+", " ", _safe_text(value).lower()).strip()


def _normalize_team_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", _safe_text(value).lower())


def _split_match_english(value: str) -> Optional[Dict[str, str]]:
    txt = _safe_text(value)
    if " vs " not in txt:
        return None
    home, away = [x.strip() for x in txt.split(" vs ", 1)]
    if not home or not away:
        return None
    return {"home": home, "away": away}


def _has_confirmed_absence(nodes: List[Any]) -> bool:
    if not isinstance(nodes, list):
        return False
    signals = (
        "out",
        "injur",
        "suspend",
        "acl",
        "meniscus",
        "sidelined",
        "absence",
        "缺阵",
        "伤",
        "停赛",
    )
    for raw in nodes:
        text = _safe_text(raw).lower()
        if text and any(sig in text for sig in signals):
            return True
    return False


def _parse_simple_scalar(raw: str) -> Any:
    txt = _safe_text(raw)
    low = txt.lower()
    if low in {"true", "yes"}:
        return True
    if low in {"false", "no"}:
        return False
    return txt


def _parse_gate_override_markdown(path: Path) -> Dict[str, Any]:
    lines = path.read_text(encoding="utf-8").splitlines()
    data: Dict[str, Any] = {}
    current_list_key: Optional[str] = None
    for raw in lines:
        line = raw.rstrip()
        if not line.strip():
            current_list_key = None
            continue
        if line.lstrip().startswith("- "):
            if current_list_key:
                data.setdefault(current_list_key, []).append(_safe_text(line.lstrip()[2:]))
            continue
        current_list_key = None
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        k = _safe_text(key)
        v = _safe_text(value)
        if not k:
            continue
        if v == "":
            data[k] = []
            current_list_key = k
        else:
            data[k] = _parse_simple_scalar(v)
    return data


class PrematchSynthesis:
    def __init__(
        self,
        issue: str,
        force_rule: bool = False,
        output_dir: Optional[Path] = None,
        stdout_only: bool = False,
        top5_only: bool = False,
        ops_mode: bool = False,
        include_rejected_caution: bool = False,
    ):
        self.issue = str(issue)
        self.force_rule = force_rule
        self.stdout_only = stdout_only
        self.top5_only = top5_only
        self.ops_mode = ops_mode or (_safe_text(os.getenv("ARES_SYNTHESIS_PROFILE")).lower() == "ops")
        self.include_rejected_caution = include_rejected_caution
        self.repo_root = Path(__file__).resolve().parent.parent.parent
        load_dotenv_into_env(self.repo_root)

        vault_env = _safe_text(os.getenv("ARES_VAULT_PATH"))
        if not vault_env:
            raise EnvironmentError("未检测到 ARES_VAULT_PATH，无法生成 Vault 最终收口报告。")
        self.vault_root = Path(normalize_vault_path(vault_env)).expanduser()

        self.issue_root = self.vault_root / "03_Match_Audits" / self.issue
        self.review_dir = self.issue_root / "03_Review_Reports"
        self.gate_override_dir = self.issue_root / "00_Gate"
        self.prematch_dir = self.issue_root / "01_Prematch_Audits"
        self.manifest_path = (
            self.vault_root / "04_RAG_Raw_Data" / "Cold_Data_Lake" / f"{self.issue}_dispatch_manifest.json"
        )
        self.diagnostics_path = self.issue_root / f"Audit-{self.issue}-team-diagnostics.json"
        self.gate_json_path = self.review_dir / f"REVIEW-{self.issue}-Prematch_Input_Gate.json"
        self.review_quality_path = self.review_dir / f"REVIEW-{self.issue}-Prematch_Data_Quality.md"
        if output_dir is not None:
            out_root = output_dir.expanduser().resolve()
        else:
            out_root = self.issue_root / "02_Special_Analyses"
        suffix = "-Top5" if self.top5_only else ""
        self.out_md_path = out_root / f"FINAL-{self.issue}-Prematch_Synthesis{suffix}.md"
        self.out_json_path = out_root / f"FINAL-{self.issue}-Prematch_Synthesis{suffix}.json"

        llm_switch_raw = _safe_text(os.getenv("ARES_USE_LLM_SYNTHESIS"))
        provider = _safe_text(os.getenv("ARES_LLM_PROVIDER")).lower()
        if not provider:
            if _safe_text(os.getenv("DEEPSEEK_API_KEY")) and not _safe_text(os.getenv("OPENAI_API_KEY")):
                provider = "deepseek"
            elif _safe_text(os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")) and not _safe_text(
                os.getenv("OPENAI_API_KEY")
            ):
                provider = "gemini"
            else:
                provider = "openai"
        if provider not in SUPPORTED_LLM_PROVIDERS:
            provider = "openai"
        self.llm_provider = provider

        common_api_key = _safe_text(os.getenv("ARES_LLM_API_KEY"))
        if provider == "gemini":
            provider_api_key = _safe_text(os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY"))
            default_base_url = "https://generativelanguage.googleapis.com/v1beta"
            default_model = "gemini-1.5-flash"
        elif provider == "deepseek":
            provider_api_key = _safe_text(os.getenv("DEEPSEEK_API_KEY"))
            default_base_url = "https://api.deepseek.com"
            default_model = "deepseek-v4-pro"
        else:
            provider_api_key = _safe_text(os.getenv("OPENAI_API_KEY"))
            default_base_url = "https://api.openai.com/v1"
            default_model = "gpt-4o-mini"

        self.llm_api_key = common_api_key or provider_api_key
        self.llm_base_url = _safe_text(os.getenv("ARES_LLM_BASE_URL")) or default_base_url
        self.llm_base_url = self.llm_base_url.rstrip("/")
        self.llm_model = _safe_text(os.getenv("ARES_LLM_MODEL")) or default_model
        self.llm_timeout_sec = int(_safe_text(os.getenv("ARES_LLM_TIMEOUT_SEC")) or "30")
        if llm_switch_raw:
            self.llm_enabled = llm_switch_raw.lower() in {"1", "true", "yes", "on"}
        else:
            # 自动模式：只要发现可用 key 即默认启用综合 LLM
            self.llm_enabled = bool(self.llm_api_key)

    def _llm_available(self) -> bool:
        return bool(self.llm_enabled and self.llm_api_key and self.llm_model and not self.force_rule)

    @staticmethod
    def _load_json(path: Path) -> Dict[str, Any]:
        return json.loads(path.read_text(encoding="utf-8"))

    def _load_gate_overrides(self) -> Dict[str, Dict[str, Any]]:
        lookup: Dict[str, Dict[str, Any]] = {}
        if not self.gate_override_dir.exists():
            return lookup
        for path in sorted(self.gate_override_dir.glob("REVIEW-*.md")):
            try:
                payload = _parse_gate_override_markdown(path)
            except Exception:
                continue
            match_name = _safe_text(payload.get("match"))
            if not match_name:
                continue
            lookup[_normalize_match_key(match_name)] = payload
            if "vs" in match_name:
                parts = [x.strip() for x in match_name.split("vs", 1)]
                if len(parts) == 2 and parts[0] and parts[1]:
                    lookup[_normalize_match_key(f"{parts[0]} vs {parts[1]}")] = payload
        return lookup

    def _load_inputs(self) -> Dict[str, Any]:
        if not self.review_dir.exists():
            raise FileNotFoundError(f"目录不存在: {self.review_dir}")
        manifest = self._load_json(self.manifest_path) if self.manifest_path.exists() else {}
        diagnostics = self._load_json(self.diagnostics_path) if self.diagnostics_path.exists() else {}
        gate_snapshot = self._load_json(self.gate_json_path) if self.gate_json_path.exists() else {}
        gate_overrides = self._load_gate_overrides()
        quality_text = self.review_quality_path.read_text(encoding="utf-8") if self.review_quality_path.exists() else ""
        gate_lookup = _build_gate_lookup(gate_snapshot)
        gate_rows = gate_snapshot.get("rows") if isinstance(gate_snapshot.get("rows"), list) else []
        selected_gate_indices = {
            int(idx)
            for idx in (gate_snapshot.get("selected_match_indices") or [])
            if isinstance(idx, int) or (isinstance(idx, str) and idx.isdigit())
        }
        manifest_matches = manifest.get("matches") if isinstance(manifest.get("matches"), list) else []
        top5_indices: set = set()
        if self.top5_only:
            top5_leagues = {"EPL", "La_liga", "Bundesliga", "Serie_A", "Ligue_1"}
            for row in manifest_matches:
                try:
                    idx = int(row.get("index"))
                except Exception:
                    continue
                if _safe_text(row.get("league")) in top5_leagues:
                    top5_indices.add(idx)

        accepted_files = set(_extract_section_bullets(quality_text, "Accepted Prematch Reports"))
        low_conf_files = set(_extract_section_bullets(quality_text, "Low Confidence Reports"))
        insufficient_files = set(_extract_section_bullets(quality_text, "Insufficient Resilience Data"))
        rejected_files = {
            path.name.replace("REJECTED-", "", 1)
            for path in self.review_dir.glob(f"REJECTED-Audit-{self.issue}-*.md")
        }

        if selected_gate_indices:
            gate_files = set()
            for idx in sorted(selected_gate_indices):
                matched = sorted(self.prematch_dir.glob(f"Audit-{self.issue}-{idx:02d}-*.md"))
                if matched:
                    gate_files.add(matched[0].name)
                elif self.include_rejected_caution:
                    rejected = sorted(self.review_dir.glob(f"REJECTED-Audit-{self.issue}-{idx:02d}-*.md"))
                    if rejected:
                        gate_files.add(rejected[0].name.replace("REJECTED-", "", 1))
            if self.include_rejected_caution:
                gate_files.update(path.name for path in self.prematch_dir.glob(f"Audit-{self.issue}-*.md"))
                gate_files.update(
                    path.name.replace("REJECTED-", "", 1)
                    for path in self.review_dir.glob(f"REJECTED-Audit-{self.issue}-*.md")
                )
            if gate_files:
                accepted_files = gate_files
        if not accepted_files and self.prematch_dir.exists():
            accepted_files = {path.name for path in self.prematch_dir.glob("Audit-*.md")}
        # 若 accepted 为空但存在 REJECTED 记录，自动回退为 CAUTION 输入，避免 matches=0。
        auto_include_rejected_caution = False
        if not accepted_files and rejected_files:
            accepted_files = set(rejected_files)
            auto_include_rejected_caution = True
        if self.top5_only:
            filtered: List[str] = []
            for filename in sorted(accepted_files):
                m = re.search(rf"Audit-{re.escape(self.issue)}-(\d+)-", filename)
                if not m:
                    continue
                idx = int(m.group(1))
                if idx in top5_indices:
                    filtered.append(filename)
            accepted_files = set(filtered)
            low_conf_files = {f for f in low_conf_files if f in accepted_files}
            insufficient_files = {f for f in insufficient_files if f in accepted_files}

        match_payloads: List[Dict[str, Any]] = []
        diagnostics_teams = diagnostics.get("teams") if isinstance(diagnostics.get("teams"), list) else []
        team_profiles: Dict[str, Dict[str, Any]] = {}
        for team_row in diagnostics_teams:
            if not isinstance(team_row, dict):
                continue
            team_name = _safe_text(team_row.get("team"))
            if not team_name:
                continue
            team_profiles[_normalize_team_key(team_name)] = {
                "team": team_name,
                "team_class_hint": _safe_text(team_row.get("team_class_hint")) or "standard",
                "injured_nodes": team_row.get("injured_nodes") if isinstance(team_row.get("injured_nodes"), list) else [],
                "suspended_nodes": team_row.get("suspended_nodes") if isinstance(team_row.get("suspended_nodes"), list) else [],
                "conversion_efficiency": _safe_float(team_row.get("conversion_efficiency")),
                "avg_xG_last_5": _safe_float(team_row.get("avg_xG_last_5")),
                "defensive_leakage": _safe_float(team_row.get("defensive_leakage")),
            }
        for filename in sorted(accepted_files):
            path = self.prematch_dir / filename
            rejected_caution = False
            if not path.exists():
                if not self.include_rejected_caution and not auto_include_rejected_caution:
                    continue
                rejected_path = self.review_dir / f"REJECTED-{filename}"
                if not rejected_path.exists():
                    continue
                path = rejected_path
                rejected_caution = True
            parsed = self._parse_prematch_audit(path)
            filtered_caution = (
                self.include_rejected_caution
                and bool(selected_gate_indices)
                and isinstance(parsed.get("match_index"), int)
                and int(parsed.get("match_index")) not in selected_gate_indices
            )
            gate_row = None
            if parsed.get("match_index") is not None:
                gate_row = gate_lookup.get(f"idx:{parsed.get('match_index')}")
            if gate_row is None and _safe_text(parsed.get("match")):
                gate_row = gate_lookup.get(f"match:{_safe_text(parsed.get('match'))}")
            match_key = _normalize_match_key(_safe_text(parsed.get("match")))
            cn_match_key = _normalize_match_key(_safe_text(parsed.get("cn_match")))
            override = gate_overrides.get(match_key) or gate_overrides.get(cn_match_key) or {}
            parsed["gate_row"] = gate_row
            readiness_level = _safe_text((gate_row or {}).get("prematch_readiness_level")).upper() or "READY"
            gate_status_override = _safe_text(override.get("gate_status")).upper()
            if gate_status_override in {"READY", "HOLD", "BLOCKED"}:
                readiness_level = gate_status_override
            if rejected_caution or filtered_caution:
                readiness_level = "HOLD"
            parsed["readiness_level"] = readiness_level
            parsed["ready_level"] = _safe_text(override.get("ready_level")).upper()
            parsed["confidence_cap"] = _safe_text(override.get("confidence_cap")).lower()
            parsed["prematch_allowed"] = bool(override.get("prematch_allowed")) if "prematch_allowed" in override else None
            parsed["required_controls"] = (
                override.get("required_prematch_controls")
                if isinstance(override.get("required_prematch_controls"), list)
                else []
            )
            parsed["is_low_confidence"] = (
                bool((gate_row or {}).get("soft_blockers"))
                or _safe_text((gate_row or {}).get("quality_tag")) == "DATA_WEAK"
                or (filename in low_conf_files and gate_row is None)
                or rejected_caution
                or filtered_caution
            )
            parsed["is_insufficient_resilience"] = bool((gate_row or {}).get("has_resilience_gap")) or (
                filename in insufficient_files and gate_row is None
            ) or rejected_caution
            parsed["is_structural_data_gap"] = bool((gate_row or {}).get("has_structural_data_gap"))
            caution_tags = []
            if rejected_caution:
                caution_tags.append("rejected_caution")
            if filtered_caution:
                caution_tags.append("gate_filtered_caution")
            parsed["risk_tags"] = sorted(set((parsed.get("risk_tags") or []) + caution_tags))
            parsed["home_profile"] = team_profiles.get(_normalize_team_key(parsed.get("home_team")))
            parsed["away_profile"] = team_profiles.get(_normalize_team_key(parsed.get("away_team")))
            match_payloads.append(parsed)

        if gate_rows:
            def _row_in_scope(row: Dict[str, Any]) -> bool:
                try:
                    row_index = int(row.get("index"))
                except Exception:
                    return not self.top5_only
                if self.top5_only and row_index not in top5_indices:
                    return False
                return _safe_text(row.get("ready")).lower() == "yes"

            low_conf_count = sum(
                1
                for row in gate_rows
                if isinstance(row, dict)
                and _row_in_scope(row)
                and (
                    _safe_text(row.get("quality_tag")) == "DATA_WEAK"
                    or bool(row.get("soft_blockers"))
                    or bool(row.get("has_structural_data_gap"))
                )
            )
            insufficient_count = sum(
                1
                for row in gate_rows
                if isinstance(row, dict)
                and _row_in_scope(row)
                and bool(row.get("has_resilience_gap"))
            )
        else:
            low_conf_count = len(low_conf_files)
            insufficient_count = len(insufficient_files)

        return {
            "manifest": manifest,
            "diagnostics": diagnostics,
            "gate_snapshot": gate_snapshot,
            "quality_text": quality_text,
            "matches": match_payloads,
            "low_conf_count": low_conf_count,
            "insufficient_count": insufficient_count,
            "top5_mode": self.top5_only,
            "team_profiles": team_profiles,
        }

    def _parse_prematch_audit(self, path: Path) -> Dict[str, Any]:
        text = path.read_text(encoding="utf-8")
        match_index = None
        m_idx = re.search(rf"Audit-{re.escape(self.issue)}-(\d+)-", path.name)
        if m_idx:
            try:
                match_index = int(m_idx.group(1))
            except Exception:
                match_index = None
        title = _safe_text(
            next(
                (
                    line
                    for line in text.splitlines()
                    if line.startswith("# Ares Prematch Audit - Issue ")
                ),
                next((line for line in text.splitlines() if line.startswith("# ")), ""),
            )
        )
        title_match = re.search(r"# Ares Prematch Audit - Issue (.+?) - (.+?) vs (.+)$", title)
        home_team = _safe_text(title_match.group(2)) if title_match else ""
        away_team = _safe_text(title_match.group(3)) if title_match else ""

        if not (home_team and away_team):
            m_en = re.search(r"- 英文对阵:\s*`([^`]+)\s+vs\s+([^`]+)`", text)
            if m_en:
                home_team = _safe_text(m_en.group(1))
                away_team = _safe_text(m_en.group(2))

        cn_match = ""
        m_cn = re.search(r"- 中文对阵:\s*`([^`]+)`", text)
        if m_cn:
            cn_match = _safe_text(m_cn.group(1))

        mapping_source = ""
        m_source = re.search(r"- 映射来源:\s*`([^`]+)`", text)
        if m_source:
            mapping_source = _safe_text(m_source.group(1)).lower()

        understat_id = ""
        m_understat = re.search(r"- Understat ID:\s*`([^`]+)`", text)
        if m_understat:
            understat_id = _safe_text(m_understat.group(1))

        odds = {"home": None, "draw": None, "away": None}
        m_odds = re.search(r"- 最新欧赔:\s*主\s*`([^`]+)`\s*/\s*平\s*`([^`]+)`\s*/\s*客\s*`([^`]+)`", text)
        if m_odds:
            odds = {
                "home": _parse_first_float(m_odds.group(1)),
                "draw": _parse_first_float(m_odds.group(2)),
                "away": _parse_first_float(m_odds.group(3)),
            }

        team_sections: List[Dict[str, Any]] = []
        current_team: Optional[Dict[str, Any]] = None
        for raw_line in text.splitlines():
            line = raw_line.strip()
            m_header = re.match(r"## (Home|Away) - ([^\n]+)$", line)
            if m_header:
                if current_team:
                    team_sections.append(current_team)
                current_team = {
                    "side": _safe_text(m_header.group(1)),
                    "team": _safe_text(m_header.group(2)),
                    "s_dynamic": None,
                    "conclusion": "",
                    "decision": "",
                    "market_prob": None,
                    "model_prob": None,
                }
                continue
            if not current_team or not line.startswith("- "):
                continue
            m_sd = re.match(r"- S_dynamic:\s*`([^`]+)`", line)
            if m_sd:
                current_team["s_dynamic"] = _parse_first_float(m_sd.group(1))
                continue
            m_con = re.match(r"- Prematch 结论:\s*`([^`]+)`", line)
            if m_con:
                current_team["conclusion"] = _safe_text(m_con.group(1))
                continue
            m_decision = re.match(r"- 决策:\s*(.+)", line)
            if m_decision:
                current_team["decision"] = _safe_text(m_decision.group(1))
                continue
            m_ev = re.match(r"- EV:\s*`[^`]*`\s*\|\s*市场\s*`([^`]+)`\s*/\s*模型\s*`([^`]+)`", line)
            if m_ev:
                current_team["market_prob"] = _parse_first_float(m_ev.group(1))
                current_team["model_prob"] = _parse_first_float(m_ev.group(2))
        if current_team:
            team_sections.append(current_team)

        risk_tags: List[str] = []
        for m_tag in re.finditer(
            r"(?mi)^\s*-\s*([a-z0-9_]+(?:exposure|variance|urgency|escape|survival|reversal|motivation|rival)[a-z0-9_]*)\s*$",
            text,
        ):
            token = _safe_text(m_tag.group(1)).lower()
            if token:
                risk_tags.append(token)

        text_l = text.lower()
        survival_escape_signal = any(
            token in text_l
            for token in (
                "win_moves_team_out_of_relegation_zone: true",
                "immediate_table_escape_condition",
                "survival_urgency: extreme",
                "relegation_zone",
                "降级区",
                "保级",
            )
        )

        return {
            "file": path.name,
            "match_index": match_index,
            "match": f"{home_team} vs {away_team}" if home_team and away_team else "",
            "home_team": home_team,
            "away_team": away_team,
            "cn_match": cn_match,
            "mapping_source": mapping_source,
            "understat_id": understat_id,
            "odds": odds,
            "teams": team_sections,
            "risk_tags": sorted(set(risk_tags)),
            "survival_escape_signal": survival_escape_signal,
        }

    @staticmethod
    def _confidence_bucket(score: float) -> str:
        if score >= 5.0:
            return "high"
        if score >= 2.5:
            return "medium"
        return "low"

    @staticmethod
    def _candidate_score(verdict: Dict[str, Any]) -> float:
        suggestion = _safe_text(verdict.get("suggestion")).lower()
        confidence = _safe_text(verdict.get("confidence")).lower()
        is_low_conf = bool(verdict.get("is_low_confidence"))
        is_insufficient = bool(verdict.get("is_insufficient_resilience"))
        readiness_level = _safe_text(verdict.get("readiness_level")).upper() or "READY"
        best_edge = verdict.get("best_edge")
        best_edge = float(best_edge) if isinstance(best_edge, (int, float)) else None

        score = 0.0
        if suggestion in {"3", "0"}:
            score += 3.0
        elif suggestion in {"3/1", "1/0", "3/0"}:
            score += 1.8
        elif suggestion == "1":
            score += 1.2
        elif suggestion == "skip":
            score -= 1.0

        if confidence == "high":
            score += 3.0
        elif confidence == "medium":
            score += 1.5

        # 数据侧惩罚：韧性不足优先级最高
        if is_insufficient:
            score -= 2.2
        if is_low_conf:
            score -= 1.5
        if readiness_level == "HOLD":
            score -= 1.2
        elif readiness_level == "BLOCKED":
            score -= 2.5
        if best_edge is not None and best_edge < 0:
            score -= 1.0

        return round(score, 2)

    @staticmethod
    def _watchlist_upgrade_reason(verdict: Dict[str, Any]) -> Optional[str]:
        suggestion = _safe_text(verdict.get("suggestion")).lower()
        readiness_level = _safe_text(verdict.get("readiness_level")).upper() or "READY"
        if readiness_level != "READY":
            return None
        if bool(verdict.get("is_low_confidence")) or bool(verdict.get("is_insufficient_resilience")):
            return None
        best_edge = verdict.get("best_edge")
        edge_home = verdict.get("edge_home")
        edge_away = verdict.get("edge_away")
        edge_gap = _safe_gap(edge_home, edge_away)
        if not isinstance(best_edge, (int, float)) or not isinstance(edge_gap, (int, float)):
            return None
        if best_edge < -9.0:
            return None
        if suggestion not in {"1/0", "3/1"}:
            return None
        if edge_gap < 4.5:
            return None
        return f"观察升级：best_edge={float(best_edge):+.1f}pp，主客差={float(edge_gap):.1f}pp，允许低仓位试探。"

    @classmethod
    def _candidate_tier(cls, verdict: Dict[str, Any]) -> str:
        suggestion = _safe_text(verdict.get("suggestion")).lower()
        score = cls._candidate_score(verdict)
        is_low_conf = bool(verdict.get("is_low_confidence"))
        is_insufficient = bool(verdict.get("is_insufficient_resilience"))
        readiness_level = _safe_text(verdict.get("readiness_level")).upper() or "READY"
        best_edge = verdict.get("best_edge")
        best_edge = float(best_edge) if isinstance(best_edge, (int, float)) else None
        if suggestion in {"3", "0"} and score >= 4.0 and readiness_level == "READY" and not is_low_conf and not is_insufficient:
            return "稳胆"
        if suggestion != "skip" and score >= 1.5 and readiness_level == "READY" and best_edge is not None and best_edge >= 0:
            return "博弈"
        if (
            suggestion in {"1", "3/1", "1/0"}
            and readiness_level in {"READY", "HOLD"}
            and not is_low_conf
            and score >= -0.5
        ):
            return "博弈"
        if suggestion != "skip" and readiness_level in {"READY", "HOLD"} and not is_insufficient and score >= -0.5:
            return "博弈"
        if cls._watchlist_upgrade_reason(verdict):
            return "博弈"
        if suggestion != "skip" and readiness_level in {"READY", "HOLD"}:
            return "观察"
        return "放弃"

    @classmethod
    def _build_candidate_board(cls, verdicts: List[Dict[str, Any]]) -> Dict[str, Any]:
        tiers: Dict[str, List[Dict[str, Any]]] = {"稳胆": [], "博弈": [], "观察": [], "放弃": []}
        ranked_items: List[Dict[str, Any]] = []
        for row in verdicts:
            score = cls._candidate_score(row)
            tier = cls._candidate_tier(row)
            item = {
                "match": _safe_text(row.get("match")),
                "cn_match": _safe_text(row.get("cn_match")),
                "suggestion": _safe_text(row.get("suggestion")) or "skip",
                "confidence": _safe_text(row.get("confidence")) or "low",
                "score": score,
                "tier": tier,
                "reason": _safe_text(row.get("reason")),
            }
            tiers[tier].append(item)
            ranked_items.append(item)

        for key in tiers:
            tiers[key].sort(key=lambda x: x.get("score", 0.0), reverse=True)
        ranked_items.sort(key=lambda x: x.get("score", 0.0), reverse=True)
        return {
            "tiers": tiers,
            "ranked": ranked_items,
            "summary": {
                "稳胆": len(tiers["稳胆"]),
                "博弈": len(tiers["博弈"]),
                "观察": len(tiers["观察"]),
                "放弃": len(tiers["放弃"]),
            },
        }

    @classmethod
    def _build_operational_candidate_board(cls, verdicts: List[Dict[str, Any]]) -> Dict[str, Any]:
        base = cls._build_candidate_board(verdicts)
        summary = base.get("summary", {}) if isinstance(base.get("summary"), dict) else {}
        if int(summary.get("稳胆", 0)) + int(summary.get("博弈", 0)) > 0:
            return base

        tiers = base.get("tiers", {}) if isinstance(base.get("tiers"), dict) else {"稳胆": [], "博弈": [], "观察": [], "放弃": []}
        tiers.setdefault("稳胆", [])
        tiers.setdefault("博弈", [])
        tiers.setdefault("观察", [])
        tiers.setdefault("放弃", [])

        all_skip = all(_safe_text(row.get("suggestion")).lower() == "skip" for row in verdicts) if verdicts else False
        promoted: List[Dict[str, Any]] = []
        if all_skip:
            skip_rows: List[Dict[str, Any]] = []
            for row in verdicts:
                home_edge = row.get("edge_home")
                away_edge = row.get("edge_away")
                if not isinstance(home_edge, (int, float)) or not isinstance(away_edge, (int, float)):
                    continue
                best_edge = max(float(home_edge), float(away_edge))
                pick = "3/1" if float(home_edge) >= float(away_edge) else "1/0"
                skip_rows.append(
                    {
                        "match": _safe_text(row.get("match")),
                        "cn_match": _safe_text(row.get("cn_match")),
                        "suggestion": pick,
                        "confidence": "low",
                        "score": round(best_edge, 2),
                        "tier": "博弈",
                        "reason": f"运营候选：原结论为 skip，按最不差边际兜底（best_edge={best_edge:+.1f}pp）。",
                    }
                )
            skip_rows.sort(key=lambda x: x.get("score", -999), reverse=True)
            promoted = skip_rows[:3]
        else:
            ranked = base.get("ranked", []) if isinstance(base.get("ranked"), list) else []
            for item in ranked[:3]:
                promoted.append(
                    {
                        "match": _safe_text(item.get("match")),
                        "cn_match": _safe_text(item.get("cn_match")),
                        "suggestion": _safe_text(item.get("suggestion")) or "1",
                        "confidence": _safe_text(item.get("confidence")) or "low",
                        "score": item.get("score", 0),
                        "tier": "博弈",
                        "reason": f"{_safe_text(item.get('reason'))} [ops提升: 综合评分前3]",
                    }
                )
        if not promoted:
            return base

        tiers["博弈"] = promoted

        promoted_matches = {x.get("match") for x in promoted}
        new_discard = []
        for item in tiers.get("放弃", []):
            if _safe_text(item.get("match")) not in promoted_matches:
                new_discard.append(item)
        tiers["放弃"] = new_discard

        ranked = tiers["稳胆"] + tiers["博弈"] + tiers["观察"] + tiers["放弃"]
        return {
            "tiers": tiers,
            "ranked": ranked,
            "summary": {
                "稳胆": len(tiers["稳胆"]),
                "博弈": len(tiers["博弈"]),
                "观察": len(tiers["观察"]),
                "放弃": len(tiers["放弃"]),
            },
            "profile": "ops",
        }

    @staticmethod
    def _build_ops_watchlist(candidate_board: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not isinstance(candidate_board, dict):
            return []
        tiers = candidate_board.get("tiers") if isinstance(candidate_board.get("tiers"), dict) else {}
        items = tiers.get("博弈") if isinstance(tiers.get("博弈"), list) else []
        if not items:
            items = tiers.get("观察") if isinstance(tiers.get("观察"), list) else []
        return items[:5]

    @staticmethod
    def _combo_type_from_suggestion(suggestion: str) -> str:
        txt = _safe_text(suggestion).strip().lower()
        if txt in {"3", "1", "0", "skip"}:
            return "NONE"
        parts = [p for p in txt.split("/") if p]
        if len(parts) == 2:
            return "DOUBLE"
        if len(parts) >= 3:
            return "TRIPLE"
        return "NONE"

    @staticmethod
    def _blocked_by_gates(verdict: Dict[str, Any]) -> List[str]:
        blocks: List[str] = []
        if bool(verdict.get("no_single_pick_gate")):
            blocks.append("NO_SINGLE_PICK_GATE")
        if bool(verdict.get("home_away_integrity_gate")):
            blocks.append("HOME_AWAY_INTEGRITY_GATE")
        if bool(verdict.get("non_elite_hot_favorite_caution")):
            blocks.append("NON_ELITE_HOT_FAVORITE_CAUTION")
        if bool(verdict.get("favorite_retreat_not_reversal_gate")):
            blocks.append("FAVORITE_RETREAT_NOT_REVERSAL")
        if bool(verdict.get("away_euro_fatigue_mild_favorite_gate")):
            blocks.append("AWAY_EURO_FATIGUE_MILD_FAVORITE")
        if bool(verdict.get("favorite_conversion_bubble_no_single_gate")):
            blocks.append("CONVERSION_BUBBLE_NO_SINGLE")
        if bool(verdict.get("away_half_ball_no_upgrade_home_xg_risk_gate")):
            blocks.append("AWAY_HALF_BALL_NO_UPGRADE_HOME_XG_RISK")
        if bool(verdict.get("compressed_table_draw_protection_gate")):
            blocks.append("COMPRESSED_TABLE_DRAW_PROTECTION")
        if bool(verdict.get("away_elite_conditional_only")):
            blocks.append("AWAY_ELITE_CONDITIONAL_ONLY")
        if bool(verdict.get("is_insufficient_resilience")):
            blocks.append("INSUFFICIENT_RESILIENCE")
        if bool(verdict.get("is_low_confidence")):
            blocks.append("LOW_CONFIDENCE")
        if bool(verdict.get("market_reversal_gate")):
            blocks.append("MARKET_REVERSAL_GATE")
        if bool(verdict.get("survival_escape_signal")):
            blocks.append("SURVIVAL_ESCAPE_SIGNAL")
        if _safe_text(verdict.get("rotation_intensity")).upper() == "HIGH":
            blocks.append("GATE_HIGH_ROTATION_RISK")
        if bool(verdict.get("postponed_market_time_decay_gate")):
            blocks.append("POSTPONED_MARKET_TIME_DECAY_GATE")
        if bool(verdict.get("cup_final_context_gate")):
            blocks.append("CUP_FINAL_CONTEXT_GATE")
        if bool(verdict.get("strong_favorite_variance_guard_gate")):
            blocks.append("STRONG_FAVORITE_VARIANCE_GUARD")
        if bool(verdict.get("underdog_repair_win_escalation_gate")):
            blocks.append("UNDERDOG_REPAIR_WIN_ESCALATION")
        if bool(verdict.get("euro_asian_split_priority_gate")):
            blocks.append("EURO_ASIAN_SPLIT_PRIORITY")
        if bool(verdict.get("locked_target_deflation_gate")):
            blocks.append("LOCKED_TARGET_DEFLATION")
        if _safe_text(verdict.get("survival_win_conversion_state")) in {"can_win", "draw_only", "draw_or_win"}:
            blocks.append(f"SURVIVAL_WIN_CONVERSION_{_safe_text(verdict.get('survival_win_conversion_state')).upper()}")
        return blocks

    @classmethod
    def _single_pick_score_01(cls, verdict: Dict[str, Any]) -> float:
        raw = cls._candidate_score(verdict)
        score = (float(raw) + 2.0) / 8.0
        return round(max(0.0, min(1.0, score)), 3)

    @classmethod
    def _apply_dynamic_ticket_structure(cls, verdicts: List[Dict[str, Any]], candidate_board: Dict[str, Any]) -> Dict[str, int]:
        tier_by_match = {
            _safe_text(item.get("match")): tier
            for tier, items in (candidate_board.get("tiers") or {}).items()
            if isinstance(items, list)
            for item in items
        }
        single_candidates: List[Dict[str, Any]] = []
        high_risk_count = 0
        for verdict in verdicts:
            tier = tier_by_match.get(_safe_text(verdict.get("match")), _safe_text(verdict.get("candidate_tier")) or "放弃")
            analysis_suggestion = _safe_text(verdict.get("suggestion")) or "skip"
            blocked = cls._blocked_by_gates(verdict)
            single_score = cls._single_pick_score_01(verdict)
            high_risk = bool(
                {"NO_SINGLE_PICK_GATE", "INSUFFICIENT_RESILIENCE", "AWAY_ELITE_CONDITIONAL_ONLY", "GATE_HIGH_ROTATION_RISK"}
                & set(blocked)
            )
            if high_risk:
                high_risk_count += 1
            eligible_single = (
                analysis_suggestion in {"3", "1", "0"}
                and tier in {"稳胆", "博弈"}
                and single_score >= 0.68
                and "NO_SINGLE_PICK_GATE" not in blocked
                and "AWAY_ELITE_CONDITIONAL_ONLY" not in blocked
                and "GATE_HIGH_ROTATION_RISK" not in blocked
            )
            verdict["candidate_tier"] = tier
            verdict["analysis_suggestion"] = analysis_suggestion
            verdict["single_pick_score"] = single_score
            verdict["blocked_by_gates"] = blocked
            verdict["single_pick_eligible"] = eligible_single
            if not _safe_text(verdict.get("key_node_absence_risk")):
                verdict["key_node_absence_risk"] = "UNKNOWN"
            if not isinstance(verdict.get("lineup_stability_precheck"), dict):
                verdict["lineup_stability_precheck"] = {"home": "UNKNOWN", "away": "UNKNOWN"}
            if eligible_single:
                single_candidates.append(verdict)

        total_matches = max(1, len(verdicts))
        base_budget = total_matches // 4
        quality_bonus = 1 if len(single_candidates) >= base_budget + 2 else 0
        high_risk_ratio = high_risk_count / total_matches
        risk_penalty = -2 if high_risk_ratio >= 0.50 else -1 if high_risk_ratio >= 0.35 else 0
        final_budget = max(0, min(5, base_budget + quality_bonus + risk_penalty))
        ranked_single = sorted(single_candidates, key=lambda x: float(x.get("single_pick_score") or 0.0), reverse=True)
        allowed_single_matches = {_safe_text(v.get("match")) for v in ranked_single[:final_budget]}
        single_ranks = {_safe_text(v.get("match")): i + 1 for i, v in enumerate(ranked_single)}

        single_used = 0
        combo_used = 0
        pass_used = 0
        for verdict in verdicts:
            match_name = _safe_text(verdict.get("match"))
            analysis_suggestion = _safe_text(verdict.get("analysis_suggestion") or verdict.get("suggestion") or "skip")
            tier = _safe_text(verdict.get("candidate_tier"))
            is_pass = analysis_suggestion == "skip" or tier in {"放弃"}
            decision_type = "PASS"
            final_suggestion = "skip"
            if not is_pass:
                if match_name in allowed_single_matches and analysis_suggestion in {"3", "1", "0"}:
                    decision_type = "SINGLE"
                    final_suggestion = analysis_suggestion
                    single_used += 1
                else:
                    decision_type = "COMBO"
                    if analysis_suggestion in {"3", "1", "0"}:
                        final_suggestion = "3/1" if analysis_suggestion == "3" else "1/0" if analysis_suggestion == "0" else "1/3"
                    else:
                        final_suggestion = analysis_suggestion
                    combo_used += 1
            else:
                pass_used += 1

            verdict["decision_type"] = decision_type
            verdict["non_actionable"] = decision_type == "PASS"
            verdict["final_suggestion"] = final_suggestion
            verdict["single_pick_rank"] = single_ranks.get(match_name)
            verdict["combo_type"] = cls._combo_type_from_suggestion(final_suggestion)

        return {
            "single_pick_dynamic_budget": final_budget,
            "single_used": single_used,
            "combo_used": combo_used,
            "pass_used": pass_used,
        }

    @staticmethod
    def _is_elite_profile(profile: Optional[Dict[str, Any]]) -> bool:
        if not isinstance(profile, dict):
            return False
        return _safe_text(profile.get("team_class_hint")).lower() == "elite_depth"

    @staticmethod
    def _brand_power(profile: Optional[Dict[str, Any]]) -> str:
        if not isinstance(profile, dict):
            return "medium"
        market_profile = profile.get("market_profile") if isinstance(profile.get("market_profile"), dict) else {}
        tier = _safe_text(market_profile.get("brand_power")).lower()
        if tier in BRAND_POWER_LEVEL:
            return tier
        if PrematchSynthesis._is_elite_profile(profile):
            return "elite"
        return "medium"

    @staticmethod
    def _motivation_intensity(context_flags: Dict[str, Any]) -> int:
        if not isinstance(context_flags, dict):
            return 0
        profile = context_flags.get("motivation_profile") if isinstance(context_flags.get("motivation_profile"), dict) else {}
        if not profile and isinstance(context_flags.get("favorite_profile"), dict):
            fav_profile = context_flags.get("favorite_profile") or {}
            profile = fav_profile.get("motivation_profile") if isinstance(fav_profile.get("motivation_profile"), dict) else {}
        intensity = profile.get("intensity")
        try:
            return int(intensity)
        except Exception:
            return 0

    @staticmethod
    def _brand_favorite_price_not_low_enough_gate(
        *,
        home_favorite: bool,
        favorite_profile: Dict[str, Any],
        underdog_profile: Dict[str, Any],
        home_market: Optional[float],
        away_market: Optional[float],
        favorite_deep_handicap: float,
        context_flags: Dict[str, Any],
    ) -> bool:
        # BRAND_FAVORITE_PRICE_NOT_LOW_ENOUGH:
        # 强品牌主场热门在题材充足时，若赔率未压进合理低位，视为“价格不够低”风险。
        if not home_favorite:
            return False
        if not isinstance(home_market, (int, float)) or not isinstance(away_market, (int, float)):
            return False
        brand = PrematchSynthesis._brand_power(favorite_profile)
        if BRAND_POWER_LEVEL.get(brand, 0) < BRAND_POWER_LEVEL["high"]:
            return False
        motivation_ok = PrematchSynthesis._motivation_intensity(context_flags) >= 3 or bool(
            context_flags.get("title_race_pressure")
        )
        if not motivation_ok:
            return False

        underdog_brand = PrematchSynthesis._brand_power(underdog_profile)
        if BRAND_POWER_LEVEL.get(underdog_brand, 0) >= BRAND_POWER_LEVEL["high"]:
            return False

        underdog_leak = _safe_float(underdog_profile.get("defensive_leakage"))
        defensive_opponent = underdog_leak is not None and underdog_leak <= 0.42
        anchor_type = "defensive_mid_table" if defensive_opponent else "default"
        anchor = EXPECTED_HOME_PRICE_ANCHOR.get(brand, EXPECTED_HOME_PRICE_ANCHOR["medium"]).get(anchor_type)
        expected_upper = float(anchor[1]) if anchor else 1.65
        current_home_odds = 100.0 / float(home_market) if float(home_market) > 0 else 99.0
        if current_home_odds < expected_upper:
            return False

        # 盘口在 -1 附近但并不深，且对手有低位防守能力时，强制禁单。
        line_around_minus_one = 0.80 <= favorite_deep_handicap <= 1.20
        if not line_around_minus_one and not defensive_opponent:
            return False
        return True

    @staticmethod
    def _narrative_strength_cap_gate(
        *,
        favorite_profile: Dict[str, Any],
        underdog_profile: Dict[str, Any],
        context_flags: Dict[str, Any],
        favorite_deep_handicap: float,
        favorite_strengthened: bool,
    ) -> bool:
        # NARRATIVE_STRENGTH_CAP:
        # 强题材可抬方向，但不能盖过伤停、xG 反证和防守泄漏风险。
        narrative_strong = bool(context_flags.get("title_race_pressure")) or PrematchSynthesis._motivation_intensity(context_flags) >= 3
        if not (narrative_strong and favorite_strengthened):
            return False
        injuries = PrematchSynthesis._has_confirmed_xi_damage(favorite_profile)
        opp_xg = _safe_float(underdog_profile.get("avg_xG_last_5"))
        fav_leak = _safe_float(favorite_profile.get("defensive_leakage"))
        water_not_low = favorite_deep_handicap <= 1.05
        return injuries or (opp_xg is not None and opp_xg >= 1.25) or (fav_leak is not None and fav_leak >= 0.56) or water_not_low

    @staticmethod
    def _strong_favorite_variance_guard_gate(
        *,
        favorite_profile: Dict[str, Any],
        underdog_profile: Dict[str, Any],
        favorite_deep_handicap: float,
        market_behavior: Dict[str, Any],
        context_flags: Dict[str, Any],
    ) -> bool:
        # STRONG_FAVORITE_VARIANCE_GUARD:
        # 强热门方向可成立，但深盘覆盖要降级，避免“过程强=穿盘稳”误判。
        fair_like = float(favorite_deep_handicap or 0.0) <= 1.05
        deeper_than_fair = bool(market_behavior.get("handicap_deepen")) or float(favorite_deep_handicap or 0.0) >= 1.0
        fav_conv = _safe_float(favorite_profile.get("conversion_efficiency"))
        conv_risk = fav_conv is not None and (fav_conv <= 0.85 or fav_conv >= 1.60)
        opp_xg = _safe_float(underdog_profile.get("avg_xG_last_5"))
        opp_not_zero = opp_xg is not None and opp_xg >= 0.90
        needs_multi_goal_cover = float(favorite_deep_handicap or 0.0) >= 1.25
        locked_target = _safe_text(context_flags.get("primary_motivation_type")).upper() in {
            "TITLE_SECURED",
            "UCL_SECURED",
            "MIDTABLE_SAFE",
        }
        return fair_like and deeper_than_fair and (conv_risk or locked_target) and opp_not_zero and needs_multi_goal_cover

    @staticmethod
    def _underdog_repair_win_escalation_gate(
        *,
        market_behavior: Dict[str, Any],
        context_flags: Dict[str, Any],
        home_market: Optional[float],
        away_market: Optional[float],
    ) -> bool:
        # UNDERDOG_REPAIR_WIN_ESCALATION:
        # 弱方修复不能只升平局，需要显式提升弱方直胜路径。
        euro_repair = bool(market_behavior.get("market_retreat_against_favorite")) and bool(
            market_behavior.get("handicap_retreat")
        )
        motivation_flip = bool(context_flags.get("opponent_survival_pressure_high")) or _safe_text(
            context_flags.get("survival_pressure_level")
        ).lower() in {"high", "extreme"}
        favorite_locked = _safe_text(context_flags.get("primary_motivation_type")).upper() in {
            "TITLE_SECURED",
            "UCL_SECURED",
            "MIDTABLE_SAFE",
        } or _safe_text(context_flags.get("rotation_intensity")).upper() == "HIGH"
        close_market = (
            isinstance(home_market, (int, float))
            and isinstance(away_market, (int, float))
            and abs(float(home_market) - float(away_market)) <= 6.0
        )
        return euro_repair and motivation_flip and (favorite_locked or close_market)

    @staticmethod
    def _euro_asian_split_priority_gate(
        *,
        market_behavior: Dict[str, Any],
        favorite_strengthened: bool,
    ) -> bool:
        # EURO_ASIAN_SPLIT_PRIORITY:
        # 欧赔退热门 vs 亚盘仍挺热门时，优先识别分裂并降让球置信。
        euro_drift = bool(market_behavior.get("market_retreat_against_favorite"))
        asian_still_support = bool(market_behavior.get("handicap_deepen")) or bool(favorite_strengthened)
        return euro_drift and asian_still_support

    @staticmethod
    def _locked_target_deflation_enhanced(
        *,
        context_flags: Dict[str, Any],
    ) -> Dict[str, float]:
        # LOCKED_TARGET_DEFLATION 加强版：主要降让球强度，次降赛果强度。
        mt = _safe_text(context_flags.get("primary_motivation_type")).upper()
        rotation_high = _safe_text(context_flags.get("rotation_intensity")).upper() == "HIGH"
        europe_sandwich = bool(context_flags.get("europe_sandwich"))
        if mt == "TITLE_SECURED":
            return {"handicap": 0.50, "result": 0.20}
        if mt == "UCL_SECURED":
            return {"handicap": 0.35, "result": 0.15}
        if mt == "MIDTABLE_SAFE":
            return {"handicap": 0.25, "result": 0.10}
        if rotation_high or europe_sandwich:
            return {"handicap": 0.30, "result": 0.15}
        return {"handicap": 0.0, "result": 0.0}

    @staticmethod
    def _survival_win_conversion_gate(
        *,
        survival_escape_signal: bool,
        market_behavior: Dict[str, Any],
        context_flags: Dict[str, Any],
        survival_side_profile: Dict[str, Any],
        opponent_profile: Dict[str, Any],
    ) -> str:
        # SURVIVAL_WIN_CONVERSION_GATE:
        # 区分“只能保平”与“可直接赢”。
        if not survival_escape_signal:
            return "none"
        survival_xg = _safe_float(survival_side_profile.get("avg_xG_last_5"))
        survival_conv = _safe_float(survival_side_profile.get("conversion_efficiency"))
        opp_leak = _safe_float(opponent_profile.get("defensive_leakage"))
        repaired = bool(market_behavior.get("market_retreat_against_favorite")) or bool(
            market_behavior.get("handicap_retreat")
        )
        opponent_locked = _safe_text(context_flags.get("primary_motivation_type")).upper() in {
            "TITLE_SECURED",
            "UCL_SECURED",
            "MIDTABLE_SAFE",
        }
        if repaired and opponent_locked and (survival_xg is not None and survival_xg >= 0.90) and (
            opp_leak is not None and opp_leak >= 0.52
        ):
            return "can_win"
        if (survival_xg is not None and survival_xg < 0.80) or (
            survival_conv is not None and survival_conv >= 1.45
        ) or (opp_leak is not None and opp_leak <= 0.45):
            return "draw_only"
        return "draw_or_win"

    @staticmethod
    def _has_confirmed_xi_damage(profile: Optional[Dict[str, Any]]) -> bool:
        if not isinstance(profile, dict):
            return False
        injuries = profile.get("injured_nodes") if isinstance(profile.get("injured_nodes"), list) else []
        susp = profile.get("suspended_nodes") if isinstance(profile.get("suspended_nodes"), list) else []
        return _has_confirmed_absence(injuries) or _has_confirmed_absence(susp)

    @staticmethod
    def _schedule_risk_controls(required_controls: List[Any]) -> bool:
        if not isinstance(required_controls, list):
            return False
        keywords = ("lineup", "首发", "rotation", "轮换", "final_lineup_recheck_required")
        for item in required_controls:
            text = _safe_text(item).lower()
            if text and any(k in text for k in keywords):
                return True
        return False

    @staticmethod
    def _is_rivalry_match(home_team: str, away_team: str, risk_tags: List[str]) -> bool:
        home_key = _normalize_team_key(home_team)
        away_key = _normalize_team_key(away_team)
        if (home_key, away_key) in RIVALRY_TEAM_PAIRS:
            return True
        for raw in risk_tags:
            tag = _safe_text(raw).lower()
            if "rival" in tag or "derby" in tag or "variance" in tag:
                return True
        return False

    @staticmethod
    def _has_transition_threat(home_profile: Dict[str, Any], away_profile: Dict[str, Any]) -> bool:
        home_xg = _safe_float(home_profile.get("avg_xG_last_5"))
        away_xg = _safe_float(away_profile.get("avg_xG_last_5"))
        home_leak = _safe_float(home_profile.get("defensive_leakage"))
        away_leak = _safe_float(away_profile.get("defensive_leakage"))
        xg_ready = (home_xg is not None and home_xg >= 1.2) and (away_xg is not None and away_xg >= 1.2)
        leak_ready = (home_leak is not None and home_leak >= 0.55) or (away_leak is not None and away_leak >= 0.55)
        return xg_ready or leak_ready

    @staticmethod
    def _away_favorite_defense_exposure_trigger(
        home_market: Optional[float],
        away_market: Optional[float],
        home_profile: Dict[str, Any],
        away_profile: Dict[str, Any],
        risk_tags: List[str],
    ) -> bool:
        away_favorite = (
            isinstance(home_market, (int, float))
            and isinstance(away_market, (int, float))
            and away_market - home_market >= 3.0
        )
        if not away_favorite:
            return False
        away_leak = _safe_float(away_profile.get("defensive_leakage"))
        home_xg = _safe_float(home_profile.get("avg_xG_last_5"))
        tag_signal = any("rest_defense_exposure" in _safe_text(tag).lower() for tag in risk_tags)
        prev_xga_proxy = (away_leak is not None and away_leak >= 0.58) or tag_signal
        home_xg_proxy = home_xg is not None and home_xg >= 1.5
        return prev_xga_proxy and home_xg_proxy

    @staticmethod
    def _low_event_home_favorite_trigger(
        home_market: Optional[float],
        away_market: Optional[float],
        home_profile: Dict[str, Any],
        away_profile: Dict[str, Any],
    ) -> bool:
        # ARES_LOW_EVENT_HOME_FAVORITE_GATE:
        # 低事件主队即便被市场轻抬，也不允许重仓单挑主胜。
        home_favorite = (
            isinstance(home_market, (int, float))
            and isinstance(away_market, (int, float))
            and home_market - away_market >= 2.5
        )
        if not home_favorite:
            return False
        home_xg = _safe_float(home_profile.get("avg_xG_last_5"))
        home_leak = _safe_float(home_profile.get("defensive_leakage"))
        away_xg = _safe_float(away_profile.get("avg_xG_last_5"))
        low_event_home = (home_xg is not None and home_xg <= 1.35) and (home_leak is not None and home_leak <= 0.55)
        away_pressure = away_xg is not None and away_xg >= 1.45
        return low_event_home or away_pressure

    @staticmethod
    def _away_recent_xg_protection_trigger(
        home_profile: Dict[str, Any],
        away_profile: Dict[str, Any],
    ) -> bool:
        # ARES_AWAY_RECENT_XG_PROTECTION_GATE:
        # 客队近期 xG 明显领先时，不能删除客胜路径。
        home_xg = _safe_float(home_profile.get("avg_xG_last_5"))
        away_xg = _safe_float(away_profile.get("avg_xG_last_5"))
        if home_xg is None or away_xg is None:
            return False
        if away_xg < home_xg + 0.4:
            return False
        away_conv = _safe_float(away_profile.get("conversion_efficiency"))
        away_xi_damage = PrematchSynthesis._has_confirmed_xi_damage(away_profile)
        away_attack_collapsed = away_xg < 1.0 and away_xi_damage and (away_conv is None or away_conv < 0.6)
        return not away_attack_collapsed

    @staticmethod
    def _market_reversal_gate_trigger(risk_tags: List[str]) -> bool:
        for raw in risk_tags:
            tag = _safe_text(raw).lower()
            if "market_reversal" in tag or "odds_reversal" in tag or "reversal" in tag:
                return True
        return False

    @staticmethod
    def _backup_gk_low_score_risk_trigger(
        home_profile: Dict[str, Any],
        away_profile: Dict[str, Any],
    ) -> bool:
        # ARES_BACKUP_GK_LOW_SCORE_RISK: 门将缺席 + 低产对局容易放大开放度
        nodes = home_profile.get("injured_nodes") if isinstance(home_profile.get("injured_nodes"), list) else []
        text = " | ".join(_safe_text(x).lower() for x in nodes)
        gk_absent = any(k in text for k in ("gk", "goalkeeper", "keeper", "门将", "gardien"))
        home_xg = _safe_float(home_profile.get("avg_xG_last_5"))
        away_xg = _safe_float(away_profile.get("avg_xG_last_5"))
        low_event = (home_xg is not None and home_xg <= 1.4) and (away_xg is not None and away_xg <= 1.3)
        return gk_absent and low_event

    @staticmethod
    def _direct_rival_home_pressure_trigger(
        home_profile: Dict[str, Any],
        away_profile: Dict[str, Any],
        risk_tags: List[str],
    ) -> bool:
        # ARES_DIRECT_RIVAL_HOME_PRESSURE_GATE
        tag_hit = any("direct_rival" in _safe_text(t).lower() or "high_motivation" in _safe_text(t).lower() for t in risk_tags)
        home_xg = _safe_float(home_profile.get("avg_xG_last_5"))
        away_xg = _safe_float(away_profile.get("avg_xG_last_5"))
        return tag_hit and (home_xg is not None and home_xg >= 1.5) and (away_xg is not None and away_xg >= 1.1)

    @staticmethod
    def _recent_big_win_noise_gate(risk_tags: List[str], away_profile: Dict[str, Any]) -> bool:
        # ARES_RECENT_BIG_WIN_NOISE_GATE
        has_big_win_signal = any("big_win" in _safe_text(t).lower() or "recent_hot_form" in _safe_text(t).lower() for t in risk_tags)
        away_inj = away_profile.get("injured_nodes") if isinstance(away_profile.get("injured_nodes"), list) else []
        away_susp = away_profile.get("suspended_nodes") if isinstance(away_profile.get("suspended_nodes"), list) else []
        multi_line_absence = len(away_inj) + len(away_susp) >= 2
        return has_big_win_signal and multi_line_absence

    @staticmethod
    def _away_favorite_injury_result_gate(away_profile: Dict[str, Any], away_favorite: bool) -> bool:
        # ARES_AWAY_FAVORITE_INJURY_RESULT_GATE
        if not away_favorite:
            return False
        away_inj = away_profile.get("injured_nodes") if isinstance(away_profile.get("injured_nodes"), list) else []
        away_susp = away_profile.get("suspended_nodes") if isinstance(away_profile.get("suspended_nodes"), list) else []
        return len(away_inj) + len(away_susp) >= 3

    @staticmethod
    def _away_process_edge_over_home_survival(
        home_profile: Dict[str, Any],
        away_profile: Dict[str, Any],
        survival_escape_signal: bool,
    ) -> bool:
        # ARES_AWAY_PROCESS_EDGE_OVER_HOME_SURVIVAL_PRESSURE
        if not survival_escape_signal:
            return False
        home_xg = _safe_float(home_profile.get("avg_xG_last_5"))
        away_xg = _safe_float(away_profile.get("avg_xG_last_5"))
        if home_xg is None or away_xg is None:
            return False
        return away_xg >= home_xg + 0.45

    @staticmethod
    def _relegation_away_draw_protection(
        home_market: Optional[float],
        away_market: Optional[float],
        risk_tags: List[str],
    ) -> bool:
        # ARES_RELEGATION_AWAY_DRAW_PROTECTION_GATE
        retreat_signal = any("market_reversal" in _safe_text(t).lower() or "home_odds_raise" in _safe_text(t).lower() for t in risk_tags)
        close_market = (
            isinstance(home_market, (int, float))
            and isinstance(away_market, (int, float))
            and abs(float(home_market) - float(away_market)) <= 4.0
        )
        return retreat_signal or close_market

    @staticmethod
    def _away_favorite_attack_cold_gate(
        home_market: Optional[float],
        away_market: Optional[float],
        home_profile: Dict[str, Any],
        away_profile: Dict[str, Any],
    ) -> bool:
        # ARES_AWAY_FAVORITE_ATTACK_COLD_GATE
        away_favorite = (
            isinstance(home_market, (int, float))
            and isinstance(away_market, (int, float))
            and away_market - home_market >= 2.5
        )
        if not away_favorite:
            return False
        away_xg = _safe_float(away_profile.get("avg_xG_last_5"))
        away_conv = _safe_float(away_profile.get("conversion_efficiency"))
        home_xg = _safe_float(home_profile.get("avg_xG_last_5"))
        attack_cold = (away_xg is not None and away_xg <= 1.35) or (away_conv is not None and away_conv <= 0.95)
        home_resistance = home_xg is not None and home_xg >= 1.35
        return attack_cold and home_resistance

    @staticmethod
    def _home_form_resistance_gate(
        home_profile: Dict[str, Any],
        away_profile: Dict[str, Any],
        home_market: Optional[float],
        away_market: Optional[float],
    ) -> bool:
        # ARES_HOME_FORM_RESISTANCE_GATE
        home_xg = _safe_float(home_profile.get("avg_xG_last_5"))
        home_conv = _safe_float(home_profile.get("conversion_efficiency"))
        away_favorite = (
            isinstance(home_market, (int, float))
            and isinstance(away_market, (int, float))
            and away_market - home_market >= 2.5
        )
        if not away_favorite:
            return False
        strong_home_form = (home_xg is not None and home_xg >= 1.45) or (home_conv is not None and home_conv >= 1.05)
        away_leak = _safe_float(away_profile.get("defensive_leakage"))
        away_not_stable = away_leak is not None and away_leak >= 0.52
        return strong_home_form and away_not_stable

    @staticmethod
    def _conversion_bubble_penalty(
        profile: Optional[Dict[str, Any]],
        opponent_profile: Optional[Dict[str, Any]],
    ) -> float:
        if not isinstance(profile, dict):
            return 0.0
        conv = _safe_float(profile.get("conversion_efficiency"))
        xg = _safe_float(profile.get("avg_xG_last_5"))
        opp_leak = _safe_float((opponent_profile or {}).get("defensive_leakage"))
        if conv is None or conv < 1.8:
            return 0.0
        # ARES_CONVERSION_BUBBLE_SPLIT:
        # 高转化不自动反转；仅在“机会质量偏弱 + 对手防守不漏”时轻微降权。
        if (xg is not None and xg < 1.35) and (opp_leak is not None and opp_leak < 0.5):
            return 0.8
        return 0.0

    @staticmethod
    def _non_elite_hot_favorite_caution(
        favorite_profile: Dict[str, Any],
        underdog_profile: Dict[str, Any],
        favorite_is_elite: bool,
        favorite_strengthened: bool,
        favorite_deep_handicap: float,
    ) -> bool:
        # NON_ELITE_HOT_FAVORITE_CAUTION:
        # 非 elite 热门被市场强化时，若同时具备多项风险，禁止单选。
        if favorite_is_elite or not favorite_strengthened:
            return False
        risk = 0
        opp_xg = _safe_float(underdog_profile.get("avg_xG_last_5"))
        fav_leak = _safe_float(favorite_profile.get("defensive_leakage"))
        fav_conv = _safe_float(favorite_profile.get("conversion_efficiency"))
        if opp_xg is not None and opp_xg >= 1.35:
            risk += 1
        if fav_leak is not None and fav_leak >= 0.52:
            risk += 1
        if fav_conv is not None and fav_conv >= 1.60:
            risk += 1
        if favorite_deep_handicap < 0.75:
            risk += 1
        return risk >= 2

    @staticmethod
    def _favorite_retreat_not_reversal(
        market_retreat_against_favorite: bool,
        home_market: Optional[float],
        away_market: Optional[float],
    ) -> bool:
        # FAVORITE_RETREAT_NOT_REVERSAL:
        # 退盘不自动反向，只做降级和防平/防冷处理。
        if not market_retreat_against_favorite:
            return False
        if not (isinstance(home_market, (int, float)) and isinstance(away_market, (int, float))):
            return False
        return abs(float(home_market) - float(away_market)) <= 8.0

    @staticmethod
    def _away_euro_fatigue_mild_favorite_gate(
        away_favorite: bool,
        europe_sandwich: bool,
        favorite_deep_handicap: float,
    ) -> bool:
        # AWAY_EURO_FATIGUE_MILD_FAVORITE:
        # 客队欧战夹击 + 让球不深时，单边客胜降级。
        if not away_favorite or not europe_sandwich:
            return False
        return favorite_deep_handicap <= 0.75

    @staticmethod
    def _favorite_conversion_bubble_no_single_gate(
        favorite_profile: Dict[str, Any],
    ) -> bool:
        conv = _safe_float(favorite_profile.get("conversion_efficiency"))
        return conv is not None and conv >= 1.80

    @staticmethod
    def _away_half_ball_no_upgrade_home_xg_risk_gate(
        away_favorite: bool,
        favorite_deep_handicap: float,
        handicap_deepen: bool,
        home_profile: Dict[str, Any],
    ) -> bool:
        # AWAY_HALF_BALL_NO_UPGRADE_HOME_XG_RISK:
        # 客让半球低水但未升盘，且主队近期机会产量不低，需防主队乱战。
        if not away_favorite:
            return False
        if not (0.45 <= favorite_deep_handicap <= 0.60):
            return False
        if handicap_deepen:
            return False
        home_xg = _safe_float(home_profile.get("avg_xG_last_5"))
        return home_xg is not None and home_xg >= 1.35

    @staticmethod
    def _compressed_table_draw_protection_gate(
        home_market: Optional[float],
        away_market: Optional[float],
        home_profile: Dict[str, Any],
        away_profile: Dict[str, Any],
        context_flags: Dict[str, Any],
    ) -> bool:
        # COMPRESSED_TABLE_DRAW_PROTECTION:
        # 赛季末压缩积分带 + 赔率浅热门（约 1.90~2.30）时，强制防平并禁单。
        season_stage = _safe_text(context_flags.get("season_stage")).lower()
        is_late_season = season_stage in {"late", "run_in", "closing"} or bool(
            context_flags.get("late_season")
        )
        if not is_late_season:
            # 数据不全时，允许以“目标强度 + 价格区间 + 双方可拿分空间”做弱触发。
            is_late_season = bool(context_flags.get("opponent_survival_pressure_high")) or bool(
                context_flags.get("title_race_pressure")
            )

        hp = _safe_float(home_profile.get("table_points"))
        ap = _safe_float(away_profile.get("table_points"))
        compressed_points = (
            hp is not None
            and ap is not None
            and 39 <= hp <= 44
            and 39 <= ap <= 44
            and abs(hp - ap) <= 5
        )

        if not (isinstance(home_market, (int, float)) and isinstance(away_market, (int, float))):
            return False
        home_odds = 100.0 / float(home_market) if float(home_market) > 0 else 99.0
        away_odds = 100.0 / float(away_market) if float(away_market) > 0 else 99.0
        favorite_odds = min(home_odds, away_odds)
        shallow_favorite = 1.90 <= favorite_odds <= 2.30

        if not shallow_favorite:
            return False

        # 双方至少一侧仍存在“非死亡动机”。
        motivation_txt = " | ".join(
            [
                _safe_text(context_flags.get("primary_motivation_type")).lower(),
                " ".join(_safe_text(x).lower() for x in (context_flags.get("motivation_context_flags") or [])),
            ]
        )
        non_dead_motivation = any(
            k in motivation_txt
            for k in ("survival", "relegation", "europe", "title", "escape", "pressure")
        ) or bool(context_flags.get("opponent_survival_pressure_high")) or bool(
            context_flags.get("title_race_pressure")
        )

        return (compressed_points or non_dead_motivation) and is_late_season

    @staticmethod
    def _is_away_elite_conditional_only(
        away_favorite: bool,
        away_elite: bool,
        recent_xga_risk: bool,
        injuries_across_lines: bool,
        opponent_survival_pressure: bool,
        market_retreat_against_favorite: bool,
        derby_or_rivalry: bool,
    ) -> bool:
        if not (away_favorite and away_elite):
            return False
        hard_risks = (
            recent_xga_risk,
            injuries_across_lines,
            opponent_survival_pressure,
            market_retreat_against_favorite,
            derby_or_rivalry,
        )
        return any(hard_risks)

    @staticmethod
    def _should_block_single_pick(
        new_manager_sample_matches: int,
        opponent_survival_pressure_high: bool,
        favorite_deep_handicap: float,
        market_retreat_against_favorite: bool,
        structural_crisis_home_survival: bool,
        both_sides_strong_counterarguments: bool,
        away_elite_conditional_only: bool,
        key_node_absence_high: bool,
        lineup_stability_red: bool,
    ) -> bool:
        triggers = [
            new_manager_sample_matches <= 1 and opponent_survival_pressure_high,
            favorite_deep_handicap >= 1.25,
            market_retreat_against_favorite,
            structural_crisis_home_survival,
            both_sides_strong_counterarguments,
            away_elite_conditional_only,
            key_node_absence_high,
            lineup_stability_red,
        ]
        return any(triggers)

    @staticmethod
    def _home_away_integrity_gate(
        parsed_home_team: str,
        parsed_away_team: str,
        manifest_english_match: str,
    ) -> bool:
        pair = _split_match_english(manifest_english_match)
        if not pair:
            return False
        parsed_home_key = _normalize_team_key(parsed_home_team)
        parsed_away_key = _normalize_team_key(parsed_away_team)
        if not parsed_home_key or not parsed_away_key:
            return False
        manifest_home_key = _normalize_team_key(pair["home"])
        manifest_away_key = _normalize_team_key(pair["away"])
        if not manifest_home_key or not manifest_away_key:
            return False
        return parsed_home_key != manifest_home_key or parsed_away_key != manifest_away_key

    @staticmethod
    def _postponed_market_time_decay_gate(
        risk_tags: List[str],
        match_context_flags: Dict[str, Any],
        market_odds_history: List[Any],
        external_odds_history: List[Any],
        understat_date: str,
    ) -> bool:
        tags = " | ".join(_safe_text(x).lower() for x in (risk_tags or []))
        if any(token in tags for token in ("postponed", "rescheduled", "补赛", "延期", "old_initial_line")):
            return True
        if isinstance(match_context_flags, dict):
            motivation_type = _safe_text(match_context_flags.get("primary_motivation_type")).upper()
            if motivation_type in {"POSTPONED", "RESCHEDULED"}:
                return True
        if isinstance(market_odds_history, list) and market_odds_history:
            return False
        if not (isinstance(external_odds_history, list) and external_odds_history):
            return False
        first_external = external_odds_history[0] if isinstance(external_odds_history[0], dict) else {}
        fetched_at_raw = _safe_text(first_external.get("fetched_at"))
        if not fetched_at_raw:
            return False
        target_time_raw = _safe_text(first_external.get("target_match_time")) or _safe_text(understat_date)
        if not target_time_raw:
            return False
        fetched_at = None
        target_time = None
        for fmt, value in (
            ("%Y-%m-%dT%H:%M:%S.%fZ", fetched_at_raw),
            ("%Y-%m-%dT%H:%M:%SZ", fetched_at_raw),
        ):
            try:
                fetched_at = datetime.strptime(value, fmt)
                break
            except Exception:
                continue
        for fmt, value in (
            ("%Y-%m-%d %H:%M:%S", target_time_raw),
            ("%Y-%m-%dT%H:%M:%S", target_time_raw),
        ):
            try:
                target_time = datetime.strptime(value, fmt)
                break
            except Exception:
                continue
        if fetched_at is None or target_time is None:
            return False
        return fetched_at + timedelta(days=5) < target_time

    @staticmethod
    def _cup_final_context_gate(manifest_row: Dict[str, Any], row: Dict[str, Any]) -> bool:
        league = _safe_text(manifest_row.get("league")).lower()
        en_match = _safe_text(manifest_row.get("english")).lower()
        cn_match = _safe_text(row.get("cn_match")).lower()
        risk_tags = " | ".join(_safe_text(x).lower() for x in (row.get("risk_tags") or []))
        joined = " | ".join([league, en_match, cn_match, risk_tags])
        keywords = ("cup final", "coppa italia", "fa cup", "copa del rey", "决赛", "杯赛")
        return any(k in joined for k in keywords)

    def _build_rule_based_result(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        diagnostics = inputs.get("diagnostics") or {}
        gate_snapshot = inputs.get("gate_snapshot") or {}
        matches = inputs.get("matches") or []
        low_conf_count = int(inputs.get("low_conf_count") or 0)
        insufficient_count = int(inputs.get("insufficient_count") or 0)
        manifest_matches = (inputs.get("manifest") or {}).get("matches") or []

        mapping_counter: Dict[str, int] = {}
        smoke_count = 0
        manifest_index_lookup: Dict[int, Dict[str, Any]] = {}
        for match in manifest_matches:
            source = _safe_text(match.get("mapping_source")).lower() or "unknown"
            mapping_counter[source] = mapping_counter.get(source, 0) + 1
            idx_val = match.get("index")
            if isinstance(idx_val, int):
                manifest_index_lookup[idx_val] = match
            mode = _safe_text(match.get("manual_anchor_mode")).lower()
            notes = _safe_text(match.get("manual_anchor_notes")).lower()
            fbref_url = _safe_text(match.get("fbref_url")).lower()
            if mode == "smoke" or "[smoke]" in notes or fbref_url.startswith("https://anchor.local/"):
                smoke_count += 1

        verdicts: List[Dict[str, Any]] = []
        for row in matches:
            team_map = {str(t.get("side")).lower(): t for t in (row.get("teams") or [])}
            home = team_map.get("home", {})
            away = team_map.get("away", {})
            match_index = row.get("match_index")
            manifest_row = manifest_index_lookup.get(int(match_index)) if isinstance(match_index, int) else {}
            context_flags = (
                manifest_row.get("match_context_flags") if isinstance(manifest_row.get("match_context_flags"), dict) else {}
            )
            market_behavior = (
                manifest_row.get("market_behavior") if isinstance(manifest_row.get("market_behavior"), dict) else {}
            )
            market_odds_history = (
                manifest_row.get("market_odds_history") if isinstance(manifest_row.get("market_odds_history"), list) else []
            )
            external_odds_history = (
                manifest_row.get("external_odds_history")
                if isinstance(manifest_row.get("external_odds_history"), list)
                else []
            )

            home_market = home.get("market_prob")
            away_market = away.get("market_prob")
            home_model = home.get("model_prob")
            away_model = away.get("model_prob")
            home_sd = home.get("s_dynamic")
            away_sd = away.get("s_dynamic")

            edge_home = None
            edge_away = None
            if isinstance(home_model, (int, float)) and isinstance(home_market, (int, float)):
                edge_home = float(home_model) - float(home_market)
            if isinstance(away_model, (int, float)) and isinstance(away_market, (int, float)):
                edge_away = float(away_model) - float(away_market)
            best_edge = None
            if isinstance(edge_home, (int, float)) and isinstance(edge_away, (int, float)):
                best_edge = max(float(edge_home), float(edge_away))
            elif isinstance(edge_home, (int, float)):
                best_edge = float(edge_home)
            elif isinstance(edge_away, (int, float)):
                best_edge = float(edge_away)
            readiness_level = _safe_text(row.get("readiness_level")).upper() or "READY"
            confidence_cap = _safe_text(row.get("confidence_cap")).lower()
            required_controls = row.get("required_controls") if isinstance(row.get("required_controls"), list) else []
            home_profile = row.get("home_profile") if isinstance(row.get("home_profile"), dict) else {}
            away_profile = row.get("away_profile") if isinstance(row.get("away_profile"), dict) else {}
            risk_tags = row.get("risk_tags") if isinstance(row.get("risk_tags"), list) else []
            home_elite = self._is_elite_profile(home_profile)
            away_elite = self._is_elite_profile(away_profile)
            home_xi_damage = self._has_confirmed_xi_damage(home_profile)
            away_xi_damage = self._has_confirmed_xi_damage(away_profile)
            schedule_risk_flag = self._schedule_risk_controls(required_controls)
            rivalry_flag = self._is_rivalry_match(
                _safe_text(row.get("home_team")),
                _safe_text(row.get("away_team")),
                risk_tags,
            )
            rivalry_transition_ready = self._has_transition_threat(home_profile, away_profile)
            away_fav_def_exposure = self._away_favorite_defense_exposure_trigger(
                home_market,
                away_market,
                home_profile,
                away_profile,
                risk_tags,
            )
            away_favorite = (
                isinstance(home_market, (int, float))
                and isinstance(away_market, (int, float))
                and away_market - home_market >= 3.0
            )
            home_favorite = (
                isinstance(home_market, (int, float))
                and isinstance(away_market, (int, float))
                and home_market - away_market >= 3.0
            )
            low_event_home_fav = self._low_event_home_favorite_trigger(
                home_market,
                away_market,
                home_profile,
                away_profile,
            )
            away_recent_xg_protect = self._away_recent_xg_protection_trigger(home_profile, away_profile)
            market_reversal_flag = self._market_reversal_gate_trigger(risk_tags)
            backup_gk_low_score_risk = self._backup_gk_low_score_risk_trigger(home_profile, away_profile)
            direct_rival_home_pressure = self._direct_rival_home_pressure_trigger(home_profile, away_profile, risk_tags)
            recent_big_win_noise = self._recent_big_win_noise_gate(risk_tags, away_profile)
            away_fav_injury_result = self._away_favorite_injury_result_gate(away_profile, away_favorite)
            away_process_over_survival = self._away_process_edge_over_home_survival(
                home_profile,
                away_profile,
                bool(row.get("survival_escape_signal")),
            )
            relegation_away_draw = self._relegation_away_draw_protection(home_market, away_market, risk_tags)
            away_attack_cold = self._away_favorite_attack_cold_gate(
                home_market,
                away_market,
                home_profile,
                away_profile,
            )
            home_form_resistance = self._home_form_resistance_gate(
                home_profile,
                away_profile,
                home_market,
                away_market,
            )
            survival_escape_signal = bool(row.get("survival_escape_signal"))
            home_away_integrity_gate = self._home_away_integrity_gate(
                parsed_home_team=_safe_text(row.get("home_team")),
                parsed_away_team=_safe_text(row.get("away_team")),
                manifest_english_match=_safe_text(manifest_row.get("english")),
            )
            postponed_market_time_decay_gate = self._postponed_market_time_decay_gate(
                risk_tags=risk_tags,
                match_context_flags=context_flags,
                market_odds_history=market_odds_history,
                external_odds_history=external_odds_history,
                understat_date=_safe_text(manifest_row.get("understat_date")),
            )
            cup_final_context_gate = self._cup_final_context_gate(manifest_row, row)
            if cup_final_context_gate:
                # 杯赛决赛语境下，联赛战意权重降级，避免错误放大保级/排名信号。
                survival_escape_signal = False
            recent_xga_risk = bool(away_fav_def_exposure)
            injuries_across_lines = bool(self._has_confirmed_xi_damage(away_profile))
            opponent_survival_pressure = bool(
                context_flags.get("opponent_survival_pressure_high") or survival_escape_signal
            )
            market_retreat_against_favorite = bool(
                market_behavior.get("market_retreat_against_favorite") or market_reversal_flag
            )
            derby_or_rivalry = bool(rivalry_flag)
            away_elite_conditional_only = self._is_away_elite_conditional_only(
                away_favorite=away_favorite,
                away_elite=away_elite,
                recent_xga_risk=recent_xga_risk,
                injuries_across_lines=injuries_across_lines,
                opponent_survival_pressure=opponent_survival_pressure,
                market_retreat_against_favorite=market_retreat_against_favorite,
                derby_or_rivalry=derby_or_rivalry,
            )
            new_manager_sample_matches = int(context_flags.get("new_manager_sample_matches") or 0)
            favorite_deep_handicap = float(context_flags.get("favorite_deep_handicap") or 0.0)
            favorite_strengthened = bool(
                market_behavior.get("favorite_odds_compressed") or market_behavior.get("handicap_deepen")
            )
            handicap_deepen = bool(market_behavior.get("handicap_deepen"))
            europe_sandwich = bool(context_flags.get("europe_sandwich"))
            favorite_profile = home_profile if home_favorite else away_profile if away_favorite else {}
            underdog_profile = away_profile if home_favorite else home_profile if away_favorite else {}
            favorite_is_elite = home_elite if home_favorite else away_elite if away_favorite else False
            favorite_conversion_bubble = self._favorite_conversion_bubble_no_single_gate(favorite_profile)
            non_elite_hot_favorite_caution = self._non_elite_hot_favorite_caution(
                favorite_profile=favorite_profile,
                underdog_profile=underdog_profile,
                favorite_is_elite=favorite_is_elite,
                favorite_strengthened=favorite_strengthened,
                favorite_deep_handicap=favorite_deep_handicap,
            )
            favorite_retreat_not_reversal_gate = self._favorite_retreat_not_reversal(
                market_retreat_against_favorite=market_retreat_against_favorite,
                home_market=home_market,
                away_market=away_market,
            )
            away_euro_fatigue_mild_gate = self._away_euro_fatigue_mild_favorite_gate(
                away_favorite=away_favorite,
                europe_sandwich=europe_sandwich,
                favorite_deep_handicap=favorite_deep_handicap,
            )
            away_half_ball_no_upgrade_gate = self._away_half_ball_no_upgrade_home_xg_risk_gate(
                away_favorite=away_favorite,
                favorite_deep_handicap=favorite_deep_handicap,
                handicap_deepen=handicap_deepen,
                home_profile=home_profile,
            )
            strong_favorite_variance_guard = self._strong_favorite_variance_guard_gate(
                favorite_profile=favorite_profile,
                underdog_profile=underdog_profile,
                favorite_deep_handicap=favorite_deep_handicap,
                market_behavior=market_behavior,
                context_flags=context_flags,
            )
            underdog_repair_win_escalation = self._underdog_repair_win_escalation_gate(
                market_behavior=market_behavior,
                context_flags=context_flags,
                home_market=home_market,
                away_market=away_market,
            )
            euro_asian_split_priority = self._euro_asian_split_priority_gate(
                market_behavior=market_behavior,
                favorite_strengthened=favorite_strengthened,
            )
            locked_target_deflation = self._locked_target_deflation_enhanced(
                context_flags=context_flags,
            )
            compressed_table_draw_protection = self._compressed_table_draw_protection_gate(
                home_market=home_market,
                away_market=away_market,
                home_profile=home_profile,
                away_profile=away_profile,
                context_flags=context_flags,
            )
            structural_crisis_home_survival = bool(
                context_flags.get("structural_crisis_context") and opponent_survival_pressure
            )
            both_sides_strong_counterarguments = bool(
                away_attack_cold or home_form_resistance or low_event_home_fav or away_recent_xg_protect
            )
            no_single_pick_gate = self._should_block_single_pick(
                new_manager_sample_matches=new_manager_sample_matches,
                opponent_survival_pressure_high=opponent_survival_pressure,
                favorite_deep_handicap=favorite_deep_handicap,
                market_retreat_against_favorite=market_retreat_against_favorite,
                structural_crisis_home_survival=structural_crisis_home_survival,
                both_sides_strong_counterarguments=both_sides_strong_counterarguments,
                away_elite_conditional_only=away_elite_conditional_only,
                key_node_absence_high=(
                    str(context_flags.get("key_node_absence_risk") or "UNKNOWN").upper() in {"HIGH", "CRITICAL"}
                ),
                lineup_stability_red=(
                    any(
                        str(side_flag or "UNKNOWN").upper() == "RED"
                        for side_flag in (
                            (context_flags.get("lineup_stability_precheck") or {}).get("home")
                            if isinstance(context_flags.get("lineup_stability_precheck"), dict)
                            else None,
                            (context_flags.get("lineup_stability_precheck") or {}).get("away")
                            if isinstance(context_flags.get("lineup_stability_precheck"), dict)
                            else None,
                        )
                    )
                ),
            ) or str(context_flags.get("rotation_intensity") or "UNKNOWN").upper() == "HIGH" or any(
                [
                    non_elite_hot_favorite_caution,
                    favorite_conversion_bubble,
                    away_euro_fatigue_mild_gate,
                    away_half_ball_no_upgrade_gate,
                    compressed_table_draw_protection,
                ]
            )
            brand_price_not_low_enough = self._brand_favorite_price_not_low_enough_gate(
                home_favorite=home_favorite,
                favorite_profile=favorite_profile,
                underdog_profile=underdog_profile,
                home_market=home_market,
                away_market=away_market,
                favorite_deep_handicap=favorite_deep_handicap,
                context_flags={**context_flags, "favorite_profile": favorite_profile},
            )
            narrative_strength_cap = self._narrative_strength_cap_gate(
                favorite_profile=favorite_profile,
                underdog_profile=underdog_profile,
                context_flags={**context_flags, "favorite_profile": favorite_profile},
                favorite_deep_handicap=favorite_deep_handicap,
                favorite_strengthened=favorite_strengthened,
            )
            if brand_price_not_low_enough:
                no_single_pick_gate = True
            if narrative_strength_cap:
                no_single_pick_gate = True
            if home_away_integrity_gate or postponed_market_time_decay_gate:
                no_single_pick_gate = True

            suggestion = "skip"
            confidence = "low"
            reason = "缺少可计算的市场/模型偏差，暂归为观望。"
            confidence_score = 1.0
            upgrade_reason = None
            conv_penalty = 0.0
            reverse_block_note = ""
            survival_gate_state = "none"

            if isinstance(edge_home, (int, float)) and isinstance(edge_away, (int, float)):
                # 仅基于主客两侧边际，避免“1 - (主+客)”带来的平局伪信号。
                if edge_home >= 3.0 and edge_away <= -1.5:
                    suggestion = "3"
                elif edge_away >= 3.0 and edge_home <= -1.5:
                    suggestion = "0"
                elif edge_home >= 2.0 and edge_away >= 2.0:
                    suggestion = "3/0"
                elif edge_home < 0 and edge_away < 0:
                    diff = abs(edge_home - edge_away)
                    if diff <= 3.0:
                        suggestion = "1"
                    elif edge_home > edge_away:
                        suggestion = "3/1"
                    else:
                        suggestion = "1/0"
                else:
                    suggestion = "skip"

                # 置信度优先看“正向边际”，对冲型建议默认降一档。
                positive_edge = max(edge_home, edge_away)
                if suggestion in {"3", "0"}:
                    confidence_score = max(0.0, positive_edge)
                elif suggestion == "3/0":
                    confidence_score = max(0.0, positive_edge - 0.5)
                elif suggestion in {"1", "3/1", "1/0"} and (edge_home >= 0 or edge_away >= 0):
                    confidence_score = 2.5
                else:
                    confidence_score = 0.8
                if row.get("is_low_confidence"):
                    confidence_score -= 1.5
                if row.get("is_insufficient_resilience"):
                    confidence_score -= 2.0
                if readiness_level == "HOLD":
                    confidence_score -= 1.0

                # ARES_CONVERSION_BUBBLE_SPLIT:
                # 高转化率只在“机会质量不足 + 对手防线较稳”场景降权，不自动反转方向。
                conv_penalty = 0.0
                if suggestion in {"3", "3/1"}:
                    conv_penalty = self._conversion_bubble_penalty(home_profile, away_profile)
                elif suggestion in {"0", "1/0"}:
                    conv_penalty = self._conversion_bubble_penalty(away_profile, home_profile)
                elif suggestion in {"3/0", "1"}:
                    conv_penalty = max(
                        self._conversion_bubble_penalty(home_profile, away_profile),
                        self._conversion_bubble_penalty(away_profile, home_profile),
                    )
                if conv_penalty > 0:
                    confidence_score -= conv_penalty

                # ARES_STRONG_TEAM_REVERSE_GATE:
                # 强队不得仅因赛程/盘口信号被机械反向；无确认首发损伤时，只允许降置信或回到对冲。
                reverse_block_note = ""
                if home_elite and not home_xi_damage and suggestion in {"0", "1/0"}:
                    suggestion = "3/1" if suggestion == "0" else "1"
                    confidence_score -= 1.0
                    reverse_block_note = "触发强队反向门禁：主队为 elite 且无确认首发损伤，禁止直接做空。"
                elif away_elite and not away_xi_damage and suggestion in {"3", "3/1"}:
                    suggestion = "1/0" if suggestion == "3" else "1"
                    confidence_score -= 1.0
                    reverse_block_note = "触发强队反向门禁：客队为 elite 且无确认首发损伤，禁止直接做空。"

                # ARES_RIVALRY_VARIANCE_GATE:
                # 德比/宿敌高方差场景默认保留主胜路径，禁止把 3 直接删空。
                if rivalry_flag and rivalry_transition_ready:
                    if suggestion in {"0", "1/0"}:
                        suggestion = "0/1/3"
                        confidence_score -= 0.4
                    elif suggestion == "1":
                        suggestion = "1/3"
                        confidence_score -= 0.2

                # ARES_AWAY_FAVORITE_DEFENSE_EXPOSURE_GATE:
                # 客队热门且防线暴露时，必须保留主胜保护腿。
                if away_fav_def_exposure:
                    if suggestion == "0":
                        suggestion = "0/1/3"
                        confidence_score -= 0.7
                    elif suggestion == "1/0":
                        suggestion = "0/1/3"
                        confidence_score -= 0.5
                    elif suggestion == "1":
                        suggestion = "1/3"
                        confidence_score -= 0.3

                if low_event_home_fav:
                    if suggestion == "3":
                        suggestion = "3/1"
                        confidence_score -= 0.8
                    elif suggestion == "3/1":
                        confidence_score -= 0.4

                if away_recent_xg_protect:
                    if suggestion == "3":
                        suggestion = "3/1/0"
                        confidence_score -= 0.6
                    elif suggestion == "3/1":
                        suggestion = "3/1/0"
                        confidence_score -= 0.4
                    elif suggestion == "1":
                        suggestion = "1/0"
                        confidence_score -= 0.2

                if market_reversal_flag:
                    if favorite_retreat_not_reversal_gate:
                        if home_favorite:
                            if suggestion == "3":
                                suggestion = "3/1"
                                confidence_score -= 0.6
                            elif suggestion == "1/0":
                                suggestion = "3/1/0"
                                confidence_score -= 0.4
                        elif away_favorite:
                            if suggestion == "0":
                                suggestion = "1/0"
                                confidence_score -= 0.6
                            elif suggestion == "3/1":
                                suggestion = "3/1/0"
                                confidence_score -= 0.4
                    else:
                        if suggestion == "3":
                            suggestion = "1/0"
                            confidence_score -= 0.9
                        elif suggestion == "3/1":
                            suggestion = "1/0"
                            confidence_score -= 0.6

                if backup_gk_low_score_risk:
                    if suggestion == "3":
                        suggestion = "3/1"
                        confidence_score -= 0.5
                    elif suggestion == "1":
                        suggestion = "1/0"
                        confidence_score -= 0.3

                if direct_rival_home_pressure:
                    if suggestion in {"0", "1/0"}:
                        suggestion = "3/0"
                        confidence_score -= 0.2
                    elif suggestion == "1":
                        suggestion = "3/1"
                        confidence_score -= 0.2

                if recent_big_win_noise:
                    if suggestion == "0":
                        suggestion = "0/1"
                        confidence_score -= 0.6
                    elif suggestion == "0/1/3":
                        confidence_score -= 0.3

                if away_fav_injury_result:
                    if suggestion == "0":
                        suggestion = "0/1/3"
                        confidence_score -= 0.7
                    elif suggestion == "0/1":
                        suggestion = "0/1/3"
                        confidence_score -= 0.5

                if away_process_over_survival:
                    if suggestion == "1":
                        suggestion = "1/0"
                        confidence_score -= 0.3
                    elif suggestion == "3/1":
                        suggestion = "1/0"
                        confidence_score -= 0.4

                # NON_ELITE_HOT_FAVORITE_CAUTION / CONVERSION_BUBBLE_NO_SINGLE:
                # 非 elite 热门热度或终结泡沫触发时，禁止单挑。
                if non_elite_hot_favorite_caution or favorite_conversion_bubble:
                    if home_favorite and suggestion == "3":
                        suggestion = "3/1"
                        confidence_score -= 0.6
                    elif away_favorite and suggestion == "0":
                        suggestion = "1/0"
                        confidence_score -= 0.6

                if brand_price_not_low_enough:
                    if suggestion == "3":
                        suggestion = "3/1/0"
                        confidence_score -= 0.8
                    elif suggestion == "3/1":
                        suggestion = "3/1/0"
                        confidence_score -= 0.5
                    elif suggestion == "1":
                        suggestion = "3/1"
                        confidence_score -= 0.3

                if narrative_strength_cap:
                    if suggestion == "3":
                        suggestion = "3/1"
                        confidence_score -= 0.6
                    elif suggestion == "0":
                        suggestion = "1/0"
                        confidence_score -= 0.6
                    elif suggestion in {"3/1", "1/0"}:
                        confidence_score -= 0.3

                # AWAY_EURO_FATIGUE_MILD_FAVORITE:
                # 客队欧战夹击下，客胜结构至少降为对冲。
                if away_euro_fatigue_mild_gate and suggestion in {"0", "1/0"}:
                    home_xg = _safe_float(home_profile.get("avg_xG_last_5"))
                    away_xg = _safe_float(away_profile.get("avg_xG_last_5"))
                    if home_xg is not None and away_xg is not None and home_xg >= away_xg:
                        suggestion = "0/1/3"
                    else:
                        suggestion = "1/0"
                    confidence_score -= 0.5

                # AWAY_HALF_BALL_NO_UPGRADE_HOME_XG_RISK:
                # 客让半球未升盘且主队 xG 尚可，强制保留主胜深覆盖。
                if away_half_ball_no_upgrade_gate and suggestion in {"0", "1/0"}:
                    suggestion = "0/1/3"
                    confidence_score -= 0.5

                if compressed_table_draw_protection:
                    if suggestion == "3":
                        suggestion = "3/1"
                        confidence_score -= 0.5
                    elif suggestion == "0":
                        suggestion = "1/0"
                        confidence_score -= 0.5
                    elif suggestion == "3/0":
                        suggestion = "3/1/0"
                        confidence_score -= 0.4

                if relegation_away_draw:
                    if suggestion == "3":
                        suggestion = "3/1"
                        confidence_score -= 0.5
                    elif suggestion == "0":
                        suggestion = "0/1"
                        confidence_score -= 0.3

                if away_attack_cold:
                    if suggestion == "0":
                        suggestion = "0/1/3"
                        confidence_score -= 0.8
                    elif suggestion == "0/1":
                        suggestion = "0/1/3"
                        confidence_score -= 0.6

                if home_form_resistance:
                    if suggestion == "0":
                        suggestion = "0/1/3"
                        confidence_score -= 0.6
                    elif suggestion == "1":
                        suggestion = "1/3"
                        confidence_score -= 0.3

                # ARES_SURVIVAL_ESCAPE_GATE:
                # 即时逃生战意触发时，提升不败与客胜保护，避免被静态伤停惩罚过度压制。
                if survival_escape_signal:
                    if suggestion == "3":
                        suggestion = "3/1"
                    elif suggestion == "1":
                        suggestion = "1/0"
                    confidence_score = max(confidence_score, 2.5)

                # v2.2 STRONG_FAVORITE_VARIANCE_GUARD:
                # 强热门深盘降级，不否定结果方向但限制让球激进度。
                if strong_favorite_variance_guard:
                    if suggestion == "3":
                        suggestion = "3/1"
                    elif suggestion == "0":
                        suggestion = "1/0"
                    confidence_score -= 0.5

                # v2.2 UNDERDOG_REPAIR_WIN_ESCALATION:
                # 弱方修复场景下，直胜权重上调，不再只保平。
                if underdog_repair_win_escalation:
                    if suggestion == "3/1":
                        suggestion = "3/1/0"
                    elif suggestion == "1/0":
                        suggestion = "3/1/0"
                    elif suggestion == "1":
                        suggestion = "1/0" if away_favorite else "3/1"
                    confidence_score -= 0.2

                # v2.2 EURO_ASIAN_SPLIT_PRIORITY:
                # 欧亚分裂优先降让球置信并补保护腿。
                if euro_asian_split_priority:
                    if suggestion == "3":
                        suggestion = "3/1"
                    elif suggestion == "0":
                        suggestion = "1/0"
                    elif suggestion == "3/0":
                        suggestion = "3/1/0"
                    confidence_score -= 0.3

                # v2.2 LOCKED_TARGET_DEFLATION:
                # 已锁目标球队的让球与结果强度下调。
                if locked_target_deflation.get("handicap", 0.0) > 0:
                    confidence_score -= float(locked_target_deflation.get("result", 0.0)) * 2.0
                    if suggestion in {"3", "0"} and float(locked_target_deflation.get("handicap", 0.0)) >= 0.35:
                        suggestion = "3/1" if suggestion == "3" else "1/0"

                # ARES_SCHEDULE_SANDWICH_RULE:
                # 赛程风险默认只降置信，不可单独翻转方向。
                if schedule_risk_flag and not (home_xi_damage or away_xi_damage):
                    confidence_score -= 0.6

                # v2.2 SURVIVAL_WIN_CONVERSION_GATE:
                # 保级方从“保平”到“可直接赢”的条件转化。
                survival_side_profile = home_profile if survival_escape_signal else {}
                opponent_side_profile = away_profile if survival_escape_signal else {}
                if away_favorite and survival_escape_signal:
                    survival_side_profile = home_profile
                    opponent_side_profile = away_profile
                elif home_favorite and survival_escape_signal:
                    survival_side_profile = away_profile
                    opponent_side_profile = home_profile
                survival_gate_state = self._survival_win_conversion_gate(
                    survival_escape_signal=survival_escape_signal,
                    market_behavior=market_behavior,
                    context_flags=context_flags,
                    survival_side_profile=survival_side_profile,
                    opponent_profile=opponent_side_profile,
                )
                if survival_gate_state == "can_win":
                    if suggestion == "1":
                        suggestion = "1/0" if away_favorite else "3/1"
                    elif suggestion == "3/1":
                        suggestion = "3/1/0"
                    elif suggestion == "1/0":
                        suggestion = "3/1/0"
                elif survival_gate_state == "draw_only":
                    if suggestion == "3/1/0":
                        suggestion = "3/1" if home_favorite else "1/0"

                provisional_verdict = {
                    "suggestion": suggestion,
                    "readiness_level": readiness_level,
                    "is_low_confidence": bool(row.get("is_low_confidence")),
                    "is_insufficient_resilience": bool(row.get("is_insufficient_resilience")),
                    "best_edge": best_edge,
                    "edge_home": edge_home,
                    "edge_away": edge_away,
                }
                upgrade_reason = self._watchlist_upgrade_reason(provisional_verdict)
                if upgrade_reason:
                    confidence_score = max(confidence_score, 2.6)
                if confidence_score >= 5.0:
                    confidence = "high"
                elif confidence_score >= 2.5:
                    confidence = "medium"
                else:
                    confidence = "low"

                if schedule_risk_flag and confidence == "high":
                    confidence = "medium"

                if away_elite_conditional_only and suggestion == "0":
                    suggestion = "1/0"
                    confidence_score -= 0.5
                    reason += " 客场强队条件保护触发，单边客胜降为客不败。"

                # Match-level gate override: cap confidence upper bound.
                if confidence_cap == "medium" and confidence == "high":
                    confidence = "medium"
                elif confidence_cap == "low" and confidence in {"high", "medium"}:
                    confidence = "low"

                if no_single_pick_gate and suggestion in {"3", "0"}:
                    suggestion = "3/1" if suggestion == "3" else "1/0"
                    reason += " 触发 NO_SINGLE_PICK_GATE，禁止单挑，改为组合路径。"
                if no_single_pick_gate and confidence == "high":
                    confidence = "medium"
                if no_single_pick_gate and confidence == "medium":
                    confidence = "low"
                if postponed_market_time_decay_gate and confidence == "high":
                    confidence = "medium"
                if postponed_market_time_decay_gate and confidence == "medium":
                    confidence = "low"
                if home_away_integrity_gate:
                    suggestion = "skip"
                    confidence = "low"
                    reason += " 触发 HOME_AWAY_INTEGRITY_GATE：审计文本主客与 manifest 不一致，禁止执行。"
            elif isinstance(home_sd, (int, float)) and isinstance(away_sd, (int, float)):
                sd_gap = float(home_sd) - float(away_sd)
                if sd_gap >= 0.7:
                    suggestion = "3"
                elif sd_gap <= -0.7:
                    suggestion = "0"
                elif abs(sd_gap) <= 0.25:
                    suggestion = "1"
                elif sd_gap > 0:
                    suggestion = "3/1"
                else:
                    suggestion = "1/0"
                confidence = "low"
                confidence_score = 1.6
                reason = (
                    "缺少可计算 EV 概率，改用 S_dynamic 差值低置信兜底。"
                    f" ΔS={sd_gap:.2f}。"
                )
                if no_single_pick_gate and suggestion in {"3", "0"}:
                    suggestion = "3/1" if suggestion == "3" else "1/0"
                    reason += " 触发 NO_SINGLE_PICK_GATE，降为组合路径。"

                if isinstance(edge_home, (int, float)) and isinstance(edge_away, (int, float)):
                    reason = (
                        f"边际偏差: 主胜{edge_home if edge_home is not None else 0:+.1f}pp / "
                        f"客胜{edge_away if edge_away is not None else 0:+.1f}pp。"
                    )
                if isinstance(edge_home, (int, float)) and isinstance(edge_away, (int, float)) and edge_home < 0 and edge_away < 0:
                    reason += " 双侧均为负边际，仅保留观察价值。"
                if upgrade_reason:
                    reason += f" {upgrade_reason}"
                if row.get("is_low_confidence") or row.get("is_insufficient_resilience"):
                    reason += " 已施加质量折扣。"
                if conv_penalty > 0:
                    reason += " 转化率高位已做因子拆分降权（非自动反转）。"
                if reverse_block_note:
                    reason += f" {reverse_block_note}"
                if schedule_risk_flag and not (home_xi_damage or away_xi_damage):
                    reason += " 赛程三明治仅用于降置信，未直接翻转方向。"
                if rivalry_flag and rivalry_transition_ready:
                    reason += " 德比高方差门禁已生效：保留主胜路径并降低单挑置信。"
                if away_fav_def_exposure:
                    reason += " 客队热门防线污染门禁已生效：强制加入主胜保护腿。"
                if low_event_home_fav:
                    reason += " 低事件主队让球风险门禁已生效：禁止强单主胜。"
                if away_recent_xg_protect:
                    reason += " 客队近期xG保护门禁已生效：保留客胜路径。"
                if market_reversal_flag:
                    reason += " 市场反转门禁已生效：主胜方向降级并提升客队不败。"
                if favorite_retreat_not_reversal_gate:
                    reason += " 退盘不反向门禁已生效：仅降级，不机械反向删除热门胜路径。"
                if backup_gk_low_score_risk:
                    reason += " 门将缺席+低产环境门禁已生效：降低小比分/零封自信。"
                if direct_rival_home_pressure:
                    reason += " 直接卡位战主场压制门禁已生效：恢复主胜主线权重。"
                if recent_big_win_noise:
                    reason += " 强队客场上一场大胜降噪门禁已生效：降低客胜单挑权重。"
                if away_fav_injury_result:
                    reason += " 强队客场伤停胜负联动门禁已生效：不仅降让球，也降1X2置信。"
                if away_process_over_survival:
                    reason += " 客队过程优势压过主队保级战意门禁已生效：提升客胜保护。"
                if relegation_away_draw:
                    reason += " 市场退主+保级客队防平门禁已生效：平局权重上调。"
                if away_attack_cold:
                    reason += " 强队客场进攻冷却门禁已生效：禁止客胜单挑并保留主胜路径。"
                if home_form_resistance:
                    reason += " 主队主场韧性保护门禁已生效：主队不败路径上调。"
                if non_elite_hot_favorite_caution:
                    reason += " 非 elite 热门禁单门禁已生效：低赔热不能直接单选。"
                if favorite_conversion_bubble:
                    reason += " 终结泡沫禁单门禁已生效：高 conversion 不得单挑外推。"
                if away_euro_fatigue_mild_gate:
                    reason += " 欧战消耗加权门禁已生效：客场轻盘优势降级。"
                if away_half_ball_no_upgrade_gate:
                    reason += " 客让半球未升盘门禁已生效：主队乱战路径已纳入深覆盖。"
                if compressed_table_draw_protection:
                    reason += " 赛季末压缩积分带防平门禁已生效：浅热门场景强制加入平局保护。"
                if brand_price_not_low_enough:
                    reason += " 品牌强队赔率不够低门禁已生效：强题材但价格未压到预期区间，禁止单选并补深防。"
                if narrative_strength_cap:
                    reason += " 题材强度上限门禁已生效：战意/名气不能覆盖伤停与xG反证。"
                if home_away_integrity_gate:
                    reason += " 主客一致性门禁已生效：审计文本主客与 manifest 不一致，本场仅可观望。"
                if postponed_market_time_decay_gate:
                    reason += " 补赛旧初盘衰减门禁已生效：旧时间盘口降权，仅看临场增量。"
                if cup_final_context_gate:
                    reason += " 杯赛决赛上下文门禁已生效：弱化联赛战意，强化大赛执行力与阵容深度。"
                if survival_escape_signal:
                    reason += " 即时逃生战意门禁已生效：上调不败/客胜保护。"
                if suggestion == "skip":
                    reason += " 正向边际不足阈值，先观望。"

            posture = "TACTICAL_STALEMATE / WAIT"
            if suggestion in {"3", "0"} and confidence in {"medium", "high"}:
                posture = "TRUE_FAVORITE / EXECUTABLE"
            elif upgrade_reason:
                posture = "WATCHLIST_UPGRADED / PROBE_EXECUTION"
            elif readiness_level == "HOLD":
                posture = "DATA_WEAK / WATCHLIST"
            elif suggestion in {"3/1", "1/0", "3/0", "1"}:
                posture = "HIGH_VARIANCE / HEDGE_REQUIRED"
            elif row.get("is_insufficient_resilience"):
                posture = "INSUFFICIENT_RESILIENCE / DATA_GAP"

            market_decoupling = (
                f"市场-模型差(主/客): {edge_home if edge_home is not None else 0:+.1f}pp / "
                f"{edge_away if edge_away is not None else 0:+.1f}pp。"
            )
            physical_edge = (
                f"S_dynamic(主/客): "
                f"{home.get('s_dynamic') if home.get('s_dynamic') is not None else 'NA'} / "
                f"{away.get('s_dynamic') if away.get('s_dynamic') is not None else 'NA'}。"
            )
            if row.get("is_insufficient_resilience"):
                physical_edge += " 韧性样本不足，结果易被停机规则放大。"

            execution_plan = "回避，等待赛前新增阵容/伤停/战术证据。"
            if suggestion in {"3", "0"} and confidence in {"medium", "high"}:
                execution_plan = f"主执行 `{suggestion}`，低仓位保守跟随。"
            elif upgrade_reason:
                execution_plan = f"允许 `{suggestion}` 低仓位试探，严格临场二次确认。"
            elif suggestion in {"3/1", "1/0", "3/0", "1"}:
                execution_plan = f"仅可做 `{suggestion}` 对冲单，不做单边重仓。"

            invalidation = [
                "开赛前 90 分钟若主力伤停与当前结论方向冲突，立即降级为观察。",
                "盘口出现反向大幅波动（>8pp）且无新增证据支撑时，取消执行。",
            ]
            if row.get("is_insufficient_resilience"):
                invalidation.append("若无法补齐逆境样本，保持回避。")
            if "final_lineup_recheck_required" in required_controls:
                invalidation.append("临场首发未确认前禁止提升置信等级。")

            verdicts.append(
                {
                    "match_index": row.get("match_index"),
                    "match": f"{row.get('home_team')} vs {row.get('away_team')}",
                    "cn_match": _safe_text(row.get("cn_match")),
                    "suggestion": suggestion,
                    "confidence": confidence,
                    "posture": posture,
                    "market_decoupling": market_decoupling,
                    "physical_edge": physical_edge,
                    "execution_plan": execution_plan,
                    "invalidation_conditions": invalidation,
                    "reason": reason,
                    "source": "rule",
                    "readiness_level": readiness_level,
                    "upgrade_reason": upgrade_reason,
                    "is_low_confidence": bool(row.get("is_low_confidence")),
                    "is_insufficient_resilience": bool(row.get("is_insufficient_resilience")),
                    "risk_tags": risk_tags,
                    "rivalry_flag": rivalry_flag,
                    "away_favorite_defense_exposure": away_fav_def_exposure,
                    "low_event_home_favorite": low_event_home_fav,
                    "away_recent_xg_protection": away_recent_xg_protect,
                    "market_reversal_gate": market_reversal_flag,
                    "favorite_retreat_not_reversal_gate": favorite_retreat_not_reversal_gate,
                    "backup_gk_low_score_risk": backup_gk_low_score_risk,
                    "direct_rival_home_pressure": direct_rival_home_pressure,
                    "recent_big_win_noise_gate": recent_big_win_noise,
                    "away_favorite_injury_result_gate": away_fav_injury_result,
                    "away_process_edge_over_home_survival": away_process_over_survival,
                    "relegation_away_draw_protection": relegation_away_draw,
                    "away_favorite_attack_cold_gate": away_attack_cold,
                    "home_form_resistance_gate": home_form_resistance,
                    "non_elite_hot_favorite_caution": non_elite_hot_favorite_caution,
                    "away_euro_fatigue_mild_favorite_gate": away_euro_fatigue_mild_gate,
                    "favorite_conversion_bubble_no_single_gate": favorite_conversion_bubble,
                    "away_half_ball_no_upgrade_home_xg_risk_gate": away_half_ball_no_upgrade_gate,
                    "compressed_table_draw_protection_gate": compressed_table_draw_protection,
                    "brand_favorite_price_not_low_enough_gate": brand_price_not_low_enough,
                    "narrative_strength_cap_gate": narrative_strength_cap,
                    "strong_favorite_variance_guard_gate": strong_favorite_variance_guard,
                    "underdog_repair_win_escalation_gate": underdog_repair_win_escalation,
                    "euro_asian_split_priority_gate": euro_asian_split_priority,
                    "locked_target_deflation_gate": bool(locked_target_deflation.get("handicap", 0.0) > 0),
                    "survival_win_conversion_state": survival_gate_state,
                    "home_away_integrity_gate": home_away_integrity_gate,
                    "postponed_market_time_decay_gate": postponed_market_time_decay_gate,
                    "cup_final_context_gate": cup_final_context_gate,
                    "survival_escape_signal": survival_escape_signal,
                    "edge_home": edge_home,
                    "edge_away": edge_away,
                    "best_edge": best_edge,
                    "confidence_score": confidence_score,
                    "confidence_cap": confidence_cap or None,
                    "no_single_pick_gate": no_single_pick_gate,
                    "away_elite_conditional_only": away_elite_conditional_only,
                    "key_node_absence_risk": str(context_flags.get("key_node_absence_risk") or "UNKNOWN"),
                    "lineup_stability_precheck": context_flags.get("lineup_stability_precheck")
                    if isinstance(context_flags.get("lineup_stability_precheck"), dict)
                    else {"home": "UNKNOWN", "away": "UNKNOWN"},
                    "rotation_intensity": str(context_flags.get("rotation_intensity") or "UNKNOWN").upper(),
                    "result_direction": {
                        "order": suggestion,
                        "confidence": confidence,
                    },
                    "handicap_direction": {
                        "best_line": f"{'-' if home_favorite else '+' if away_favorite else ''}{favorite_deep_handicap:.2f}"
                        if favorite_deep_handicap
                        else "NA",
                        "avoid_line": "deep_handicap"
                        if (
                            strong_favorite_variance_guard
                            or bool(locked_target_deflation.get("handicap", 0.0) > 0)
                            or euro_asian_split_priority
                        )
                        else "NA",
                        "confidence": "medium"
                        if (
                            strong_favorite_variance_guard
                            or bool(locked_target_deflation.get("handicap", 0.0) > 0)
                            or euro_asian_split_priority
                        )
                        else confidence,
                    },
                    "game_script_direction": {
                        "main_script": "favorite_pressure"
                        if suggestion in {"3", "3/1", "3/1/0", "0", "1/0"}
                        else "balanced_or_draw",
                        "danger_script": "process_right_result_risk"
                        if strong_favorite_variance_guard
                        else "underdog_repair_live"
                        if underdog_repair_win_escalation
                        else "normal_variance",
                    },
                    "risk_labels": sorted(
                        set(
                            [
                                "PROCESS_RIGHT_RESULT_RISK" if strong_favorite_variance_guard else "",
                                "UNDERDOG_WIN_LIVE" if underdog_repair_win_escalation else "",
                                "EURO_ASIAN_SPLIT" if euro_asian_split_priority else "",
                                "LOCKED_TARGET_DEFLATION" if bool(locked_target_deflation.get("handicap", 0.0) > 0) else "",
                                f"SURVIVAL_WIN_CONVERSION_{survival_gate_state.upper()}"
                                if survival_gate_state and survival_gate_state != "none"
                                else "",
                            ]
                        )
                        - {""}
                    ),
                    "euro_asian_split": {
                        "euro_signal": "favorite_drift" if bool(market_behavior.get("market_retreat_against_favorite")) else "neutral",
                        "asian_signal": "favorite_support"
                        if (bool(market_behavior.get("handicap_deepen")) or bool(favorite_strengthened))
                        else "neutral",
                        "priority_read": "euro_first_caution" if euro_asian_split_priority else "aligned_or_no_split",
                        "final_effect": "reduce_handicap_confidence_and_add_cover" if euro_asian_split_priority else "none",
                    },
                    "ready_level": _safe_text(row.get("ready_level")) or None,
                }
            )

        preflight_status = _safe_text(diagnostics.get("status")) or "UNKNOWN"
        gate_status = _safe_text(gate_snapshot.get("issue_status")).upper()
        gate_selected = int(gate_snapshot.get("selected_matches") or len(matches))
        global_posture = "CAUTION"
        if (
            preflight_status == "READY"
            and gate_status in {"", "READY"}
            and low_conf_count == 0
            and insufficient_count == 0
            and smoke_count == 0
        ):
            global_posture = "READY"
        if gate_selected <= 0 or gate_status == "BLOCKED":
            global_posture = "HOLD"
        elif gate_status == "HOLD" or preflight_status == "HOLD" or insufficient_count > max(2, len(matches) // 2):
            global_posture = "HOLD"

        summary = (
            f"issue={self.issue} 共 {len(matches)} 场，低置信 {low_conf_count} 场，"
            f"韧性不足 {insufficient_count} 场，smoke 锚点 {smoke_count} 场。"
        )
        actionable = [v for v in verdicts if _safe_text(v.get("suggestion")) != "skip"]
        confidence_ok = [v for v in actionable if _safe_text(v.get("confidence")) in {"medium", "high"}]
        final_recommendation = "以观望为主，仅做小仓位实验单。"
        if global_posture == "READY":
            final_recommendation = (
                f"可按标准流程执行；当前可执行场次 {len(actionable)} 场，其中中高置信 {len(confidence_ok)} 场。"
            )
        elif global_posture == "HOLD":
            final_recommendation = (
                f"建议继续补证据（尤其韧性样本）后再做最终下单。当前仅 {len(confidence_ok)} 场达到中高置信。"
            )

        candidate_board = self._build_candidate_board(verdicts)
        if self.ops_mode:
            candidate_board = self._build_operational_candidate_board(verdicts)
        budget_stats = self._apply_dynamic_ticket_structure(verdicts, candidate_board)
        return {
            "mode": "rule_only",
            "executive_summary": summary,
            "global_posture": global_posture,
            "final_recommendation": final_recommendation,
            "risk_points": [
                f"Low confidence 场次: {low_conf_count}",
                f"Insufficient resilience 场次: {insufficient_count}",
                f"Smoke anchors 场次: {smoke_count}",
                f"Mapping 分布: {mapping_counter}",
            ],
            "next_actions": [
                "对 low confidence 与韧性不足场次追加阵容和伤停核验。",
                "如存在 smoke 锚点，先替换为真实锚点后再执行实盘推演。",
                "保留最终结论作为 issue 级封板记录。",
            ],
            "match_verdicts": verdicts,
            "candidate_board": candidate_board,
            "single_pick_dynamic_budget": budget_stats.get("single_pick_dynamic_budget", 0),
            "single_used": budget_stats.get("single_used", 0),
            "combo_used": budget_stats.get("combo_used", 0),
            "pass_used": budget_stats.get("pass_used", 0),
        }

    def _call_llm_openai(self, system_prompt: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        endpoint = f"{self.llm_base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.llm_api_key}",
            "Content-Type": "application/json",
        }
        request_payload = {
            "model": self.llm_model,
            "temperature": 0.2,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            "response_format": {"type": "json_object"},
        }
        try:
            resp = requests.post(endpoint, headers=headers, json=request_payload, timeout=self.llm_timeout_sec)
            resp.raise_for_status()
            data = resp.json()
            content = _safe_text(data.get("choices", [{}])[0].get("message", {}).get("content", ""))
            return _extract_json_object(content)
        except Exception as exc:
            if self.llm_provider == "deepseek":
                fallback_bases = ["https://api.deepseek.com", "https://api.deepseek.com/v1"]
                provider_tag = "DeepSeek"
            else:
                fallback_bases = ["https://api.openai.com/v1"]
                provider_tag = "OpenAI"

            for fallback_base in fallback_bases:
                fallback_endpoint = f"{fallback_base.rstrip('/')}/chat/completions"
                if endpoint == fallback_endpoint:
                    continue
                try:
                    resp = requests.post(
                        fallback_endpoint,
                        headers=headers,
                        json=request_payload,
                        timeout=self.llm_timeout_sec,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    content = _safe_text(data.get("choices", [{}])[0].get("message", {}).get("content", ""))
                    parsed = _extract_json_object(content)
                    if parsed is not None:
                        logger.info("LLM(%s) 已自动回退端点成功: %s", provider_tag, fallback_endpoint)
                        return parsed
                except Exception as retry_exc:
                    logger.warning(
                        "LLM(%s) 综合分析回退端点失败 endpoint=%s: %s",
                        provider_tag,
                        fallback_endpoint,
                        retry_exc,
                    )

            logger.warning("LLM(%s) 综合分析失败，回退规则输出: %s", provider_tag, exc)
            return None

    def _call_llm_gemini(self, system_prompt: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        endpoint = f"{self.llm_base_url}/models/{self.llm_model}:generateContent"
        headers = {"Content-Type": "application/json"}
        request_payload = {
            "systemInstruction": {"parts": [{"text": system_prompt}]},
            "contents": [{"role": "user", "parts": [{"text": json.dumps(payload, ensure_ascii=False)}]}],
            "generationConfig": {"temperature": 0.2, "responseMimeType": "application/json"},
        }
        try:
            resp = requests.post(
                endpoint,
                headers=headers,
                params={"key": self.llm_api_key},
                json=request_payload,
                timeout=self.llm_timeout_sec,
            )
            resp.raise_for_status()
            data = resp.json()
            content = _safe_text(
                data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            )
            return _extract_json_object(content)
        except Exception as exc:
            fallback_model = "gemini-1.5-flash"
            if self.llm_model != fallback_model:
                try:
                    retry_endpoint = f"{self.llm_base_url}/models/{fallback_model}:generateContent"
                    resp = requests.post(
                        retry_endpoint,
                        headers=headers,
                        params={"key": self.llm_api_key},
                        json=request_payload,
                        timeout=self.llm_timeout_sec,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    content = _safe_text(
                        data.get("candidates", [{}])[0]
                        .get("content", {})
                        .get("parts", [{}])[0]
                        .get("text", "")
                    )
                    parsed = _extract_json_object(content)
                    if parsed is not None:
                        logger.info("LLM(Gemini) 已自动回退模型 %s 成功。", fallback_model)
                        return parsed
                except Exception as retry_exc:
                    logger.warning(
                        "LLM(Gemini) 综合分析失败，主模型=%s 回退模型=%s 均失败: %s | %s",
                        self.llm_model,
                        fallback_model,
                        exc,
                        retry_exc,
                    )
                    return None
            logger.warning("LLM(Gemini) 综合分析失败，回退规则输出: %s", exc)
            return None

    def _llm_synthesize(self, inputs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self._llm_available():
            return None

        system_prompt = (
            "你是 Ares Prematch 封板分析器。请基于输入 JSON 给出 issue 级综合结论。"
            "必须只基于给定数据，不要虚构伤停或历史。输出 JSON 对象，字段："
            "executive_summary, global_posture(READY|CAUTION|HOLD), final_recommendation, "
            "risk_points(string[]), next_actions(string[]), "
            "match_verdicts([{match,cn_match,suggestion,confidence,posture,market_decoupling,physical_edge,execution_plan,invalidation_conditions,reason,source}])。"
            "suggestion 仅允许：skip,3,1,0,3/1,3/0,1/0。confidence 仅允许 low|medium|high。"
            "当证据不足时优先给 skip。posture 请使用英文大写风格标签（例如 TRUE_FAVORITE / HIGH_VARIANCE）。"
        )
        payload = {
            "issue": self.issue,
            "preflight_status": (inputs.get("diagnostics") or {}).get("status"),
            "quality_report_markdown": inputs.get("quality_text") or "",
            "matches": inputs.get("matches") or [],
            "manifest_stats": {
                "match_count": len((inputs.get("manifest") or {}).get("matches") or []),
                "mapping_sources": [
                    _safe_text(row.get("mapping_source"))
                    for row in ((inputs.get("manifest") or {}).get("matches") or [])
                ],
            },
        }
        if self.llm_provider == "gemini":
            result = self._call_llm_gemini(system_prompt, payload)
        else:
            result = self._call_llm_openai(system_prompt, payload)
        if not isinstance(result, dict):
            return None
        result["mode"] = "llm_assisted"
        return result

    @staticmethod
    def _is_llm_degenerate(
        normalized_llm: Dict[str, Any],
        inputs: Dict[str, Any],
        normalized_rule: Dict[str, Any],
    ) -> bool:
        verdicts = normalized_llm.get("match_verdicts") if isinstance(normalized_llm.get("match_verdicts"), list) else []
        if not verdicts:
            return True
        total = len(verdicts)
        actionable = sum(1 for row in verdicts if _safe_text(row.get("suggestion")).lower() != "skip")

        diagnostics = inputs.get("diagnostics") if isinstance(inputs.get("diagnostics"), dict) else {}
        preflight_status = _safe_text(diagnostics.get("status")).upper()
        low_conf_count = int(inputs.get("low_conf_count") or 0)
        insufficient_count = int(inputs.get("insufficient_count") or 0)

        # 只在输入质量不差时触发退化判断，避免真实 HOLD 场景被误覆盖
        quality_good = preflight_status in {"READY", "CAUTION"} and low_conf_count <= max(2, total // 5) and insufficient_count <= max(2, total // 5)
        if not quality_good:
            return False

        # 若 LLM 全 skip，但规则层有可执行信号，判定为退化输出
        if actionable == 0:
            rule_board = normalized_rule.get("candidate_board") if isinstance(normalized_rule.get("candidate_board"), dict) else {}
            rule_summary = rule_board.get("summary") if isinstance(rule_board.get("summary"), dict) else {}
            rule_actionable = int(rule_summary.get("稳胆", 0)) + int(rule_summary.get("博弈", 0))
            if rule_actionable > 0:
                return True
        return False

    @staticmethod
    def _normalize_result(result: Dict[str, Any], matches: List[Dict[str, Any]]) -> Dict[str, Any]:
        allowed_posture = {"READY", "CAUTION", "HOLD"}
        allowed_pick = {"skip", "3", "1", "0", "3/1", "3/0", "1/0", "3/1/0", "0/1/3", "0/1", "1/3"}
        allowed_conf = {"low", "medium", "high"}

        normalized = {
            "mode": _safe_text(result.get("mode")) or "rule_only",
            "executive_summary": _safe_text(result.get("executive_summary")),
            "global_posture": _safe_text(result.get("global_posture")).upper() or "CAUTION",
            "final_recommendation": _safe_text(result.get("final_recommendation")),
            "risk_points": result.get("risk_points") if isinstance(result.get("risk_points"), list) else [],
            "next_actions": result.get("next_actions") if isinstance(result.get("next_actions"), list) else [],
            "match_verdicts": result.get("match_verdicts") if isinstance(result.get("match_verdicts"), list) else [],
            "candidate_board": result.get("candidate_board") if isinstance(result.get("candidate_board"), dict) else {},
        }
        if normalized["global_posture"] not in allowed_posture:
            normalized["global_posture"] = "CAUTION"

        by_match = {
            f"{_safe_text(row.get('home_team'))} vs {_safe_text(row.get('away_team'))}": row
            for row in matches
        }
        gate_fields = [
            "rivalry_flag",
            "away_favorite_defense_exposure",
            "low_event_home_favorite",
            "away_recent_xg_protection",
            "market_reversal_gate",
            "favorite_retreat_not_reversal_gate",
            "backup_gk_low_score_risk",
            "direct_rival_home_pressure",
            "recent_big_win_noise_gate",
            "away_favorite_injury_result_gate",
            "away_process_edge_over_home_survival",
            "relegation_away_draw_protection",
            "away_favorite_attack_cold_gate",
            "home_form_resistance_gate",
            "non_elite_hot_favorite_caution",
            "away_euro_fatigue_mild_favorite_gate",
            "favorite_conversion_bubble_no_single_gate",
            "away_half_ball_no_upgrade_home_xg_risk_gate",
            "compressed_table_draw_protection_gate",
            "brand_favorite_price_not_low_enough_gate",
            "narrative_strength_cap_gate",
            "home_away_integrity_gate",
            "postponed_market_time_decay_gate",
            "cup_final_context_gate",
            "survival_escape_signal",
            "no_single_pick_gate",
            "away_elite_conditional_only",
            "single_pick_eligible",
            "single_pick_score",
            "single_pick_rank",
            "decision_type",
            "combo_type",
            "strong_favorite_variance_guard_gate",
            "underdog_repair_win_escalation_gate",
            "euro_asian_split_priority_gate",
            "locked_target_deflation_gate",
            "survival_win_conversion_state",
            "result_direction",
            "handicap_direction",
            "game_script_direction",
            "risk_labels",
            "euro_asian_split",
        ]
        fixed_verdicts: List[Dict[str, Any]] = []
        for item in normalized["match_verdicts"]:
            if not isinstance(item, dict):
                continue
            match_name = _safe_text(item.get("match"))
            source_row = by_match.get(match_name)
            cn_match = _safe_text(item.get("cn_match"))
            if not cn_match and source_row:
                cn_match = _safe_text(source_row.get("cn_match"))
            suggestion = _safe_text(item.get("suggestion")).lower() or "skip"
            if suggestion not in allowed_pick:
                suggestion = "skip"
            confidence = _safe_text(item.get("confidence")).lower() or "low"
            if confidence not in allowed_conf:
                confidence = "low"
            fixed_verdicts.append(
                {
                    "match": match_name,
                    "cn_match": cn_match,
                    "suggestion": suggestion,
                    "analysis_suggestion": _safe_text(item.get("analysis_suggestion")) or suggestion,
                    "final_suggestion": _safe_text(item.get("final_suggestion")) or suggestion,
                    "confidence": confidence,
                    "posture": _safe_text(item.get("posture")) or "TACTICAL_STALEMATE / WAIT",
                    "market_decoupling": _safe_text(item.get("market_decoupling")),
                    "physical_edge": _safe_text(item.get("physical_edge")),
                    "execution_plan": _safe_text(item.get("execution_plan")),
                    "invalidation_conditions": [
                        _safe_text(v)
                        for v in (item.get("invalidation_conditions") if isinstance(item.get("invalidation_conditions"), list) else [])
                        if _safe_text(v)
                    ],
                    "reason": _safe_text(item.get("reason")),
                    "source": _safe_text(item.get("source")) or normalized["mode"],
                    "readiness_level": _safe_text(item.get("readiness_level")) or _safe_text((source_row or {}).get("readiness_level")) or "READY",
                    "ready_level": _safe_text(item.get("ready_level")) or _safe_text((source_row or {}).get("ready_level")) or None,
                    "confidence_cap": _safe_text(item.get("confidence_cap")) or _safe_text((source_row or {}).get("confidence_cap")) or None,
                    "upgrade_reason": _safe_text(item.get("upgrade_reason")),
                    "is_low_confidence": bool(item.get("is_low_confidence")),
                    "is_insufficient_resilience": bool(item.get("is_insufficient_resilience")),
                    "candidate_tier": _safe_text(item.get("candidate_tier")),
                    "non_actionable": bool(item.get("non_actionable")),
                    "edge_home": item.get("edge_home"),
                    "edge_away": item.get("edge_away"),
                    "best_edge": item.get("best_edge"),
                    "confidence_score": item.get("confidence_score"),
                    **{k: item.get(k) for k in gate_fields if k in item},
                }
            )

        if not fixed_verdicts:
            for row in matches:
                fixed_verdicts.append(
                    {
                        "match": f"{row.get('home_team')} vs {row.get('away_team')}",
                        "cn_match": _safe_text(row.get("cn_match")),
                        "suggestion": "skip",
                        "analysis_suggestion": "skip",
                        "final_suggestion": "skip",
                        "confidence": "low",
                        "posture": "TACTICAL_STALEMATE / WAIT",
                        "market_decoupling": "",
                        "physical_edge": "",
                        "execution_plan": "回避。",
                        "invalidation_conditions": [],
                        "reason": "无有效综合结论，默认回避。",
                        "source": "fallback",
                        "readiness_level": "BLOCKED",
                        "upgrade_reason": "",
                        "is_low_confidence": True,
                        "is_insufficient_resilience": True,
                        "candidate_tier": "放弃",
                        "non_actionable": True,
                    }
                )
        normalized["match_verdicts"] = fixed_verdicts
        normalized["candidate_board"] = PrematchSynthesis._build_candidate_board(fixed_verdicts)
        budget_stats = PrematchSynthesis._apply_dynamic_ticket_structure(
            normalized["match_verdicts"],
            normalized["candidate_board"],
        )
        normalized["single_pick_dynamic_budget"] = budget_stats.get("single_pick_dynamic_budget", 0)
        normalized["single_used"] = budget_stats.get("single_used", 0)
        normalized["combo_used"] = budget_stats.get("combo_used", 0)
        normalized["pass_used"] = budget_stats.get("pass_used", 0)

        normalized["risk_points"] = [_safe_text(x) for x in normalized["risk_points"] if _safe_text(x)]
        normalized["next_actions"] = [_safe_text(x) for x in normalized["next_actions"] if _safe_text(x)]
        return normalized

    def _render_markdown(
        self,
        synthesis: Dict[str, Any],
        inputs: Dict[str, Any],
    ) -> str:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
        diagnostics = inputs.get("diagnostics") or {}
        low_conf_count = int(inputs.get("low_conf_count") or 0)
        insufficient_count = int(inputs.get("insufficient_count") or 0)
        manifest_matches = (inputs.get("manifest") or {}).get("matches") or []
        smoke_count = 0
        for match in manifest_matches:
            mode = _safe_text(match.get("manual_anchor_mode")).lower()
            notes = _safe_text(match.get("manual_anchor_notes")).lower()
            fbref_url = _safe_text(match.get("fbref_url")).lower()
            if mode == "smoke" or "[smoke]" in notes or fbref_url.startswith("https://anchor.local/"):
                smoke_count += 1

        lines: List[str] = []
        lines.append(f"# FINAL-{self.issue}-Prematch Synthesis")
        lines.append("")
        lines.append(f"- Updated At: {now}")
        lines.append(f"- Issue: `{self.issue}`")
        lines.append(f"- Synthesis Mode: `{synthesis.get('mode')}`")
        lines.append(f"- Synthesis Profile: `{'ops' if self.ops_mode else 'strict'}`")
        lines.append(f"- Scope: `{'Top5 Only' if self.top5_only else 'All Matches'}`")
        lines.append(f"- LLM Enabled: `{'yes' if self._llm_available() else 'no'}`")
        lines.append(f"- Preflight Status: `{_safe_text(diagnostics.get('status')) or 'UNKNOWN'}`")
        gate_snapshot = inputs.get("gate_snapshot") if isinstance(inputs.get("gate_snapshot"), dict) else {}
        if gate_snapshot:
            lines.append(f"- Gate Status: `{_safe_text(gate_snapshot.get('issue_status')) or 'UNKNOWN'}`")
        lines.append(f"- Low Confidence Reports: `{low_conf_count}`")
        lines.append(f"- Insufficient Resilience Reports: `{insufficient_count}`")
        lines.append(f"- Smoke Anchor Matches: `{smoke_count}`")
        candidate_board = synthesis.get("candidate_board") if isinstance(synthesis.get("candidate_board"), dict) else {}
        candidate_summary = candidate_board.get("summary") if isinstance(candidate_board.get("summary"), dict) else {}
        stable_count = int(candidate_summary.get("稳胆", 0))
        value_count = int(candidate_summary.get("博弈", 0))
        watch_count = int(candidate_summary.get("观察", 0))
        discard_count = int(candidate_summary.get("放弃", 0))
        lines.append(
            f"- Candidate Board: 稳胆 `{stable_count}` / "
            f"博弈 `{value_count}` / 观察 `{watch_count}` / 放弃 `{discard_count}`"
        )
        lines.append("")
        lines.append("## A层：执行开关")
        global_posture = _safe_text(synthesis.get("global_posture")).upper()
        execution_light = "YELLOW"
        execution_instruction = "允许小仓位试探，按候选池控制风险。"
        if global_posture == "HOLD" or (stable_count + value_count == 0):
            execution_light = "RED"
            execution_instruction = "暂停实盘，先完成最小补料清单后再重跑。"
        elif global_posture == "READY" and stable_count > 0:
            execution_light = "GREEN"
            execution_instruction = "可按候选池执行，优先稳胆，博弈位严格控仓。"
        lines.append(f"- 执行灯号: `{execution_light}`")
        lines.append(f"- Global Posture: `{_safe_text(synthesis.get('global_posture'))}`")
        lines.append(
            f"- Ticket Structure: budget(single)=`{int(synthesis.get('single_pick_dynamic_budget') or 0)}` / "
            f"used(single/combo/pass)=`{int(synthesis.get('single_used') or 0)}`/`{int(synthesis.get('combo_used') or 0)}`/`{int(synthesis.get('pass_used') or 0)}`"
        )
        lines.append(f"- 一句话指令: {execution_instruction}")
        lines.append(f"- Recommendation: {_safe_text(synthesis.get('final_recommendation')) or '暂无'}")
        lines.append("")
        lines.append("## Executive Summary")
        lines.append(_safe_text(synthesis.get("executive_summary")) or "暂无总结。")
        lines.append("")
        if execution_light == "RED":
            lines.append("## 最小补料清单")
            backlog: List[str] = []
            for row in synthesis.get("match_verdicts") or []:
                reason = _safe_text(row.get("reason")).lower()
                match_name = _safe_text(row.get("match")) or "-"
                if "insufficient resilience" in reason or "韧性" in reason:
                    backlog.append(f"{match_name}: 补逆境样本（先丢球/领先保分/70'后抗压）")
                elif "mapping" in reason or "map" in reason or "titan" in reason:
                    backlog.append(f"{match_name}: 校验映射与锚点来源（Understat/Titan/FBref）")
                elif "low confidence" in reason or "置信" in reason:
                    backlog.append(f"{match_name}: 补伤停与首发信息，并复核盘口异动")
                if len(backlog) >= 8:
                    break
            if not backlog:
                backlog = [
                    "先补 Team Archive 的 resilience_core 与 market_behavior_core 关键字段。",
                    "校验 unmapped/手工锚点后重跑 preflight 与 synthesis。",
                ]
            for item in backlog:
                lines.append(f"- {item}")
            lines.append("")
        lines.append("## Match Verdicts")
        lines.append("| Match | 中文对阵 | 建议 | 置信度 | Posture |")
        lines.append("| --- | --- | --- | --- | --- |")
        for row in synthesis.get("match_verdicts") or []:
            match_name = _safe_text(row.get("match")) or "-"
            cn_match = _safe_text(row.get("cn_match")) or "-"
            suggestion = _safe_text(row.get("final_suggestion")) or "skip"
            confidence = _safe_text(row.get("confidence")) or "low"
            posture = _safe_text(row.get("posture")) or "-"
            lines.append(f"| {match_name} | {cn_match} | `{suggestion}` | `{confidence}` | `{posture}` |")
        lines.append("")
        lines.append("## B层：候选池")
        tier_map = candidate_board.get("tiers") if isinstance(candidate_board.get("tiers"), dict) else {}
        for tier in ["稳胆", "博弈", "观察", "放弃"]:
            lines.append(f"### {tier}")
            lines.append("| Match | 中文对阵 | 建议 | 置信度 | 评分 |")
            lines.append("| --- | --- | --- | --- | --- |")
            items = tier_map.get(tier) if isinstance(tier_map.get(tier), list) else []
            if items:
                for item in items:
                    lines.append(
                        f"| {_safe_text(item.get('match')) or '-'} | {_safe_text(item.get('cn_match')) or '-'} | "
                        f"`{_safe_text(item.get('suggestion')) or 'skip'}` | `{_safe_text(item.get('confidence')) or 'low'}` | "
                        f"`{item.get('score', 0)}` |"
                    )
            else:
                lines.append("| - | - | `skip` | `low` | `0` |")
            lines.append("")

        lines.append("")
        lines.append("## Decision Narratives")
        for row in synthesis.get("match_verdicts") or []:
            match_name = _safe_text(row.get("match")) or "-"
            cn_match = _safe_text(row.get("cn_match")) or "-"
            suggestion = _safe_text(row.get("final_suggestion")) or "skip"
            analysis_suggestion = _safe_text(row.get("analysis_suggestion")) or suggestion
            confidence = _safe_text(row.get("confidence")) or "low"
            lines.append(f"### {match_name} ({cn_match or '-'})")
            lines.append(f"- Posture: `{_safe_text(row.get('posture')) or 'TACTICAL_STALEMATE / WAIT'}`")
            lines.append(f"- 决策落点: `{suggestion}` (`{confidence}`)")
            if analysis_suggestion != suggestion:
                lines.append(f"- 分析候选: `{analysis_suggestion}`（未进入执行池）")
            lines.append(f"- 市场解耦: {_safe_text(row.get('market_decoupling')) or _safe_text(row.get('reason')) or '-'}")
            lines.append(f"- 物理面: {_safe_text(row.get('physical_edge')) or '-'}")
            lines.append(f"- 执行建议: {_safe_text(row.get('execution_plan')) or '-'}")
            if _safe_text(row.get("upgrade_reason")):
                lines.append(f"- 升级依据: {_safe_text(row.get('upgrade_reason'))}")
            invalidation = row.get("invalidation_conditions") if isinstance(row.get("invalidation_conditions"), list) else []
            if invalidation:
                lines.append("- 反证条件:")
                for cond in invalidation:
                    lines.append(f"  - {cond}")
            lines.append(f"- 备注: {_safe_text(row.get('reason')) or '-'}")
            lines.append("")
        lines.append("")
        lines.append("## 回避原因分解")
        reason_counter = {"insufficient_resilience": 0, "low_confidence": 0, "ev_negative": 0, "other": 0}
        for row in synthesis.get("match_verdicts") or []:
            if not bool(row.get("non_actionable")):
                continue
            if row.get("is_insufficient_resilience"):
                reason_counter["insufficient_resilience"] += 1
            elif row.get("is_low_confidence"):
                reason_counter["low_confidence"] += 1
            else:
                reason = _safe_text(row.get("reason")).lower()
                if "ev" in reason or "边际偏差" in reason or "market" in reason or "市场" in reason:
                    reason_counter["ev_negative"] += 1
                else:
                    reason_counter["other"] += 1
        lines.append(f"- `insufficient_resilience`: `{reason_counter['insufficient_resilience']}`")
        lines.append(f"- `low_confidence`: `{reason_counter['low_confidence']}`")
        lines.append(f"- `ev_negative_or_market_gap`: `{reason_counter['ev_negative']}`")
        lines.append(f"- `other`: `{reason_counter['other']}`")
        lines.append("")
        if self.ops_mode:
            ops_watchlist = synthesis.get("ops_watchlist") if isinstance(synthesis.get("ops_watchlist"), list) else []
            lines.append("## Ops 观察候选（不改变主结论）")
            if ops_watchlist:
                lines.append("| Match | 中文对阵 | 候选 | 置信度 | 说明 |")
                lines.append("| --- | --- | --- | --- | --- |")
                for item in ops_watchlist:
                    lines.append(
                        f"| {_safe_text(item.get('match')) or '-'} | {_safe_text(item.get('cn_match')) or '-'} | "
                        f"`{_safe_text(item.get('suggestion')) or 'skip'}` | `{_safe_text(item.get('confidence')) or 'low'}` | "
                        f"{_safe_text(item.get('reason')) or '-'} |"
                    )
            else:
                lines.append("- 无。")
            lines.append("")
        lines.append("## Risk Points")
        risks = synthesis.get("risk_points") or []
        if risks:
            for risk in risks:
                lines.append(f"- {risk}")
        else:
            lines.append("- None")
        lines.append("")
        lines.append("## Next Actions")
        actions = synthesis.get("next_actions") or []
        if actions:
            for action in actions:
                lines.append(f"- {action}")
        else:
            lines.append("- None")
        lines.append("")
        lines.append("## Source Files")
        lines.append(f"- `{self.review_quality_path}`")
        lines.append(f"- `{self.diagnostics_path}`")
        lines.append(f"- `{self.manifest_path}`")
        return "\n".join(lines).strip() + "\n"

    def run(self) -> Dict[str, Any]:
        inputs = self._load_inputs()
        rule_result = self._build_rule_based_result(inputs)
        normalized_rule = self._normalize_result(rule_result, inputs.get("matches") or [])
        llm_result = self._llm_synthesize(inputs)
        if isinstance(llm_result, dict):
            normalized_llm = self._normalize_result(llm_result, inputs.get("matches") or [])
            if self._is_llm_degenerate(normalized_llm, inputs, normalized_rule):
                normalized = dict(normalized_rule)
                normalized["mode"] = "llm_fallback_rule"
                logger.warning("LLM 输出退化为全回避，已自动回退规则候选池。")
            else:
                normalized = normalized_llm
        else:
            normalized = normalized_rule
        if self.ops_mode:
            verdicts_for_ops = normalized_rule.get("match_verdicts") if isinstance(normalized_rule.get("match_verdicts"), list) else []
            ops_board = self._build_operational_candidate_board(verdicts_for_ops or [])
            normalized["ops_watchlist"] = self._build_ops_watchlist(ops_board)
            normalized["mode"] = f"{_safe_text(normalized.get('mode')) or 'rule_only'}+ops"
        markdown = self._render_markdown(normalized, inputs)

        if not self.stdout_only:
            self.out_md_path.parent.mkdir(parents=True, exist_ok=True)
            self.out_md_path.write_text(markdown, encoding="utf-8")
        payload = {
            "issue": self.issue,
            "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ"),
            "inputs_summary": {
                "accepted_reports": len(inputs.get("matches") or []),
                "low_confidence_reports": int(inputs.get("low_conf_count") or 0),
                "insufficient_resilience_reports": int(inputs.get("insufficient_count") or 0),
            },
            "result": normalized,
        }
        if not self.stdout_only:
            self.out_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info("Prematch synthesis 写入完成 -> %s", self.out_md_path)
            logger.info("Prematch synthesis JSON 写入完成 -> %s", self.out_json_path)
        else:
            print(markdown)
        return {
            "md": str(self.out_md_path),
            "json": str(self.out_json_path),
            "mode": normalized.get("mode"),
            "global_posture": normalized.get("global_posture"),
            "matches": len(normalized.get("match_verdicts") or []),
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="Issue 级 prematch 推演综合收口（LLM + 规则兜底）")
    parser.add_argument("--issue", required=True, help="体彩期号，如 26066")
    parser.add_argument("--force-rule", action="store_true", help="禁用 LLM，强制规则兜底")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="",
        help="可选：覆盖输出目录（默认写入 Vault 期号目录的 02_Special_Analyses）",
    )
    parser.add_argument("--stdout-only", action="store_true", help="仅打印结果，不落盘文件")
    parser.add_argument("--top5-only", action="store_true", help="仅汇总五大联赛场次（EPL/LaLiga/Bundesliga/SerieA/Ligue1）")
    parser.add_argument("--ops-mode", action="store_true", help="运营模式：在严格结论外提供博弈候选池兜底排序")
    parser.add_argument(
        "--include-rejected-caution",
        action="store_true",
        help="将 REJECTED prematch 报告作为 HOLD/低置信 CAUTION 记录纳入最终汇总",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir).expanduser() if _safe_text(args.output_dir) else None
    runner = PrematchSynthesis(
        issue=args.issue,
        force_rule=args.force_rule,
        output_dir=output_dir,
        stdout_only=args.stdout_only,
        top5_only=args.top5_only,
        ops_mode=args.ops_mode,
        include_rejected_caution=args.include_rejected_caution,
    )
    summary = runner.run()
    print("[summary]")
    print(f"issue={args.issue}")
    print(f"mode={summary['mode']}")
    print(f"global_posture={summary['global_posture']}")
    print(f"matches={summary['matches']}")
    print(f"markdown={summary['md']}")
    print(f"json={summary['json']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
