import argparse
import json
import logging
import os
import re
from datetime import datetime
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


class PrematchSynthesis:
    def __init__(
        self,
        issue: str,
        force_rule: bool = False,
        output_dir: Optional[Path] = None,
        stdout_only: bool = False,
        top5_only: bool = False,
    ):
        self.issue = str(issue)
        self.force_rule = force_rule
        self.stdout_only = stdout_only
        self.top5_only = top5_only
        self.repo_root = Path(__file__).resolve().parent.parent.parent
        load_dotenv_into_env(self.repo_root)

        vault_env = _safe_text(os.getenv("ARES_VAULT_PATH"))
        if not vault_env:
            raise EnvironmentError("未检测到 ARES_VAULT_PATH，无法生成 Vault 最终收口报告。")
        self.vault_root = Path(normalize_vault_path(vault_env)).expanduser()

        self.issue_root = self.vault_root / "03_Match_Audits" / self.issue
        self.review_dir = self.issue_root / "03_Review_Reports"
        self.prematch_dir = self.issue_root / "01_Prematch_Audits"
        self.manifest_path = (
            self.vault_root / "04_RAG_Raw_Data" / "Cold_Data_Lake" / f"{self.issue}_dispatch_manifest.json"
        )
        self.diagnostics_path = self.issue_root / f"Audit-{self.issue}-team-diagnostics.json"
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

    def _load_inputs(self) -> Dict[str, Any]:
        if not self.review_dir.exists():
            raise FileNotFoundError(f"目录不存在: {self.review_dir}")
        manifest = self._load_json(self.manifest_path) if self.manifest_path.exists() else {}
        diagnostics = self._load_json(self.diagnostics_path) if self.diagnostics_path.exists() else {}
        quality_text = self.review_quality_path.read_text(encoding="utf-8") if self.review_quality_path.exists() else ""
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

        accepted_files = _extract_section_bullets(quality_text, "Accepted Prematch Reports")
        low_conf_files = _extract_section_bullets(quality_text, "Low Confidence Reports")
        insufficient_files = _extract_section_bullets(quality_text, "Insufficient Resilience Data")

        if not accepted_files and self.prematch_dir.exists():
            accepted_files = sorted(path.name for path in self.prematch_dir.glob("Audit-*.md"))
        if self.top5_only:
            filtered: List[str] = []
            for filename in accepted_files:
                m = re.search(rf"Audit-{re.escape(self.issue)}-(\d+)-", filename)
                if not m:
                    continue
                idx = int(m.group(1))
                if idx in top5_indices:
                    filtered.append(filename)
            accepted_files = filtered
            low_conf_files = [f for f in low_conf_files if f in set(accepted_files)]
            insufficient_files = [f for f in insufficient_files if f in set(accepted_files)]

        match_payloads: List[Dict[str, Any]] = []
        for filename in accepted_files:
            path = self.prematch_dir / filename
            if not path.exists():
                continue
            parsed = self._parse_prematch_audit(path)
            parsed["is_low_confidence"] = filename in set(low_conf_files)
            parsed["is_insufficient_resilience"] = filename in set(insufficient_files)
            match_payloads.append(parsed)

        return {
            "manifest": manifest,
            "diagnostics": diagnostics,
            "quality_text": quality_text,
            "matches": match_payloads,
            "low_conf_count": len(low_conf_files),
            "insufficient_count": len(insufficient_files),
            "top5_mode": self.top5_only,
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
        title = _safe_text(next((line for line in text.splitlines() if line.startswith("# ")), ""))
        title_match = re.search(r"# Ares Prematch Audit - Issue (\d+) - (.+?) vs (.+)$", title)
        home_team = _safe_text(title_match.group(2)) if title_match else ""
        away_team = _safe_text(title_match.group(3)) if title_match else ""

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
        for block in re.finditer(r"## (Home|Away) - ([^\n]+)\n(.*?)(?=\n## |\Z)", text, flags=re.S):
            side = _safe_text(block.group(1))
            team_name = _safe_text(block.group(2))
            body = block.group(3)
            s_dynamic = None
            m_sd = re.search(r"- S_dynamic:\s*`([^`]+)`", body)
            if m_sd:
                s_dynamic = _parse_first_float(m_sd.group(1))
            conclusion = ""
            m_con = re.search(r"- Prematch 结论:\s*`([^`]+)`", body)
            if m_con:
                conclusion = _safe_text(m_con.group(1))
            decision = ""
            m_decision = re.search(r"- 决策:\s*(.+)", body)
            if m_decision:
                decision = _safe_text(m_decision.group(1))
            market_prob = None
            model_prob = None
            m_ev = re.search(r"- EV:\s*`[^`]*`\s*\|\s*市场\s*`([^`]+)`\s*/\s*模型\s*`([^`]+)`", body)
            if m_ev:
                market_prob = _parse_first_float(m_ev.group(1))
                model_prob = _parse_first_float(m_ev.group(2))

            team_sections.append(
                {
                    "side": side,
                    "team": team_name,
                    "s_dynamic": s_dynamic,
                    "conclusion": conclusion,
                    "decision": decision,
                    "market_prob": market_prob,
                    "model_prob": model_prob,
                }
            )

        return {
            "file": path.name,
            "match_index": match_index,
            "home_team": home_team,
            "away_team": away_team,
            "cn_match": cn_match,
            "mapping_source": mapping_source,
            "understat_id": understat_id,
            "odds": odds,
            "teams": team_sections,
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

        return round(score, 2)

    @classmethod
    def _candidate_tier(cls, verdict: Dict[str, Any]) -> str:
        suggestion = _safe_text(verdict.get("suggestion")).lower()
        score = cls._candidate_score(verdict)
        is_low_conf = bool(verdict.get("is_low_confidence"))
        is_insufficient = bool(verdict.get("is_insufficient_resilience"))
        if suggestion in {"3", "0"} and score >= 4.0 and not is_low_conf and not is_insufficient:
            return "稳胆"
        if suggestion != "skip" and score >= 1.0:
            return "博弈"
        return "放弃"

    @classmethod
    def _build_candidate_board(cls, verdicts: List[Dict[str, Any]]) -> Dict[str, Any]:
        tiers: Dict[str, List[Dict[str, Any]]] = {"稳胆": [], "博弈": [], "放弃": []}
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
                "放弃": len(tiers["放弃"]),
            },
        }

    def _build_rule_based_result(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        diagnostics = inputs.get("diagnostics") or {}
        matches = inputs.get("matches") or []
        low_conf_count = int(inputs.get("low_conf_count") or 0)
        insufficient_count = int(inputs.get("insufficient_count") or 0)
        manifest_matches = (inputs.get("manifest") or {}).get("matches") or []

        mapping_counter: Dict[str, int] = {}
        smoke_count = 0
        for match in manifest_matches:
            source = _safe_text(match.get("mapping_source")).lower() or "unknown"
            mapping_counter[source] = mapping_counter.get(source, 0) + 1
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

            home_market = home.get("market_prob")
            away_market = away.get("market_prob")
            home_model = home.get("model_prob")
            away_model = away.get("model_prob")

            edge_home = None
            edge_away = None
            if isinstance(home_model, (int, float)) and isinstance(home_market, (int, float)):
                edge_home = float(home_model) - float(home_market)
            if isinstance(away_model, (int, float)) and isinstance(away_market, (int, float)):
                edge_away = float(away_model) - float(away_market)

            suggestion = "skip"
            confidence = "low"
            reason = "缺少可计算的市场/模型偏差，暂归为观望。"
            confidence_score = 1.0

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
                elif suggestion in {"1", "3/1", "1/0"}:
                    confidence_score = 2.5
                else:
                    confidence_score = 1.0
                if row.get("is_low_confidence"):
                    confidence_score -= 1.5
                if row.get("is_insufficient_resilience"):
                    confidence_score -= 2.0
                if confidence_score >= 5.0:
                    confidence = "high"
                elif confidence_score >= 2.5:
                    confidence = "medium"
                else:
                    confidence = "low"

                reason = (
                    f"边际偏差: 主胜{edge_home if edge_home is not None else 0:+.1f}pp / "
                    f"客胜{edge_away if edge_away is not None else 0:+.1f}pp。"
                )
                if row.get("is_low_confidence") or row.get("is_insufficient_resilience"):
                    reason += " 已施加质量折扣。"
                if suggestion == "skip":
                    reason += " 正向边际不足阈值，先观望。"

            posture = "TACTICAL_STALEMATE / WAIT"
            if suggestion in {"3", "0"} and confidence in {"medium", "high"}:
                posture = "TRUE_FAVORITE / EXECUTABLE"
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
            elif suggestion in {"3/1", "1/0", "3/0", "1"}:
                execution_plan = f"仅可做 `{suggestion}` 对冲单，不做单边重仓。"

            invalidation = [
                "开赛前 90 分钟若主力伤停与当前结论方向冲突，立即降级为观察。",
                "盘口出现反向大幅波动（>8pp）且无新增证据支撑时，取消执行。",
            ]
            if row.get("is_insufficient_resilience"):
                invalidation.append("若无法补齐逆境样本，保持回避。")

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
                    "is_low_confidence": bool(row.get("is_low_confidence")),
                    "is_insufficient_resilience": bool(row.get("is_insufficient_resilience")),
                }
            )

        preflight_status = _safe_text(diagnostics.get("status")) or "UNKNOWN"
        global_posture = "CAUTION"
        if preflight_status == "READY" and low_conf_count == 0 and insufficient_count == 0 and smoke_count == 0:
            global_posture = "READY"
        if preflight_status == "HOLD" or insufficient_count > max(2, len(matches) // 2):
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
        allowed_pick = {"skip", "3", "1", "0", "3/1", "3/0", "1/0"}
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
                    "is_low_confidence": bool(item.get("is_low_confidence")),
                    "is_insufficient_resilience": bool(item.get("is_insufficient_resilience")),
                }
            )

        if not fixed_verdicts:
            for row in matches:
                fixed_verdicts.append(
                    {
                        "match": f"{row.get('home_team')} vs {row.get('away_team')}",
                        "cn_match": _safe_text(row.get("cn_match")),
                        "suggestion": "skip",
                        "confidence": "low",
                        "posture": "TACTICAL_STALEMATE / WAIT",
                        "market_decoupling": "",
                        "physical_edge": "",
                        "execution_plan": "回避。",
                        "invalidation_conditions": [],
                        "reason": "无有效综合结论，默认回避。",
                        "source": "fallback",
                        "is_low_confidence": True,
                        "is_insufficient_resilience": True,
                    }
                )
        normalized["match_verdicts"] = fixed_verdicts
        normalized["candidate_board"] = PrematchSynthesis._build_candidate_board(fixed_verdicts)

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
        lines.append(f"- Scope: `{'Top5 Only' if self.top5_only else 'All Matches'}`")
        lines.append(f"- LLM Enabled: `{'yes' if self._llm_available() else 'no'}`")
        lines.append(f"- Preflight Status: `{_safe_text(diagnostics.get('status')) or 'UNKNOWN'}`")
        lines.append(f"- Low Confidence Reports: `{low_conf_count}`")
        lines.append(f"- Insufficient Resilience Reports: `{insufficient_count}`")
        lines.append(f"- Smoke Anchor Matches: `{smoke_count}`")
        candidate_board = synthesis.get("candidate_board") if isinstance(synthesis.get("candidate_board"), dict) else {}
        candidate_summary = candidate_board.get("summary") if isinstance(candidate_board.get("summary"), dict) else {}
        lines.append(
            f"- Candidate Board: 稳胆 `{int(candidate_summary.get('稳胆', 0))}` / "
            f"博弈 `{int(candidate_summary.get('博弈', 0))}` / 放弃 `{int(candidate_summary.get('放弃', 0))}`"
        )
        lines.append("")
        lines.append("## Executive Summary")
        lines.append(_safe_text(synthesis.get("executive_summary")) or "暂无总结。")
        lines.append("")
        lines.append("## Final Verdict")
        lines.append(f"- Global Posture: `{_safe_text(synthesis.get('global_posture'))}`")
        lines.append(f"- Recommendation: {_safe_text(synthesis.get('final_recommendation')) or '暂无'}")
        lines.append("")
        lines.append("## Match Verdicts")
        lines.append("| Match | 中文对阵 | 建议 | 置信度 | Posture |")
        lines.append("| --- | --- | --- | --- | --- |")
        for row in synthesis.get("match_verdicts") or []:
            match_name = _safe_text(row.get("match")) or "-"
            cn_match = _safe_text(row.get("cn_match")) or "-"
            suggestion = _safe_text(row.get("suggestion")) or "skip"
            confidence = _safe_text(row.get("confidence")) or "low"
            posture = _safe_text(row.get("posture")) or "-"
            lines.append(f"| {match_name} | {cn_match} | `{suggestion}` | `{confidence}` | `{posture}` |")
        lines.append("")
        lines.append("## Candidate Board")
        tier_map = candidate_board.get("tiers") if isinstance(candidate_board.get("tiers"), dict) else {}
        for tier in ["稳胆", "博弈", "放弃"]:
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
            suggestion = _safe_text(row.get("suggestion")) or "skip"
            confidence = _safe_text(row.get("confidence")) or "low"
            lines.append(f"### {match_name} ({cn_match or '-'})")
            lines.append(f"- Posture: `{_safe_text(row.get('posture')) or 'TACTICAL_STALEMATE / WAIT'}`")
            lines.append(f"- 决策落点: `{suggestion}` (`{confidence}`)")
            lines.append(f"- 市场解耦: {_safe_text(row.get('market_decoupling')) or _safe_text(row.get('reason')) or '-'}")
            lines.append(f"- 物理面: {_safe_text(row.get('physical_edge')) or '-'}")
            lines.append(f"- 执行建议: {_safe_text(row.get('execution_plan')) or '-'}")
            invalidation = row.get("invalidation_conditions") if isinstance(row.get("invalidation_conditions"), list) else []
            if invalidation:
                lines.append("- 反证条件:")
                for cond in invalidation:
                    lines.append(f"  - {cond}")
            lines.append(f"- 备注: {_safe_text(row.get('reason')) or '-'}")
            lines.append("")
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
    args = parser.parse_args()

    output_dir = Path(args.output_dir).expanduser() if _safe_text(args.output_dir) else None
    runner = PrematchSynthesis(
        issue=args.issue,
        force_rule=args.force_rule,
        output_dir=output_dir,
        stdout_only=args.stdout_only,
        top5_only=args.top5_only,
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
