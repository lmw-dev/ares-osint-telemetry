import os
import json
import logging
import argparse
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import yaml
import requests
from bs4 import BeautifulSoup
from audit_router import AuditRouter
from team_archive_paths import candidate_team_filenames, league_archive_dir

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AresTelemetry.PostMatch")

INTEL_BASE_DEFAULTS: Dict[str, Any] = {
    "manager_doctrine": "Unknown",
    "market_sentiment": "Neutral",
    "key_node_dependency": [],
    "recent_news_summary": "",
}

PHYSICAL_REALITY_DEFAULTS: Dict[str, Any] = {
    "avg_xG_last_5": 1.0,
    "conversion_efficiency": 0.05,
    "defensive_leakage": 0.5,
    "variance_history": [],
    "actual_tactical_entropy": 0.40,
}

REALITY_GAP_DEFAULTS: Dict[str, Any] = {
    "bias_type": "Aligned",
    "S_dynamic_modifier": 0.0,
}

ALLOWED_BIAS_TYPES = {"Fame_Trap", "Underestimated", "Aligned"}
SUPPORTED_LLM_PROVIDERS = {"openai", "gemini"}


def load_dotenv_into_env(base_dir: Path) -> None:
    env_path = base_dir / ".env"
    if not env_path.exists():
        return
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if not key:
                    continue
                if value and value[0] == value[-1] and value[0] in ("'", '"'):
                    value = value[1:-1]
                os.environ.setdefault(key, value)
    except Exception:
        # main入口预加载失败时不阻断，由类内逻辑再次兜底
        pass


class MatchTelemetryPipeline:
    _dotenv_loaded = False

    def __init__(
        self,
        issue: str,
        match_id: str,
        source: str = "auto",
        fbref_url: Optional[str] = None,
        official_score: Optional[str] = None,
        league: Optional[str] = None,
        expected_match: Optional[Dict[str, Any]] = None,
    ):
        self.issue = issue
        self.match_id = match_id
        self.source = source
        self.fbref_url = fbref_url or (match_id if "fbref.com" in str(match_id).lower() else None)
        self.official_score = self._normalize_score(official_score) if official_score else None
        self.league = league
        self.expected_match = expected_match or {}
        if official_score and not self.official_score:
            raise ValueError(f"official_score 格式非法: {official_score}（应为 2-1）")
        
        # 路径配置
        self.base_dir = Path(__file__).resolve().parent.parent.parent

        self._load_project_env_file()
        self.vault_path = os.getenv("ARES_VAULT_PATH")
        if self.vault_path:
            normalized_vault_path = self._normalize_vault_path(self.vault_path)
            if normalized_vault_path != self.vault_path:
                logger.info(f"检测到转义路径，已规范化 ARES_VAULT_PATH: {normalized_vault_path}")
            self.vault_path = normalized_vault_path
        if self.vault_path:
            vault_root = Path(self.vault_path)
            self.issue_audit_dir = vault_root / "03_Match_Audits" / str(self.issue)
            self.issue_postmatch_dir = self.issue_audit_dir / "04_Postmatch_Telemetry"
            self.cold_data_dir = vault_root / "04_RAG_Raw_Data" / "Cold_Data_Lake"
            self.hot_reports_dir = self.issue_postmatch_dir
            self.team_archives_dir = vault_root / "02_Team_Archives"
            self.team_archives_runtime_dir = self.team_archives_dir / "_Postmatch_Runtime"
        else:
            logger.warning("未检测到环境变量 ARES_VAULT_PATH，将降级写入项目目录。")
            self.issue_audit_dir = self.base_dir / "draft_audits" / str(self.issue)
            self.issue_postmatch_dir = self.issue_audit_dir / "04_Postmatch_Telemetry"
            self.cold_data_dir = self.base_dir / "raw_reports"
            self.hot_reports_dir = self.issue_postmatch_dir
            self.team_archives_dir = self.base_dir / "02_Team_Archives"
            self.team_archives_runtime_dir = self.team_archives_dir / "_Postmatch_Runtime"

        self.cold_data_dir.mkdir(parents=True, exist_ok=True)
        self.issue_audit_dir.mkdir(parents=True, exist_ok=True)
        self.hot_reports_dir.mkdir(parents=True, exist_ok=True)
        self.team_archives_dir.mkdir(parents=True, exist_ok=True)
        self.team_archives_runtime_dir.mkdir(parents=True, exist_ok=True)
        
        # 加载队名映射
        self.alias_map = self._load_team_alias_map()
        self._load_llm_runtime_config()

    def _load_team_alias_map(self) -> Dict[str, str]:
        map_path = Path(__file__).resolve().parent / "team_alias_map.json"
        try:
            with open(map_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载 team_alias_map.json 失败 (可能文件不存在): {e}")
            return {}

    def _load_project_env_file(self) -> None:
        """
        加载项目根目录 .env 到进程环境变量（仅在当前进程生效）。
        若变量已存在，不覆盖外部环境注入值。
        """
        if MatchTelemetryPipeline._dotenv_loaded:
            return

        env_path = self.base_dir / ".env"
        if not env_path.exists():
            MatchTelemetryPipeline._dotenv_loaded = True
            return

        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for raw_line in f:
                    line = raw_line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()

                    if not key:
                        continue
                    if value and value[0] == value[-1] and value[0] in ("'", '"'):
                        value = value[1:-1]
                    os.environ.setdefault(key, value)
            logger.info(f"检测到并加载 .env 配置: {env_path}")
        except Exception as e:
            logger.warning(f"加载 .env 失败，将继续使用系统环境变量: {e}")
        finally:
            MatchTelemetryPipeline._dotenv_loaded = True

    def _load_llm_runtime_config(self) -> None:
        self.llm_enabled = str(os.getenv("ARES_USE_LLM_BACKFILL", "0")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        provider = str(os.getenv("ARES_LLM_PROVIDER", "openai")).strip().lower()
        if provider not in SUPPORTED_LLM_PROVIDERS:
            logger.warning("未知 ARES_LLM_PROVIDER=%s，已回退为 openai。", provider)
            provider = "openai"
        self.llm_provider = provider

        common_api_key = str(os.getenv("ARES_LLM_API_KEY", "")).strip()
        if self.llm_provider == "gemini":
            provider_api_key = str(os.getenv("GEMINI_API_KEY", "")).strip() or str(os.getenv("GOOGLE_API_KEY", "")).strip()
            default_base_url = "https://generativelanguage.googleapis.com/v1beta"
            default_model = "gemini-1.5-flash"
        else:
            provider_api_key = str(os.getenv("OPENAI_API_KEY", "")).strip()
            default_base_url = "https://api.openai.com/v1"
            default_model = "gpt-4o-mini"

        self.llm_api_key = common_api_key or provider_api_key
        base_url_raw = (
            str(os.getenv("ARES_LLM_BASE_URL", "")).strip()
            or str(os.getenv("ARES_LLM_BAE_URL", "")).strip()
        )
        self.llm_base_url = (base_url_raw or default_base_url).rstrip("/")
        self.llm_model = str(os.getenv("ARES_LLM_MODEL", default_model)).strip()
        self.llm_timeout_sec = int(os.getenv("ARES_LLM_TIMEOUT_SEC", "20"))
        self.llm_min_confidence = float(os.getenv("ARES_LLM_MIN_CONFIDENCE", "0.6"))
        self.llm_max_entropy_delta = float(os.getenv("ARES_LLM_MAX_ENTROPY_DELTA", "0.05"))

        if self.llm_enabled and not self.llm_api_key:
            if self.llm_provider == "gemini":
                key_hint = "ARES_LLM_API_KEY/GEMINI_API_KEY"
            else:
                key_hint = "ARES_LLM_API_KEY/OPENAI_API_KEY"
            logger.warning("ARES_USE_LLM_BACKFILL=1 但未检测到 %s，将回退规则判定。", key_hint)

    def _llm_available(self) -> bool:
        return bool(self.llm_enabled and self.llm_api_key and self.llm_model)

    @staticmethod
    def _looks_like_understat_id(match_id: str) -> bool:
        return str(match_id).strip().isdigit()

    @staticmethod
    def _safe_float(value: str) -> Optional[float]:
        if value is None:
            return None
        txt = str(value).strip()
        if not txt:
            return None
        num = re.sub(r"[^0-9.\-]", "", txt)
        if not num:
            return None
        try:
            return float(num)
        except Exception:
            return None

    @staticmethod
    def _safe_int(value: str) -> Optional[int]:
        f = MatchTelemetryPipeline._safe_float(value)
        if f is None:
            return None
        return int(round(f))

    def _extract_pair_from_row(self, soup: BeautifulSoup, labels: List[str]) -> Tuple[Optional[str], Optional[str]]:
        for tr in soup.select("tr"):
            th = tr.select_one("th")
            if not th:
                continue
            key = th.get_text(" ", strip=True).lower()
            if not any(label.lower() in key for label in labels):
                continue

            cells = [td.get_text(" ", strip=True) for td in tr.select("td")]
            if len(cells) >= 2:
                return cells[0], cells[1]
        return None, None

    @staticmethod
    def _normalize_vault_path(path_text: str) -> str:
        # Support shell-escaped .env paths, e.g. "/Users/.../Mobile\\ Documents/com\\~apple\\~CloudDocs"
        normalized = str(path_text).replace("\\ ", " ").replace("\\~", "~")
        return str(Path(normalized).expanduser())

    @staticmethod
    def _normalize_score(score: Optional[str]) -> Optional[str]:
        if not score:
            return None
        txt = str(score).strip()
        m = re.match(r"^\s*(\d+)\s*[-:：]\s*(\d+)\s*$", txt)
        if not m:
            return None
        return f"{int(m.group(1))}-{int(m.group(2))}"

    @staticmethod
    def _parse_match_datetime(value: Any) -> Optional[datetime]:
        txt = str(value or "").strip()
        if not txt:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(txt, fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def _normalize_team_token(value: str) -> str:
        cleaned = str(value or "").replace("_", " ").replace("-", " ").strip().lower()
        return re.sub(r"[^a-z0-9]+", "", cleaned)

    @staticmethod
    def _split_match_english(english: str) -> Tuple[str, str]:
        txt = str(english or "").strip()
        if " vs " in txt:
            home, away = txt.split(" vs ", 1)
            return home.strip(), away.strip()
        if " VS " in txt:
            home, away = txt.split(" VS ", 1)
            return home.strip(), away.strip()
        return txt, ""

    def _dump_raw_artifact(self, suffix: str, content: str) -> str:
        path = self.cold_data_dir / f"{self.issue}_{self.match_id}_{suffix}"
        path.write_text(content, encoding="utf-8")
        return str(path)

    def _dump_raw_json_artifact(self, suffix: str, payload: Dict[str, Any]) -> str:
        path = self.cold_data_dir / f"{self.issue}_{self.match_id}_{suffix}"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return str(path)

    def fetch_raw_data(self) -> Dict[str, Any]:
        """
        阶段一：遥测抓取与冷存储 (Cold Data)
        """
        logger.info(f"开始抓取期号 {self.issue} 的赛事数据：比赛ID {self.match_id}")

        raw_data: Optional[Dict[str, Any]] = None
        errors: List[str] = []

        if self.source in ("auto", "understat"):
            if self._looks_like_understat_id(self.match_id):
                try:
                    raw_data = self._fetch_understat_raw_data()
                except Exception as e:
                    errors.append(f"Understat 失败: {e}")
                    logger.warning(f"Understat 抓取失败，准备回退: {e}")
            elif self.source == "understat":
                errors.append("Understat 模式要求 match_id 为纯数字 ID")

        if raw_data is None and self.source in ("auto", "fbref"):
            try:
                raw_data = self._fetch_fbref_raw_data()
            except Exception as e:
                errors.append(f"FBref 失败: {e}")
                logger.warning(f"FBref 抓取失败: {e}")

        if raw_data is None:
            raise RuntimeError(" | ".join(errors) if errors else "未获取到有效数据")

        self.validate_expected_match_identity(raw_data)
        
        # 落盘结构化冷数据
        raw_file_path = self.cold_data_dir / f"{self.issue}_{self.match_id}.json"
        try:
            with open(raw_file_path, "w", encoding='utf-8') as f:
                json.dump(raw_data, f, ensure_ascii=False, indent=2)
            logger.info(f"冷存储数据已落盘 -> {raw_file_path}")
        except Exception as e:
            logger.error(f"冷存储落盘失败: {e}")
            
        return raw_data

    def validate_expected_match_identity(self, raw_data: Dict[str, Any]) -> None:
        if not self.expected_match:
            return

        expected_english = str(self.expected_match.get("english", "")).strip()
        expected_home, expected_away = self._split_match_english(expected_english)
        actual_home = self._normalize_team_name(raw_data.get("home_team_raw", ""))
        actual_away = self._normalize_team_name(raw_data.get("away_team_raw", ""))

        if expected_home and expected_away:
            if (
                self._normalize_team_token(expected_home) != self._normalize_team_token(actual_home)
                or self._normalize_team_token(expected_away) != self._normalize_team_token(actual_away)
            ):
                raise ValueError(
                    "[ContaminationAlert] 比赛身份校验失败: "
                    f"expected={expected_home} vs {expected_away}, "
                    f"actual={actual_home} vs {actual_away}"
                )

        expected_dt = self._parse_match_datetime(
            self.expected_match.get("understat_date")
            or self.expected_match.get("fbref_date")
            or self.expected_match.get("football_data_date")
        )
        actual_dt = self._parse_match_datetime(raw_data.get("match_date"))
        if expected_dt and actual_dt and abs((actual_dt - expected_dt).total_seconds()) > 36 * 3600:
            raise ValueError(
                "[ContaminationAlert] 比赛日期校验失败: "
                f"expected={expected_dt.strftime('%Y-%m-%d %H:%M:%S')}, "
                f"actual={actual_dt.strftime('%Y-%m-%d %H:%M:%S')}"
            )

    def _fetch_understat_raw_data(self) -> Dict[str, Any]:
        url = f"https://understat.com/match/{self.match_id}"
        logger.info(f"正在从 Understat 抓取数据: {url}")
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
        }

        response = requests.get(url, headers=headers, timeout=12)
        response.raise_for_status()
        html = response.text
        html_raw_path = self._dump_raw_artifact("understat_raw.html", html)

        match = re.search(r"var match_info\s*=\s*JSON\.parse\('([^']+)'\)", html)
        if not match:
            raise ValueError("未能找到有效的 match_info 数据树")

        data_str = match.group(1)
        decoded = data_str.encode("utf-8").decode("unicode_escape")
        match_data = json.loads(decoded)
        json_raw_path = self._dump_raw_json_artifact("understat_match_info_raw.json", match_data)

        return {
            "source": "understat",
            "source_ref": url,
            "match_id": str(self.match_id),
            "match_date": match_data.get("date"),
            "home_team_raw": match_data.get("team_h", "Home"),
            "away_team_raw": match_data.get("team_a", "Away"),
            "goals_home": int(match_data.get("h_goals", 0)),
            "goals_away": int(match_data.get("a_goals", 0)),
            "expected_goals_home": float(match_data.get("h_xg", 0.0)),
            "expected_goals_away": float(match_data.get("a_xg", 0.0)),
            # Understat match_info 不自带控球率
            "possession_home": 50,
            "possession_away": 50,
            "shots_on_target_home": int(match_data.get("h_shotOnTarget", 0)),
            "shots_on_target_away": int(match_data.get("a_shotOnTarget", 0)),
            "events": [],
            "passes_attacking_third_home": int(match_data.get("h_deep", 0)),
            "passes_attacking_third_away": int(match_data.get("a_deep", 0)),
            "raw_artifacts": [html_raw_path, json_raw_path],
        }

    def _fetch_fbref_raw_data(self) -> Dict[str, Any]:
        if not self.fbref_url:
            raise ValueError("缺少 fbref_url（可通过 --fbref-url 传入，或将 --match-id 直接设为 FBref 比赛链接）")

        logger.info(f"正在从 FBref 抓取数据: {self.fbref_url}")
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
        }
        resp = requests.get(self.fbref_url, headers=headers, timeout=20)
        resp.raise_for_status()

        html = resp.text
        if "Just a moment" in html and "cf-chl" in html:
            raise RuntimeError("FBref 触发 Cloudflare 校验，请稍后重试或切换网络")
        html_raw_path = self._dump_raw_artifact("fbref_raw.html", html)

        soup = BeautifulSoup(html, "html.parser")
        scorebox = soup.select_one("div.scorebox")
        if not scorebox:
            raise ValueError("未找到 FBref scorebox，可能链接无效或页面结构变化")

        team_names: List[str] = []
        for a in scorebox.select("a[href*='/squads/']"):
            name = a.get_text(" ", strip=True)
            if name and name not in team_names:
                team_names.append(name)
            if len(team_names) >= 2:
                break
        if len(team_names) < 2:
            title = soup.title.get_text(" ", strip=True) if soup.title else ""
            m = re.search(r"^(.*?)\s+vs\.?\s+(.*?)\s+Match Report", title, flags=re.IGNORECASE)
            if m:
                team_names = [m.group(1).strip(), m.group(2).strip()]
        if len(team_names) < 2:
            raise ValueError("未能解析 FBref 主客队名")

        score_nodes = scorebox.select("div.score")
        if len(score_nodes) < 2:
            raise ValueError("未能解析 FBref 比分")
        goals_home = self._safe_int(score_nodes[0].get_text(" ", strip=True))
        goals_away = self._safe_int(score_nodes[1].get_text(" ", strip=True))
        if goals_home is None or goals_away is None:
            raise ValueError("FBref 比分字段异常")

        xg_vals: List[float] = []
        for n in scorebox.select("div.score_xg"):
            v = self._safe_float(n.get_text(" ", strip=True))
            if v is not None:
                xg_vals.append(v)
        if len(xg_vals) < 2:
            xg_home_txt, xg_away_txt = self._extract_pair_from_row(soup, ["expected goals", "xg"])
            xg_home = self._safe_float(xg_home_txt)
            xg_away = self._safe_float(xg_away_txt)
        else:
            xg_home, xg_away = xg_vals[0], xg_vals[1]
        if xg_home is None or xg_away is None:
            raise ValueError("未能从 FBref 解析 xG")

        pos_home_txt, pos_away_txt = self._extract_pair_from_row(soup, ["possession"])
        sot_home_txt, sot_away_txt = self._extract_pair_from_row(soup, ["shots on target", "sot"])

        pos_home = self._safe_int(pos_home_txt) or 50
        pos_away = self._safe_int(pos_away_txt) or 50
        sot_home = self._safe_int(sot_home_txt) or 0
        sot_away = self._safe_int(sot_away_txt) or 0

        return {
            "source": "fbref",
            "source_ref": self.fbref_url,
            "match_id": str(self.match_id),
            "match_date": None,
            "home_team_raw": team_names[0],
            "away_team_raw": team_names[1],
            "goals_home": goals_home,
            "goals_away": goals_away,
            "expected_goals_home": float(xg_home),
            "expected_goals_away": float(xg_away),
            "possession_home": pos_home,
            "possession_away": pos_away,
            "shots_on_target_home": sot_home,
            "shots_on_target_away": sot_away,
            "events": [],
            # FBref 回退路径暂无 deep passes，保留 0
            "passes_attacking_third_home": 0,
            "passes_attacking_third_away": 0,
            "raw_artifacts": [html_raw_path],
        }

    def _normalize_team_name(self, raw_name: str) -> str:
        return self.alias_map.get(raw_name, raw_name)

    @staticmethod
    def _sanitize_segment(value: str, field_name: str) -> str:
        cleaned = str(value).strip()
        cleaned = re.sub(r"\s+", " ", cleaned)
        cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1F]+', "_", cleaned).strip(" .")
        if not cleaned:
            raise ValueError(f"{field_name} 不能为空或仅包含非法字符")
        if cleaned in {".", ".."}:
            raise ValueError(f"{field_name} 非法：不能为 . 或 ..")
        return cleaned

    @staticmethod
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return float(default)

    @staticmethod
    def _split_frontmatter(content: str) -> Tuple[Dict[str, Any], str]:
        if content.startswith("---\n"):
            closing_marker_index = content.find("\n---\n", 4)
            if closing_marker_index != -1:
                yaml_text = content[4:closing_marker_index]
                body = content[closing_marker_index + len("\n---\n"):]
                parsed = yaml.safe_load(yaml_text) or {}
                if isinstance(parsed, dict):
                    return parsed, body
                raise ValueError("frontmatter 结构非法：必须是 YAML 对象")
        return {}, content

    @staticmethod
    def _build_markdown(frontmatter: Dict[str, Any], body: str) -> str:
        yaml_text = yaml.safe_dump(frontmatter, allow_unicode=True, sort_keys=False)
        return f"---\n{yaml_text}---\n{body}"

    @staticmethod
    def _write_text_safely(target_path: Path, content: str) -> None:
        temp_path = target_path.with_suffix(target_path.suffix + ".tmp")
        temp_path.write_text(content, encoding="utf-8")
        temp_path.replace(target_path)

    @staticmethod
    def _clamp(value: float, min_value: float, max_value: float) -> float:
        return max(min_value, min(max_value, value))

    @staticmethod
    def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
        if not text:
            return None
        txt = str(text).strip()
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
        if start == -1 or end == -1 or end <= start:
            return None
        snippet = txt[start : end + 1]
        try:
            parsed = json.loads(snippet)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return None
        return None

    def _call_reality_gap_llm(
        self,
        *,
        team_name: str,
        intel_base: Dict[str, Any],
        physical_reality: Dict[str, Any],
        match_payload: Dict[str, Any],
        rule_gap: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if not self._llm_available():
            return None

        system_prompt = (
            "You are an elite football risk model judge. "
            "Output STRICT JSON only. "
            "Never include markdown. "
            "Allowed bias_type: Fame_Trap, Underestimated, Aligned."
        )
        user_payload = {
            "task": "Evaluate reality gap and whether tactical entropy should be backfilled.",
            "team": team_name,
            "issue": self.issue,
            "match_id": self.match_id,
            "intel_base": intel_base,
            "physical_reality": physical_reality,
            "latest_match_payload": match_payload,
            "rule_baseline": rule_gap,
            "output_schema": {
                "bias_type": "Fame_Trap|Underestimated|Aligned",
                "S_dynamic_modifier": "float",
                "should_backfill_entropy": "bool",
                "entropy_delta": "float",
                "confidence": "0~1 float",
                "reasoning_brief": "short string",
            },
        }

        if self.llm_provider == "gemini":
            return self._call_reality_gap_llm_gemini(system_prompt=system_prompt, user_payload=user_payload)
        return self._call_reality_gap_llm_openai(system_prompt=system_prompt, user_payload=user_payload)

    def _call_reality_gap_llm_openai(self, *, system_prompt: str, user_payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        endpoint = f"{self.llm_base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.llm_api_key}",
            "Content-Type": "application/json",
        }
        request_payload = {
            "model": self.llm_model,
            "temperature": 0.1,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
            ],
            "response_format": {"type": "json_object"},
        }
        try:
            resp = requests.post(
                endpoint,
                headers=headers,
                json=request_payload,
                timeout=self.llm_timeout_sec,
            )
            resp.raise_for_status()
            data = resp.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            parsed = self._extract_json_object(content)
            if parsed is None:
                logger.warning("LLM(OpenAI) 输出无法解析为 JSON，将回退规则判定。")
            return parsed
        except Exception as e:
            logger.warning("LLM(OpenAI) Reality-Gap 调用失败，将回退规则判定: %s", e)
            return None

    def _call_reality_gap_llm_gemini(self, *, system_prompt: str, user_payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        endpoint = f"{self.llm_base_url}/models/{self.llm_model}:generateContent"
        headers = {"Content-Type": "application/json"}
        request_payload = {
            "systemInstruction": {"parts": [{"text": system_prompt}]},
            "contents": [
                {"role": "user", "parts": [{"text": json.dumps(user_payload, ensure_ascii=False)}]},
            ],
            "generationConfig": {
                "temperature": 0.1,
                "responseMimeType": "application/json",
            },
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
            content = (
                data.get("candidates", [{}])[0]
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text", "")
            )
            parsed = self._extract_json_object(content)
            if parsed is None:
                logger.warning("LLM(Gemini) 输出无法解析为 JSON，将回退规则判定。")
            return parsed
        except Exception as e:
            logger.warning("LLM(Gemini) Reality-Gap 调用失败，将回退规则判定: %s", e)
            return None

    def _apply_reality_gap_with_optional_llm(
        self,
        *,
        team_name: str,
        intel_base: Dict[str, Any],
        physical_reality: Dict[str, Any],
        match_payload: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        rule_gap = self.calculate_reality_gap(intel_base, physical_reality)
        audit: Dict[str, Any] = {
            "team": team_name,
            "issue": self.issue,
            "match_id": self.match_id,
            "mode": "rule_only",
            "rule_gap": rule_gap,
            "llm_enabled": self.llm_enabled,
            "llm_provider": self.llm_provider,
            "llm_model": self.llm_model,
        }
        reality_gap = dict(rule_gap)

        if not self._llm_available():
            return physical_reality, reality_gap, audit

        llm_result = self._call_reality_gap_llm(
            team_name=team_name,
            intel_base=intel_base,
            physical_reality=physical_reality,
            match_payload=match_payload,
            rule_gap=rule_gap,
        )
        if not llm_result:
            return physical_reality, reality_gap, audit

        audit["mode"] = "llm_assisted"
        audit["llm_raw"] = llm_result

        bias_type = str(llm_result.get("bias_type", "")).strip()
        if bias_type not in ALLOWED_BIAS_TYPES:
            bias_type = rule_gap["bias_type"]

        s_dynamic_modifier = self._to_float(
            llm_result.get("S_dynamic_modifier"),
            rule_gap["S_dynamic_modifier"],
        )
        s_dynamic_modifier = round(self._clamp(s_dynamic_modifier, -0.3, 0.3), 4)

        confidence = self._clamp(self._to_float(llm_result.get("confidence"), 0.0), 0.0, 1.0)
        should_backfill_entropy = bool(llm_result.get("should_backfill_entropy", False))
        entropy_delta = self._to_float(llm_result.get("entropy_delta"), 0.0)
        entropy_delta = self._clamp(
            entropy_delta,
            -abs(self.llm_max_entropy_delta),
            abs(self.llm_max_entropy_delta),
        )

        reality_gap = {
            "bias_type": bias_type,
            "S_dynamic_modifier": s_dynamic_modifier,
        }
        audit["final_gap"] = reality_gap

        entropy_before = self._to_float(physical_reality.get("actual_tactical_entropy"), 0.40)
        entropy_after = entropy_before
        entropy_applied = False
        if should_backfill_entropy and confidence >= self.llm_min_confidence and abs(entropy_delta) > 0:
            entropy_after = round(self._clamp(entropy_before + entropy_delta, 0.10, 1.20), 4)
            physical_reality["actual_tactical_entropy"] = entropy_after
            entropy_applied = True

        audit["entropy_backfill"] = {
            "should_backfill_entropy": should_backfill_entropy,
            "confidence": round(confidence, 4),
            "min_confidence_required": self.llm_min_confidence,
            "entropy_delta_requested": round(entropy_delta, 4),
            "entropy_before": round(entropy_before, 4),
            "entropy_after": round(entropy_after, 4),
            "applied": entropy_applied,
        }
        audit["reasoning_brief"] = str(llm_result.get("reasoning_brief", "")).strip()

        return physical_reality, reality_gap, audit

    def _dump_reality_gap_audit(self, team_name: str, audit_payload: Dict[str, Any]) -> None:
        safe_team = re.sub(r"[^A-Za-z0-9._-]+", "_", team_name).strip("_") or "team"
        path = self.cold_data_dir / f"{self.issue}_{self.match_id}_{safe_team}_reality_gap_audit.json"
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(audit_payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Reality-Gap 审计落盘失败: {e}")

    def _resolve_team_archive_md_path(self, team_name: str) -> Path:
        safe_team = self._sanitize_segment(team_name, "team")
        candidate_names = candidate_team_filenames(safe_team)

        def _pick_preferred(paths: List[Path]) -> Optional[Path]:
            existing_by_name: Dict[str, Path] = {}
            for path in paths:
                if path.exists():
                    existing_by_name.setdefault(path.name, path)
            for candidate in candidate_names:
                match = existing_by_name.get(f"{candidate}.md")
                if match is not None:
                    return match
            unique_existing = list(existing_by_name.values())
            if len(unique_existing) == 1:
                return unique_existing[0]
            if len(unique_existing) > 1:
                raise ValueError(
                    f"检测到多个球队档案候选: {[p.name for p in unique_existing]}，请统一命名。"
                )
            return None

        if self.league:
            safe_league = self._sanitize_segment(self.league, "league")
            primary_dir = league_archive_dir(self.team_archives_dir, safe_league)
            preferred = _pick_preferred(
                [primary_dir / f"{candidate}.md" for candidate in candidate_names]
            )
            if preferred is not None:
                return preferred

        candidates: List[Path] = []
        for candidate in candidate_names:
            candidates.extend(self.team_archives_dir.glob(f"**/{candidate}.md"))
        preferred = _pick_preferred(candidates)
        if preferred is not None:
            return preferred
        raise FileNotFoundError(
            f"未找到球队档案: {candidate_names}。请先执行 team_forge.py 初始化，"
            f"或在命令中传入 --league 以定位 {self.team_archives_dir}/{{league}}/<team>.md"
        )

    def _update_physical_reality(
        self,
        physical_reality: Dict[str, Any],
        *,
        xg_for: float,
        goals_for: int,
        variance_flag: bool,
    ) -> Dict[str, Any]:
        merged = {**PHYSICAL_REALITY_DEFAULTS, **(physical_reality or {})}

        existing_history = merged.get("xg_history_last_5", [])
        xg_history: List[float] = []
        if isinstance(existing_history, list):
            for v in existing_history:
                try:
                    xg_history.append(float(v))
                except Exception:
                    continue

        if not xg_history:
            xg_history = [self._to_float(merged.get("avg_xG_last_5"), 1.0)]
        xg_history.append(float(xg_for))
        xg_history = xg_history[-5:]

        merged["xg_history_last_5"] = [round(v, 4) for v in xg_history]
        merged["avg_xG_last_5"] = round(sum(xg_history) / len(xg_history), 4)

        xg_for_safe = max(float(xg_for), 1e-6)
        merged["conversion_efficiency"] = round(max(int(goals_for), 0) / xg_for_safe, 4)

        variance_history = merged.get("variance_history", [])
        if not isinstance(variance_history, list):
            variance_history = []
        if variance_flag:
            variance_history.append(True)
        merged["variance_history"] = variance_history[-5:]
        return merged

    def calculate_reality_gap(
        self,
        intel_base: Dict[str, Any],
        physical_reality: Dict[str, Any],
    ) -> Dict[str, Any]:
        market_sentiment = str(intel_base.get("market_sentiment", "")).strip().lower()
        avg_xg_last_5 = self._to_float(physical_reality.get("avg_xG_last_5"), 1.0)
        conversion_efficiency = self._to_float(physical_reality.get("conversion_efficiency"), 0.05)
        actual_tactical_entropy = self._to_float(physical_reality.get("actual_tactical_entropy"), 0.40)

        if market_sentiment in {"optimistic", "overheated"} and avg_xg_last_5 < 1.0 and conversion_efficiency < 0.05:
            return {"bias_type": "Fame_Trap", "S_dynamic_modifier": 0.15}
        if market_sentiment == "pessimistic" and avg_xg_last_5 > 1.8 and actual_tactical_entropy < 0.40:
            return {"bias_type": "Underestimated", "S_dynamic_modifier": -0.10}
        return dict(REALITY_GAP_DEFAULTS)

    def _update_team_archive_markdown(self, team_name: str, payload: Dict[str, Any]) -> Path:
        archive_path = self._resolve_team_archive_md_path(team_name)
        content = archive_path.read_text(encoding="utf-8")
        frontmatter, body = self._split_frontmatter(content)

        intel_base = frontmatter.get("intel_base")
        if not isinstance(intel_base, dict):
            intel_base = {}
        intel_base = {**INTEL_BASE_DEFAULTS, **intel_base}

        physical_reality = frontmatter.get("physical_reality")
        if not isinstance(physical_reality, dict):
            physical_reality = {}

        physical_reality = self._update_physical_reality(
            physical_reality,
            xg_for=float(payload["xg_for"]),
            goals_for=int(payload["score_for"]),
            variance_flag=bool(payload["variance_flag"]),
        )
        physical_reality, reality_gap, gap_audit = self._apply_reality_gap_with_optional_llm(
            team_name=team_name,
            intel_base=intel_base,
            physical_reality=physical_reality,
            match_payload=payload,
        )

        frontmatter["intel_base"] = intel_base
        frontmatter["physical_reality"] = physical_reality
        frontmatter["reality_gap"] = reality_gap

        updated_markdown = self._build_markdown(frontmatter, body)
        self._write_text_safely(archive_path, updated_markdown)
        self._dump_reality_gap_audit(team_name, gap_audit)
        logger.info(
            "球队档案已更新 reality_gap -> %s [%s / %.2f]",
            archive_path,
            reality_gap["bias_type"],
            reality_gap["S_dynamic_modifier"],
        )
        return archive_path

    def extract_hot_features(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        阶段二：热精炼 (Hot Data Extraction)
        从原始JSON中提取符合PRD规范的核心物理因子
        """
        logger.info("开始提取热精炼特征...")
        home_team = self._normalize_team_name(raw_data.get("home_team_raw", "Home"))
        away_team = self._normalize_team_name(raw_data.get("away_team_raw", "Away"))
        
        goals_home = raw_data.get("goals_home", 0)
        goals_away = raw_data.get("goals_away", 0)
        
        if goals_home > goals_away:
            winner = "home"
        elif goals_home < goals_away:
            winner = "away"
        else:
            winner = "draw"

        red_cards = [event["player"] for event in raw_data.get("events", []) if event.get("type") == "red_card"]
        penalties = [event["player"] for event in raw_data.get("events", []) if event.get("type") == "penalty"]

        hot_data = {
            "version": 2.1,
            "issue": str(self.issue),
            "match_id": str(self.match_id),
            "match_name": f"{home_team} vs {away_team}",
            "data_source": raw_data.get("source", "unknown"),
            "data_source_ref": raw_data.get("source_ref", ""),
            "result": {
                "score": f"{goals_home}-{goals_away}",
                "winner": winner,
                "validation_passed": None
            },
            "physical_metrics": {
                "home_xG": float(raw_data.get("expected_goals_home", 0.0)),
                "away_xG": float(raw_data.get("expected_goals_away", 0.0)),
                "possession_home": int(raw_data.get("possession_home", 50)),
                "possession_away": int(raw_data.get("possession_away", 50)),
                "shots_on_target_home": int(raw_data.get("shots_on_target_home", 0)),
                "shots_on_target_away": int(raw_data.get("shots_on_target_away", 0)),
                "passes_attacking_third_home": int(raw_data.get("passes_attacking_third_home", 0)),
                "passes_attacking_third_away": int(raw_data.get("passes_attacking_third_away", 0))
            },
            "key_events": {
                "red_cards": red_cards,
                "penalties": penalties
            }
        }
        return hot_data

    def update_team_archives(self, hot_data: Dict[str, Any]) -> None:
        team_a, team_b = hot_data["match_name"].split(" vs ", 1)
        score = hot_data["result"]["score"]
        h_score, a_score = [int(x) for x in score.split("-")]
        metrics = hot_data["physical_metrics"]

        team_payloads = [
            (
                team_a,
                {
                    "issue": hot_data["issue"],
                    "match_id": hot_data["match_id"],
                    "team": team_a,
                    "opponent": team_b,
                    "is_home": True,
                    "score_for": h_score,
                    "score_against": a_score,
                    "xg_for": metrics["home_xG"],
                    "xg_against": metrics["away_xG"],
                    "shots_on_target_for": metrics["shots_on_target_home"],
                    "shots_on_target_against": metrics["shots_on_target_away"],
                    "passes_attacking_third_for": metrics["passes_attacking_third_home"],
                    "passes_attacking_third_against": metrics["passes_attacking_third_away"],
                    "data_source": hot_data.get("data_source", ""),
                    "data_source_ref": hot_data.get("data_source_ref", ""),
                    "variance_flag": hot_data["system_evaluation"]["variance_flag"],
                },
            ),
            (
                team_b,
                {
                    "issue": hot_data["issue"],
                    "match_id": hot_data["match_id"],
                    "team": team_b,
                    "opponent": team_a,
                    "is_home": False,
                    "score_for": a_score,
                    "score_against": h_score,
                    "xg_for": metrics["away_xG"],
                    "xg_against": metrics["home_xG"],
                    "shots_on_target_for": metrics["shots_on_target_away"],
                    "shots_on_target_against": metrics["shots_on_target_home"],
                    "passes_attacking_third_for": metrics["passes_attacking_third_away"],
                    "passes_attacking_third_against": metrics["passes_attacking_third_home"],
                    "data_source": hot_data.get("data_source", ""),
                    "data_source_ref": hot_data.get("data_source_ref", ""),
                    "variance_flag": hot_data["system_evaluation"]["variance_flag"],
                },
            ),
        ]

        for team_name, payload in team_payloads:
            archive_path = self._update_team_archive_markdown(team_name, payload)
            runtime_league = re.sub(r"[^A-Za-z0-9._-]+", "_", archive_path.parent.name).strip("_") or "league"
            runtime_team = re.sub(r"[^A-Za-z0-9._-]+", "_", archive_path.stem).strip("_") or "team"
            team_dir = self.team_archives_runtime_dir / runtime_league / runtime_team
            team_dir.mkdir(parents=True, exist_ok=True)

            latest_path = team_dir / "latest_postmatch.json"
            history_path = team_dir / "postmatch_history.jsonl"

            with open(latest_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            with open(history_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def validate_official_score(self, hot_data: Dict[str, Any]) -> bool:
        actual_score = hot_data["result"]["score"]
        if not self.official_score:
            logger.info("未提供 official_score，跳过官方比分校验。")
            return True

        if actual_score != self.official_score:
            raise ValueError(
                f"[ContaminationAlert] 比分校验失败: 抓取={actual_score}, 官方={self.official_score}。已中止热数据落盘。"
            )
        logger.info(f"官方比分校验通过: {actual_score}")
        return True

    @staticmethod
    def quarantine_stale_issue_report(
        *,
        vault_path: Optional[str],
        issue: str,
        match_id: str,
        reason: str,
    ) -> Optional[Path]:
        if not vault_path:
            return None

        vault_root = Path(vault_path)
        postmatch_name = f"{issue}_{match_id}_postmatch.md"
        issue_src = vault_root / "03_Match_Audits" / issue / "04_Postmatch_Telemetry" / postmatch_name
        legacy_src = vault_root / "03_Match_Audits" / "Postmatch_Telemetry" / postmatch_name
        src = issue_src if issue_src.exists() else legacy_src
        if not src.exists():
            return None

        legacy_dir = vault_root / "03_Match_Audits" / issue / "04_Postmatch_Legacy"
        legacy_dir.mkdir(parents=True, exist_ok=True)
        dst = legacy_dir / f"STALE-{postmatch_name}"

        try:
            shutil.move(str(src), str(dst))
            logger.warning(
                "已隔离疑似串期旧报告: %s -> %s (reason=%s)",
                src,
                dst,
                reason,
            )
            return dst
        except Exception as e:
            logger.warning("隔离疑似串期旧报告失败 %s: %s", src, e)
            return None

    def calculate_variance(self, hot_data: Dict[str, Any]) -> bool:
        """
        阶段三：逻辑运算 (Enhanced Variance Flag)
        若高 xG 方未获胜（输球/平局）且 xG 差值 > 1.0，则返回 True
        """
        winner = hot_data["result"]["winner"]
        home_xg = hot_data["physical_metrics"]["home_xG"]
        away_xg = hot_data["physical_metrics"]["away_xG"]

        xg_gap = abs(home_xg - away_xg)
        if xg_gap <= 1.0:
            variance_flag = False
        elif winner == "draw":
            variance_flag = True
        elif home_xg > away_xg:
            variance_flag = winner != "home"
        else:
            variance_flag = winner != "away"

        logger.info(f"方差运算完成: Variance Flag = {variance_flag}")
        return variance_flag

    def generate_markdown(self, hot_data: Dict[str, Any]) -> str:
        """
        阶段四：落盘生成 Obsidian Markdown 笔记 (带深度战术解读)
        """
        out_file = self.hot_reports_dir / f"{self.issue}_{self.match_id}_postmatch.md"

        yaml_content = yaml.dump(hot_data, sort_keys=False, allow_unicode=True)
        markdown_content = f"---\n{yaml_content}---\n\n"
        markdown_content += f"# {hot_data['match_name']} ({self.issue})\n\n"
        markdown_content += "> 📊 本复盘报告由 Ares OSINT Telemetry (Understat + FBref 双源回退) 自动生成。\n\n"
        
        # 提取关键数据以生成解读
        metrics = hot_data["physical_metrics"]
        score = hot_data["result"]["score"]
        winner = hot_data["result"]["winner"]
        h_xg = metrics["home_xG"]
        a_xg = metrics["away_xG"]
        h_deep = metrics.get("passes_attacking_third_home", 0)
        a_deep = metrics.get("passes_attacking_third_away", 0)
        
        markdown_content += "## 📈 物理遥测深度解读\n\n"
        
        # 核心方差判断
        if hot_data["system_evaluation"]["variance_flag"]:
            markdown_content += "### ⚡ 危险方差倒挂 (严重警报)\n"
            markdown_content += f"比赛比分最终为 `{score}`，但根据底层物理遥测，双方的预期进球真实转化存在巨大撕裂：主队 xG **{h_xg:.2f}** 对比 客队 xG **{a_xg:.2f}**。\n"
            markdown_content += "👉 **Ares 引擎建议**：此场赛果具有强烈的运气、神仙球或门将爆种因素。在下一周期的量化推演中，**必须无视本场比分结果**，直接采信 xG 物理预期，以防止大模型判断失真！\n\n"
        else:
            markdown_content += "### ✅ 赛果吻合度正常\n"
            markdown_content += f"本场比分 `{score}` 基本客观地反映了场上的物理真实。主客队的绝对进球机会占比 (主 {h_xg:.2f} vs 客 {a_xg:.2f}) 未见明显扭曲。\n\n"
            
        # 战术对抗解析
        markdown_content += "### ⚔️ 战术压制力剥析\n"
        xg_diff = abs(h_xg - a_xg)
        if xg_diff < 0.5:
            markdown_content += "- **机会创造端**：这是一场**绝对意义上的均势局**（或互啄局）。两边打出的高质量威胁机会甚至拉不开半球的差距。\n"
        elif h_xg > a_xg + 0.5:
            markdown_content += "- **机会创造端**：**主队完全接管了威胁区域**。创造出的绝对进球机会明显多于对手。\n"
        elif a_xg > h_xg + 0.5:
            markdown_content += "- **机会创造端**：**客队反客为主**。在射门质量与绝对得分机会上形成了对主队的单方面压制。\n"
            
        if h_deep > a_deep * 1.5:
            markdown_content += f"- **阵地纵深打击**：主队在进攻三区成功送出了高达 **{h_deep}** 次的高危传球（对比客队的 {a_deep} 次）。客队防线全场处于深度退守并被反复摩擦的状态。\n"
        elif a_deep > h_deep * 1.5:
            markdown_content += f"- **阵地纵深打击**：客队在进攻三区成功送出 **{a_deep}** 次高危传球（对比主队 {h_deep} 次），主队在自己的半场承受了极大的阵地战火力渗透。\n"
        else:
            markdown_content += f"- **阵地纵深打击**：双方在禁区前沿的相互渗透次数相对平衡（主 {h_deep} 次 / 客 {a_deep} 次）。\n"

        try:
            with open(out_file, "w", encoding='utf-8') as f:
                f.write(markdown_content)
            logger.info(f"Markdown 战报落盘 -> {out_file}")
        except Exception as e:
            logger.error(f"Markdown战报落盘失败: {e}")
            
        return str(out_file)

    def run(self) -> str:
        raw_data = self.fetch_raw_data()
        hot_data = self.extract_hot_features(raw_data)
        hot_data["result"]["validation_passed"] = self.validate_official_score(hot_data)
        variance_flag = self.calculate_variance(hot_data)
        
        hot_data["system_evaluation"] = {
            "variance_flag": variance_flag
        }

        self.update_team_archives(hot_data)
        
        out_file = self.generate_markdown(hot_data)
        logger.info(f"[{self.match_id}] Pipeline 节点执行完毕。")
        return out_file

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ares OSINT Telemetry - PostMatch Extraction")
    parser.add_argument("--issue", type=str, required=True, help="期号，如 26062")
    parser.add_argument("--match-id", type=str, required=False, help="目标比赛的唯一ID (如果不填，则全自动执行14场复盘)")
    parser.add_argument("--league", type=str, required=False, help="联赛名，用于精准定位 Team_Archives/{league}/{team}.md")
    parser.add_argument(
        "--source",
        type=str,
        default="auto",
        choices=["auto", "understat", "fbref"],
        help="数据源策略: auto(先 Understat 后 FBref) | understat | fbref",
    )
    parser.add_argument(
        "--fbref-url",
        type=str,
        required=False,
        help="FBref 比赛链接（source=fbref 时建议提供）",
    )
    parser.add_argument(
        "--official-score",
        type=str,
        required=False,
        help="官方比分，格式如 2-1。若提供则会执行污染校验，不一致时中止落盘。",
    )
    args = parser.parse_args()
    base_dir = Path(__file__).resolve().parent.parent.parent
    load_dotenv_into_env(base_dir)
    audit_router = AuditRouter(base_dir=base_dir)
    
    if args.match_id:
        pipeline = MatchTelemetryPipeline(
            issue=args.issue,
            match_id=args.match_id,
            source=args.source,
            fbref_url=args.fbref_url,
            official_score=args.official_score,
            league=args.league,
        )
        pipeline.run()
        if audit_router.enabled:
            try:
                audit_router.ensure_issue_governance(
                    issue=args.issue,
                    manifest=None,
                    create_prematch_stubs=False,
                )
            except Exception as e:
                logger.warning(f"AuditRouter 自动整理失败（不影响主流程）: {e}")
    else:
        # Batch Mode
        logger.info(f"未提供特定 match_id，启动全自动批量复盘引擎 (Issue: {args.issue})...")
        vault_path = os.getenv("ARES_VAULT_PATH")
        if vault_path:
            vault_path = MatchTelemetryPipeline._normalize_vault_path(vault_path)
        manifest_path = None
        if vault_path:
            primary_manifest = Path(vault_path) / "04_RAG_Raw_Data" / "Cold_Data_Lake" / f"{args.issue}_dispatch_manifest.json"
            if primary_manifest.exists():
                manifest_path = primary_manifest

        if manifest_path is None:
            legacy_manifest = base_dir / "raw_reports" / f"{args.issue}_dispatch_manifest.json"
            manifest_path = legacy_manifest
        
        if not manifest_path.exists():
            logger.error(f"找不到战术派发单 {manifest_path}，请先执行赛前爬虫 (osint_crawler.py)！")
        else:
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)

            match_dates: List[datetime] = []
            for match in manifest.get("matches", []):
                candidate_dt = MatchTelemetryPipeline._parse_match_datetime(
                    match.get("understat_date")
                    or match.get("fbref_date")
                    or match.get("football_data_date")
                )
                if candidate_dt is not None:
                    match_dates.append(candidate_dt)

            issue_window_start: Optional[datetime] = None
            issue_window_end: Optional[datetime] = None
            if match_dates:
                match_dates.sort()
                anchor = match_dates[len(match_dates) // 2]
                window_days = int(os.getenv("ARES_POSTMATCH_ISSUE_WINDOW_DAYS", "3"))
                in_window = [
                    dt for dt in match_dates if abs((dt - anchor).total_seconds()) <= window_days * 86400
                ]
                if in_window:
                    issue_window_start = min(in_window)
                    issue_window_end = max(in_window)
                    logger.info(
                        "检测到本期赛程时间窗口: %s -> %s",
                        issue_window_start.strftime("%Y-%m-%d %H:%M:%S"),
                        issue_window_end.strftime("%Y-%m-%d %H:%M:%S"),
                    )
            
            success = 0
            skipped = 0
            generated_reports = []
            for match in manifest.get("matches", []):
                fbref_url = match.get("fbref_url")
                uid = match.get("understat_id") or fbref_url
                official_score = match.get("official_score") or match.get("result_score")
                expected_dt = MatchTelemetryPipeline._parse_match_datetime(
                    match.get("understat_date")
                    or match.get("fbref_date")
                    or match.get("football_data_date")
                )
                if expected_dt and issue_window_start and issue_window_end:
                    if expected_dt < issue_window_start or expected_dt > issue_window_end:
                        skipped += 1
                        logger.warning(
                            "[ContaminationAlert] 跳过 manifest 映射疑似串期比赛: %s | expected_date=%s | issue_window=%s -> %s",
                            match.get("english"),
                            expected_dt.strftime("%Y-%m-%d %H:%M:%S"),
                            issue_window_start.strftime("%Y-%m-%d %H:%M:%S"),
                            issue_window_end.strftime("%Y-%m-%d %H:%M:%S"),
                        )
                        if uid:
                            MatchTelemetryPipeline.quarantine_stale_issue_report(
                                vault_path=vault_path,
                                issue=args.issue,
                                match_id=str(uid),
                                reason=(
                                    f"expected_date={expected_dt.strftime('%Y-%m-%d %H:%M:%S')} "
                                    f"outside_issue_window={issue_window_start.strftime('%Y-%m-%d %H:%M:%S')}~"
                                    f"{issue_window_end.strftime('%Y-%m-%d %H:%M:%S')}"
                                ),
                            )
                        continue
                if uid:
                    logger.info(f"==> 正在批量复盘: {match['chinese']} (Ref: {uid})")
                    pipeline = MatchTelemetryPipeline(
                        issue=args.issue,
                        match_id=str(uid),
                        source=args.source,
                        fbref_url=fbref_url,
                        official_score=official_score,
                        league=(
                            match.get("league")
                            or match.get("competition")
                            or manifest.get("league")
                            or args.league
                        ),
                        expected_match=match,
                    )
                    try:
                        out_file = pipeline.run()
                        generated_reports.append(out_file)
                        success += 1
                    except Exception as e:
                        logger.error(f"批量复盘 {match['chinese']} 时发生严重错误: {e}")
                else:
                    skipped += 1
                    
            logger.info(f"批量复盘完毕！共成功生成 {success} 份深度战报，跳过了 {skipped} 场无需复盘的超纲赛事。")
            if generated_reports:
                logger.info("本次输出文件清单：")
                for report in generated_reports:
                    logger.info(f"  - {report}")

            if audit_router.enabled:
                try:
                    audit_router.ensure_issue_governance(
                        issue=args.issue,
                        manifest=manifest,
                        create_prematch_stubs=False,
                    )
                except Exception as e:
                    logger.warning(f"AuditRouter 自动整理失败（不影响主流程）: {e}")
