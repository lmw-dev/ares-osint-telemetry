import os
import json
import logging
import argparse
import time
import re
import unicodedata
import requests
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from bs4 import BeautifulSoup, Comment
from audit_router import AuditRouter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AresTelemetry.Crawler")

FBREF_COMPETITIONS: Dict[str, Tuple[int, str]] = {
    "EPL": (9, "EPL"),
    "La_liga": (12, "La_liga"),
    "Bundesliga": (20, "Bundesliga"),
    "Serie_A": (11, "Serie_A"),
    "Ligue_1": (13, "Ligue_1"),
    # Secondary leagues
    "Championship": (10, "Championship"),
    "Bundesliga_2": (33, "Bundesliga_2"),
    "Ligue_2": (60, "Ligue_2"),
    "Serie_B": (18, "Serie_B"),
}

FOOTBALL_DATA_COMPETITIONS: Dict[str, str] = {
    "PL": "EPL",
    "PD": "La_liga",
    "BL1": "Bundesliga",
    "SA": "Serie_A",
    "FL1": "Ligue_1",
    "ELC": "Championship",
    "BL2": "Bundesliga_2",
    "FL2": "Ligue_2",
    "SB": "Serie_B",
}

LEAGUE_TO_ODDS_SPORT_KEY: Dict[str, str] = {
    "EPL": "soccer_epl",
    "La_liga": "soccer_spain_la_liga",
    "Bundesliga": "soccer_germany_bundesliga",
    "Serie_A": "soccer_italy_serie_a",
    "Ligue_1": "soccer_france_ligue_one",
    "Championship": "soccer_efl_champ",
    "Bundesliga_2": "soccer_germany_bundesliga2",
    "Ligue_2": "soccer_france_ligue_two",
    "Serie_B": "soccer_italy_serie_b",
}

TITAN_PREMATCH_PAGE_TEMPLATES: Dict[str, str] = {
    "analysis": "https://zq.titan007.com/analysis/{match_id}cn.htm",
    "asian_odds": "https://vip.titan007.com/AsianOdds_n.aspx?id={match_id}&l=0",
    "over_down": "https://vip.titan007.com/OverDown_n.aspx?id={match_id}&l=0",
    "euro_odds": "https://1x2.titan007.com/oddslist/{match_id}.htm",
}


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
        # 入口预加载失败不阻断，类内还会再兜底
        pass


class AresOsintCrawler:
    _dotenv_loaded = False

    def __init__(self, issue: str):
        self.issue = issue
        self.base_dir = Path(__file__).resolve().parent.parent.parent
        self._load_project_env_file()
        self.vault_path = os.getenv("ARES_VAULT_PATH")
        if self.vault_path:
            self.vault_path = self.vault_path.replace("\\ ", " ").replace("\\~", "~")
            self.vault_path = str(Path(self.vault_path).expanduser())

        if self.vault_path:
            vault_root = Path(self.vault_path)
            self.raw_reports_dir = vault_root / "04_RAG_Raw_Data" / "Cold_Data_Lake"
        else:
            self.raw_reports_dir = self.base_dir / "raw_reports"
        self.raw_reports_dir.mkdir(parents=True, exist_ok=True)

        self.audit_router = AuditRouter(base_dir=self.base_dir, vault_path=self.vault_path)
        self.last_500_cold_refs = []
        self._football_data_cold_refs: List[str] = []
        self._odds_cold_refs: List[str] = []
        self._odds_events_cache: Dict[str, List[Dict[str, Any]]] = {}
        self._titan_cold_refs: List[str] = []
        self._titan_prematch_cache: Dict[str, Dict[str, Any]] = {}
        self._manual_anchor_source_path: Optional[str] = None
        self._manual_anchor_overrides = self._load_manual_anchor_overrides()
        self._load_provider_runtime_config()
        
        # Load aliases
        alias_path = self.base_dir / "src" / "data" / "team_alias_map.json"
        try:
            with open(alias_path, 'r', encoding='utf-8') as f:
                self.team_alias = json.load(f)
        except Exception as e:
            logger.warning(f"无法加载字典 {e}")
            self.team_alias = {}

    def _load_project_env_file(self) -> None:
        if AresOsintCrawler._dotenv_loaded:
            return
        env_path = self.base_dir / ".env"
        if not env_path.exists():
            AresOsintCrawler._dotenv_loaded = True
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
            AresOsintCrawler._dotenv_loaded = True

    def _load_provider_runtime_config(self) -> None:
        self.football_data_api_key = (
            str(os.getenv("ARES_FOOTBALL_DATA_API_KEY", "")).strip()
            or str(os.getenv("FOOTBALL_DATA_API_KEY", "")).strip()
        )
        football_data_base_url_raw = str(os.getenv("ARES_FOOTBALL_DATA_BASE_URL", "")).strip()
        self.football_data_base_url = (
            football_data_base_url_raw or "https://api.football-data.org/v4"
        ).rstrip("/")

        self.the_odds_api_key = (
            str(os.getenv("ARES_THE_ODDS_API_KEY", "")).strip()
            or str(os.getenv("THE_ODDS_API_KEY", "")).strip()
        )
        the_odds_base_url_raw = str(os.getenv("ARES_THE_ODDS_BASE_URL", "")).strip()
        self.the_odds_base_url = (the_odds_base_url_raw or "https://api.the-odds-api.com/v4").rstrip("/")
        self.enable_external_odds_enrich = str(
            os.getenv("ARES_ENABLE_EXTERNAL_ODDS_ENRICH", "0")
        ).strip().lower() in {"1", "true", "yes", "on"}
        self.enable_titan_prematch_enrich = str(
            os.getenv("ARES_ENABLE_TITAN_PREMATCH_ENRICH", "1")
        ).strip().lower() in {"1", "true", "yes", "on"}
        self.mapping_max_gap_days = int(os.getenv("ARES_MATCH_MAPPING_MAX_GAP_DAYS", "10"))

        if not self.football_data_api_key:
            logger.info("未配置 football-data API Key，映射回退将跳过 football-data 源。")
        if self.enable_external_odds_enrich and not self.the_odds_api_key:
            logger.warning("ARES_ENABLE_EXTERNAL_ODDS_ENRICH=1 但未配置 THE_ODDS_API_KEY，赔率补采将跳过。")
        if not self.enable_titan_prematch_enrich:
            logger.info("ARES_ENABLE_TITAN_PREMATCH_ENRICH=0，已禁用 Titan prematch 补采。")

    @staticmethod
    def _extract_titan_match_id_from_html(html_fragment: str) -> Optional[str]:
        text = str(html_fragment or "")
        patterns = [
            r"/fenxi/(?:stat|shuju|touzhu|ouzhi|yazhi|rangqiu|zoushi)-(\d+)\.shtml",
            r"zq\.titan007\.com/analysis/(\d+)cn\.htm",
            r"(?:AsianOdds_n|OverDown_n)\.aspx\?id=(\d+)",
            r"1x2\.titan007\.com/oddslist/(\d+)\.htm",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    @staticmethod
    def _decode_html_bytes(content: bytes) -> Tuple[str, str]:
        for enc in ("utf-8", "gb18030", "gbk"):
            try:
                return content.decode(enc), enc
            except Exception:
                continue
        return content.decode("utf-8", errors="ignore"), "utf-8(ignore)"

    def _fetch_titan_page(self, *, page_key: str, url: str, match_id: str) -> Tuple[Dict[str, Any], Optional[str]]:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
        }
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            status_code = resp.status_code
            text, encoding_used = self._decode_html_bytes(resp.content)
            raw_path = self.raw_reports_dir / f"{self.issue}_titan_{match_id}_{page_key}.html"
            raw_path.write_text(text, encoding="utf-8")
            raw_ref = str(raw_path)
            soup = BeautifulSoup(text, "html.parser")
            title = soup.title.get_text(" ", strip=True) if soup.title else ""
            table_count = len(soup.select("table"))
            row_count = len(soup.select("tr"))
            has_init_keyword = ("初盘" in text) or ("初始" in text)
            has_live_keyword = ("即时" in text) or ("即盘" in text)
            has_ah_keyword = ("亚盘" in text) or ("让球" in text)
            has_ou_keyword = "欧赔" in text
            has_ouu_keyword = ("大小" in text) or ("Over/Under" in text)
            status = "ok" if status_code == 200 else "http_error"
            if raw_ref not in self._titan_cold_refs:
                self._titan_cold_refs.append(raw_ref)
            return {
                "status": status,
                "http_status": status_code,
                "url": url,
                "title": title,
                "encoding": encoding_used,
                "table_count": table_count,
                "row_count": row_count,
                "has_init_keyword": has_init_keyword,
                "has_live_keyword": has_live_keyword,
                "has_ah_keyword": has_ah_keyword,
                "has_ou_keyword": has_ou_keyword,
                "has_ouu_keyword": has_ouu_keyword,
            }, raw_ref
        except Exception as exc:
            return {
                "status": "error",
                "url": url,
                "error": str(exc),
            }, None

    def _fetch_titan_prematch_snapshot(self, cn_match_id: Optional[str]) -> Optional[Dict[str, Any]]:
        match_id = str(cn_match_id or "").strip()
        if not self.enable_titan_prematch_enrich or not match_id or not match_id.isdigit():
            return None
        if match_id in self._titan_prematch_cache:
            return dict(self._titan_prematch_cache[match_id])

        pages: Dict[str, Any] = {}
        raw_refs: List[str] = []
        ok_count = 0
        for page_key, tmpl in TITAN_PREMATCH_PAGE_TEMPLATES.items():
            url = tmpl.format(match_id=match_id)
            page_payload, raw_ref = self._fetch_titan_page(page_key=page_key, url=url, match_id=match_id)
            pages[page_key] = page_payload
            if raw_ref:
                raw_refs.append(raw_ref)
            if page_payload.get("status") == "ok":
                ok_count += 1
            time.sleep(0.08)

        coverage = "none"
        if ok_count == len(TITAN_PREMATCH_PAGE_TEMPLATES):
            coverage = "full"
        elif ok_count > 0:
            coverage = "partial"

        snapshot = {
            "source": "titan007",
            "match_id": match_id,
            "pages": pages,
            "raw_refs": raw_refs,
            "signals": {
                "coverage": coverage,
                "ok_page_count": ok_count,
                "total_page_count": len(TITAN_PREMATCH_PAGE_TEMPLATES),
            },
            "fetched_at": datetime.utcnow().isoformat() + "Z",
        }
        self._titan_prematch_cache[match_id] = snapshot
        return dict(snapshot)

    def _normalize_match_english(self, english: str) -> str:
        home, away = [part.strip() for part in str(english or "").split(" vs ", 1)] if " vs " in str(english or "") else (str(english or "").strip(), "")
        if not home or not away:
            return self._normalize_team_name(str(english or ""))
        return f"{self._normalize_team_name(home)}vs{self._normalize_team_name(away)}"

    def _load_manual_anchor_overrides(self) -> Dict[str, Dict[Any, Dict[str, Any]]]:
        result: Dict[str, Dict[Any, Dict[str, Any]]] = {"by_index": {}, "by_english": {}}
        if not self.vault_path:
            return result
        issue_dir = Path(self.vault_path) / "03_Match_Audits" / str(self.issue) / "03_Review_Reports"
        candidates = [
            issue_dir / f"UNMAPPED-ANCHORS-{self.issue}.json",
            issue_dir / f"UNMAPPED-ANCHORS-{self.issue}.generated.json",
        ]
        payload = None
        loaded_path = None
        for candidate in candidates:
            if not candidate.exists():
                continue
            try:
                payload = json.loads(candidate.read_text(encoding="utf-8"))
                loaded_path = candidate
                break
            except Exception as exc:
                logger.warning("读取手工锚点文件失败 %s: %s", candidate, exc)
        if not isinstance(payload, dict):
            return result

        matches = payload.get("matches")
        if not isinstance(matches, list):
            return result
        for item in matches:
            if not isinstance(item, dict):
                continue
            idx = item.get("index")
            if isinstance(idx, int):
                result["by_index"][idx] = item
            key = self._normalize_match_english(str(item.get("english") or ""))
            if key:
                result["by_english"][key] = item
        if loaded_path:
            self._manual_anchor_source_path = str(loaded_path)
            logger.info("已加载手工锚点覆盖: %s, rows=%s", loaded_path, len(matches))
        return result

    @staticmethod
    def _infer_anchor_mode(override: Dict[str, Any]) -> str:
        mode = str(override.get("anchor_mode") or "").strip().lower()
        if mode in {"smoke", "production"}:
            return mode
        notes = str(override.get("notes") or "").strip().lower()
        fbref_url = str(override.get("fbref_url") or "").strip().lower()
        if "[smoke]" in notes or fbref_url.startswith("https://anchor.local/"):
            return "smoke"
        return "production"

    def fetch_500_lottery(self) -> list:
        logger.info(f"[A端获取] 尝试从 500.com 抓取足彩期号: {self.issue}")
        url = f"https://trade.500.com/sfc/?expect={self.issue}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        }
        
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.encoding = "gbk"
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"访问 500.com 失败: {e}")
            return []

        fetch_time = datetime.utcnow().isoformat() + "Z"
        html_raw_path = self.raw_reports_dir / f"{self.issue}_500_raw.html"
        json_raw_path = self.raw_reports_dir / f"{self.issue}_500_raw.json"
        try:
            html_raw_path.write_text(resp.text, encoding="utf-8")
        except Exception as e:
            logger.warning(f"500 原始 HTML 冷存储失败: {e}")

        soup = BeautifulSoup(resp.text, "html.parser")
        tr_blocks = soup.select("tr[data-vs]")
        raw_rows = []
        matches = []
        for idx, tr in enumerate(tr_blocks, start=1):
            data_attrs: Dict[str, str] = {}
            for key, value in tr.attrs.items():
                if not str(key).startswith("data-"):
                    continue
                if isinstance(value, list):
                    data_attrs[str(key)] = " ".join(str(v) for v in value)
                else:
                    data_attrs[str(key)] = str(value)
            tr_html = str(tr)
            cn_match_id = self._extract_titan_match_id_from_html(tr_html)
            raw_rows.append(
                {
                    "index": idx,
                    "data_attrs": data_attrs,
                    "raw_tr": tr_html,
                    "cn_match_id": cn_match_id,
                }
            )

            vs = data_attrs.get("data-vs")
            if not vs:
                continue

            bjpl = data_attrs.get("data-bjpl")
            asian = data_attrs.get("data-asian")
            kl = data_attrs.get("data-kl")
            pjgl = data_attrs.get("data-pjgl")

            if "vs" in vs:
                h, a = vs.split("vs")
                
                market_snapshot = {}
                try:
                    if bjpl:
                        p = bjpl.split(",")
                        market_snapshot["europe"] = {"win": float(p[0]), "draw": float(p[1]), "loss": float(p[2])}
                    if asian:
                        p = asian.split(",")
                        market_snapshot["asian_handicap"] = {"home": float(p[0]), "line": p[1], "away": float(p[2])}
                    if kl:
                        p = kl.split(",")
                        market_snapshot["kelly_index"] = {"win": float(p[0]), "draw": float(p[1]), "loss": float(p[2])}
                    if pjgl:
                        p = pjgl.split(",")
                        market_snapshot["probabilities"] = {"win": float(p[0]), "draw": float(p[1]), "loss": float(p[2])}
                except Exception:
                    pass
                
                matches.append(
                    {
                        "home_zh": h.strip(),
                        "away_zh": a.strip(),
                        "market_snapshot": market_snapshot,
                        "cn_match_id": cn_match_id,
                    }
                )

        try:
            with open(json_raw_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "issue": self.issue,
                        "source": "500.com",
                        "source_ref": url,
                        "fetched_at": fetch_time,
                        "row_count": len(raw_rows),
                        "rows": raw_rows,
                    },
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
            self.last_500_cold_refs = [str(html_raw_path), str(json_raw_path)]
        except Exception as e:
            logger.warning(f"500 原始 JSON 冷存储失败: {e}")

        if not matches:
            logger.error("未从该期号中解析出任何有效比赛，请核实该期号存在且为 14场胜负彩。")
        else:
            logger.info(f"成功攫取 {len(matches)} 场中文物理对阵及赛前预设赔率参数。")
            
        return matches

    def translate_team(self, zh_name: str) -> str:
        team = str(zh_name or "").strip()
        if not team:
            return team
        direct = self.team_alias.get(team)
        if direct:
            return direct

        compact = re.sub(r"\s+", "", team)
        if compact and compact != team:
            direct_compact = self.team_alias.get(compact)
            if direct_compact:
                return direct_compact

        # 兼容 500 页面在不同时期返回的简称/全称差异，例如 “布雷斯” vs “布雷斯特”
        fuzzy_candidates: List[Tuple[int, str]] = []
        for alias_zh, alias_en in self.team_alias.items():
            alias_key = re.sub(r"\s+", "", str(alias_zh))
            if not alias_key:
                continue
            if alias_key in compact or compact in alias_key:
                fuzzy_candidates.append((len(alias_key), str(alias_en)))
        if fuzzy_candidates:
            fuzzy_candidates.sort(key=lambda item: item[0], reverse=True)
            return fuzzy_candidates[0][1]
        return team

    @staticmethod
    def _normalize_team_name(name: str) -> str:
        if not name:
            return ""

        ascii_name = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii")
        normalized = re.sub(r"[^a-z0-9]+", "", ascii_name.strip().lower())
        if normalized.startswith("fc") and len(normalized) > 5:
            normalized = normalized[2:]
        if normalized.endswith("club") and len(normalized) > 8:
            normalized = normalized[:-4]
        for suffix in ("afc", "fc", "cf", "ac", "sc"):
            if normalized.endswith(suffix) and len(normalized) > len(suffix) + 3:
                normalized = normalized[:-len(suffix)]
                break
        normalized = re.sub(r"\d+$", "", normalized)
        alias = {
            "fcheidenheim": "heidenheim",
            "heidenheim": "heidenheim",
            "rasenballsportleipzig": "rbleipzig",
            "hellasverona": "verona",
            "verona": "verona",
            "psg": "parissaintgermain",
            "parissaintgermain": "parissaintgermain",
            "parisstgermain": "parissaintgermain",
            "westbromwichalbion": "westbrom",
            "westbrom": "westbrom",
            "leedsunited": "leeds",
            "leeds": "leeds",
            "portsmouthfc": "portsmouth",
            "portsmouth": "portsmouth",
            "oxfordunited": "oxfordunited",
            "wrexhamafc": "wrexham",
            "wrexham": "wrexham",
            "bristolcity": "bristolcity",
            "stokecity": "stokecity",
            "millwall": "millwall",
            "norwichcity": "norwich",
            "norwich": "norwich",
            "derbycounty": "derby",
            "derby": "derby",
            "southampton": "southampton",
            "coventrycity": "coventry",
            "coventry": "coventry",
            "watford": "watford",
            "internazionale": "inter",
            "internazionalemilano": "inter",
            "intermilan": "inter",
            "inter": "inter",
            "como1907": "como",
            "como": "como",
            "athleticclub": "athleticclub",
            "athleticbilbao": "athleticclub",
            "realmadridcf": "realmadrid",
            "realmadrid": "realmadrid",
            "deportivoalaves": "alaves",
            "alaves": "alaves",
            "realbetisbalompie": "realbetis",
            "realbetis": "realbetis",
        }
        return alias.get(normalized, normalized)

    @staticmethod
    def _get_target_understat_years() -> list:
        # Understat year uses season start year; prefer newest seasons first.
        current_year = datetime.utcnow().year
        return [str(current_year), str(current_year - 1), str(current_year - 2)]

    @staticmethod
    def _parse_datetime(value: str):
        if not value:
            return None
        txt = str(value).strip()
        if not txt:
            return None

        txt = txt.replace("T", " ")
        if txt.endswith("Z"):
            txt = txt[:-1]

        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(txt, fmt)
            except Exception:
                continue
        return None

    def _extract_anchor_time(self, existing_match: dict, default_dt: datetime) -> datetime:
        if not existing_match:
            return default_dt

        history = existing_match.get("market_odds_history")
        if not isinstance(history, list):
            return default_dt

        ts_list = []
        for snap in history:
            if not isinstance(snap, dict):
                continue
            dt = self._parse_datetime(snap.get("timestamp", ""))
            if dt:
                ts_list.append(dt)

        if not ts_list:
            return default_dt
        # Use earliest odds snapshot to anchor this issue, avoid rerun-time drift.
        return min(ts_list)

    def _pick_understat_id_by_time(self, candidates: list, anchor_dt: datetime, max_gap_days: int = 45):
        if not candidates:
            return None, None, None, None

        best = None
        best_gap = None
        for m in candidates:
            m_dt = self._parse_datetime(m.get("date", ""))
            if not m_dt:
                continue
            gap = abs((m_dt - anchor_dt).total_seconds())
            if best is None or gap < best_gap:
                best = m
                best_gap = gap

        if best is None:
            return None, None, None, None

        gap_days = round(best_gap / 86400.0, 3)
        if gap_days > max_gap_days:
            return None, None, gap_days, None

        return best.get("id"), best.get("date"), gap_days, best.get("league")

    def _fetch_understat_league(self, league: str, year: str) -> list:
        url = f"https://understat.com/getLeagueData/{league}/{year}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "X-Requested-With": "XMLHttpRequest"
        }
        matches = []
        last_error = None

        for attempt in range(1, 4):
            try:
                resp = requests.get(url, headers=headers, timeout=12)
                if resp.status_code != 200:
                    last_error = f"HTTP {resp.status_code}"
                    time.sleep(0.8 * attempt)
                    continue

                data = resp.json()
                if "dates" in data:
                    for m in data["dates"]:
                        matches.append({
                            "id": m["id"],
                            "home_en": m["h"]["title"],
                            "away_en": m["a"]["title"],
                            "date": m["datetime"],
                            "league": league,
                        })
                return matches
            except requests.exceptions.RequestException as e:
                last_error = str(e)
                time.sleep(0.8 * attempt)
            except Exception as e:
                last_error = str(e)
                break

        if last_error:
            logger.error(f"解析联赛数据失败 {league}/{year}: {last_error}")
        return matches

    def build_understat_db(self, year="2023") -> list:
        logger.info(f"[B端获取] 开始潜入 Understat 内部 API 构建全球五大联赛全息日历 (年份: {year})...")
        leagues = ["EPL", "La_liga", "Bundesliga", "Serie_A", "Ligue_1"]
        global_matches = []
        for lg in leagues:
            logger.info(f"  > 同步联赛: {lg}")
            global_matches.extend(self._fetch_understat_league(lg, year))
            time.sleep(0.5)
        logger.info(f"全息日历构建完毕，总条目数: {len(global_matches)} 场。")
        return global_matches

    @staticmethod
    def _extract_fbref_table_soup(html: str) -> BeautifulSoup:
        soup = BeautifulSoup(html, "html.parser")
        if soup.find("table", id=re.compile(r"^sched_")):
            return soup

        for node in soup.find_all(string=lambda text: isinstance(text, Comment)):
            text = str(node)
            if "sched_" not in text or "<table" not in text:
                continue
            comment_soup = BeautifulSoup(text, "html.parser")
            if comment_soup.find("table", id=re.compile(r"^sched_")):
                return comment_soup
        return soup

    def _fetch_fbref_comp_matches(self, comp_id: int, league_name: str) -> List[Dict[str, Any]]:
        url = f"https://fbref.com/en/comps/{comp_id}/schedule/"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
        }

        try:
            resp = requests.get(url, headers=headers, timeout=20)
            if resp.status_code != 200:
                logger.warning(f"FBref 赛程抓取失败 {league_name}({comp_id}) HTTP {resp.status_code}")
                return []
        except Exception as e:
            logger.warning(f"FBref 赛程抓取异常 {league_name}({comp_id}): {e}")
            return []

        soup = self._extract_fbref_table_soup(resp.text)
        rows = soup.select("table tr")
        matches: List[Dict[str, Any]] = []

        for tr in rows:
            date_cell = tr.select_one("td[data-stat='date']")
            home_cell = tr.select_one("td[data-stat='home_team']")
            away_cell = tr.select_one("td[data-stat='away_team']")
            if not date_cell or not home_cell or not away_cell:
                continue

            date_txt = date_cell.get_text(" ", strip=True)
            home_en = home_cell.get_text(" ", strip=True)
            away_en = away_cell.get_text(" ", strip=True)
            if not date_txt or not home_en or not away_en:
                continue

            report_link = tr.select_one("td[data-stat='match_report'] a")
            fbref_url = None
            if report_link and report_link.get("href"):
                href = report_link.get("href")
                fbref_url = f"https://fbref.com{href}" if href.startswith("/") else href

            matches.append(
                {
                    "home_en": home_en,
                    "away_en": away_en,
                    "date": date_txt,
                    "fbref_url": fbref_url,
                    "league": league_name,
                }
            )

        return matches

    @staticmethod
    def _sanitize_segment(value: str, fallback: str = "segment") -> str:
        txt = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value).strip())
        txt = txt.strip("_")
        return txt or fallback

    def _fetch_football_data_comp_matches(
        self,
        *,
        competition_code: str,
        league_name: str,
        date_from: str,
        date_to: str,
    ) -> List[Dict[str, Any]]:
        if not self.football_data_api_key:
            return []

        url = f"{self.football_data_base_url}/competitions/{competition_code}/matches"
        headers = {"X-Auth-Token": self.football_data_api_key}
        params = {"dateFrom": date_from, "dateTo": date_to}

        try:
            resp = requests.get(url, headers=headers, params=params, timeout=20)
            if resp.status_code != 200:
                logger.warning(
                    "football-data 抓取失败 %s(%s) HTTP %s",
                    league_name,
                    competition_code,
                    resp.status_code,
                )
                return []
            data = resp.json()
        except Exception as e:
            logger.warning("football-data 抓取异常 %s(%s): %s", league_name, competition_code, e)
            return []

        cold_path = self.raw_reports_dir / f"{self.issue}_football_data_{competition_code}_raw.json"
        try:
            with open(cold_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "issue": self.issue,
                        "source": "football-data.org",
                        "source_ref": url,
                        "fetched_at": datetime.utcnow().isoformat() + "Z",
                        "competition_code": competition_code,
                        "league_name": league_name,
                        "params": params,
                        "response": data,
                    },
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
            self._football_data_cold_refs.append(str(cold_path))
        except Exception as e:
            logger.warning("football-data 原始 JSON 冷存储失败 %s: %s", competition_code, e)

        matches: List[Dict[str, Any]] = []
        for m in data.get("matches", []) or []:
            home_team = (m.get("homeTeam") or {}).get("name")
            away_team = (m.get("awayTeam") or {}).get("name")
            utc_date = m.get("utcDate")
            match_id = m.get("id")
            if not home_team or not away_team or not utc_date or not match_id:
                continue
            matches.append(
                {
                    "id": match_id,
                    "home_en": home_team,
                    "away_en": away_team,
                    "date": utc_date,
                    "league": league_name,
                    "competition_code": competition_code,
                    "competition_name": ((m.get("competition") or {}).get("name") or league_name),
                }
            )
        return matches

    def build_football_data_db(self, anchor_dt: Optional[datetime] = None) -> List[Dict[str, Any]]:
        if not self.football_data_api_key:
            return []

        anchor = anchor_dt or datetime.utcnow()
        date_from = (anchor - timedelta(days=10)).strftime("%Y-%m-%d")
        date_to = (anchor + timedelta(days=14)).strftime("%Y-%m-%d")

        logger.info(
            "[football-data 回退] 同步赛程窗口 %s ~ %s（含五大联赛+次级联赛）...",
            date_from,
            date_to,
        )

        all_matches: List[Dict[str, Any]] = []
        for code, league_name in FOOTBALL_DATA_COMPETITIONS.items():
            comp_matches = self._fetch_football_data_comp_matches(
                competition_code=code,
                league_name=league_name,
                date_from=date_from,
                date_to=date_to,
            )
            logger.info("  > football-data 同步: %s(%s) => %s 条", league_name, code, len(comp_matches))
            all_matches.extend(comp_matches)
            time.sleep(0.15)
        logger.info("football-data 赛程索引构建完毕，总条目数: %s", len(all_matches))
        return all_matches

    def _pick_football_data_match_by_time(self, candidates: list, anchor_dt: datetime, max_gap_days: int = 45):
        if not candidates:
            return None, None, None, None, None

        best = None
        best_gap = None
        for m in candidates:
            m_dt = self._parse_datetime(m.get("date", ""))
            if not m_dt:
                continue
            gap = abs((m_dt - anchor_dt).total_seconds())
            if best is None or gap < best_gap:
                best = m
                best_gap = gap

        if best is None:
            return None, None, None, None, None

        gap_days = round(best_gap / 86400.0, 3)
        if gap_days > max_gap_days:
            return None, None, gap_days, None, None

        return (
            best.get("id"),
            best.get("date"),
            gap_days,
            best.get("league"),
            best.get("competition_code"),
        )

    @staticmethod
    def _to_iso_z(dt: datetime) -> str:
        return dt.replace(microsecond=0).isoformat() + "Z"

    @staticmethod
    def _build_odds_cache_key(
        sport_key: str,
        commence_time_from: Optional[str],
        commence_time_to: Optional[str],
    ) -> str:
        return f"{sport_key}|{commence_time_from or ''}|{commence_time_to or ''}"

    def _fetch_the_odds_sport_events(
        self,
        sport_key: str,
        *,
        commence_time_from: Optional[str] = None,
        commence_time_to: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        cache_key = self._build_odds_cache_key(sport_key, commence_time_from, commence_time_to)
        if cache_key in self._odds_events_cache:
            return self._odds_events_cache[cache_key]

        if not self.the_odds_api_key:
            self._odds_events_cache[cache_key] = []
            return []

        url = f"{self.the_odds_base_url}/sports/{sport_key}/odds"
        params = {
            "apiKey": self.the_odds_api_key,
            "regions": "eu,uk",
            "markets": "h2h",
            "oddsFormat": "decimal",
            "dateFormat": "iso",
        }
        if commence_time_from:
            params["commenceTimeFrom"] = commence_time_from
        if commence_time_to:
            params["commenceTimeTo"] = commence_time_to
        headers = {"User-Agent": "Ares-OSINT-Telemetry/1.0"}

        try:
            resp = requests.get(url, params=params, headers=headers, timeout=20)
            if resp.status_code != 200:
                logger.warning("The Odds API 拉取失败 %s HTTP %s", sport_key, resp.status_code)
                self._odds_events_cache[cache_key] = []
                return []
            events = resp.json()
        except Exception as e:
            logger.warning("The Odds API 拉取异常 %s: %s", sport_key, e)
            self._odds_events_cache[cache_key] = []
            return []

        cold_name = self._sanitize_segment(sport_key, "sport")
        cold_path = self.raw_reports_dir / f"{self.issue}_the_odds_{cold_name}_raw.json"
        try:
            with open(cold_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "issue": self.issue,
                        "source": "the-odds-api.com",
                        "source_ref": url,
                        "fetched_at": datetime.utcnow().isoformat() + "Z",
                        "sport_key": sport_key,
                        "params": {k: v for k, v in params.items() if k != "apiKey"},
                        "response": events,
                    },
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
            self._odds_cold_refs.append(str(cold_path))
        except Exception as e:
            logger.warning("The Odds API 原始 JSON 冷存储失败 %s: %s", sport_key, e)

        self._odds_events_cache[cache_key] = events if isinstance(events, list) else []
        return self._odds_events_cache[cache_key]

    def _pick_the_odds_event_by_time(
        self,
        events: List[Dict[str, Any]],
        *,
        home_en: str,
        away_en: str,
        anchor_dt: datetime,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[float]]:
        if not events:
            return None, None
        home_norm = self._normalize_team_name(home_en)
        away_norm = self._normalize_team_name(away_en)
        best_event = None
        best_gap = None
        for event in events:
            event_dt = self._parse_datetime(event.get("commence_time", ""))
            if not event_dt:
                continue
            event_home = self._normalize_team_name(str(event.get("home_team", "")))
            event_away = self._normalize_team_name(str(event.get("away_team", "")))
            home_away_match = (event_home == home_norm and event_away == away_norm)
            away_home_match = (event_home == away_norm and event_away == home_norm)
            if not home_away_match and not away_home_match:
                continue
            gap = abs((event_dt - anchor_dt).total_seconds())
            if best_event is None or gap < best_gap:
                best_event = event
                best_gap = gap
        if best_event is None:
            return None, None
        return best_event, round(best_gap / 86400.0, 3)

    def _extract_the_odds_h2h_snapshot(self, event: Dict[str, Any]) -> Dict[str, Any]:
        bookmakers = event.get("bookmakers") or []
        for bm in bookmakers:
            markets = bm.get("markets") or []
            for mk in markets:
                if mk.get("key") != "h2h":
                    continue
                outcomes = mk.get("outcomes") or []
                odds_map = {}
                for out in outcomes:
                    name = out.get("name")
                    price = out.get("price")
                    if name is None or price is None:
                        continue
                    odds_map[name] = price
                if odds_map:
                    return {
                        "bookmaker_key": bm.get("key"),
                        "bookmaker_title": bm.get("title"),
                        "last_update": mk.get("last_update"),
                        "odds": odds_map,
                    }
        return {}

    def _enrich_external_odds_snapshot(
        self,
        *,
        home_en: str,
        away_en: str,
        league: Optional[str],
        anchor_dt: datetime,
        target_match_time: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        if not self.enable_external_odds_enrich or not self.the_odds_api_key:
            return None
        if not league:
            return None
        sport_key = LEAGUE_TO_ODDS_SPORT_KEY.get(league)
        if not sport_key:
            return None

        # The issue id is not used for odds lookup.
        # We always search by mapped match time + team pair.
        target_dt = self._parse_datetime(target_match_time) if target_match_time else None
        now_dt = datetime.utcnow()
        if target_dt and target_dt < now_dt - timedelta(hours=2):
            return {
                "provider": "the-odds-api.com",
                "status": "skipped_historical_on_free_plan",
                "sport_key": sport_key,
                "target_match_time": target_match_time,
                "reason": "historical_odds_requires_paid_plan",
                "fetched_at": datetime.utcnow().isoformat() + "Z",
            }

        commence_from = None
        commence_to = None
        reference_dt = anchor_dt
        if target_dt:
            reference_dt = target_dt
            commence_from = self._to_iso_z(target_dt - timedelta(hours=36))
            commence_to = self._to_iso_z(target_dt + timedelta(hours=36))

        events = self._fetch_the_odds_sport_events(
            sport_key,
            commence_time_from=commence_from,
            commence_time_to=commence_to,
        )
        event, gap_days = self._pick_the_odds_event_by_time(
            events,
            home_en=home_en,
            away_en=away_en,
            anchor_dt=reference_dt,
        )
        if not event:
            return {
                "provider": "the-odds-api.com",
                "status": "no_match_in_feed",
                "sport_key": sport_key,
                "target_match_time": target_match_time,
                "commence_time_from": commence_from,
                "commence_time_to": commence_to,
                "fetched_at": datetime.utcnow().isoformat() + "Z",
            }
        h2h_snapshot = self._extract_the_odds_h2h_snapshot(event)
        if not h2h_snapshot:
            return {
                "provider": "the-odds-api.com",
                "status": "event_found_but_no_h2h_market",
                "sport_key": sport_key,
                "event_id": event.get("id"),
                "commence_time": event.get("commence_time"),
                "target_match_time": target_match_time,
                "fetched_at": datetime.utcnow().isoformat() + "Z",
            }
        return {
            "provider": "the-odds-api.com",
            "status": "ok",
            "sport_key": sport_key,
            "event_id": event.get("id"),
            "commence_time": event.get("commence_time"),
            "gap_days": gap_days,
            "home_team": event.get("home_team"),
            "away_team": event.get("away_team"),
            "target_match_time": target_match_time,
            "h2h_snapshot": h2h_snapshot,
            "fetched_at": datetime.utcnow().isoformat() + "Z",
        }

    def build_fbref_db(self) -> List[Dict[str, Any]]:
        logger.info("[FBref 回退] 开始同步可用联赛赛程数据用于次级联赛回退...")
        all_matches: List[Dict[str, Any]] = []
        for _, (comp_id, league_name) in FBREF_COMPETITIONS.items():
            comp_matches = self._fetch_fbref_comp_matches(comp_id, league_name)
            logger.info(f"  > FBref 同步: {league_name}({comp_id}) => {len(comp_matches)} 条")
            all_matches.extend(comp_matches)
            time.sleep(0.4)
        logger.info(f"FBref 赛程索引构建完毕，总条目数: {len(all_matches)}")
        return all_matches

    def _pick_fbref_match_by_time(self, candidates: list, anchor_dt: datetime, max_gap_days: int = 45):
        if not candidates:
            return None, None, None, None

        best = None
        best_gap = None
        for m in candidates:
            if not m.get("fbref_url"):
                continue
            m_dt = self._parse_datetime(m.get("date", ""))
            if not m_dt:
                continue
            gap = abs((m_dt - anchor_dt).total_seconds())
            if best is None or gap < best_gap:
                best = m
                best_gap = gap

        if best is None:
            return None, None, None, None

        gap_days = round(best_gap / 86400.0, 3)
        if gap_days > max_gap_days:
            return None, None, gap_days, None

        return best.get("fbref_url"), best.get("date"), gap_days, best.get("league")

    def scan_and_map(self):
        # 1. Fetch Chinese matches & odds
        cn_matches = self.fetch_500_lottery()
        if not cn_matches:
            return
            
        manifest_path = self.raw_reports_dir / f"{self.issue}_dispatch_manifest.json"
        
        # 2. Check if we already mapped these to save API calls
        output_manifest = None
        if manifest_path.exists():
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    output_manifest = json.load(f)
                logger.info("检测到已存在的 dispatch_manifest，将执行动态追加赔率 (Track Variances) 模式...")
            except Exception:
                output_manifest = None
                
        if not output_manifest or "matches" not in output_manifest or not output_manifest["matches"]:
            output_manifest = {
                "issue": self.issue,
                "mapping_status": "OK",
                "cold_data_refs": self.last_500_cold_refs,
                "matches": []
            }
            needs_db = True
            logger.info("[B端与C端融合] 开始双极映射扫描 (首次构建)...")
        else:
            # Check if any existing matches need remapping/healing.
            needs_db = False
            for m in output_manifest["matches"]:
                has_understat = bool(m.get("understat_id"))
                has_fbref = bool(m.get("fbref_url"))
                has_football_data = bool(m.get("football_data_match_id"))
                understat_date = m.get("understat_date")
                understat_gap_days = m.get("understat_gap_days")

                # Understat mapped but missing date audit => heal.
                if has_understat and not understat_date:
                    needs_db = True
                    break
                # Understat mapped but gap suspicious => heal.
                try:
                    if (
                        has_understat
                        and understat_gap_days is not None
                        and float(understat_gap_days) > self.mapping_max_gap_days
                    ):
                        needs_db = True
                        break
                except Exception:
                    pass
                # No Understat and no FBref fallback => heal.
                if (not has_understat) and (not has_fbref) and (not has_football_data):
                    needs_db = True
                    break
            if needs_db:
                logger.info("[B端与C端融合] 检测到存在缺失映射/缺失时间审计字段，启动自修复重新映射机制...")
                
        output_manifest["cold_data_refs"] = self.last_500_cold_refs

        if needs_db:
            understat_db = []
            for year in self._get_target_understat_years():
                understat_db.extend(self.build_understat_db(year=year))
            understat_index = {}
            for m in understat_db:
                key = (
                    self._normalize_team_name(m["home_en"]),
                    self._normalize_team_name(m["away_en"])
                )
                understat_index.setdefault(key, []).append(m)

            fbref_db = self.build_fbref_db()
            fbref_index = {}
            for m in fbref_db:
                key = (
                    self._normalize_team_name(m["home_en"]),
                    self._normalize_team_name(m["away_en"])
                )
                fbref_index.setdefault(key, []).append(m)

            football_data_db = self.build_football_data_db(anchor_dt=datetime.utcnow())
            football_data_index = {}
            for m in football_data_db:
                key = (
                    self._normalize_team_name(m["home_en"]),
                    self._normalize_team_name(m["away_en"]),
                )
                football_data_index.setdefault(key, []).append(m)
        else:
            understat_db = []
            understat_index = {}
            fbref_db = []
            fbref_index = {}
            football_data_db = []
            football_data_index = {}
            
        success_count = 0
        current_time_dt = datetime.utcnow()
        current_time = current_time_dt.isoformat() + "Z"
        
        for i, match in enumerate(cn_matches):
            home_zh = match["home_zh"]
            away_zh = match["away_zh"]
            cn_match_id = str(match.get("cn_match_id") or "").strip() or None
            market_snapshot = match.get("market_snapshot", {})
            market_snapshot["timestamp"] = current_time
            
            existing_match = None
            if output_manifest.get("matches"):
                for em in output_manifest["matches"]:
                    if em.get("index") == i + 1:
                        existing_match = em
                        break
            
            home_en = self.translate_team(home_zh)
            away_en = self.translate_team(away_zh)
            titan_prematch_snapshot = self._fetch_titan_prematch_snapshot(cn_match_id)
            
            # Understat -> FBref 双源映射
            found_id = None
            found_date = None
            found_gap_days = None
            found_fbref_url = None
            found_fbref_date = None
            found_fbref_gap_days = None
            found_football_data_match_id = None
            found_football_data_date = None
            found_football_data_gap_days = None
            found_football_data_competition = None
            found_league = None

            if understat_index or fbref_index or football_data_index:
                lookup_key = (
                    self._normalize_team_name(home_en),
                    self._normalize_team_name(away_en)
                )
                anchor_dt = self._extract_anchor_time(existing_match, current_time_dt)

                understat_candidates = understat_index.get(lookup_key, [])
                found_id, found_date, found_gap_days, found_league = self._pick_understat_id_by_time(
                    understat_candidates,
                    anchor_dt,
                    max_gap_days=self.mapping_max_gap_days,
                )

                if not found_id:
                    fbref_candidates = fbref_index.get(lookup_key, [])
                    (
                        found_fbref_url,
                        found_fbref_date,
                        found_fbref_gap_days,
                        fbref_league,
                    ) = self._pick_fbref_match_by_time(
                        fbref_candidates,
                        anchor_dt,
                        max_gap_days=self.mapping_max_gap_days,
                    )
                    if fbref_league:
                        found_league = fbref_league

                if not found_id and not found_fbref_url:
                    football_data_candidates = football_data_index.get(lookup_key, [])
                    (
                        found_football_data_match_id,
                        found_football_data_date,
                        found_football_data_gap_days,
                        football_data_league,
                        found_football_data_competition,
                    ) = self._pick_football_data_match_by_time(
                        football_data_candidates,
                        anchor_dt,
                        max_gap_days=self.mapping_max_gap_days,
                    )
                    if football_data_league:
                        found_league = football_data_league

            elif existing_match:
                anchor_dt = self._extract_anchor_time(existing_match, current_time_dt)
                found_id = existing_match.get("understat_id")
                found_date = existing_match.get("understat_date")
                found_gap_days = existing_match.get("understat_gap_days")
                found_fbref_url = existing_match.get("fbref_url")
                found_fbref_date = existing_match.get("fbref_date")
                found_fbref_gap_days = existing_match.get("fbref_gap_days")
                found_football_data_match_id = existing_match.get("football_data_match_id")
                found_football_data_date = existing_match.get("football_data_date")
                found_football_data_gap_days = existing_match.get("football_data_gap_days")
                found_football_data_competition = existing_match.get("football_data_competition")
                found_league = existing_match.get("league")
            else:
                anchor_dt = current_time_dt

            override = self._manual_anchor_overrides["by_index"].get(i + 1)
            manual_anchor_mode = None
            manual_anchor_notes = None
            manual_anchor_applied = False
            if not override:
                override_key = self._normalize_match_english(f"{home_en} vs {away_en}")
                override = self._manual_anchor_overrides["by_english"].get(override_key)
            if override and not (found_id or found_fbref_url or found_football_data_match_id):
                found_id = override.get("understat_id") or found_id
                found_fbref_url = override.get("fbref_url") or found_fbref_url
                found_football_data_match_id = override.get("football_data_match_id") or found_football_data_match_id
                found_date = override.get("understat_date") or found_date
                found_fbref_date = override.get("fbref_date") or found_fbref_date
                found_football_data_date = override.get("football_data_date") or found_football_data_date
                found_league = override.get("league") or found_league
                manual_anchor_mode = self._infer_anchor_mode(override)
                manual_anchor_notes = str(override.get("notes") or "").strip() or None
                manual_anchor_applied = True
                logger.info(
                    "[%s/14] 应用手工锚点覆盖: %s vs %s (understat=%s fbref=%s football-data=%s)",
                    i + 1,
                    home_zh,
                    away_zh,
                    bool(found_id),
                    bool(found_fbref_url),
                    bool(found_football_data_match_id),
                )
            if not manual_anchor_applied and found_fbref_url and str(found_fbref_url).lower().startswith("https://anchor.local/"):
                logger.info(
                    "[%s/14] 检测到历史 smoke 锚点残留，已清空以避免误判生产映射: %s vs %s",
                    i + 1,
                    home_zh,
                    away_zh,
                )
                found_fbref_url = None
                found_fbref_date = None
                found_fbref_gap_days = None

            mapping_source = "unmapped"
            if found_id:
                mapping_source = "understat"
            elif found_fbref_url:
                mapping_source = "fbref"
            elif found_football_data_match_id:
                mapping_source = "football-data"
            elif (
                isinstance(titan_prematch_snapshot, dict)
                and str(
                    ((titan_prematch_snapshot.get("signals") or {}).get("coverage") or "none")
                ).strip().lower()
                in {"full", "partial"}
            ):
                # Titan 仅作为 prematch 映射回退锚点，不用于 postmatch xG 主源。
                mapping_source = "titan"

            mapped_match_time = None
            if mapping_source == "understat":
                mapped_match_time = found_date
            elif mapping_source == "fbref":
                mapped_match_time = found_fbref_date
            elif mapping_source == "football-data":
                mapped_match_time = found_football_data_date

            external_odds_snapshot = self._enrich_external_odds_snapshot(
                home_en=home_en,
                away_en=away_en,
                league=found_league,
                anchor_dt=anchor_dt,
                target_match_time=mapped_match_time,
            )

            if existing_match:
                # Merge into existing map
                existing_match["understat_id"] = found_id
                existing_match["understat_date"] = found_date
                existing_match["understat_gap_days"] = found_gap_days
                existing_match["fbref_url"] = found_fbref_url
                existing_match["fbref_date"] = found_fbref_date
                existing_match["fbref_gap_days"] = found_fbref_gap_days
                existing_match["football_data_match_id"] = found_football_data_match_id
                existing_match["football_data_date"] = found_football_data_date
                existing_match["football_data_gap_days"] = found_football_data_gap_days
                existing_match["football_data_competition"] = found_football_data_competition
                existing_match["chinese"] = f"{home_zh} vs {away_zh}"
                existing_match["english"] = f"{home_en} vs {away_en}"
                existing_match["cn_match_id"] = cn_match_id
                existing_match["mapping_source"] = mapping_source
                if found_league:
                    existing_match["league"] = found_league
                if titan_prematch_snapshot:
                    existing_match["titan_prematch"] = titan_prematch_snapshot
                if manual_anchor_applied:
                    existing_match["manual_anchor_applied"] = True
                    existing_match["manual_anchor_mode"] = manual_anchor_mode
                    existing_match["manual_anchor_notes"] = manual_anchor_notes
                    existing_match["manual_anchor_source"] = self._manual_anchor_source_path
                else:
                    existing_match["manual_anchor_applied"] = False
                    existing_match["manual_anchor_mode"] = None
                    existing_match["manual_anchor_notes"] = None
                    existing_match["manual_anchor_source"] = None
                if "market_odds_history" not in existing_match:
                    existing_match["market_odds_history"] = []
                # Remove initial legacy snapshot mapping if present, just keep history clean
                existing_match["market_odds_history"].append(market_snapshot)
                if external_odds_snapshot:
                    if "external_odds_history" not in existing_match or not isinstance(existing_match["external_odds_history"], list):
                        existing_match["external_odds_history"] = []
                    existing_match["external_odds_history"].append(external_odds_snapshot)
                
                if found_id:
                    logger.info(
                        f"[{i+1}/14] 已映射（追踪更新）: {home_zh} vs {away_zh} "
                        f"(ID: {found_id}, date: {found_date}, gap_days: {found_gap_days})"
                    )
                    success_count += 1
                elif found_fbref_url:
                    logger.info(
                        f"[{i+1}/14] 已映射（FBref回退）: {home_zh} vs {away_zh} "
                        f"(url: {found_fbref_url}, date: {found_fbref_date}, gap_days: {found_fbref_gap_days})"
                    )
                    success_count += 1
                elif found_football_data_match_id:
                    logger.info(
                        f"[{i+1}/14] 已映射（football-data回退）: {home_zh} vs {away_zh} "
                        f"(match_id: {found_football_data_match_id}, date: {found_football_data_date}, gap_days: {found_football_data_gap_days})"
                    )
                    success_count += 1
                else:
                    logger.warning(
                        f"[{i+1}/14] 依然无法映射: {home_zh} vs {away_zh} "
                        "(超纲赛事或时间门禁未通过，Understat/FBref/football-data 均未命中)"
                    )
            else:
                # Add fully new match
                if found_id:
                    logger.info(
                        f"[{i+1}/14] 映射成功: {home_zh} vs {away_zh} -> {home_en} vs {away_en} "
                        f"(ID: {found_id}, date: {found_date}, gap_days: {found_gap_days})"
                    )
                    success_count += 1
                elif found_fbref_url:
                    logger.info(
                        f"[{i+1}/14] 映射成功（FBref回退）: {home_zh} vs {away_zh} -> {home_en} vs {away_en} "
                        f"(url: {found_fbref_url}, date: {found_fbref_date}, gap_days: {found_fbref_gap_days})"
                    )
                    success_count += 1
                elif found_football_data_match_id:
                    logger.info(
                        f"[{i+1}/14] 映射成功（football-data回退）: {home_zh} vs {away_zh} -> {home_en} vs {away_en} "
                        f"(match_id: {found_football_data_match_id}, date: {found_football_data_date}, gap_days: {found_football_data_gap_days})"
                    )
                    success_count += 1
                else:
                    logger.warning(
                        f"[{i+1}/14] 映射失败或超纲: {home_zh} vs {away_zh} "
                        "(未能匹配到 Understat/FBref/football-data 或时间门禁未通过)"
                    )

                match_item = {
                    "index": i + 1,
                    "chinese": f"{home_zh} vs {away_zh}",
                    "english": f"{home_en} vs {away_en}",
                    "cn_match_id": cn_match_id,
                    "understat_id": found_id,
                    "understat_date": found_date,
                    "understat_gap_days": found_gap_days,
                    "fbref_url": found_fbref_url,
                    "fbref_date": found_fbref_date,
                    "fbref_gap_days": found_fbref_gap_days,
                    "football_data_match_id": found_football_data_match_id,
                    "football_data_date": found_football_data_date,
                    "football_data_gap_days": found_football_data_gap_days,
                    "football_data_competition": found_football_data_competition,
                    "mapping_source": mapping_source,
                    "league": found_league,
                    "titan_prematch": titan_prematch_snapshot,
                    "manual_anchor_applied": bool(manual_anchor_applied),
                    "manual_anchor_mode": manual_anchor_mode,
                    "manual_anchor_notes": manual_anchor_notes,
                    "manual_anchor_source": self._manual_anchor_source_path if manual_anchor_applied else None,
                    "market_odds_history": [market_snapshot]
                }
                if external_odds_snapshot:
                    match_item["external_odds_history"] = [external_odds_snapshot]
                output_manifest["matches"].append(match_item)
            
        output_manifest["cold_data_refs"] = (
            list(
                dict.fromkeys(
                    self.last_500_cold_refs
                    + self._football_data_cold_refs
                    + self._odds_cold_refs
                    + self._titan_cold_refs
                )
            )
        )

        logger.warning(
            f"扫描收尾，14 场对阵成功映射 {success_count} 场。"
            "缺失的通常为欧冠/欧联或拼写不匹配。"
        )
        
        # 保存派发单
        manifest_path = self.raw_reports_dir / f"{self.issue}_dispatch_manifest.json"
        try:
            with open(manifest_path, "w", encoding='utf-8') as f:
                json.dump(output_manifest, f, ensure_ascii=False, indent=2)
            logger.info(f"Ares 战术派发单已落盘 -> {manifest_path}")
        except Exception as e:
            logger.error(f"保存落盘失败: {e}")

        if self.audit_router.enabled:
            try:
                self.audit_router.ensure_issue_governance(
                    issue=self.issue,
                    manifest=output_manifest,
                    create_prematch_stubs=True,
                )
            except Exception as e:
                logger.warning(f"AuditRouter 自动整理失败（不影响主流程）: {e}")

        return manifest_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ares OSINT Telemetry - PREMATCH Crawler Mapping")
    parser.add_argument("--issue", type=str, required=True, help="中国体彩 足彩期号，如 24040")
    args = parser.parse_args()
    
    load_dotenv_into_env(Path(__file__).resolve().parent.parent.parent)
    crawler = AresOsintCrawler(issue=args.issue)
    crawler.scan_and_map()
