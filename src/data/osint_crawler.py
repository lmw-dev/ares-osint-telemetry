import os
import json
import logging
import argparse
import time
import re
import requests
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from bs4 import BeautifulSoup, Comment

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
        self.raw_reports_dir = self.base_dir / "raw_reports"
        self.raw_reports_dir.mkdir(parents=True, exist_ok=True)
        self.last_500_cold_refs = []
        self._football_data_cold_refs: List[str] = []
        self._odds_cold_refs: List[str] = []
        self._odds_events_cache: Dict[str, List[Dict[str, Any]]] = {}
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
        self.football_data_base_url = str(
            os.getenv("ARES_FOOTBALL_DATA_BASE_URL", "https://api.football-data.org/v4")
        ).rstrip("/")

        self.the_odds_api_key = (
            str(os.getenv("ARES_THE_ODDS_API_KEY", "")).strip()
            or str(os.getenv("THE_ODDS_API_KEY", "")).strip()
        )
        self.the_odds_base_url = str(
            os.getenv("ARES_THE_ODDS_BASE_URL", "https://api.the-odds-api.com/v4")
        ).rstrip("/")
        self.enable_external_odds_enrich = str(
            os.getenv("ARES_ENABLE_EXTERNAL_ODDS_ENRICH", "0")
        ).strip().lower() in {"1", "true", "yes", "on"}

        if not self.football_data_api_key:
            logger.info("未配置 football-data API Key，映射回退将跳过 football-data 源。")
        if self.enable_external_odds_enrich and not self.the_odds_api_key:
            logger.warning("ARES_ENABLE_EXTERNAL_ODDS_ENRICH=1 但未配置 THE_ODDS_API_KEY，赔率补采将跳过。")

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

        tr_blocks = re.findall(r"<tr[^>]*data-vs=\"[^\"]+\"[^>]*>", resp.text)
        raw_rows = []
        matches = []
        for idx, tr in enumerate(tr_blocks, start=1):
            data_attrs = {k: v for k, v in re.findall(r"\b(data-[a-zA-Z0-9_-]+)=\"([^\"]*)\"", tr)}
            raw_rows.append({"index": idx, "data_attrs": data_attrs, "raw_tr": tr})

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
                
                matches.append({"home_zh": h.strip(), "away_zh": a.strip(), "market_snapshot": market_snapshot})

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
        return self.team_alias.get(zh_name, zh_name)

    @staticmethod
    def _normalize_team_name(name: str) -> str:
        if not name:
            return ""

        normalized = re.sub(r"[\s\.\-']", "", name.strip().lower())
        alias = {
            "fcheidenheim": "heidenheim",
            "heidenheim": "heidenheim",
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
            "intermilan": "inter",
            "inter": "inter",
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

    def _fetch_the_odds_sport_events(self, sport_key: str) -> List[Dict[str, Any]]:
        if sport_key in self._odds_events_cache:
            return self._odds_events_cache[sport_key]

        if not self.the_odds_api_key:
            self._odds_events_cache[sport_key] = []
            return []

        url = f"{self.the_odds_base_url}/sports/{sport_key}/odds"
        params = {
            "apiKey": self.the_odds_api_key,
            "regions": "eu,uk",
            "markets": "h2h",
            "oddsFormat": "decimal",
            "dateFormat": "iso",
        }
        headers = {"User-Agent": "Ares-OSINT-Telemetry/1.0"}

        try:
            resp = requests.get(url, params=params, headers=headers, timeout=20)
            if resp.status_code != 200:
                logger.warning("The Odds API 拉取失败 %s HTTP %s", sport_key, resp.status_code)
                self._odds_events_cache[sport_key] = []
                return []
            events = resp.json()
        except Exception as e:
            logger.warning("The Odds API 拉取异常 %s: %s", sport_key, e)
            self._odds_events_cache[sport_key] = []
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

        self._odds_events_cache[sport_key] = events if isinstance(events, list) else []
        return self._odds_events_cache[sport_key]

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
            if event_home != home_norm or event_away != away_norm:
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
    ) -> Optional[Dict[str, Any]]:
        if not self.enable_external_odds_enrich or not self.the_odds_api_key:
            return None
        if not league:
            return None
        sport_key = LEAGUE_TO_ODDS_SPORT_KEY.get(league)
        if not sport_key:
            return None
        events = self._fetch_the_odds_sport_events(sport_key)
        event, gap_days = self._pick_the_odds_event_by_time(
            events,
            home_en=home_en,
            away_en=away_en,
            anchor_dt=anchor_dt,
        )
        if not event:
            return None
        h2h_snapshot = self._extract_the_odds_h2h_snapshot(event)
        if not h2h_snapshot:
            return None
        return {
            "provider": "the-odds-api.com",
            "sport_key": sport_key,
            "event_id": event.get("id"),
            "commence_time": event.get("commence_time"),
            "gap_days": gap_days,
            "home_team": event.get("home_team"),
            "away_team": event.get("away_team"),
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
                    if has_understat and understat_gap_days is not None and float(understat_gap_days) > 45:
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
                )

                if not found_id:
                    fbref_candidates = fbref_index.get(lookup_key, [])
                    (
                        found_fbref_url,
                        found_fbref_date,
                        found_fbref_gap_days,
                        fbref_league,
                    ) = self._pick_fbref_match_by_time(fbref_candidates, anchor_dt)
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
                    ) = self._pick_football_data_match_by_time(football_data_candidates, anchor_dt)
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

            mapping_source = "unmapped"
            if found_id:
                mapping_source = "understat"
            elif found_fbref_url:
                mapping_source = "fbref"
            elif found_football_data_match_id:
                mapping_source = "football-data"

            external_odds_snapshot = self._enrich_external_odds_snapshot(
                home_en=home_en,
                away_en=away_en,
                league=found_league,
                anchor_dt=anchor_dt,
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
                existing_match["mapping_source"] = mapping_source
                if found_league:
                    existing_match["league"] = found_league
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
                    "market_odds_history": [market_snapshot]
                }
                if external_odds_snapshot:
                    match_item["external_odds_history"] = [external_odds_snapshot]
                output_manifest["matches"].append(match_item)
            
        output_manifest["cold_data_refs"] = (
            list(dict.fromkeys(self.last_500_cold_refs + self._football_data_cold_refs + self._odds_cold_refs))
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ares OSINT Telemetry - PREMATCH Crawler Mapping")
    parser.add_argument("--issue", type=str, required=True, help="中国体彩 足彩期号，如 24040")
    args = parser.parse_args()
    
    load_dotenv_into_env(Path(__file__).resolve().parent.parent.parent)
    crawler = AresOsintCrawler(issue=args.issue)
    crawler.scan_and_map()
