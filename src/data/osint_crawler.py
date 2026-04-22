import os
import json
import logging
import argparse
import time
import re
import requests
from pathlib import Path
from datetime import datetime
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

class AresOsintCrawler:
    def __init__(self, issue: str):
        self.issue = issue
        self.base_dir = Path(__file__).resolve().parent.parent.parent
        self.raw_reports_dir = self.base_dir / "raw_reports"
        self.raw_reports_dir.mkdir(parents=True, exist_ok=True)
        self.last_500_cold_refs = []
        
        # Load aliases
        alias_path = self.base_dir / "src" / "data" / "team_alias_map.json"
        try:
            with open(alias_path, 'r', encoding='utf-8') as f:
                self.team_alias = json.load(f)
        except Exception as e:
            logger.warning(f"无法加载字典 {e}")
            self.team_alias = {}

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
                if (not has_understat) and (not has_fbref):
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
        else:
            understat_db = []
            understat_index = {}
            fbref_db = []
            fbref_index = {}
            
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
            found_league = None

            if understat_index or fbref_index:
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

            elif existing_match:
                found_id = existing_match.get("understat_id")
                found_date = existing_match.get("understat_date")
                found_gap_days = existing_match.get("understat_gap_days")
                found_fbref_url = existing_match.get("fbref_url")
                found_fbref_date = existing_match.get("fbref_date")
                found_fbref_gap_days = existing_match.get("fbref_gap_days")
                found_league = existing_match.get("league")
                        
            if existing_match:
                # Merge into existing map
                existing_match["understat_id"] = found_id
                existing_match["understat_date"] = found_date
                existing_match["understat_gap_days"] = found_gap_days
                existing_match["fbref_url"] = found_fbref_url
                existing_match["fbref_date"] = found_fbref_date
                existing_match["fbref_gap_days"] = found_fbref_gap_days
                existing_match["chinese"] = f"{home_zh} vs {away_zh}"
                existing_match["english"] = f"{home_en} vs {away_en}"
                if found_league:
                    existing_match["league"] = found_league
                if "market_odds_history" not in existing_match:
                    existing_match["market_odds_history"] = []
                # Remove initial legacy snapshot mapping if present, just keep history clean
                existing_match["market_odds_history"].append(market_snapshot)
                
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
                else:
                    logger.warning(
                        f"[{i+1}/14] 依然无法映射: {home_zh} vs {away_zh} "
                        "(超纲赛事或时间门禁未通过)"
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
                else:
                    logger.warning(
                        f"[{i+1}/14] 映射失败或超纲: {home_zh} vs {away_zh} "
                        "(未能匹配到 Understat/FBref 或时间门禁未通过)"
                    )
                    
                output_manifest["matches"].append({
                    "index": i + 1,
                    "chinese": f"{home_zh} vs {away_zh}",
                    "english": f"{home_en} vs {away_en}",
                    "understat_id": found_id,
                    "understat_date": found_date,
                    "understat_gap_days": found_gap_days,
                    "fbref_url": found_fbref_url,
                    "fbref_date": found_fbref_date,
                    "fbref_gap_days": found_fbref_gap_days,
                    "league": found_league,
                    "market_odds_history": [market_snapshot]
                })
            
        logger.warning(f"扫描收尾，14 场对阵成功映射 {success_count} 场。缺失的通常为欧冠/欧联或拼写不匹配。")
        
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
    
    crawler = AresOsintCrawler(issue=args.issue)
    crawler.scan_and_map()
