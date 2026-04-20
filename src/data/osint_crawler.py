import os
import json
import logging
import argparse
import time
import re
import requests
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AresTelemetry.Crawler")

class AresOsintCrawler:
    def __init__(self, issue: str):
        self.issue = issue
        self.base_dir = Path(__file__).resolve().parent.parent.parent
        self.raw_reports_dir = self.base_dir / "raw_reports"
        self.raw_reports_dir.mkdir(parents=True, exist_ok=True)
        
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
            
        matches = []
        for tr in re.findall(r"<tr[^>]*data-vs=\"[^\"]+\"[^>]*>", resp.text):
            vs_m = re.search(r"data-vs=\"([^\"]+)\"", tr)
            if not vs_m:
                continue
            vs = vs_m.group(1)
            
            bjpl = re.search(r"data-bjpl=\"([^\"]+)\"", tr)
            asian = re.search(r"data-asian=\"([^\"]+)\"", tr)
            kl = re.search(r"data-kl=\"([^\"]+)\"", tr)
            pjgl = re.search(r"data-pjgl=\"([^\"]+)\"", tr)
            
            if "vs" in vs:
                h, a = vs.split("vs")
                
                market_snapshot = {}
                try:
                    if bjpl:
                        p = bjpl.group(1).split(",")
                        market_snapshot["europe"] = {"win": float(p[0]), "draw": float(p[1]), "loss": float(p[2])}
                    if asian:
                        p = asian.group(1).split(",")
                        market_snapshot["asian_handicap"] = {"home": float(p[0]), "line": p[1], "away": float(p[2])}
                    if kl:
                        p = kl.group(1).split(",")
                        market_snapshot["kelly_index"] = {"win": float(p[0]), "draw": float(p[1]), "loss": float(p[2])}
                    if pjgl:
                        p = pjgl.group(1).split(",")
                        market_snapshot["probabilities"] = {"win": float(p[0]), "draw": float(p[1]), "loss": float(p[2])}
                except Exception:
                    pass
                
                matches.append({"home_zh": h.strip(), "away_zh": a.strip(), "market_snapshot": market_snapshot})
                
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
        }
        return alias.get(normalized, normalized)

    @staticmethod
    def _get_target_understat_years() -> list:
        # Understat year uses season start year; prefer newest seasons first.
        current_year = datetime.utcnow().year
        return [str(current_year), str(current_year - 1), str(current_year - 2)]

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
                            "date": m["datetime"]
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
                "matches": []
            }
            needs_db = True
            logger.info("[B端与C端融合] 开始双极映射扫描 (首次构建)...")
        else:
            # Check if any existing matches are missing understat_ids
            needs_db = False
            for m in output_manifest["matches"]:
                if not m.get("understat_id"):
                    needs_db = True
                    break
            if needs_db:
                logger.info("[B端与C端融合] 检测到存在缺失 ID 的历史遗留场次，启动自修复重新映射机制...")
                
        if needs_db:
            db = []
            for year in self._get_target_understat_years():
                db.extend(self.build_understat_db(year=year))
            db_index = {}
            for m in db:
                key = (
                    self._normalize_team_name(m["home_en"]),
                    self._normalize_team_name(m["away_en"])
                )
                if key not in db_index:
                    db_index[key] = m["id"]
        else:
            db = []
            db_index = {}
            
        success_count = 0
        current_time = datetime.utcnow().isoformat() + "Z"
        
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
            
            # 只有在初次创建，或者历史记录中没有抓到ID的情况下，才进行 B端查找
            found_id = None
            if existing_match and existing_match.get("understat_id"):
                found_id = existing_match.get("understat_id")
            elif db_index:
                lookup_key = (
                    self._normalize_team_name(home_en),
                    self._normalize_team_name(away_en)
                )
                found_id = db_index.get(lookup_key)
                        
            if existing_match:
                # Merge into existing map
                existing_match["understat_id"] = found_id
                existing_match["chinese"] = f"{home_zh} vs {away_zh}"
                existing_match["english"] = f"{home_en} vs {away_en}"
                if "market_odds_history" not in existing_match:
                    existing_match["market_odds_history"] = []
                # Remove initial legacy snapshot mapping if present, just keep history clean
                existing_match["market_odds_history"].append(market_snapshot)
                
                if found_id:
                    logger.info(f"[{i+1}/14] 已映射（追踪更新）: {home_zh} vs {away_zh} (ID: {found_id})")
                    success_count += 1
                else:
                    logger.warning(f"[{i+1}/14] 依然无法映射: {home_zh} vs {away_zh} (超纲赛事)")
            else:
                # Add fully new match
                if found_id:
                    logger.info(f"[{i+1}/14] 映射成功: {home_zh} vs {away_zh} -> {home_en} vs {away_en} (ID: {found_id})")
                    success_count += 1
                else:
                    logger.warning(f"[{i+1}/14] 映射失败或超纲: {home_zh} vs {away_zh} (未能匹配到 Understat ID)")
                    
                output_manifest["matches"].append({
                    "index": i + 1,
                    "chinese": f"{home_zh} vs {away_zh}",
                    "english": f"{home_en} vs {away_en}",
                    "understat_id": found_id,
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
