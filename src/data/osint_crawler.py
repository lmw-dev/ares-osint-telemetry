import os
import json
import logging
import argparse
import time
import re
import requests
from urllib.error import HTTPError
from bs4 import BeautifulSoup
from pathlib import Path

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
            
        # Regex extraction
        matches_raw = re.findall(r'data-vs="(.*?)"', resp.text)
        
        matches = []
        for raw in matches_raw:
            if "vs" in raw:
                h, a = raw.split("vs")
                matches.append({"home_zh": h.strip(), "away_zh": a.strip()})
                
        if not matches:
            logger.error("未从该期号中解析出任何有效比赛，请核实该期号存在且为 14场胜负彩。")
        else:
            logger.info(f"成功攫取 {len(matches)} 场中文物理对阵。")
            
        return matches

    def _fetch_understat_league(self, league: str, year: str) -> list:
        url = f"https://understat.com/league/{league}/{year}"
        headers = {"User-Agent": "Mozilla/5.0"}
        matches = []
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code != 200:
                return []
            match = re.search(r"var datesData\s*=\s*JSON\.parse\('([^']+)'\)", resp.text)
            if match:
                data_str = match.group(1).encode("utf-8").decode("unicode_escape")
                data = json.loads(data_str)
                for m in data:
                    matches.append({
                        "id": m["id"],
                        "home_en": m["h"]["title"],
                        "away_en": m["a"]["title"],
                        "date": m["datetime"]
                    })
        except Exception:
            pass
        return matches

    def build_understat_db(self, year="2023") -> list:
        logger.info(f"[B端获取] 开始潜入 Understat 构建全球五大联赛全息日历 (年份: {year})...")
        leagues = ["EPL", "La_liga", "Bundesliga", "Serie_A", "Ligue_1"]
        global_matches = []
        for lg in leagues:
            logger.info(f"  > 同步联赛: {lg}")
            global_matches.extend(self._fetch_understat_league(lg, year))
            time.sleep(0.5)
        logger.info(f"全息日历构建完毕，总条目数: {len(global_matches)} 场。")
        return global_matches

    def translate_team(self, zh_name: str) -> str:
        return self.team_alias.get(zh_name, zh_name)

    def scan_and_map(self):
        # 1. Fetch Chinese matches
        cn_matches = self.fetch_500_lottery()
        if not cn_matches:
            return
            
        # 2. Build Understat DB
        # To make it robust, we fetch both 2023 and 2024 to cross-reference (since seasons span years)
        db = self.build_understat_db(year="2023")
        db.extend(self.build_understat_db(year="2024"))
        
        # 3. Mapping
        logger.info("[C端融合] 开始双极映射扫描...")
        output_manifest = {
            "issue": self.issue,
            "mapping_status": "OK",
            "matches": []
        }
        
        success_count = 0
        for i, match in enumerate(cn_matches):
            home_zh = match["home_zh"]
            away_zh = match["away_zh"]
            
            home_en = self.translate_team(home_zh)
            away_en = self.translate_team(away_zh)
            
            # Simple linear search
            found_id = None
            for u_match in db:
                if home_en == u_match["home_en"] and away_en == u_match["away_en"]:
                    found_id = u_match["id"]
                    break
                    
            if found_id:
                logger.info(f"[{i+1}/14] 映射成功: {home_zh} vs {away_zh} -> {home_en} vs {away_en} (ID: {found_id})")
                success_count += 1
            else:
                logger.warning(f"[{i+1}/14] 映射失败或超纲: {home_zh} vs {away_zh} (未能匹配到 Understat 五大联赛表)")
                
            output_manifest["matches"].append({
                "index": i + 1,
                "chinese": f"{home_zh} vs {away_zh}",
                "english": f"{home_en} vs {away_en}",
                "understat_id": found_id
            })
            
        logger.warning(f"扫描收尾，14 场对阵成功映射 {success_count} 场。缺失的通常为欧冠/欧联等特殊杯赛或次级联赛。")
        
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
