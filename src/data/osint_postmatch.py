import os
import json
import logging
import argparse
import re
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import yaml
import requests
from bs4 import BeautifulSoup

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AresTelemetry.PostMatch")

class MatchTelemetryPipeline:
    _dotenv_loaded = False

    def __init__(
        self,
        issue: str,
        match_id: str,
        source: str = "auto",
        fbref_url: Optional[str] = None,
        official_score: Optional[str] = None,
    ):
        self.issue = issue
        self.match_id = match_id
        self.source = source
        self.fbref_url = fbref_url or (match_id if "fbref.com" in str(match_id).lower() else None)
        self.official_score = self._normalize_score(official_score) if official_score else None
        if official_score and not self.official_score:
            raise ValueError(f"official_score 格式非法: {official_score}（应为 2-1）")
        
        # 路径配置
        self.base_dir = Path(__file__).resolve().parent.parent.parent
        self.raw_reports_dir = self.base_dir / "raw_reports"
        self.raw_reports_dir.mkdir(parents=True, exist_ok=True)

        self._load_project_env_file()
        self.vault_path = os.getenv("ARES_VAULT_PATH")
        if not self.vault_path:
            logger.warning("未检测到环境变量 ARES_VAULT_PATH，热数据报告默认输出到 draft_reports 目录。")
        
        # 加载队名映射
        self.alias_map = self._load_team_alias_map()

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
    def _normalize_score(score: Optional[str]) -> Optional[str]:
        if not score:
            return None
        txt = str(score).strip()
        m = re.match(r"^\s*(\d+)\s*[-:：]\s*(\d+)\s*$", txt)
        if not m:
            return None
        return f"{int(m.group(1))}-{int(m.group(2))}"

    def _dump_raw_artifact(self, suffix: str, content: str) -> str:
        path = self.raw_reports_dir / f"{self.issue}_{self.match_id}_{suffix}"
        path.write_text(content, encoding="utf-8")
        return str(path)

    def _dump_raw_json_artifact(self, suffix: str, payload: Dict[str, Any]) -> str:
        path = self.raw_reports_dir / f"{self.issue}_{self.match_id}_{suffix}"
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
        
        # 落盘结构化冷数据
        raw_file_path = self.raw_reports_dir / f"{self.issue}_{self.match_id}.json"
        try:
            with open(raw_file_path, "w", encoding='utf-8') as f:
                json.dump(raw_data, f, ensure_ascii=False, indent=2)
            logger.info(f"冷存储数据已落盘 -> {raw_file_path}")
        except Exception as e:
            logger.error(f"冷存储落盘失败: {e}")
            
        return raw_data

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
        if self.vault_path:
            out_dir = Path(self.vault_path) / "3_Resources" / "3.x_Match_Reports"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"{self.issue}_{self.match_id}_postmatch.md"
        else:
            out_dir = self.base_dir / "draft_reports"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"{self.issue}_{self.match_id}_postmatch.md"

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
        
        out_file = self.generate_markdown(hot_data)
        logger.info(f"[{self.match_id}] Pipeline 节点执行完毕。")
        return out_file

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ares OSINT Telemetry - PostMatch Extraction")
    parser.add_argument("--issue", type=str, required=True, help="期号，如 26062")
    parser.add_argument("--match-id", type=str, required=False, help="目标比赛的唯一ID (如果不填，则全自动执行14场复盘)")
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
    
    if args.match_id:
        pipeline = MatchTelemetryPipeline(
            issue=args.issue,
            match_id=args.match_id,
            source=args.source,
            fbref_url=args.fbref_url,
            official_score=args.official_score,
        )
        pipeline.run()
    else:
        # Batch Mode
        logger.info(f"未提供特定 match_id，启动全自动批量复盘引擎 (Issue: {args.issue})...")
        base_dir = Path(__file__).resolve().parent.parent.parent
        manifest_path = base_dir / "raw_reports" / f"{args.issue}_dispatch_manifest.json"
        
        if not manifest_path.exists():
            logger.error(f"找不到战术派发单 {manifest_path}，请先执行赛前爬虫 (osint_crawler.py)！")
        else:
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
            
            success = 0
            skipped = 0
            generated_reports = []
            for match in manifest.get("matches", []):
                fbref_url = match.get("fbref_url")
                uid = match.get("understat_id") or fbref_url
                official_score = match.get("official_score") or match.get("result_score")
                if uid:
                    logger.info(f"==> 正在批量复盘: {match['chinese']} (Ref: {uid})")
                    pipeline = MatchTelemetryPipeline(
                        issue=args.issue,
                        match_id=str(uid),
                        source=args.source,
                        fbref_url=fbref_url,
                        official_score=official_score,
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
