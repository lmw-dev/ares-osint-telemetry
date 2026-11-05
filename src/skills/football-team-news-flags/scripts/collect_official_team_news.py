#!/usr/bin/env python3
"""
EPL 球队伤停官方原件采集与降级保护执行器 (Ares OSINT Telemetry Pipeline)
严格遵守 Raw-Evidence-First 铁律：
1. 优先扫描各俱乐部官网赛前发布会速记与伤病更新专栏；
2. 成功获取静态落地正文者，落盘原件并写入 _meta.json，标记 VERIFIED 并提取正向 flag；
3. 遇到 JS 客户端水合渲染（JS Shell）、403 拦截或无正文时，生成 _blocked.json 阻断事实，标记 PENDING_NOT_PUBLISHED 并将 flags 严格清空为 []；
4. 自动生成标准 team_news_flags.json 与 TEAM_NEWS_EVIDENCE_MATRIX.md。
"""

import urllib.request
import ssl
import json
import hashlib
import os
import time
import argparse
from bs4 import BeautifulSoup

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
}

# 已知英超官方俱乐部新闻首页与高频赛前专栏路径
CLUB_OFFICIAL_DIRECTORIES = {
    "Liverpool": {
        "index": "https://www.liverpoolfc.com/news",
        "keywords": ["injury", "Forest", "team news", "Slot", "availability"]
    },
    "Leeds United": {
        "index": "https://www.leedsunited.com/news",
        "keywords": ["fitness", "Brentford", "Farke", "availability", "injury"]
    },
    "Newcastle": {
        "index": "https://www.newcastleunited.com/en/news",
        "keywords": ["team news", "Spurs", "Tottenham", "Jaissle", "injury", "how to watch"]
    },
    "Tottenham": {
        "index": "https://www.tottenhamhotspur.com/news",
        "keywords": ["team news", "Newcastle", "injury", "Postecoglou"]
    },
    "Arsenal": {
        "index": "https://www.arsenal.com/news",
        "keywords": ["team news", "Villa", "Arteta", "injury", "press conference"]
    },
    "Aston Villa": {
        "index": "https://www.avfc.co.uk/news",
        "keywords": ["team news", "Arsenal", "Emery", "injury", "press conference"]
    },
    "Chelsea": {
        "index": "https://www.chelseafc.com/en/news",
        "keywords": ["team news", "Brighton", "Maresca", "injury", "press conference"]
    },
    "Brighton": {
        "index": "https://www.brightonandhovealbion.com/news",
        "keywords": ["team news", "Chelsea", "Hurzeler", "injury"]
    },
    "Manchester United": {
        "index": "https://www.manutd.com/en/news",
        "keywords": ["team news", "Ipswich", "injury", "press conference"]
    },
    "Ipswich Town": {
        "index": "https://www.itfc.co.uk/news",
        "keywords": ["team news", "United", "McKenna", "injury"]
    },
    "Bournemouth": {
        "index": "https://www.afcb.co.uk/news",
        "keywords": ["team news", "Everton", "Iraola", "injury"]
    },
    "Everton": {
        "index": "https://www.evertonfc.com/news",
        "keywords": ["team news", "Bournemouth", "Dyche", "injury"]
    },
    "Nottingham Forest": {
        "index": "https://www.nottinghamforest.co.uk/news",
        "keywords": ["team news", "Liverpool", "Nuno", "injury"]
    },
    "Brentford": {
        "index": "https://www.brentfordfc.com/en/news",
        "keywords": ["team news", "Leeds", "Frank", "injury"]
    },
    "Fulham": {
        "index": "https://www.fulhamfc.com/news",
        "keywords": ["team news", "Sunderland", "Silva", "injury"]
    },
    "Sunderland": {
        "index": "https://www.safc.com/news",
        "keywords": ["team news", "Fulham", "Le Bris", "injury"]
    },
    "Coventry City": {
        "index": "https://www.ccfc.co.uk/news",
        "keywords": ["team news", "Hull", "Robins", "injury"]
    },
    "Hull City": {
        "index": "https://www.wearehullcity.co.uk/news",
        "keywords": ["team news", "Coventry", "Walter", "injury"]
    }
}

def scan_and_collect_team_news(team, fixture, out_dir):
    team_dir_name = team.replace(" ", "_")
    target_dir = os.path.join(out_dir, team_dir_name)
    os.makedirs(target_dir, exist_ok=True)
    
    cfg = CLUB_OFFICIAL_DIRECTORIES.get(team)
    if not cfg:
        print(f"  [未配置官网] {team}")
        return None
        
    url_index = cfg["index"]
    keywords = cfg["keywords"]
    print(f"\n>>> 扫描 [{team}] 官方新闻: {url_index} ...")
    
    candidate_links = []
    block_reason = None
    http_status = None
    
    req = urllib.request.Request(url_index, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=12, context=CTX) as resp:
            http_status = resp.status
            html = resp.read().decode("utf-8", errors="ignore")
            soup = BeautifulSoup(html, "html.parser")
            text_all = soup.get_text(strip=True)
            
            if len(text_all) < 200:
                block_reason = f"JS_SHELL_OR_EMPTY: Page returned HTTP 200 but text length is only {len(text_all)} chars (Client-side React/Next hydration shell)."
            else:
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    t = a.get_text(strip=True)
                    if any(k.lower() in t.lower() or k.lower() in href.lower() for k in keywords):
                        if len(t) > 10 or "news" in href:
                            full_url = href if href.startswith("http") else url_index.rstrip("/") + "/" + href.lstrip("/")
                            candidate_links.append((t, full_url))
                            
                if not candidate_links:
                    block_reason = f"NO_DIRECT_LINKS_IN_STATIC_HTML: Index returned HTTP 200 but match-specific preview links are dynamically injected via client-side script."
    except urllib.error.HTTPError as e:
        http_status = e.code
        block_reason = f"HTTP_ERROR_{e.code}: {e.reason}"
    except Exception as e:
        block_reason = f"NETWORK_ERROR: {str(e)}"
        
    ts_now = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    
    # 若有阻断或无直接文章链接，落盘 _blocked.json
    if block_reason or not candidate_links:
        blocked_file = os.path.join(target_dir, f"{ts_now}_official_index_blocked.json")
        record = {
            "source_url": url_index,
            "source_name": f"{team} Official Index",
            "accessed_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "team": team,
            "fixture": fixture,
            "http_status": http_status,
            "block_reason": block_reason,
            "parse_status": "BLOCKED"
        }
        with open(blocked_file, "w", encoding="utf-8") as wf:
            json.dump(record, wf, ensure_ascii=False, indent=2)
        print(f"  -> 记录阻断原件: {block_reason[:80]}...")
        return {
            "team": team,
            "fixture": fixture,
            "news_status": "PENDING_NOT_PUBLISHED",
            "flags": [],
            "unknowns": [block_reason],
            "evidence_files": [f"raw/team_news/{team_dir_name}/{os.path.basename(blocked_file)}"]
        }

    # 尝试抓取候选文章中第一篇最相关的
    best_title, best_url = candidate_links[0]
    print(f"  -> 尝试抓取具体文章: {best_title[:50]} -> {best_url}")
    
    try:
        art_req = urllib.request.Request(best_url, headers=HEADERS)
        with urllib.request.urlopen(art_req, timeout=12, context=CTX) as a_resp:
            a_data = a_resp.read()
            a_html = a_data.decode("utf-8", errors="ignore")
            a_soup = BeautifulSoup(a_html, "html.parser")
            body = a_soup.get_text(separator="\n", strip=True)
            sha = hashlib.sha256(a_data).hexdigest()
            
            slug = re.sub(r'[^a-zA-Z0-9_]', '_', best_title[:30]).lower()
            out_raw = os.path.join(target_dir, f"{ts_now}_{slug}.txt")
            out_meta = os.path.join(target_dir, f"{ts_now}_{slug}_meta.json")
            
            with open(out_raw, "w", encoding="utf-8") as wf:
                wf.write(body)
                
            meta = {
                "source_url": best_url,
                "source_name": f"{team} Official Article",
                "accessed_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "published_at": time.strftime("%Y-%m-%d", time.gmtime()),
                "team": team,
                "fixture": fixture,
                "source_tier": "T1",
                "sha256": sha,
                "extraction_status": "SUCCESS",
                "blocker_reason": None,
                "raw_file": f"raw/team_news/{team_dir_name}/{os.path.basename(out_raw)}"
            }
            with open(out_meta, "w", encoding="utf-8") as wf:
                json.dump(meta, wf, ensure_ascii=False, indent=2)
                
            print(f"  ✅ 官方原件落地成功! 长度: {len(body)} 字符, SHA: {sha[:10]}...")
            return {
                "team": team,
                "fixture": fixture,
                "news_status": "VERIFIED",
                "flags": [], # 由调用方基于正文解析提炼
                "unknowns": [],
                "evidence_files": [f"raw/team_news/{team_dir_name}/{os.path.basename(out_raw)}", f"raw/team_news/{team_dir_name}/{os.path.basename(out_meta)}"]
            }
    except Exception as e:
        blocked_file = os.path.join(target_dir, f"{ts_now}_article_fetch_failed_blocked.json")
        b_record = {
            "source_url": best_url,
            "source_name": f"{team} Official Article",
            "accessed_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "team": team,
            "fixture": fixture,
            "http_status": None,
            "block_reason": f"FETCH_FAILED: {str(e)}",
            "parse_status": "BLOCKED"
        }
        with open(blocked_file, "w", encoding="utf-8") as wf:
            json.dump(b_record, wf, ensure_ascii=False, indent=2)
        return {
            "team": team,
            "fixture": fixture,
            "news_status": "PENDING_NOT_PUBLISHED",
            "flags": [],
            "unknowns": [f"Article fetch error: {e}"],
            "evidence_files": [f"raw/team_news/{team_dir_name}/{os.path.basename(blocked_file)}"]
        }

def run_team_news_pipeline(target_root, league="epl", denom_path=None):
    is_vault = ("03_Match_Audits" in target_root) or os.path.exists(os.path.join(target_root, "00_Denominator_Ledger.json"))
    if is_vault:
        team_news_raw_dir = os.path.join(target_root, "01_Evidence/raw/team_news")
    else:
        team_news_raw_dir = os.path.join(target_root, "raw/team_news")
    os.makedirs(team_news_raw_dir, exist_ok=True)
    
    print(f"=== 启动官方伤停采集与降级保护流水线 ===")
    print(f"  联赛: {league.upper()} | 输出目录: {team_news_raw_dir}")
    
    ZH_TO_EN_EPL = {
        "伯恩茅斯": "Bournemouth",
        "布伦特福德": "Brentford",
        "阿斯顿维拉": "Aston Villa",
        "诺丁汉森林": "Nottingham Forest",
        "切尔西": "Chelsea",
        "赫尔城": "Hull City",
        "水晶宫": "Crystal Palace",
        "伊普斯维奇": "Ipswich Town",
        "利物浦": "Liverpool",
        "富勒姆": "Fulham",
        "托特纳姆热刺": "Tottenham",
        "埃弗顿": "Everton",
        "桑德兰": "Sunderland",
        "阿森纳": "Arsenal",
        "考文垂": "Coventry",
        "布莱顿": "Brighton",
        "曼彻斯特联": "Manchester United",
        "曼彻斯特城": "Manchester City",
        "利兹联": "Leeds United",
        "纽卡斯尔联": "Newcastle",
        "西汉姆联": "West Ham",
        "狼队": "Wolves"
    }

    # 提取比赛球队列表：优先检查 target_root 下已采集生成的 parsed/*_odds.json
    fixtures = []
    parsed_dirs_to_check = [
        os.path.join(target_root, "parsed"),
        os.path.join(target_root, "01_Evidence/skill_runs/parsed")
    ]
    for p_dir in parsed_dirs_to_check:
        if os.path.exists(p_dir):
            import glob
            for of in sorted(glob.glob(os.path.join(p_dir, "*_odds.json"))):
                try:
                    with open(of, "r", encoding="utf-8") as f:
                        od = json.load(f)
                        h_raw = od.get("home", "")
                        a_raw = od.get("away", "")
                        h_en = ZH_TO_EN_EPL.get(h_raw, h_raw)
                        a_en = ZH_TO_EN_EPL.get(a_raw, a_raw)
                        if h_en and a_en:
                            fixtures.append((h_en, a_en))
                except Exception:
                    pass
            if fixtures:
                break

    if not fixtures:
        if denom_path and os.path.exists(denom_path):
            with open(denom_path, "r", encoding="utf-8") as df:
                d_data = json.load(df)
                for fx in d_data.get("fixtures", []):
                    fixtures.append((fx.get("home"), fx.get("away")))
        elif league.lower() == "bundesliga":
            fixtures = [
                ("VfB Stuttgart", "1. FC Köln"),
                ("TSG Hoffenheim", "Borussia Dortmund"),
                ("Bayer 04 Leverkusen", "1. FC Union Berlin"),
                ("Borussia Mönchengladbach", "SV 07 Elversberg"),
                ("SV Werder Bremen", "RB Leipzig"),
                ("SC Paderborn 07", "SC Freiburg"),
                ("FC Schalke 04", "FC Bayern München"),
                ("Hamburger SV", "1. FSV Mainz 05"),
                ("Eintracht Frankfurt", "FC Augsburg")
            ]
        else:
            # 默认英超样例对阵
            fixtures = [
                ("Liverpool", "Nottingham Forest"),
                ("Leeds United", "Brentford"),
                ("Newcastle", "Tottenham"),
                ("Chelsea", "Brighton"),
                ("Arsenal", "Aston Villa")
            ]

    teams_result = []
    matrix_lines = [
        f"# {league.upper()} 官方伤停与降级保护证据链矩阵",
        "",
        "> **核心原则**：Raw-Evidence-First。唯有取得官网落地正文方可准入正向 Flag；未接入官方静态白名单或遇阻断，强制执行降级保护，正向 Flags 清空为 `[]`。",
        "> **防虚构严正声明**：严禁将空伤停数组 `flags=[]` 误读为全员健康；缺失官方 T1 医疗确证时，严禁模型主观脑补或硬编码伤停。",
        f"> **纪律管辖红线**：{league.upper()} 国内联赛纪律处罚严禁套用外部杯赛抵扣规则，停赛裁决受限于法定纪律范围。",
        "",
        "| 序号 | 球队名称 | 所属对阵 | 证据状态 | 正向 Flag 数 | Prematch 准入判定 | 降级保护说明与纪律声明 |",
        "|:---:|---|---|:---:|:---:|:---:|---|"
    ]

    idx = 1
    for home, away in fixtures:
        fixture_name = f"{home} vs {away}"
        for team in [home, away]:
            team_slug = team.lower().replace(" ", "_").replace(".", "").replace("04", "04").replace("07", "07").replace("05", "05")
            team_dir = os.path.join(team_news_raw_dir, team_slug)
            os.makedirs(team_dir, exist_ok=True)
            
            # 如果是英超且在俱乐部白名单中，尝试抓取
            if league.lower() == "epl" and team in CLUB_OFFICIAL_DIRECTORIES:
                res = scan_and_collect_team_news(team, fixture_name, team_news_raw_dir)
                teams_result.append(res)
                st = res["news_status"]
                f_count = len(res["flags"])
                admit = "🟢 可准入" if st == "VERIFIED" else "🔴 严禁进入模型"
                matrix_lines.append(f"| {idx} | **{team}** | {fixture_name} | {'🟢' if st == 'VERIFIED' else '🟡'} **{st}** | {f_count} 个 | {admit} | {'官方原件确证' if st == 'VERIFIED' else '降级保护'} |")
            else:
                # 德甲或未配置官网抓取白名单，执行严格降级保护
                ts_now = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
                blocked_p = os.path.join(team_dir, f"{ts_now}_not_in_static_whitelist_blocked.json")
                b_data = {
                    "source_url": None,
                    "source_name": f"{team} Official Channel",
                    "accessed_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "team": team,
                    "fixture": fixture_name,
                    "http_status": None,
                    "block_reason": f"TEAM_NEWS_NOT_IN_STATIC_SCRAPER_WHITELIST: {league.upper()} clubs not configured for headless HTML scraping; Raw-Evidence-First fallback triggered",
                    "parse_status": "BLOCKED"
                }
                with open(blocked_p, "w", encoding="utf-8") as bf:
                    json.dump(b_data, bf, ensure_ascii=False, indent=2)
                    
                teams_result.append({
                    "team": team,
                    "fixture": fixture_name,
                    "news_status": "PENDING_NOT_PUBLISHED",
                    "flags": [],
                    "unknowns": ["官方新闻源未配置本地静态抓取白名单，已安全触发降级保护，正向 flags 置空"],
                    "evidence_files": [os.path.relpath(blocked_p, target_root)]
                })
                matrix_lines.append(f"| {idx} | **{team}** | {fixture_name} | 🟡 **PENDING_NOT_PUBLISHED** | 0 个 | 🔴 **严禁进入模型** | 缺乏官网 T1 静态速记，严格降级保护 |")
            idx += 1

    # 保存 team_news_flags.json
    flags_path = os.path.join(team_news_raw_dir, "team_news_flags.json")
    with open(flags_path, "w", encoding="utf-8") as wf:
        json.dump({
            "meta": {
                "league": league.upper(),
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "policy": "Raw-Evidence-First Strict Compliance"
            },
            "teams": teams_result
        }, wf, ensure_ascii=False, indent=2)
        
    matrix_path = os.path.join(team_news_raw_dir, "TEAM_NEWS_EVIDENCE_MATRIX.md")
    with open(matrix_path, "w", encoding="utf-8") as wf:
        wf.write("\n".join(matrix_lines))
        
    top_matrix_path = os.path.join(target_root, "TEAM_NEWS_EVIDENCE_MATRIX.md")
    with open(top_matrix_path, "w", encoding="utf-8") as wf:
        wf.write("\n".join(matrix_lines))
        
    print(f"✅ 官方伤停与降级保护数据已全部落地: {team_news_raw_dir}")

def main():
    parser = argparse.ArgumentParser(description="自动抓取或审计指定对阵的官方伤停与降级保护记录")
    parser.add_argument("--output-root", "--output_root", dest="output_root", default=None, help="项目输出根目录")
    parser.add_argument("--output_dir", "--output-dir", dest="output_dir", default=None, help="兼容性输出根目录")
    parser.add_argument("--league", default="epl", help="联赛标识 (epl, bundesliga 等)")
    parser.add_argument("--denominator", default=None, help="权威 Denominator 路径")
    args = parser.parse_args()
    
    target_root = args.output_root or args.output_dir
    if not target_root:
        raise ValueError("必须指定 --output-root 或 --output_dir！")
        
    run_team_news_pipeline(target_root, args.league, args.denominator)

if __name__ == "__main__":
    main()
