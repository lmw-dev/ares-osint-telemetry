# 🤖 Antigravity 智能 Skills 使用手册

> **“以大模型为执行引擎，在 Antigravity 中直接闭环运行。”**

在 `Ares v4.1` 的全新架构中，我们移除了旧版繁琐的外部 Python 执行脚本（即不需要外部 DeepSeek/OpenAI 的 API Key 和网络请求管道）。
所有的 Skill 均已被重构为 **模型无关的 Agent 规范文件 (SKILL.md)**。

当您在 Antigravity 中加载并执行这些 Skill 时，**您当前对话的 AI 模型（无论是 Claude、Gemini 还是其他模型）将直接扮演分析引擎的角色**，并利用本地的 `search_web` 和 `read_url_content` 工具，完成实时情报的抓取、交叉比对、异常标志判定，并最终输出格式化的 Markdown 报告与 JSON 数据至 `AresVault`。

---

## 🛰️ 智能 Skills 快速索引

目前已在 `src/skills/` 下注册以下六大核心 OSINT 技能：

| 技能名称 | 目录路径 | 核心定位 | 最终落盘路径 (Obsidian Vault) |
| :--- | :--- | :--- | :--- |
| **football-team-news-flags** | `src/skills/football-team-news-flags/` | 赛前球队关键异常情报抓取 (伤停/发布会口径/舆论) | `03_Match_Audits/DATE-{date}-top5/` |
| **football-prematch-odds-intelligence** | `src/skills/football-prematch-odds-intelligence/` | 赛前盘口与赔率异动分析 (欧赔/亚盘/大小球) | `03_Match_Audits/DATE-{date}-top5/` |
| **football-physical-profile** | `src/skills/football-physical-profile/` | 物理期望进球（xG/xGA）画像智能剪枝与无损 Soft Update | `02_Team_Archives/{league}/{team}.md` |
| **football-prematch-material-pack** | `src/skills/football-prematch-material-pack/` | 赛前空白资料包/材料包一键生成与 Obsidian 对齐交付 | `03_Match_Audits/AresMatchday_{date}/` |
| **recurring-team-signal-collection** | `src/skills/recurring-team-signal-collection/` | 定期球队异常信号采集（weekly_baseline / matchday_live / postmatch_validation），建立 durable learning 闭环 | `draft_reports/recurring-team-signal-collection_{league}_{date}.{md,json}` |
| **youtube-tactical-url-discovery** | `src/skills/youtube-tactical-url-discovery/` | YouTube 战术视频 URL 发现与筛选（v1.2 transcript-first routing），输出 ingestion queue 供 YT-02 消费，NotebookLM 降为 optional secondary synthesis | `AresVault/04_RAG_Raw_Data/youtube_tactical_sources/candidates/` + `ingestion_queue/` |
| **youtube-transcript-ingestion** | `src/skills/youtube-transcript-ingestion/` | YouTube 字幕/转录文本提取（transcript-first 主路径，YT-02），通过 yt-dlp 字幕专项提取原始 transcript，不做 claim extraction | `AresVault/04_RAG_Raw_Data/youtube_tactical_sources/transcripts/` |
| **transcript-to-tactical-claims** | `src/skills/transcript-to-tactical-claims/` | YouTube transcript 战术 claims 提取（YT-03），从 transcript_raw.md 中提取结构化战术 claims，不做 validation，不做 Team Archive patch | `AresVault/04_RAG_Raw_Data/youtube_tactical_sources/claims/` |
| **tactical-claim-validation** | `src/skills/tactical-claim-validation/` | 战术 claims 验证（YT-04），对 claims.md 进行事实核查和战术解读评估，输出 validated/rejected/needs_review 结果，不做 Team Archive patch | `AresVault/04_RAG_Raw_Data/youtube_tactical_sources/validation/` |
| **team-archive-patch-proposal** | `src/skills/team-archive-patch-proposal/` | Team Archive patch proposal 生成（YT-05），从 validation.md 筛选 validated+candidate_after_review claims，生成可审查的 patch proposal，不直接修改 Team Archive | `AresVault/04_RAG_Raw_Data/youtube_tactical_sources/patch_proposals/` |
| **youtube-tactical-batch-runner** | `src/skills/youtube-tactical-batch-runner/` | YouTube Tactical Intelligence 批量端到端 runner，读取 ingestion_queue.json，对合格条目执行 YT-02 → YT-06，输出 batch run report，支持幂等性检测 | `AresVault/04_RAG_Raw_Data/youtube_tactical_sources/batch_runs/` |
| **youtube-source-quality-dashboard** | `src/skills/youtube-source-quality-dashboard/` | YouTube 来源质量 dashboard，读取现有 pipeline artifacts，生成来源性能报告和推荐，只读不修改 | `AresVault/04_RAG_Raw_Data/youtube_tactical_sources/quality_reports/` |

---

## 🚀 全自动原生数据采集器 (Code-Native Data Pipelines)

为解决大模型现场搜索容易遭遇 **动态 JS 壳、Google Search 重定向（grounding-redirect）、反爬 403 拦截及实体幻觉** 的痛点，我们已将经过协议逆向与实战检验的完整采集链路，沉淀为 **原生 Python 采集执行器**，深度集成在 Skill 中：

### 1. 市场赔率全量采集器 (`collect_league_odds.py`)
- **脚本路径**：`src/skills/football-prematch-odds-intelligence/scripts/collect_league_odds.py`
- **核心能力**：
  - **赛程自动对齐**：传入 `--league epl --round N`，自动从球探联赛底层赛历（`s36.js`）解析本轮所有比赛的主客队及 `schedule_id`；
  - **已赛场次严格隔离**：自动识别 `state == -1`（已完赛），直接排除在 Prematch 采集之外，绝不使用赛后数据污染赛前；
  - **三大盘口全量并发**：
    - **1X2 欧赔**（`1x2d.titan007.com/{id}.js`）：平均每场抓取 **≥ 140 家独立博彩公司**初盘、即时盘、概率、返还率、凯利指数及**精确到秒的变盘时间戳**；
    - **亚让 AH**（`vip.titan007.com/AsianOdds_n.aspx?id={id}&l=0`）：提取各大机构主盘与多盘口（盘2、盘3）水位与让球数；
    - **大小球 OU**（`vip.titan007.com/OverDown_n.aspx?id={id}&l=0`）：提取进球数界限与大小球水位；
  - **证据链闭环**：27 份原始原件落盘至 `raw/market/titan007/` 并自动登记 SHA-256 清单，结构化数据输出至 `parsed/*_odds.json`。

### 2. 官方伤停采集与降级保护器 (`collect_official_team_news.py`)
- **脚本路径**：`src/skills/football-team-news-flags/scripts/collect_official_team_news.py`
- **核心能力**：
  - **Raw-Evidence-First 铁律**：按优先级扫描对阵双方俱乐部官网的赛前发布会速记与官方伤病专栏；
  - **命中静态正文**：落地 `.txt` 原始正文与 `_meta.json`，判定 `VERIFIED` 并提取带有原文引言的确证 Flag；
  - **遇阻断或空壳（JS Shell / 403 / 404）**：自动落盘标准 `_blocked.json`，记录阻断技术事实，**强制降级为 `PENDING_NOT_PUBLISHED`，正向 flags 坚决清空为空数组 `[]`**，确保绝对零幻觉。

### 3. 历史交锋与核心机构初终盘采集器 (`collect_h2h_market_history.py`)
- **脚本路径**：`src/skills/football-prematch-odds-intelligence/scripts/collect_h2h_market_history.py`
- **核心能力**：
  - **交锋全景深度挖掘**：自动从分析页（`analysis/{id}cn.htm`）解析 `v_data`，提取 15+ 场历史交锋详情（日期、赛事、赛前联赛排名、全场/半场比分、角球比、让球盘输赢、大小球输赢）；
  - **历史赔率数据库穿透**：根据交锋历史赛事 ScheduleID，自动化拉取 `1x2d.titan007.com/{mid}.js`；
  - **指定机构走势提炼**：输出威廉希尔（William Hill）、澳门（Macauslot）等核心机构的**开盘（初盘）和终盘（即时盘）数据与升降走势**，自动对比历史爆冷或正路博弈信号，输出结构化 JSON 并落盘原始原件。

---

## ⚡️ 在 Antigravity 对话框中一键唤醒与使用

如果您在 Antigravity 的 `@` 下拉菜单中暂时看不到技能（可能是由于插件更新、索引重置导致），您可以通过**“文件即技能（File-as-a-Skill）”**机制，在对话中直接发送以下 **黄金提问模板**。

AI 助手读取到该路径后，会隐式使用 `view_file` 工具的 `IsSkillFile: true` 标记加载文件，从而被 SKILL 的规则完全接管，在后台自动执行全流程。

### 1. 唤醒 `football-team-news-flags`（赛前新闻异常情报收集）

> 💡 **复制以下指令至 Antigravity 聊天框：**

```text
请加载并以 IsSkillFile 方式执行以下技能，开始我的足球赛前关键异常扫描：
技能路径: /Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/src/skills/football-team-news-flags/SKILL.md

输入参数：
- league: 英超
- round_or_date: 2026-05-20 (可以使用 YYYY-MM-DD 日期或轮次，如 第38轮)
- matches: [阿森纳 vs 切尔西, 曼城 vs 利物浦]  # 可选。若不指定，AI 会自动搜索本轮的完整五大联赛对阵。
```

### 2. 唤醒 `football-prematch-odds-intelligence`（赔率市场情报分析）

> 💡 **复制以下指令至 Antigravity 聊天框：**

```text
请加载并以 IsSkillFile 方式执行以下技能，分析指定比赛的盘口与赔率异动：
技能路径: /Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/src/skills/football-prematch-odds-intelligence/SKILL.md

输入参数：
- league: 西甲
- round_or_date: 第37轮
- matches: [巴塞罗那 vs 皇家马德里]
```

### 3. 极简一键调用（直接使用原生采集流水线，推荐 🌟）

> 💡 **无需复杂参数，直接在对话框中告诉 AI 轮次即可：**

```text
获取英超第 2 轮比赛数据
```
*(系统将自动触发 `collect_league_odds.py` 与 `collect_official_team_news.py`，全自动完成：解析 ScheduleID -> 剔除已赛场次 -> 抓取 140+ 机构欧亚大小球原件 -> 扫描官方伤停原件/记录阻断 -> 结构化打包交付)*

### 4. 唤醒 `football-physical-profile`（期望数据画像智能提炼）

> 💡 **复制以下指令至 Antigravity 聊天框：**

```text
请加载并以 IsSkillFile 方式执行以下技能，为指定球队进行物理画像数据提炼与智能剪枝：
技能路径: /Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/src/skills/football-physical-profile/SKILL.md

输入参数：
- team_name: 热刺 (或者阿森纳、曼城等)
- league: 英超 (可选)
- year: 2025 (可选，默认为当前赛季)
```

### 4. 唤醒 `football-prematch-material-pack`（空白推演资料包一键生成）

> 💡 **复制以下指令至 Antigravity 聊天框：**

```text
请加载并以 IsSkillFile 方式执行以下技能，为指定比赛日一键打包生成 Obsidian 交付材料：
技能路径: /Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/src/skills/football-prematch-material-pack/SKILL.md

输入参数：
- date: 20260520 (比赛日日期，格式 YYYYMMDD)
```

### 5. 唤醒 `recurring-team-signal-collection`（定期球队异常信号采集）

> 💡 **复制以下指令至 Antigravity 聊天框：**

```text
请加载并以 IsSkillFile 方式执行以下技能，开始定期球队异常信号采集：
技能路径: /Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/src/skills/recurring-team-signal-collection/SKILL.md

输入参数：
- league: 英超
- scan_type: weekly_baseline  # weekly_baseline / matchday_live / postmatch_validation
- reference_date: 2026-05-22
- teams: []  # 留空则全量扫描
```

### 6. 唤醒 `youtube-tactical-url-discovery`（YouTube 战术视频 URL 发现）

> 💡 **复制以下指令至 Antigravity 聊天框：**

```text
请加载并以 IsSkillFile 方式执行以下技能，发现并筛选 YouTube 战术视频候选来源：
技能路径: /Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/src/skills/youtube-tactical-url-discovery/SKILL.md

输入参数：
- target_team: Arsenal
- target_league: EPL
- coach_name: Mikel Arteta
- search_focus: tactical_analysis  # tactical_analysis / coach_system / postmatch_review / set_piece / pressing
- reference_date: 2026-05-22
```

---

## 🛠️ Python 编程式调用与集成

除了直接在 Antigravity 中让 AI 跑之外，该设计同样保留了强大的**本地 Python 管道集成能力**。在 `src/skills/skill_runner.py` 中，我们提供了 `SkillRunner` 类，供您的自动化脚本加载和装配 Skill 上下文。

### 本地 Python 调用示例：
```python
from pathlib import Path
from src.skills.skill_runner import SkillRunner

# 1. 实例化执行器
runner = SkillRunner("football-team-news-flags")

# 2. 组装输入上下文，自动映射 Ares 规范路径
context = runner.build_context(
    league="英超",
    season="2025/26",
    round="第38轮",
    date="2026-05-20",
    matches=["阿森纳 vs 切尔西", "曼城 vs 利物浦"]
)

# 3. 获取装配好的 System 与 User Prompt
system_prompt = runner.get_system_prompt()
user_prompt = runner.render_user_prompt(context)

# 4. 打印中间输出产物路径（符合 OSINT 规范路由）
print(f"原始中间数据路径: {runner.raw_output_path('英超', '2026-05-20')}")
print(f"最终 Obsidian 报告路径: {runner.draft_output_path('英超', '2026-05-20')}")
```

---

## 📐 底层核心原则

1. **绝对零幻觉（Zero-Hallucination Lock）**：在注入本技能后，AI 将被施加**强力锁机制**。任何未经 `search_web` 或 `read_url_content` 搜索到的“小道消息”、“猜测”均被禁止入选，以确保数据的绝对真实性。
2. **证据分层系统（Evidence Source Tiers）**：
   - **T1 - 官方发布会与官网**：主帅亲口言论、伤缺名单。
   - **T2 - 可靠跟队与主流媒体**：跟队记者爆料、伤停确证。
   - **T3 - 伤停/数据站**：数据交叉核准。
   - *（T4/T5 社交媒体、论坛等线索严禁单独入旗）*。
3. **输出闭环**：生成的 Markdown 报告和 JSON 数据会自动写入 `$ARES_VAULT_PATH/03_Match_Audits/DATE-{date}-top5/` 目录，供 `Ares` 量化系统直接读取。
