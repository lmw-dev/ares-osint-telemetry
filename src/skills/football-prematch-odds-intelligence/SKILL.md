---
name: football-prematch-odds-intelligence
version: "2.1"
source: ares-osint-telemetry native
description: >
  赛前公司级欧赔、亚盘、大小球赔率市场情报收集与时间逻辑深度研判。
  V2.1 升级版：模型无关的 Agent Workflow 架构。当前活跃大模型（Gemini/Claude等）扮演分析引擎直接执行全流程，
  使用 search_web / read_url_content 抓取实时赔率数据。
  全面对齐 Ares P0/P1/P2 规范，输出防错 sanity_check、均值 odds_avg、自动化 market_move/market_tags 标签组，
  并剥离一切强主观投注预测，仅输出纯正的博弈市场客观信号。
inputs:
  - league: 联赛名称（英超/西甲/意甲/德甲/法甲）
  - round_or_date: 轮次或比赛日期（必填）
  - matches: 比赛列表（可选，若未提供则自动联网搜索获取）
outputs:
  - Markdown 报告 → /vaults/AresVault/03_Match_Audits/DATE-{date}-top5/{联赛中文名} {season} 第{N}轮赛前赔率与市场时间逻辑报告_Ares.md
  - JSON 数据 → 同目录 .json 文件
---

# Football Prematch Odds Intelligence V2.1

## 执行模式说明

本 Skill 是一套 **基于大模型抓取 + Python 精密量化引擎混合运行** 的 Agent 执行规约。当您在 Antigravity 中被用户通过 `IsSkillFile` 引用或唤醒时，请严格按照以下 5 阶段 Workflow 进行执行。

**您是核心数据检索与博弈信号研判引擎。** 您的职责不是给出强主观的投注推荐（禁止使用“博主胜、防平防冷价值不高”等越界断言），而是提供**极致防错、完全结构化、可供 batch screening 自动筛选**的公司级赔率及客观博弈标签包。

---

## 核心原则

1. **绝对零幻觉（Zero-Hallucination Lock）**：绝对禁止捏造、猜测任何赔率、水位或最后更新时间。数据必须能够追溯至真实的网页抓取源。
2. **缺口诚实（Missing Honesty）**：若某家博彩公司或某个盘口数据缺失，在报告中诚实标注 `source_missing`，严禁“凭空填补”以维持表格完整。
3. **数据抓取、脚本量化与后置研判混合执行**：
   - 大模型负责抓取原始公司级数据（`odds_raw`）；
   - 大模型调用 [normalize_odds_report.py](file:///Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/src/skills/football-prematch-odds-intelligence/scripts/normalize_odds_report.py) 脚本对数据进行清洗、对齐与自动打标，生成完全结构化的核心字段（`sanity_check`, `odds_avg`, `market_move`, `market_tags`, `risk_tags`, `data_confidence`, `company_coverage`）；
   - 大模型基于脚本生成的 JSON，进行后置的 `market_time_logic` 拆分研判与纯客观 `market_conclusion` 提炼，完成 Markdown 报告。

---

## 5 阶段 Agent 运行流程 (5-Phase Workflow)

### Phase 1: 比赛锁定与范围校准
1. **解析输入参数**：明确联赛名称（如“英超”）、赛季（如“2025/2026”）、当前轮次/日期。
2. **提取对阵列表**：若未提供 `matches`，利用 `search_web` 搜索本轮的完整五大联赛对阵。

### Phase 2: 赔率多点联网检索 (Multi-Source Scrape)
针对每场比赛，构建特定的搜索词，利用 `search_web` 检索赔率汇总站并使用 `read_url_content` 提取数据。
- 检索主要以 500.com 赔率指数、捷报比分、OddsPortal 为主，拉取威廉、澳门、立博、365、易胜博、伟德、Pinnacle/平博、Betfair/交易所类 的初盘与即时数据。
- 提取数据时，必须注意：**不要混淆主客方向！**
- 记录采集绝对时间（禁止脑补、禁止写“实时获取”字样），填入 `source_fetch_time`。

### Phase 3: 调用 Python 引擎进行精密清洗与打标 (Quant Engine Execution)
大模型将提取到的原始数据写入临时 JSON 文件，并使用 shell 调用 `/src/skills/football-prematch-odds-intelligence/scripts/normalize_odds_report.py` 运行。
Python 量化引擎将为每场比赛自动生成并补充以下核心结构化字段：
- **`sanity_check`**：主客防错校验块，强制显示让球及大小球的正负号和格式。
- **`odds_avg`**：欧赔、亚盘水位与让步、大小球水位与线的精准算术平均。
- **`market_move`**：各玩法变动信号客观推导。
- **`market_tags` / `risk_tags`**：基于 `euro_asian_handicap_theory.md` 数学阈值规则的 100% 自动打标（包含 `EURO_ASIAN_SPLIT`, `HOME_HANDICAP_WATER_SUPPORT`, `DRAW_COMPRESSED` 等）。
- **`data_confidence` 与 `company_coverage`**：置信度与覆盖率评估。

### Phase 4: 细化研判时间逻辑 (Time-Logic Analysis)
大模型基于 Python 量化清洗后的结构化 JSON 字段，针对每场比赛进行深度博弈分析，且必须严格拆分为以下四个结构段落：
1. **`initial_read` (初盘多空博弈)**：剖析庄家开出初盘的深浅、受水倾向与博弈意图。
2. **`movement_read` (即时资金盘面移动)**：分析随着比赛临近，各玩法赔率/水位变动的资金和风险控制走向。
3. **`split_check` (双盘同向裂痕校验)**：核验欧赔与亚赔的指向是否完全一致；如出现欧亚大裂痕，指出背后的高危风控异常。
4. **`ares_warning` (Ares 风控级预警)**：输出本场最核心的博弈博热/爆冷预警（严禁给出强主观投注结论）。

### Phase 5: 客观市场结论与 Obsidian 落地
1. **禁止投注过早下结论**：
   - 绝对严禁写入“建议博主胜、防平防冷价值不高、主独赢稳”等投注指导。
   - 必须使用 `market_conclusion` 结构化列表，客观列出市场偏向事实，并将最终的战意与阵容决策留给 Prematch 决策引擎。
2. **生成精确的元数据时间戳**：
   - 必须在报告开头包含：
     - `generated_at`: 生成时间（当前 local 绝对时间，格式为 YYYY-MM-DD HH:mm:ss）
     - `source_fetch_time`: 数据采集时间
     - `data_source`: 提取的数据源网址
3. **双重持久化落地**：
   - 写入 Obsidian 归档目录：`/vaults/AresVault/03_Match_Audits/DATE-{date}-top5/` 下的 `.md` 与 `.json` 文件。
   - JSON 架构必须严格符合推荐的顶级嵌套格式。

---

## 验证清单 (Scrape Quality Checks)

- [ ] `sanity_check` 在报告首要位置输出，数据方向 100% 对齐。
- [ ] 欧赔均值、亚盘让球贴水均值、大小球贴水均值已通过 Python 引擎精密计算无偏差。
- [ ] `market_tags` 与 `risk_tags` 依据数学理论完美判定，无遗漏和冲突。
- [ ] 结论部分 100% 剥离了主观投注推荐，表述客观克制。
- [ ] 元数据时间戳中不再包含“实时获取”字样，采集与生成时间精确可溯。
