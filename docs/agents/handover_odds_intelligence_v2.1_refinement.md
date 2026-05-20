# Ares OSINT Telemetry - 赔率分析技能 V2.1 最严量化与打标重构交接文档

## 1. 交付概览

* **任务目标**：将赛前赔率市场情报分析技能（`football-prematch-odds-intelligence`）深度重构并升级至 **Ares V2.1 最严标准规范**，注入 P0 级防错校验、高精度算术均值、自动化市场变化与博弈标签（`market_move`/`market_tags`/`risk_tags`）系统，并且 100% 剥离投注强主观断言。
* **交付日期**：2026-05-20
* **交付人**：Antigravity
* **交付物理状态**：已在本地完成核心 Python 量化引擎重构、自测脚本运行编译、V2.1 报告规范和模板重构，且以英超热刺 vs 埃弗顿第38轮赛事完成实战复跑与持久化双重落盘。

---

## 2. 核心架构升级与变更清单

我们首创了 **“大模型抓取原始数据 + Python 引擎精密量化研判 (Quant Engine)”** 的混合管道模式，完全隔离了大模型在复杂数学计算和根据硬性阈值进行数据打标时的幻觉与差错：

| 修改类型 | 物理路径 | 核心修改点与技术决策 |
| :--- | :--- | :--- |
| **[MODIFY]** | `src/skills/football-prematch-odds-intelligence/scripts/normalize_odds_report.py` | **核心重构**。升级为 **Ares V2.1 赔率量化研判与清洗引擎 (Quant Engine)**。内置浮点化盘口解析器，实现高精度的 `odds_avg` 均值数学计算；编写硬编码博弈判断与分类规则，自动对齐打出 `market_tags` / `risk_tags`；自动生成 P0 级主客防错 `sanity_check`；计算 `data_confidence` 与 `company_coverage`。 |
| **[MODIFY]** | `src/skills/football-prematch-odds-intelligence/SKILL.md` | **升级运行规约**。升级 Frontmatter 至 2.1。明确大模型联网抓取后必须将原始 raw 数据流经 Python 引擎进行精密清洗与打标的核心混合闭环流程。要求绝对诚实记录数据采集时间戳，去除“实时获取”模糊字眼。 |
| **[MODIFY]** | `src/skills/football-prematch-odds-intelligence/templates/prematch_odds_report_template.md` | **升级数据与报告模板**。完全对齐顶级嵌套 JSON 架构。将研判时间逻辑 `market_time_logic` 拆分为 `initial_read`、`movement_read`、`split_check`、`ares_warning` 四段，并规范了纯客观 `market_conclusion` 数组占位。 |
| **[NEW]** | `scratch/test_ares_quant_engine.py` | **量化引擎自测脚本**。用于验证热刺 vs 埃弗顿赔率输入时，各项标签、均值和防错块输出的 100% 数学精确度。 |

---

## 3. 实战测试与落盘验证 (Spike Test)

### 3.1 测试用例：英超第38轮 热刺 vs 埃弗顿 (2026-05-24)
* **执行机制**：大模型使用 `search_web` 搜寻实时盘口数据，并将其以 V2.1 大模型提取 raw 的格式构造写入，通过 Shell 执行 Python Quant Engine 精密处理，最终由大模型进行客观博弈研判编写。
* **时效性元数据**：
  * `generated_at`: 2026-05-20 11:58:00
  * `source_fetch_time`: 2026-05-20 11:40:00
  * `data_source`: 捷报比分 & OddsPortal 联网汇总
* **双盘同向性与量化打标**：
  * `euro_signal`: `"HOME_STRENGTHENED"` (欧指均值主胜下调 -0.10)
  * `asian_signal`: `"HOME_WATER_SUPPORT"` (亚盘维持主让平半 -0.25，但主贴水经历暴降 -0.13)
  * `total_signal`: `"OVER_WATER_COMPRESSED"` (大小球 2.5 线下，大球水位大幅压缩 -0.18，压至 0.70 超低水位)
  * `market_tags`: `[EURO_ASIAN_ALIGNED, HOME_EURO_STRENGTHENED, HOME_HANDICAP_WATER_SUPPORT, LOW_TO_MEDIUM_HANDICAP_FAVORITE, OVER_WATER_COMPRESSED]`
  * `risk_tags`: `[]` (未触发欧亚分裂或水位超载风险)
* **客观博弈诊断与投注剥离**：
  完全过滤了任何诸如“博主场谢幕战独赢、防平价值低”等强主观投注诱导，改为客观呈现欧赔拉低与亚赔低水同向对齐的防御态势，警示末轮收官战意与轮换对物理因子的冲击，最终决策交由 Prematch 综合表决引擎。

### 3.2 双重持久化归档产物（Obsidian 物理落盘）
* **结构化 JSON 成功落盘**：  
  [`英超 2025_26 第38轮赛前赔率与市场时间逻辑报告_Ares.json`](file:///Users/liumingwei/vaults/AresVault/03_Match_Audits/DATE-20260520-top5/英超%202025_26%20%E7%AC%AC38%E8%BD%AE%E8%B5%9B%E5%89%8D%E8%B5%94%E7%8E%87%E4%B8%8E%E5%B8%82%E5%9C%BA%E6%97%B6%E9%97%B4%E9%80%BB%E8%BE%91%E6%8A%A5%E5%91%8A_Ares.json)
* **Markdown 研判报告成功落盘**：  
  [`英超 2025_26 第38轮赛前赔率与市场时间逻辑报告_Ares.md`](file:///Users/liumingwei/vaults/AresVault/03_Match_Audits/DATE-20260520-top5/英超%202025_26%20%E7%AC%AC38%E8%BD%AE%E8%B5%9B%E5%89%8D%E8%B5%94%E7%8E%87%E4%B8%8E%E5%B8%82%E5%9C%BA%E6%97%B6%E9%97%B4%E9%80%BB%E8%BE%91%E6%8A%A5%E5%91%8A_Ares.md)

---

## 4. 后续演进建议 (Next Steps)

1. **自动打标边界微调**：
   当前 Python Quant Engine 的打标规则完全硬编码契合 `euro_asian_handicap_theory.md`（如大水降幅 $\ge 0.10$ 打出大球压缩标签）。后续可以在实践中不断回顾，看是否存在某特定联赛的波动常态大于此阈值而产生假阳性，进而对特定联赛的水位浮动边界做配置化支持。
2. **多场批量筛选流水线 (Batch Screening Pipeline)**：
   由于生成的 `.json` 包含了无幻觉的 `market_tags`，这使得上层 Ares 决策脚本（如 batch screener）可以直接通过 `json.load()` 大量扫描所有比赛日的 JSON，筛选出满足 `HOME_EURO_STRENGTHENED` & `EURO_ASIAN_ALIGNED` 且无 `risk_tags` 预警的优质赛事，实现比赛日赔率初选自动化。
