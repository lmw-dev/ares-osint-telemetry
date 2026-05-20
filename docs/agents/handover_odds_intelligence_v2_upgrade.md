# Ares OSINT Telemetry - 赔率分析技能 V2.0 架构升级交接文档

## 1. 交付概览

* **任务目标**：将赛前赔率市场情报分析技能（`football-prematch-odds-intelligence`）重构并升级至全新的 **V2.0 架构**（模型无关的 Agent Workflow），解除对外部 Python 爬虫和第三方 LLM API 的耦合，并由 Antigravity 活跃大模型自身直接扮演闭环分析引擎。
* **交付日期**：2026-05-20
* **交付人**：Antigravity
* **交付物理状态**：已在本地完成全量重构、实战测试，且数据已完美持久化落地到 Obsidian Vault 目录中。

---

## 2. 变更内容清单

| 修改类型 | 物理路径 | 核心修改点与技术决策 |
| :--- | :--- | :--- |
| **[MODIFY]** | `src/skills/football-prematch-odds-intelligence/SKILL.md` | 重构 frontmatter 至 2.0；设计 Phase 1-5 的 5 阶段 Agent Workflow；指导大模型通过 `search_web` 定位 500网、捷报网、OddsPortal 获取初/即时欧赔、亚盘及大小球。 |
| **[NEW]** | `src/skills/football-prematch-odds-intelligence/references/euro_asian_handicap_theory.md` | **新增赔率博弈研判参考指南**。梳理了欧亚对照折算比、水位区间分类（超低水至超高水），总结出“诱盘警戒”、“阻上保护”、“欧亚分裂”三大博弈操盘推演法则。 |
| **[MODIFY]** | `src/skills/README.md` | 将本赔率技能 V2.0 快捷唤醒提问模板、落盘约定正式写入技能说明书中。 |
| **[NEW]** | `.cursorrules` / `.geminirules` | **新增全局核心规则文件**。强制在项目会话中继承简体中文注释与回答、规范化 Python 代码质量与 logging 管道、锁定 Obsidian 的物理落盘目录（AresVault）与四大智能技能协同规则。 |

---

## 3. 实战测试与落盘验证 (Spike Test)

### 3.1 测试用例：英超第38轮 热刺 vs 埃弗顿 (2026-05-24)
* **执行机制**：大模型使用自身 `search_web` 搜寻实时盘口数据。
* **数据收集**：成功采集 Bet365、威廉、立博、平博（Pinnacle）、必发（Betfair）等八大博彩公司的即时指数（欧赔 1.85 / 4.00 / 3.75，亚盘折合 `-0.25` 低水，部分盘口临场回升 `-0.5` 超高水 2.36，2.5线大球 0.67 超低水）。
* **博弈诊断**：本场欧亚赔一致下调主胜，虽没有欧亚分裂，但临场贴水震荡巨大。分析得出庄家正通过让半球高水设置高门槛阻碍上盘谢幕热度资金，属于**“正路阻盘保护”**，建议正路博主赢，高进球数打出概率极高。

### 3.2 双重归档结果（Obsidian 物理落盘）
* **Markdown 报告落地**：  
  [`英超 2025_26 第38轮赛前赔率与市场时间逻辑报告_Ares.md`](file:///Users/liumingwei/vaults/AresVault/03_Match_Audits/DATE-20260520-top5/英超%202025_26%20第38轮赛前赔率与市场时间逻辑报告_Ares.md)
* **JSON 数据落地**（满足量化引擎无幻觉读取）：  
  [`英超 2025_26 第38轮赛前赔率与市场时间逻辑报告_Ares.json`](file:///Users/liumingwei/vaults/AresVault/03_Match_Audits/DATE-20260520-top5/英超%202025_26%20第38轮赛前赔率与市场时间逻辑报告_Ares.json)

---

## 4. 后续演进建议与待办 (Next Steps)

1. **赔率回演方差审计（Outcome Postmatch synthesis）**：  
   在赛后执行 `postmatch_synthesis.py` 时，建议程序加载赛前生成的赔率 JSON 记录，将“赛果物理方差（RAG Reality Gap）”与“赛前赔率博弈预期”进行交叉比对。这能帮助审计庄家在中段和临场的资金操盘是属于“真实诱盘”还是“刻意诱多”，从而生成高难度的足球博弈知识库沉淀。
2. **多场次批处理（Batch Execution Support）**：  
   目前本 Skill 在大模型端通过单场焦点对阵运行验证完美。如果用户需要对某一整轮的 10 场比赛进行全量欧亚赔检索，建议在大模型端分批（每批 3-4 场）或由 `osint_pipeline` 先跑出派发单（`dispatch_manifest.json`），大模型直接对照派发单上的 `cold_data_refs` 实施定点赔率提取，这能极大缩短 web 搜索耗时，避免大模型上下文过载。
