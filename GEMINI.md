# Ares OSINT Telemetry - Workspace Rules (项目专属规则)

## 🚨 P0 核心铁律：Session 启动前置契约 (Pre-flight Mandatory Protocol)

在当前工作区 (`21-ares-osint-telemetry`) 中，无论何种模型承接任何足球情报会话，在执行首条命令或生成任何回复前，**必须首先读取并严格遵循**：
👉 `docs/sop/ARES_INTELLIGENCE_LIFECYCLE_SOP.md` 与 `docs/sop/ARES_INTELLIGENCE_END_TO_END_ARCHITECTURE_SPEC.md`

---

## 🎯 业务中枢：中国体彩实战排期与三步节奏 (China Lottery Real-World Cadence)

Ares 系统服务于**中国体育彩票 (竞彩/北单/传足)** 的实体投研与实战出单节奏，**严禁使用逐场开球前 60~90 分钟半夜盯盘的过度设计**。全体 Agent 必须严格遵循以下每周业务时钟：

| 时间节点 | 业务动作 | 覆盖范围与要求 |
| :--- | :--- | :--- |
| **① 周五 18:00 前** | **全周赛前底座定稿**<br>(Phase 1: Prematch Base & Deep) | 一次性完成本期五大联赛全部 **48 场比赛** 的赔率市场流、H2H、官网伤停原件及 ChatGPT Deep Prematch 初盘复核定稿。 |
| **② 分析日 18:00~19:00**<br>(周五/周六/周日/周一) | **当日场次临场刷新**<br>(Phase 2: Final Refresh & 出单定稿) | 针对**当夜开打的所有场次**，在 18:00~19:00 集中执行临场刷新，汇总最新盘口异动与官方最新名单，直接支撑当晚出单决策。 |
| **③ 周二集中处理日**<br>(周一全部比赛收官后) | **赛后事实、知识提炼与双核入库**<br>(Phase 3 + Phase 4/5 + Stage 6) | 周二全天流水线作业：<br>1. 全量采集 48 场赛后技术统计与权威 xG (Phase 3)；<br>2. 提炼 96 队 Team Learning 与 5 项 Model Learning，生成无环 DAG 封包 (Phase 4/5)；<br>3. 五大联赛集中对齐合并至 `02_Team_Archives` (Stage 6: Layer B 赛后观察 + 滚动首发身价侧车原子原地替换)。 |
| **★ 周中补赛/双赛档**<br>(周二/周三/周四) | **独立周中节奏 (主动唤醒)**<br>(全流程主动唤醒，无后台定时任务) | 若有周中补赛，保持相同实战节奏：<br>1. 比赛日 18:00 前完成 Phase 1 赛前底座；<br>2. 比赛日 18:00~19:00 完成 Phase 2 临场出单刷新；<br>3. 次日集中完成 Phase 3~5 赛后事实与复盘。 |

---

## ⚡ 极简一键唤醒指令解析器 (One-Liner Commands)

用户只需输入以下极简指令，Agent 必须自动识别参数、锁定分母并装载完整规范：

| 业务指令示例 | 自动触发动作 | 调度单点入口 |
| :--- | :--- | :--- |
| **`Ares W41`**<br>`启动 W41 比赛分析`<br>`Ares 本周` | 启动周作战大盘：自动解析五大联赛轮次，按自然分析日 (周五/周六/周日/周一) 输出 48 场对阵与出单指引 | `python3 scripts/ares_cycle_deck.py --cycle <Wxx>` |
| **`Ares 赛前 <联赛> <轮次>`**<br>如: `Ares 赛前 英超 6` | 抓取 140+ 赔率、H2H、官网伤停降级、FotMob 绑定，部署 AresVault 门禁 | `python3 scripts/run_prematch_round.py --league <league> --round <N> --full-pipeline` |
| **`Ares 临场刷新 <周五/周六/周日/周一>`**<br>或指定: `Ares 临场 英超 6` | 针对指定分析日或联赛，集中抓取 18:00 最新终盘与确证首发 Delta，生成当夜出单审计包 | `python3 scripts/run_final_refresh.py --league <league> --round <N>` |
| **`Ares 赛后采集 <联赛> <轮次>`** | 完赛状态 (FT) 断言，采集全场技术统计、Shotmap 射门流图、xG 权威三级判定 | `python3 scripts/run_postmatch_round.py --league <league> --round <N>` |
| **`Ares 赛后知识 <联赛> <轮次> Review: <路径>`** | 冻结 Deep Review，提取 18/20 队 Team + 5 项 Model 候选，产出 DAG 无环发布封包 | 自动执行事实矩阵与 DAG 元数据流水线，通过 T8/T9/T12 物理回读校验 |
| **`Ares 集中对齐`**<br>`Ares 集中知识维护` | 横向对齐五大联赛 96 队 Proposal，绑定有效基线，生成全量 Candidate/Diff 冻结封包 | `python3 scripts/prepare_five_league_team_archive_gate.py` |
| **`Ares 生产归档同步`**<br>`Ares 集中知识入库` | **全自动双核闭环**：验证获批封包、自动备份、原子替换写入 (Layer B + 96 队首发身价侧车)、写后独立物理回读与证据封包交付 | `python3 scripts/run_stage6_full_cycle_archive_sync.py --cycle <Wxx>` |


---

## 🛡️ 必须严格执行的 5 项核心防线

1. **法定分母硬锁定 (Strict Denominator Lock)**：
   - 英超: **10 场** | 西甲: **10 场** | 意甲: **10 场** | 德甲: **9 场** | 法甲: **9 场** (全周标准结构总分母 **48 场 / 96 队**)。
   - **结构分母与行政挂起定义**：48 场/96 队是完整轮次的法定结构分母，包含已正常完赛场次与延期、中止或行政挂起场次。严禁因延期或中止将其从法定分母中剔除，也严禁将行政中断 (ADMINISTRATIVE_HOLD) 伪装成正常赛后分析（标准基线示例：本次 94 场正常观察 + 2 场行政 HOLD）。
   - 凡分母缺少或多出，立即阻断并报 `DENOMINATOR_MISMATCH`，严禁猜测。

2. **实体血统零漂移 (Zero Identity Drift)**：
   - Phase 2 与 Phase 3 **严禁联网重新搜索赛程**；
   - 必须且仅能从 Phase 1 已经沉淀的 `00-fixture-binding.json` 读取 `fotmob_match_id`，杜绝实体漂移。
3. **元数据无环 DAG 铁律 (Acyclic Metadata Law - No Quine Paradox)**：
   - 杜绝在任何 Manifest 或 Receipt 内部登记自身 SHA-256；
   - 严格遵循：Layer 0 (业务 Payload 冻结) $\rightarrow$ Layer 1 (Manifest 仅登记 Layer 0) $\rightarrow$ Layer 2 (Receipt 登记 Layer 0+1) $\rightarrow$ Layer 3 (External Index 登记全集) $\rightarrow$ Layer 4 (最终物理 ZIP 回读)；
   - 所有登记哈希与文件大小必须从物理字节动态读取，严禁人工转录或大模型虚构。
4. **生产知识库隔离红线 (Air-Gapped Archive Ingestion)**：
   - 赛后复盘单轮阶段严格执行 `TEAM_ARCHIVE_WRITE = 0`，仅产出 `04_Team_Knowledge_Maintenance_Proposal`；
   - 必须在周二集中维护阶段（Stage 6）经全五大联赛横向对齐后统一合并至生产档案（正常既定范围合并属于 L2 受控内部更新；未经授权的批量篡改属于 L3 严禁操作）。
5. **首发身价双核对齐与多级实证回退 (Dual-Layer Valuation & Multi-Tier Fallback)**：
   - 生产 Markdown 档案身价侧车与底层 `team_recent_xi_value.current.json` 必须 100% 保持在同一当前周期，严禁版本分裂；
   - 强制执行多级实证回退体系：L0 TM 权威 ID $\rightarrow$ L1 本地消歧注册表 $\rightarrow$ L2 二级图文实证 (雷速/官方 11/11 真实阵容图文补偿) $\rightarrow$ L3 部分已知数学兜底（缺值不按零计，不拿总身价替代）；
   - 严禁任何球队机械置为 `UNKNOWN` 或全零空置，违者在 Stage 6 归档时立即阻断。

---

## ⚖️ 执行与轻量化授权规范 (v3.1)

执行与授权遵守《ARES_INTELLIGENCE_LIFECYCLE_SOP.md》中的《Ares 轻量化授权与执行原则 v1.0》：L1 默认执行，L2 在既定任务范围内执行，L3 需要用户明确授权。历史 Receipt 缺失不自动造成全局阻断；关键数据安全校验仍必须执行。ChatGPT 独立签发最终足球 DECISION。

---

## 📦 交付物与 AresVault 门禁规范
- 所有数据必须落地至 Staging 并通过 `CONTENT_MANIFEST.tsv` 与 SHA-256 校验；
- AresVault 必须初始化 `00-denominator.json` 与 `01-round-gate.md`；
- 每次完成较大任务前，必须在 `docs/agents/` 归档 Markdown 格式的交接文档。
