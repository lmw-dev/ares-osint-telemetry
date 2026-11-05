# Ares OSINT Telemetry — W45 赛前 Linear 阶段进度同步与工作流优化评估交接文档
## ARES W45 Pre-flight Linear Progress Sync & End-to-End Workflow Optimization Audit

> **文档路径**: `docs/agents/ARES_W45_PREFLIGHT_LINEAR_PROGRESS_AND_WORKFLOW_AUDIT_HANDOFF.md`  
> **归档日期**: 2026-11-05 (W45 赛前准备就绪)  
> **归档角色**: Ares OSINT Telemetry System (技术专家/架构师视角)  
> **终审中枢**: ChatGPT (独立足球决策与架构评审)  
> **核心目标**: 全面同步 Linear 阶段任务真实进度，固化五大联赛 96 队画像底座与森林试点成果；暂缓 LMW-374/375 通用建模，收紧 LMW-413 工程边界；深度评估 Prematch 导入与 Postmatch 赛后回流工作流闭环。

---

## 目录索引
1. [Linear 核心任务与阶段进度全景对照表](#1-linear-核心任务与阶段进度全景对照表)
2. [球队画像与战术底座落地成果 (摆脱旧试点认知)](#2-球队画像与战术底座落地成果)
3. [端到端工作流闭环深度评估 (Prematch 导入 vs Postmatch 回流)](#3-端到端工作流闭环深度评估)
4. [工作流优化建议与演进规范 (Profile Evolution Protocol)](#4-工作流优化建议与演进规范)
5. [周五 W45 实战执行与交付指引](#5-周五-w45-实战执行与交付指引)

---

## 1. Linear 核心任务与阶段进度全景对照表

为确保后续新会话或外部追踪时准确掌握真实系统状态，杜绝停留在历史旧状态（如“两队试点”），特同步最新阶段进展：

| 任务 / 工单标识 | 任务名称与范围 | 当前确证阶段进展 (Current State) | Linear 推荐状态 | 核心决策与边界纪律 |
| :--- | :--- | :--- | :---: | :--- |
| **球队画像工程**<br>(Base Profiling) | **五大联赛 96 队战术画像建档与赛前提取** | • **五大联赛 96 队画像已全量写入生产档案**（AresVault 独立画像区块）；<br>• `scripts/run_prematch_round.py` 已完成五大联赛多源别名映射与歧义拦截验证；<br>• **诺丁汉森林完成“单队战术问题持续积累”示范试点**并成功落档。 | **Done (阶段完结)**<br>*(待周五真实跑包实测)* | 摆脱仅有“英超2队”的历史认知；真实跑包时若有未匹配队名，依规则显式报缺失，严禁猜测。 |
| **[LMW-381](https://linear.app/lmw-lab/issue/LMW-381)** | **Shot Volume & Chance Quality Gate**<br>(射门总量、机会质量与比分状态门禁) | • 足球机理分析已完成，已完整写入赛前两个核心 Skill：<br>  - `football-prematch-odds-intelligence/SKILL.md`<br>  - `football-team-news-flags/SKILL.md`<br>• 确立机会质量、非点球 xG 与比分状态切片使用纪律。 | **In Progress**<br>*(保持进行中)* | 规则已固化至运行底座，保持 In Progress，待 W45 真实出单实战中验证其决策过滤效果。 |
| **LMW-374 / 375** | **通用战术假说与盘口模型抽象** | • **架构决策：暂缓脱离具体球队的通用抽象建模**；<br>• 转向在真实比赛周期（W45 起）以具体球队为单位持续积累战术证据与反证（以森林模式推进）。 | **On Hold / Deferred**<br>*(暂缓通用抽象)* | 避免过早陷入脱离具体球队战术情境的机械化泛化；优先积累真实比赛事实。 |
| **[LMW-413](https://linear.app/lmw-lab/issue/LMW-413)** | **Repo 修复 Ares Prematch/Postmatch 基线规则冲突与回归缺陷** | • 身价接口已做局部修复与验收；<br>• 历史基线冲突与部分潜在回归缺陷仍未彻底清零，工程任务不可虚假关闭。 | **In Progress**<br>*(保持进行中)* | 严格保持 In Progress；赛前不做破坏性大改，通过轻量级门禁隔离风险，后续定点收敛。 |

---

## 2. 球队画像与战术底座落地成果 (摆脱旧试点认知)

截至本周期，系统已完成由“概念验证”向“全量生产底座”的跨越：

1. **五大联赛 96 队画像基座 100% 物理归档**：
   - 覆盖英超 (20 队)、西甲 (20 队)、意甲 (20 队)、德甲 (18 队)、法甲 (18 队)；
   - 全部独立写入 `/Users/liumingwei/vaults/AresVault/02_Team_Archives/1_Top_Five_Europe/` 对应 Markdown 档案的 `## 当前球队画像` 注释区间中；
   - 严格遵循：保留历史 Layer B 赛后观察表与身价侧车不变，未篡改任何历史事实。
2. **诺丁汉森林“纵深战术演进”试点落档**：
   - 读取并吸收了高质量试点稿，将森林画像从机械的“逐场统计记录”提升为“围绕一个核心战术问题（主动进攻时的机会创造能力）持续积累”；
   - 明确归纳了森林在落后攻坚时的推进困难、面对逼抢时的出球边界，并确立了反证与下一场对阵曼城的具体观察点；
   - 该试点成为后续 96 队逐步演进的标准示范模版。
3. **诺丁汉森林 vs 曼城赛前资料增量完成定点收口**：
   - 曼城周中欧冠客战莱比锡 (3-2) 真实首发、替补及出场时间（4 人打满 90'，格瓦迪奥尔等轮休）已客观沉淀；
   - 市场数据流（1X2、AH、OU）直接回读 `3003947_1x2.js` 等物理原件完成校准，纠正了转录数字，源站时间显式标注“时区未知”，彻底清除了无资金数据支持的“承接资金”等臆测；
   - 人员可用性严格分级，多明戈斯保持 `[UNKNOWN: 状态待核]`，其余球员杜绝机械定性为“官方确认首发可用”，等待周五官方正式名单。

---

## 3. 端到端工作流闭环深度评估 (Prematch 导入 vs Postmatch 回流)

针对用户关于“如果下个新的 W45 启动，Prematch 打包是否能把现在的东西导入进去？Postmatch 赛后是否能再导入进来？工作流是否需要优化补充？”的关切，系统架构评估如下：

### 3.1 Prematch 阶段导入能力：已打通且具备生产可用性
- **当前机制**：`scripts/run_prematch_round.py` 在执行 `--full-pipeline` 时，流水线第四步会自动触发 `attach_team_operational_profiles_for_round()`。
- **导入表现**：
  1. 它读取对阵双方的联赛属性与标准名称；
  2. 经过 `extract_team_operational_profile()` 别名表与歧义过滤，直接从生产档案物理提取双方的 `## 当前球队画像`；
  3. 将双方画像与本场早盘市场赔率、官网伤停、FotMob 阵容底座聚合生成 `prematch_operational_profiles.json` 与 Markdown 摘要；
  4. 最终打入当期联赛的 Prematch 交付包（Staging），直接移交给 ChatGPT。
- **评估结论**：**Prematch 导入完全打通，无需新增框架，周五可直接真实跑包验证。**

### 3.2 Postmatch 阶段赛后回流能力：存在“知识断层”，亟需轻量闭环
- **当前现状**：
  - 完赛后，Phase 3 (`run_postmatch_round.py`) 采集技术统计、Shotmap 与权威 xG；
  - Phase 4/5 提炼出 Deep Review 与 Team/Model Learning 候选包；
  - Stage 6 (`run_stage6_full_cycle_archive_sync.py`) 在周二集中归档时，将赛后客观事实追加进 Layer B 观察表，并原地替换滚动身价侧车。
- **关键断层 (The Missing Link)**：
  - **目前的工作流中，赛后产出的“战术认识演进”（Team Learning）没有标准化的通道自动或半自动回流到 `## 当前球队画像`**；
  - **潜在风险**：如果画像不更新，未来各周的 Prematch 将永远读取静态的历史初版；如果脚本盲目自动覆盖画像，又极易违反“禁止单场归纳推断覆盖稳定特征”的 P0 铁律。

---

## 4. 工作流优化建议与演进规范 (Profile Evolution Protocol)

为了打通赛后到赛前的知识飞轮，建议在保持极简、不引入复杂框架的前提下，确立以下工作流优化规范：

### 优化 1：确立“赛后画像演进协议” (Profile Evolution Protocol / PEP)
- **非全自动重写，而是增量 Proposal 审查**：
  - 周二集中处理日（Stage 6），在提炼 96 队赛后观察的同时，仅对当周“有重大战术特征变化或关键假说被验证/反驳”的球队生成轻量的 `Operational Profile Update Proposal`；
  - 格式严格参照森林试点：**[本场新验证事实] $\rightarrow$ [已有假说修正] $\rightarrow$ [最新反证] $\rightarrow$ [下轮观察点]**；
  - 经 ChatGPT 终审批准后，原子写入对应球队档案的画像区间。

### 优化 2：中国体彩实战排期的“分层交付解耦”
- 严格遵循 Ares 双层实战节奏，坚决杜绝逐场开球前半夜盯盘：
  1. **Phase 1 (周五 18:00 前)**：定稿“全周赛前底座”（48 场赔率市场流 + 双方历史球队画像 + 周中出场/官网伤停初盘）；
  2. **Phase 2 (比赛日 18:00~19:00)**：定稿“临场刷新包”（集中抓取当夜所有场次 18:00 终盘与确证首发 Delta），两层严格解耦，不在赛前早盘中混淆临场即时价。

### 优化 3：LMW-413 工程工单定点收敛路径
- 本周不为了追求工单关闭而在赛前重构底层代码；
- 身价接口已保障当前运行，其余基线冲突放在 W45 真实周期运行中收集真实日志，赛后集中定点修复。

---

## 5. 周五 W45 实战执行与交付指引

1. **环境与数据资产状态**：
   - 生产档案库 `02_Team_Archives` 保持干净与只读，96 队画像底座稳固；
   - 试点文档 `docs/agents/ARES_EPL_GW10_NOTTINGHAM_VS_MANCITY_LATEST_PREMATCH_DOSSIER.md` 已完成数据源校准。
2. **周五 18:00 前标准跑包动作**：
   - 由 agy 在终端正常执行 W45 赛前采集，调度入口：
     ```bash
     # 例如英超第10轮
     python3 scripts/run_prematch_round.py --league epl --round 10 --full-pipeline
     ```
   - 脚本将自动完成：Titan007 盘赔采集 $\rightarrow$ 官网伤停采集 $\rightarrow$ FotMob 阵容绑定 $\rightarrow$ 动态提取双方生产画像 $\rightarrow$ 组装交付包。
3. **会话流转与出单决策**：
   - 周五新开会话后，直接将 agy 生成的联赛 Prematch 交付包传递给 ChatGPT；
   - ChatGPT 结合周五最新伤停、确证首发倾向与即时市场异动，独立签发最终比赛出单决策（曼城客战森林已有初步研判：曼城胜优先，1–2 / 0–2，周五结合交付包做终审定稿）。
