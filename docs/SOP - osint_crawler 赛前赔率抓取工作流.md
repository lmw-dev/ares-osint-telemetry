---
tags:
  - sop/quant/data-acquisition
  - workflow/market-analysis
  - automation
status: evergreen
version: 2.0
creation_date: 2026-04-18
related_area: "[[领域 - 量化系统与算法研发]]"
---

# SOP - osint_crawler 赛前赔率抓取工作流 v2.0

> [!tip] 核心目标
本 SOP 规范了 Ares v4.1 引擎的“市场感知”环节。通过**冷热分离架构**，在保留原始市场全量数据（冷数据）的同时，为 [[体系 - Ares v4.1 量化推演引擎]] 提炼最具物理意义的赔率 Alpha 因子（热数据），确保决策路径的纯净与可追溯。

## 1. 架构原则：冷热分离 (Lambda Architecture)
- **冷数据 (Cold)**: 抓取时保留原始 JSON 响应，存储于 `ares-osint-telemetry/raw_odds/`，用于未来机器学习模型训练。
- **热数据 (Hot)**: 仅提取核心 Alpha 因子进入 Obsidian YAML，减少 LLM 推理噪音。

## 2. 核心 Alpha 因子定义 (Feature Set)
脚本执行时需强制提取以下 5 个核心指标：
1. **Initial_Odds (初盘)**: 机构对双方实力的原始物理定价。
2. **Current_Odds (即时盘)**: 反映市场资金流向与临场基本面变动。
3. **Movement_Velocity (异动速率)**: 判定是“平稳受热”还是“暴力洗盘”。
4. **Spread_Barrier (让球门槛)**: 亚指的物理阻力位（如 0.75 升 1.00）。
5. **Market_Decoupling_Index (离散度)**: 统计威廉、立博、Bet365 等主流机构的观点一致性。

## 3. 标准作业流程

### 第一阶段：定时遥测 (Timing)
必须在以下三个物理时间节点触发抓取，以捕捉完整的博弈轨迹：
- **T-24h (基准期)**: 记录市场初盘，确立原始护城河。
- **T-5h (资金震荡期)**: 观察主流机构的调指方向。
- **T-1h (收货期)**: 最终封盘数据，判定是否存在“反直觉升降”。

### 第二阶段：执行指令
1. 运行：`python src/data/osint_crawler.py --issue 26062 --mode production`
2. **队名校验**: 若日志出现 `[AliasWarning]`，必须立即更新 `src/data/team_alias_map.json`。
3. **特征映射**: 脚本自动将 `纽卡` 映射为 `Newcastle United`，并将赔率格式化为浮点数。

### 第三阶段：落盘规范
热数据落盘至 `{ARES_VAULT_PATH}/04_Matchday_Odds/{issue}_odds.md`，格式要求：

```yaml
---
issue: "26062"
match_id: "3471925"
alpha_factors:
  pinnacle_init: [1.85, 3.40, 4.20]
  bet365_current: [2.05, 3.30, 3.80]
  handicap_move: "0.5 -> 0.25"
  velocity_flag: "aggressive_drop" # 暴力下调预警
---
````

## 4. 异常处理

- **反爬触发**: 脚本自动切换 `ProxyPool`。
    
- **数据真空**: 若主流机构（Pinnacle/Bet365）数据缺失，脚本须触发 `[Halt]`，禁止使用二流小公司数据填充。
    

---

## 知识连接

- **决策引擎**: [[体系 - Ares v4.1 量化推演引擎]] (定义了如何消耗这些 Alpha 因子)
    
- **赛后闭环**: [[项目 - Ares OSINT Telemetry Pipeline 开发]] (验证这些异动是否转化为真实的物理方差)
    
- **底层架构**: [[领域 - 量化系统与算法研发]]
    


---

