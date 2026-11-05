---
name: ares-postmatch-evidence-collector
version: "2.0"
source: ares-osint-telemetry native
description: >
  Ares 生产级确定性赛后事实与过程证据采集 Skill。
  实现赛后真实比分、确认首发 11 人、阵型、比赛事件（进球/点球/红牌/换人）、
  全场技术统计（射门/射正/控球/角球/绝佳机会）、多源 xG 过程数据（Understat 与 FotMob 射门流/时间线）
  以及单场确证首发身价 (06-lineup-valuation.json) 与三场滚动基线的确定性萃取与跨源一致性交叉验证。
  严格杜绝任何博彩倾向、Fair Line 或主观足球裁决。
inputs:
  - project_fixture_id: 项目内部赛事 ID
  - league: 联赛标识（如 EPL_GW05）
  - season: 赛季（如 2026/27）
  - round: 轮次（如 5）
  - home_team: 主队名
  - away_team: 客队名
  - kickoff_utc: 开球时间戳
  - mode: 运行模式（FULL_POSTMATCH 或 PROCESS_FILL_ONLY，默认 PROCESS_FILL_ONLY）
  - existing_binding: 已有绑定字典（可选，已绑定时直接复用）
  - understat_match_id: Understat 比赛唯一标识（可选）
  - output_dir: 产物 Staging 目录
outputs:
  - 00-postmatch-identity.json: 赛事实体与来源凭证
  - 01-result-evidence.json: 终场比分与半场比分事实
  - 02-lineup-events.json: 确认首发阵容、替补及核心比赛事件
  - 03-process-stats.json: 射门、射正、控球率、角球、绝佳机会等核心过程统计
  - 04-xg-process.json: 预期进球（xG）、射门流明细与累积时间线
  - 05-source-lineage.json: 来源血统与跨源交叉比对审计
  - 06-lineup-valuation.json: 确证首发 11 人身价、Transfermarkt 绑定、多因子核验明细与下界汇总
  - manifest.json: 单场证据就绪度与 Gap 清单
  - 00-ROUND-POSTMATCH-COLLECTOR-SUMMARY.json: 轮次级汇总与覆盖率审计
  - SHA256SUMS.txt: 完整性哈希校验清单
source_policy:
  status: CAUTION
  note: "Automated/systematic FotMob and Understat access requires production review."
---

# Ares Postmatch Evidence Collector Skill (v2.0)

## 0. 为什么存在此 Skill (Why This Skill Exists)

### 职责解耦与生产缺陷治理
在过往流水线中，Ares 在单次会话中混杂了外部源探索、FotMob 抓取、Understat 抓取、回退、持久化和初步分析，导致 10 场比赛耗时超 80 分钟、0/10 xG 覆盖率、虚假 `PACKAGE_GAP=[]`、缺乏过程证据等严重缺陷。

本 Skill 确立了清晰的职责边界契约：
- **Antigravity (本 Skill)**：专注确定性外部赛后证据采集（真实比分、官方确认 XI、比赛重大事件、全场技术统计、Understat/FotMob 原生 xG 与 Shot Map、单场首发身价确定性推导与滚动基线维护），输出标准化 Staging 数据包；绝不生成博彩最终决策、Fair Line、比分预测、EDGE 或 LEAN。
- **Ares (下游系统)**：负责读取本 Skill 产物，执行身份与完整度校验，规范合入 Canonical Postmatch，进行初步分析初判（Preliminary Analysis）。
- **ChatGPT (终审中枢)**：审计不可篡改的完整证据链，进行反向推演，做出最终 Postmatch 决策与球队档案沉淀。

---

## 1. 运行模式 (Modes)

1. **`FULL_POSTMATCH` (全量赛后采集)**：
   - 适用于新建赛后档案或无预存基础产物的场景；
   - 采集并生成完整的 00~06 全套数据与 Manifest。
2. **`PROCESS_FILL_ONLY` (过程证据填补模式)**：
   - 适用于 Ares 已生成基础赛果但过程数据（统计、xG、射门流）缺失的场景；
   - 严禁覆盖 Ares 现有的赛果主文件，专注于生成完整且高置信度的过程证据层包，供下游确定性合并。

---

## 2. 数据源优先级与有界回退 (Source Priority & 1-Bounded Fallback)

### 比分、首发、事件与技术统计
- **PRIMARY**: FotMob canonical completed-match 状态树（`__NEXT_DATA__`）。
- **SECONDARY**: 官方联赛/俱乐部页面（仅当 FotMob 缺失核心客观字段时触发一次）。

### xG 与射门过程数据
- **PRIMARY**: Understat 完赛页面（若已收录对应赛季）。
- **SECONDARY (1-Bounded Fallback)**: FotMob 原生 xG 与 Shot Map（分钟级每脚射门坐标、xG、方式、情景与结果）。
- 若两者均不可用，标为 `UNKNOWN`。绝无死循环尝试。

### 首发身价数据基准 (Valuation Baseline)
- **PRIMARY**: Transfermarkt 固定基准快照（当前基准：`2026-09-30`，SHA-256: `d22e407981d5b51a79bf8ff59835729f3526f7dc3495a6d8ed2f852ed2e86403`）。
- 杜绝联网随意取用赛后变动价格，确保全赛季横向可比。

---

## 3. 证据差距语义规范 (Evidence Gap Semantics)

严禁以“文件是否存在”作为完整度评判标准：
- **`artifact_gap`**：物理文件是否缺失（如 `01-result-evidence.json` 或 `06-lineup-valuation.json` 是否生成）；
- **`evidence_gap`**：客观证据要素是否完整（如“Understat xG 不可用”、“客队首发 11 人未完整公布”等）。
- 绝不在证据缺失时输出误导性的 `evidence_gap = []`。

---

## 4. 产物规范结构

```text
data/postmatch/<SEASON_OR_ROUND>/<FIXTURE_ID>/
├── 00-postmatch-identity.json      # 赛事实体与来源凭证
├── 01-result-evidence.json         # 终场比分与半场比分事实
├── 02-lineup-events.json           # 确认首发阵容、替补及核心比赛事件
├── 03-process-stats.json           # 射门、射正、控球率、角球、绝佳机会等核心过程统计
├── 04-xg-process.json              # 预期进球（xG）、射门流明细与累积时间线
├── 05-source-lineage.json          # 来源血统与跨源交叉比对审计
├── 06-lineup-valuation.json        # 确证首发身价、多因子核验明细与单场已知金额下界
└── manifest.json                   # 单场证据就绪度与 Gap 清单
```

---

## 5. Postmatch XI Valuation Gate (首发身价准入与持久化门禁)

每次完成比赛 Postmatch 采集后，必须通过以下 9 步标准核验流水线：

1. **确证首发断言**：比赛必须具备官方或权威数据源确认的 11 人首发阵容，若未取得标记 `PENDING_LINEUP_EVIDENCE`；
2. **快照基准锁定**：必须绑定指定 Transfermarkt 静态历史快照（`2026-09-30`），严禁使用未经冻结的实时变动数据；
3. **多因子身份核验**：每名首发球员必须通过姓名、国籍、年龄、位置语义及正价核验，降级球员归入 `IDENTITY_UNRESOLVED` 且金额赋 `null`；
4. **单场身价生成**：生成标准化 `06-lineup-valuation.json`，计算 `known_xi_value_eur = sum(已确认正价球员身价)`；
5. **滚动参考聚合**：以物理开球时间 (`kickoff_utc`) 严格排序，维护参赛球队最近 3 场窗口，分流 `MEAN_3` / `MEAN_2` / `SINGLE_1` / `UNKNOWN`；
6. **部分覆盖下界容错**：若队伍出现个别小将缺价（如 10/11），必须标记 `PARTIAL_LOWER_BOUND` 并完整保留下界，严禁将整场作废，严禁将缺值当 0 元；
7. **受控候选封包**：生成轮次级全量候选资产包 (`team_recent_xi_value.<ROUND>_POSTMATCH_CANDIDATE.json`) 与 `CANDIDATE_MANIFEST.tsv`；
8. **生产零写入守卫**：赛后采集阶段严格执行 `TEAM_ARCHIVE_WRITE = 0`，若身价资产未获批入库，标记 `VALUATION_PERSISTENCE_PENDING`；
9. **下游消费可读性**：确保下一轮 Prematch 的 `SquadValuePriorAdapter` 能够自适应读取已发布的权威版本或指定的候选资产。

### 执行入口与调用拓扑
- **CLI 入口 (标准命令格式，必须包含 `--phase handoff`)**：
  `python3 scripts/run_postmatch_round.py --league <league> --round <N> --phase handoff`
- **查看轮次绑定状态**：
  `python3 scripts/run_postmatch_round.py --league <league> --round <N> --phase status`
- **执行顺序与打包保障**：身价计算流水线必须在 Handoff 封包之前完成，单场 `06-lineup-valuation.json` 直接纳入全轮交付 ZIP。
- **底层引擎**：`src/engine/postmatch_valuation_integrator.py`
- **消费适配**：`src/engine/squad_value_prior_adapter.py`

