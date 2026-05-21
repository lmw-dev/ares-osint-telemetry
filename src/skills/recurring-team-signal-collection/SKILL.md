---
name: recurring-team-signal-collection
version: "1.1"
source: ares-osint-telemetry native
description: >
  定期球队异常信号采集 Skill（v1.1 — weekly cadence hardened）。
  用于建立"定期采集 → 异常信号入库 → prematch 调用 → post-match 验证 → durable learning"的闭环。
  v1.1 新增：profile crosscheck hard gate、linked_signals 字段、
  postmatch_validation scan 输出格式、weekly cadence 触发规则。
inputs:
  - league: 联赛名称（英超/西甲/意甲/德甲/法甲，或 all）
  - scan_type: 扫描类型（weekly_baseline / matchday_live / postmatch_validation）
  - teams: 球队列表（可选，若未提供则按联赛全量扫描）
  - reference_date: 参考日期（YYYY-MM-DD，用于 matchday 计算）
outputs:
  - Signal Log（JSON）→ draft_reports/recurring-team-signal-collection_{league}_{date}.json
  - Signal Report（Markdown）→ draft_reports/recurring-team-signal-collection_{league}_{date}.md
changelog:
  - v1.1 (2026-05-21): profile crosscheck hard gate; linked_signals field; postmatch_validation schema; weekly cadence rules; source-bound statement cleanup
  - v1.0 (2026-05-21): initial release
---

# Recurring Team Signal Collection V1.1

## 执行模式说明

本 Skill 是一套**模型无关的 Agent 执行规范**。当你在 Antigravity 中被用户通过 `@skill` 或文件路径方式调用时，请按照本文件的步骤逐一执行。

**你就是信号采集与研判引擎。** 不需要调用任何外部 LLM API。你自身的推理能力就是判断能力，`search_web` 和 `read_url_content` 是你的情报采集工具，`write_to_file` 是你的落盘工具。

**与 `football-team-news-flags` 的区别**：
- `football-team-news-flags` = 赛前单次扫描，输出当轮异常标志，服务于即时 prematch 判断
- `recurring-team-signal-collection` = 持续性定期采集，输出带 lifetime/severity 的结构化信号，服务于跨周期的 durable learning 闭环

---

## 核心原则

1. **信号 ≠ 新闻**。新闻是原材料，信号是经过判断后的结构化结论。
2. **采集与判定分离**。先收集候选事实（附来源），再执行 signal taxonomy 分类和 severity 评级，最后输出成品。
3. **Source-bound**。所有信号必须有可追溯的 `source_ref`，并标注 `source_confidence`。
4. **脚本只负责 fetch / normalize / duplicate check，Skill 负责判断 / severity / lifetime / handoff / promotion**。
5. **lifetime 决定信号的生命周期**。每个信号必须明确标注 `expires_after`，避免过期信号污染 prematch 判断。

---

## Weekly Cadence 触发规则（v1.1 固化）

```
每周固定节奏：

  周一 / 周二（距下轮比赛 5-6 天）
    → weekly_baseline scan
    → 覆盖全联赛所有球队
    → 重点：coach_pressure / coach_change / injury_cluster / table_pressure / fixture_congestion / tactical_drift

  matchday -2（赛前两天）
    → matchday_live scan（第一次）
    → 覆盖本轮参赛球队
    → 重点：goalkeeper_change / rotation_pattern / motivation_shift / market_narrative_risk / injury_cluster（更新）

  matchday -1（赛前一天）
    → matchday_live scan（第二次，更新版）
    → 重点：官方首发确认 / official_confirmed 更新 / runtime_caveat 精修

  赛后 24-48 小时内
    → postmatch_validation scan
    → 覆盖本轮已赛球队
    → 重点：xg_anomaly / conversion_anomaly / defensive_leakage_anomaly / tactical_drift（验证）
    → 执行 promotion / rejection 判定
```

---

## 采集节奏（Collection Cadence）

### 1. Weekly Baseline Scan
- **触发时机**：每周一/二
- **扫描范围**：全联赛所有球队
- **重点信号类型**：`coach_pressure`、`coach_change`、`tactical_drift`、`injury_cluster`、`table_pressure`、`fixture_congestion`
- **目标**：建立本周信号基线，识别慢变量趋势，更新 durable_candidate 候选池

### 2. Matchday Live Scan
- **触发时机**：matchday -2 和 matchday -1 各一次
- **扫描范围**：本轮参赛球队
- **重点信号类型**：`goalkeeper_change`、`rotation_pattern`、`motivation_shift`、`market_narrative_risk`、`injury_cluster`（更新）
- **目标**：捕捉临场变化，更新信号 severity，生成 prematch handoff 清单

### 3. Post-Match Validation Scan
- **触发时机**：赛后 24-48 小时内
- **扫描范围**：本轮已赛球队
- **重点信号类型**：`xg_anomaly`、`conversion_anomaly`、`defensive_leakage_anomaly`、`tactical_drift`（验证）
- **目标**：验证赛前信号是否兑现，执行 promotion/rejection 判定，更新 durable learning 候选池


---

## Signal Taxonomy（信号分类法）

| Signal Type | 触发条件 | 典型来源 |
|---|---|---|
| `coach_pressure` | 主帅公开受到俱乐部/媒体/球迷压力，但尚未下课；连败后管理层表态模糊 | 主流媒体、发布会 |
| `coach_change` | 主帅正式离任、被解雇、临时主帅上任 | 俱乐部官方公告 |
| `tactical_drift` | 近 3-5 场阵型/打法出现系统性偏移，与历史基线显著不同 | xG 数据、赛后分析 |
| `injury_cluster` | 同一位置 2 名以上主力同时缺席，或核心球员伤停超过 2 周 | 官方伤停名单、发布会 |
| `goalkeeper_change` | 主力门将确认缺席或被替换，首发门将发生变化 | 官方首发、发布会 |
| `rotation_pattern` | 主帅明确表示大规模轮换，或近 2 场已出现 3+ 首发变动 | 发布会、官方首发 |
| `motivation_shift` | 球队已数学锁定目标（降级/保级/夺冠/欧战），战意结构性变化 | 积分榜、官方表态 |
| `table_pressure` | 球队处于降级区或积分榜关键位置，面临生死战压力 | 积分榜数据 |
| `market_narrative_risk` | 盘口/赔率出现异常移动，与球队实际状态明显背离 | 赔率数据、盘口分析 |
| `xg_anomaly` | 近 3 场 xG 与实际进球差异超过 1.5，存在明显转化率异常 | Understat、FBref |
| `conversion_anomaly` | 近 5 场 conversion_efficiency 偏离历史均值超过 30% | Understat 数据 |
| `defensive_leakage_anomaly` | 近 3 场 xGA 显著高于赛季均值，防线出现系统性漏洞 | Understat、FBref |
| `fixture_congestion` | 未来 10 天内有 3 场以上比赛，或连续 3 天一赛 | 赛程数据 |
| `media_fan_pressure` | 球迷抗议、媒体集中负面报道、更衣室冲突公开化 | 主流媒体、社媒（需 T1-T2 交叉验证） |

---

## Severity Taxonomy（严重程度分级）

| Severity | 定义 | prematch 处理 |
|---|---|---|
| `CRITICAL` | 直接影响本场比赛结果预判的核心变量 | 必须进入 prematch runtime_caveats，标红提示 |
| `HIGH` | 显著影响球队战斗力或战术执行的重要信号 | 必须进入 prematch runtime_caveats |
| `MEDIUM` | 需要关注但不直接决定结果的背景信号 | 进入 prematch context notes |
| `LOW` | 弱信号，单次出现不足以影响判断 | 只进入 background，除非连续出现 2+ 次 |

---

## Signal Lifetime（信号生命周期）

| Lifetime | 含义 | 典型场景 |
|---|---|---|
| `expires_after_next_match` | 下场比赛后自动失效 | 临场轮换、单场 rotation_pattern |
| `expires_after_matchday` | 本轮比赛日结束后失效 | matchday_live scan 产生的临时信号 |
| `monitor_2_matches` | 持续监控 2 场，观察是否升级或消退 | 初现的 tactical_drift、轻度 injury_cluster |
| `durable_candidate` | 候选长期记忆，需 post-match 验证后决定是否 promote | 连续 3+ 场出现的系统性信号 |
| `archived_noise` | 已确认为噪点，归档不再追踪 | 一次性事件、已被证伪的信号 |


---

## Signal Log Schema（信号记录结构 v1.1）

每条信号记录包含以下字段：

```json
{
  "signal_id": "唯一标识，格式：{team_slug}_{signal_type}_{YYYYMMDD}",
  "team": "球队名称（英文标准名）",
  "league": "联赛名称（EPL / La_liga / Serie_A / Bundesliga / Ligue_1）",
  "signal_type": "见 Signal Taxonomy",
  "severity": "CRITICAL | HIGH | MEDIUM | LOW",
  "source_type": "official | press_conference | reliable_media | journalist_report | aggregated_media | data_analysis | standings | market_data",
  "source_ref": "来源 URL 或描述",
  "source_confidence": "high | medium | low",
  "cross_source_verified": true,
  "official_confirmed": false,
  "observed_at": "YYYY-MM-DD",
  "summary": "一句话信号描述，基于实际搜索结果，禁止编造",
  "prematch_relevance": "high | medium | low | none",
  "affected_matches": ["比赛描述，如 Arsenal vs Chelsea 2026-05-25 16:00 BST"],
  "fixture_resolved": true,
  "runtime_caveat": "若 severity=CRITICAL/HIGH，此处填写 prematch 需要注意的具体内容；MEDIUM/LOW 填 null",
  "context_note": "若 severity=MEDIUM，此处填写 prematch context notes 内容；CRITICAL/HIGH 填 null",
  "handoff_level": "runtime_caveat | context_note | background_note",
  "linked_signals": {
    "linked_signal_ids": ["signal_id_1", "signal_id_2"],
    "linked_reason": "说明联动关系，如：Cremonese 降级与 Lecce 降级互为条件",
    "linked_scope": "team | match | league | table | fixture_cluster"
  },
  "durable_candidate": true,
  "expires_after": "见 Signal Lifetime",
  "postmatch_validation_required": true,
  "profile_crosschecked": false,
  "profile_conflict_detected": false,
  "profile_crosscheck_pending": true,
  "profile_crosscheck_pending_reason": "Team Archive not available / placeholder / crosscheck not executed",
  "blocking_status": "clear | conditional | blocked",
  "scan_type": "weekly_baseline | matchday_live | postmatch_validation",
  "created_at": "YYYY-MM-DDTHH:mm:ssZ",
  "updated_at": "YYYY-MM-DDTHH:mm:ssZ",
  "status": "active | validated | rejected | archived"
}
```

**v1.1 新增字段说明**：

- `linked_signals`: 多队/多场联动关系（如降级区多队互相依赖的结果）
  - `linked_signal_ids`: 关联信号的 signal_id 列表
  - `linked_reason`: 联动原因描述
  - `linked_scope`: `team`（同队多信号）/ `match`（同场两队）/ `league`（联赛级联动）/ `table`（积分榜联动）/ `fixture_cluster`（多场同时开踢）
- `profile_crosscheck_pending_reason`: 当 `profile_crosscheck_pending=true` 时必须填写原因
- `blocking_status`: `clear`（可直接进入 prematch handoff）/ `conditional`（有 pending，需注明）/ `blocked`（不得进入 handoff）
- `source_type` 说明：`press_conference` 仅用于俱乐部官方发布会；`journalist_report` 用于记者个人报道；`aggregated_media` 用于聚合媒体二次报道


---

## Profile Crosscheck Hard Gate（v1.1 强制规则）

**这是 v1.1 的核心硬化规则。所有 HIGH / CRITICAL 信号在进入 prematch handoff 前必须通过此 gate。**

```
Gate 检查逻辑：

对每条 severity=HIGH 或 CRITICAL 的信号：

  CASE 1: Team Archive 存在且为 usable
    → 读取 02_Team_Archives/{league}/{team}.md
    → 检查信号内容是否与 profile 历史基线冲突
    → 完成后标记：
        profile_crosschecked: true
        profile_conflict_detected: true/false
        blocking_status: clear

  CASE 2: Team Archive 不存在或为 placeholder
    → 标记：
        profile_crosschecked: false
        profile_crosscheck_pending: true
        profile_crosscheck_pending_reason: "Team Archive not available: {team}"
        blocking_status: conditional
    → 信号仍可输出，但 prematch 引擎必须知晓此状态
    → runtime_caveat 中必须注明 "profile_crosscheck_pending"

  CASE 3: 未执行 crosscheck（如首次 run）
    → 标记：
        profile_crosschecked: false
        profile_crosscheck_pending: true
        profile_crosscheck_pending_reason: "First run — crosscheck not executed"
        blocking_status: conditional

  BLOCKED 条件（不得进入 prematch handoff）：
    → profile_crosschecked=false
    → profile_crosscheck_pending=false（即：既没做 crosscheck，也没标 pending）
    → 这种情况表示信号质量控制失败，必须修正后才能进入 handoff
```

**规则摘要**：
- 正式 weekly run：HIGH/CRITICAL 信号 `profile_crosschecked=true` 或 `profile_crosscheck_pending=true + reason`
- 两者都没有 → `blocking_status=blocked`，不得进入 prematch handoff
- MEDIUM/LOW 信号：crosscheck 可选，不强制

---

## Prematch Handoff Rules（v1.1）

执行 matchday_live scan 后，按以下规则生成 prematch handoff 清单：

```
CRITICAL / HIGH severity 信号：
  → handoff_level: runtime_caveat
  → runtime_caveat 字段必须填写具体影响描述
  → context_note 字段填 null
  → 在 prematch 报告中标注 ⚠️ 警告
  → official_confirmed=false 时，caveat 中必须注明"需 matchday_live scan 确认"
  → blocking_status=blocked 时，不得进入 handoff，必须先修正

MEDIUM severity 信号：
  → handoff_level: context_note
  → context_note 字段必须填写背景信息描述
  → runtime_caveat 字段填 null

LOW severity 信号：
  → handoff_level: background_note（可选）
  → 连续出现 2+ 次的 LOW 信号自动升级为 MEDIUM 处理

fixture_resolved=false 的信号：
  → 不得进入 prematch_handoff_preview
  → 必须先解析 affected_matches 为具体比赛

linked_signals 处理：
  → linked_scope=fixture_cluster 的信号，在 prematch handoff 中必须同时列出所有关联信号
  → 例如：Cremonese table_pressure 与 Lecce table_pressure 联动，两者必须同时出现在 handoff 中
```

---

## Post-Match Validation Schema（v1.1 新增）

执行 postmatch_validation scan 时，对每个 `postmatch_validation_required=true` 的信号输出以下结构：

```json
{
  "signal_id": "原信号 ID",
  "team": "球队名称",
  "signal_type": "原信号类型",
  "original_severity": "原 severity",
  "expected_effect": "赛前预期该信号会产生的影响，如：主力门将缺席导致失球增加",
  "match_observation": "赛后实际观察到的情况，基于比赛数据/报道，禁止编造",
  "match_result": "比赛结果，如 Cremonese 1-2 Como",
  "validation_result": "validated | rejected | inconclusive | data_error",
  "validation_notes": "验证说明，解释为何得出此结论",
  "promote_to_memory_candidate": true,
  "archive_as_noise": false,
  "followup_required": false,
  "followup_reason": "若 followup_required=true，说明原因",
  "promotion_type": "durable_learning | one_off_noise | data_error | needs_more_samples",
  "validated_at": "YYYY-MM-DDTHH:mm:ssZ"
}
```

**validation_result 说明**：
- `validated`: 信号预期效果在赛后被数据/结果证实
- `rejected`: 信号预期效果未出现，或与实际结果相反
- `inconclusive`: 比赛数据不足以判断，需更多样本
- `data_error`: 原信号来源数据有误

**promotion_type 说明**：
- `durable_learning`: 信号具有跨场次复现性，推送至 Team Archive 长期记忆
- `one_off_noise`: 单场出现，无法复现，标记 archived_noise
- `data_error`: 来源数据有误，立即 archived_noise
- `needs_more_samples`: 样本不足，保持 monitor_2_matches，下轮继续追踪


---

## Post-Match Promotion / Rejection Rules

执行 postmatch_validation scan 后，对每个 `postmatch_validation_required=true` 的信号执行以下判定：

| 判定结果 | 条件 | 后续动作 |
|---|---|---|
| `durable_learning` | 信号在赛后被数据/结果验证，且具有跨场次复现性 | 标记为 durable_candidate，推送至 Team Archive 长期记忆 |
| `one_off_noise` | 信号仅在单场出现，赛后无法复现或验证 | 标记为 archived_noise，不进入长期记忆 |
| `data_error` | 信号来源数据有误，或采集时存在错误 | 立即标记为 archived_noise，记录错误原因 |
| `needs_more_samples` | 信号有一定支撑但样本不足，需继续观察 | 保持 monitor_2_matches 状态，下轮继续追踪 |

---

## 执行流程（4 Phases）

### Phase 1: 范围确认与参数解析

1. 从用户输入中提取 `league`、`scan_type`、`teams`、`reference_date`
2. 若 `scan_type=weekly_baseline`，扫描范围为全联赛所有球队
3. 若 `scan_type=matchday_live`，扫描范围为本轮参赛球队（需先确认赛程）
4. 若 `scan_type=postmatch_validation`，扫描范围为本轮已赛球队
5. 确认输出路径：`draft_reports/recurring-team-signal-collection_{league}_{date}.{md|json}`

### Phase 2: 分层情报采集

对每支球队，按 scan_type 对应的重点信号类型执行搜索：

**Step A: 基础搜索**
```
search_web("{team} team news injury manager {reference_date}")
```

**Step B: 针对性搜索**（按 scan_type 调整关键词）
```
# weekly_baseline
search_web("{team} tactical analysis form last 5 matches {date}")
search_web("{league} standings table pressure relegation {date}")

# matchday_live
search_web("{team} predicted lineup rotation {opponent} {date}")
search_web("{team} press conference {opponent} {date}")

# postmatch_validation
search_web("{team} match report xG stats {date}")
search_web("{team} vs {opponent} analysis tactical {date}")
```

**Step C: 数据源补充**（按需）
- xG/xGA 数据：Understat、FBref
- 赔率异动：500.com、OddsPortal
- 积分榜：官方联赛网站、ESPN

### Phase 3: 信号研判与结构化

对每支球队的候选事实执行：

1. **Signal Type 匹配**：对照 Signal Taxonomy 判断是否触发
2. **Severity 评级**：按 Severity Taxonomy 评级
3. **Profile Crosscheck**：按 Profile Crosscheck Hard Gate 规则执行
4. **Lifetime 标注**：按信号性质标注 expires_after
5. **linked_signals 标注**：识别多队/多场联动关系，填写 linked_signals 字段
6. **prematch_relevance 评估**：判断对即将到来的比赛的相关性
7. **durable_candidate 标记**：判断是否有长期记忆价值
8. **空值处理**：若无可验证信号，不输出该球队的信号记录（不强制填充）

### Phase 4: 输出渲染

生成 Signal Report（Markdown）和 Signal Log（JSON），写入 `draft_reports/`。

---

## 验证清单（v1.1）

交付前确认：
- [ ] 每条信号记录包含所有必填字段（含 v1.1 新增字段）
- [ ] `signal_type` 只使用 14 个规范类型
- [ ] `severity` 只使用 CRITICAL / HIGH / MEDIUM / LOW
- [ ] `expires_after` 已标注
- [ ] CRITICAL / HIGH 信号的 `runtime_caveat` 已填写
- [ ] CRITICAL / HIGH 信号已通过 Profile Crosscheck Hard Gate（`profile_crosschecked=true` 或 `profile_crosscheck_pending=true + reason`）
- [ ] `blocking_status` 已正确标注（blocked 的信号不得进入 prematch handoff）
- [ ] 多队/多场联动信号已填写 `linked_signals`
- [ ] `fixture_resolved=false` 的信号未进入 prematch_handoff_preview
- [ ] `postmatch_validation_required` 已正确标注
- [ ] 最终成品包含 Markdown 报告和 JSON 数据
- [ ] 所有来源 URL 真实存在（来自搜索结果）
- [ ] 报告末尾使用 source-bound statement，不使用"零幻觉承诺"

---

## Source-Bound Statement（标准结尾）

所有报告末尾统一使用以下声明（替代旧版"零幻觉承诺"）：

```
*Source-bound report: All listed signals include source_ref, source_confidence, and cross_source_verified.
Signals marked official_confirmed=false require matchday revalidation before entering prematch runtime_caveats.
Profile crosscheck status is explicitly marked per signal — HIGH/CRITICAL signals must be profile_crosschecked=true
or profile_crosscheck_pending=true with reason in future weekly runs.*
```

---

## 打包资源

- `templates/team_signal_log_template.md` — 单队信号记录 Markdown 模板（v1.1 更新）
- `templates/weekly_signal_collection_template.md` — 周度采集报告模板（v1.1 更新）
- `templates/postmatch_signal_validation_template.md` — 赛后验证报告模板（v1.1 新增）
