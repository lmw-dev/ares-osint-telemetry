# 交接文档：recurring-team-signal-collection Skill 硬化记录（v1.1）

**日期**: 2026-05-21
**关联 Issue**: LMW-93
**状态**: 已完成，可进入正式 weekly cadence

---

## 一、硬化内容汇总

### 1. SKILL.md 升级至 v1.1

| 变更项 | v1.0 | v1.1 |
|---|---|---|
| Profile crosscheck | 建议执行，可 pending | **Hard gate**：HIGH/CRITICAL 必须 `profile_crosschecked=true` 或 `profile_crosscheck_pending=true + reason`，否则 `blocking_status=blocked` |
| linked_signals 字段 | 不存在 | **新增**：支持 `linked_signal_ids / linked_reason / linked_scope` |
| postmatch_validation schema | 仅有 promotion/rejection 规则 | **新增完整 schema**：`expected_effect / match_observation / validation_result / promote_to_memory_candidate / archive_as_noise / followup_required / promotion_type` |
| Weekly cadence 触发规则 | 描述性文字 | **固化为代码块**：周一/二 baseline → matchday -2/-1 live → 赛后 24-48h validation |
| blocking_status 字段 | 不存在 | **新增**：`clear / conditional / blocked` |
| profile_crosscheck_pending_reason | 不存在 | **新增**：必须填写原因 |
| 报告末尾声明 | "零幻觉承诺" | **统一为 source-bound statement** |
| 版本号 | v1.0 | **v1.1** |

### 2. 模板更新

| 模板 | 状态 | 路径 |
|---|---|---|
| `team_signal_log_template.md` | ✅ 更新（v1.1 新增字段） | `src/skills/recurring-team-signal-collection/templates/` |
| `weekly_signal_collection_template.md` | ✅ 更新（Profile Gate 列、Linked Signals 节） | `src/skills/recurring-team-signal-collection/templates/` |
| `postmatch_signal_validation_template.md` | ✅ **新增** | `src/skills/recurring-team-signal-collection/templates/` |

---

## 二、Profile Crosscheck Hard Gate 规则速查

```
HIGH / CRITICAL 信号进入 prematch handoff 前必须满足以下条件之一：

  条件 A（推荐）：
    profile_crosschecked: true
    blocking_status: clear

  条件 B（允许，但需注明）：
    profile_crosschecked: false
    profile_crosscheck_pending: true
    profile_crosscheck_pending_reason: "原因"
    blocking_status: conditional

  不允许（blocked）：
    profile_crosschecked: false
    profile_crosscheck_pending: false（或缺失）
    → blocking_status: blocked
    → 不得进入 prematch handoff
```

---

## 三、linked_signals 字段使用指南

**适用场景**：多队/多场联动，如降级区多队互相依赖结果

```json
"linked_signals": {
  "linked_signal_ids": ["lecce_table_pressure_20260521"],
  "linked_reason": "Cremonese 赢球且 Lecce 不赢则 Lecce 降级，两场同时开踢",
  "linked_scope": "fixture_cluster"
}
```

**linked_scope 枚举**：
- `team`：同一球队的多个信号互相关联
- `match`：同场比赛双方信号关联
- `league`：联赛级别的系统性信号
- `table`：积分榜联动（如多队同积分）
- `fixture_cluster`：多场同时开踢，结果互相影响

---

## 四、postmatch_validation scan 执行指南

**触发时机**：赛后 24-48 小时内

**执行步骤**：
1. 读取本轮 `postmatch_validation_required=true` 的信号列表
2. 对每条信号搜索赛后比赛数据（xG、比分、首发、关键事件）
3. 对照 `expected_effect` 判断 `validation_result`
4. 填写 `match_observation`（基于真实数据，禁止编造）
5. 判断 `promotion_type`
6. 输出 `draft_reports/recurring-team-signal-collection_{league}_{date}_postmatch_validation.{json,md}`

---

## 五、Weekly Cadence 正式触发规则

```
每周固定节奏（以 EPL 为例）：

  周一 / 周二
    → weekly_baseline scan
    → 输出：draft_reports/recurring-team-signal-collection_EPL_{YYYYMMDD}.{json,md}

  matchday -2（赛前两天）
    → matchday_live scan（第一次）
    → 输出：draft_reports/recurring-team-signal-collection_EPL_{YYYYMMDD}_matchday_live.{json,md}

  matchday -1（赛前一天）
    → matchday_live scan（更新版，重点更新 official_confirmed）
    → 输出：draft_reports/recurring-team-signal-collection_EPL_{YYYYMMDD}_matchday_live_d1.{json,md}

  赛后 24-48 小时内
    → postmatch_validation scan
    → 输出：draft_reports/recurring-team-signal-collection_EPL_{YYYYMMDD}_postmatch_validation.{json,md}
```

---

## 六、Go/No-Go 最终结论

**结论：GO — 正式进入 weekly cadence**

v1.1 硬化后，所有 LMW-92 试运行的 Go 条件均已满足：

| 条件 | 状态 |
|---|---|
| 正式 weekly run 必须执行 profile crosscheck | ✅ Hard gate 已固化 |
| SKILL.md 增加 linked_signals 字段 | ✅ 已加入 schema |
| 下一轮赛后必须执行 postmatch_validation scan | ✅ 完整 schema 已定义，模板已创建 |

**首次正式 weekly run 建议**：
- 下赛季开始时（2026-08 左右）执行第一次正式 weekly_baseline scan
- 执行时必须完成 HIGH/CRITICAL 信号的 profile crosscheck（`profile_crosschecked=true`）
- 赛后 24-48h 执行 postmatch_validation scan，完成完整闭环

---

## 七、文件清单

| 文件 | 路径 | 状态 |
|---|---|---|
| SKILL.md v1.1 | `src/skills/recurring-team-signal-collection/SKILL.md` | ✅ |
| team_signal_log_template.md | `src/skills/recurring-team-signal-collection/templates/` | ✅ 更新 |
| weekly_signal_collection_template.md | `src/skills/recurring-team-signal-collection/templates/` | ✅ 更新 |
| postmatch_signal_validation_template.md | `src/skills/recurring-team-signal-collection/templates/` | ✅ 新增 |
| 本硬化报告 | `docs/agents/handover_recurring_signal_hardening_2026-05-21.md` | ✅ |
