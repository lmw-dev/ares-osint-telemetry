# {联赛} 周度球队信号采集报告 v1.1

**扫描类型**: Weekly Baseline Scan
**参考周**: {week_start} ~ {week_end}
**生成时间**: {generated_at}
**分析引擎**: {model_name}
**联赛**: {league}
**覆盖球队**: {N} 支
**有效信号**: {M} 条

> **Source-bound report**: All listed signals include `source_ref`, `source_confidence`, and `cross_source_verified`. Signals marked `official_confirmed: false` require matchday revalidation before entering prematch runtime_caveats.
> **Profile crosscheck status**: {N} signals have `profile_crosschecked=true`; {M} signals have `profile_crosscheck_pending=true`. Per Skill v1.1 rules, HIGH/CRITICAL signals must be `profile_crosschecked=true` or `profile_crosscheck_pending=true + reason`.

---

## 一、信号概览

| 球队 | Signal Type | Severity | Lifetime | Prematch Relevance | Profile Gate | 摘要 |
|---|---|---|---|---|---|---|
| **{team}** | `{signal_type}` | `{severity}` | `{expires_after}` | `{high/medium/low}` | `clear/conditional` | {一句话摘要} |

---

## 二、CRITICAL 信号详情

### {N}. {球队名}（{联赛位置}）

> [!WARNING]
> **{信号标题}**

- **Signal Type**: `{signal_type}`
- **Severity**: CRITICAL
- **Observed At**: {YYYY-MM-DD}
- **Fixture**: {比赛描述}（fixture_resolved: true/false）
- **来源**: [{来源标题}]({来源 URL})
- **source_confidence**: {high/medium/low} | **cross_source_verified**: {true/false} | **official_confirmed**: {true/false}
- **摘要**: {基于搜索结果的具体描述，禁止编造}
- **Prematch Caveat**: {对即将到来比赛的具体影响}
- **Expires After**: `{expires_after}`
- **Durable Candidate**: `{true/false}`
- **Profile Gate**: `{clear/conditional/blocked}` — {说明}
- **Linked Signals**: {如有联动，列出 linked_signal_ids 和 linked_reason}

---

## 三、HIGH 信号详情

（同 CRITICAL 格式）

---

## 四、MEDIUM 信号汇总（context notes）

| 球队 | Signal Type | Context Note | Fixture | Expires After | Profile Gate |
|---|---|---|---|---|---|
| {team} | `{signal_type}` | {context note} | {fixture} | `{expires_after}` | `{clear/conditional}` |

---

## 五、Linked Signals 联动关系

（仅当存在 fixture_cluster / table 联动时输出此节）

```
{联动关系描述，如：
  Cremonese table_pressure ↔ Lecce table_pressure
  linked_scope: fixture_cluster
  linked_reason: Cremonese 赢球且 Lecce 不赢则 Lecce 降级，两场同时开踢
}
```

---

## 六、Durable Candidate 候选池

| 球队 | Signal Type | Severity | 首次观察 | Profile Gate | 说明 |
|---|---|---|---|---|---|
| {team} | `{signal_type}` | `{severity}` | {YYYY-MM-DD} | `{clear/conditional}` | {为何认为具有长期记忆价值} |

---

## 七、Prematch Handoff 预告

### runtime_caveats（CRITICAL / HIGH，blocking_status=clear 或 conditional）
- **{球队}** *(vs {对手})*: {runtime_caveat}

### context_notes（MEDIUM）
- **{球队}** *(vs {对手})*: {context_note}

---

## 八、matchday_live scan 建议

需要在 matchday -2/-1 重点验证的信号：
- **{球队}**: {具体需要验证的内容}

---

## 九、无信号球队

{球队列表}

> 说明：无信号不代表无风险，仅代表当前公开信息中未发现符合触发条件的事实。

---

## 十、情报来源汇总

| 球队 | 来源 | source_type | source_confidence | 支持信号 |
|---|---|---|---|---|
| {team} | [{来源标题}]({URL}) | `{source_type}` | {high/medium/low} | `{signal_type}` |

---

*Source-bound report: All listed signals include source_ref, source_confidence, and cross_source_verified. Signals marked official_confirmed=false require matchday revalidation before entering prematch runtime_caveats. Profile crosscheck status is explicitly marked per signal — HIGH/CRITICAL signals must be profile_crosschecked=true or profile_crosscheck_pending=true with reason in future weekly runs.*
*下次扫描建议：matchday_live scan — {next_matchday_minus_2}*
