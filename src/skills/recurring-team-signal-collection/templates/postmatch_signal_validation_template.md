# {联赛} 赛后信号验证报告 v1.1

**扫描类型**: postmatch_validation
**比赛日**: {matchday_date}
**生成时间**: {generated_at}
**分析引擎**: {model_name}
**联赛**: {league}
**验证信号数**: {N} 条
**validated**: {N1} | **rejected**: {N2} | **inconclusive**: {N3} | **data_error**: {N4}

> **Source-bound report**: All validation observations include source_ref. Match data sourced from Understat / FBref / official match reports.

---

## 一、验证结果概览

| 球队 | Signal Type | 原 Severity | 验证结果 | Promotion Type | 摘要 |
|---|---|---|---|---|---|
| **{team}** | `{signal_type}` | `{severity}` | `validated/rejected/inconclusive` | `durable_learning/one_off_noise` | {一句话说明} |

---

## 二、逐信号验证详情

### {N}. {球队名} — `{signal_type}` ({原 severity})

- **Signal ID**: `{signal_id}`
- **Expected Effect**: {赛前预期该信号会产生的影响}
- **Match Result**: {比赛结果，如 Cremonese 1-2 Como}
- **Match Observation**: {赛后实际观察到的情况，基于比赛数据/报道，禁止编造}
- **来源**: [{来源标题}]({来源 URL})
- **Validation Result**: `validated / rejected / inconclusive / data_error`
- **Validation Notes**: {验证说明，解释为何得出此结论}
- **Promote to Memory Candidate**: `true / false`
- **Archive as Noise**: `true / false`
- **Promotion Type**: `durable_learning / one_off_noise / data_error / needs_more_samples`
- **Followup Required**: `true / false`
- **Followup Reason**: {若 followup_required=true，说明原因}

---

## 三、Durable Learning 候选（promote_to_memory_candidate=true）

| 球队 | Signal Type | 验证结果 | 说明 | 推送至 Team Archive |
|---|---|---|---|---|
| {team} | `{signal_type}` | validated | {说明} | `02_Team_Archives/{league}/{team}.md` |

---

## 四、Archived Noise（archive_as_noise=true）

| 球队 | Signal Type | 原因 |
|---|---|---|
| {team} | `{signal_type}` | {one_off_noise / data_error 说明} |

---

## 五、Needs More Samples（followup_required=true）

| 球队 | Signal Type | Followup Reason | 下轮追踪建议 |
|---|---|---|---|
| {team} | `{signal_type}` | {原因} | {建议} |

---

## 六、下周 weekly_baseline scan 建议

基于本轮验证结果，下周 weekly_baseline scan 重点关注：
- {球队}: {原因}

---

*Source-bound report: All validation observations include source_ref. Match data sourced from Understat / FBref / official match reports. Promotion decisions are based on observed match data, not predictions.*
