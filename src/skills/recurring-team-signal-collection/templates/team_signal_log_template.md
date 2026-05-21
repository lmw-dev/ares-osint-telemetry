# 球队信号记录 v1.1 — {team} — {date}

**联赛**: {league}
**扫描类型**: {scan_type}（weekly_baseline / matchday_live / postmatch_validation）
**参考日期**: {reference_date}

---

## 信号记录

### 信号 {N}

| 字段 | 值 |
|---|---|
| **Signal ID** | `{team_slug}_{signal_type}_{YYYYMMDD}` |
| **Signal Type** | `{signal_type}` |
| **Severity** | `CRITICAL / HIGH / MEDIUM / LOW` |
| **Source Type** | `official / press_conference / reliable_media / journalist_report / aggregated_media / data_analysis / standings / market_data` |
| **Source Ref** | {来源 URL 或描述} |
| **Source Confidence** | `high / medium / low` |
| **Cross Source Verified** | `true / false` |
| **Official Confirmed** | `true / false` |
| **Observed At** | {YYYY-MM-DD} |
| **Fixture Resolved** | `true / false` |
| **Prematch Relevance** | `high / medium / low / none` |
| **Expires After** | `expires_after_next_match / expires_after_matchday / monitor_2_matches / durable_candidate / archived_noise` |
| **Durable Candidate** | `true / false` |
| **Postmatch Validation Required** | `true / false` |
| **Profile Crosschecked** | `true / false` |
| **Profile Conflict Detected** | `true / false` |
| **Profile Crosscheck Pending** | `true / false` |
| **Profile Crosscheck Pending Reason** | {原因，如 "Team Archive not available"} |
| **Blocking Status** | `clear / conditional / blocked` |
| **Handoff Level** | `runtime_caveat / context_note / background_note` |
| **Status** | `active / validated / rejected / archived` |

**摘要**:
> {一句话信号描述，基于实际搜索结果，禁止编造}

**Prematch Caveat**（仅 CRITICAL / HIGH 填写）:
> {prematch 需要注意的具体内容}

**Context Note**（仅 MEDIUM 填写）:
> {背景信息描述}

**受影响比赛**:
- {比赛描述，如 Arsenal vs Chelsea 2026-05-25 16:00 BST}

**Linked Signals**（如有联动）:
- linked_signal_ids: [{signal_id_1}, {signal_id_2}]
- linked_reason: {联动原因}
- linked_scope: `team / match / league / table / fixture_cluster`

**Post-Match Validation 结果**（赛后填写）:
- validation_result: `validated / rejected / inconclusive / data_error`
- promotion_type: `durable_learning / one_off_noise / data_error / needs_more_samples`
- 说明: {验证说明}

---

*Source-bound report: All listed signals include source_ref, source_confidence, and cross_source_verified. Signals marked official_confirmed=false require matchday revalidation. Profile crosscheck status is explicitly marked per signal.*
