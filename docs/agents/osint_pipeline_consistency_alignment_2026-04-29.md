# osint_pipeline 一致性对齐记录 2026-04-29

## 背景
- 目标：修复 `26068` 一期中 `Audit / Gate / Synthesis / Outcome / README` 口径不一致问题。
- 代码提交：`e4bd8c9`

## 本次完成

1. Prematch Input Gate 升级为结构化真源
- 新增并落盘：`REVIEW-{issue}-Prematch_Input_Gate.json`
- 字段包含：`issue_status`、`selected_matches`、`filtered_matches`、`rows`、`selected_match_indices`

2. Pipeline 与 Gate 状态联动
- `selected_matches=0` 时，pipeline 不再默默推进 prematch，改为写 blocker 并停在门禁层。

3. Preflight 状态对齐 Gate
- `prematch_preflight.py` 读取 gate json，避免出现 `Audit=READY` 但 gate 实际阻断的冲突。

4. Synthesis 执行语义拆分
- 每场新增：
  - `analysis_suggestion`
  - `final_suggestion`
  - `candidate_tier`
  - `non_actionable`
- `放弃` 桶统一强制 `final_suggestion=skip`，分析候选只做叙述，不进入执行统计。

5. Outcome 计分口径修正
- `prematch_outcome_review.py` 改为优先读取 synthesis json。
- `Actionable Picks` 仅统计 `稳胆/博弈` 且 `final_suggestion != skip` 的场次。
- pending 细分为：
  - `pending_match_result`
  - `missing_postmatch_artifact`

6. Postmatch 覆盖解释增强
- `postmatch_synthesis.py` 增加 `expected vs actual` 覆盖段，逐场列出缺失原因。

7. README 口径修正
- `Mapping Progress` 更名并拆分为：
  - `Mapping Ready`
  - `Postmatch Coverage`

8. 新增自动一致性告警层
- 自动生成：
  - `REVIEW-{issue}-Consistency_Warnings.md`
  - `REVIEW-{issue}-Consistency_Warnings.json`
- 检查项覆盖：
  - Gate 计数守恒/明细一致性
  - Audit 与 Gate 状态冲突
  - Synthesis 姿态与候选池冲突
  - Outcome 与 Synthesis 可执行计数冲突
  - README 与 manifest/postmatch 汇总冲突

9. 入口红灯层（README 顶部）
- 新增：
  - `Consistency Status: OK|WARN|ERROR`
  - `Consistency Warnings: N (error=x, warn=y)`

## 变更脚本
- `src/data/osint_pipeline.py`
- `src/data/prematch_preflight.py`
- `src/data/prematch_synthesis.py`
- `src/data/prematch_outcome_review.py`
- `src/data/postmatch_synthesis.py`
- `src/data/audit_router.py`

## 26068 回归结果
- `Consistency Status: OK`
- `Consistency Warnings: 0 (error=0, warn=0)`
- Prematch outcome：
  - `Actionable Picks: 0`
  - `Skipped: 14`
  - `Hit Rate: 0.0%`
- Postmatch coverage：
  - `10/14`
  - 缺失 4 场均为 `titan_only_no_understat_id`

## 后续建议
- 可在 `00_Governance` 追加最近 N 期一致性看板。
- 可在发布路径上加 `warning_count > 0` 的阻断或人工复核开关。
