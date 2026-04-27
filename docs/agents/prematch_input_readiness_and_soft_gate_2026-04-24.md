# prematch 输入诊断与软门禁恢复记录（2026-04-24）

## 1. 背景
- `26066` 暴露出的真实问题不是“文件没生成”，而是两层问题叠加：
- `AuditRouter` 之前的硬拒收存量仍遗留在 `03_Review_Reports/REJECTED-*`，与当前软门禁策略不一致。
- `osint_pipeline.py` 的 prematch readiness 只检查 RAG metadata 覆盖，不检查 Team Archive 是否只是 `team_forge.py` 生成的默认壳档。
- 结果是：
- 单场如果刚好命中已有实质档案的球队，看起来正常。
- 全量 mixed-league 跑时，虽然 coverage 通过，但 engine 仍会大量产出 `整体韧性 0.000 / Insufficient Resilience Data`。

## 2. 本次改动
- 文件：`src/data/audit_router.py`
- 新增 `_restore_soft_gated_reviews(...)`
- 对仅命中 `insufficient_resilience_data / low_confidence` 的历史 `REJECTED-*` 自动恢复回 `01_Prematch_Audits`
- 并清理 review 里的软门禁旧存量
- `draft_stub` / `cross_team_contamination` 仍保持硬拒收，不会恢复

- 文件：`src/data/osint_pipeline.py`
- 新增 `inspect_prematch_input_readiness(...)`
- 诊断项包括：
- `mapping_source=unmapped`
- Team Archive 是否存在
- Team Archive 是否仍是 `Baseline profile initialized by team_forge.py` / `待更新` / `Unknown` 高密度壳档
- 每队在 Chroma `embedding_metadata` 中的文档条数（`thin_rag_docs`）
- 流水线会自动写入：
- `03_Match_Audits/{issue}/03_Review_Reports/REVIEW-{issue}-Prematch_Input_Readiness.md`

## 3. 实测结论
- `26066` 输入诊断显示：
- `14` 场里 `11` 场是 `unmapped`
- `28` 支球队里 `25` 支仍是 placeholder Team Archive
- 几乎全部球队在 RAG 中都只有 `1` 条 team metadata 文档
- 这解释了为什么 issue 级 coverage 可通过，但批量 prematch 仍大面积 `HALT`

## 4. 当前流程含义
- `01_Prematch_Audits`
- 保留正式生成稿
- 其中允许存在 `low confidence / Insufficient Resilience Data`，但会在 `Prematch_Data_Quality` 里持续标红
- `03_Review_Reports/REJECTED-*`
- 只保留真正需要移出的稿件：
- `draft`
- `cross-team contamination`
- 以及其他非软门禁异常

## 5. 后续操作建议
- 如果要提升全量 issue 成功率，重点不是继续调 router，而是补强 Team Archives 的有效内容，避免仅靠默认壳档通过 readiness gate。
- `REVIEW-{issue}-Prematch_Input_Readiness.md` 应作为批量 issue 的首个排障入口。
