# prematch 质量闸门升级记录（2026-04-23）

## 1. 背景
- `03_Match_Audits/{issue}/01_Prematch_Audits` 之前会混入三类不该保留的文件：
- `draft` 骨架稿
- 带 `Insufficient Resilience Data` / 停机标记的低质量正式稿
- 被错误 RAG 样本污染的跨队 prematch 报告

## 2. 本次变更
- 文件：`src/data/audit_router.py`
- 新增 prematch 质量闸门，路由阶段会自动拒收并移出 `01_Prematch_Audits`：
- `draft_stub`
- `insufficient_resilience_data`
- `low_confidence`
- `cross_team_contamination`
- 被拒收文件会落到：
- `03_Match_Audits/{issue}/03_Review_Reports/REJECTED-<原文件名>.md`
- 拒收件会保留原文，并在顶部写明拒收原因与时间。
- 汇总单 `REVIEW-{issue}-Prematch_Data_Quality.md` 现在会区分：
- accepted prematch
- rejected prematch
- drafts
- low confidence
- insufficient resilience data
- cross-team contamination

## 3. 当前行为
- `01_Prematch_Audits` 只保留可用的 match-level prematch 审计。
- 质量不过关的 prematch 不再作为正式报告留在 issue 主目录。
