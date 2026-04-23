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

## 4. 追加收敛（2026-04-23）
- 文件：`src/data/osint_pipeline.py`
- 新增 Prematch RAG Readiness Gate：
- 在调用 `20-engine/main.py audit-issue` 前，先只读检查 `20-engine/chromadb/chroma.sqlite3`
- 检查项包含：
- `embeddings` 总文档数
- `embedding_metadata.key='team'` 的 issue 球队覆盖率
- 若命中 `rag_undercoverage / rag_missing_database / rag_unreadable_database / rag_query_failed`，则：
- 不再继续执行 prematch 引擎
- 不再制造一批 `整体韧性=0.000` 的伪正式报告
- 自动写入 `03_Match_Audits/{issue}/03_Review_Reports/REVIEW-{issue}-Prematch_Blocker.md`
- 结论层面会明确区分：这是上游 RAG 样本库供给不足，不是路径映射故障

## 5. 今天复发后的补丁（2026-04-24）
- 根因不是 `docs/agents` 没记，而是代码链路还有两个漏点：
- `osint_pipeline.py` 昨天只做了 prematch 前的 readiness gate，但 `20-engine audit-issue` 跑完后没有立刻再次收口，低质量正式稿会先留在 `01_Prematch_Audits`，直到后续 postmatch/收尾才有机会被搬走。
- `audit_router.py` 之前只会处理 `_Host` 这种重复真实稿，像 `PSG` / `Paris_Saint_Germain` 这类同场别名变体，仍可能在 rejected review 里留下两份。
- 今天新增的代码收敛：
- `osint_pipeline.py` 在 prematch 引擎执行完后，立即再跑一次 `audit_router.ensure_issue_governance(...)`，把低质量 prematch 当场转入 review。
- `audit_router.py` 现在会基于 dispatch manifest 的 match index + canonical filename 统一识别“同一场比赛”，不再只靠 `_Host` 后缀。
- rejected review 现在也会按 canonical match file 去重，避免同一场比赛出现多份 `REJECTED-*` 变体稿。
- rejected review 顶部会写入结构化元数据（`canonical_report / source_report / reject_reasons`），后续汇总单不再只能靠字符串扫描。

## 6. 上游阻断再收紧（2026-04-24）
- 复盘 `26065` 后确认，昨天的问题不是 router 失效，而是 readiness gate 过松：
- 当时只要求 issue 球队覆盖率达到 `1/4`，像 `26065` 这种 `14/28` 的半覆盖 issue 仍会被放行，随后产出整批 `HALT / low confidence / contamination` 报告，再由 router 全部拒收。
- 现在 `src/data/osint_pipeline.py` 改为双阈值阻断：
- `ARES_PREMATCH_RAG_MIN_TEAM_COVERAGE_RATIO`，默认 `0.75`
- `ARES_PREMATCH_RAG_MAX_MISSING_TEAMS`，默认 `4`
- `ARES_PREMATCH_RAG_MIN_DOC_COUNT`，默认 `3`
- 命中阈值时不再继续调用 `20-engine audit-issue`。

## 7. 直跑 engine 的兜底（2026-04-24）
- 另一个复发风险点是：有人可能直接在 `20-ares-v4-engine` 里跑 `main.py audit-issue`，这会绕开 telemetry orchestrator。
- 现在 `20-ares-v4-engine/main.py` 也加了最小兜底：
- `audit-issue` 批量写入完成后，会尝试回调同级目录下的 `21-ares-osint-telemetry/src/data/audit_router.py --issue <issue> --no-prematch-stubs`
- 也就是说，即使 direct-run engine，最终也会落到同一套 prematch 质量闸门，而不是把低质量正式稿长期留在 `01_Prematch_Audits`
