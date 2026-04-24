# postmatch 存量清理机制记录（2026-04-24）

## 1. 背景
- `26065` 的 prematch / postmatch 上游门禁已经收紧，但 `03_Match_Audits/Postmatch_Telemetry/` 仍残留历史存量。
- 这些旧文件是在 `official_score/result_score` 尚未纳入强制门禁时落盘的，虽然 frontmatter 中写着 `validation_passed: true`，但不应再被视为已验证赛后事实。

## 2. 新增机制
- 文件：`src/data/postmatch_cleanup.py`
- CLI：
  - `./venv/bin/python src/data/postmatch_cleanup.py --issue 26065`

## 3. 清理规则
- `STALE`
  - manifest / cold data 对应比赛日期超出 issue 时间窗口时，自动移出 `Postmatch_Telemetry` 主索引。
  - 文件迁入：`03_Match_Audits/{issue}/04_Postmatch_Legacy/STALE-*.md`
- `PENDING_VERIFICATION`
  - manifest 缺少 `official_score/result_score` 时，历史 postmatch 不得继续作为“已验证结果”留在主索引。
  - 文件迁入：`03_Match_Audits/{issue}/04_Postmatch_Legacy/PENDING-VERIFY-*.md`
- 两类文件都会补写：
  - `result.validation_passed: false`
  - `postmatch_review_status`
  - `postmatch_review_reasons`
  - `postmatch_reviewed_at`
  - 正文顶部 warning 标记

## 4. 报告产物
- 输出：
  - `03_Match_Audits/{issue}/03_Review_Reports/REVIEW-{issue}-Postmatch_Cleanup.md`
- 用途：
  - 记录 issue 时间窗口
  - 给出主索引清理前后对比
  - 列出新隔离 / 新待核验 / 已在 legacy 的文件

## 5. 26065 实测结果
- 首次执行：
  - `before=9`
  - `after=0`
  - `new_pending=9`
  - `already_legacy=10`
- 其中：
  - `30449` 作为明确串期样本保留在 `STALE-26065_30449_postmatch.md`
  - 其余 9 份主索引历史文件因缺少 `official_score/result_score`，统一迁为 `PENDING-VERIFY-*`
- 再次执行验证幂等：
  - `before=0`
  - `after=0`
  - `new_pending=0`
  - `already_legacy=10`
