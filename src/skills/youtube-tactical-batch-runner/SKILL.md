---
name: youtube-tactical-batch-runner
version: "1.0"
source: ares-osint-telemetry native
description: >
  YouTube Tactical Intelligence 批量端到端 runner（v1.0）。
  读取 ingestion_queue.json，对每个合格条目依次执行
  YT-02 → YT-03 → YT-04 → YT-05 → YT-06，
  输出 batch run report。
  不负责 source discovery，只消费 queue。
  NotebookLM 不在 batch v1 范围内。
inputs:
  required:
    - queue_file: ingestion_queue.json 文件路径
  optional:
    - reuse_existing: 复用已有产物（默认 false）
    - force: 强制重跑所有条目（默认 false）
    - dry_run: 只检查 eligibility，不实际执行（默认 false）
    - max_items: 最大处理条目数（默认无限制）
outputs:
  - Batch Run Report（JSON + Markdown）→
      /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/batch_runs/
      <run_id>_batch_run_report.md
      <run_id>_batch_run_report.json
changelog:
  - v1.0 (2026-05-27): initial release，基于 LMW-132 定义的 batch runner 规范
---

# YouTube Tactical Batch Runner v1.0

## 执行模式说明

本 Skill 是一套**模型无关的 Agent 执行规范**。当你在 Antigravity / Kiro 中被用户通过 `@skill` 或文件路径方式调用时，请按照本文件的步骤逐一执行。

**你是 batch orchestrator，不是 source discovery 引擎。** 你的职责是：
1. 读取 ingestion_queue.json
2. 对每个条目检查 eligibility
3. 对合格条目依次执行 YT-02 → YT-03 → YT-04 → YT-05 → YT-06
4. 记录每个条目的执行状态
5. 输出 batch run report

**严格边界**：
- ✅ 读取 ingestion_queue.json
- ✅ 检查条目 eligibility
- ✅ 执行 YT-02 → YT-06 pipeline
- ✅ 输出 batch run report
- ✅ 幂等性检测（默认不覆盖已有产物）
- ❌ 不做 source discovery（不搜索 YouTube）
- ❌ 不处理 `pending_user_lookup` 条目
- ❌ 不发明缺失的 URL
- ❌ 不下载视频/音频
- ❌ 不写入 `profile_authority: true`
- ❌ 不将 rejected/needs_review claims 写入 Team Archive
- ❌ 不覆盖现有 Team Archive 内容
- ❌ 不使用 NotebookLM 作为主路径

---

## 测试 Fixture 说明

### Test A — Arsenal Regression Queue（幂等性/复用/skip 测试）

```
fixture_type: regression_idempotency
queue_file: 2026-05-27_Arsenal_transcript_ingestion_queue.json
```

**用途**：Arsenal 已有大量既有产物，适合验证：
- `already_exists` 检测（默认不覆盖）
- `--reuse-existing` 复用已有产物
- `--force` 才允许重跑
- `pending_user_lookup` 条目被 skip
- batch report 对 skipped / reused / already_exists 的记录

**不适合**：验证 fresh production run（已有产物会触发 already_exists）

### Test B — Liverpool Fresh Queue（完整 fresh production run 测试）

```
fixture_type: fresh_production_run
queue_file: 2026-05-27_Liverpool_transcript_ingestion_queue.json
```

**用途**：Liverpool 无既有产物，适合验证：
- 从 0 开始跑完整 YT-02 → YT-06
- 每个阶段的正常执行路径
- 新 Team Archive 的 candidate zone 写入

**不适合**：幂等性测试（无既有产物）

---

## Eligibility 检查规则

对每个 queue 条目，按以下顺序检查：

| 检查项 | 通过条件 | 失败时 |
|--------|---------|--------|
| `ingestion_status` | `queued` | skip: `not_queued` |
| `recommended_next_step` | `transcript_ingestion` | skip: `wrong_next_step` |
| `source_url` | 非空且非 `PENDING_USER_LOOKUP` | skip: `missing_url` |
| `video_id` | 非空且非 `PENDING_USER_LOOKUP` | skip: `missing_video_id` |
| `source_tier` | `tier_1` 或 `tier_2`（不含 excluded） | skip: `source_authority_not_allowed` |
| `likely_has_subtitles` | `true` 或 `probable` | warn（不 skip，但降低优先级） |
| `source_quality` | 非 `low_trust` / `excluded` | skip: `source_quality_excluded` |

---

## 幂等性行为

| 模式 | 行为 |
|------|------|
| 默认（无参数） | 检测已有产物，跳过并记录 `already_exists` |
| `--reuse-existing` | 复用已有 transcript/claims/validation，只重跑缺失的阶段 |
| `--force` | 强制重跑所有阶段，覆盖已有产物 |

**已有产物检测**：按 canonical filename 检查：
```
transcripts/<date>_<team>_<channel>_<video_id>_transcript_raw.md
claims/<date>_<team>_<channel>_<video_id>_claims.md
validation/<date>_<team>_<channel>_<video_id>_validation.md
patch_proposals/<date>_<team>_<channel>_<video_id>_team_archive_patch_proposal.md
```

---

## 执行流程（Per Item）

```
Step 1: Eligibility check
  → 不合格 → skip + 记录 skip_reason

Step 2: Idempotency check
  → 已有完整产物 + 默认模式 → already_exists + skip
  → 已有完整产物 + --reuse-existing → reuse + 跳过已完成阶段
  → 已有完整产物 + --force → 重跑所有阶段

Step 3: YT-02 transcript ingestion
  → 成功 → 继续 Step 4
  → blocked → 记录 blocked，停止该条目，继续下一条目

Step 4: YT-03 claim extraction
  → 有 claims → 继续 Step 5
  → empty/blocked → 记录，停止该条目

Step 5: YT-04 validation
  → 有 validated + candidate_after_review → 继续 Step 6
  → 全部 rejected/needs_review → 记录，停止该条目

Step 6: YT-05 patch proposal
  → 生成 proposal → 继续 Step 7

Step 7: YT-06 candidate auto-apply
  → 满足 auto-apply policy → 写入 Team Archive candidate zone
  → 不满足 → 记录 manual_review_required
```

**关键原则**：一个条目失败不停止整个 batch。

---

## 命令示例

### Agent 调用（推荐）

在 Antigravity / Kiro 中加载本 SKILL.md，然后提供：

```
queue_file: /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/ingestion_queue/2026-05-27_Liverpool_transcript_ingestion_queue.json
```

### Helper Script 调用

```bash
# Test A — Arsenal regression（幂等性测试）
./venv/bin/python src/skills/youtube-tactical-batch-runner/scripts/run_queue.py \
  --queue "/path/to/2026-05-27_Arsenal_transcript_ingestion_queue.json" \
  --dry-run

# Test A — 验证 already_exists（默认不覆盖）
./venv/bin/python src/skills/youtube-tactical-batch-runner/scripts/run_queue.py \
  --queue "/path/to/2026-05-27_Arsenal_transcript_ingestion_queue.json"

# Test A — 复用已有产物
./venv/bin/python src/skills/youtube-tactical-batch-runner/scripts/run_queue.py \
  --queue "/path/to/2026-05-27_Arsenal_transcript_ingestion_queue.json" \
  --reuse-existing

# Test A — 强制重跑
./venv/bin/python src/skills/youtube-tactical-batch-runner/scripts/run_queue.py \
  --queue "/path/to/2026-05-27_Arsenal_transcript_ingestion_queue.json" \
  --force

# Test B — Liverpool fresh run
./venv/bin/python src/skills/youtube-tactical-batch-runner/scripts/run_queue.py \
  --queue "/path/to/2026-05-27_Liverpool_transcript_ingestion_queue.json"
```

---

## Source Authority 检查

在处理每个条目前，检查来源是否符合 Source Authority Matrix（LMW-131）：

| 来源 Tier | transcript_ingestion | claim_extraction | 备注 |
|----------|---------------------|-----------------|------|
| Tier 1 | ✅ | ✅ | 高质量战术分析 |
| Tier 2 | ✅ | ✅ | 高质量战术分析 |
| Tier 3 | ❌（人工路径） | ❌ | 不在 batch v1 范围 |
| Tier 4 | ❌ | ❌ | 新闻背景，不适合 transcript |
| Tier 5 | ❌ | ❌ | 仅 discovery lead |
| excluded | ❌ | ❌ | 直接 skip |

---

## Batch Run Report 格式

输出到 `batch_runs/` 目录，文件名：
```
<run_id>_batch_run_report.md
<run_id>_batch_run_report.json
```

`run_id` 格式：`<YYYYMMDD>_<team>_<queue_basename_short>`

---

## 验证清单（v1.0）

Test A（Arsenal regression）：
- [ ] `pending_user_lookup` 条目被 skip，记录 `missing_url`
- [ ] 已有 transcript 被检测为 `already_exists`（默认模式）
- [ ] `--reuse-existing` 复用已有 transcript，只重跑后续阶段
- [ ] `--force` 重跑所有阶段
- [ ] batch report 记录 skipped / reused / already_exists 数量

Test B（Liverpool fresh）：
- [ ] 2 个 queued 条目全部进入 YT-02
- [ ] YT-02 成功或生成 blocked report
- [ ] YT-03 提取 claims（如 transcript 成功）
- [ ] YT-04 验证 claims
- [ ] YT-05 生成 patch proposal
- [ ] YT-06 auto-apply（如满足 policy）
- [ ] batch report 记录完整 per-item 状态

---

## 打包资源

- `AresVault/01_Governance/模板 - YouTube Tactical Batch Run Report v1.md` — run report 模板
- `AresVault/04_RAG_Raw_Data/youtube_tactical_sources/ingestion_queue/2026-05-27_Arsenal_transcript_ingestion_queue.json` — Test A fixture
- `AresVault/04_RAG_Raw_Data/youtube_tactical_sources/ingestion_queue/2026-05-27_Liverpool_transcript_ingestion_queue.json` — Test B fixture
- `AresVault/01_Governance/规范 - Ares Source Authority Matrix v1.md` — 来源权威性矩阵（LMW-131）
- `AresVault/01_Governance/规范 - Ares Team Archive candidate auto-apply policy v1.md` — YT-06 policy（LMW-129）
