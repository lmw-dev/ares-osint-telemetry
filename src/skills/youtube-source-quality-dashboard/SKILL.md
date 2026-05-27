---
name: youtube-source-quality-dashboard
version: "1.0"
source: ares-osint-telemetry native
description: >
  YouTube Tactical Intelligence 来源质量 dashboard（v1.0）。
  读取现有 pipeline artifacts（queue / transcripts / claims / validation / patch_proposals / batch_runs），
  生成来源质量报告，回答"哪些来源值得继续使用"。
  只生成报告，不修改任何 artifact，不修改 Team Archive，不修改来源注册表。
inputs:
  optional:
    - vault_path: AresVault 根目录（默认从环境变量读取）
    - output_label: 报告标签（默认 YYYY-MM-DD）
    - min_sample_size: 最小样本量阈值（默认 2，低于此值标注 needs_more_sample）
outputs:
  - Quality Report（Markdown）→
      /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/quality_reports/
      <YYYY-MM-DD>_source_quality_report.md
  - Quality Report（JSON）→
      /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/quality_reports/
      <YYYY-MM-DD>_source_quality_report.json
changelog:
  - v1.0 (2026-05-27): initial release，基于 LMW-133 定义的 source quality dashboard 规范
---

# YouTube Source Quality Dashboard v1.0

## 执行模式说明

本 Skill 是一套**只读报告生成规范**。你的职责是：
1. 读取现有 pipeline artifacts
2. 计算质量指标
3. 生成来源性能报告和推荐
4. 不修改任何 artifact

**严格边界**：
- ✅ 读取 queue / transcripts / claims / validation / patch_proposals / batch_runs
- ✅ 计算质量指标
- ✅ 生成报告和推荐
- ❌ 不获取新 transcript
- ❌ 不发现新 YouTube URL
- ❌ 不提取新 claims
- ❌ 不验证 claims
- ❌ 不修改 Team Archive
- ❌ 不修改来源 artifact
- ❌ 不自动修改来源 tier 注册表
- ❌ 不修改 Team Archive memory

---

## 输入 Artifacts

```text
04_RAG_Raw_Data/youtube_tactical_sources/ingestion_queue/*.json
04_RAG_Raw_Data/youtube_tactical_sources/transcripts/*.md
04_RAG_Raw_Data/youtube_tactical_sources/transcripts/blocked/*.md
04_RAG_Raw_Data/youtube_tactical_sources/claims/*.md
04_RAG_Raw_Data/youtube_tactical_sources/validation/*.md
04_RAG_Raw_Data/youtube_tactical_sources/patch_proposals/*.md
04_RAG_Raw_Data/youtube_tactical_sources/batch_runs/*.json
```

第一版可以只读取 batch run reports + queue files，不要求所有历史 artifact 都存在。

---

## 指标定义

### Pipeline Volume

```yaml
total_queue_items: 所有 queue 文件中的条目总数
eligible_items: ingestion_status=queued 且 recommended_next_step=transcript_ingestion 的条目数
skipped_items: 被 skip 的条目数（pending_user_lookup / source_authority_not_allowed 等）
processed_items: 实际进入 YT-02 的条目数
blocked_items: YT-02 blocked 的条目数
success_items: YT-02 成功的条目数
```

### Transcript Quality

```yaml
transcript_success_count: 成功生成 transcript_raw.md 的数量
transcript_blocked_count: 生成 blocked report 的数量
transcript_language_distribution: {en: N, zh-Hans: N, ...}
auto_caption_count: transcript_source=yt_dlp_subtitles 的数量
average_cleaned_word_count: 清洗后平均字数（从 transcript 文件估算）
```

### Claim Quality

```yaml
claims_total: 所有 claims 文件中的 claim 总数
claims_by_type: {build_up: N, set_piece: N, ...}
average_claims_per_successful_transcript: claims_total / transcript_success_count
high_confidence_claims: confidence=high 的数量
medium_confidence_claims: confidence=medium 的数量
low_confidence_claims: confidence=low 的数量
```

### Validation Quality

```yaml
validated_count: validation_status=validated 的数量
rejected_count: validation_status=rejected 的数量
needs_review_count: validation_status=needs_review 的数量
validated_rate: validated / total
rejected_rate: rejected / total
needs_review_rate: needs_review / total
reason_code_distribution: {rejected_fact_conflict: N, needs_review_tactical_interpretation: N, ...}
```

### Team Archive Value

```yaml
candidate_after_review_count: patch_recommendation=candidate_after_review 的数量
patch_proposal_count: patch_proposals/ 目录下的文件数
auto_applied_count: 实际写入 Team Archive 的 claims 数量
archive_write_rate: auto_applied / candidate_after_review
```

### Source Performance（每个频道）

```yaml
source_channel: 频道名
source_tier: tier_1 | tier_2
videos_seen: 处理过的视频数
transcript_success_rate: 成功 / 总数
average_claims_per_video: 平均每视频 claims 数
validated_rate: validated claims / total claims
candidate_after_review_rate: candidate_after_review / validated
blocked_rate: blocked / total
notes: 备注
recommendation: keep | watch | downgrade | exclude | needs_more_sample
```

---

## Recommendation 规则

```text
keep:
  - transcript_success_rate >= 0.8
  - average_claims_per_video >= 5
  - validated_rate >= 0.5
  - videos_seen >= 2

watch:
  - 有潜力但样本量不足（videos_seen < 2）
  - 或 needs_review_rate 偏高（> 0.6）

downgrade:
  - 反复 blocked（blocked_rate > 0.5）
  - 或 average_claims_per_video < 2
  - 或 validated_rate < 0.2

exclude:
  - 来源质量违规
  - 仅社交来源
  - 反复无法使用的 transcript
  - 误导性内容

needs_more_sample:
  - videos_seen < 2（默认）
```

**保守原则**：不因单个视频失败而永久降级 Tier 1/Tier 2 来源。

---

## 命令示例

```bash
./venv/bin/python src/skills/youtube-source-quality-dashboard/scripts/build_report.py

# 指定输出标签
./venv/bin/python src/skills/youtube-source-quality-dashboard/scripts/build_report.py \
  --label "2026-05-27"

# 指定最小样本量
./venv/bin/python src/skills/youtube-source-quality-dashboard/scripts/build_report.py \
  --min-sample 3
```

---

## 打包资源

- `AresVault/01_Governance/模板 - YouTube Source Quality Report v1.md` — 报告模板
- `AresVault/01_Governance/规范 - Ares Source Authority Matrix v1.md` — 来源权威性矩阵（LMW-131）
- `AresVault/01_Governance/规范 - Ares YouTube Channel Tier Registry v1.md` — 频道注册表（LMW-131）
