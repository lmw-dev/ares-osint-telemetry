---
name: transcript-to-tactical-claims
version: "1.0"
source: ares-osint-telemetry native
description: >
  YouTube transcript 战术 claims 提取 Skill（v1.0）。
  读取 youtube-transcript-ingestion 生成的 transcript_raw.md，
  提取结构化战术 claims 并写入 claims/ 目录。
  本 Skill 只负责 claim extraction，不做 validation，不做 Team Archive patch。
  对应工作流节点：YT-03（transcript-first 主路径）。
inputs:
  required:
    - transcript_file: transcript_raw.md 文件路径（绝对路径或相对于 AresVault 的路径）
    - target_team: 目标球队名称（英文标准名）
  optional:
    - source_url: YouTube 视频 URL（可从 transcript frontmatter 读取）
    - video_id: YouTube video_id（可从 transcript frontmatter 读取）
    - target_league: 目标联赛
    - source_channel: 来源频道名称
    - language: transcript 语言（可从 frontmatter 读取）
    - claim_scope: 提取范围（all / tactical_only / coach_principle_only，默认 all）
    - max_claims: 最大 claim 数量（默认无限制，建议 ≤30）
    - notes: 备注
outputs:
  - Claims Output（Markdown）→
      /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/claims/
      <date>_<team>_<channel>_<video_id>_claims.md
  - Blocked/Empty Report（Markdown，仅提取失败或无 claims 时）→
      /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/claims/
      <date>_<team>_<channel>_<video_id>_claims_blocked.md
changelog:
  - v1.0 (2026-05-27): initial release，基于 LMW-126 定义的 YT-03 claim extraction 规范
---

# Transcript-to-Tactical-Claims Skill v1.0

## 执行模式说明

本 Skill 是一套**模型无关的 Agent 执行规范**。当你在 Antigravity / Kiro 中被用户通过 `@skill` 或文件路径方式调用时，请按照本文件的步骤逐一执行。

**你是 claim extraction 引擎，不是 validation 引擎。** 你的职责是：
1. 读取 transcript_raw.md 文件
2. 清洗 VTT/SRT 噪音，提取可读文本
3. 从文本中识别战术 claims，每个 claim 必须有直接的 transcript 证据引用
4. 将结构化 claims 写入 AresVault claims/ 目录
5. 无法提取时生成 blocked/empty report

**严格边界**：
- ✅ 读取 transcript_raw.md
- ✅ 清洗 VTT/SRT 格式噪音
- ✅ 提取战术 claims（每个 claim 必须有 evidence_quote）
- ✅ 写入 claims/ 目录
- ✅ 无法提取时生成 blocked/empty report
- ❌ 不获取 YouTube transcript（这是 YT-02 的职责）
- ❌ 不下载视频/音频
- ❌ 不验证 claims（这是 YT-04 的职责）
- ❌ 不修改 Team Archive
- ❌ 不创建 memory cards
- ❌ 不生成 prematch 结论
- ❌ 不写入 validation/、notebooklm_outputs/
- ❌ 不覆盖原始 transcript 文件
- ❌ 不在证据不足时发明 claims

---

## AresVault 路径模型

```text
Vault Base: /Users/liumingwei/vaults/AresVault

1. Skill 代码（代码项目）
   src/skills/transcript-to-tactical-claims/SKILL.md

2. 治理规则 / 模板（AresVault 01_Governance）
   01_Governance/规范 - Ares YouTube transcript claim extraction rules v1.md
   01_Governance/模板 - YouTube tactical claims output v1.md

3. 输入（YT-02 输出）
   04_RAG_Raw_Data/youtube_tactical_sources/transcripts/
     <date>_<team>_<channel>_<video_id>_transcript_raw.md

4. 输出（YT-03 输出）
   04_RAG_Raw_Data/youtube_tactical_sources/claims/
     <date>_<team>_<channel>_<video_id>_claims.md
     <date>_<team>_<channel>_<video_id>_claims_blocked.md（仅失败时）
```

---

## 核心原则

1. **Evidence-first**。每个 claim 必须有直接的 transcript 文本证据，不得从通用足球知识推断。
2. **Atomic claims**。一个 claim = 一个战术命题，不合并多个观察。
3. **Fewer, better**。宁可输出 5 个高质量 claims，不输出 20 个弱 claims。
4. **Language awareness**。中文/自动字幕的 claim 需降低 confidence，并在 metadata 中记录。
5. **Truth > Completeness**。无法找到足够证据时，输出 blocked/empty report，不发明内容。

---

## Claim Type 受控词汇表

```text
team_identity       — 球队整体战术身份/风格定义
formation_shape     — 阵型/站位结构
pressing            — 高位逼抢/中场逼抢行为
build_up            — 后场组织/出球模式
chance_creation     — 进攻创造/机会制造模式
defensive_block     — 防守阵型/低位防守行为
transition          — 攻守转换行为
player_role         — 球员角色/位置使用
coach_principle     — 教练特定战术原则/哲学
set_piece           — 定位球模式（仅当 transcript 明确提及）
opponent_specific   — 针对特定对手的战术观察
other_tactical      — 其他战术相关观察（不适合以上分类时）
```

---

## 执行流程（5 Phases）

### Phase 1: 读取与验证 transcript 文件

1. 读取 `transcript_file` 指定的 markdown 文件
2. 解析 YAML frontmatter，提取：
   - `video_id`、`source_url`、`target_team`、`target_league`
   - `source_channel`、`language`、`transcript_source`
   - `source_authority`（必须是 `raw_transcript`）
   - `downstream_allowed`（必须包含 `claim_extraction`）
3. 验证失败条件（触发 blocked report）：
   - 文件不存在
   - frontmatter 缺失或无效
   - `source_authority` 不是 `raw_transcript`
   - `downstream_allowed` 不包含 `claim_extraction`
   - transcript 正文为空

### Phase 2: 文本清洗

VTT/SRT 格式的 transcript 包含大量噪音，必须先清洗：

**清洗规则**：
```
1. 移除 VTT 头部（WEBVTT、NOTE 行）
2. 移除时间戳行（格式：HH:MM:SS.mmm --> HH:MM:SS.mmm）
3. 移除内联时间标记（格式：<HH:MM:SS.mmm><c>...</c>）
4. 移除重复行（VTT 格式中同一句话会出现多次）
5. 移除空行
6. 合并连续的短句为完整句子
7. 保留时间戳参考（用于 evidence_location）
```

**清洗后评估**：
- 如果清洗后文本 < 100 字，触发 blocked report（`transcript_too_short`）
- 如果文本明显是纯音乐/非语言内容，触发 blocked report（`no_speech_content`）
- 如果语言无法识别，记录 `language: unknown`，降低所有 claims 的 confidence

### Phase 3: 战术内容识别

在清洗后的文本中，识别以下类型的战术相关内容：

**高优先级（直接战术描述）**：
- 阵型描述（4-3-3、4-2-3-1 等）
- 具体战术行为（高位逼抢、边路进攻、后场出球等）
- 球员角色描述（单后腰、倒三角中场等）
- 教练战术原则（明确引用教练的战术理念）

**中优先级（战术观察）**：
- 比赛模式描述（控球、反击等）
- 防守组织描述
- 进攻套路描述

**低优先级（需要推断）**：
- 从比赛结果推断的战术（confidence: low，标注 inference）
- 模糊的战术描述（confidence: low）

**排除**：
- 纯情绪/球迷评论（"太棒了"、"太差了"）
- 转会/人员新闻（不是战术 claim）
- 历史回顾（除非明确描述战术模式）
- 无法找到 transcript 证据的推断

### Phase 4: Claim 结构化

每个 claim 必须包含以下字段：

```yaml
claim_id: <team>-<video_id>-<序号，如 001>
claim_type: <受控词汇表中的类型>
target_team: <英文标准队名>
claim_text: <一句话描述战术命题，中文或英文>
evidence_quote: <直接引用 transcript 原文，不得修改>
evidence_location: <时间戳或段落位置，如 "~16:27" 或 "transcript line ~4377">
source_file: <transcript_raw.md 文件名>
source_url: <YouTube URL>
video_id: <video_id>
source_channel: <频道名>
language: <transcript 语言>
transcript_source: <yt_dlp_subtitles 等>
confidence: low | medium | high
confidence_note: <降低 confidence 的原因，如 "auto-generated Chinese subtitles, translation quality uncertain">
requires_validation: true
validation_status: not_validated
team_archive_patch_allowed: false
```

**Confidence 评级标准**：

| 条件 | Confidence |
|------|-----------|
| 英文原声字幕 + 明确战术描述 | high |
| 英文原声字幕 + 需要一定推断 | medium |
| 自动生成字幕（任何语言）+ 明确战术描述 | medium |
| 中文/翻译字幕 + 明确战术描述 | medium |
| 自动生成字幕 + 需要推断 | low |
| 中文/翻译字幕 + 需要推断 | low |
| 任何字幕 + 高度推断 | low（标注 inference） |

### Phase 5: 输出文件生成

**有 claims 时**：生成 `_claims.md`（见下方格式规范）

**无 claims 或失败时**：生成 `_claims_blocked.md`（见下方 blocked report 规范）

---

## 输出文件格式规范

### Claims Output Markdown（`_claims.md`）

```markdown
---
source_kind: tactical_claims
source_transcript: <transcript_raw.md 文件名>
video_id: <video_id>
source_url: <YouTube URL>
target_team: <队名>
target_league: <联赛>
source_channel: <频道>
language: <语言>
transcript_source: <yt_dlp_subtitles 等>
extraction_skill: transcript-to-tactical-claims v1.0
extracted_at: <ISO 8601>
total_claims: <数量>
claims_by_type:
  team_identity: <数量>
  formation_shape: <数量>
  pressing: <数量>
  build_up: <数量>
  coach_principle: <数量>
  # ... 其他类型
source_authority: extracted_claims
profile_authority: false
requires_validation: true
validation_status: not_validated
team_archive_patch_allowed: false
downstream_allowed:
  - validation
downstream_forbidden:
  - direct_team_archive_patch
  - prematch_conclusion
---

# Tactical Claims — <target_team> — <source_channel> — <video_id>

## Source

| Field | Value |
|-------|-------|
| Transcript File | <source_transcript> |
| Video URL | <source_url> |
| Channel | <source_channel> |
| Target Team | <target_team> |
| Language | <language> |
| Extracted At | <extracted_at> |

## ⚠️ Boundary Notice

> **These are unvalidated tactical claims extracted from raw transcript.**
>
> - ✅ Allowed as input for: validation (YT-04)
> - ❌ Must NOT directly patch Team Archive
> - ❌ Must NOT be treated as verified tactical memory
> - ❌ Must NOT be used to generate prematch conclusions without YT-04 validation

## Claims Summary

Total: <N> claims | High confidence: <N> | Medium: <N> | Low: <N>

---

## Claim 001

**Type**: `<claim_type>`
**Confidence**: `<high|medium|low>`
**Team**: <target_team>

**Claim**: <claim_text>

**Evidence**:
> "<evidence_quote>"

**Location**: <evidence_location>

**Metadata**:
- `requires_validation: true`
- `validation_status: not_validated`
- `team_archive_patch_allowed: false`
- `confidence_note`: <confidence_note>

---

## Claim 002
...
```

---

## Blocked/Empty Report 格式规范

```markdown
---
source_kind: claims_blocked
source_transcript: <transcript_raw.md 文件名>
video_id: <video_id>
target_team: <队名>
blocked_at: <ISO 8601>
failure_reason: <原因代码>
status: blocked | empty
---

# Claims Blocked/Empty — <target_team> — <source_channel> — <video_id>

## Summary

| Field | Value |
|-------|-------|
| Source Transcript | <source_transcript> |
| Failure Reason | <failure_reason> |
| Status | <blocked/empty> |
| Blocked At | <blocked_at> |

## Failure Details

<具体原因描述>

## Manual Review Recommended

<是否建议人工审查>

## Next Suggested Action

<建议的下一步操作>

> Truth > Completeness. No claims were invented.
```

**Blocked 原因代码**：

| 代码 | 说明 |
|------|------|
| `transcript_file_missing` | transcript 文件不存在 |
| `invalid_frontmatter` | frontmatter 缺失或无效 |
| `claim_extraction_not_allowed` | downstream_allowed 不包含 claim_extraction |
| `empty_transcript` | transcript 正文为空 |
| `transcript_too_short` | 清洗后文本过短（< 100 字） |
| `no_speech_content` | 无语音内容（纯音乐等） |
| `no_tactical_content` | 无战术相关内容 |
| `evidence_ungroundable` | 无法为 claims 找到 transcript 证据 |
| `language_too_noisy` | 语言/翻译质量过低，无法可靠提取 |

---

## 命令示例

### Agent 调用（推荐）

在 Antigravity / Kiro 中加载本 SKILL.md，然后提供：

```
transcript_file: /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/transcripts/2026-05-22_Arsenal_Tifo_GxvSAS97L9c_transcript_raw.md
target_team: Arsenal
```

### Helper Script 调用

```bash
./venv/bin/python src/skills/transcript-to-tactical-claims/scripts/extract_claims.py \
  --transcript "/Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/transcripts/2026-05-22_Arsenal_Tifo_GxvSAS97L9c_transcript_raw.md" \
  --team "Arsenal" \
  --max-claims 20
```

---

## 验证清单（v1.0）

交付前确认：
- [ ] transcript_file 已读取且 frontmatter 有效
- [ ] `downstream_allowed` 包含 `claim_extraction`
- [ ] VTT/SRT 噪音已清洗
- [ ] 每个 claim 有直接的 evidence_quote
- [ ] 每个 claim 有 evidence_location（时间戳或行号）
- [ ] claim_type 使用受控词汇表
- [ ] confidence 评级符合标准
- [ ] 自动字幕/翻译字幕已在 confidence_note 中说明
- [ ] 未做 validation
- [ ] 未修改 Team Archive
- [ ] 输出路径在 claims/ 目录下
- [ ] 无法提取时生成了 blocked/empty report

---

## 打包资源

- `AresVault/01_Governance/规范 - Ares YouTube transcript claim extraction rules v1.md` — 治理规范
- `AresVault/01_Governance/模板 - YouTube tactical claims output v1.md` — 输出模板
- `src/skills/transcript-to-tactical-claims/scripts/extract_claims.py` — 可选 helper script（文本清洗 + 格式化）
- `src/skills/youtube-transcript-ingestion/SKILL.md` — 上游 YT-02 Skill
