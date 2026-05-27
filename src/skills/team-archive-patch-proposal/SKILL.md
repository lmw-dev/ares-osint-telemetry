---
name: team-archive-patch-proposal
version: "1.0"
source: ares-osint-telemetry native
description: >
  Team Archive patch proposal 生成 Skill（v1.0）。
  读取 tactical-claim-validation 生成的 validation.md，
  筛选 validated + candidate_after_review 的 claims，
  生成结构化的 Team Archive patch proposal 到 patch_proposals/ 目录。
  本 Skill 只生成 proposal，不直接修改 Team Archive，不创建 memory cards。
  对应工作流节点：YT-05（transcript-first 主路径）。
inputs:
  required:
    - validation_file: validation.md 文件路径（绝对路径或相对于 AresVault 的路径）
    - target_team: 目标球队名称（英文标准名）
    - target_archive_path: Team Archive 文件路径（用于 proposal 中的引用，不直接修改）
  optional:
    - target_section: 建议写入的 Team Archive 章节（默认 tactical_analysis_candidates）
    - source_url: YouTube 视频 URL（可从 validation frontmatter 读取）
    - video_id: YouTube video_id（可从 validation frontmatter 读取）
    - source_channel: 来源频道名称
    - proposal_scope: 提案范围（all / build_up_only / set_piece_only 等，默认 all）
    - notes: 备注
outputs:
  - Patch Proposal（Markdown）→
      /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/patch_proposals/
      <date>_<team>_<channel>_<video_id>_team_archive_patch_proposal.md
changelog:
  - v1.0 (2026-05-27): initial release，基于 LMW-128 定义的 YT-05 patch proposal 规范
---

# Team Archive Patch Proposal Skill v1.0

## 执行模式说明

本 Skill 是一套**模型无关的 Agent 执行规范**。当你在 Antigravity / Kiro 中被用户通过 `@skill` 或文件路径方式调用时，请按照本文件的步骤逐一执行。

**你是 patch proposal 生成引擎，不是 Team Archive 修改引擎。** 你的职责是：
1. 读取 validation.md 文件
2. 筛选 `validation_status: validated` + `team_archive_patch_recommendation: candidate_after_review` 的 claims
3. 为每个候选 claim 生成结构化的 patch proposal 条目
4. 将 proposal 写入 patch_proposals/ 目录
5. 不直接修改 Team Archive

**严格边界**：
- ✅ 读取 validation.md
- ✅ 筛选 validated + candidate_after_review 的 claims
- ✅ 生成结构化 patch proposal
- ✅ 写入 patch_proposals/ 目录
- ❌ 不直接修改 Team Archive（`02_Team_Archives/<team>.md`）
- ❌ 不创建 memory cards
- ❌ 不生成 prematch 结论
- ❌ 不重新验证 claims
- ❌ 不从 transcript 提取新 claims
- ❌ 不修改 validation 文件
- ❌ 不包含 rejected 或 needs_review 的 claims

---

## AresVault 路径模型

```text
Vault Base: /Users/liumingwei/vaults/AresVault

1. Skill 代码（代码项目）
   src/skills/team-archive-patch-proposal/SKILL.md

2. 治理规则 / 模板（AresVault 01_Governance）
   01_Governance/规范 - Ares Team Archive patch proposal rules v1.md
   01_Governance/模板 - Team Archive patch proposal output v1.md

3. 输入（YT-04 输出）
   04_RAG_Raw_Data/youtube_tactical_sources/validation/
     <date>_<team>_<channel>_<video_id>_validation.md

4. 输出（YT-05 输出）
   04_RAG_Raw_Data/youtube_tactical_sources/patch_proposals/
     <date>_<team>_<channel>_<video_id>_team_archive_patch_proposal.md

5. 目标（只读引用，不修改）
   02_Team_Archives/<league>/<team>.md
```

---

## 核心原则

1. **Proposal only**。本 Skill 只生成 proposal，不直接修改 Team Archive。
2. **Validated claims only**。只包含 `validation_status: validated` + `candidate_after_review` 的 claims。
3. **Human review gate**。每个 patch item 都有 `apply_allowed: false` + `requires_human_review: true`。
4. **Traceable**。每个 patch item 必须引用 claim_id、validation_file、source_url、video_id。
5. **Truth > Completeness**。宁可少提案，不提无根据的 patch。

---

## Patch Candidate 筛选规则

只包含满足以下所有条件的 claims：

```yaml
validation_status: validated
team_archive_patch_recommendation: candidate_after_review
team_archive_patch_allowed: false  # 确认未被直接 patch
```

排除以下 claims：
- `validation_status: rejected`
- `validation_status: needs_review`
- 没有 `evidence_checked` 的 claims
- 没有 `source_url` 或 `video_id` 的 claims
- 存在重大语言歧义的 claims（`confidence_after_validation: low` 且 reason_code 包含 `language_ambiguity`）
- 来源不可追溯的 claims

---

## Target Section 映射

根据 `claim_type` 建议写入 Team Archive 的目标章节：

| Claim Type | 建议 Target Section |
|-----------|-------------------|
| `build_up` | `tactical_analysis_candidates.build_up` |
| `defensive_block` | `tactical_analysis_candidates.defensive_shape` |
| `set_piece` | `tactical_analysis_candidates.set_pieces` |
| `player_role` | `tactical_analysis_candidates.player_roles` |
| `pressing` | `tactical_analysis_candidates.pressing` |
| `team_identity` | `tactical_analysis_candidates.team_identity` |
| `coach_principle` | `tactical_analysis_candidates.coach_principles` |
| `transition` | `tactical_analysis_candidates.transitions` |
| `chance_creation` | `tactical_analysis_candidates.chance_creation` |
| `formation_shape` | `tactical_analysis_candidates.formation` |
| `opponent_specific` | `tactical_analysis_candidates.opponent_specific` |
| `other_tactical` | `tactical_analysis_candidates.other` |

---

## 执行流程（4 Phases）

### Phase 1: 读取与验证 validation 文件

1. 读取 `validation_file` 指定的 markdown 文件
2. 解析 YAML frontmatter，提取：
   - `video_id`、`source_url`、`target_team`、`target_league`
   - `source_channel`、`language`、`total_claims`
   - `validation_summary`（validated/rejected/needs_review 数量）
3. 解析所有 Validation 块，提取每个 validation 的：
   - `claim_id`、`claim_type`、`validation_status`
   - `validation_reason_code`、`confidence_after_validation`
   - `team_archive_patch_recommendation`
   - `evidence_checked`、`validation_summary`
4. 验证失败条件（触发 blocked report）：
   - 文件不存在
   - frontmatter 缺失或无效
   - 无法解析任何 validation 条目

### Phase 2: 筛选 Patch Candidates

按筛选规则过滤，记录：
- 包含的 candidates（validated + candidate_after_review）
- 排除的 items 及原因

### Phase 3: 生成 Patch Proposal 条目

为每个 candidate 生成：

```yaml
patch_id: <team>-<video_id>-patch-<序号>
target_team: <队名>
target_archive_path: <Team Archive 文件路径>
target_section: <建议章节>
claim_id: <原始 claim_id>
claim_type: <claim_type>
proposed_text: <建议写入 Team Archive 的文本>
rationale: <为什么这条 claim 值得写入>
evidence_summary: <证据摘要>
source_url: <YouTube URL>
video_id: <video_id>
source_channel: <频道名>
validation_file: <validation.md 文件名>
validation_status: validated
confidence_after_validation: <low|medium|high>
patch_status: proposed
apply_allowed: false
requires_human_review: true
```

**`proposed_text` 格式**：
- 简洁、可直接写入 Team Archive 的文本
- 不超过 3 句话
- 包含来源引用（视频标题/频道/日期）
- 不包含主观评价

### Phase 4: 输出 Patch Proposal 文件

写入 `patch_proposals/` 目录，文件名：
```
<date>_<team>_<channel>_<video_id>_team_archive_patch_proposal.md
```

---

## 输出文件格式规范

```markdown
---
source_kind: team_archive_patch_proposal
source_validation: <validation.md 文件名>
video_id: <video_id>
source_url: <YouTube URL>
target_team: <队名>
target_league: <联赛>
source_channel: <频道>
target_archive_path: <Team Archive 路径>
proposal_skill: team-archive-patch-proposal v1.0
proposed_at: <ISO 8601>
total_candidates: <数量>
excluded_count: <数量>
patch_status: proposed
apply_allowed: false
requires_human_review: true
---

# Team Archive Patch Proposal — <target_team> — <source_channel> — <video_id>

## Source

| Field | Value |
|-------|-------|
| Validation File | <source_validation> |
| Video URL | <source_url> |
| Channel | <source_channel> |
| Target Team | <target_team> |
| Target Archive | <target_archive_path> |
| Proposed At | <proposed_at> |

## ⚠️ Review Gate

> **This is a patch proposal only. No changes have been made to Team Archive.**
>
> - `apply_allowed: false` — must not be applied without human review
> - `requires_human_review: true` — human must review each item before applying
> - Source authority: `secondary_synthesis` (not `profile_authority`)

## Proposal Summary

| Metric | Value |
|--------|-------|
| Total validation items | <N> |
| Candidates included | <N> |
| Items excluded | <N> |
| Exclusion reasons | <reasons> |

---

## Patch Item 001

**Patch ID**: `<patch_id>`
**Claim ID**: `<claim_id>`
**Claim Type**: `<claim_type>`
**Target Section**: `<target_section>`
**Confidence**: `<confidence_after_validation>`

**Proposed Text**:
> <proposed_text>

**Rationale**: <rationale>

**Evidence Summary**: <evidence_summary>

**Source**: [<source_channel> — <video_id>](<source_url>)

**Metadata**:
- `apply_allowed: false`
- `requires_human_review: true`
- `patch_status: proposed`
- `validation_file`: <validation_file>

---
```

---

## 命令示例

### Agent 调用（推荐）

在 Antigravity / Kiro 中加载本 SKILL.md，然后提供：

```
validation_file: /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/validation/2025-10-11_Arsenal_HALFSPACETHEORY_VHymL0kvIXQ_validation.md
target_team: Arsenal
target_archive_path: /Users/liumingwei/vaults/AresVault/02_Team_Archives/1_Top_Five_Europe/ENG_England/Arsenal.md
```

### Helper Script 调用

```bash
./venv/bin/python src/skills/team-archive-patch-proposal/scripts/generate_proposal.py \
  --validation "/path/to/validation.md" \
  --team "Arsenal" \
  --archive "/path/to/Arsenal.md" \
  --write-output
```

---

## 验证清单（v1.0）

交付前确认：
- [ ] validation_file 已读取且 frontmatter 有效
- [ ] 只包含 validated + candidate_after_review 的 claims
- [ ] 排除了 rejected 和 needs_review 的 claims
- [ ] 每个 patch item 有 claim_id、validation_file、source_url、video_id
- [ ] 每个 patch item 有 `apply_allowed: false` + `requires_human_review: true`
- [ ] 未修改 Team Archive
- [ ] 输出路径在 patch_proposals/ 目录下

---

## 打包资源

- `AresVault/01_Governance/规范 - Ares Team Archive patch proposal rules v1.md` — 治理规范
- `AresVault/01_Governance/模板 - Team Archive patch proposal output v1.md` — 输出模板
- `src/skills/team-archive-patch-proposal/scripts/generate_proposal.py` — 可选 helper script
- `src/skills/tactical-claim-validation/SKILL.md` — 上游 YT-04 Skill
