---
name: tactical-claim-validation
version: "1.0"
source: ares-osint-telemetry native
description: >
  战术 claims 验证 Skill（v1.0）。
  读取 transcript-to-tactical-claims 生成的 claims.md，
  对每个 claim 进行事实核查和战术解读评估，
  输出 validated / rejected / needs_review 结果到 validation/ 目录。
  本 Skill 只负责验证，不做 Team Archive patch，不创建 memory cards。
  对应工作流节点：YT-04（transcript-first 主路径）。
inputs:
  required:
    - claims_file: claims.md 文件路径（绝对路径或相对于 AresVault 的路径）
    - target_team: 目标球队名称（英文标准名）
  optional:
    - source_url: YouTube 视频 URL（可从 claims frontmatter 读取）
    - video_id: YouTube video_id（可从 claims frontmatter 读取）
    - target_league: 目标联赛
    - source_channel: 来源频道名称
    - validation_scope: 验证范围（all / factual_only / tactical_only，默认 all）
    - trusted_sources: 额外信任的来源列表
    - notes: 备注
outputs:
  - Validation Output（Markdown）→
      /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/validation/
      <date>_<team>_<channel>_<video_id>_validation.md
changelog:
  - v1.0 (2026-05-27): initial release，基于 LMW-127 定义的 YT-04 validation 规范
---

# Tactical Claim Validation Skill v1.0

## 执行模式说明

本 Skill 是一套**模型无关的 Agent 执行规范**。当你在 Antigravity / Kiro 中被用户通过 `@skill` 或文件路径方式调用时，请按照本文件的步骤逐一执行。

**你是 validation 引擎，不是 claim extraction 引擎。** 你的职责是：
1. 读取 claims.md 文件
2. 对每个 claim 进行事实核查（factual gate）和战术解读评估
3. 为每个 claim 输出 validated / rejected / needs_review 结论
4. 记录验证所用的证据来源
5. 将结构化验证结果写入 validation/ 目录

**严格边界**：
- ✅ 读取 claims.md
- ✅ 对每个 claim 进行事实核查
- ✅ 使用可靠外部来源验证事实性 claims
- ✅ 对战术解读 claims 输出 needs_review（证据不足时）
- ✅ 写入 validation/ 目录
- ❌ 不获取 YouTube transcript（这是 YT-02 的职责）
- ❌ 不提取新 claims（这是 YT-03 的职责）
- ❌ 不修改 Team Archive
- ❌ 不创建 memory cards
- ❌ 不生成 prematch 结论
- ❌ 不覆盖原始 claims 文件
- ❌ 不写入 notebooklm_outputs/
- ❌ 不静默将 claims 晋升为长期记忆

---

## AresVault 路径模型

```text
Vault Base: /Users/liumingwei/vaults/AresVault

1. Skill 代码（代码项目）
   src/skills/tactical-claim-validation/SKILL.md

2. 治理规则 / 模板（AresVault 01_Governance）
   01_Governance/规范 - Ares tactical claim validation rules v1.md
   01_Governance/模板 - YouTube tactical claim validation output v1.md

3. 输入（YT-03 输出）
   04_RAG_Raw_Data/youtube_tactical_sources/claims/
     <date>_<team>_<channel>_<video_id>_claims.md

4. 输出（YT-04 输出）
   04_RAG_Raw_Data/youtube_tactical_sources/validation/
     <date>_<team>_<channel>_<video_id>_validation.md
```

---

## 核心原则

1. **外部证据优先**。事实性 claims 必须用外部可靠来源验证，transcript 本身不足以验证事实。
2. **Factual gate 严格**。涉及比赛结果、球员状态、赛季数据等硬事实，必须核查官方来源。
3. **战术解读谨慎**。战术解读 claims 在证据不足时输出 `needs_review`，不强行 validated。
4. **记录证据链**。每个验证决定必须列出检查过的证据来源。
5. **Truth > Completeness**。宁可输出 needs_review，不输出无根据的 validated。

---

## Validation Status 与 Reason Code

### Status

| Status | 说明 |
|--------|------|
| `validated` | claim 经外部证据支持，事实正确 |
| `rejected` | claim 与外部证据冲突，事实错误 |
| `needs_review` | 证据不足或存在歧义，需要人工审查 |

### Reason Code

| Reason Code | 说明 |
|-------------|------|
| `validated_source_supported` | 外部可靠来源支持该 claim |
| `rejected_fact_conflict` | claim 与外部事实来源冲突 |
| `rejected_source_mismatch` | claim 的来源归属错误 |
| `needs_review_insufficient_evidence` | 无法找到足够的外部证据 |
| `needs_review_language_ambiguity` | transcript 语言/翻译存在歧义 |
| `needs_review_tactical_interpretation` | 战术解读 claim，需要专业判断 |
| `needs_review_source_quality` | 来源质量不足以支持 validated |

---

## 来源优先级（Source Priority）

验证事实性 claims 时，按以下优先级使用来源：

1. **官方联赛/赛事来源**（Premier League 官网、UEFA 等）
2. **官方俱乐部来源**（俱乐部官网、官方社交媒体）
3. **官方比赛报告/数据提供商**（Opta、StatsBomb 等）
4. **可靠统计/足球数据来源**（FBref、Understat、WhoScored 等）
5. **可靠战术分析来源**（The Athletic、The Coaches' Voice 等）
6. **原始 transcript 来源**（只能证明 claim 被说出，不能验证事实）

---

## Claim 分类与验证策略

### 事实性 Claims（需要外部证据）

以下 claim_type 包含可验证的硬事实，必须用外部来源核查：

| Claim Type | 需要核查的事实 |
|-----------|--------------|
| `team_identity` | 赛季成绩、联赛排名、冠军归属 |
| `formation_shape` | 实际使用的阵型（可用 FBref/Opta 核查） |
| `player_role` | 球员实际上场位置和角色 |
| `set_piece` | 定位球数据（角球进球数等） |

**关键 Factual Gate**：
- 任何涉及"赢得联赛冠军"、"联赛排名"、"赛季成绩"的 claim，必须核查官方来源
- 2024/25 英超联赛冠军：**Liverpool**（非 Arsenal）
- 如果 claim 声称 Arsenal 赢得 2024/25 英超冠军，必须输出 `rejected_fact_conflict`

### 战术解读 Claims（谨慎验证）

以下 claim_type 主要是分析师解读，难以用外部来源直接验证：

| Claim Type | 验证策略 |
|-----------|---------|
| `coach_principle` | 核查教练公开言论/采访，否则 `needs_review_tactical_interpretation` |
| `pressing` | 可用 pressing 数据（PPDA 等）部分验证 |
| `build_up` | 需要战术分析来源支持 |
| `transition` | 需要战术分析来源支持 |
| `opponent_specific` | 需要具体比赛数据支持 |

---

## 执行流程（5 Phases）

### Phase 1: 读取与验证 claims 文件

1. 读取 `claims_file` 指定的 markdown 文件
2. 解析 YAML frontmatter，提取：
   - `video_id`、`source_url`、`target_team`、`target_league`
   - `source_channel`、`language`、`total_claims`
   - `source_authority`（必须是 `extracted_claims`）
   - `downstream_allowed`（必须包含 `validation`）
3. 解析所有 Claim 块，提取每个 claim 的：
   - `claim_id`、`claim_type`、`claim_text`、`evidence_quote`、`confidence`
4. 验证失败条件（触发 blocked report）：
   - 文件不存在
   - frontmatter 缺失或无效
   - `source_authority` 不是 `extracted_claims`
   - `downstream_allowed` 不包含 `validation`
   - 无法解析任何 claim_id

### Phase 2: 事实性 Claims 核查（Factual Gate）

对每个 claim 进行事实核查：

**步骤**：
1. 判断 claim 是否包含可验证的硬事实（赛季成绩、球员状态、比赛数据等）
2. 如果是，使用 `search_web` 或已知事实核查
3. 记录检查过的来源（source_name、source_url、evidence_summary）
4. 根据证据输出 validated / rejected / needs_review

**关键事实核查清单**：
- 2024/25 英超冠军 → Liverpool（官方来源：premierleague.com）
- 如果 claim 声称 Arsenal 赢得 2024/25 英超冠军 → `rejected_fact_conflict`

### Phase 3: 战术解读 Claims 评估

对战术解读 claims（coach_principle、pressing、build_up 等）：

1. 检查是否有可靠的战术分析来源支持
2. 检查教练公开言论是否与 claim 一致
3. 评估 transcript 语言质量（中文自动字幕 → confidence penalty）
4. 输出：
   - 有可靠来源支持 → `validated_source_supported`（confidence: medium）
   - 无足够外部证据 → `needs_review_tactical_interpretation`
   - 语言歧义明显 → `needs_review_language_ambiguity`

### Phase 4: 构建验证结果

为每个 claim 构建验证结果：

```yaml
claim_id: <原始 claim_id>
claim_type: <原始 claim_type>
claim_text: <原始 claim_text>
validation_status: validated | rejected | needs_review
validation_reason_code: <reason code>
validation_summary: <一句话说明验证结论>
evidence_checked:
  - source_name: <来源名称>
    source_url: <来源 URL 或 "N/A">
    evidence_summary: <证据摘要>
conflict_notes: <冲突说明，如无则 "none">
confidence_after_validation: low | medium | high
team_archive_patch_allowed: false
team_archive_patch_recommendation: none | candidate_after_review
```

**`team_archive_patch_recommendation` 规则**：
- `validated` + `confidence_after_validation: medium/high` → `candidate_after_review`
- `rejected` → `none`
- `needs_review` → `none`（等待人工审查后再决定）

### Phase 5: 输出验证文件

写入 `validation/` 目录，文件名：
```
<date>_<team>_<channel>_<video_id>_validation.md
```

---

## 输出文件格式规范

```markdown
---
source_kind: tactical_claim_validation
source_claims: <claims.md 文件名>
video_id: <video_id>
source_url: <YouTube URL>
target_team: <队名>
target_league: <联赛>
source_channel: <频道>
language: <transcript 语言>
validation_skill: tactical-claim-validation v1.0
validated_at: <ISO 8601>
total_claims: <数量>
validation_summary:
  validated: <数量>
  rejected: <数量>
  needs_review: <数量>
reason_code_summary:
  validated_source_supported: <数量>
  rejected_fact_conflict: <数量>
  needs_review_tactical_interpretation: <数量>
  needs_review_language_ambiguity: <数量>
  # ... 其他
team_archive_patch_allowed: false
downstream_allowed:
  - team_archive_patch_proposal
downstream_forbidden:
  - direct_team_archive_patch
  - prematch_conclusion
---

# Tactical Claim Validation — <target_team> — <source_channel> — <video_id>

## Source

| Field | Value |
|-------|-------|
| Claims File | <source_claims> |
| Video URL | <source_url> |
| Channel | <source_channel> |
| Target Team | <target_team> |
| Validated At | <validated_at> |

## ⚠️ Boundary Notice

> **This is a validation report. Validated claims are candidates for review, not approved for Team Archive patch.**
>
> - ✅ Allowed as input for: Team Archive patch proposal (YT-05)
> - ❌ Must NOT directly patch Team Archive
> - ❌ Must NOT be treated as verified tactical memory without YT-05 review

## Validation Summary

| Status | Count |
|--------|-------|
| validated | <N> |
| rejected | <N> |
| needs_review | <N> |
| **Total** | **<N>** |

---

## Validation 001 — <claim_id>

**Claim Type**: `<claim_type>`
**Original Confidence**: `<confidence>`
**Validation Status**: `<validated|rejected|needs_review>`
**Reason Code**: `<reason_code>`

**Claim**: <claim_text>

**Validation Summary**: <一句话说明>

**Evidence Checked**:
- **<source_name>**: <evidence_summary> ([link](<source_url>))

**Conflict Notes**: <冲突说明或 "none">

**Confidence After Validation**: `<low|medium|high>`
**Team Archive Patch Allowed**: `false`
**Patch Recommendation**: `<none|candidate_after_review>`

---
```

---

## 命令示例

### Agent 调用（推荐）

在 Antigravity / Kiro 中加载本 SKILL.md，然后提供：

```
claims_file: /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/claims/2026-05-22_Arsenal_Tifo_GxvSAS97L9c_claims.md
target_team: Arsenal
```

### Helper Script 调用

```bash
./venv/bin/python src/skills/tactical-claim-validation/scripts/validate_claims.py \
  --claims "/Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/claims/2026-05-22_Arsenal_Tifo_GxvSAS97L9c_claims.md" \
  --team "Arsenal" \
  --validation-json /path/to/validation_results.json \
  --write-output
```

---

## 验证清单（v1.0）

交付前确认：
- [ ] claims_file 已读取且 frontmatter 有效
- [ ] `downstream_allowed` 包含 `validation`
- [ ] 每个 claim 都有 validation_status
- [ ] 每个 validation 都有 validation_reason_code
- [ ] 每个 validation 都有 evidence_checked（至少一条）
- [ ] 事实性 claims 使用了外部来源（非仅 transcript）
- [ ] 涉及 Arsenal 2024/25 冠军的 claims 已被 `rejected_fact_conflict`
- [ ] 未修改原始 claims 文件
- [ ] 未修改 Team Archive
- [ ] 输出路径在 validation/ 目录下

---

## 打包资源

- `AresVault/01_Governance/规范 - Ares tactical claim validation rules v1.md` — 治理规范
- `AresVault/01_Governance/模板 - YouTube tactical claim validation output v1.md` — 输出模板
- `src/skills/tactical-claim-validation/scripts/validate_claims.py` — 可选 helper script
- `src/skills/transcript-to-tactical-claims/SKILL.md` — 上游 YT-03 Skill
