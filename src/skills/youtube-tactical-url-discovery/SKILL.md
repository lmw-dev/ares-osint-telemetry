---
name: youtube-tactical-url-discovery
version: "1.2"
source: ares-osint-telemetry native
description: >
  YouTube 战术视频 URL 发现与筛选 Skill（v1.2 — transcript-first routing）。
  用于从 YouTube 中发现、筛选和输出高质量战术分析 / 赛后战术复盘 / 教练体系解析视频 URL，
  供用户确认后进入 YT-02 youtube-transcript-ingestion 流程（transcript-first 主路径）。
  本 Skill 只处理 URL discovery，不解析视频内容，不调用 NotebookLM。
  NotebookLM 仅作为 optional secondary synthesis，不再是默认下游路由。
inputs:
  - target_team: 目标球队名称（英文标准名）
  - target_league: 目标联赛（EPL / La_liga / Serie_A / Bundesliga / Ligue_1）
  - coach_name: 主帅姓名（可选，用于教练体系搜索）
  - target_match: 目标比赛描述（可选，如 Arsenal vs Chelsea 2026-05-25）
  - search_focus: 搜索重点（tactical_analysis / coach_system / postmatch_review / set_piece / pressing）
  - reference_date: 参考日期（YYYY-MM-DD，用于时效性过滤）
  - max_candidates: 最大候选数量（默认 10，上限 20）
outputs:
  - Candidate Review（Markdown，Obsidian-editable）→
      /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/candidates/
      YYYY-MM-DD_{Team}_tactical_source_candidates.md
  - Candidate Data（JSON）→
      /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/candidates/
      YYYY-MM-DD_{Team}_tactical_source_candidates.json
  - Ingestion Queue（用户确认后生成，供 YT-02 消费）→
      /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/ingestion_queue/
      YYYY-MM-DD_{Team}_transcript_ingestion_queue.json
changelog:
  - v1.2 (2026-05-27): transcript-first routing; ingestion queue output for YT-02; NotebookLM demoted to optional secondary synthesis; new transcript-first metadata fields
  - v1.1 (2026-05-22): AresVault path alignment; source_kind / discovery_status fields; PENDING_USER_LOOKUP fallback; Obsidian-editable candidate table
  - v1.0 (2026-05-22): initial release
---

# YouTube Tactical URL Discovery V1.2

## 执行模式说明

本 Skill 是一套**模型无关的 Agent 执行规范**。当你在 Antigravity / Kiro 中被用户通过 `@skill` 或文件路径方式调用时，请按照本文件的步骤逐一执行。

**你是 URL 发现与筛选引擎，不是内容分析引擎。** 你的职责是：
1. 通过 `search_web` 搜索战术视频/文章来源
2. 按频道分层和视频筛选标准过滤候选
3. 输出 Obsidian-editable 候选清单到 AresVault，供用户手动确认 URL
4. 用户在 Obsidian 中确认 URL 后，生成 **transcript ingestion queue**（供 YT-02 消费）

**严格边界**：
- ✅ 搜索 YouTube URL 和战术文章 URL
- ✅ 基于标题和频道判断战术相关性
- ✅ 输出候选清单到 AresVault（Obsidian-editable）
- ✅ 生成 transcript ingestion queue（供 YT-02 消费）
- ❌ 不解析视频内容（不读字幕、不看视频）
- ❌ 不调用 NotebookLM（NotebookLM 是 optional secondary synthesis，不是默认路由）
- ❌ 不做 browser automation
- ❌ 不下载视频
- ❌ 不绕过 YouTube / Google 限制

---

## 下游路由（v1.2 更新）

```
旧路线（v1.1）：
  candidate YouTube URLs -> NotebookLM

新路线（v1.2）：
  team / coach / tactical topic
    -> candidate YouTube URLs（本 Skill 输出）
    -> ingestion queue
    -> YT-02 youtube-transcript-ingestion（主路径）
    -> [optional] YT-02b NotebookLM secondary synthesis（仅在 transcript 不可用时）
```

**NotebookLM 的定位（v1.2）**：
- 不再是默认下游路由
- 仅作为 optional secondary synthesis（YT-02b）
- 仅在 transcript 不可获取时考虑
- 每个候选条目有 `notebooklm_secondary_synthesis_allowed` 字段标注

---

## AresVault 路径模型

```text
Vault Base: /Users/liumingwei/vaults/AresVault

1. Skill 代码（代码项目）
   src/skills/youtube-tactical-url-discovery/SKILL.md

2. 治理规则 / 模板（AresVault 01_Governance）
   01_Governance/规范 - Ares YouTube 战术情报摄取与候选记忆规则 v1.md
   01_Governance/模板 - YouTube 战术源候选清单 v1.md
   01_Governance/规范 - Ares Transcript-first YouTube 战术摄取工作流 v1.md

3. 候选源（人工查看、补 URL）
   04_RAG_Raw_Data/youtube_tactical_sources/candidates/
     YYYY-MM-DD_<Team>_tactical_source_candidates.md   ← Obsidian-editable
     YYYY-MM-DD_<Team>_tactical_source_candidates.json

4. Ingestion Queue（传给 YT-02，v1.2 新增）
   04_RAG_Raw_Data/youtube_tactical_sources/ingestion_queue/
     YYYY-MM-DD_<Team>_transcript_ingestion_queue.json
   YT-02 消费此文件，按 priority 顺序处理

5. [Optional] NotebookLM 输出（仅 secondary synthesis）
   04_RAG_Raw_Data/youtube_tactical_sources/notebooklm_outputs/
     YYYY-MM-DD_<Team>_notebooklm_tactical_synthesis.md
```

---

## 核心原则

1. **URL 发现 ≠ 内容分析**。本 Skill 只负责找到值得进入 YT-02 transcript ingestion 的 URL，内容分析由后续 skill 完成。
2. **频道可信度优先**。Tier 1 频道的视频默认进入候选池，Tier 2 需要标题验证，excluded 频道直接跳过。
3. **Human confirmation gate 不可绕过**。Ares 推荐来源线索，用户在 Obsidian 中确认具体 URL 后才进入 ingestion queue。
4. **支持 PENDING_USER_LOOKUP**。当 search_web 无法直接返回 YouTube URL 时，输出来源线索 + 搜索建议，用户手动补充 URL。
5. **Transcript-first 优先**。优先选择有字幕的视频（`likely_has_subtitles: true`），提高 YT-02 成功率。

---

## Source Kind（来源类型）

```yaml
source_kind:
  - youtube_video       # 直接 YouTube 视频 URL
  - tactical_article    # 战术分析文章（如 coachesvoice.com、breakingthelines.com）
  - channel_lead        # 频道线索（已知频道有相关内容，但具体视频 URL 待用户查找）
  - search_lead         # 搜索线索（搜索结果指向相关内容，但 URL 需要用户确认）
```

## Discovery Status（发现状态）

```yaml
discovery_status:
  - direct_source_url_found           # 直接找到可用 URL（youtube_video 或 tactical_article）
  - source_lead_found_pending_user_lookup  # 找到来源线索，URL 需要用户手动查找
  - no_viable_source_found            # 未找到可用来源
```

---

## 频道分层（Channel Tier System）

详细频道列表见：`docs/agents/tactical_youtube_channel_whitelist.md`

### Tier 1 — Tactical Candidate Sources
- 以深度战术分析为核心内容
- 视频标题通常包含战术关键词
- `confidence` 默认为 `high`
- 典型：Tifo Football、The Coaches' Voice、Breaking The Lines、Spielverlagerung、Total Football Analysis、HALF-SPACE THEORY

### Tier 2 — Context / Postmatch / Sentiment Sources
- 内容混合：战术分析 + 新闻 + 评论
- 需要标题关键词验证才进入候选池
- `confidence` 默认为 `medium`

### Excluded / Low-Trust Sources
- 博彩预测、转会谣言、球迷情绪、纯集锦
- 直接跳过，记录 `exclude_reason`

---

## 视频保留标准（Retention Criteria）

| 类型 | 关键词示例 |
|---|---|
| `tactical_analysis` | "tactical analysis", "tactics explained", "how X plays" |
| `match_tactical_breakdown` | "tactical breakdown", "match analysis", "how X beat Y" |
| `postmatch_tactical_review` | "postmatch analysis", "tactical review", "what went wrong" |
| `coach_system_analysis` | "Arteta system", "Guardiola philosophy", "pressing system" |
| `role_map_analysis` | "role map", "positional play", "half-space", "third man" |
| `pressing_analysis` | "pressing triggers", "high press", "gegenpressing" |
| `build_up_analysis` | "build-up play", "ball progression", "goalkeeper distribution" |
| `transition_analysis` | "counter-attack", "transition", "defensive shape", "low block" |
| `set_piece_analysis` | "set piece", "corner routine", "free kick", "dead ball" |

## 视频排除标准（Exclusion Criteria）

| 类型 | 关键词示例 |
|---|---|
| `highlights_only` | "highlights", "goals", "best moments" |
| `betting_prediction` | "prediction", "betting tips", "odds" |
| `transfer_rumour` | "transfer news", "signing", "rumour" |
| `fan_rant` | "rant", "reaction", "angry fan" |
| `pure_news_gossip` | "news", "latest", "breaking" |
| `no_tactical_claim` | 标题无战术关键词，且频道为 Tier 2 |

---

## 执行流程（4 Phases）

### Phase 1: 参数解析与搜索策略制定

1. 从用户输入中提取 `target_team`、`target_league`、`coach_name`、`search_focus`、`reference_date`
2. 根据 `search_focus` 制定搜索查询
3. 确认输出路径（AresVault candidates 目录 + ingestion_queue 目录）

### Phase 2: 搜索与候选收集

**搜索查询模板**：
```
# tactical_analysis
search_web("{team} tactical analysis YouTube {year}")
search_web("{team} tactics {coach_name} YouTube {year}")

# coach_system
search_web("{coach_name} system analysis tactics YouTube")
search_web("{coach_name} pressing build-up philosophy YouTube")

# postmatch_review
search_web("{team} tactical breakdown {match_description} YouTube")
search_web("{team} postmatch analysis {year} YouTube")

# set_piece / pressing
search_web("{team} set piece analysis YouTube {year}")
search_web("{team} pressing system {coach_name} YouTube")
```

**⚠️ Fallback 策略（当 search_web 无法返回直接 YouTube URL 时）**：

```
Fallback Step 1: 识别来源线索
  → 从搜索结果中识别 Tier 1 频道的内容（文章/网站 URL）
  → 记录频道名称和参考文章 URL
  → 标注 source_kind: channel_lead 或 tactical_article

Fallback Step 2: 输出 PENDING_USER_LOOKUP
  → video_id: "PENDING_USER_LOOKUP"
  → discovery_status: source_lead_found_pending_user_lookup
  → action_required: "在 YouTube 搜索 '{channel} {team} {topic}'"

Fallback Step 3: 战术文章作为直接来源
  → 若搜索结果返回 The Coaches' Voice / Breaking The Lines 等 Tier 1 网站文章
  → 标注 source_kind: tactical_article
  → discovery_status: direct_source_url_found
  → 这类文章可进入 NotebookLM secondary synthesis（YT-02b），但不进入 transcript ingestion queue
```

### Phase 3: 筛选与评级

1. **频道分层判断**：对照白名单确定 `channel_tier`
2. **保留/排除判断**：对照保留标准和排除标准
3. **confidence 评级**：Tier 1 + 标题匹配 → `high`；Tier 2 + 战术关键词 → `medium`；模糊 → `low`
4. **discovery_status 标注**：按上方 fallback 策略标注
5. **transcript-first 评估**：
   - `likely_has_subtitles`：Tier 1 英文频道默认 `true`；中文频道或非英语频道标注 `uncertain`
   - `language_hint`：根据频道和标题推断语言
   - `tactical_density_estimate`：根据标题关键词密度评估（high / medium / low）
6. **recommended_next_step 判断（v1.2 更新）**：
   - `direct_source_url_found` + `confidence=high` + `likely_has_subtitles=true` → `transcript_ingestion`
   - `direct_source_url_found` + `confidence=high` + `likely_has_subtitles=false` → `transcript_ingestion_or_notebooklm`
   - `source_lead_found_pending_user_lookup` → `user_lookup_then_transcript_ingestion`
   - `confidence=low` → `skip`

### Phase 4: 输出到 AresVault

1. 生成 Obsidian-editable 候选清单（MD）和结构化数据（JSON）
2. 写入 `04_RAG_Raw_Data/youtube_tactical_sources/candidates/`
3. **等待用户在 Obsidian 中确认 URL**
4. 用户确认后，生成 **transcript ingestion queue** 写入 `ingestion_queue/`

---

## Candidate Video Output Schema（v1.2）

```json
{
  "source_url": "YouTube 视频 URL 或文章 URL 或频道 URL",
  "video_id": "YouTube video_id 或 PENDING_USER_LOOKUP",
  "source_kind": "youtube_video | tactical_article | channel_lead | search_lead",
  "discovery_status": "direct_source_url_found | source_lead_found_pending_user_lookup | no_viable_source_found",
  "title": "视频/文章标题",
  "source_channel": "频道/网站名称",
  "channel_tier": "T1 | T2 | excluded",
  "source_date": "YYYY-MM-DD 或 unknown",
  "duration": "HH:MM:SS 或 unknown（仅 youtube_video）",
  "target_team": "目标球队",
  "target_league": "目标联赛",
  "coach_context": "相关教练（若适用）",
  "topic_tags": ["build_up", "set_piece", "pressing"],
  "source_type": "tactical_analysis | match_tactical_breakdown | postmatch_tactical_review | coach_system_analysis | role_map_analysis | pressing_analysis | build_up_analysis | transition_analysis | set_piece_analysis",
  "tactical_relevance_reason": "选择原因",
  "expected_claim_types": ["build_up", "defensive_block", "set_piece"],
  "tactical_claim_hint": "标题中暗示的战术主张",
  "confidence": "high | medium | low",
  "priority": 1,
  "ingestion_status": "queued | pending_user_lookup | skip",
  "likely_has_subtitles": true,
  "language_hint": "en | zh | unknown",
  "source_quality": "T1_tactical | T2_mixed | low_trust",
  "tactical_density_estimate": "high | medium | low",
  "notebooklm_secondary_synthesis_allowed": false,
  "notebooklm_reason": "transcript available — NotebookLM not needed",
  "exclude_reason": null,
  "action_required": "若 discovery_status=source_lead_found_pending_user_lookup，填写具体搜索指引",
  "recommended_next_step": "transcript_ingestion | transcript_ingestion_or_notebooklm | user_lookup_then_transcript_ingestion | skip",
  "confirmation_status": "pending | confirmed | rejected",
  "user_confirmed_url": null
}
```

---

## Transcript Ingestion Queue Format（v1.2 新增）

用户确认后，生成以下格式写入 `ingestion_queue/` 目录：

```json
{
  "queue_metadata": {
    "target_team": "Arsenal",
    "target_league": "EPL",
    "generated_at": "YYYY-MM-DDTHH:mm:ssZ",
    "total_items": 3,
    "next_skill": "youtube-transcript-ingestion",
    "next_skill_version": "1.0"
  },
  "items": [
    {
      "priority": 1,
      "source_url": "https://www.youtube.com/watch?v=VHymL0kvIXQ",
      "video_id": "VHymL0kvIXQ",
      "target_team": "Arsenal",
      "target_league": "EPL",
      "source_channel": "HALF-SPACE THEORY",
      "source_date": "2025-10-11",
      "coach_context": "Arteta",
      "topic_tags": ["build_up", "set_piece", "defensive_block"],
      "expected_claim_types": ["build_up", "set_piece", "defensive_block", "player_role"],
      "likely_has_subtitles": true,
      "language_hint": "en",
      "tactical_density_estimate": "high",
      "ingestion_status": "queued",
      "notebooklm_secondary_synthesis_allowed": false,
      "notebooklm_reason": "transcript available — NotebookLM not needed"
    }
  ]
}
```

---

## Obsidian Candidate Review Table（候选清单 Markdown 格式）

候选清单 MD 文件必须包含以下 Obsidian-editable 表格，用户只需填写最后两列：

```markdown
| # | 来源 / 频道 | Tier | 类型 | 战术密度 | 字幕预期 | 搜索建议 / 参考 URL | 用户确认 URL | 决定 |
|---|------------|------|------|---------|---------|-------------------|-------------|------|
| 1 | HALF-SPACE THEORY | T1 | tactical_analysis | high | en | https://www.youtube.com/watch?v=VHymL0kvIXQ | （用户填写） | pending |
```

用户只需填写：
- **用户确认 URL**：粘贴实际 YouTube 视频 URL 或文章 URL
- **决定**：`accept` / `reject` / `pending`

---

## Human Confirmation Gate

```
Ares 的职责：
  → 搜索并筛选候选来源
  → 输出候选清单到 AresVault candidates/
  → 等待用户在 Obsidian 中确认 URL

用户的职责：
  → 在 Obsidian 中打开候选清单 MD
  → 手动查找并粘贴 YouTube URL（对 PENDING_USER_LOOKUP 条目）
  → 标注 accept / reject / pending
  → 通知 Ares 确认完成

Ares 不得：
  → 自动将 URL 提交给 YT-02 transcript ingestion
  → 跳过用户确认步骤
  → 将 PENDING_USER_LOOKUP 的 URL 传给 YT-02
```

---

## NotebookLM 的定位（v1.2）

NotebookLM **不再是**默认下游路由。

| 场景 | 处理方式 |
|------|---------|
| YouTube 视频有字幕 | → YT-02 transcript ingestion（主路径） |
| YouTube 视频无字幕 | → YT-02 blocked report → 考虑 YT-02b NotebookLM（optional） |
| 战术文章（非视频） | → 可进入 YT-02b NotebookLM secondary synthesis |
| 需要跨多来源综合 | → 可进入 YT-02b NotebookLM secondary synthesis |

每个候选条目的 `notebooklm_secondary_synthesis_allowed` 字段标注是否允许进入 NotebookLM。

---

## 验证清单（v1.2）

交付前确认：
- [ ] 候选清单已写入 AresVault `candidates/` 目录
- [ ] MD 文件包含 Obsidian-editable 候选表格（含 `likely_has_subtitles` 和 `tactical_density_estimate` 列）
- [ ] 每条候选有 `source_kind` 和 `discovery_status`
- [ ] 每条候选有 `likely_has_subtitles`、`language_hint`、`tactical_density_estimate`
- [ ] 每条候选有 `expected_claim_types`
- [ ] 每条候选有 `notebooklm_secondary_synthesis_allowed` 和 `notebooklm_reason`
- [ ] `recommended_next_step` 使用 transcript-first 路由（`transcript_ingestion` 而非 `notebooklm`）
- [ ] `PENDING_USER_LOOKUP` 条目有 `action_required` 搜索指引
- [ ] 候选清单已等待用户确认，未自动进入 YT-02
- [ ] 用户确认后生成 ingestion queue（`ingestion_queue/` 目录）

---

## 打包资源

- `docs/agents/tactical_youtube_channel_whitelist.md` — 频道白名单
- `AresVault/01_Governance/规范 - Ares YouTube 战术情报摄取与候选记忆规则 v1.md` — 治理规范
- `AresVault/01_Governance/规范 - Ares Transcript-first YouTube 战术摄取工作流 v1.md` — 工作流规范（LMW-120）
- `AresVault/01_Governance/模板 - YouTube 战术源候选清单 v1.md` — 候选清单模板
- `src/skills/youtube-transcript-ingestion/SKILL.md` — 下游 YT-02 Skill
inputs:
  - target_team: 目标球队名称（英文标准名）
  - target_league: 目标联赛（EPL / La_liga / Serie_A / Bundesliga / Ligue_1）
  - coach_name: 主帅姓名（可选，用于教练体系搜索）
  - target_match: 目标比赛描述（可选，如 Arsenal vs Chelsea 2026-05-25）
  - search_focus: 搜索重点（tactical_analysis / coach_system / postmatch_review / set_piece / pressing）
  - reference_date: 参考日期（YYYY-MM-DD，用于时效性过滤）
  - max_candidates: 最大候选数量（默认 10，上限 20）
outputs:
  - Candidate Review（Markdown，Obsidian-editable）→
      /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/candidates/
      YYYY-MM-DD_{Team}_tactical_source_candidates.md
  - Candidate Data（JSON）→
      /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/candidates/
      YYYY-MM-DD_{Team}_tactical_source_candidates.json
  - Confirmed Handoff（用户确认后生成）→
      /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/confirmed/
      YYYY-MM-DD_{Team}_confirmed_sources_for_notebooklm.md
changelog:
  - v1.1 (2026-05-22): AresVault path alignment; source_kind / discovery_status fields; PENDING_USER_LOOKUP fallback; Obsidian-editable candidate table
  - v1.0 (2026-05-22): initial release
---

# YouTube Tactical URL Discovery V1.1

## 执行模式说明

本 Skill 是一套**模型无关的 Agent 执行规范**。当你在 Antigravity 中被用户通过 `@skill` 或文件路径方式调用时，请按照本文件的步骤逐一执行。

**你是 URL 发现与筛选引擎，不是内容分析引擎。** 你的职责是：
1. 通过 `search_web` 搜索战术视频/文章来源
2. 按频道分层和视频筛选标准过滤候选
3. 输出 Obsidian-editable 候选清单到 AresVault，供用户手动确认 URL
4. 用户在 Obsidian 中确认 URL 后，生成 NotebookLM handoff 文件

**严格边界**：
- ✅ 搜索 YouTube URL 和战术文章 URL
- ✅ 基于标题和频道判断战术相关性
- ✅ 输出候选清单到 AresVault（Obsidian-editable）
- ❌ 不解析视频内容（不读字幕、不看视频）
- ❌ 不调用 NotebookLM
- ❌ 不做 browser automation
- ❌ 不下载视频
- ❌ 不绕过 YouTube / Google 限制

---

## AresVault 路径模型

```text
Vault Base: /Users/liumingwei/vaults/AresVault

1. Skill 代码（代码项目）
   src/skills/youtube-tactical-url-discovery/SKILL.md

2. 治理规则 / 模板（AresVault 01_Governance）
   01_Governance/规范 - Ares YouTube 战术情报摄取与候选记忆规则 v1.md
   01_Governance/模板 - YouTube 战术源候选清单 v1.md
   01_Governance/模板 - NotebookLM confirmed sources handoff v1.md

3. 候选源（人工查看、补 URL）
   04_RAG_Raw_Data/youtube_tactical_sources/candidates/
     YYYY-MM-DD_<Team>_tactical_source_candidates.md   ← Obsidian-editable
     YYYY-MM-DD_<Team>_tactical_source_candidates.json

4. Confirmed Handoff（传给 YT-02）
   04_RAG_Raw_Data/youtube_tactical_sources/confirmed/
     YYYY-MM-DD_<Team>_confirmed_sources_for_notebooklm.md
   YT-02 只读取 handoff_status: ready_for_notebooklm 的文件

5. NotebookLM 输出
   04_RAG_Raw_Data/youtube_tactical_sources/notebooklm_outputs/
     YYYY-MM-DD_<Team>_notebooklm_tactical_synthesis.md
     YYYY-MM-DD_<Team>_notebooklm_tactical_synthesis.json
```

---

## 核心原则

1. **URL 发现 ≠ 内容分析**。本 Skill 只负责找到值得进入 NotebookLM 的 URL，内容分析由 NotebookLM 完成。
2. **频道可信度优先**。Tier 1 频道的视频默认进入候选池，Tier 2 需要标题验证，excluded 频道直接跳过。
3. **Human confirmation gate 不可绕过**。Ares 推荐来源线索，用户在 Obsidian 中确认具体 URL 后才进入 NotebookLM。
4. **支持 PENDING_USER_LOOKUP**。当 search_web 无法直接返回 YouTube URL 时，输出来源线索 + 搜索建议，用户手动补充 URL。
5. **支持 tactical_article**。战术文章（如 The Coaches' Voice、Breaking The Lines 网站文章）也是有效来源，可与 YouTube 视频并列进入 NotebookLM。

---

## Source Kind（来源类型）

```yaml
source_kind:
  - youtube_video       # 直接 YouTube 视频 URL
  - tactical_article    # 战术分析文章（如 coachesvoice.com、breakingthelines.com）
  - channel_lead        # 频道线索（已知频道有相关内容，但具体视频 URL 待用户查找）
  - search_lead         # 搜索线索（搜索结果指向相关内容，但 URL 需要用户确认）
```

## Discovery Status（发现状态）

```yaml
discovery_status:
  - direct_source_url_found           # 直接找到可用 URL（youtube_video 或 tactical_article）
  - source_lead_found_pending_user_lookup  # 找到来源线索，URL 需要用户手动查找
  - no_viable_source_found            # 未找到可用来源
```

---

## 频道分层（Channel Tier System）

详细频道列表见：`docs/agents/tactical_youtube_channel_whitelist.md`

### Tier 1 — Tactical Candidate Sources
- 以深度战术分析为核心内容
- 视频标题通常包含战术关键词
- `confidence` 默认为 `high`
- 典型：Tifo Football、The Coaches' Voice、Breaking The Lines、Spielverlagerung、Total Football Analysis

### Tier 2 — Context / Postmatch / Sentiment Sources
- 内容混合：战术分析 + 新闻 + 评论
- 需要标题关键词验证才进入候选池
- `confidence` 默认为 `medium`

### Excluded / Low-Trust Sources
- 博彩预测、转会谣言、球迷情绪、纯集锦
- 直接跳过，记录 `exclude_reason`

---

## 视频保留标准（Retention Criteria）

| 类型 | 关键词示例 |
|---|---|
| `tactical_analysis` | "tactical analysis", "tactics explained", "how X plays" |
| `match_tactical_breakdown` | "tactical breakdown", "match analysis", "how X beat Y" |
| `postmatch_tactical_review` | "postmatch analysis", "tactical review", "what went wrong" |
| `coach_system_analysis` | "Arteta system", "Guardiola philosophy", "pressing system" |
| `role_map_analysis` | "role map", "positional play", "half-space", "third man" |
| `pressing_analysis` | "pressing triggers", "high press", "gegenpressing" |
| `build_up_analysis` | "build-up play", "ball progression", "goalkeeper distribution" |
| `transition_analysis` | "counter-attack", "transition", "defensive shape", "low block" |
| `set_piece_analysis` | "set piece", "corner routine", "free kick", "dead ball" |

## 视频排除标准（Exclusion Criteria）

| 类型 | 关键词示例 |
|---|---|
| `highlights_only` | "highlights", "goals", "best moments" |
| `betting_prediction` | "prediction", "betting tips", "odds" |
| `transfer_rumour` | "transfer news", "signing", "rumour" |
| `fan_rant` | "rant", "reaction", "angry fan" |
| `pure_news_gossip` | "news", "latest", "breaking" |
| `no_tactical_claim` | 标题无战术关键词，且频道为 Tier 2 |

---

## 执行流程（4 Phases）

### Phase 1: 参数解析与搜索策略制定

1. 从用户输入中提取 `target_team`、`target_league`、`coach_name`、`search_focus`、`reference_date`
2. 根据 `search_focus` 制定搜索查询
3. 确认输出路径（AresVault candidates 目录）

### Phase 2: 搜索与候选收集

**搜索查询模板**：
```
# tactical_analysis
search_web("{team} tactical analysis YouTube {year}")
search_web("{team} tactics {coach_name} YouTube {year}")

# coach_system
search_web("{coach_name} system analysis tactics YouTube")
search_web("{coach_name} pressing build-up philosophy YouTube")

# postmatch_review
search_web("{team} tactical breakdown {match_description} YouTube")
search_web("{team} postmatch analysis {year} YouTube")

# set_piece / pressing
search_web("{team} set piece analysis YouTube {year}")
search_web("{team} pressing system {coach_name} YouTube")
```

**⚠️ Fallback 策略（当 search_web 无法返回直接 YouTube URL 时）**：

```
Fallback Step 1: 识别来源线索
  → 从搜索结果中识别 Tier 1 频道的内容（文章/网站 URL）
  → 记录频道名称和参考文章 URL
  → 标注 source_kind: channel_lead 或 tactical_article

Fallback Step 2: 输出 PENDING_USER_LOOKUP
  → video_id: "PENDING_USER_LOOKUP"
  → discovery_status: source_lead_found_pending_user_lookup
  → action_required: "在 YouTube 搜索 '{channel} {team} {topic}'"

Fallback Step 3: 战术文章作为直接来源
  → 若搜索结果返回 The Coaches' Voice / Breaking The Lines 等 Tier 1 网站文章
  → 标注 source_kind: tactical_article
  → discovery_status: direct_source_url_found
  → 这类文章可直接进入 NotebookLM（无需 YouTube URL）
```

### Phase 3: 筛选与评级

1. **频道分层判断**：对照白名单确定 `channel_tier`
2. **保留/排除判断**：对照保留标准和排除标准
3. **confidence 评级**：Tier 1 + 标题匹配 → `high`；Tier 2 + 战术关键词 → `medium`；模糊 → `low`
4. **discovery_status 标注**：按上方 fallback 策略标注
5. **recommended_next_step 判断**：
   - `direct_source_url_found` + `confidence=high` → `notebooklm`
   - `source_lead_found_pending_user_lookup` → `user_lookup_then_notebooklm`
   - `confidence=low` → `skip`

### Phase 4: 输出到 AresVault

1. 生成 Obsidian-editable 候选清单（MD）和结构化数据（JSON）
2. 写入 `04_RAG_Raw_Data/youtube_tactical_sources/candidates/`
3. **等待用户在 Obsidian 中确认 URL**
4. 用户确认后，生成 confirmed handoff 文件写入 `confirmed/`

---

## Candidate Video Output Schema（v1.1）

```json
{
  "url": "YouTube 视频 URL 或文章 URL 或频道 URL",
  "video_id": "YouTube video_id 或 PENDING_USER_LOOKUP",
  "source_kind": "youtube_video | tactical_article | channel_lead | search_lead",
  "discovery_status": "direct_source_url_found | source_lead_found_pending_user_lookup | no_viable_source_found",
  "title": "视频/文章标题",
  "channel": "频道/网站名称",
  "channel_tier": "T1 | T2 | excluded",
  "published_at": "YYYY-MM-DD 或 unknown",
  "duration": "HH:MM:SS 或 unknown（仅 youtube_video）",
  "target_team": "目标球队",
  "target_match": "目标比赛（若适用）",
  "coach_context": "相关教练（若适用）",
  "source_type": "tactical_analysis | match_tactical_breakdown | postmatch_tactical_review | coach_system_analysis | role_map_analysis | pressing_analysis | build_up_analysis | transition_analysis | set_piece_analysis",
  "reason_selected": "选择原因",
  "tactical_claim_hint": "标题中暗示的战术主张",
  "confidence": "high | medium | low",
  "exclude_reason": null,
  "action_required": "若 discovery_status=source_lead_found_pending_user_lookup，填写具体搜索指引",
  "recommended_next_step": "notebooklm | user_lookup_then_notebooklm | manual_transcript | skip",
  "confirmation_status": "pending | confirmed | rejected",
  "user_confirmed_url": null
}
```

---

## Obsidian Candidate Review Table（候选清单 Markdown 格式）

候选清单 MD 文件必须包含以下 Obsidian-editable 表格，用户只需填写最后两列：

```markdown
| # | 来源 / 频道 | Tier | 类型 | 选择原因 | 搜索建议 / 参考 URL | 用户确认 URL | 决定 |
|---|------------|------|------|---------|-------------------|-------------|------|
| 1 | Tifo Football | T1 | tactical_analysis | 已知有 Arsenal 战术分析内容 | YouTube: "Tifo Football Arsenal tactical 2026" | （用户填写） | pending |
```

用户只需填写：
- **用户确认 URL**：粘贴实际 YouTube 视频 URL 或文章 URL
- **决定**：`accept` / `reject` / `pending`

---

## Human Confirmation Gate

```
Ares 的职责：
  → 搜索并筛选候选来源
  → 输出候选清单到 AresVault candidates/
  → 等待用户在 Obsidian 中确认 URL

用户的职责：
  → 在 Obsidian 中打开候选清单 MD
  → 手动查找并粘贴 YouTube URL（对 PENDING_USER_LOOKUP 条目）
  → 标注 accept / reject / pending
  → 通知 Ares 确认完成

Ares 不得：
  → 自动将 URL 提交给 NotebookLM
  → 跳过用户确认步骤
  → 将 PENDING_USER_LOOKUP 的 URL 传给 NotebookLM
```

---

## NotebookLM Confirmed Handoff Format

用户确认后，生成以下格式写入 `confirmed/` 目录：

```yaml
---
handoff_status: ready_for_notebooklm
notebook_name_suggestion: "Ares Tactical Notebook — {Team} — {YYYY-MM-DD}"
fixed_query_set_ref: notebooklm_tactical_query_set_v1
target_team: {team}
target_league: {league}
coach_context: {coach}
source_scope: tactical_analysis | coach_system | postmatch_review | mixed
generated_at: YYYY-MM-DDTHH:mm:ssZ
---

# Confirmed Sources for NotebookLM — {Team} — {YYYY-MM-DD}

## confirmed_source_urls

- source_kind: youtube_video
  url: https://www.youtube.com/watch?v=...
  source_scope: tactical_analysis
  note: Tifo Football — Arsenal pressing system 2025/26

- source_kind: tactical_article
  url: https://learning.coachesvoice.com/cv/...
  source_scope: coach_system_analysis
  note: The Coaches' Voice — Arteta system analysis

## next_step

YT-02: NotebookLM tactical synthesis
```

**YT-02 规则**：只读取 `handoff_status: ready_for_notebooklm` 的文件，不读取 candidates/ 目录。

---

## 验证清单（v1.1）

交付前确认：
- [ ] 候选清单已写入 AresVault `candidates/` 目录
- [ ] MD 文件包含 Obsidian-editable 候选表格
- [ ] 每条候选有 `source_kind` 和 `discovery_status`
- [ ] `PENDING_USER_LOOKUP` 条目有 `action_required` 搜索指引
- [ ] `tactical_article` 来源有完整 URL（可直接进入 NotebookLM）
- [ ] 候选清单已等待用户确认，未自动进入 NotebookLM
- [ ] confirmed handoff 文件只在用户确认后生成

---

## 打包资源

- `docs/agents/tactical_youtube_channel_whitelist.md` — 频道白名单
- `docs/agents/tactical_video_triage_template.md` — 视频筛选操作模板
- `docs/agents/tactical_video_candidate_schema.md` — 候选视频 JSON schema
- `AresVault/01_Governance/规范 - Ares YouTube 战术情报摄取与候选记忆规则 v1.md` — 治理规范
- `AresVault/01_Governance/模板 - YouTube 战术源候选清单 v1.md` — 候选清单模板
- `AresVault/01_Governance/模板 - NotebookLM confirmed sources handoff v1.md` — handoff 模板
