---
name: youtube-tactical-url-discovery
version: "1.0"
source: ares-osint-telemetry native
description: >
  YouTube 战术视频 URL 发现与筛选 Skill。
  用于从 YouTube 中发现、筛选和输出高质量战术分析 / 赛后战术复盘 / 教练体系解析视频 URL，
  供用户确认后进入 NotebookLM tactical synthesis 流程。
  本 Skill 只处理 URL discovery，不解析视频内容，不调用 NotebookLM。
inputs:
  - target_team: 目标球队名称（英文标准名）
  - target_league: 目标联赛（EPL / La_liga / Serie_A / Bundesliga / Ligue_1）
  - coach_name: 主帅姓名（可选，用于教练体系搜索）
  - target_match: 目标比赛描述（可选，如 Arsenal vs Chelsea 2026-05-25）
  - search_focus: 搜索重点（tactical_analysis / coach_system / postmatch_review / set_piece / pressing）
  - reference_date: 参考日期（YYYY-MM-DD，用于时效性过滤）
  - max_candidates: 最大候选数量（默认 10，上限 20）
outputs:
  - Candidate List（JSON）→ draft_reports/youtube-tactical-url-discovery_{team}_{date}.json
  - Candidate Report（Markdown）→ draft_reports/youtube-tactical-url-discovery_{team}_{date}.md
  - NotebookLM Handoff（JSON）→ 用户确认后生成
---

# YouTube Tactical URL Discovery V1.0

## 执行模式说明

本 Skill 是一套**模型无关的 Agent 执行规范**。当你在 Antigravity 中被用户通过 `@skill` 或文件路径方式调用时，请按照本文件的步骤逐一执行。

**你是 URL 发现与筛选引擎，不是内容分析引擎。** 你的职责是：
1. 通过 `search_web` 搜索 YouTube 视频
2. 按频道分层和视频筛选标准过滤候选
3. 输出结构化候选列表供用户确认
4. 用户确认后生成 NotebookLM handoff 格式

**严格边界**：
- ✅ 搜索 YouTube URL，读取视频标题/频道/时长/发布时间
- ✅ 基于标题和频道判断战术相关性
- ❌ 不解析视频内容（不读字幕、不看视频）
- ❌ 不调用 NotebookLM
- ❌ 不做 browser automation
- ❌ 不下载视频
- ❌ 不绕过 YouTube / Google 限制

---

## 核心原则

1. **URL 发现 ≠ 内容分析**。本 Skill 只负责找到值得进入 NotebookLM 的 URL，内容分析由 NotebookLM 完成。
2. **频道可信度优先**。Tier 1 频道的视频默认进入候选池，Tier 2 需要标题验证，excluded 频道直接跳过。
3. **Human confirmation gate 不可绕过**。Ares 推荐 URL，用户确认后才进入 NotebookLM synthesis。
4. **宁缺毋滥**。不确定的视频标注 `confidence: low`，不强制填充候选池。
5. **时效性优先**。优先推荐近 12 个月内的视频，超过 24 个月的视频需要特别说明。

---

## 频道分层（Channel Tier System）

详细频道列表见：`docs/agents/tactical_youtube_channel_whitelist.md`

### Tier 1 — Tactical Candidate Sources（战术分析主力来源）

这些频道以深度战术分析为核心内容，视频标题通常包含战术关键词。

**判定标准**：
- 频道主要内容为战术分析、阵型解析、教练体系研究
- 有固定的战术分析格式（如 pressing map、role map、build-up analysis）
- 内容基于数据或视频证据，而非纯观点

**处理规则**：
- Tier 1 频道的视频，若标题通过保留标准，直接进入候选池
- `confidence` 默认为 `high`

### Tier 2 — Context / Postmatch / Sentiment Sources（背景/赛后/情绪来源）

这些频道提供赛后分析、球队新闻、战术评论，但深度不如 Tier 1。

**判定标准**：
- 频道内容混合：战术分析 + 新闻 + 评论
- 赛后分析质量参差不齐
- 部分视频有战术价值，部分只是情绪反应

**处理规则**：
- Tier 2 频道的视频，必须通过标题关键词验证才进入候选池
- `confidence` 默认为 `medium`，需要用户确认

### Excluded / Low-Trust Sources（排除来源）

**判定标准**：
- 主要内容为博彩预测、转会谣言、球迷情绪
- 无战术分析内容
- 标题党、点击诱饵

**处理规则**：
- 直接跳过，不进入候选池
- 若搜索结果中出现，记录 `exclude_reason`

---

## 视频保留标准（Retention Criteria）

以下类型的视频应进入候选池：

| 类型 | 关键词示例 | 说明 |
|---|---|---|
| `tactical_analysis` | "tactical analysis", "tactics explained", "how X plays" | 系统性战术分析 |
| `match_tactical_breakdown` | "tactical breakdown", "match analysis", "how X beat Y" | 单场比赛战术拆解 |
| `postmatch_tactical_review` | "postmatch analysis", "tactical review", "what went wrong" | 赛后战术复盘 |
| `coach_system_analysis` | "Arteta system", "Guardiola philosophy", "pressing system" | 教练体系解析 |
| `role_map_analysis` | "role map", "positional play", "half-space", "third man" | 角色/位置分析 |
| `pressing_analysis` | "pressing triggers", "high press", "gegenpressing", "press resistance" | 逼抢体系分析 |
| `build_up_analysis` | "build-up play", "ball progression", "goalkeeper distribution" | 进攻组织分析 |
| `transition_analysis` | "counter-attack", "transition", "defensive shape", "low block" | 攻守转换分析 |
| `set_piece_analysis` | "set piece", "corner routine", "free kick", "dead ball" | 定位球分析 |

---

## 视频排除标准（Exclusion Criteria）

以下类型的视频不得进入候选池：

| 类型 | 关键词示例 | 说明 |
|---|---|---|
| `highlights_only` | "highlights", "goals", "best moments" | 纯集锦，无战术内容 |
| `betting_prediction` | "prediction", "betting tips", "odds", "accumulator" | 博彩预测 |
| `transfer_rumour` | "transfer news", "signing", "rumour", "done deal" | 转会谣言 |
| `fan_rant` | "rant", "reaction", "angry fan", "disaster" | 球迷情绪，无战术内容 |
| `pure_news_gossip` | "news", "latest", "breaking", "update" | 纯新闻，无战术分析 |
| `no_tactical_claim` | 标题无任何战术关键词，且频道为 Tier 2 | 无战术主张 |

**排除优先级**：若视频同时满足保留和排除标准，排除优先。

---

## 执行流程（4 Phases）

### Phase 1: 参数解析与搜索策略制定

1. 从用户输入中提取 `target_team`、`target_league`、`coach_name`、`search_focus`、`reference_date`
2. 根据 `search_focus` 制定搜索查询模板（见下方查询模板）
3. 确认输出路径：`draft_reports/youtube-tactical-url-discovery_{team}_{date}.{md|json}`

### Phase 2: YouTube 搜索与候选收集

对每个搜索查询执行 `search_web`，收集候选视频：

**搜索查询模板**：
```
# tactical_analysis
search_web("site:youtube.com {team} tactical analysis {season}")
search_web("site:youtube.com {team} tactics {coach_name} {year}")

# coach_system
search_web("site:youtube.com {coach_name} system analysis tactics")
search_web("site:youtube.com {coach_name} pressing build-up philosophy")

# postmatch_review
search_web("site:youtube.com {team} tactical breakdown {match_description}")
search_web("site:youtube.com {team} postmatch analysis {year}")

# set_piece
search_web("site:youtube.com {team} set piece analysis corners {year}")

# pressing
search_web("site:youtube.com {team} pressing system {coach_name}")
```

**⚠️ 工具限制与 Fallback 策略**：

`search_web` 工具可能无法直接返回 YouTube 视频 URL（`site:youtube.com` 过滤可能返回空结果）。当遇到此情况时，执行以下 fallback 策略：

```
Fallback Step 1: 不带 site 限定搜索
  search_web("{team} tactical analysis YouTube {year}")
  search_web("{channel_name} {team} tactical breakdown")
  → 从结果中识别 Tier 1 频道的内容（文章/网站 URL）
  → 记录频道名称和参考文章 URL

Fallback Step 2: 输出 action_required
  → 对无法直接获取 YouTube URL 的候选，标注：
      video_id: "PENDING_USER_LOOKUP"
      action_required: "用户需要在 YouTube 搜索 '{channel} {team} {topic}' 获取具体视频 URL"
  → 候选仍然有价值：确认了哪些频道有相关内容

Fallback Step 3: 提供搜索建议
  → 在报告末尾输出"YouTube 搜索建议"章节
  → 列出每个候选的推荐搜索词
```

**收集字段**（从搜索结果中提取）：
- URL（YouTube 视频 URL 或参考文章 URL）
- 视频标题（或文章标题）
- 频道名称
- 发布时间（若可获取）
- 时长（若可获取）

### Phase 3: 筛选与评级

对每个候选视频执行：

1. **频道分层判断**：对照 `tactical_youtube_channel_whitelist.md` 确定 `channel_tier`
2. **保留/排除判断**：对照保留标准和排除标准
3. **confidence 评级**：
   - Tier 1 + 标题匹配保留标准 → `high`
   - Tier 2 + 标题明确包含战术关键词 → `medium`
   - Tier 2 + 标题模糊 → `low`
   - Excluded 频道 → 不进入候选池
4. **recommended_next_step 判断**：
   - `confidence=high` → `notebooklm`
   - `confidence=medium` → `manual_transcript`（用户手动确认后再决定）
   - `confidence=low` → `skip`（除非用户特别指定）

### Phase 4: 输出与 Human Confirmation Gate

1. 生成候选列表（JSON + Markdown）
2. 明确标注哪些视频推荐进入 NotebookLM，哪些需要用户手动确认
3. **等待用户确认**：不自动进入 NotebookLM
4. 用户确认后，生成 NotebookLM handoff 格式

---

## Candidate Video Output Schema

每条候选视频记录包含以下字段：

```json
{
  "url": "https://www.youtube.com/watch?v={video_id}",
  "video_id": "{video_id}",
  "title": "视频标题",
  "channel": "频道名称",
  "channel_tier": "T1 | T2 | excluded",
  "published_at": "YYYY-MM-DD（若可获取，否则填 unknown）",
  "duration": "HH:MM:SS（若可获取，否则填 unknown）",
  "target_team": "目标球队",
  "target_match": "目标比赛（若适用，否则填 null）",
  "coach_context": "相关教练（若适用，否则填 null）",
  "source_type": "tactical_analysis | match_tactical_breakdown | postmatch_tactical_review | coach_system_analysis | role_map_analysis | pressing_analysis | build_up_analysis | transition_analysis | set_piece_analysis",
  "reason_selected": "选择原因，基于标题和频道的判断",
  "tactical_claim_hint": "标题中暗示的战术主张，如 'high press triggers' 或 'positional play breakdown'",
  "confidence": "high | medium | low",
  "exclude_reason": "若被排除，填写排除原因；否则填 null",
  "recommended_next_step": "notebooklm | manual_transcript | skip"
}
```

---

## Human Confirmation Gate

**这是不可绕过的规则。**

```
Ares 的职责：
  → 搜索并筛选候选视频
  → 输出候选列表（JSON + Markdown）
  → 明确标注 recommended_next_step
  → 等待用户确认

用户的职责：
  → 审核候选列表
  → 确认哪些 URL 进入 NotebookLM
  → 可以添加、删除、修改候选
  → 确认后触发 NotebookLM handoff

Ares 不得：
  → 自动将 URL 提交给 NotebookLM
  → 跳过用户确认步骤
  → 假设用户同意所有推荐
```

---

## NotebookLM Handoff Format

用户确认后，生成以下格式的 handoff 文件：

```json
{
  "handoff_type": "notebooklm_tactical_synthesis",
  "generated_at": "YYYY-MM-DDTHH:mm:ssZ",
  "target_team": "球队名称",
  "target_league": "联赛名称",
  "coach_context": "教练姓名（若适用）",
  "confirmed_source_urls": [
    "https://www.youtube.com/watch?v=...",
    "https://www.youtube.com/watch?v=..."
  ],
  "notebook_name_suggestion": "Ares_{team}_{coach}_Tactical_Analysis_{YYYYMM}",
  "fixed_query_set_ref": "docs/agents/tactical_video_triage_template.md",
  "source_scope": "tactical_analysis | coach_system | postmatch_review | mixed",
  "user_notes": "用户在确认时添加的备注（可选）",
  "next_step": "YT-02: NotebookLM tactical synthesis"
}
```

---

## 验证清单

交付前确认：
- [ ] 每条候选视频包含所有必填字段
- [ ] `channel_tier` 已对照白名单确认
- [ ] `source_type` 只使用 9 个规范类型
- [ ] `confidence` 只使用 high / medium / low
- [ ] `recommended_next_step` 已正确标注
- [ ] 排除的视频有 `exclude_reason`
- [ ] 候选列表已等待用户确认，未自动进入 NotebookLM
- [ ] 最终成品包含 Markdown 报告和 JSON 数据

---

## 打包资源

- `docs/agents/tactical_youtube_channel_whitelist.md` — 频道白名单（Tier 1 / Tier 2 / excluded）
- `docs/agents/tactical_video_triage_template.md` — 视频筛选操作模板
- `docs/agents/tactical_video_candidate_schema.md` — 候选视频 JSON schema 定义
