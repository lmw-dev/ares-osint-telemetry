# Tactical Video Candidate Schema

**版本**: v1.0
**日期**: 2026-05-22
**关联 Skill**: `src/skills/youtube-tactical-url-discovery/SKILL.md`
**关联 Issue**: LMW-103

> 本文档定义 `youtube-tactical-url-discovery` Skill 输出的候选视频 JSON schema。

---

## 一、顶层结构

```json
{
  "meta": {
    "skill": "youtube-tactical-url-discovery",
    "version": "1.0",
    "generated_at": "YYYY-MM-DDTHH:mm:ssZ",
    "target_team": "球队名称",
    "target_league": "联赛名称",
    "coach_context": "教练姓名（若适用）",
    "search_focus": "tactical_analysis | coach_system | postmatch_review | set_piece | pressing",
    "reference_date": "YYYY-MM-DD",
    "total_candidates": 0,
    "confirmed_candidates": 0,
    "pending_confirmation": 0,
    "skipped": 0
  },
  "candidates": [],
  "excluded": [],
  "notebooklm_handoff": null
}
```

---

## 二、候选视频字段（candidates[]）

```json
{
  "url": {
    "type": "string",
    "format": "https://www.youtube.com/watch?v={video_id}",
    "required": true,
    "description": "YouTube 视频完整 URL"
  },
  "video_id": {
    "type": "string",
    "required": true,
    "description": "YouTube 视频 ID（URL 中 v= 后的部分）"
  },
  "title": {
    "type": "string",
    "required": true,
    "description": "视频标题（从搜索结果中获取）"
  },
  "channel": {
    "type": "string",
    "required": true,
    "description": "频道名称"
  },
  "channel_tier": {
    "type": "string",
    "enum": ["T1", "T2", "unknown"],
    "required": true,
    "description": "频道分层，对照 tactical_youtube_channel_whitelist.md"
  },
  "published_at": {
    "type": "string",
    "format": "YYYY-MM-DD",
    "required": false,
    "description": "发布日期（若无法获取填 unknown）"
  },
  "duration": {
    "type": "string",
    "format": "HH:MM:SS 或 MM:SS",
    "required": false,
    "description": "视频时长（若无法获取填 unknown）"
  },
  "target_team": {
    "type": "string",
    "required": true,
    "description": "视频分析的目标球队"
  },
  "target_match": {
    "type": ["string", "null"],
    "required": false,
    "description": "视频分析的目标比赛（若适用），如 Arsenal vs Chelsea 2026-05-25"
  },
  "coach_context": {
    "type": ["string", "null"],
    "required": false,
    "description": "相关教练（若适用）"
  },
  "source_type": {
    "type": "string",
    "enum": [
      "tactical_analysis",
      "match_tactical_breakdown",
      "postmatch_tactical_review",
      "coach_system_analysis",
      "role_map_analysis",
      "pressing_analysis",
      "build_up_analysis",
      "transition_analysis",
      "set_piece_analysis"
    ],
    "required": true,
    "description": "视频内容类型"
  },
  "reason_selected": {
    "type": "string",
    "required": true,
    "description": "选择原因，基于标题和频道的判断，禁止编造"
  },
  "tactical_claim_hint": {
    "type": ["string", "null"],
    "required": false,
    "description": "标题中暗示的战术主张，如 'high press triggers' 或 'positional play breakdown'"
  },
  "confidence": {
    "type": "string",
    "enum": ["high", "medium", "low"],
    "required": true,
    "description": "置信度：high=Tier1+标题匹配；medium=Tier2+战术关键词；low=模糊"
  },
  "exclude_reason": {
    "type": "null",
    "description": "候选视频此字段为 null（排除视频才填写）"
  },
  "recommended_next_step": {
    "type": "string",
    "enum": ["notebooklm", "manual_transcript", "skip"],
    "required": true,
    "description": "推荐下一步：notebooklm=直接进入；manual_transcript=用户手动确认；skip=跳过"
  },
  "confirmation_status": {
    "type": "string",
    "enum": ["pending", "confirmed", "rejected"],
    "default": "pending",
    "description": "用户确认状态"
  }
}
```

---

## 三、排除视频字段（excluded[]）

```json
{
  "url": "https://www.youtube.com/watch?v={video_id}",
  "video_id": "{video_id}",
  "title": "视频标题",
  "channel": "频道名称",
  "channel_tier": "T1 | T2 | excluded | unknown",
  "exclude_reason": "排除原因，如 highlights_only / betting_prediction / fan_rant / no_tactical_claim",
  "exclude_type": "highlights_only | betting_prediction | transfer_rumour | fan_rant | pure_news_gossip | no_tactical_claim | excluded_channel"
}
```

---

## 四、NotebookLM Handoff 字段（notebooklm_handoff）

用户确认后填充此字段：

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

## 五、完整示例

```json
{
  "meta": {
    "skill": "youtube-tactical-url-discovery",
    "version": "1.0",
    "generated_at": "2026-05-22T10:00:00Z",
    "target_team": "Arsenal",
    "target_league": "EPL",
    "coach_context": "Mikel Arteta",
    "search_focus": "tactical_analysis",
    "reference_date": "2026-05-22",
    "total_candidates": 3,
    "confirmed_candidates": 0,
    "pending_confirmation": 3,
    "skipped": 2
  },
  "candidates": [
    {
      "url": "https://www.youtube.com/watch?v=example1",
      "video_id": "example1",
      "title": "Arsenal's Pressing System Under Arteta - Tactical Analysis 2025/26",
      "channel": "Tifo Football",
      "channel_tier": "T1",
      "published_at": "2026-03-15",
      "duration": "12:34",
      "target_team": "Arsenal",
      "target_match": null,
      "coach_context": "Mikel Arteta",
      "source_type": "pressing_analysis",
      "reason_selected": "Tier 1 channel (Tifo Football), title explicitly mentions pressing system and Arteta",
      "tactical_claim_hint": "pressing system, Arteta tactical philosophy",
      "confidence": "high",
      "exclude_reason": null,
      "recommended_next_step": "notebooklm",
      "confirmation_status": "pending"
    }
  ],
  "excluded": [
    {
      "url": "https://www.youtube.com/watch?v=example_excluded",
      "video_id": "example_excluded",
      "title": "Arsenal vs Chelsea HIGHLIGHTS 2026",
      "channel": "Sky Sports Football",
      "channel_tier": "T2",
      "exclude_reason": "Title contains 'HIGHLIGHTS' — highlights_only exclusion rule",
      "exclude_type": "highlights_only"
    }
  ],
  "notebooklm_handoff": null
}
```

---

## 六、字段约束规则

| 字段 | 约束 |
|---|---|
| `url` | 必须是 `https://www.youtube.com/watch?v=` 格式 |
| `channel_tier` | 必须对照 `tactical_youtube_channel_whitelist.md` |
| `source_type` | 只使用 9 个规范枚举值 |
| `confidence` | 只使用 high / medium / low |
| `recommended_next_step` | 只使用 notebooklm / manual_transcript / skip |
| `exclude_reason` | 候选视频此字段必须为 null |
| `confirmation_status` | 初始值必须为 pending |
| `notebooklm_handoff` | 用户确认前必须为 null |
