# Tactical Video Triage Template

**版本**: v1.0
**日期**: 2026-05-22
**关联 Skill**: `src/skills/youtube-tactical-url-discovery/SKILL.md`
**关联 Issue**: LMW-103

> 本模板用于 `youtube-tactical-url-discovery` Skill 执行视频筛选时的操作指南，
> 以及用户确认候选视频时的参考框架。

---

## 一、搜索查询模板

### 1.1 战术分析搜索

```
# 通用战术分析
site:youtube.com "{team}" tactical analysis {year}
site:youtube.com "{team}" tactics explained {coach_name}
site:youtube.com "{team}" how they play {season}

# 教练体系
site:youtube.com "{coach_name}" system analysis
site:youtube.com "{coach_name}" pressing philosophy
site:youtube.com "{coach_name}" positional play

# 赛后战术复盘
site:youtube.com "{team}" tactical breakdown {opponent} {year}
site:youtube.com "{team}" postmatch analysis {month} {year}
site:youtube.com "{team}" vs "{opponent}" tactics

# 专项分析
site:youtube.com "{team}" pressing triggers {year}
site:youtube.com "{team}" build-up play analysis
site:youtube.com "{team}" set piece analysis {year}
site:youtube.com "{team}" defensive shape {coach_name}
```

### 1.2 搜索参数说明

| 参数 | 说明 | 示例 |
|---|---|---|
| `{team}` | 球队英文标准名 | Arsenal, Liverpool |
| `{coach_name}` | 主帅姓名 | Arteta, Slot |
| `{year}` | 年份 | 2025, 2026 |
| `{season}` | 赛季 | 2025-26 |
| `{opponent}` | 对手名称 | Manchester City |
| `{month}` | 月份 | May, April |

---

## 二、筛选决策树

```
收到候选视频
    ↓
检查频道 → Excluded? → 直接跳过，记录 exclude_reason
    ↓ 否
检查标题 → 包含排除关键词? → 跳过，记录 exclude_reason
    ↓ 否
检查频道 Tier
    ↓
  Tier 1 → 标题包含保留关键词? → 是 → confidence=high, recommended=notebooklm
                                → 否 → confidence=medium, recommended=manual_transcript
    ↓
  Tier 2 → 标题明确包含战术关键词? → 是 → confidence=medium, recommended=manual_transcript
                                    → 否 → confidence=low, recommended=skip
```

---

## 三、标题关键词速查

### 保留关键词（出现即加分）

**战术分析类**：
`tactical analysis`, `tactics explained`, `how they play`, `tactical breakdown`,
`match analysis`, `postmatch analysis`, `tactical review`, `system analysis`

**教练体系类**：
`pressing system`, `positional play`, `build-up`, `high press`, `gegenpressing`,
`low block`, `counter-attack`, `transition`, `defensive shape`

**专项分析类**：
`set piece`, `corner routine`, `free kick`, `role map`, `half-space`,
`third man`, `pressing triggers`, `ball progression`, `goalkeeper distribution`

### 排除关键词（出现即减分/排除）

`highlights`, `goals`, `best moments`, `prediction`, `betting tips`, `odds`,
`transfer news`, `signing`, `rumour`, `done deal`, `rant`, `reaction`,
`angry`, `disaster`, `breaking news`, `latest update`

---

## 四、候选视频评审清单（用户确认用）

用户在确认候选列表时，对每条视频检查以下项目：

```
□ 标题是否真实反映战术内容？
□ 频道是否可信（Tier 1 / Tier 2）？
□ 发布时间是否在合理范围内（建议 12 个月内）？
□ 视频时长是否合理（建议 5-30 分钟，过短可能是集锦，过长可能是综合节目）？
□ 是否与目标球队/教练直接相关？
□ 是否有具体战术主张（而非泛泛而谈）？
```

---

## 五、NotebookLM 确认格式

用户确认后，填写以下信息触发 NotebookLM handoff：

```markdown
## 确认进入 NotebookLM 的视频

目标球队: {team}
教练: {coach_name}
分析重点: {search_focus}

确认 URL 列表:
1. {url_1} — {reason}
2. {url_2} — {reason}
...

Notebook 命名建议: Ares_{team}_{coach}_Tactical_{YYYYMM}
分析查询集参考: docs/agents/tactical_video_triage_template.md
```

---

## 六、常见问题处理

### Q: 视频标题模糊，无法判断是否有战术内容？
A: 标注 `confidence=low`，`recommended_next_step=skip`。不强制进入候选池。

### Q: 频道不在白名单中？
A: 默认按 Tier 2 处理，需要标题关键词验证。若频道明显是博彩/谣言类，直接排除。

### Q: 视频发布时间超过 24 个月？
A: 可以保留，但在 `reason_selected` 中注明"历史视频，教练体系参考"。若教练已离任，降低 `confidence`。

### Q: 同一频道有多个相关视频？
A: 最多选取 3 个，优先选最新的和最具体的（针对特定比赛或特定战术主题）。

### Q: 用户要求搜索特定比赛的战术分析？
A: 在搜索查询中加入比赛描述，如 `"{team} vs {opponent} tactical breakdown {date}"`。
