# Tactical YouTube Channel Whitelist

**版本**: v1.0
**日期**: 2026-05-22
**关联 Skill**: `src/skills/youtube-tactical-url-discovery/SKILL.md`
**关联 Issue**: LMW-103

> 本文档定义了 `youtube-tactical-url-discovery` Skill 使用的频道分层白名单。
> Tier 1 频道的视频默认进入候选池；Tier 2 需要标题验证；Excluded 直接跳过。
> 本列表应随使用经验持续更新。

---

## Tier 1 — Tactical Candidate Sources

这些频道以深度战术分析为核心，视频质量高，战术主张明确，适合直接进入 NotebookLM synthesis。

### 英语频道

| 频道名称 | YouTube 频道 | 专长 | 备注 |
|---|---|---|---|
| Tifo Football | @TifoFootball | 战术图解、阵型分析、教练体系 | 高质量动画图解，适合 coach system |
| Tifo IRL | @TifoIRL | 深度战术访谈、球员角色分析 | Tifo 旗下深度内容 |
| The Coaches' Voice | @TheCoachesVoice | 教练亲述战术体系 | 一手教练视角，极高价值 |
| Breaking The Lines | @BreakingTheLines | 战术分析、pressing、positional play | 学术风格，数据支撑 |
| Spielverlagerung | @Spielverlagerung | 德式战术分析、pressing theory | 德语为主，英语内容也有 |
| Total Football Analysis | @TotalFootballAnalysis | 系统性战术分析 | 覆盖五大联赛 |
| Football Made Simple | @FootballMadeSimple | 战术简化解析 | 适合 role map / build-up |
| Tactical Theory | @TacticalTheory | 战术理论、教练体系 | 深度分析 |
| Opta Analyst | @OptaAnalyst | 数据驱动战术分析 | 官方数据支撑 |
| StatsBomb | @StatsBomb | 高级数据分析 | 专业数据机构 |

### 联赛专属频道（Tier 1）

| 频道名称 | YouTube 频道 | 专长 | 备注 |
|---|---|---|---|
| The Athletic Football | @TheAthleticFC | 深度战术报道 | 付费媒体，YouTube 有部分免费内容 |
| Football Tactics | @FootballTactics | 战术分析 | 覆盖多联赛 |

---

## Tier 2 — Context / Postmatch / Sentiment Sources

这些频道提供赛后分析和战术评论，但内容质量参差不齐。需要标题关键词验证才进入候选池。

### 英语综合频道

| 频道名称 | YouTube 频道 | 专长 | 注意事项 |
|---|---|---|---|
| Sky Sports Football | @SkySportsFootball | 赛后分析、发布会 | 大量非战术内容，需标题过滤 |
| BBC Sport | @BBCSport | 赛后报道 | 战术内容较少，需严格过滤 |
| ESPN FC | @ESPNFC | 综合足球内容 | 战术内容混杂，需过滤 |
| The Guardian Football | @GuardianFootball | 深度报道 | 部分有战术价值 |
| Football Daily | @FootballDaily | 综合内容 | 战术内容较少 |
| GOAL | @goal | 综合足球内容 | 主要是新闻，战术内容少 |

### 球队专属频道（Tier 2）

| 频道名称 | YouTube 频道 | 专长 | 注意事项 |
|---|---|---|---|
| Arsenal Official | @Arsenal | 官方内容 | 战术内容极少，主要是宣传 |
| Liverpool FC | @LiverpoolFC | 官方内容 | 同上 |
| 其他俱乐部官方频道 | 各自官方 | 官方内容 | 一般无战术分析价值 |

### 战术评论频道（Tier 2）

| 频道名称 | YouTube 频道 | 专长 | 注意事项 |
|---|---|---|---|
| Zonal Marking | @ZonalMarking | 战术评论 | 质量参差，需标题验证 |
| Football Explained | @FootballExplained | 战术解释 | 深度不稳定 |
| Tactics Board | @TacticsBoard | 战术分析 | 需标题验证 |

---

## Excluded / Low-Trust Sources

以下类型的频道直接跳过，不进入候选池：

### 明确排除类型

| 类型 | 典型频道特征 | 排除原因 |
|---|---|---|
| 博彩预测频道 | 标题含 "tips", "predictions", "odds", "accumulator" | 无战术价值 |
| 转会谣言频道 | 标题含 "transfer news", "signing", "done deal" | 无战术价值 |
| 球迷情绪频道 | 标题含 "rant", "reaction", "angry", "disaster" | 无战术内容 |
| 纯集锦频道 | 标题含 "highlights", "goals", "best moments" | 无战术分析 |
| 点击诱饵频道 | 标题全大写、大量感叹号、无具体战术主张 | 低质量内容 |

### 已知低质量频道（持续更新）

> 此列表随使用经验更新，初始为空。

---

## 频道更新规则

1. **新增 Tier 1**：需要至少 3 个视频样本验证，且视频质量达到"战术主张明确、有数据或视频证据支撑"
2. **从 Tier 2 升级到 Tier 1**：需要用户明确确认该频道的战术分析质量
3. **降级到 Excluded**：若频道内容质量持续低于标准，或主要内容转向博彩/谣言
4. **更新记录**：每次更新需在本文档末尾记录变更日期和原因

---

## 变更记录

| 日期 | 变更内容 | 原因 |
|---|---|---|
| 2026-05-22 | 初始版本创建 | LMW-103 |
