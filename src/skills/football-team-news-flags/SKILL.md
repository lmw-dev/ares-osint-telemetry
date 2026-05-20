---
name: football-team-news-flags
version: "2.0"
source: ares-osint-telemetry native
description: >
  足球赛前球队关键异常信息收集工作流。
  用于收集、判定、规范化并输出影响赛前判断的重大异常标志，包括：
  主帅更换、更衣室问题、核心伤停/停赛、多名主力轮换、杯赛前后、
  欧战资格已锁定、已降级/已夺冠/无欲无求、负面舆论、临场大名单突变。
  适用场景：赛前球队信息日报、全轮异常扫描、博彩背景分析。
  V2.0：模型无关的 Agent Workflow 架构——无论 Claude、Gemini 还是其他模型，
  在 Antigravity 中调用时，由当前活跃模型直接执行全流程，
  利用 search_web / read_url_content 主动联网采集情报，无需调用外部 LLM API。
inputs:
  - league: 联赛名称（英超/西甲/意甲/德甲/法甲）
  - round_or_date: 轮次或比赛日期窗口（必填）
  - matches: 比赛列表（可选，若未提供则自动搜索获取）
outputs:
  - Markdown 报告 → /vaults/AresVault/03_Match_Audits/DATE-{date}-top5/{联赛中文名} {season} 第{N}轮关键异常信息汇总_Ares.md
  - JSON 数据 → 同目录 .json 文件
---

# Football Team News Flags V2.0

## 执行模式说明

本 Skill 是一套 **模型无关的 Agent 执行规范**。当你（无论是 Claude、Gemini 还是其他模型）在 Antigravity 中被用户通过 `@skill` 调用时，请按照本文件的步骤逐一执行。

**你就是分析引擎。** 不需要调用任何外部 LLM API（如 DeepSeek、OpenAI 等）。你自身的推理能力就是分析能力。你的工具（`search_web`、`read_url_content`、`write_to_file`）就是情报采集工具。

---

## 核心原则

1. **保留用户的 flag 分类法。** 不得发明新 flag，除非用户明确扩展列表。
2. 将普通战意、常规预赛乐观、例行轻伤、泛泛状态评论视为**背景信息**，而非异常标志。
3. 使用 **`news_status: 暂无明显异常`** 当无可验证的合格信号时。此时 `news_flags` 置空数组，仅写一句简洁校准语，不填充低价值新闻。
4. **收集与判定分离。** 先附带来源收集候选事实，再规范化标志、排除弱信号，最后输出成品。
5. **零幻觉铁律。** 绝对禁止编造任何不在搜索结果或来源中的事实。宁缺毋滥。

---

## 规范标志（Canonical Flags）

| Flag | 触发条件 |
|---|---|
| `主帅下课/更衣室问题` | 主帅正式下课、临时主帅、公开更衣室冲突、俱乐部治理危机 |
| `核心球员伤停` | 核心首发、队长、主力门将/中卫/中场/射手确认缺席、停赛或高度伤疑 |
| `多名主力轮换` | 主帅明确表示大规模轮换，或官方首发显示至少3名关键首发被替换 |
| `杯赛前后` | 比赛前后紧邻重要杯赛/洲际赛/决赛，且间隔足以影响体能或选择 |
| `欧战资格已锁定` | 数学上已锁定欧战资格，当前联赛目标结构性变化 |
| `已降级/已夺冠/已无欲无求` | 数学降级、夺冠、保级且无可达到的更高目标 |
| `球队近期负面舆论明显` | 球迷抗议、管理层危机、严重连败被定性为危机、纪律/财务事件 |
| `临场阵容确认后有重大变化` | 官方首发公布后核心球员突然缺席/替补，仅可在官方首发后使用 |

> 详细触发/排除规则见 `references/flag_taxonomy_and_review.md`

---

## 情报源分层体系 (Evidence Source Tiers)

按"证据强度"分层搜索。核心准则：**官方 > 发布会/跟队 > 主流媒体 > 专业数据库 > 社媒线索**。

| Tier | 来源类型 | 证据强度 | 可否单独入旗 | 典型来源 |
|:---:|:---|:---:|:---:|:---|
| **T1** | 俱乐部/联赛官方公告 | 🟢 最强 | ✅ 可以 | 俱乐部官网、英超/西甲/意甲/德甲/法甲官网、UEFA、FIFA |
| **T1** | 官方社媒与赛前发布会 | 🟢 最强 | ✅ 可以 | 俱乐部 X/Twitter、YouTube 发布会、官方 App |
| **T2** | 可靠主流/本地媒体 | 🟡 强 | ✅ 可以 | BBC Sport、Sky Sports、The Athletic、ESPN、Reuters、AP、当地跟队媒体 |
| **T2** | 联赛专业媒体 | 🟡 强 | ✅ 可以 | Football Italia、Liverpool Echo、Manchester Evening News、Get German Football News |
| **T3** | 伤停/停赛数据库 | 🔵 中 | ⚠️ 需交叉验证 | Transfermarkt、FotMob、SofaScore、Flashscore、WhoScored |
| **T3** | 积分榜/赛程数据 | 🔵 中 | ⚠️ 仅用于数学判定 | Soccerway、FBref、ESPN standings、官方积分榜 |
| **T4** | 预测首发/预览站 | ⚪ 弱 | ❌ 仅作线索 | WhoScored previews、Sports Mole、90min、FotMob lineup prediction |
| **T5** | 社媒/论坛/聚合站 | 🔴 最弱 | ❌ 不作证据 | Reddit、球迷论坛、非认证 X 账号、聚合新闻页 |

**关键规则**：
- T4/T5 来源**永远不能单独入旗**。只能作为线索去查找 T1-T3 级证据验证。
- 多名主力轮换：仅当主帅发布会、可靠跟队记者、官方首发或多家 T1-T2 来源共同支持时才入旗。
- 社媒爆料不直接进入 `news_flags`，除非能被 T1-T2 来源确认。

---

## 联赛-语言搜索策略映射表

| 联赛 | 英文搜索 | 本地语言搜索 | 关键本地媒体 |
|:---:|:---|:---|:---|
| **EPL (英超)** | ✅ 默认 | — | BBC Sport, Sky Sports, The Athletic, Guardian, Liverpool Echo, MEN |
| **La Liga (西甲)** | ✅ | 🇪🇸 西班牙语辅助 | Marca, AS, Mundo Deportivo, Sport, Diario de Sevilla |
| **Serie A (意甲)** | ✅ | 🇮🇹 意大利语辅助 | Gazzetta dello Sport, Corriere dello Sport, Tuttomercatoweb, Football Italia |
| **Bundesliga (德甲)** | ✅ | 🇩🇪 德语辅助 | Kicker, Sport1, Bild Sport, Sportbuzzer |
| **Ligue 1 (法甲)** | ✅ | 🇫🇷 法语辅助 | L'Équipe, RMC Sport, Foot Mercato, Le Parisien |

---

## 搜索查询模板

对每支球队，按以下模板构建搜索查询：

```text
# 基础查询（每队必做）
"{team_english}" team news injury suspension {YYYY-MM-DD}
"{team_english}" press conference {opponent} {round}

# 扩展查询（按需）
"{team_english}" predicted lineup rotation {opponent} {date}
"{team_english}" manager dressing room crisis latest

# 本地语言查询（非英超联赛时追加）
"{team_local_name}" lesiones baja convocatoria {date}        # 西甲
"{team_local_name}" infortunio squalifica conferenza {date}   # 意甲
"{team_local_name}" Verletzung Sperre Pressekonferenz {date}  # 德甲
"{team_local_name}" blessure suspension conférence {date}     # 法甲

# 联赛全局查询（每轮只做一次）
{league} standings {season} matchweek {round}
{league} relegated promoted qualified champion {date}
```

---

## 执行流程（5 Phases）

### Phase 1: 范围确认与赛程锁定

**目标**：确定联赛、轮次、比赛列表、当前积分榜。

**步骤**：
1. 从用户输入中提取 `league`、`round_or_date`、`matches`
2. 如果用户未提供完整比赛列表，使用 `search_web` 搜索当轮赛程：
   - 查询：`"{league}" fixtures matchweek {round} {date}`
3. 使用 `search_web` 获取最新积分榜：
   - 查询：`"{league}" standings table {season} {date}`
4. 如果搜索结果不够详细，用 `read_url_content` 抓取积分榜页面（如 ESPN standings 或官方联赛页）
5. 构建输出：每队记录 `team`、`opponent`、`home_away`、`fixture`、`kickoff`、`current_rank`、`current_points`、`season_objective`

**本地数据增强**（可选）：
如果本地存在预抓取数据（如 `/vaults/AresVault/03_Match_Audits/` 中的 manifests 或 TEAM-INTEL JSON），可以读取作为补充上下文，但不作为唯一来源。

---

### Phase 2: 分层情报采集

**目标**：为每支球队收集候选事实。这是 V2.0 的核心升级。

**执行策略**：

对全轮 20 支球队（10 场比赛），按以下策略分批采集：

#### 2.1 联赛全局情报（执行 1 次）
```
search_web("{league} matchweek {round} team news injury preview {date}")
search_web("{league} {round} press conference manager quotes")
```
- 从全局搜索结果中提取关键发现，标记涉及的球队

#### 2.2 逐队深度采集（每队执行 2-3 次搜索）

对每支球队：

**Step A: 主搜索**
```
search_web("{team} team news injury suspension {date}")
```
- 阅读搜索结果摘要
- 标记高价值链接（T1-T3 来源优先）

**Step B: 发布会/阵容搜索**
```
search_web("{team} press conference lineup {opponent} {date}")
```
- 查找主帅赛前发布会内容
- 查找预计首发或官方大名单

**Step C: 本地语言搜索**（仅非英超联赛）
```
search_web("{team_local_name} {本地语言关键词} {date}")
```

**Step D: 深度抓取**（按需）
- 对搜索结果中最高价值的 1-2 个链接使用 `read_url_content` 获取全文
- 优先抓取：俱乐部官方伤停页、权威媒体的详细赛前分析、发布会转录

#### 2.3 候选事实记录

对每支球队，收集到的候选事实按以下格式整理：

```
球队: {team}
候选事实:
  1. [事实描述] — 来源: [URL/媒体名] — Tier: T1/T2/T3/T4/T5
  2. [事实描述] — 来源: [URL/媒体名] — Tier: T1/T2/T3/T4/T5
  ...
```

---

### Phase 3: 逐队独立研判

**目标**：对每队的候选事实执行 Flag 分类和入旗判定。

**判定逻辑**：

```
对每支球队:
  1. 检查候选事实列表
  2. 对每条事实:
     a. 确认 Tier 等级
     b. 判断是否匹配某个 Canonical Flag 的触发条件
     c. 如果匹配:
        - T1/T2 来源 → 直接入旗
        - T3 来源 → 需要至少一条 T1/T2 来源交叉验证
        - T4/T5 来源 → 不入旗，仅作背景提及
  3. 应用排除测试:
     - 普通战意（保级压力、争冠压力）→ 不是异常，作为背景
     - 边缘球员轻伤 → 不触发"核心球员伤停"
     - 无署名轮换猜测 → 不触发"多名主力轮换"
     - 对手信息混入本队 → 删除
  4. 生成 JSON 记录
```

**空值降级规则**：
- 如果搜索后仍然没有 T1-T3 级证据支持任何异常 → 必须判定 `news_status: "暂无明显异常"`
- 绝对禁止因为"没搜到信息"而编造假事实来填充报告

---

### Phase 4: 横向审校

**目标**：全联赛视角的质量门检查。

**审校清单**：

1. **覆盖完整性**：每支球队恰好一条记录，不漏不重
2. **标签合法性**：`news_flags` 中只有 8 个 Canonical Flag，无非法标签
3. **证据闭环**：每个正向 Flag 至少有一个 T1-T3 级来源支撑
4. **对称性**：同场比赛双方不得混写（对手已降级不能写在本队异常中）
5. **一致性**：积分榜状态不得在球队间矛盾（如 A 队被标为"已降级"但积分明显不是最后）
6. **零幻觉终审**：
   - 检查所有 key_abnormalities 中提到的主帅引言、杯赛日期、球员名字是否有搜索结果支持
   - 若发现任何无来源事实，立即删除或降级

---

### Phase 5: 输出渲染

**目标**：生成最终的 Markdown 报告和结构化 JSON。

#### 5.1 Markdown 报告格式

```markdown
# 📋 {联赛中文名}第{N}轮赛前异常情报报告

> **报告生成时间**: {当前时间}
> **数据源**: 开源情报实时采集 (OSINT Telemetry V2.0)
> **分析引擎**: {当前模型名称}
> **覆盖球队**: {N} 支 / {M} 场比赛

---

## 一、全局异常状态概览

> [!NOTE] 本轮综述
> {简洁的一段话全局概要}

| 球队 | 对阵与时间 | 异常状态 | 规范标志 | 关键异常与深度推演 | 置信度 |
|------|-----------|---------|---------|------------------|--------|
| ... |

---

## 二、强异常信号深度技战术剖析

（仅对 news_status == "有异常" 的球队展开分析）

### {N}. {球队名} ({对阵信息})

> [!WARNING] / [!CAUTION]
> **{异常标题}**

- **核心事实**：
  - {从搜索结果中确认的具体事实}
- **战术传导推演**：
  - {基于确认事实的战术影响分析}

---

## 三、情报来源与溯源 (References & Footnotes)

| 球队 | 情报来源 | 证据类型 | Tier | 支持标志 |
|------|---------|---------|:---:|---------|
| ... |

---
*报告结束 | 数据真实性承诺：本报告所有内容基于实时搜索采集的真实数据*
```

#### 5.2 JSON 数据格式

```json
{
  "meta": {
    "league": "...",
    "season": "...",
    "round": "...",
    "date": "...",
    "generated_at": "...",
    "model": "当前执行模型",
    "version": "2.0"
  },
  "teams": [
    {
      "team": "球队名",
      "opponent": "对手名",
      "home_away": "Home/Away",
      "fixture": "主队 vs 客队",
      "kickoff": "YYYY-MM-DD HH:mm",
      "news_status": "暂无明显异常 | 有异常",
      "news_flags": [],
      "key_abnormalities": "简洁说明",
      "sources": [
        {
          "title": "来源标题",
          "url": "https://...",
          "publisher": "发布方",
          "evidence_type": "official | press_conference | reliable_media | injury_database | standings | weak_lead",
          "tier": "T1 | T2 | T3 | T4 | T5",
          "supports_flags": ["flag1"]
        }
      ],
      "confidence": "high | medium | low",
      "review_notes": "判定依据说明"
    }
  ]
}
```

#### 5.3 输出路径

使用 `write_to_file` 保存到以下路径：

```
# Markdown 报告
/Users/liumingwei/vaults/AresVault/03_Match_Audits/DATE-{YYYYMMDD}-top5/{联赛中文名} {season} 第{N}轮关键异常信息汇总_Ares.md

# JSON 数据
/Users/liumingwei/vaults/AresVault/03_Match_Audits/DATE-{YYYYMMDD}-top5/{联赛中文名} {season} 第{N}轮关键异常信息汇总_Ares.json
```

其中 `{YYYYMMDD}` 取该轮比赛中**最多场次的那个日期**。

---

## 零幻觉安全锁 (Zero-Hallucination Safe Lock)

在执行全流程中，必须始终遵守以下铁律：

1. **只写你搜到的**：任何写入 `key_abnormalities` 或 `news_flags` 的事实，必须有至少一条搜索结果或抓取页面的明确支持。
2. **严禁编造引言**：不得编造主帅在发布会上的原声发言。如果搜索结果中没有具体引言，只写"据 {来源} 报道，{概要}"。
3. **严禁时空漂移**：不得凭记忆臆造杯赛日期、决赛对阵、积分排名等事实。所有此类信息必须从当前搜索结果中获取。
4. **空值不可耻**：如果一支球队搜不到任何有价值的异常情报，`news_status: "暂无明显异常"` 是完全正确的输出。不要为了"让报告看起来丰满"而编造内容。
5. **来源可追溯**：每个正向 Flag 必须在 `sources` 数组中有对应的来源条目，且来源必须是你实际搜索或抓取到的。

---

## 验证清单

交付前确认：
- [ ] 每队恰好一条最终记录
- [ ] `news_status` 枚举值正确：`暂无明显异常` 或 `有异常`
- [ ] `news_flags` 只使用 8 个规范标签
- [ ] 每个正向 flag 至少有一个 T1-T3 来源
- [ ] `暂无明显异常` 行不含投机性异常描述
- [ ] 最终成品包含 Markdown 报告和结构化 JSON
- [ ] 所有来源 URL 真实存在（来自搜索结果）
- [ ] 无编造的主帅引言、杯赛日期或球员名字

---

## 打包资源

- `prompts/agent_prompts.md` — 分析框架参考文档（System/Worker/Reviewer 分析视角，供理解分析逻辑）
- `references/flag_taxonomy_and_review.md` — 边界案例处理、触发/排除规则（**必读**）
- `references/pseudocode.md` — 算法流程图参考
- `references/universal_agent_spec.md` — 输入输出协议规范
- `templates/team_flags_schema.json` — JSON Schema
- `templates/team_news_report_template.md` — Markdown 报告模板
