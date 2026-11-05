---
name: ares-fotmob-roster-collector
version: "1.1"
source: ares-osint-telemetry native
description: >
  Ares 生产级 FotMob 赛前阵容与球队新闻情报采集 Skill (v1.1 验收加固版)。
  实现赛事实体独立绑定（彻底杜绝将 internal/Titan ID 直接拼接为 FotMob URL 的缺陷）、
  支持 PREMATCH 与 FINAL_REFRESH 双运行模式、首发名单（Confirmed/Predicted）提取、
  伤停/停赛/训练恢复正交分类萃取、赛前情报增量比对（Roster Delta）及全流程证据血统持久化。
inputs:
  - project_fixture_id: 项目内部/Titan 赛事 ID（严禁直接作为 FotMob match ID）
  - league: 联赛名称（如 EPL_GW05、LaLiga_GW07、SerieA、Bundesliga、Ligue1）
  - season: 赛季（如 "2026/27"）
  - round: 轮次（如 "5"、"7"）
  - home_team: 主队名称（支持中文或标准英文名）
  - away_team: 客队名称（支持中文或标准英文名）
  - kickoff_utc: 比赛 UTC 开球时间（ISO 8601，如 "2026-09-20T13:00:00Z"）
  - kickoff_cst: 比赛北京时间（可选，如 "2026-09-20 21:00:00"）
  - mode: 运行模式（PREMATCH 或 FINAL_REFRESH，默认 PREMATCH）
  - existing_binding: 已有绑定字典（可选，已绑定时直接复用，避免重复寻址）
  - previous_roster_snapshot_path: 上一阶段赛前快照路径（FINAL_REFRESH 模式下必填）
  - output_dir: 产物落盘目录（可选）
  - force_fetch_post_kickoff: 是否在已完赛状态下仍强制抓取页面（仅供离线审计/冒烟测试，默认 false）
  - reschedule_proven: 是否具有显式改期证据（默认 false）
outputs:
  - 00-fixture-binding.json: 规范 FotMob 实体绑定与置信度审计
  - 01-fotmob-prematch-roster.json: 赛前分析期完整阵容与伤停快照
  - 02-fotmob-prematch-news.json: 规范语义新闻（publisher, headline, source_url, published_at, official）
  - 03-fotmob-final-roster.json: 临场终极刷新阵容与伤停快照
  - 04-fotmob-roster-delta.json: 赛前至临场阵容与伤停确定性增量对比
source_policy:
  status: CAUTION
  note: "Automated/systematic FotMob access requires separate production-policy review."
official_source_fallback: NOT_IMPLEMENTED
---

# Ares FotMob Roster & Team-News Collector Skill (v1.1)

## 0. 为什么存在此 Skill (Why This Skill Exists)

### 生产缺陷根治 (Production Defect Addressed)
在过往流水线中，容易产生一种机制缺陷：直接将项目内部或 Titan007 的赛事 ID（如 `3013703`）直接拼接到 FotMob 路径生成伪 URL（如 `https://www.fotmob.com/match/3013703`）。
**事实：FotMob 拥有其完全独立的内部赛事 ID 命名空间（例如马德里德比在 FotMob 上的真实 ID 为 `5868072`，页面路径为 `/matches/real-madrid-vs-atletico-madrid/2ubozo#5868072`）。**

本 Skill 建立了一套高可靠、多维度的实体绑定机制：
`project_fixture_id` $\rightarrow$ 多维属性解析（联赛 + 主队 + 客队 + 开球日期） $\rightarrow$ 探测并校准 FotMob 官方全量赛程 $\rightarrow$ 锁定规范 FotMob 实体 URL 与真实 match ID $\rightarrow$ 持久化绑定。
一旦绑定成功，Prematch / Final Refresh / Postmatch 将直接复用此实体标识，杜绝重复寻址与错误拼接。

---

## 1. 职责边界划分 (Role Boundary)

- **Antigravity (本 Skill 执行引擎)**：
  - 外部数据源采集与解析（优选真实 FotMob match_id $\rightarrow$ `/match/<id>` 跟随重定向并硬性断言 parsed match id）
  - FotMob 赛事实体多维解析与规范绑定
  - 首发大名单（Confirmed / Predicted）、阵型、伤停、停赛、恢复训练提取
  - 赛前新闻事实萃取（规范化新闻语义：publisher, headline, source_url, published_at, official）
  - 赛前 $\rightarrow$ 终极刷新（Prematch $\rightarrow$ Final Refresh）阵容确定性增量比对
  - 严禁产生任何主观预测、指数判断、Fair Line、投注建议或 LEAN/EDGE。
- **Ares (下游核心系统)**：
  - 证据与绑定有效性验证、门禁状态（Gate）判定、初步分析生成、交接封包。
- **ChatGPT (终审中枢)**：
  - 审计全部证据、反向证据推演、Fair Line 确立、终极盘口解释与决策。

---

## 2. 运行模式与生命周期契约 (Modes & Lifecycle)

### 双运行模式
1. **`PREMATCH` (赛前分析模式)**：
   - 检查或生成 `00-fixture-binding.json`
   - 提取首发大名单（若尚未官方公布，则提取预测首发并标记为 `PREDICTED`，绝不阻塞流程）
   - 提取伤停、停赛、恢复训练名单
   - 提取赛前新闻，输出 `01-fotmob-prematch-roster.json` 与 `02-fotmob-prematch-news.json`
2. **`FINAL_REFRESH` (临场刷新模式)**：
   - 严格复用已持久化的 `00-fixture-binding.json`，严禁重新探测（`fotmob_rediscovery = 0`）
   - 抓取临场最新状态，输出 `03-fotmob-final-roster.json`
   - 读取前期 `01-fotmob-prematch-roster.json`，计算确定性差异，输出 `04-fotmob-roster-delta.json`

### 生命周期锁 (Lifecycle Lock)
- 执行采集前强制对比：**当前实际运行时间 (UTC) vs 比赛开球时间 (UTC)**
- 若 `实际时间 < 开球时间`：判定为 `ACTIVE_PREMATCH` 或 `ACTIVE_FINAL_REFRESH`，正常采集；
- 若 `实际时间 >= 开球时间`：判定为 `POST_KICKOFF_LOCKED`，**严禁抓取走地或滚球阵容**，保留已有静态赛前事实，标记 `execution_usable: false`。
- 若处于离线审计或冒烟验证需要，显式开启 `force_fetch_post_kickoff: true`，但依然强制保持 `execution_usable: false`。

---

## 3. 实体绑定与数据抓取规范 (v1.1 Acceptance Gaps Fixed)

### 3.1 实体匹配优先级 (Binding Priorities)
严禁全局丢弃具有实体标识意义的词元（如 `city`, `united`, `real`, `athletic`, `club`, `town` 等），仅过滤纯语法俱乐部缩写（`fc`, `cf`, `vfb` 等）：
1. **Priority 1**: Known alias / exact external identity match（映射字典精确命中）
2. **Priority 2**: Normalized full name exact match（全词元无遗漏精确匹配）
3. **Priority 3**: Token similarity fallback（Jaccard 词元交集回退）

### 3.2 开球时间窗口收紧 (Kickoff Matching Window)
- $\le 3\text{h}$: PASS candidate
- $3\text{–}12\text{h}$: CAUTION
- $> 12\text{h}$: 拒绝（Reject），除非显式提供 `reschedule_proven: true` 元数据。

### 3.3 比赛 ID 硬性断言 (Match-ID Hard Assertion)
- 抓取页面优选路径：`https://www.fotmob.com/match/{bound_fotmob_match_id}`，自动跟随重定向；
- 页面必须解析并验证：`parsed_fotmob_match_id == bound_fotmob_match_id`；
- 一旦不匹配，立刻终止该赛事并报告 `FOTMOB_PAGE_IDENTITY_MISMATCH`；
- 提取 parsed page 的主队、客队、开球时间，双向印证。

### 3.4 SSR 机制与 1 次有界 Fallback (SSR & 1-Bounded Fallback)
- 主通道通过 Next.js 服务端内嵌状态树 `<script id="__NEXT_DATA__">` 提取；
- 若出现 `props.pageProps.content` 缺失或延迟加载（deferred），允许**恰好一次**有界的备用渲染路径抓取；
- 评定状态为 `PASS / PARTIAL / UNKNOWN`，严禁死循环重试；
- 移除任何将 SSR 称作“100% stable”的过度宣称。

### 3.5 官方源回退声明 (Official Source Fallback)
- 目前系统未接入俱乐部官网/官方 RSS 爬虫，明确标记为：`OFFICIAL_SOURCE_FALLBACK: "NOT_IMPLEMENTED"`。

### 3.6 新闻语义结构化 (News Semantics)
- FotMob 托管新闻不自动标记为 official；
- 输出字段统一为：`publisher`, `headline`, `short_summary`, `source_url`, `published_at`, `official`（boolean）。

### 3.7 源政策声明 (Source Policy)
- `SOURCE_POLICY_STATUS = "CAUTION"`，自动化或系统化访问 FotMob 必须遵循生产合规审查。

---

## 4. 规范文件结构与 Schema

```text
<fixture_id>/
├── 00-fixture-binding.json         # FotMob 唯一绑定及审计证据
├── 01-fotmob-prematch-roster.json  # 赛前阵容、阵型与伤停明细
├── 02-fotmob-prematch-news.json    # 规范新闻语义明细
├── 03-fotmob-final-roster.json     # [FINAL_REFRESH] 临场阵容刷新
└── 04-fotmob-roster-delta.json     # [FINAL_REFRESH] 确定性增量变化
```
