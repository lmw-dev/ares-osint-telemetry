---
name: football-team-news-flags
version: "1.0"
source: manus-skill-v1 (ported to ares-osint-telemetry)
description: >
  足球赛前球队关键异常信息收集工作流。
  用于收集、判定、规范化并输出影响赛前判断的重大异常标志，包括：
  主帅更换、更衣室问题、核心伤停/停赛、多名主力轮换、杯赛前后、
  欧战资格已锁定、已降级/已夺冠/无欲无求、负面舆论、临场大名单突变。
  适用场景：赛前球队信息日报、全轮异常扫描、博彩背景分析、Claude/Codex Agent 工作流。
inputs:
  - league: 联赛名称
  - season: 赛季（可选）
  - round: 轮次（可选）
  - date_window: 比赛日期或时间窗口（必填）
  - matches: 比赛列表（主队、客队、开球时间）
  - lineup_mode: pre_lineup 或 post_lineup（默认 pre_lineup）
outputs:
  - reports/team_news_flags_{league}_{date}.md   # Markdown 报告
  - reports/team_news_flags_{league}_{date}.json # JSON 结构化数据
---

# Football Team News Flags

## 核心原则

**保留用户的 flag 分类法。** 不得发明新 flag，除非用户明确扩展列表。
将普通战意、常规预赛乐观、例行轻伤、泛泛状态评论视为背景信息，而非异常标志。

使用 **`news_status: 暂无明显异常`** 当无可验证的合格信号时。此时 `news_flags` 置空数组，仅写一句简洁校准语，不填充低价值新闻。

**将收集与判定分离。** 先附带来源收集候选事实，再规范化标志、排除弱信号，最后输出 Markdown/JSON 成品。

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

## 标准执行流程

1. **确认范围** — 联赛、轮次/日期、开球窗口、比赛列表、语言偏好、输出格式
2. **构建输入表** — 每队记录 `team`、`opponent`、`home_away`、`fixture`、`kickoff`、已知积分榜/战意背景
3. **收集候选事实** — 官方俱乐部/联赛来源、发布会报告、权威预赛媒体、伤停/停赛页面、积分榜/赛程置换
4. **逐队独立核查** — 对多球队任务，每队独立运行 `prompts/agent_prompts.md` 中的 single-team worker prompt，再合并结果
5. **规范化证据** — 将自由文本标签转换为规范 flag；删除不规范标签如 `保级压力`、`争冠压力`、`状态低迷`
6. **应用排除测试** — 排除弱信号、边缘球员轻伤、泛泛状态叙述、无来源轮换猜测、对手异常写入本队
7. **一致性审校** — 确认同一比赛双方横向对比；联赛积分榜状态不得非对称应用
8. **输出成品** — Markdown 报告 + JSON 结构化数据

## 推荐来源优先级

1. 官方俱乐部/联赛公告、发布会文字记录（最强）
2. 权威专业体育媒体、有署名的驻队记者报道
3. 专业伤停数据库（如 Transfermarkt 伤停、官方医疗公告汇总）
4. 积分榜、赛程、数学置换（用于战意状态判定）
5. 社交媒体转述、无署名预测站、AI 式预览（仅作线索，不作最终证据）

## 输出契约

```yaml
team: 球队名
fixture: 主队 vs 客队
kickoff: YYYY-MM-DD HH:mm TZ
news_status: 暂无明显异常 | 有异常
news_flags: []
key_abnormalities: 一句基于证据的简洁说明。
sources:
  - title: 来源标题
    url: https://...
    evidence_type: official | press_conference | reliable_media | standings | injury_database | weak_lead
confidence: high | medium | low
review_notes: 说明为何入旗或排除；如有不确定性请说明。
```

> 完整 JSON Schema 见 `templates/team_flags_schema.json`
> Markdown 报告模板见 `templates/team_news_report_template.md`

## 平台适配

本 skill 工具无关。本项目（ares-osint-telemetry）中的使用方式：

- 输入：`data/input_matches.json`（联赛、轮次、日期、球队、赛程）
- 中间产物：`raw_reports/team_news_raw_{date}.jsonl`（每队候选事实和初步 flags）
- 最终输出：`draft_reports/team_news_flags_{league}_{date}.md` 和 `.json`
- 若无浏览器环境，可由外部系统提供链接列表，模型执行证据读取、判定、审校

> 不同平台详细适配方案见 `references/platform_adapter.md`

## 搜索查询模板

```text
{team} team news {opponent} {date}
{team} injury suspension press conference {round}
{team} predicted lineup rotation cup final
{league} standings relegated qualified champion {date}
{team_local_name} injury {opponent_local_name}
```

## 验证清单

交付前确认：
- [ ] 每队恰好一条最终记录
- [ ] `news_status` 枚举值正确：`暂无明显异常` 或 `有异常`
- [ ] `news_flags` 只使用规范标签
- [ ] 每个正向 flag 至少有一个可信来源或显式 `confidence: low` 说明
- [ ] `暂无明显异常` 行不含投机性异常描述
- [ ] 最终成品包含 Markdown 报告和结构化 JSON/CSV

## 打包资源

- `prompts/agent_prompts.md` — System Prompt、User Prompt、单队 worker prompt、reviewer prompt
- `references/flag_taxonomy_and_review.md` — 边界案例处理、触发/排除规则
- `references/platform_adapter.md` — 各平台实现方式（Manus/Claude Code/Codex）
- `references/pseudocode.md` — 平台无关算法和 Mermaid 流程图
- `references/universal_agent_spec.md` — 独立 Agent 规范（可用于 AGENTS.md）
- `templates/team_flags_schema.json` — JSON Schema（2020-12）
- `templates/team_news_report_template.md` — Markdown 报告模板
