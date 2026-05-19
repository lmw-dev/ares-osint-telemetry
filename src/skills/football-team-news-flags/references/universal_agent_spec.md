# Universal Agent Specification

本文件可作为 Manus Skill 之外的独立 Agent 规范使用，适合放入 Claude Code 的 `AGENTS.md`、Codex 项目的 `instructions.md`，或任意自动化脚本仓库的任务规范中。

## Purpose

该 Agent 的职责是为指定足球比赛或联赛轮次收集球队关键异常信息，并输出可审计的 `news_status` 与 `news_flags`。它不是通用新闻摘要器，也不是预测模型。它只判断公开信息中是否存在足以影响赛前判断的异常信号。

## Input protocol

| Field | Required | Description | Example |
|---|---:|---|---|
| `league` | Yes | 联赛或赛事名称 | `English Premier League` |
| `season` | No | 赛季 | `2025/26` |
| `round` | No | 轮次或阶段 | `Matchweek 37` |
| `date_window` | Yes | 比赛日期或时间窗口 | `2026-05-17 to 2026-05-19` |
| `matches` | Yes | 主客队、开球时间、场地等 | `Arsenal vs Burnley` |
| `canonical_flags` | Yes | 允许使用的标签列表 | 见下方 |
| `source_preferences` | No | 优先来源或禁用来源 | 官方、发布会、可靠媒体 |
| `output_formats` | No | 需要的交付格式 | `markdown,json` |
| `lineup_mode` | No | 是否已有官方首发 | `pre_lineup` 或 `post_lineup` |

## Output protocol

最终输出必须包含用户可读报告和机器可读数据。机器可读数据中的每队记录必须满足：当 `news_status` 为 `暂无明显异常` 时，`news_flags` 必须为空数组；当 `news_status` 为 `有异常` 时，`news_flags` 必须至少包含一个规范标签。

```json
{
  "meta": {
    "league": "...",
    "season": "...",
    "round": "...",
    "date": "...",
    "generated_at": "...",
    "flag_taxonomy": ["主帅下课/更衣室问题", "核心球员伤停"]
  },
  "teams": [
    {
      "team": "...",
      "opponent": "...",
      "home_away": "home",
      "fixture": "...",
      "kickoff": "...",
      "news_status": "暂无明显异常",
      "news_flags": [],
      "key_abnormalities": "未核实到足以进入指定 news_flags 的重大异常。",
      "sources": [],
      "confidence": "medium",
      "review_notes": "No qualifying flag after review."
    }
  ]
}
```

## Agent states

| State | Entry condition | Exit condition |
|---|---|---|
| `scope_confirmed` | 已获得联赛、日期和比赛列表 | 已构建每队输入表 |
| `evidence_collecting` | 每队输入表就绪 | 每队至少完成基础检索或标注无法检索 |
| `preliminary_flagging` | 候选来源文本就绪 | 每队生成初步 JSON |
| `normalizing` | 初步 JSON 就绪 | 所有 flags 映射到规范标签 |
| `reviewing` | 规范化记录就绪 | 删除弱信号、错配、非规范标签 |
| `reporting` | 审校记录就绪 | Markdown 与 JSON 完成 |

## Decision policy

Agent 应采用保守入旗策略。若一个事实无法被来源证明，宁可不入旗。若事实存在但影响不足，写入背景或删除。若冲突来源同时存在，优先官方、最新、直接引用发布会的来源。若只有弱来源，应将其作为检索线索而非最终证据。

## Canonical flags

```text
主帅下课/更衣室问题
核心球员伤停
多名主力轮换
杯赛前后
欧战资格已锁定
已降级/已夺冠/已无欲无求
球队近期负面舆论明显
临场阵容确认后有重大变化
```

## Quality gates

交付前必须通过五个质量门。第一，球队覆盖完整，不能漏队。第二，标签合法，不能出现非规范标签。第三，证据闭环，每个正向标签均可追溯到来源。第四，语义一致，普通战意不得被误写为异常。第五，报告简洁，`暂无明显异常` 行不得被冗余新闻污染。
