# Platform Adapter

本规范是平台无关的 Agent 工作流。不同环境可以使用不同工具完成检索、并行、存储和审校，但必须保持同一套输入输出协议、异常标签定义和证据审校标准。

## Invariants

无论平台如何，必须保留四个不变量。第一，`news_flags` 只能使用规范标签。第二，每个正向异常必须能回指到来源。第三，`暂无明显异常` 不是“没有新闻”，而是“没有达到入旗阈值的异常”。第四，最终输出必须经过二次审校，不得直接交付单轮搜索结果。

## Manus implementation

在 Manus 中，适合把球队列表拆成队级并行子任务。每个子任务只判断一支球队，返回结构化字段、来源和置信度。主任务随后合并、规范化、删除非规范标签，并写成 Markdown 与 JSON。浏览器和网页提取适合读取公开预览、官方新闻和积分榜；文件工具适合保存中间证据与最终产物。

| 阶段 | Manus 做法 | 注意事项 |
|---|---|---|
| 搜索 | 使用网页检索和公开页面抽取 | 不要只看搜索摘要；重要事实要打开来源核验。 |
| 并行 | 每队一个独立核查任务 | 子任务不得引用其他球队结论来替代本队证据。 |
| 合并 | 主任务统一清洗 flags | 删除 `保级压力`、`争冠压力` 等非规范标签。 |
| 交付 | 附 Markdown 与 JSON/CSV | 保留引用和低置信度说明。 |

## Claude Code implementation

在 Claude Code 中，可以把本 Skill 作为项目内 `AGENTS.md` 或 `skills/football-team-news-flags/` 文档使用。推荐创建 `data/input_matches.json`、`data/raw_team_checks.jsonl`、`reports/team_news_flags.md` 和 `reports/team_news_flags.json`。若环境没有浏览器，可要求用户提供链接清单，或使用命令行 HTTP、新闻 API、RSS、搜索 API 获取文本。

建议流程是：先让模型生成队级搜索查询；再用脚本或人工导入方式收集页面文本；然后让模型基于本规范判定每队异常；最后运行审校提示词。Claude Code 不一定有原生并行工具，因此可以用批处理 JSONL 或分文件方式模拟并行。

## Codex implementation

在 Codex 或类似代码代理中，建议把此任务实现为可重复脚本流水线。脚本负责读取输入、调用搜索/API、缓存页面、抽取文本和输出候选 JSON；模型负责解释证据、规范化标签和写报告。若使用 OpenAI/兼容模型 API，单队 worker prompt 可逐队调用，结果写入 JSONL，再由 reviewer prompt 合并。

| 文件 | 用途 |
|---|---|
| `input_matches.json` | 联赛、轮次、日期、球队与赛程。 |
| `sources_cache/` | 保存已抓取网页文本，避免重复检索。 |
| `raw_team_checks.jsonl` | 每队候选事实和初步 flags。 |
| `reviewed_team_flags.json` | 审校后的最终结构化输出。 |
| `team_news_report.md` | 用户可读报告。 |

## Minimal non-agent implementation

如果没有可联网 Agent，也可以让人工或外部系统提供每队 2-5 个来源链接。模型只执行证据读取、判定、审校和写作。此时输入必须显式说明来源文本或链接，并允许将无法核验的事实降级为低置信度或删除。

## Failure handling

当来源冲突时，优先官方和最新来源。若球队名称、日期或赛事轮次不确定，应先返回澄清问题，而不是继续检索。若无法打开某来源，应标注 `source_unavailable` 并尝试替代来源。若某队完全缺少可信信息，不要编造异常，应输出 `暂无明显异常` 并在 `review_notes` 说明“未找到足够可信的当前轮异常证据”。
