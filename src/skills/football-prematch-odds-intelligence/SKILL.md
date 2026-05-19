---
name: football-prematch-odds-intelligence
version: "1.0"
source: manus-skill-v1 (ported to ares-osint-telemetry)
description: >
  足球赛前市场情报工作流，用于收集球队新闻异常标志、战意背景、
  博彩公司级欧赔（初盘/即时）、亚盘（初盘/即时/盘口水位）、大小球，
  处理更新时间逻辑，分析盘口时间逻辑，输出可审计的 Markdown 报告和 JSON 数据。
  适用场景：比赛日赔率报告、赛前市场分析、公司级赔率表、亚盘移动分析、联赛末轮战意分析。
inputs:
  - league: 联赛名称
  - date: 比赛日期（YYYY-MM-DD）
  - round: 轮次（可选）
  - match_list: 比赛列表
  - markets: "euro|asian|totals|all（默认 all）"
outputs:
  - reports/prematch_odds_{league}_{date}.md   # Markdown 报告
  - reports/prematch_odds_{league}_{date}.json # JSON 结构化数据
---

# Football Prematch Odds Intelligence

## 核心原则

**优先结构化公司级数据**，不以截图替代赔率表，除非用户明确要求视觉证据。
始终将事实数据收集与市场解读分离。

**诚实保留缺失**。若某博彩公司、市场或时间戳不可用，保留该公司行并标注：
- `status: source_missing` — 来源中不存在
- `status: market_not_offered` — 该公司不提供此市场
- `status: blocked` — 访问被阻止
- `status: source_no_timestamp` — 无更新时间

不得编造赔率或更新时间。

**优先公司**（按优先级）：**威廉**、**澳门**、**立博**、**365**、**易胜博**、**伟德**、**Pinnacle/平博**、**Betfair/交易所类**。
若来源使用别名，规范化后再写入报告。

## 标准执行流程

1. **确认范围** — 联赛、日期、轮次、开球时间、比赛列表，以及用户需要的市场类型
2. **收集比赛背景** — 仅收集实质性新闻标志（主帅下课、更衣室问题、核心伤停/停赛、多名主力轮换、杯赛前后、资格已锁定、晋级/降级状态、负面舆论、确认大名单变化）
3. **收集公司级赔率** — 每场收集初盘和即时盘欧赔、亚盘，必要时含大小球。使用公司级表格，不仅仅用平均值
4. **记录时间戳** — 优先使用历史最后变化时间；不可用时用来源时间戳；仍不可用时用抓取时间并显式标注 `source_no_timestamp`
5. **规范化验证** — 转换博彩公司名称、球队名称、盘口和大小球为一致格式；检查初盘/即时方向，确认无主客颠倒
6. **分析盘口时间逻辑** — 描述初盘定价、中段移动、临场修复，主/客胜赔是否下降，平赔是否收窄，欧赔方向是否与盘口方向冲突
7. **输出两件成品** — 人类可读 Markdown 报告 + 机器可读 JSON 文件

## 数据源策略

优先使用能通过渲染表格或 JSON/Ajax 端点暴露公司级赔率的来源。
页面为动态渲染时，检查页面脚本和 Ajax 参数，不依赖截图。
针对中文赔率中心页面，寻找包含 `listOdds`、`FIRST_HOST`、`HOST`、`FIRST_PANKOU`、`PANKOU`、`FIRST_DXPANKOU` 等字段的端点。

> 详细数据源策略见 `references/data-source-strategy.md`

## 时间戳优先级

| 优先级 | 字段 | 报告格式 |
|---|---|---|
| 1 | 历史最后变化时间 | `update_time: YYYY-MM-DD HH:mm` |
| 2 | 来源提供的当前更新时间 | `update_time: YYYY-MM-DD HH:mm` |
| 3 | 仅有抓取时间 | `update_time: source_no_timestamp; scrape_time=...` |
| 4 | 无可用时间 | `update_time: null; status: source_no_timestamp` |

## 报告结构（每场必填 YAML 块）

```yaml
match: 编号 主队 vs 客队
kickoff: YYYY-MM-DD HH:mm:ss
motivation: 欧战/保级/无欲无求/已夺冠/已降级/未确认
news_status: 暂无明显异常
news_flags: []
priority_euro:
  威廉:
    initial: 主 / 平 / 客
    current: 主 / 平 / 客
    last_change: 主胜降 / 客胜降 / 平赔压低 / 无明显变化
    update_time: YYYY-MM-DD HH:mm or source_no_timestamp; scrape_time=...
    status: ok | source_missing | market_not_offered | blocked | source_no_timestamp
  澳门: ...
  立博: ...
  365: ...
  易胜博: ...
  伟德: ...
  Pinnacle/平博: ...
  Betfair/交易所类: ...
asian_handicap:
  威廉:
    initial: 主水 / 盘口 / 客水
    current: 主水 / 盘口 / 客水
    key_change: 升盘 / 退盘 / 降主水 / 降客水 / 无变化
    update_time: ...
    status: ...
  average:
    initial: 主水 / 盘口 / 客水
    current: 主水 / 盘口 / 客水
    key_change: ...
total_goals:
  365:
    initial: 大水 / 盘口 / 小水
    current: 大水 / 盘口 / 小水
    key_change: ...
    update_time: ...
    status: ...
  average:
    initial: 大水 / 盘口 / 小水
    current: 大水 / 盘口 / 小水
    key_change: ...
market_time_logic:
  initial_read: 初盘怎么看
  middle_move: 中段怎么走
  live_repair: 临场是否修复，修复在欧赔还是盘口
  split_check: 胜平负方向和让球方向是否分裂
  conclusion: prematch 读法
```

> 完整模板见 `templates/prematch_odds_report_template.md`

## 解读规则

**欧赔**：说明最大移动是主胜降、客胜降、平赔收窄还是反向漂移。尽可能报告数值变化量。

**亚盘**：区分**盘口移动**和**水位移动**。
- `-1.5` → `-1.75` 是盘口修复/升盘
- 盘口稳定但主水降低是水位修复
- 若欧赔支持某方但盘口削弱该方，标注 `market_conflict: true`

**大小球**：至少涵盖强大热门、大盘口、2.5 → 2/2.5 线降、让球支持热门但 1X2 不够深的情形，以及有分数段判断请求的场次。

## 博彩公司名称规范化

| 规范键 | 常用别名 |
|---|---|
| 威廉 | William Hill, 威廉希尔, William |
| 澳门 | Macauslot, 澳彩, 澳门彩票 |
| 立博 | Ladbrokes, 利记, 立博国际 |
| 365 | Bet365, bet365 |
| 易胜博 | Easbet, 易胜 |
| 伟德 | BetVictor, Victor Chandler |
| Pinnacle/平博 | Pinnacle, 平博, Pinnacle Sports |
| Betfair/交易所类 | Betfair, 交易所, Exchange |

## 验证陷阱

- 合并多来源数据后，始终核查主/客方向（部分来源会反转）
- 亚盘负值（-）= 主队让球；正值（+）= 主队受让
- 大小球是 `大水 / 盘口 / 小水`，不要与亚盘的 `主水 / 盘口 / 客水` 混淆

## 验证清单

交付前确认：
- [ ] 每场比赛的 `priority_euro` 包含相同的公司键
- [ ] 亚盘至少包含可用优先公司 + average 行
- [ ] `Betfair/交易所类` 若不可用须显式标注缺失
- [ ] 时间戳未编造
- [ ] 最终成品包含 Markdown 和 JSON

## 打包资源

- `references/data-source-strategy.md` — 数据源发现与时间戳处理
- `scripts/normalize_odds_report.py` — 博彩公司名称规范化及赔率方向汇总工具
- `templates/prematch_odds_report_template.md` — 默认报告模板
