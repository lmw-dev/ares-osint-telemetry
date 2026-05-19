# [联赛][日期][轮次]赛前赔率与市场时间逻辑报告

> 数据说明：本报告优先使用公司级结构化赔率。若来源不提供逐次变化时间，必须标注 `source_no_timestamp`，不得编造更新时间。若指定公司缺失，保留该公司键并标注缺失状态。

## 数据覆盖

| 项目 | 覆盖情况 | 说明 |
|---|---|---|
| 比赛 | [N]场 | [联赛/轮次/日期] |
| 欧赔 | [覆盖公司] | 初盘、即时盘、方向变化 |
| 亚盘 | [覆盖公司] | 初盘、即时盘、盘口/水位变化 |
| 大小球 | [覆盖公司] | 必要场次或全量场次 |
| 更新时间 | [历史时间/源时间/抓取时间] | 明确时间来源 |

## 市场方向总览

| 场次 | 比赛 | 新闻/战意 | 平均欧赔方向 | 平均亚盘方向 | 平均大小球方向 | 欧亚是否分裂 | 数据缺口 |
|---|---|---|---|---|---|---|---|
| [编号] | [主队 vs 客队] | [标签] | [主胜降/客胜降/平赔压低] | [升盘/退盘/水位修复] | [升大/降小/稳定] | [是/否] | [缺失项] |

## 逐场赔率明细

### [编号] [主队] vs [客队]

```yaml
match: [编号] [主队] vs [客队]
kickoff: YYYY-MM-DD HH:mm:ss
motivation: [欧战/保级/无欲无求/已夺冠/已降级/未确认]
news_status: 暂无明显异常
news_flags: []
priority_euro:
  威廉:
    initial: null
    current: null
    last_change: null
    update_time: null
    status: source_missing
  澳门:
    initial: null
    current: null
    last_change: null
    update_time: null
    status: source_missing
  立博:
    initial: null
    current: null
    last_change: null
    update_time: null
    status: source_missing
  365:
    initial: null
    current: null
    last_change: null
    update_time: null
    status: source_missing
  易胜博:
    initial: null
    current: null
    last_change: null
    update_time: null
    status: source_missing
  伟德:
    initial: null
    current: null
    last_change: null
    update_time: null
    status: source_missing
  Pinnacle/平博:
    initial: null
    current: null
    last_change: null
    update_time: null
    status: source_missing
  Betfair/交易所类:
    initial: null
    current: null
    last_change: null
    update_time: null
    status: source_missing
asian_handicap:
  威廉:
    initial: null
    current: null
    key_change: null
    update_time: null
    status: source_missing
  澳门:
    initial: null
    current: null
    key_change: null
    update_time: null
    status: source_missing
  立博:
    initial: null
    current: null
    key_change: null
    update_time: null
    status: source_missing
  365:
    initial: null
    current: null
    key_change: null
    update_time: null
    status: source_missing
  易胜博:
    initial: null
    current: null
    key_change: null
    update_time: null
    status: source_missing
  伟德:
    initial: null
    current: null
    key_change: null
    update_time: null
    status: source_missing
  Pinnacle/平博:
    initial: null
    current: null
    key_change: null
    update_time: null
    status: source_missing
  average:
    initial: null
    current: null
    key_change: null
total_goals:
  365:
    initial: null
    current: null
    key_change: null
    update_time: null
    status: source_missing
  Pinnacle/平博:
    initial: null
    current: null
    key_change: null
    update_time: null
    status: source_missing
  average:
    initial: null
    current: null
    key_change: null
market_time_logic:
  initial_read: [初盘怎么看]
  middle_move: [中段怎么走]
  live_repair: [临场是否修复，修复在欧赔还是盘口]
  split_check: [胜平负方向和让球方向是否分裂]
  conclusion: [prematch读法]
```

## References

[1]: [数据源名称](URL)
