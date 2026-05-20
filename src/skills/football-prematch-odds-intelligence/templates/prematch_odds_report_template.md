# [联赛][日期][轮次] 赛前赔率与市场时间逻辑报告

> 采集时间：[YYYY-MM-DD HH:mm:ss]
> 生成时间：[YYYY-MM-DD HH:mm:ss]
> 数据源：[数据源名称与网址]
>
> ⚠️ 核心声明：本报告使用 Ares V2.1 标准规范，优先使用公司级结构化赔率。所有数据自带 sanity_check 防错校验，通过精密 Python 量化引擎清洗，完全剥离主观投注推荐，仅客观提炼市场信号（market_move & market_tags），供 Prematch 决策引擎参考。

## 数据覆盖

| 项目 | 覆盖情况 | 说明 |
|---|---|---|
| 比赛 | [N] 场 | [联赛/轮次/日期] |
| 欧赔 | [覆盖公司数] 个主流机构 | 初盘、即时盘、方向变化 |
| 亚盘 | [覆盖公司数] 个主流机构 | 初盘、即时盘、贴水与盘口变化 |
| 大小球 | [覆盖公司数] 个主流机构 | 必要场次或全量场次 |

---

## 市场方向总览

| 场次 | 比赛 | 欧赔均值方向 | 亚盘均值方向 | 大小球均值方向 | 欧亚分裂 | 核心市场标签 (market_tags) | 风险标记 (risk_tags) |
|---|---|---|---|---|---|---|---|
| [编号] | [主队 vs 客队] | [HOME_STRENGTHENED等] | [HOME_WATER_SUPPORT等] | [OVER_WATER_COMPRESSED等] | [是/否] | [HOME_EURO_STRENGTHENED等] | [EURO_ASIAN_SPLIT等] |

---

## 逐场赔率与博弈明细

### [编号] [主队] vs [客队]

#### 1. 结构化博弈数据 (JSON Structured)

```json
{
  "match_no": "[双位数编号，如 01]",
  "home": "[主队 canonical 名]",
  "away": "[客队 canonical 名]",
  "kickoff": "YYYY-MM-DD HH:mm:ss",
  "sanity_check": {
    "match_no": "[双位数编号]",
    "home": "[主队]",
    "away": "[客队]",
    "euro_order": "home/draw/away",
    "asian_format": "home_water / handicap_from_home_view / away_water",
    "handicap_sign_rule": {
      "negative": "home_gives_ball",
      "positive": "home_receives_ball"
    },
    "total_format": "over_water / goal_line / under_water"
  },
  "odds_raw": {
    "euro": {
      "威廉": { "initial": [null, null, null], "current": [null, null, null], "last_change": null, "update_time": null, "status": "source_missing" },
      "澳门": { "initial": [null, null, null], "current": [null, null, null], "last_change": null, "update_time": null, "status": "source_missing" },
      "立博": { "initial": [null, null, null], "current": [null, null, null], "last_change": null, "update_time": null, "status": "source_missing" },
      "365": { "initial": [null, null, null], "current": [null, null, null], "last_change": null, "update_time": null, "status": "source_missing" },
      "易胜博": { "initial": [null, null, null], "current": [null, null, null], "last_change": null, "update_time": null, "status": "source_missing" },
      "伟德": { "initial": [null, null, null], "current": [null, null, null], "last_change": null, "update_time": null, "status": "source_missing" },
      "Pinnacle/平博": { "initial": [null, null, null], "current": [null, null, null], "last_change": null, "update_time": null, "status": "source_missing" },
      "Betfair/交易所类": { "initial": [null, null, null], "current": [null, null, null], "last_change": null, "update_time": null, "status": "source_missing" }
    },
    "asian": {
      "威廉": { "initial": [null, null, null], "current": [null, null, null], "key_change": null, "update_time": null, "status": "source_missing" },
      "澳门": { "initial": [null, null, null], "current": [null, null, null], "key_change": null, "update_time": null, "status": "source_missing" },
      "立博": { "initial": [null, null, null], "current": [null, null, null], "key_change": null, "update_time": null, "status": "source_missing" },
      "365": { "initial": [null, null, null], "current": [null, null, null], "key_change": null, "update_time": null, "status": "source_missing" },
      "易胜博": { "initial": [null, null, null], "current": [null, null, null], "key_change": null, "update_time": null, "status": "source_missing" },
      "伟德": { "initial": [null, null, null], "current": [null, null, null], "key_change": null, "update_time": null, "status": "source_missing" },
      "Pinnacle/平博": { "initial": [null, null, null], "current": [null, null, null], "key_change": null, "update_time": null, "status": "source_missing" }
    },
    "total": {
      "365": { "initial": [null, null, null], "current": [null, null, null], "key_change": null, "update_time": null, "status": "source_missing" },
      "Pinnacle/平博": { "initial": [null, null, null], "current": [null, null, null], "key_change": null, "update_time": null, "status": "source_missing" }
    }
  },
  "odds_avg": {
    "euro": {
      "initial": { "home": null, "draw": null, "away": null },
      "current": { "home": null, "draw": null, "away": null }
    },
    "asian": {
      "initial": { "home_water": null, "handicap": null, "away_water": null },
      "current": { "home_water": null, "handicap": null, "away_water": null }
    },
    "total": {
      "initial": { "over_water": null, "line": null, "under_water": null },
      "current": { "over_water": null, "line": null, "under_water": null }
    }
  },
  "market_move": {
    "euro_signal": "[HOME_STRENGTHENED | AWAY_STRENGTHENED | DRAW_COMPRESSED | STABLE]",
    "asian_signal": "[HOME_WATER_SUPPORT | FAVORITE_RETREAT | AWAY_WATER_SUPPORT | STABLE]",
    "total_signal": "[OVER_WATER_COMPRESSED | UNDER_WATER_SUPPORT | STABLE]",
    "euro_asian_split": false
  },
  "market_move_detail": {
    "euro": {
      "home_delta": null,
      "draw_delta": null,
      "away_delta": null
    },
    "asian": {
      "handicap_delta": null,
      "home_water_delta": null,
      "away_water_delta": null
    },
    "total": {
      "line_delta": null,
      "over_water_delta": null,
      "under_water_delta": null
    }
  },
  "market_tags": [
    "HOME_EURO_STRENGTHENED",
    "HOME_HANDICAP_WATER_SUPPORT"
  ],
  "risk_tags": [
    "EURO_ASIAN_SPLIT"
  ],
  "data_confidence": {
    "euro": "high | medium | low",
    "asian": "high | medium | low",
    "total": "high | medium | low"
  },
  "company_coverage": {
    "euro": {
      "active": 0,
      "expected": 8,
      "coverage_rate": 0.0,
      "missing": []
    },
    "asian": {
      "active": 0,
      "expected": 8,
      "coverage_rate": 0.0,
      "missing": []
    },
    "total": {
      "active": 0,
      "expected": 2,
      "coverage_rate": 0.0,
      "missing": []
    }
  },
  "market_time_logic": {
    "initial_read": "[剖析庄家开出初盘的深浅、受水倾向与博弈意图]",
    "movement_read": "[分析随比赛临近各玩法赔率/水位变动的资金和风险控制走向]",
    "split_check": "[核验欧赔与亚赔的指向是否完全一致；如出现欧亚大裂痕，指出背后的风控异常]",
    "ares_warning": "[输出最核心的博弈爆冷预警，不给出投注推荐]"
  },
  "market_conclusion": [
    "客观事实 1：例如主胜欧赔与亚盘低水同向",
    "客观事实 2：例如大小球大球水显著压低",
    "博弈警告：例如双方无硬目标，仍需在 prematch 中结合战意/阵容/过程确认"
  ]
}
```

#### 2. 时间博弈研判 (Time-Logic Analysis)

- **初盘多空博弈 (initial_read)**：  
  [分析说明，指出庄家对初盘深度的基本面偏好]
  
- **即时资金盘面移动 (movement_read)**：  
  [详细剖析欧赔即时均值、亚盘让步水位、大小球水位随时间推进的移动轨迹]
  
- **双盘同向裂痕校验 (split_check)**：  
  [校验欧亚是否对齐，若触发 EURO_ASIAN_SPLIT 标签，必须详细解释裂痕成因与对敲阻水的可疑性]
  
- **Ares 风控级预警 (ares_warning)**：  
  [站在风控视角，客观提炼本场比赛大盘玩法的多空博弈预警，为后续技战术情报对齐奠定基调]

---

## References

[1] [数据源名称](URL)
