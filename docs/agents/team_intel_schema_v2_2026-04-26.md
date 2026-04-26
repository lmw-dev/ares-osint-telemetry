# TEAM-INTEL Schema v2（2026-04-26）

## 目的

统一 `TEAM-INTEL-{issue}.json` / `TEAM-INTEL-{issue}.generated.json` 字段，确保：

- `team_archive_backfill.py` 可稳定消费；
- 队档可沉淀“市面信息 + YouTube 技战术解读”；
- prematch 前回填链路可脚本化回归。

## 顶层结构

```json
{
  "issue": "26068",
  "updated_at": "2026-04-26 03:21:59Z",
  "source": "prematch_preflight.py",
  "description": "Auto-generated enrichment skeleton. Fill substantive fields, then pass this file to team_archive_backfill.py --intel-file.",
  "teams": []
}
```

## team 对象字段（v2）

必填字段：

- `team`：英文队名（需与队档 team 名一致）
- `league`：联赛（EPL / Bundesliga / Serie_A / La_liga / Ligue_1 等）

建议字段（命中任意一项即可判定为“有实质补料”）：

- `manager_doctrine`：主教练风格（文本）
- `recent_news_summary`：近期新闻摘要（文本）
- `key_node_dependency`：关键节点依赖（字符串数组）
- `tactical_logic`：战术矩阵（对象，键：`P/Space/F/H/Set_Piece`）
- `prematch_focus_items`：prematch 关注项（字符串数组）

物理指标字段：

- `avg_xG_last_5`（float）
- `conversion_efficiency`（float）
- `defensive_leakage`（float）
- `actual_tactical_entropy`（float）

偏差修正字段：

- `bias_type`（`Aligned/Fame_Trap/Underestimated`）
- `S_dynamic_modifier`（float）

新增“市面/YouTube”字段（v2）：

- `market_external_notes`：市面观点摘要（字符串数组）
- `youtube_tactical_briefs`：YouTube 技战术要点（字符串数组）

## 示例

```json
{
  "team": "VfB Stuttgart",
  "league": "Bundesliga",
  "manager_doctrine": "High-intensity transition with aggressive half-space pressing.",
  "market_sentiment": "Neutral",
  "recent_news_summary": "Rotation pressure eased, right-side progression recovered in last 2 matches.",
  "key_node_dependency": [
    "right half-space carrier",
    "first-line press trigger"
  ],
  "tactical_logic": {
    "P": "P2",
    "Space": "W",
    "F": "F",
    "H": "H",
    "Set_Piece": "N"
  },
  "avg_xG_last_5": 1.62,
  "conversion_efficiency": 0.11,
  "defensive_leakage": 0.44,
  "actual_tactical_entropy": 0.47,
  "bias_type": "Aligned",
  "S_dynamic_modifier": 0.0,
  "prematch_focus_items": [
    "防高位逼抢时的后场出球稳定性",
    "75分钟后边路回防速度"
  ],
  "market_external_notes": [
    "主流欧赔分歧缩小，市场对主队高估有收敛迹象（2026-04-26）"
  ],
  "youtube_tactical_briefs": [
    "频道A：强调中场二点球保护问题，建议关注对手二次进攻（视频 12:30-14:05）"
  ]
}
```

## 与脚本的关系

- 生产 skeleton：`prematch_preflight.py`（自动带 v2 字段）
- 消费回填：`team_archive_backfill.py --intel-file ...`
- 自动串联：`osint_pipeline.py` 在 prematch 前默认执行回填（可 `--skip-team-backfill`）
