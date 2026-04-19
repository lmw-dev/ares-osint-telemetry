---
version: 1.0
issue: '260418'
match_id: evt_ger_b2a_mock
match_name: Bayer Leverkusen vs FC Augsburg
result:
  score: 1-2
  winner: away
physical_metrics:
  home_xG: 3.1
  away_xG: 0.65
  possession_home: 72
  possession_away: 28
  shots_on_target_home: 11
  shots_on_target_away: 3
  passes_attacking_third_home: 240
  passes_attacking_third_away: 35
key_events:
  red_cards: []
  penalties: []
system_evaluation:
  variance_flag: true
---

# Bayer Leverkusen vs FC Augsburg (260418)

> 📊 本复盘报告由 Ares OSINT Telemetry (Understat 强驱动引擎) 自动生成。

## 📈 物理遥测深度解读

### ⚡ 危险方差倒挂 (严重警报)
比赛比分最终为 `1-2`，但根据底层物理遥测，双方的预期进球真实转化存在巨大撕裂：主队 xG **3.10** 对比 客队 xG **0.65**。
👉 **Ares 引擎建议**：此场赛果具有强烈的运气、神仙球或门将爆种因素。在下一周期的量化推演中，**必须无视本场比分结果**，直接采信 xG 物理预期，以防止大模型判断失真！

### ⚔️ 战术压制力剥析
- **机会创造端**：**主队完全接管了威胁区域**。创造出的绝对进球机会明显多于对手。
- **阵地纵深打击**：主队在进攻三区成功送出了高达 **240** 次的高危传球（对比客队的 35 次）。客队防线全场处于深度退守并被反复摩擦的状态。
