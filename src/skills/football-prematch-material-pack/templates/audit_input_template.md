---
match_no: "{match_no}"
match: "{home_team} vs {away_team}"
league: "{league}"
kickoff: "{kickoff}"
material_status:
  market_json: "{market_json_status}"
  abnormal_json: "{abnormal_json_status}"
  home_card: "{home_card_status}"
  away_card: "{away_card_status}"
fact_gate:
  status: "{fact_gate_status}"
  final_confidence: "{fact_gate_confidence}"
  reason: {fact_gate_reason_yaml}
sanity_check:
  home: "{home_resolved}"
  away: "{away_resolved}"
  source_order: "home/draw/away"
fair_line_seed:
  process_edge:
    side: "{process_edge_side}"
    confidence: "{process_edge_confidence}"
    reason: {process_edge_reason}
  home_advantage:
    side: "{home_team}"
    strength: "small"
market_internal_divergence:
  status: {market_internal_divergence_status}
  note: "{market_internal_divergence_note}"
market_process_divergence:
  status: {market_process_divergence_status}
  severity: "{market_process_divergence_severity}"
  note: "{market_process_divergence_note}"
auto_risk_tags: {auto_risk_tags}
prematch_mode: "{prematch_mode}"
deep_queue_score: {deep_queue_score}
---

# {home_team} vs {away_team} 前瞻材料卡 (Prematch Audit Input v2.3)

> [!NOTE]
> 本卡片由 Ares Prematch 生产力套件 V2.3 自动生成，对齐 SOP v2.2 batch screening 输入卡规范。

---

## 0. 调度判定 (Dispatch Gate)

| 字段 | 值 |
|------|----|
| **Prematch Mode** | `{prematch_mode}` |
| **Deep Queue Score** | `{deep_queue_score}` |
| **Process Edge** | **{process_edge_side}** (置信度: `{process_edge_confidence}`) |
| **Market JSON** | `{market_json_status}` |
| **Abnormal JSON** | `{abnormal_json_status}` |
| **Fact Gate** | `{fact_gate_status}` (`{fact_gate_confidence}`) |

> [!WARNING]
> **Auto Risk Tags**：{auto_risk_tags_formatted}

*过程优势研判逻辑*: {process_edge_reason_formatted}

---

## 1. 赔率现实与市场状态 (Market Reality)

- **赔率数据状态**: `{market_json_status}`
- **欧亚内部偏离 (Market Internal Divergence)**: {market_internal_divergence_formatted}
- **市场过程背离 (Market Process Divergence)**: {market_process_divergence_formatted}

### 赔率市场实时数据 (`{match_no}_market.json`)
{market_data_block}

---

## 2. 事实门禁与置信度审核 (Fact Gate)

- **Fact Gate Status**: `{fact_gate_status}` (总置信度: `{fact_gate_confidence}`)
- **降级原因**:
{fact_gate_reason_formatted}

### 🚨 临场伤停异动 — 三分层 (`{match_no}_abnormal.json`)
{abnormal_suspicious_block}

### 💥 受灾单元评级
{abnormal_rotation_block}

---

## 3. 长期战术底座画像 (Base Tactical Reality)

### 🏠 主队：{home_team} (`{home_card_status}`)
| 指标 | 值 |
|------|----|
| 主教练 | `{home_coach}` |
| 常用阵型 | `{home_formation}` |
| 战术风格 | `{home_tactical_style}` |
| avg_xG_last_5 | `{home_avg_xg}` |
| defensive_leakage | `{home_defensive_leakage}` |
| conversion_efficiency | `{home_conversion_efficiency}` |
| actual_tactical_entropy | `{home_actual_tactical_entropy}` |

### 🚌 客队：{away_team} (`{away_card_status}`)
| 指标 | 值 |
|------|----|
| 主教练 | `{away_coach}` |
| 常用阵型 | `{away_formation}` |
| 战术风格 | `{away_tactical_style}` |
| avg_xG_last_5 | `{away_avg_xg}` |
| defensive_leakage | `{away_defensive_leakage}` |
| conversion_efficiency | `{away_conversion_efficiency}` |
| actual_tactical_entropy | `{away_actual_tactical_entropy}` |

---

## 4. 比赛情景推演种子 (Game Script Seed — 变量式)

{game_script_seed_block}

---

## 5. 三分离推演结论插槽 (Deduction Slots)

> [!IMPORTANT]
> 本卡为 Prematch 前瞻输入卡，以下结论框须经 AI 深度演绎或人工研判后填补。

### 📌 胜平负指向 (Result Slot)
- [ ] 主队赢球 (Home Win)
- [ ] 双方战平 (Draw)
- [ ] 客队赢球 (Away Win)
- *推演依据*:

### 📌 让球博弈指向 (Handicap Slot)
- [ ] 让球方穿盘 (Favorite Cover)
- [ ] 受让方不败 (Underdog Cover/Draw)
- *盘口与赔率防线研判*:

### 📌 进球数与节奏预期 (Process Slot)
- [ ] 大球倾向 / 高频进攻
- [ ] 小球倾向 / 焦灼防守
- *节奏与战术熵依据*:
