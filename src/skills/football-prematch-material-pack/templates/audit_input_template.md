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
---

# {home_team} vs {away_team} 前瞻材料卡 (Prematch Audit Input)

> [!NOTE]
> 本卡片由 Ares Prematch 生产力套件自动生成，对齐 SOP v2.2 batch screening 输入卡规范。

---

## 0. 基础物料与博弈冲突标签 (Auto Risk Tags)

> [!WARNING]
> **自动冲突检测 (Sanity Audit Tags)**：以下为基于底座过程数据（avg_xG_last_5 / defensive_leakage）对比赔率盘口让格，自动洗涤暴露出的博弈裂痕与风险标签。

- **博弈冲突标签 (Auto Risk Tags)**: {auto_risk_tags_formatted}
- **底座过程优势方 (Process Edge)**: **{process_edge_side}** (置信度: `{process_edge_confidence}`)  
  *过程优势研判逻辑*: {process_edge_reason_formatted}

---

## 1. 赔率现实与市场状态 (Market Reality)

- **赔率数据状态 (market_json)**: `{market_json_status}`
- **市场内部偏离预警 (Market Internal Divergence)**: {market_internal_divergence_formatted}
- **市场过程背离预警 (Market Process Divergence)**: {market_process_divergence_formatted}

### 赔率市场实时数据 (数据分发: `{match_no}_market.json`)
{market_data_block}

---

## 2. 事实门禁与置信度审核 (Fact Gate & Team Abnormalities)

- **事实门禁置信度 (Fact Gate Status)**: `{fact_gate_status}` (总置信度: `{fact_gate_confidence}`)
- **降级防卫原因 (Fact Gate Reason)**: 
{fact_gate_reason_formatted}

### 🚨 临场特异噪点与存疑伤停 (数据分发: `{match_no}_abnormal.json`)
{abnormal_suspicious_block}

### 💥 受灾单元与阵容残缺评级
{abnormal_rotation_block}

---

## 3. 长期战术底座画像 (Base Tactical Reality)

> [!TIP]
> **物理过程面速读**：主客队最近 5 场常态进攻值（avg_xG）与防守泄露值（defensive_leakage）。

### 🏠 主队：{home_team} (底座强度: `{home_card_status}`)
- **主教练 (Coach)**: `{home_coach}`
- **常用阵型 (Formation)**: `{home_formation}`
- **战术风格 (Tactical Style)**: `{home_tactical_style}`
- **智能期望进球 (avg_xG_last_5)**: `{home_avg_xg}`
- **智能期望失球 (defensive_leakage)**: `{home_defensive_leakage}`
- **终结效率 (conversion_efficiency)**: `{home_conversion_efficiency}`
- **实际战术熵 (actual_tactical_entropy)**: `{home_actual_tactical_entropy}`

### 🚌 客队：{away_team} (底座强度: `{away_card_status}`)
- **主教练 (Coach)**: `{away_coach}`
- **常用阵型 (Formation)**: `{away_formation}`
- **战术风格 (Tactical Style)**: `{away_tactical_style}`
- **智能期望进球 (avg_xG_last_5)**: `{away_avg_xg}`
- **智能期望失球 (defensive_leakage)**: `{away_defensive_leakage}`
- **终结效率 (conversion_efficiency)**: `{away_conversion_efficiency}`
- **实际战术熵 (actual_tactical_entropy)**: `{away_actual_tactical_entropy}`

---

## 4. 比赛情景剧本推演树种子 (Game Script Seed)

- [ ] **Scenario A: 主队 先破门场景 (Seed)**
  - *战术传导逻辑*: 主队转入 P3/P4 低位防反，评估客队中场空间（Space）在面对低位锁链时的组织穿透力；若主队防守泄漏（{home_defensive_leakage}）偏高，需警惕低位传球失误被反抢二次进攻。
- [ ] **Scenario B: 客队 先破门场景 (Seed)**
  - *战术传导逻辑*: 客队转入低位锁链并展开反击，评估主队在高位压迫下的前场重组速度；若客队进攻期望（{away_avg_xg}）强劲，需关注其高效反击对主队高位防线身后的致命打击。
- [ ] **Scenario C: 僵局与焦灼拉锯场景 (Seed)**
  - *战术传导逻辑*: 前 60 分钟维持 0-0，评估双方在体能下降、核心节点开始大面积消耗/换人后的战术熵波动。定位球（Set Piece）比拼将成为解冻破冰的核心抓手。

---

## 5. 三分离推演结论插槽 (Deduction Slots)

> [!IMPORTANT]
> **注意**：本卡为 Prematch 前瞻输入卡，以下结论框须经 ChatGPT/Claude 结合主客队只读档案进行深度演绎，或由人工研判后勾选填补。

### 📌 最终胜平负指向 (Result Slot)
- [ ] 主队赢球 (Home Win)
- [ ] 双方战平 (Draw)
- [ ] 客队赢球 (Away Win)
- *推演佐证依据*: 

### 📌 让球博弈指向 (Handicap Slot)
- [ ] 让球方穿盘 (Favorite Cover)
- [ ] 受让方受损/受让方不败 (Underdog Cover/Draw)
- *盘口与赔率防线研判*: 

### 📌 进球数与节奏预期 (Process Slot)
- [ ] 大球倾向 / 高频进攻大战
- [ ] 小球倾向 / 焦灼防守闷战
- *节奏与战术熵依据*: 
