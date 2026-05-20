# {home_team} vs {away_team} 战术推演卡 (Match Audit)

> [!NOTE]
> 本报告由 Ares Prematch 生产力套件自动生成，对齐 SOP v1.0 战术推演卡规范。

---

## 1. 比赛基本信息与赔率现实 (Market Reality)

- **开赛时间 (Kickoff)**: `{kickoff}`
- **主队**: `{home_team}`
- **客队**: `{away_team}`
- **数据源 (Data Source)**: `{data_source}`
- **别名对照**: 主队 -> `{home_resolved}` | 客队 -> `{away_resolved}`

### 赔率市场实时数据 (来自 market.json)
{market_data_block}

---

## 2. 事实门禁与伤停异动 (Fact Gate & Team Abnormalities)

> [!IMPORTANT]
> **异常事实门禁 Fact Gate 2.1 审查结论**：以下为经由新闻异常 Skill 硬门禁洗涤出的高可信临场情报。

### 🚨 临场特异噪点与存疑伤停 (NEEDS_LATEST_CONFIRMATION)
{abnormal_suspicious_block}

### 💥 受灾单元与阵容残缺评级 (Unit Impact & Rotation Level)
{abnormal_rotation_block}

---

## 3. 长期战术底座画像 (Base Tactical Reality)

> [!TIP]
> **物理现实指标对齐**：以下为只读拷贝自底座档案最新的智能 physical_reality 数据。

### 🏠 主队：{home_team}
- **主教练 (Coach)**: `{home_coach}`
- **常用阵型 (Formation)**: `{home_formation}`
- **战术风格 (Tactical Style)**: `{home_tactical_style}`
- **智能期望进球 (avg_xG_last_5)**: `{home_avg_xg}`
- **智能期望失球 (defensive_leakage)**: `{home_defensive_leakage}`
- **终结效率 (conversion_efficiency)**: `{home_conversion_efficiency}`
- **实际战术熵 (actual_tactical_entropy)**: `{home_actual_tactical_entropy}`

### 🚌 客队：{away_team}
- **主教练 (Coach)**: `{away_coach}`
- **常用阵型 (Formation)**: `{away_formation}`
- **战术风格 (Tactical Style)**: `{away_tactical_style}`
- **智能期望进球 (avg_xG_last_5)**: `{away_avg_xg}`
- **智能期望失球 (defensive_leakage)**: `{away_defensive_leakage}`
- **终结效率 (conversion_efficiency)**: `{away_conversion_efficiency}`
- **实际战术熵 (actual_tactical_entropy)**: `{away_actual_tactical_entropy}`

---

## 4. 比赛情景剧本推演树 (Game Script Tree)

- [ ] **Scenario A: {home_team} 先破门场景**
  - 主队转入低位防守反击时，客队中场创造力是否能解构主队的低位锁链？
  - 核心换人节点与受灾单元暴露时间点评估。
- [ ] **Scenario B: {away_team} 先破门场景**
  - 主队的高位逼抢与前场重组速度，面对客队的反击保护链路表现。
- [ ] **Scenario C: 僵局与焦灼拉锯场景**
  - 前 60 分钟无进球，体能节点消耗后的后半段博弈与定位球比拼。

---

## 5. 双端偏差与风控警告 (Bias Type & Risk Warning)

- **市场倾向 (Market Sentiment)**: `{market_sentiment}`
- **博弈偏差类型 (Bias Type)**: `{bias_type}`
- **动态修正系数 (S_dynamic_modifier)**: `{S_dynamic_modifier}`
- **Ares 核心博弈预警**:
  - `{bookmaker_divergence_warning}`

---

## 6. 量化预测与战术推演结论 (Tactical Deduction)

### 终极剧本推断
- [ ] **主干剧本胜负指向**: 
- [ ] **进球数与节奏预期**: 
- [ ] **战术层博弈结论**: 
