---
name: football-physical-profile
version: "1.0"
source: ares-osint-telemetry native
description: >
  足球战力物理画像智能修正 Skill。
  通过拉取 Understat 隐藏 API 提取球队近期的实际比赛 raw 期望进球数据（xG/xGA），
  引入 LLM 大模型作为“智能噪点修剪器 (Noise Pruning)”，
  平滑红牌罚下、保级大轮换、伤停爆发、极端天气等物理噪点对期望值均值的过度污染，
  拟合计算出真正代表球队常态战斗力的 avg_xG_last_5 (进攻期望) 和 avg_xGA_last_5 (防守期望) 等高价值结构化画像，
  并以无损 Soft Update 机制安全合回 02_Team_Archives 长期底座。
inputs:
  - team_name: 球队名称（中英文及别名均可）
  - league: 球队所属联赛（英超/西甲/意甲/德甲/法甲）
  - year: 赛季年份（如 2024 对应 2024/25 赛季，2025 对应 2025/26 赛季）
outputs:
  - 更新后的球队底座档案 → 02_Team_Archives/{league}/{team}.md (仅 Frontmatter 中的 physical_reality 部分进行 soft update，Body 100% 无损留存)
---

# Football Physical Profile Intelligence V1.0

## 执行模式说明

本 Skill 是一套 **基于 Understat XHR 数据提取 + 大模型智能修剪噪点 + 底座无损合并** 的物理面智能画像更新规约。
原有的 `avg_xG_last_5` 计算方式采用机械算术平均，易受“红牌多打少打”、“无欲无求大轮换”等噪点严重污染。
本 Skill 的职责是利用大模型强大的常识推理能力，对这 5 场比赛的 raw 物理期望值进行**合理剪枝、代偿拟合**，从而产出更具预警和战力指示意义的常态物理面画像。

---

## 核心原则

1. **冷启动与时序抗灾力**：
   - 彻底摒弃依赖历史复盘的累加机制，通过 `getLeagueData` 联赛 XHR 一键拉取本赛季所有已赛记录。
   - 无论是赛季初还是赛季中，随时运行均能直接获得准确的最近 5 场物理面历史。
2. **大模型智能噪点剪枝 (LLM-based Noise Pruning & Normalization)**：
   - 大模型不用于做猜测性结论，而是作为 **“智能代偿器”**。
   - 例如：某场比赛在第 10 分钟球队吃红牌少一人，全场被动，raw xG 为 0.15。大模型识别到这一红牌后，应启动**“代偿拟合”**，将该场用于计算战力均值的拟合 xG 平滑修正为代表其健康水准的数值（例如 1.15 xG），防止算术均值暴跌；反之，若大打小遇到超级大红牌刷了异常虚高的 xG，进行适当调回。
3. **无损 Soft Update 原则**：
   - 绝不能简单暴力的覆盖 `02_Team_Archives` 下的球队档案。
   - 仅利用 Python 脚本读取 YAML Frontmatter，进行 `physical_reality` 字段的无损合并更新，保留底座的其余一切长期沉淀内容（如球风特征、核心打法、Body 中的推演笔记）100% 完好无损。

---

## 3 阶段 Agent 运行流程 (3-Phase Workflow)

### Phase 1: 数据提取与时序过滤 (Data Retrieval)
1. **解析队名与联赛**：
   - 读取 `team_alias_map.json` 别名映射，将输入的 `team_name` 标准化为 Understat 的英文 Title，并确定其所属的五大联赛（EPL, La_liga, Bundesliga, Serie_A, Ligue_1）。
2. **拉取 XHR 数据**：
   - 调用 `https://understat.com/getLeagueData/{league}/{year}` 获取整季比赛列表。
   - 过滤出该球队在该赛季**所有已赛 (isResult == true)** 的比赛，按 `datetime` 时间降序排列。
   - 截取最近 5 场已踢完比赛（同时提取最近 8 场，供大模型理解长期走势作为参考）。

### Phase 2: 大模型智能噪点平滑与均值拟合 (LLM Noise Pruning)
1. **组装历史上下文**：
   - 提取最近 5 场的详细数据（对手、比分、主/客、raw xG、对方 raw xG、比赛时间）。
   - 将这 5 场比赛的历史事实（包含胜负、比分、是否多打少打等关键提示）提供给大模型。
2. **执行大模型推理决策**：
   - 识别并标记是否存在**物理噪点**（红牌早退、大范围主力轮换或爆发流感、保级提前上岸完全练兵、极端暴雨雪天气导致皮球无法滚动的垃圾场次）。
   - 对受污场次的 xG / xGA 进行**代偿拟合**。
   - 计算智能拟合后的常态期望值：
     - **智能修正的 `avg_xG_last_5`**（常态进攻期望均值，保留 4 位小数）。
     - **智能修正的 `avg_xGA_last_5`**（常态防守期望均值，保留 4 位小数）。
     - **智能修正的 `conversion_efficiency`**（终结效率，即 5 场进球总数 / 5 场智能修正后 xG 均值）。
     - **智能修正的 `actual_tactical_entropy`**（战术熵，反映战绩波动，可由大模型基于这 5 场在平滑后的偏离度评估）。

### Phase 3: 无损回填与底座合并 (Soft Update Backfill)
1. **底座档案无损回填**：
   - 定位 `02_Team_Archives/{league}/{team}.md` 路径。
   - 解析出原有的 Frontmatter 和 Body 内容。
   - 将新计算的 `physical_reality` YAML 字段合并入 Frontmatter。
   - 无损回写，确保原有的底座数据（包括 Body 部分）完好无损。
