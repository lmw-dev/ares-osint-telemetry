# Ares Prematch 料包生产引擎 V2.2 BATCH_READY 交接文档

**归档日期**: 2026-05-20  
**提交节点**: `04111fd`  
**状态**: ✅ PASS — 完整 BATCH_READY 闭环验证通过

---

## 一、本次完成的核心改造

### 1. `format_market_data()` — 赔率格式化（双版本兼容）

| 能力 | 旧版 | V2.2 |
|------|------|-------|
| 新式 odds_avg 格式支持 | ❌ | ✅ current/initial 双时段展示 |
| 旧式 euro/asian/over_under 兼容 | ✅ | ✅ 自动识别版本 |
| 大盘变动信号渲染 | 仅 home_win_trend | euro/asian/total 三信号独立 |
| MISSING 强阻断 | ✅ | ✅ |

### 2. `extract_abnormal_blocks()` — 事实门禁（五元组返回）

```
返回: (abnormal_suspicious, abnormal_rotation, fg_status, fg_confidence, fg_reasons)
```

- 新增 `fact_gate` schema 直接解析（status / final_confidence / reason）
- 旧格式 `suspicious_items + unit_impact` 自动兼容转化
- **严密降级逻辑**：只要有 `needs_latest_confirmation` 且无 `confirmed`，`fg_status` 自动降为 `PARTIAL_PASS`，`fg_confidence` 强制降为 `medium_low`

### 3. `calculate_auto_risk_tags()` — 双轨大裂缝（六元组返回）

```
返回: (tags, process_edge_side, process_edge_confidence, process_edge_reason,
        market_internal_div, market_process_div)
```

- `market_internal_divergence`：检测欧赔与亚盘水位分裂（`euro_asian_split` 字段）
- `market_process_divergence`：检测市场盘口与底座物理优势背离（`MARKET_PROCESS_CONFLICT`）
- `process_edge_confidence`：根据档案强度自动降级（weak → medium_low）

### 4. `_extract_handicap_euro()` — 新增独立 Helper

从新版 `odds_avg.asian.current.handicap` 或旧版 `odds.asian.handicap` 两种路径安全提取浮点数。

### 5. Demo 数据源升级为完整 v2.2 Schema

- `odds_avg`：包含 `euro/asian/total` 各自的 `initial/current`
- `market_move`：`euro_signal / asian_signal / total_signal / euro_asian_split`
- `abnormal`：完整 `fact_gate + teams[{side, needs_latest_confirmation, affected_units}]`

### 6. `audit_input_template.md` — 全面对齐新占位符

新增/变更占位符：

| 占位符 | 说明 |
|--------|------|
| `{process_edge_confidence}` | 过程优势置信度 |
| `{fact_gate_confidence}` | 事实门禁总置信度 |
| `{fact_gate_reason_yaml}` | YAML 行内列表格式 |
| `{market_internal_divergence_status/note/formatted}` | 欧亚内部大裂缝 |
| `{market_process_divergence_status/severity/note/formatted}` | 市场-底座大裂缝 |

### 7. `00_match_list.csv` 字段扩展

新增字段：`fact_gate_status` / `process_edge` / `risk_tags`

---

## 二、验证实测日志（Demo 模式）

```
[WARNING] 未检测到 market.json，启动 Demo 演练数据
[INFO]    ===> 开始处理第 01 场: 热刺 vs 埃弗顿
[INFO]    [只读拷贝] 主队底座 Tottenham_Hotspur.md -> 01_home.md
[INFO]    [只读拷贝] 客队底座 Everton.md -> 01_away.md
[INFO]    [分发物料] 派生专属赔率卡 -> 01_market.json
[INFO]    [生成说明卡] -> 01_market.md
[INFO]    [分发物料] 派生专属伤停卡 -> 01_abnormal.json
[INFO]    [生成说明卡] -> 01_abnormal.md
[INFO]    [博弈冲突标签检测] -> ['AWAY_DEFENSIVE_FLOOR_HIGH', 'CONTAMINATION_HISTORY',
                                  'FAVORITE_DEEP_HANDICAP_CAUTION', 'HOME_DEFENSIVE_LEAKAGE_HIGH',
                                  'MARKET_OVERPRICES_HOME', 'MARKET_PROCESS_CONFLICT', 'TEAM_ARCHIVE_WEAK']
[INFO]    [底座优势判定方] -> Away (置信度: medium_low)
[INFO]    [生成推演前瞻输入卡] -> 01_audit_input.md
[INFO]    V2.2 运行成功 (BATCH_READY)！
```

**Fact Gate 级联降级验证** ✅：
- Demo 数据中两支球队均只有 `needs_latest_confirmation`，无 `confirmed`
- 输出 `fact_gate.status = PARTIAL_PASS`, `final_confidence = medium_low`
- 原因标注：`but_all_player_items_needs_latest_confirmation`

**双轨大裂缝验证** ✅：
- `market_internal_divergence.status = false`（欧亚同向，无分裂）
- `market_process_divergence.status = true, severity = high`（市场主让 -1.0 但底座优势在客队，博弈大裂缝触发）

---

## 三、生成物目录结构

```
AresMatchday_20260520/
├── 00_match_list.csv              # 拉链大表（新增 fact_gate_status/process_edge/risk_tags）
└── 01_Tottenham_Hotspur_Everton/
    ├── 01_market.json             # Ares v2.2 完整赔率 Schema
    ├── 01_market.md               # 初始/当前双时段赔率速览卡
    ├── 01_abnormal.json           # 完整 fact_gate + teams Schema
    ├── 01_abnormal.md             # 受灾单元 & 伤停置信度评级卡
    ├── 01_audit_input.md          # V2.2 batch screening 前瞻输入卡
    ├── 01_home.md                 # 主队底座只读拷贝
    └── 01_away.md                 # 客队底座只读拷贝
```

---

## 四、后续建议

1. **接入真实 market.json / abnormal.json**：Skill 已完全支持，将文件放入 `AresMatchday_YYYYMMDD/` 目录后执行即可，自动走真实数据路径。

2. **xG 画像智能 Skill**：目前 `avg_xG_last_5` 从底座 frontmatter 读取（静态）。建议后续引入独立的 xG Skill 从 Understat 实时拉取近 5 场数据，覆盖写回底座或直接注入打包流程。

3. **Understat xG 数据来源说明**：
   - 当前网站 `https://understat.com/team/Arsenal/2025` 的 xG 数据为累计赛季数据
   - 精确的 `avg_xG_last_5` 需在 API 层面提取最近 5 场比赛的 xG 并自行平均
   - 建议采用 `understat` Python 库的 `get_team_results()` 方法获取

4. **批量多场扩展**：只需在 `market.json` 中包含多个 match 对象（数组格式），脚本已支持完整批量遍历。
