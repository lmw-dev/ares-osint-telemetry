# 交接文档：recurring-team-signal-collection Skill 落地记录

**日期**: 2026-05-21  
**关联 Issue**: LMW-91  
**状态**: 已落地，待试运行  

---

## 一、交付物清单

| 文件 | 路径 | 说明 |
|---|---|---|
| Skill 主定义 | `src/skills/recurring-team-signal-collection/SKILL.md` | Agent 执行规范，含完整 taxonomy、schema、handoff 规则 |
| 单队信号记录模板 | `src/skills/recurring-team-signal-collection/templates/team_signal_log_template.md` | 单支球队信号记录的 Markdown 填写模板 |
| 周度采集报告模板 | `src/skills/recurring-team-signal-collection/templates/weekly_signal_collection_template.md` | 周度 weekly_baseline scan 的完整报告模板 |
| 本交接文档 | `docs/agents/handover_recurring_team_signal_collection_2026-05-21.md` | 落地记录与后续建议 |

---

## 二、Skill 核心规则速查

### Skill 定位
`recurring-team-signal-collection` = HOW layer，负责定期采集球队异常信号并建立闭环。

与现有 Skill 的关系：
- `football-team-news-flags`：赛前单次扫描 → 即时 prematch 判断
- `recurring-team-signal-collection`：持续定期采集 → 跨周期 durable learning 闭环

### 采集节奏
| 扫描类型 | 触发时机 | 重点信号 |
|---|---|---|
| `weekly_baseline` | 每周一/二 | coach_pressure, injury_cluster, tactical_drift, table_pressure |
| `matchday_live` | matchday -2 / -1 | goalkeeper_change, rotation_pattern, motivation_shift, market_narrative_risk |
| `postmatch_validation` | 赛后 24-48h | xg_anomaly, conversion_anomaly, defensive_leakage_anomaly |

### Signal Taxonomy（14 类）
`coach_pressure` / `coach_change` / `tactical_drift` / `injury_cluster` / `goalkeeper_change` / `rotation_pattern` / `motivation_shift` / `table_pressure` / `market_narrative_risk` / `xg_anomaly` / `conversion_anomaly` / `defensive_leakage_anomaly` / `fixture_congestion` / `media_fan_pressure`

### Severity（4 级）
- `CRITICAL` → prematch runtime_caveats（标红）
- `HIGH` → prematch runtime_caveats
- `MEDIUM` → prematch context notes
- `LOW` → background（连续 2+ 次升级为 MEDIUM）

### Signal Lifetime（5 种）
`expires_after_next_match` / `expires_after_matchday` / `monitor_2_matches` / `durable_candidate` / `archived_noise`

### Post-Match Promotion 规则
| 结果 | 条件 | 动作 |
|---|---|---|
| `durable_learning` | 赛后验证，具有跨场次复现性 | 推送至 Team Archive 长期记忆 |
| `one_off_noise` | 单场出现，无法复现 | 标记 archived_noise |
| `data_error` | 来源数据有误 | 立即 archived_noise，记录原因 |
| `needs_more_samples` | 样本不足 | 保持 monitor_2_matches，下轮继续 |

---

## 三、路径修正说明

原始 LMW-91 prompt 中的 `records/` 路径不存在于项目中，已按现有约定修正：

| 原路径 | 修正后路径 |
|---|---|
| `records/operations/team_signal_log_template.md` | `src/skills/recurring-team-signal-collection/templates/team_signal_log_template.md` |
| `records/operations/weekly_signal_collection_template.md` | `src/skills/recurring-team-signal-collection/templates/weekly_signal_collection_template.md` |
| `records/operations/recurring_team_signal_collection_protocol_YYYY-MM-DD.md` | 本文档 |
| `records/dev_log.md` | `docs/agents/INDEX.md`（追加条目） |

---

## 四、与现有代码结构的对齐

### Skill 注册机制
`src/skills/__init__.py` 的 `list_skills()` 是动态扫描，只要 `src/skills/recurring-team-signal-collection/SKILL.md` 存在，新 Skill 自动注册，无需修改注册代码。

### SkillRunner 兼容性
现有 `SkillRunner` 类支持任意 skill name，可直接使用：
```python
from src.skills.skill_runner import SkillRunner
runner = SkillRunner("recurring-team-signal-collection")
```

### 输出路径约定
- 中间产物：`raw_reports/recurring-team-signal-collection_{league}_{date}.jsonl`
- 最终报告：`draft_reports/recurring-team-signal-collection_{league}_{date}.md`
- 结构化数据：`draft_reports/recurring-team-signal-collection_{league}_{date}.json`

---

## 五、Antigravity 唤醒模板

```text
请加载并以 IsSkillFile 方式执行以下技能，开始球队信号定期采集：
技能路径: /Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/src/skills/recurring-team-signal-collection/SKILL.md

输入参数：
- league: 英超
- scan_type: weekly_baseline
- reference_date: 2026-05-21
- teams: []  # 留空则全量扫描
```

---

## 六、后续建议

### 一周试运行计划
1. **本周（2026-05-21）**：执行一次 `weekly_baseline` scan，覆盖英超全队，验证 Skill 执行流程
2. **matchday -2（下轮赛前两天）**：执行 `matchday_live` scan，生成 prematch handoff 清单
3. **赛后 24h**：执行 `postmatch_validation` scan，验证信号兑现情况
4. **复盘**：评估信号质量，调整 taxonomy 触发条件或 severity 阈值

### 潜在扩展方向（Out of Scope for v1.0）
- 信号持久化存储（SQLite 或 JSONL 累积文件）
- 跨周期信号趋势可视化
- 与 `football-physical-profile` Skill 的 xG 数据联动
- 自动触发 `team_archive_backfill.py` 更新 Team Archive

---

## 七、Linear Comment 记录

已在 LMW-91 更新以下内容：
- Skill 路径：`src/skills/recurring-team-signal-collection/SKILL.md`
- 核心规则：14 类 signal taxonomy、4 级 severity、5 种 lifetime、prematch handoff、post-match promotion
- 路径修正说明：`records/` → 项目实际路径
- 后续建议：进入一周试运行
