# Manus Skill 迁移交接文档

**日期：** 2026-05-19  
**任务：** 将 Manus 生成的两个 football skill 移植到 ares-osint-telemetry 项目

---

## 一、变更摘要

将 `docs/skill/` 下两个 Manus `.skill` ZIP 包解包，重构为本项目可识别的 `src/skills/` 目录结构，并创建统一 Skill 加载 API 和运行器。

### 新增文件一览

```
src/skills/
├── __init__.py                                  # Skill 注册 & 加载 API
├── skill_runner.py                              # SkillRunner 执行器（含路径约定）
│
├── football-team-news-flags/                    # 球队异常信息 Skill
│   ├── SKILL.md                                 # 主定义（适配本项目）
│   ├── prompts/agent_prompts.md                 # System/User/Worker/Reviewer prompts
│   ├── references/
│   │   ├── flag_taxonomy_and_review.md          # 触发/排除规则（边界案例）
│   │   ├── platform_adapter.md                  # Manus/Claude Code/Codex 适配
│   │   ├── pseudocode.md                        # 平台无关算法 + Mermaid 流程图
│   │   └── universal_agent_spec.md              # 独立 Agent 规范（AGENTS.md 用）
│   └── templates/
│       ├── team_flags_schema.json               # JSON Schema (2020-12)
│       └── team_news_report_template.md         # Markdown 报告模板
│
└── football-prematch-odds-intelligence/         # 赔率市场情报 Skill
    ├── SKILL.md                                 # 主定义（适配本项目）
    ├── references/data-source-strategy.md       # 数据源发现 & 时间戳处理
    ├── scripts/normalize_odds_report.py         # 公司名规范化 & 赔率方向汇总
    └── templates/prematch_odds_report_template.md
```

---

## 二、Skill 说明

### 2.1 football-team-news-flags（球队异常信息）

**职责：** 收集、判定、规范化足球赛前球队关键异常标志。

**8 个规范 Flag：**

| Flag | 触发场景 |
|---|---|
| `主帅下课/更衣室问题` | 下课/临时主帅/内部危机 |
| `核心球员伤停` | 核心首发确认缺席/停赛 |
| `多名主力轮换` | 官方/可信信源证实大轮换 |
| `杯赛前后` | 临近重要杯赛影响选人强度 |
| `欧战资格已锁定` | 数学锁定欧战，目标结构变化 |
| `已降级/已夺冠/已无欲无求` | 数学降级/夺冠/无可达目标 |
| `球队近期负面舆论明显` | 球迷抗议/俱乐部危机/纪律事件 |
| `临场阵容确认后有重大变化` | 仅在官方首发后使用 |

**输入/输出路径约定：**
- 输入：`data/input_matches.json`
- 中间产物：`raw_reports/football-team-news-flags_{league}_{date}.jsonl`
- 最终报告：`draft_reports/football-team-news-flags_{league}_{date}.md/.json`

### 2.2 football-prematch-odds-intelligence（赔率情报）

**职责：** 收集公司级欧赔/亚盘/大小球，分析盘口时间逻辑。

**优先博彩公司：** 威廉、澳门、立博、365、易胜博、伟德、Pinnacle/平博、Betfair/交易所类

**时间戳优先级：** 历史最后变化时间 → 来源时间戳 → 抓取时间（`source_no_timestamp`）→ null

**输入/输出路径约定：**
- 中间产物：`raw_reports/football-prematch-odds-intelligence_{league}_{date}.jsonl`
- 最终报告：`draft_reports/football-prematch-odds-intelligence_{league}_{date}.md/.json`

---

## 三、API 使用方式

### 3.1 基础用法

```python
from src.skills import list_skills, load_skill_definition

# 列出所有已注册 skill
print(list_skills())
# => ['football-prematch-odds-intelligence', 'football-team-news-flags']
```

### 3.2 SkillRunner 用法

```python
from src.skills.skill_runner import team_news_runner, odds_runner

# --- 球队异常信息 ---
runner = team_news_runner()

# 获取 System Prompt（可直接送入 Claude/GPT API）
system_prompt = runner.get_system_prompt()

# 构建上下文并渲染 User Prompt
ctx = runner.build_context(
    league="英超",
    season="2025/26",
    round="第38轮",
    date="2026-05-19",
    matches=["阿森纳 vs 切尔西", "曼城 vs 利物浦"],
)
user_prompt = runner.render_user_prompt(ctx)

# 获取单队 worker / reviewer prompt
worker_prompt = runner.get_single_team_worker_prompt()
reviewer_prompt = runner.get_reviewer_prompt()

# 输出路径
raw_path = runner.raw_output_path("英超", "2026-05-19")   # raw_reports/...
draft_path = runner.draft_output_path("英超", "2026-05-19")  # draft_reports/...

# --- 赔率情报 ---
runner2 = odds_runner()
data_source_guide = runner2.get_reference("data-source-strategy.md")
report_template = runner2.get_template("prematch_odds_report_template.md")
```

### 3.3 与现有 pipeline 集成建议

在 `prematch_preflight.py` 或 `prematch_synthesis.py` 中：

```python
from src.skills.skill_runner import team_news_runner, odds_runner

# 赛前信息收集阶段：注入球队异常 skill 规范
news_runner = team_news_runner()
# 将 system_prompt 传入 Claude/GPT 调用
# 将输出写入 raw_output_path

# 赛前赔率收集阶段：注入赔率 skill 规范
odd_runner = odds_runner()
# 将数据源策略作为参考注入
```

---

## 四、架构决策

| 决策点 | 选择 | 理由 |
|---|---|---|
| Skill 目录位置 | `src/skills/` | 与现有 `src/data/` 同级，属于源代码层 |
| 保留原始资源文件 | 是 | references/templates/prompts 完整保留，不降级 |
| SKILL.md 重写 | 是（适配本项目） | 新增项目路径约定、frontmatter inputs/outputs、平台适配说明 |
| 加载 API | `__init__.py` + `skill_runner.py` | 解耦文件路径与使用方；支持懒加载 |
| 临时解压目录 | 已清理（`tmp/skill_extract/`） | 不遗留中间产物 |

---

## 五、后续建议

1. **与 Claude API 集成**：`SkillRunner.get_system_prompt()` 可直接传入 Claude API 的 `system` 参数，`render_user_prompt(ctx)` 传入 `user` 参数，实现零胶水代码调用。

2. **扩展 skill**：新增 skill 只需在 `src/skills/` 下创建子目录并放置 `SKILL.md`，`list_skills()` 自动识别。

3. **`normalize_odds_report.py` 集成**：赔率规范化脚本已复制至 `src/skills/football-prematch-odds-intelligence/scripts/`，可在赔率数据后处理阶段引入。

4. **JSON Schema 校验**：`team_flags_schema.json` 可用于 `jsonschema` 库校验球队异常 JSON 输出，建议在 `tests/` 中加入对应单元测试。

5. **原始 `.skill` 文件**：`docs/skill/` 目录下的原始 ZIP 文件已保留，作为历史存档，不影响项目运行。
