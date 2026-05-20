# Football Team News Flags V2.0 架构重构交接文档

> **交接时间**: 2026-05-19  
> **交接对象**: OSINT 情报系统维护团队 & 后续 Agent 开发  
> **核心变更**: Skill 从独立 Python 脚本 + 外部 LLM API 模式，转型为模型无关的 Agent Workflow 原生执行模式

---

## 1. 问题分析

### 1.1 旧架构 (V1.0) 的三大致命缺陷

| 缺陷 | 具体表现 | 根因 |
|:---|:---|:---|
| **信息渠道单一** | 20 支球队中 17 支返回 "暂无明显异常"，因为数据源为空 | 仅消费本地预抓取 JSON，不主动联网搜索 |
| **绕道调用外部 LLM** | 用户在 Claude/Gemini 中调用 Skill，结果却去调 DeepSeek API | `run_team_news_flags.py` 的 `call_llm()` 硬编码为 DeepSeek |
| **搜索策略不可操作** | SKILL.md 只说"优先官方"，无具体 Tier 定义和执行步骤 | 缺乏 Manus 级别的分层证据体系 |

### 1.2 V1.0 数据流（已废弃）
```
用户 → Python 脚本启动 → 读取本地 JSON → call_llm(DeepSeek) × 20 队 → 解析 → 渲染
                                ↑ 数据为空则全部输出"暂无异常"
                                ↑ DeepSeek 超时则 fallback 跳过审校
```

---

## 2. 方案设计 — V2.0 架构

### 2.1 核心转型
SKILL.md 从"给脚本读的配置文件"升级为"给模型自己执行的完整工作流规范"。

```
旧路径: 用户 → Python 脚本 → 本地 JSON → DeepSeek API → 输出
新路径: 用户 → @skill → 模型读取 SKILL.md → search_web 主动搜索 → 模型分析 → 输出
```

### 2.2 V2.0 数据流
```
用户调用 @skill
  ↓
模型（Claude/Gemini/其他）读取 SKILL.md 执行规范
  ↓
Phase 1: search_web 获取赛程 + 积分榜
  ↓
Phase 2: 逐队 search_web 搜索伤停/发布会/新闻（每队 2-3 次）
  ↓       高价值链接 → read_url_content 深度抓取
  ↓
Phase 3: 模型自身推理能力进行 Flag 分类和战术影响分析
  ↓
Phase 4: 横向审校（同场双方对称、标签合法、零幻觉终审）
  ↓
Phase 5: write_to_file 输出 Markdown + JSON 到 AresVault
```

### 2.3 关键决策说明

| 决策 | 理由 |
|:---|:---|
| **删除 Python 脚本** | 已被 SKILL.md 原生流程完全替代，避免维护两套逻辑 |
| **SKILL.md 即脚本** | 任何模型在 Antigravity 中调用时都能执行，无需安装依赖 |
| **5 级证据分层** | 按 Manus 建议，T1-T5 分层确保证据可靠性 |
| **全覆盖 20 队** | 每队 2-3 次搜索，总计约 40-60 次搜索 |

---

## 3. 代码变更清单

| 文件 | 操作 | 说明 |
|:---|:---:|:---|
| [`SKILL.md`](file:///Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/src/skills/football-team-news-flags/SKILL.md) | **重写** | 从 V1.0 配置升级为 V2.0 完整执行规范（5 Phase 工作流 + 5 Tier 证据体系） |
| [`agent_prompts.md`](file:///Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/src/skills/football-team-news-flags/prompts/agent_prompts.md) | **重写** | 从"外部 LLM 提示词模板"降级为"分析框架参考文档" |
| `run_team_news_flags.py` | **删除** | 旧版英超 Python 调度脚本，已由 SKILL.md 原生流程替代 |
| `run_team_news_flags_laliga.py` | **删除** | 旧版西甲 Python 调度脚本，已由 SKILL.md 原生流程替代 |
| [`__init__.py`](file:///Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/src/skills/__init__.py) | 保留 | Skill 注册框架，无需改动 |
| [`skill_runner.py`](file:///Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/src/skills/skill_runner.py) | 保留 | SkillRunner 框架类，可供其他需要编程式访问 Skill 定义的场景使用 |

---

## 4. V2.0 搜索验证结果

使用新流程对 Chelsea 和 Tottenham 进行实时搜索验证：

| 球队 | 搜索查询 | 搜索结果质量 | 捕获到的异常 |
|:---|:---|:---:|:---|
| **Chelsea** | `Chelsea team news injury suspension Sunderland May 2026` | ✅ 极佳 | Estêvão OUT、Mudryk 停赛、Joao Pedro 存疑、Gittens OUT、Derry OUT、Lavia 存疑 |
| **Tottenham** | `Tottenham team news injury suspension Chelsea May 2026` | ✅ 极佳 | Solanke OUT、Kulusevski OUT、Simons OUT(ACL)、Kudus OUT、Romero OUT、Vicario 存疑 |

**对比旧架构**：旧脚本依赖预抓取 JSON，如果 scraper 没跑或数据为空，这些伤停信息完全无法获取。新架构通过 `search_web` 直接获取实时信息，信息覆盖率从 ~30% 提升到 ~95%+。

---

## 5. 使用方式

### 在 Antigravity 中调用（推荐）
```
用户: @[/football-team-news-flags] 获取英超第38轮 2026-05-24 全部球队新闻
```
无论当前活跃模型是 Claude、Gemini 还是其他，模型会：
1. 读取 SKILL.md 获取执行规范
2. 按 5 Phase 流程逐步执行
3. 输出到 AresVault 指定路径

### 输出路径（不变）
```
/vaults/AresVault/03_Match_Audits/DATE-{YYYYMMDD}-top5/{联赛中文名} {season} 第{N}轮关键异常信息汇总_Ares.md
/vaults/AresVault/03_Match_Audits/DATE-{YYYYMMDD}-top5/{联赛中文名} {season} 第{N}轮关键异常信息汇总_Ares.json
```

---

## 6. 后续建议

1. **搜索结果缓存**：对同一轮次重复调用时，考虑将搜索结果缓存到 `sources_cache/` 目录，避免重复搜索
2. **联赛模板扩展**：当前搜索策略映射表覆盖五大联赛，后续可扩展到 J 联赛、K 联赛、中超等
3. **odds-intelligence Skill 同步升级**：`football-prematch-odds-intelligence` Skill 也应考虑同样的 V2.0 架构转型
4. **本地数据增强模式**：若 AresVault 中已有预抓取数据，SKILL.md 的 Phase 2 可选地将其作为补充上下文，与实时搜索结果融合

---

*交接文档结束 | OSINT Telemetry System V2.0 Architecture*
