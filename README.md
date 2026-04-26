# 🦅 Ares OSINT Telemetry Pipeline

> **"Truth > Completeness."** 

本项目是 Ares v4.1 量化推演引擎的专属外部数据喂养节点。负责在赛前获取高频异动赔率，在赛后遥测真实的物理技术指标（xG、控球率、红牌等），以 YAML 格式落盘至 Obsidian Vault，彻底消除量化模型在复盘与推演中的大模型幻觉 (AI Hallucination)。

---

## 🛰️ 侦察与供血系统定位

- **系统名**：`21-ares-osint-telemetry`（数据管线）
- **定位**：系统的“眼睛、耳朵和矿工”，只负责对外沟通，把脏活累活干完。
- **职责**：爬取网页、调用 API、清洗脏数据、总结新闻情绪、计算简单统计学方差。
- **核心输出**：绝不轻易下结论（不预测比分），仅将清洗后的标准化 YAML 与 Markdown 文件写入 Obsidian 数据库。

---

## ⚙️ 核心模块 (Core Modules)

- `src/data/osint_postmatch.py` : 赛后物理数据遥测与方差 (Variance) 标记。
- `src/data/osint_crawler.py` : 足彩期号三段式扫描器，实现从中国体彩（500.com）到海外数据库（Understat）的双向映射。

## 🛠 开发与部署环境 (Setup)

为保证项目作为核心节点的高可用性和依赖干净，请强制使用虚拟环境。
推荐 Python `3.10+`（`3.9` 可运行，但会在自检中提示告警）。

1. **环境初始化**
```bash
# 1. 创建虚拟环境
python3 -m venv venv

# 2. 激活环境 (macOS/Linux)
source venv/bin/activate
```

2. **安装依赖**
```bash
pip install -r requirements.txt
```

3. **运行环境自检（推荐先执行）**
```bash
python scripts/env_doctor.py
```
如果你没有激活虚拟环境，也可直接使用：
```bash
./venv/bin/python scripts/env_doctor.py
```

4. **配置系统变量**
必须配置 `ARES_VAULT_PATH` 以接入 Ares Obsidian 知识库。支持两种方式：
```bash
# 方式A：写入当前 shell 环境（临时）
export ARES_VAULT_PATH="/path/to/your/Vault"
```
```bash
# 方式B：项目根目录创建 .env（推荐长期使用）
cp .env.example .env
# 然后编辑 .env 中的 ARES_VAULT_PATH
```
如果两者都配置，脚本优先使用当前 shell 已存在的环境变量，不会被 `.env` 覆盖。

推荐在 `.env` 同步配置 LLM（用于 `osint_postmatch.py` 的可选 Reality Gap 回填）：
```bash
# 必填：Ares Vault 根目录
ARES_VAULT_PATH="/path/to/your/Vault"

# 可选：是否启用 LLM 回填（默认 0）
ARES_USE_LLM_BACKFILL=0

# 可选：openai | gemini（默认 openai）
ARES_LLM_PROVIDER=openai

# 可选：统一 Key（优先级最高）
ARES_LLM_API_KEY=

# Provider 专用 Key（当 ARES_LLM_API_KEY 为空时生效）
OPENAI_API_KEY=
GEMINI_API_KEY=

# 可选：自定义网关地址（不填则自动使用官方）
# OpenAI 默认: https://api.openai.com/v1
# Gemini 默认: https://generativelanguage.googleapis.com/v1beta
ARES_LLM_BASE_URL=

# 可选：模型名（不填则按 provider 走默认）
ARES_LLM_MODEL=
```

可选：为赛前映射启用免费外部数据源补采（`osint_crawler.py`）：
```bash
# football-data 回退映射（未配置则跳过）
ARES_FOOTBALL_DATA_API_KEY=
# 可选，默认官方 v4
ARES_FOOTBALL_DATA_BASE_URL=

# The Odds API 补采赔率（默认关闭）
ARES_ENABLE_EXTERNAL_ODDS_ENRICH=0
ARES_THE_ODDS_API_KEY=
# 可选，默认官方 v4
ARES_THE_ODDS_BASE_URL=
```

旧方式示例（依然可用）：
```bash
export ARES_VAULT_PATH="/path/to/your/Vault"
```

## 🚀 运行示例 (Usage)

### 0. 脚本导航（建议按这个顺序）

| 目标 | 脚本 | 什么时候用 |
| --- | --- | --- |
| 先抓当期赛程与赔率，生成派发单 | `python src/data/osint_crawler.py --issue <issue>` | 当你还没有 `dispatch_manifest.json` 时 |
| 先做 prematch 预检总揽 | `python src/data/prematch_preflight.py --issue <issue>` | 当你要判断“直接跑全量”还是“先补档”时 |
| 批量补 Team Archives | `python src/data/team_archive_backfill.py --issue <issue> [--intel-file ...]` | 当预检结果提示 placeholder 队档过多，或你已经整理好一批 issue 级球队情报时 |
| 生成 issue 最终收口结论 | `python src/data/prematch_synthesis.py --issue <issue>` | 当 prematch 已跑完，准备输出最终执行结论时 |
| 赛后命中复盘（推演 vs 赛果） | `python src/data/prematch_outcome_review.py --issue <issue>` | 当官方比分入库后，评估 prematch 推演命中率 |
| 一键主流程 | `python src/data/osint_pipeline.py --issue <issue>` | 当预检通过或你确认可以继续跑时 |
| 单场/单队情报补录 | `python src/data/intel_sweeper.py --team <team> --league <league> --url ...` | 当你已经有明确新闻源，要回填单支球队情报时 |

### 1. 赛前映射（Crawler）
获取中国足彩期号并从海外数据库寻找比赛映射 ID。
*(内置高级网络爬取：利用逆向工程提取了 Understat 的私有隐密接口，无需 Selenium/代理池即可无痕获取数据且避免 API-Football 等授权费)*。
```bash
python src/data/osint_crawler.py --issue 24040
```
**产出物**：
- 配置了 `ARES_VAULT_PATH`：落盘到 `$ARES_VAULT_PATH/04_RAG_Raw_Data/Cold_Data_Lake/[issue]_dispatch_manifest.json`
- 未配置 `ARES_VAULT_PATH`：回退到 `raw_reports/[issue]_dispatch_manifest.json`
- 如需生成 issue 总揽页，继续执行：`python src/data/prematch_preflight.py --issue <issue>`

默认映射回退链路：
- `Understat`（主源）
- `FBref`（次级联赛回退）
- `football-data.org`（API 回退，需配置 Key）

如启用 `ARES_ENABLE_EXTERNAL_ODDS_ENRICH=1` 且配置 `ARES_THE_ODDS_API_KEY`，派发单中会新增 `external_odds_history` 字段。
赔率检索键不使用中国足彩 `issue`，而是使用已映射的赛程时间（`understat/football-data/fbref`）+ 主客队名。
若目标比赛为历史场次，免费版会记录 `skipped_historical_on_free_plan` 审计状态（不报错不中断）。

### 1.5 一键全流程（推荐）
一条命令串起：`crawler -> 审计目录路由 -> postmatch 批量复盘 -> 索引更新`。
```bash
python src/data/osint_pipeline.py --issue 24040
```
常用参数：
```bash
# 只跑 crawler + 路由，不跑 postmatch
python src/data/osint_pipeline.py --issue 24040 --skip-postmatch

# 跳过 crawler，只消费已有 dispatch_manifest 继续 postmatch
python src/data/osint_pipeline.py --issue 24040 --skip-crawler

# 关闭按场次输入质量门槛（默认已开启）
python src/data/osint_pipeline.py --issue 24040 --no-prematch-ready-gate
```

### 1.6 Prematch 预检总揽（推荐先跑）
在进入全量 prematch 前，先生成 issue 总揽页，判断是直接跑主流程，还是先补 Team Archives / 映射锚点。
```bash
python src/data/prematch_preflight.py --issue 24040
```
**产出物**：
- `$ARES_VAULT_PATH/03_Match_Audits/{issue}/Audit-{issue}.md`
- `$ARES_VAULT_PATH/03_Match_Audits/{issue}/Audit-{issue}-team-diagnostics.json`
- `$ARES_VAULT_PATH/03_Match_Audits/{issue}/03_Review_Reports/TEAM-INTEL-{issue}.generated.json`
- `$ARES_VAULT_PATH/03_Match_Audits/{issue}/03_Review_Reports/UNMAPPED-ANCHORS-{issue}.generated.json`

### 1.7 批量补 Placeholder Team Archives
当 `Audit-{issue}.md` 提示 placeholder 队档过多时，先批量把空壳档案升级成可维护模板；如果你已经整理了 issue 级球队情报，还可以直接批量把部分队档提升为 `usable`。若未手工准备 intel 文件，脚本会自动尝试读取预检生成的 `TEAM-INTEL-{issue}.generated.json`。
```bash
python src/data/team_archive_backfill.py --issue 24040
```
```bash
python src/data/team_archive_backfill.py --issue 24040 --intel-file /path/to/TEAM-INTEL-24040.json
```
**产出物**：
- 更新 `$ARES_VAULT_PATH/02_Team_Archives/` 中本期相关的 placeholder 球队档案
- 写入 `$ARES_VAULT_PATH/03_Match_Audits/{issue}/03_Review_Reports/REVIEW-{issue}-Team_Archive_Backfill.md`
- 自动优先读取：
  - `$ARES_VAULT_PATH/03_Match_Audits/{issue}/03_Review_Reports/TEAM-INTEL-{issue}.json`
  - 若不存在，则回退读取 `$ARES_VAULT_PATH/03_Match_Audits/{issue}/03_Review_Reports/TEAM-INTEL-{issue}.generated.json`

### 1.8 unmapped 锚点回注（可选）
当 `Audit-{issue}.md` 仍有大量 `unmapped`，可先编辑预检生成的锚点骨架，再重新跑 crawler 做回注：
```bash
# 1) 先把 generated 复制成手工版并补字段（understat_id / fbref_url / football_data_match_id 三选一即可）
# 03_Match_Audits/{issue}/03_Review_Reports/UNMAPPED-ANCHORS-{issue}.json

# 2) 重新跑 crawler，自动应用手工锚点覆盖
python src/data/osint_crawler.py --issue 24040
```
手工锚点文件优先级高于 generated 骨架。

Titan prematch 补采（默认开启）：
- `osint_crawler.py` 会优先从 500 行内链接自动提取 `cn_match_id`，再抓取以下页面并写入 `dispatch_manifest` 的 `titan_prematch`：
  - `https://zq.titan007.com/analysis/{id}cn.htm`
  - `https://vip.titan007.com/AsianOdds_n.aspx?id={id}&l=0`
  - `https://vip.titan007.com/OverDown_n.aspx?id={id}&l=0`
  - `https://1x2.titan007.com/oddslist/{id}.htm`
- 对应原始 HTML 冷数据会落盘到 `Cold_Data_Lake`，并自动合并进 `cold_data_refs`。
- 如需临时关闭，可设置：`ARES_ENABLE_TITAN_PREMATCH_ENRICH=0`。

如需做回归测试（不依赖真实第三方锚点），可用 smoke 注入脚本：
```bash
# 自动给前 3 个 unmapped 场次注入测试锚点
python src/data/unmapped_anchor_seed.py --issue 24040 --mode smoke --allow-smoke --smoke-count 3

# 只给指定场次注入测试锚点
python src/data/unmapped_anchor_seed.py --issue 24040 --mode smoke --allow-smoke --indices 2,3,4

# 清理 smoke 锚点
python src/data/unmapped_anchor_seed.py --issue 24040 --clear-smoke
```
注意：
- `unmapped_anchor_seed.py` 默认 `--mode production`，不会生成 synthetic 锚点；只有显式 `--mode smoke --allow-smoke` 才会注入测试锚点。
- smoke 锚点仅用于流程回归，不应用于正式生产回放。`prematch_preflight.py` 会在 `Audit-{issue}.md` 中显式标注 smoke 场次。

一键回归（seed -> crawler -> preflight）：
```bash
# smoke 回归：注入后立即跑全链路
python src/data/prematch_regression.py --issue 24040 --mode smoke --smoke-count 3

# 清理 smoke 后重跑全链路
python src/data/prematch_regression.py --issue 24040 --mode smoke --clear-smoke
```

Prematch 输入质量门槛（默认开启）：
- `osint_pipeline.py` 会在进入 `20-engine audit-issue` 前按场次检查输入质量（两队 `archive_status=usable`、`needs_enrichment=false`、且 `rag_doc_count` 达到阈值）。
- 不达标场次会被自动过滤，避免“低质量输入 -> 模板化回避结论”。
- 过滤明细会写入：`$ARES_VAULT_PATH/03_Match_Audits/{issue}/03_Review_Reports/REVIEW-{issue}-Prematch_Input_Gate.md`
- 同时会输出球队补强优先队列（阻断原因 + 优先级）：`REVIEW-{issue}-Team_Enrichment_Queue.md` 与 `TEAM-ENRICHMENT-QUEUE-{issue}.json`
- 阈值可通过环境变量调整：`ARES_PREMATCH_READY_MIN_TEAM_DOCS`（默认 `3`）。

Prematch 最终收口（LLM 综合）：
```bash
# 默认读取 ARES_USE_LLM_SYNTHESIS（未设置时回退 ARES_USE_LLM_BACKFILL）
python src/data/prematch_synthesis.py --issue 24040

# 如需禁用 LLM，走规则兜底
python src/data/prematch_synthesis.py --issue 24040 --force-rule
```
产出物：
- `$ARES_VAULT_PATH/03_Match_Audits/{issue}/02_Special_Analyses/FINAL-{issue}-Prematch_Synthesis.md`
- `$ARES_VAULT_PATH/03_Match_Audits/{issue}/02_Special_Analyses/FINAL-{issue}-Prematch_Synthesis.json`

Prematch 赛后命中复盘（当比分已入库）：
```bash
# 全量场次回测
python src/data/prematch_outcome_review.py --issue 24040

# 仅五大联赛口径回测（对应 FINAL-...-Top5）
python src/data/prematch_outcome_review.py --issue 24040 --top5-only
```
产出物：
- `$ARES_VAULT_PATH/03_Match_Audits/{issue}/03_Review_Reports/REVIEW-{issue}-Prematch_Outcome.md`
- `$ARES_VAULT_PATH/03_Match_Audits/{issue}/03_Review_Reports/REVIEW-{issue}-Prematch_Outcome-Top5.md`

`--intel-file` 结构示例：
```json
{
  "issue": "24040",
  "teams": [
    {
      "team": "Arsenal",
      "manager_doctrine": "High press with left-sided overloads.",
      "market_sentiment": "Pessimistic",
      "recent_news_summary": "Recent coverage focuses on rotation pressure and a thinner midfield base.",
      "key_node_dependency": ["left-side progression", "rest defense"],
      "tactical_logic": {
        "P": "P2",
        "Space": "V",
        "F": "M",
        "H": "M",
        "Set_Piece": "N"
      },
      "avg_xG_last_5": 1.42,
      "conversion_efficiency": 0.1,
      "defensive_leakage": 0.54,
      "actual_tactical_entropy": 0.46,
      "bias_type": "Overestimated",
      "prematch_focus_items": [
        "Rest-defense exposure after fullback advance",
        "Set-piece second-ball control"
      ]
    }
  ]
}
```


### 2. 赛后遥测（Postmatch）
执行赛后物理事实数据遥测（以西汉姆联为例）：
```bash
python src/data/osint_postmatch.py --issue 24040 --match-id 22064
```

默认即为 `--source auto`，会优先走 Understat，失败时自动回退 FBref。

如需强制使用 FBref（适合非五大联赛补采）：
```bash
python src/data/osint_postmatch.py \
  --issue 24040 \
  --match-id "fbref-match-ref" \
  --source fbref \
  --fbref-url "https://fbref.com/en/matches/<match-id>/<slug>"
```

如需做官方比分污染校验（不一致即中止热报告落盘）：
```bash
python src/data/osint_postmatch.py --issue 24040 --match-id 22064 --official-score 2-1
```

**档案管道说明：**
* **Cold Data (冷数据)**：保存结构化冷数据，同时落盘源站原始响应（赛前 `500_raw.html/json`、赛后 `understat_raw.html/json` 或 `fbref_raw.html`）到 `$ARES_VAULT_PATH/04_RAG_Raw_Data/Cold_Data_Lake/`。
* **Hot Data (热数据)**：提取洗练后带 Frontmatter 的 Markdown 报告（含战术分析与预期进球警告），输出至 `$ARES_VAULT_PATH/03_Match_Audits/{issue}/04_Postmatch_Telemetry/`。
* **Team Archives (球队底座)**：`osint_pipeline.py` 默认会先按 issue 自动补齐本期球队 Markdown 档案，再由赛后流程持续更新 `$ARES_VAULT_PATH/02_Team_Archives/`（每队 `latest_postmatch.json` + `postmatch_history.jsonl`）。
* **Team Archive Backfill**：`team_archive_backfill.py` 会按 issue 批量扫描 placeholder 队档，把默认空壳升级成统一的可维护模板；若提供 `--intel-file`（或 issue 目录下存在 `TEAM-INTEL-{issue}.json`），还会把结构化情报直接写入 frontmatter 与正文，并将满足最小实质内容的队档提升为 `archive_quality: usable`。
* **Placeholder Backfilled 语义**：`archive_quality: placeholder_backfilled` 表示“模板已回填但内容仍低质量占位”，在 `prematch_preflight.py` 中会独立识别，不会按 `usable` 处理。
* **Audit Router (审计路由)**：自动创建 `$ARES_VAULT_PATH/03_Match_Audits/{issue}/01~04` 结构、自动生成 prematch 骨架、自动归档重复 prematch/postmatch、自动执行 prematch 质量闸门（`draft` / `Insufficient Resilience Data` / `low confidence` / `cross-team contamination` 自动转入 `03_Review_Reports`）、按 manifest canonical 名收敛同场 prematch / rejected review 重复稿、自动更新 `00_Governance/INDEX`。
* **Prematch Soft Gate Recovery**：`audit_router.py` 现在会保留正式生成但仅命中 `low confidence / Insufficient Resilience Data` 的 prematch，并自动清理历史上误移入 `REJECTED-*` 的软门禁存量；`draft` 与 `cross-team contamination` 仍维持硬拒收。
* **Prematch RAG Readiness Gate**：`osint_pipeline.py` 在调用 `20-engine audit-issue` 前，会先检查 `20-engine/chromadb/chroma.sqlite3` 的文档量和 issue 球队覆盖率。默认要求 issue 球队 coverage ratio 至少 `75%`，且缺失球队不超过 `4` 支；若 RAG 库明显供给不足，将直接阻断 prematch，写入 `REVIEW-{issue}-Prematch_Blocker.md`，避免链路“跑完再整批 REJECTED”。
* **Prematch Preflight Overview**：`prematch_preflight.py` 会单独生成 `Audit-{issue}.md`，并区分 `usable / placeholder / placeholder_backfilled / missing` 四类 Team Archive 状态；总览页中的摘要统计、比赛看板、球队诊断、风险场次会保持一致，作为 agent 是否继续主流程的前置判断。
* **Prematch Immediate Closeout**：`osint_pipeline.py` 在 `20-engine audit-issue` 完成后，会立刻再次执行 `audit_router` 收口，不再等 postmatch 收尾后才搬运低质量 prematch。
* **Engine Direct-Run Safety Net**：`20-ares-v4-engine/main.py audit-issue` 在直跑写入 prematch 后，也会尝试回调同目录下的 `21-ares-osint-telemetry/src/data/audit_router.py`，避免 direct-run 绕过质量闸门。
* **Postmatch Official-Score Gate**：`osint_pipeline.py --issue <issue>` 只有在 dispatch manifest 已具备足够的 `official_score/result_score` 后才会继续 batch postmatch；若官方比分尚未入库，则直接跳过 postmatch，避免把赛前/串期映射误落到 `04_Postmatch_Telemetry/`。
* **批量模式命名规则**：每场单独输出为 `{issue}_{match_id}_postmatch.md`，避免 14 场互相覆盖。
* **数据源审计字段**：YAML 中新增 `data_source` 与 `data_source_ref`，可追溯本场来自 Understat 还是 FBref。

## 📁 工程约定架构 (Directory Structure)

```text
ares-osint-telemetry/
├── README.md               # 项目主说明
├── requirements.txt        # PIP依赖清单
├── .gitignore              # 规避大体积产出物与敏感环境变量提交
├── venv/                   # 环境沙盒 (勿提交)
├── src/
│   └── data/
│       ├── osint_crawler.py        # [映射] 足彩期号映射搜刮抓取器
│       ├── osint_postmatch.py      # [核心] 赛后物理抽取脚本
│       └── team_alias_map.json     # [字典] Ares中英文队名映射集
├── scripts/
│   └── env_doctor.py               # 环境自检脚本（版本/依赖/路径/入口检查）
├── docs/                   # Ares体系文档与工作交接库
├── raw_reports/            # [自动生成] 冷数据暂存区及映射单 (Ignored)
└── draft_reports/          # [自动生成] 兜底热数据区 (Ignored)
```

## ⚠️ 贡献规范 (Contribution & Auditing)
- 强制使用 **Python 3.10+** 特性，并在核心函数加入严格的 `Type Hinting` 类型提示。
- 全局使用 `logging` 提供标准结构化输出，禁止使用 `print()` 破坏系统日志管道。
- 保证“小步快跑，单向流动”的数据思维，永远不在同一个脚本中循环反复读取黑石塔的数据。
