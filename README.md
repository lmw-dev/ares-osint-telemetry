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

旧方式示例（依然可用）：
```bash
export ARES_VAULT_PATH="/path/to/your/Vault"
```

## 🚀 运行示例 (Usage)

### 1. 赛前映射（Crawler）
获取中国足彩期号并从海外数据库寻找比赛映射 ID。
*(内置高级网络爬取：利用逆向工程提取了 Understat 的私有隐密接口，无需 Selenium/代理池即可无痕获取数据且避免 API-Football 等授权费)*。
```bash
python src/data/osint_crawler.py --issue 24040
```
**产出物**：14 场赛事的结构化中英文对照清单，自动落盘为 `raw_reports/[issue]_dispatch_manifest.json`。

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
* **Hot Data (热数据)**：提取洗练后带 Frontmatter 的 Markdown 报告（含战术分析与预期进球警告），输出至 `$ARES_VAULT_PATH/03_Match_Audits/Postmatch_Telemetry/`。
* **Team Archives (球队底座)**：每场赛后会同步更新 `$ARES_VAULT_PATH/02_Team_Archives/`（每队 `latest_postmatch.json` + `postmatch_history.jsonl`）。
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
