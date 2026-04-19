# 🦅 Ares OSINT Telemetry Pipeline

> **"Truth > Completeness."** 

本项目是 Ares v4.1 量化推演引擎的专属外部数据喂养节点。负责在赛前获取高频异动赔率，在赛后遥测真实的物理技术指标（xG、控球率、红牌等），以 YAML 格式落盘至 Obsidian Vault，彻底消除量化模型在复盘与推演中的大模型幻觉 (AI Hallucination)。

---

## ⚙️ 核心模块 (Core Modules)

- `src/data/osint_postmatch.py` : 赛后物理数据遥测与方差 (Variance) 标记。
- `src/data/osint_crawler.py` : 足彩期号三段式扫描器，实现从中国体彩（500.com）到海外数据库（Understat）的双向映射。

## 🛠 开发与部署环境 (Setup)

为保证项目作为核心节点的高可用性和依赖干净，请强制使用虚拟环境。

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

3. **配置系统变量**
必须配置 `ARES_VAULT_PATH` 以接入 Ares Obsidian 知识库 (建议写入你的 `~/.zshrc` 或 `.env`):
```bash
export ARES_VAULT_PATH="/path/to/your/Vault"
```

## 🚀 运行示例 (Usage)

### 1. 赛前映射（Crawler）
获取中国足彩期号并从海外数据库寻找比赛映射 ID：
```bash
python src/data/osint_crawler.py --issue 24040
```
**产出物**：14 场赛事的结构化中英文对照清单，自动落盘为 `raw_reports/[issue]_dispatch_manifest.json`。

### 2. 赛后遥测（Postmatch）
执行赛后物理事实数据遥测（以西汉姆联为例）：
```bash
python src/data/osint_postmatch.py --issue 24040 --match-id 22064
```

**档案管道说明：**
* **Cold Data (冷数据)**：原始 json 保存至项目级 `raw_reports/`，用于随时无损复原。
* **Hot Data (热数据)**：提取洗练后带 Frontmatter 的 Markdown 报告（含战术分析与预期进球警告），自动输出至 `$ARES_VAULT_PATH/3_Resources/3.x_Match_Reports/`。若路径未正确挂载，系统将降级保存至项目的 `draft_reports/` 下避免数据丢失。

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
├── docs/                   # Ares体系文档与工作交接库
├── raw_reports/            # [自动生成] 冷数据暂存区及映射单 (Ignored)
└── draft_reports/          # [自动生成] 兜底热数据区 (Ignored)
```

## ⚠️ 贡献规范 (Contribution & Auditing)
- 强制使用 **Python 3.10+** 特性，并在核心函数加入严格的 `Type Hinting` 类型提示。
- 全局使用 `logging` 提供标准结构化输出，禁止使用 `print()` 破坏系统日志管道。
- 保证“小步快跑，单向流动”的数据思维，永远不在同一个脚本中循环反复读取黑石塔的数据。