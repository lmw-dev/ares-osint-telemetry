# intel_sweeper 回填升级记录（2026-04-21）

## 1. 目标
- 在保留新闻全文冷数据落盘能力的前提下，新增球队档案 `intel_base` 回填能力。
- 让日常情报抓取与 Team Archive 形成最小闭环。

## 2. 变更文件
- `src/data/intel_sweeper.py`（新建）

## 3. 实现要点
- 冷数据落盘：
  - 抓取或输入的新闻全文写入 `{ARES_VAULT_PATH}/04_RAG_Raw_Data/`
- 回填逻辑：
  - 目标档案：`{ARES_VAULT_PATH}/02_Team_Archives/{league}/{team}.md`
  - 读取并解析 Frontmatter
  - 更新字段：
    - `intel_base.market_sentiment`
    - `intel_base.recent_news_summary`
- 情绪分析接口（模拟）：
  - `analyze_sentiment(text)` 采用关键词规则
  - 包含 `injury` 或 `crisis` -> `Pessimistic`
  - 否则 -> `Neutral`
- 安全更新：
  - 保留 Markdown 正文原样
  - 仅重写 YAML（`yaml.safe_dump`）
  - 临时文件替换，避免写入中断造成损坏

## 4. CLI 用法
- `--team`、`--league`（必填）
- `--url` 可重复传入多条新闻链接
- `--text` 可重复传入手工新闻文本

## 5. 验证
- `./venv/bin/python -m py_compile src/data/intel_sweeper.py`
- `ARES_VAULT_PATH=/private/tmp/ares_vault_test ./venv/bin/python src/data/intel_sweeper.py --team Arsenal --league EPL --text "Injury crisis hits Arsenal before derby. Manager confirms two starters are unavailable."`
