# Ares OSINT Telemetry Pipeline - `osint_postmatch.py` 交接文档

## 1. 问题分析（架构视角）
当前系统需要在比赛结束后回收数据，进行复盘反馈，该项目的主要要求为能够可靠地通过抓取提取物理结果并与主模型进行修正核对（SSOT）。核心痛点在于：
* **容错性**：数据源不稳定时，可能会出现脏数据。
* **分离解耦**：原始抓取数据（Cold Data）需完好保留备查。分析落盘（Hot Data）结构需标准化以适应 Obsidian Dataview 或其它向量检索工具的要求。
* **物理倒挂识别**：在球场上存在大量玄学现象（即输了预期进球，却赢了比分的局面），即著名的 Variance（方差扰动），系统需自动剔除这种情况带来的数据混淆。

## 2. 方案设计
主要采用模块化、管线式的 `Pipeline` 设计：

```text
Run 命令 -> [抓取与保存 JSON (Cold)] -> [解析核心因子 (Hot)] -> [计算 Variance] -> [生成包含 YAML 的 Markdown 注入 Obsidian]
```
- **核心模块拆分**：分为 `fetch_raw_data`、`extract_hot_features`、`calculate_variance` 与 `generate_markdown`。
- **环境隔离配置**：考虑到不同环境部署情况，没有配置完整的 `ARES_VAULT_PATH` 时会预设一个降级兜底的目录 `draft_reports` 保证运行完整。

## 3. 代码实现重点
* **异常处理**文件加载、路径注入都有 `try...catch` 保驾护航，避免阻断任务。
* 使用 `python 3.10+` `Typing` 以及基于 `logging` 标准库提供健全审计日志流。

## 4. 部署与执行建议
* 必须确保系统已经安装依赖库 `pyyaml` (例如使用 `pip/uv/poetry install pyyaml`)。
* 为了将报告写入您的 Obsidian 中，可以将其环境变量 `ARES_VAULT_PATH` 放置于 bash 相关 `.env` 或 `rc` 结尾配置：`export ARES_VAULT_PATH="/path/to/your/Vault"`。
* 未来实现 V5.0 时，只需拓展 `fetch_raw_data` 中的抓取逻辑或者接入 LLM 即可不改动主流程。
