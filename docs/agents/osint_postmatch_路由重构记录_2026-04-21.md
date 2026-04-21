# osint_postmatch 路由重构记录（2026-04-21）

## 1. 目标
- 将 `osint_postmatch.py` 的冷/热数据落盘从项目本地目录迁移到 Vault 统一数据分层目录。
- 保留球队底座档案的稳定更新路径，确保赛后更新可持续沉淀。

## 2. 路由调整
- 热数据（Markdown）：
  - 旧：`3_Resources/3.x_Match_Reports/`
  - 新：`{ARES_VAULT_PATH}/03_Match_Audits/Postmatch_Telemetry/`
- 冷数据（JSON/HTML）：
  - 旧：项目目录 `raw_reports/`
  - 新：`{ARES_VAULT_PATH}/04_RAG_Raw_Data/Cold_Data_Lake/`
- 球队底座：
  - 保持目标：`{ARES_VAULT_PATH}/02_Team_Archives/`
  - 新增每场自动更新：
    - `latest_postmatch.json`
    - `postmatch_history.jsonl`

## 3. 代码变更点
- 文件：`src/data/osint_postmatch.py`
- 关键改造：
  - 初始化阶段新增三类目录路由：`cold_data_dir`、`hot_reports_dir`、`team_archives_dir`
  - `_dump_raw_artifact` 与 `_dump_raw_json_artifact` 全量改写到 `cold_data_dir`
  - `generate_markdown` 改写到 `hot_reports_dir`
  - 新增 `update_team_archives(hot_data)`，每场落盘后更新两支球队档案
  - 批量模式 manifest 路径优先从 `04_RAG_Raw_Data/Cold_Data_Lake/` 读取

## 4. 兼容处理
- 支持 `.env` 中 shell 转义写法路径（如 `Mobile\ Documents`、`com\~apple\~CloudDocs`）。
- 若 `ARES_VAULT_PATH` 缺失，仍降级到项目目录以避免中断。

## 5. 验证
- 命令：
  - `python3 -m py_compile src/data/osint_postmatch.py`
  - `./venv/bin/python src/data/osint_postmatch.py --issue 26063 --match-id 23191 --source understat --official-score 3-0`
- 结果：
  - 冷数据成功写入 `04_RAG_Raw_Data/Cold_Data_Lake/`
  - 热数据成功写入 `03_Match_Audits/Postmatch_Telemetry/`
  - 球队档案成功写入 `02_Team_Archives/`

