# osint_crawler 免费 API 插件化升级记录（2026-04-22）

## 1. 目标
- 将赛前映射升级为“多源回退”插件化链路：`Understat -> FBref -> football-data.org`。
- 可选接入 The Odds API，给派发单补采外部赔率快照。
- 所有配置统一可由 `.env` 驱动。

## 2. 变更文件
- `src/data/osint_crawler.py`
- `README.md`
- `.env.example`

## 3. 核心改动
- 新增 `.env` 自动加载（不覆盖外部已注入环境变量）。
- 新增 football-data 回退映射：
  - 支持赛事代码：`PL/PD/BL1/SA/FL1/ELC/BL2/FL2/SB`
  - 时间门禁仍保留，避免映射到历史错场。
  - 原始响应冷存储到 `raw_reports/{issue}_football_data_{code}_raw.json`。
- 新增 The Odds API 可选赔率补采：
  - 开关：`ARES_ENABLE_EXTERNAL_ODDS_ENRICH`
  - 按联赛拉取缓存，按“主客队 + 时间”匹配事件，降低串场概率。
  - 写入 `dispatch_manifest.json` 的 `external_odds_history`。
  - 原始响应冷存储到 `raw_reports/{issue}_the_odds_{sport}_raw.json`。
- `dispatch_manifest.json` 新增字段：
  - `football_data_match_id`, `football_data_date`, `football_data_gap_days`, `football_data_competition`
  - `mapping_source`（`understat|fbref|football-data|unmapped`）
  - 可选 `external_odds_history`

## 4. 新增环境变量
- `ARES_FOOTBALL_DATA_API_KEY`
- `ARES_FOOTBALL_DATA_BASE_URL`（可选）
- `ARES_ENABLE_EXTERNAL_ODDS_ENRICH`
- `ARES_THE_ODDS_API_KEY`
- `ARES_THE_ODDS_BASE_URL`（可选）

## 5. 验证
- `PYTHONPYCACHEPREFIX=/tmp/pycache ./venv/bin/python -m py_compile src/data/osint_crawler.py`
