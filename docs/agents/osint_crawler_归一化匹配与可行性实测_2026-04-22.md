# osint_crawler 归一化匹配与可行性实测记录（2026-04-22）

## 1. 背景
- 目标：验证 `.env` 下 `football-data` 与 `The Odds API` 是否可用，并修复回退映射命中率。
- 测试期号：`26064`。

## 2. 代码修复
- 文件：`src/data/osint_crawler.py`
- 修复点：
  - `ARES_FOOTBALL_DATA_BASE_URL` / `ARES_THE_ODDS_BASE_URL` 为空时，自动回退官方默认地址。
  - 队名标准化增强：
    - 统一 ASCII 归一化（去重音符号）。
    - 去除常见后缀（`FC/AFC/CF/AC/SC`）与尾部数字。
    - 增补别名（如 `como1907 -> como`、`internazionalemilano -> inter`）。

## 3. 实测结果
- `.env` 成功加载。
- `football-data` 成功拉取：`PL/PD/BL1/SA/FL1/ELC`。
- `BL2/FL2/SB` 返回 `HTTP 403`（套餐权限限制）。
- 映射结果由之前的 `7/14` 提升至 `13/14`：
  - `understat: 7`
  - `football-data: 6`
  - `unmapped: 1`（`国米 vs 科莫`）

## 4. The Odds API 结论
- 临时开启 `ARES_ENABLE_EXTERNAL_ODDS_ENRICH=1` 后，接口可正常返回赛事数据并落盘。
- 本次 `26064` 是历史期号，免费端点不含历史赔率；对应对阵对在当下 odds feed 中不存在，因此 `external_odds_history` 为 `0`。
- 结论：
  - **赛前当期**可用于免费赔率补采。
  - **历史回补**需付费历史端点，或保留当前“只采当期实时”策略。

## 5. 产出物检查
- `raw_reports/26064_dispatch_manifest.json` 新状态：
  - `mapping_source`: `{'understat': 7, 'football-data': 6, 'unmapped': 1}`
  - `football_data_match_id` 命中：`6`
- 冷存储新增：`26064_football_data_*.json`（以及启用赔率补采时的 `26064_the_odds_*.json`）。
