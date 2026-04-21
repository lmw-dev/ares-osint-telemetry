# Ares OSINT Telemetry Pipeline v2.1 实施记录（2026-04-21）

## 1. 本次目标
- 对齐 PRD：落地官方比分校验熔断、增强方差逻辑、全量冷数据落盘。
- 保持当前可运行链路：`Understat + FBref` 双源回退不被破坏。

## 2. 变更文件
- `docs/项目 - Ares OSINT Telemetry Pipeline 开发.md`
- `src/data/osint_postmatch.py`
- `src/data/osint_crawler.py`
- `README.md`

## 3. 核心改造

### 3.1 官方比分校验熔断（Postmatch）
- 新增参数：
  - `--official-score`（格式 `2-1`）
- 逻辑：
  - 若提供 `official_score`，抓取比分不一致时抛出 `[ContaminationAlert]` 并中止热报告落盘。
  - 一致时写入 `result.validation_passed: true`。
  - 未提供时默认跳过校验并标记通过（日志会显示“跳过官方比分校验”）。

### 3.2 增强方差逻辑（修复平局盲区）
- 规则升级为：
  - 若 `abs(home_xG - away_xG) > 1.0` 且高 xG 方未赢（输球或平局），`variance_flag = true`。

### 3.3 全量冷数据落盘（赛前赔率 + 赛后遥测）
- 赛前 `osint_crawler.py`：
  - 新增 `raw_reports/{issue}_500_raw.html`
  - 新增 `raw_reports/{issue}_500_raw.json`（保存每场 `tr` 的全量 `data-*` 字段）
  - `dispatch_manifest.json` 增加 `cold_data_refs`
- 赛后 `osint_postmatch.py`：
  - Understat 新增
    - `raw_reports/{issue}_{match_id}_understat_raw.html`
    - `raw_reports/{issue}_{match_id}_understat_match_info_raw.json`
  - FBref 新增
    - `raw_reports/{issue}_{match_id}_fbref_raw.html`（若抓取成功）
  - 结构化冷数据 `raw_reports/{issue}_{match_id}.json` 中加入 `raw_artifacts` 引用

## 4. 文档同步
- `项目文档` 升级到 `v2.1`，并更新：
  - YAML schema（`version: 2.1`、`validation_passed`、`data_source*`）
  - 执行流（双源、校验熔断、全量冷存、单场独立热报告文件命名）
- `README` 增加 `--official-score` 示例，并更新冷数据说明。

## 5. 验证记录
- 语法检查通过：
  - `python3 -m py_compile src/data/osint_crawler.py src/data/osint_postmatch.py`
- 赛前全量冷存验证：
  - `./venv/bin/python src/data/osint_crawler.py --issue 26063`
  - 生成 `26063_500_raw.html/json`，且 manifest 存在 `cold_data_refs`
- 赛后校验通过路径：
  - `./venv/bin/python src/data/osint_postmatch.py --issue 26063 --match-id 23191 --source understat --official-score 3-0`
- 赛后污染熔断路径：
  - `./venv/bin/python src/data/osint_postmatch.py --issue 26063 --match-id 23191 --source understat --official-score 0-3`
  - 触发 `[ContaminationAlert]` 并退出

## 6. 已知限制
- FBref 在当前网络环境可能触发 Cloudflare（403 / “Just a moment...”）。
- 当前批量模式的官方比分校验依赖 manifest 中存在 `official_score` 或 `result_score` 字段；若缺失则跳过校验。

## 7. 建议下一步
- 在 `osint_crawler` 增加官方赛果抓取与落盘，使 `postmatch --issue` 可默认启用校验。
- 为 `FBref` 回退引入可控代理层或采集窗口策略，降低 Cloudflare 触发率。
