# osint_crawler 外部赔率检索逻辑修正（2026-04-22）

## 1. 问题
- 传足 `issue` 不是全球统一比赛主键。
- 外部赔率补采不应基于 `issue` 推断，而应基于“映射出的真实赛程信息”。

## 2. 修正
- 文件：`src/data/osint_crawler.py`
- The Odds API 检索改为：
  - 使用 `mapped_match_time`（Understat / FBref / football-data 映射出的比赛时间）
  - + 主客队名归一化匹配
  - issue 仅作为本地派发批次标识，不参与外部赔率检索
- 新增状态审计：
  - `status=ok`：成功拿到 h2h 盘口
  - `status=no_match_in_feed`
  - `status=event_found_but_no_h2h_market`
  - `status=skipped_historical_on_free_plan`（免费层历史赔率不可回补）

## 3. 兼容性
- 不影响主映射链路（Understat -> FBref -> football-data）。
- 赔率补采仍由 `ARES_ENABLE_EXTERNAL_ODDS_ENRICH` 控制。

## 4. 说明
- 该修正保证“检索键正确性”：不再把中国本地期号当成海外赔率索引依据。
