# osint_crawler Understat + FBref 双源回退记录（2026-04-22）

## 1. 目标
- 在既有 Understat 主映射基础上，增加 FBref 回退映射能力，覆盖英冠/次级联赛场景。

## 2. 代码改造
- 文件：`src/data/osint_crawler.py`
- 新增能力：
  - Understat 时间门禁继续保留（按时间最近 + 最大时间窗）
  - 新增 FBref 赛程抓取与候选匹配逻辑：
    - `_fetch_fbref_comp_matches`
    - `build_fbref_db`
    - `_pick_fbref_match_by_time`
  - manifest 新增字段：
    - `fbref_url`
    - `fbref_date`
    - `fbref_gap_days`
  - 映射优先级：
    - Understat 命中 -> 用 Understat
    - Understat 未命中 -> 尝试 FBref 回退
  - 回填修复触发条件优化：
    - Understat 缺少时间审计字段
    - Understat gap 异常过大
    - 既无 Understat 也无 FBref

## 3. 辅助改造
- 文件：`src/data/team_alias_map.json`
- 补充了本期映射缺失的中英文别名（佛罗伦萨、毕尔巴鄂、阿拉维斯、英冠多队全称等）。

## 4. 实测结果（Issue 26064）
- Understat 时间错配问题已修复（例：`22227` -> `29101`）。
- FBref 回退逻辑代码已接通，但当前运行环境访问 FBref 命中 Cloudflare 防护：
  - 返回 `403`
  - 页面为 `Just a moment... / Performing security verification`
- 结论：
  - 代码路径正确；
  - 当前网络/目标站反爬策略导致 FBref 回退在该环境暂不可用。
