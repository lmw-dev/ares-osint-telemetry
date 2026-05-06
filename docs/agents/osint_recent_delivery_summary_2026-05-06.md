# OSINT 近期完成内容总览（2026-05-06）

## 1. 范围与目标
- 项目：`21-ares-osint-telemetry`
- 近期主线：
  - 打通 prematch 输入门禁与 Team Archive 收敛
  - 将 `unmapped / thin_rag_docs / team archive` 三条链路脚本化回归
  - 在不大改主干前提下，增强可执行性（SINGLE/COMBO/PASS）
  - 增补国内源（Titan/Nowscore）的伤停与上一场阵容入库

## 2. 已完成能力（代码层）

### 2.1 Prematch 预检与收敛
- `src/data/prematch_preflight.py`
  - 支持 `archive_quality` 四态：`usable / placeholder / placeholder_backfilled / missing`
  - 输出物统一：
    - `Audit-{issue}.md`
    - `Audit-{issue}-team-diagnostics.json`
    - `TEAM-INTEL-{issue}.generated.json`
    - `UNMAPPED-ANCHORS-{issue}.generated.json`
  - 审计文案补充“需要补强球队”与 Next Actions 修正

### 2.2 Team Archive 回填
- `src/data/team_archive_backfill.py`
  - 可消费 preflight 诊断
  - intel 读取优先级：
    - `TEAM-INTEL-{issue}.json`
    - fallback `TEAM-INTEL-{issue}.generated.json`
  - 修复：
    - “无实质情报误判 usable”
    - “skipped_usable 仍被写盘”副作用
  - 新增回填字段：
    - `last_match_lineup_snapshot`
    - `lineup_rotation_signals`
    - `lineup_snapshot_status`
  - 同步进 manifest `match_context_flags`：
    - `rotation_intensity`
    - `lineup_snapshot_status`

### 2.3 Crawler / Pipeline
- `src/data/osint_pipeline.py`
  - `sync_issue_team_archives_to_rag` 改为“全文 + 分段”多文档入库
  - 新增 `--sync-team-rag-only`
  - `thin_rag_docs` 已验证可从 28 -> 0
- `src/data/osint_crawler.py`
  - 支持手工锚点回注：优先读取 `UNMAPPED-ANCHORS-{issue}.json`
  - DATE 模式已增强，支持按日期+scope 构建当期（如 `DATE-20260504-top5`）

### 2.4 Unmapped Anchor 回归
- `src/data/unmapped_anchor_seed.py`
  - 已具备 smoke 锚点注入回归能力
  - 与 preflight/crawler 链路形成可脚本化验证

### 2.5 Prematch 决策可执行性（P0）
- `src/data/prematch_synthesis.py`
  - 动态票池结构：`SINGLE / COMBO / PASS`
  - 引入 `single_pick_dynamic_budget`（允许为 0）
  - 输出 `blocked_by_gates`、`single_pick_score`、`decision_type`
  - 加入轮换高风险门禁：`GATE_HIGH_ROTATION_RISK`
  - 当 `rotation_intensity=HIGH` 时禁单挑（进入 COMBO/PASS）

## 3. P0.3（Titan/Nowscore 伤停+阵容）新增

### 3.1 新脚本
- `src/data/injury_lineup_intel_collect.py`
  - 自动采集 TeamArchive 基线 + Nowscore/Titan + 可选 Transfermarkt
  - Titan 解析采用 `id=013` 区块（阵容情况）
  - 解析内容：
    - `absences`（球员+缺阵原因）
    - `last_match_lineup_snapshot`（上一场首发/替补）
    - `lineup_rotation_signals`（LOW/MEDIUM/HIGH/UNKNOWN）
    - `lineup_snapshot_status`（`LIVE_OK/LIVE_BLOCKED/SEEDED/UNKNOWN`）
  - 新增离线注入参数：
    - `--titan-html-dir`（`<cn_match_id>.html`）

### 3.2 解析稳健性修复
- 使用 `utf-8-sig` 解码 Titan 页面
- 过滤“球员上一场出场评分”噪音行，避免污染首发名单

## 4. 关键回归结果

### 4.1 Issue 26066（收敛验证）
- 预期状态已达成：
  - `unmapped=0`（依赖锚点补齐）
  - `usable=28`
  - `需要补强球队=0`
  - `thin_rag_docs=0`

### 4.2 DATE-20260504-top5（P0.3 链路验证）
- `osint_crawler --date 20260504 --scope top5 --date-source understat`
  - 产出：`DATE-20260504-top5_dispatch_manifest.json`，`matches=5`
- `injury_lineup_intel_collect --issue DATE-20260504-top5 --merge`
  - 产出：`TEAM-INTEL-DATE-20260504-top5.json`，`teams=10`
- `team_archive_backfill --issue DATE-20260504-top5 ...`
  - 回填：`enriched=10`
- 验证结论：
  - `lineup_snapshot_status` 分布：`LIVE_OK=10`
  - `rotation_intensity` 已进入 manifest，并可被 synthesis 门禁消费

## 5. 当前已知边界
- 国内站存在间歇性反爬/WAF，可能导致在线采集波动
- 已提供两类兜底：
  - `lineup_snapshot_status` 显式标注数据可用性
  - `--titan-html-dir` 离线注入保证回归可复现

## 6. 推荐执行顺序（当前版本）
1. `osint_crawler.py`（issue 或 date 模式）
2. `injury_lineup_intel_collect.py --merge`（必要时加 `--titan-html-dir`）
3. `team_archive_backfill.py --issue ... --intel-file ...`
4. `prematch_preflight.py --issue ...`
5. `prematch_synthesis.py --issue ...`
6. （赛后）`osint_postmatch.py` -> `postmatch_synthesis.py`

## 7. 交接提示
- 当前主干原则：不做大重构，优先“门禁可解释 + 决策可执行 + 回归可复现”。
- 若出现“全 PASS/全 COMBO”，先查：
  - `blocked_by_gates`
  - `rotation_intensity`
  - `lineup_snapshot_status`
  - `key_node_absence_risk`
