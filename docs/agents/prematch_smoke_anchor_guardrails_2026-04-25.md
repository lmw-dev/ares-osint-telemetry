# prematch smoke anchor guardrails (2026-04-25)

## 变更范围
- `src/data/unmapped_anchor_seed.py`
- `src/data/osint_crawler.py`
- `src/data/prematch_preflight.py`
- `src/data/prematch_regression.py`
- `README.md`

## 目标
1. 明确区分 smoke / production 锚点模式，避免测试锚点误入生产。
2. 在 `Audit-{issue}.md` 显式标注 smoke 锚点场次。
3. 提供可脚本化的一键回归链路：`anchor seed -> crawler -> preflight`。

## 关键实现
- `unmapped_anchor_seed.py`
  - 新增 `--mode smoke|production`（默认 `production`）。
  - `smoke` 模式必须显式 `--allow-smoke`。
  - `production` 模式必须传真实锚点字段（`--understat-id` / `--fbref-url` / `--football-data-match-id`）并指定 `--indices`。
  - `--clear-smoke` 基于 `anchor_mode` / notes / `anchor.local` 识别并清理。

- `osint_crawler.py`
  - 应用手工锚点后，在 manifest 写入：
    - `manual_anchor_applied`
    - `manual_anchor_mode`
    - `manual_anchor_notes`
    - `manual_anchor_source`
  - 新增历史残留防护：若本轮未应用手工锚点但命中 `anchor.local`，自动清空旧 smoke 锚点，防止“clear 后仍被旧 manifest 回填”。

- `prematch_preflight.py`
  - 增加 `smoke_anchor_fixture` 风险信号。
  - 汇总新增 `smoke_anchor_matches`。
  - `READY + smoke` 自动降级为 `CAUTION`。
  - `Audit` 增加 smoke 统计行、比赛看板 `manual=smoke` 标记、Next Actions 的清理提示。

- `prematch_regression.py`
  - 新增一键回归入口：
    - `seed -> crawler -> preflight`
    - 支持 `--clear-smoke`
  - 回归完成后输出关键统计：`unmapped/smoke_anchor_matches/usable_team_archives/thin_rag_docs`。

## 26066 验证
- smoke 回归后：`unmapped=0`, `smoke_anchor_matches=10`, `usable=28`, `thin_rag_docs=0`, `preflight_status=CAUTION`
- clear-smoke 回归后：`unmapped=10`, `smoke_anchor_matches=0`, `usable=28`, `thin_rag_docs=0`, `preflight_status=HOLD`

