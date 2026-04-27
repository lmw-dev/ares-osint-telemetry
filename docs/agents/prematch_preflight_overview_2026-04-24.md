# prematch 预检总揽脚本记录（2026-04-24）

## 1. 目的
- 把 issue 级 prematch 输入质量检查从主流程里拆出来，变成单独的预处理脚本。
- 让 agent 或人工先看总揽报告，再决定：
- 直接进入 `osint_pipeline.py`
- 还是先补 Team Archives / 赛程映射锚点

## 2. 新增脚本
- 文件：`src/data/prematch_preflight.py`
- 用法：
```bash
python src/data/prematch_preflight.py --issue <issue>
```

## 3. 产出物
- Vault 路径：
- `03_Match_Audits/{issue}/Audit-{issue}.md`

报告内容包括：
- 本期比赛总数与对阵清单
- `mapping_source` 汇总
- `unmapped` 场次
- RAG readiness 摘要
- Team Archive 四态：`usable / placeholder / placeholder_backfilled / missing`
- `thin_rag_docs` 球队
- 需要补强球队清单（直接列出 gap / archive path）
- issue 级建议动作：`READY / CAUTION / HOLD / BLOCKED`

附带产物：
- `03_Match_Audits/{issue}/Audit-{issue}-team-diagnostics.json`
- `03_Match_Audits/{issue}/03_Review_Reports/TEAM-INTEL-{issue}.generated.json`
- `03_Match_Audits/{issue}/03_Review_Reports/UNMAPPED-ANCHORS-{issue}.generated.json`

## 4. 设计原则
- 不混入 `osint_pipeline.py`
- 不直接触发 prematch 主流程
- 不替 agent 做最终业务判断，但把关键输入质量问题一次性摊开

## 5. 当前意义
- 之前“单场正常、全量质量差”的问题，根因常常不是 router，而是：
- `unmapped fixture`
- `placeholder / placeholder_backfilled` 低质量队档
- RAG metadata 覆盖看似通过，但每队只有极薄的样本
- `placeholder_backfilled` 仅表示模板完成结构化回填，不代表可直接用于 prematch 决策
- 现在先跑 `prematch_preflight.py`，会比直接跑全量 pipeline 更容易判断该补哪里。
- 预检会顺手生成 issue 级 intel skeleton，便于 `team_archive_backfill.py` 接着消费，不需要再手工从 Markdown 抄名单。
- 预检也会生成 unmapped 锚点 skeleton，可直接补 `understat_id / fbref_url / football_data_match_id` 后回注到 crawler。
