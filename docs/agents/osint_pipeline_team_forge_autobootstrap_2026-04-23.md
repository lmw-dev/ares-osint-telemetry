# osint pipeline 自动补 Team Archives 记录（2026-04-23）

## 1. 问题
- 之前 `team_forge.py --issue <issue>` 虽然已经能批量补档，但没有接进主流水线。
- 结果是每到新期次，仍然需要额外手动执行一次补档，否则 prematch/postmatch 可能因为球队 Markdown 档案缺失而中断。

## 2. 本次变更
- 文件：`src/data/osint_pipeline.py`
- 默认流程新增：
- `crawler/manifest -> team_forge 批量补档 -> audit_router -> prematch/postmatch`
- 新增参数：
- `--skip-team-forge`

## 3. 结果
- 正常跑 `python src/data/osint_pipeline.py --issue <issue>` 时，会自动按当期 manifest 补齐 `02_Team_Archives` 所需 Markdown 档案。
- 不再需要为每个新 issue 手动补一次球队底座。
