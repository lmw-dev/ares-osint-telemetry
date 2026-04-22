# Governance SOP v4.2 对齐更新记录（2026-04-22）

## 1. 更新对象
- Vault 文档：
  - `01_Governance/SOP - Ares Football 完整执行流程规范 v4.0.md`

## 2. 主要修订
- 将 SOP 从旧描述升级为 **v4.2**，并与当前代码实现对齐：
  - 引入一键入口：`osint_pipeline.py`
  - 明确 `AuditRouter` 自动路由职责
  - 修正冷/热数据真实落盘路径
  - 修正 postmatch 文件命名（`{issue}_{match_id}_postmatch.md`）
  - 增补重复 postmatch 去重归档规则
  - 增补异常熔断与降级策略
  - 删除已过时项（如 `--mode production`、`Prematch_Odds/{issue}_odds.md`）

## 3. 结果
- SOP 现在与 `src/data/osint_pipeline.py + osint_crawler.py + osint_postmatch.py + audit_router.py` 行为一致。
- 目录治理、自动化执行和人工干预边界更清晰，便于稳定运营。
