# osint 一键串联与审计路由升级记录（2026-04-22）

## 1. 目标
- 让流程尽量自动化，减少人工搬运与手动归档。
- 打通 `crawler -> postmatch` 并自动维护 `03_Match_Audits` 目录规范。

## 2. 变更文件
- `src/data/audit_router.py`（新增）
- `src/data/osint_pipeline.py`（新增）
- `src/data/osint_crawler.py`
- `src/data/osint_postmatch.py`
- `README.md`

## 3. 核心能力
### 3.1 Audit Router（新）
- 自动创建并维护：
  - `03_Match_Audits/00_Governance`
  - `03_Match_Audits/{issue}/01_Prematch_Audits`
  - `03_Match_Audits/{issue}/02_Special_Analyses`
  - `03_Match_Audits/{issue}/03_Review_Reports`
  - `03_Match_Audits/{issue}/04_Postmatch_Legacy`
  - `03_Match_Audits/99_Legacy_Archive`
- 自动生成 prematch 骨架文档（按 dispatch_manifest 的 14 场）
- 自动归档重复 `*_postmatch.md`（与 `Postmatch_Telemetry` 同名）到：
  - `99_Legacy_Archive/Duplicate_Postmatch/{issue}/`
- 自动更新：
  - `03_Match_Audits/00_Governance/INDEX - 审计文档导航.md`
  - `03_Match_Audits/{issue}/README.md`

### 3.2 Crawler 路径自动对齐
- 若配置 `ARES_VAULT_PATH`，crawler 冷数据直接落盘：
  - `{ARES_VAULT_PATH}/04_RAG_Raw_Data/Cold_Data_Lake/`
- 不再默认只写项目 `raw_reports/`（未配置 vault 时才回退）。

### 3.3 Postmatch 自动收尾
- `osint_postmatch.py` 在单场/批量结束后自动触发 Audit Router 收尾，更新 issue 导航与去重。

### 3.4 一键入口（新）
- `python src/data/osint_pipeline.py --issue <issue>`
- 默认串联：`crawler -> 路由 -> postmatch 批量 -> 路由收尾`
- 支持：
  - `--skip-crawler`
  - `--skip-postmatch`
  - `--no-prematch-stubs`

## 4. 兼容性
- 旧命令仍可用：`osint_crawler.py` / `osint_postmatch.py`。
- 新增的自动路由不会阻断主流程：路由失败仅 warning。

## 5. 验证
- `PYTHONPYCACHEPREFIX=/tmp/pycache ./venv/bin/python -m py_compile src/data/audit_router.py src/data/osint_crawler.py src/data/osint_postmatch.py src/data/osint_pipeline.py`
