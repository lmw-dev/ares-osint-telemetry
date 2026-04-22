# osint_postmatch LLM 回填升级记录（2026-04-22）

## 1. 目标
- 在赛后回填阶段引入“规则兜底 + LLM 判官”双轨，提升偏差判断与熵值回填质量。

## 2. 变更文件
- `src/data/osint_postmatch.py`

## 3. 实现内容
- 新增 LLM 运行配置（环境变量）：
  - `ARES_USE_LLM_BACKFILL`：是否启用（默认关闭）
  - `ARES_LLM_API_KEY` / `OPENAI_API_KEY`
  - `ARES_LLM_BASE_URL`（默认 `https://api.openai.com/v1`）
  - `ARES_LLM_MODEL`（默认 `gpt-4o-mini`）
  - `ARES_LLM_TIMEOUT_SEC`（默认 20）
  - `ARES_LLM_MIN_CONFIDENCE`（默认 0.6）
  - `ARES_LLM_MAX_ENTROPY_DELTA`（默认 0.05）
- 新增双轨评估：
  - 先计算规则基线 `calculate_reality_gap`
  - 若 LLM 可用则调用 `chat/completions` 输出结构化 JSON
  - 做字段校验与限幅，再决定是否回填 `actual_tactical_entropy`
- 安全机制：
  - 低置信度不回填熵值
  - 熵值变化限幅
  - 熵值上下界钳制
  - LLM 失败自动回退规则，不中断主流程
- 新增审计落盘：
  - 每队每场写入 `Cold_Data_Lake/*_reality_gap_audit.json`
  - 记录规则基线、LLM原始输出、最终采用值与熵值变更决策

## 4. 验证
- `PYTHONPYCACHEPREFIX=/tmp/pycache ./venv/bin/python -m py_compile src/data/osint_postmatch.py`
- 在 `ARES_USE_LLM_BACKFILL=0` 下执行本地 mock 回填：
  - 球队档案正常更新
  - 审计文件成功写入 `Cold_Data_Lake`
