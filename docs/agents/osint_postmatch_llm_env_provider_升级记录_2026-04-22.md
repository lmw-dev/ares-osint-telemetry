# osint_postmatch LLM `.env` Provider 化升级记录（2026-04-22）

## 1. 目标
- 统一通过 `.env` 管理 LLM Key。
- 增加 Gemini Provider 支持。
- `base_url` 变为可选：不填自动回退官方地址。

## 2. 变更文件
- `src/data/osint_postmatch.py`
- `README.md`
- `.env.example`

## 3. 核心变更
- 新增 `ARES_LLM_PROVIDER`：支持 `openai | gemini`（默认 `openai`）。
- API Key 选择逻辑：
  - 统一优先：`ARES_LLM_API_KEY`
  - OpenAI 回退：`OPENAI_API_KEY`
  - Gemini 回退：`GEMINI_API_KEY`（兼容 `GOOGLE_API_KEY`）
- Base URL 选择逻辑：
  - 优先读取：`ARES_LLM_BASE_URL`
  - 兼容别名：`ARES_LLM_BAE_URL`
  - 未配置时按 Provider 使用官方默认：
    - OpenAI: `https://api.openai.com/v1`
    - Gemini: `https://generativelanguage.googleapis.com/v1beta`
- LLM 调用分流：
  - OpenAI：`/chat/completions`
  - Gemini：`/models/{model}:generateContent`
- 审计增强：`reality_gap_audit.json` 增加 `llm_provider` 与 `llm_model` 字段。

## 4. 兼容性
- 保留旧变量可用（`ARES_LLM_API_KEY`, `OPENAI_API_KEY`, `ARES_LLM_BASE_URL`）。
- 未启用 LLM 或 Key 缺失时，继续规则兜底，不影响主流程。

## 5. 验证
- `PYTHONPYCACHEPREFIX=/tmp/pycache ./venv/bin/python -m py_compile src/data/osint_postmatch.py`
