# osint_postmatch Reality Gap 升级记录（2026-04-22）

## 1. 目标
- 在保留既有赛后冷/热数据产出的前提下，为 `osint_postmatch.py` 增加：
  - 球队档案物理面更新
  - 现实偏差（Reality Gap）自动计算并回写

## 2. 变更文件
- `src/data/osint_postmatch.py`

## 3. 核心实现
- 新增 `--league` 参数，支持精准定位：
  - `{ARES_VAULT_PATH}/02_Team_Archives/{league}/{team}.md`
- 若未传 `--league`：
  - 自动在 `02_Team_Archives/*/{team}.md` 下查找唯一命中
  - 多命中时报歧义错误，要求显式传联赛
- 对每支球队执行 YAML 回填：
  - 读取原档案 frontmatter + 正文
  - 更新 `physical_reality`：
    - `avg_xG_last_5`：基于 `xg_history_last_5` 做滑动平均
    - `variance_history`：仅当 `variance_flag=True` 时压入 `true`，最多 5 个
    - `conversion_efficiency`：按 `goals_for / xg_for` 更新
  - 计算 `reality_gap`（新增 `calculate_reality_gap`）：
    - `Fame_Trap`：`market_sentiment in {Optimistic, Overheated}` 且 `avg_xG_last_5 < 1.0` 且 `conversion_efficiency < 0.05`
      - `S_dynamic_modifier = +0.15`
    - `Underestimated`：`market_sentiment == Pessimistic` 且 `avg_xG_last_5 > 1.8` 且 `actual_tactical_entropy < 0.40`
      - `S_dynamic_modifier = -0.10`
    - 其他：`Aligned`，`modifier = 0.0`
  - 安全写回：临时文件替换，正文保持原样

## 4. 兼容策略
- 保留旧的 `latest_postmatch.json` + `postmatch_history.jsonl` 写入行为，避免影响现有消费链路。
- 批量模式下尝试从 `match.league / match.competition / manifest.league / --league` 获取联赛名。

## 5. 验证
- `PYTHONPYCACHEPREFIX=/tmp/pycache ./venv/bin/python -m py_compile src/data/osint_postmatch.py`
- 通过本地 mock `hot_data` 直接调用 `update_team_archives`，验证：
  - 两支球队档案被回写
  - `physical_reality.avg_xG_last_5` 与 `variance_history` 更新
  - `reality_gap` 根据规则更新
