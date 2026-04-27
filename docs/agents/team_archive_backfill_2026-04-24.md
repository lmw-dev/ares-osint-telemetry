# team archive placeholder 批量回填记录（2026-04-24）

## 1. 背景
- `prematch_preflight.py` 已经能识别 issue 中哪些球队档案仍是 placeholder。
- 但此前项目里缺少一个按 issue 批量处理 placeholder Team Archives 的独立脚本。
- 结果是：
- 用户容易在 `crawler / preflight / pipeline / intel_sweeper` 之间找错脚本。
- placeholder 队档只能靠 `team_forge.py` 建壳，或手工逐队补内容。

## 2. 本次新增
- 文件：`src/data/team_archive_backfill.py`
- 用法：
```bash
python src/data/team_archive_backfill.py --issue <issue>
```

## 3. 当前行为
- 读取本期 `dispatch_manifest.json`
- 扫描本期涉及的球队档案
- 对仍是 placeholder 的档案：
- 保留 frontmatter 基础字段
- 写入统一的可维护模板正文
- 标记 `archive_quality: placeholder_backfilled`
- 记录回填上下文（issue / 时间 / script）

- 如提供 `--intel-file`，或 issue 目录下存在 `TEAM-INTEL-{issue}.json`：
- 批量读取每队的结构化情报
- 回填 `intel_base / tactical_logic / physical_reality / reality_gap`
- 重新渲染正文中的 prematch 关注项与数据缺口
- 满足最小实质内容的队档会直接提升为 `archive_quality: usable`

- 对已有实质内容的球队档案：
- 不覆盖
- 仅标记为 `skipped_usable`

## 4. 产出物
- 更新：
- `02_Team_Archives/.../*.md`
- 新增回填报告：
- `03_Match_Audits/{issue}/03_Review_Reports/REVIEW-{issue}-Team_Archive_Backfill.md`
- 可选情报输入：
- `03_Match_Audits/{issue}/03_Review_Reports/TEAM-INTEL-{issue}.json`
- 预检自动生成骨架：
- `03_Match_Audits/{issue}/03_Review_Reports/TEAM-INTEL-{issue}.generated.json`

## 5. 边界
- 该脚本不会伪造战术结论。
- 该脚本不会自动抓新闻。
- 它的目标是把“空壳”升级为统一、清晰、便于后续补录的模板。
- `archive_quality: placeholder_backfilled` 的含义是“结构已补齐，但内容仍低质量占位”，后续仍需补新闻、战术上下文、物理指标。
- 若未显式传 `--intel-file`，脚本会优先读取手工版 `TEAM-INTEL-{issue}.json`，否则回退读取预检生成的 `TEAM-INTEL-{issue}.generated.json`。

## 6. 推荐顺序
1. `osint_crawler.py --issue <issue>`
2. `prematch_preflight.py --issue <issue>`
3. 如提示 placeholder 过多，先查看预检生成的 `TEAM-INTEL-{issue}.generated.json`，必要时复制或整理成 `TEAM-INTEL-{issue}.json`
4. 执行 `team_archive_backfill.py --issue <issue> --intel-file <path>`
5. 对仍缺新闻源的个别球队，再用 `intel_sweeper.py` 补单队文本/链接
6. 对 `placeholder_backfilled` 队档继续补实质内容后，再进入 `osint_pipeline.py`
