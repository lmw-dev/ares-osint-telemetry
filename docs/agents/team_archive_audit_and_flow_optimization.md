# 球队底座档案安全防线与 Prematch 流程优化审计报告

本交接文档旨在总结对 Ares 系统**球队底座档案**（`02_Team_Archives`）的机制审计，论证其在当前 Prematch 流程下的执行必要性，并明确**非破坏性 Soft Update 合并防线**，以确保已有高价值手工精修档案 100% 不被简单覆盖。

---

## 1. 核心结论与执行建议

### 1.1 赛前是否仍需要提前执行档案脚本？
**结论：依然高度需要提前执行，但其定位从“重建”优化为“增量更新与数据预检（Incremental Backfill & Quality Preflight）”。**

*   **原因 1：打通 Prematch Preflight 预检关卡**
    `prematch_preflight.py` 在赛前会扫描主客队的档案文件。如果底座档案缺失（`missing`）、全为初始占位（`placeholder`）或内容过期（`stale_archive` 超过 21 天），预检会直接抛出 `Blocking Gaps`（阻断性数据缺口），并将比赛日评级强行拉低至 `HOLD` 或 `CAUTION`。
*   **原因 2：同步最新动态事实（伤停与 xG）**
    每期比赛日 OSINT 会搜集最新的伤停与 physical 指标。必须先执行 `team_archive_backfill.py`，把最新的 `absences`、`injured_nodes` 和近期 `avg_xG_last_5` 动态写入底座档案，后续 ChatGPT 作为推演中枢时，读入的 `home_card`/`away_card` 才是最新、最真实的情报，否则推演结论就会产生“信息延迟”带来的致命幻觉。
*   **原因 3：对齐 SOP v1.0 材料包规范**
    SOP 要求材料包包含 `home_card` 和 `away_card`。提前执行 backfill，能确保在制作材料包（`football-prematch-material-pack` 技能运行）并将其拷贝至 `03_Match_Audits` 目录时，拷贝的是已经注入了最新事实的完整档案。

---

## 2. 球队底座档案的“防覆写安全机制”深度审计

针对“已有档案内容已经很多，不能简单覆盖”的隐患，我们在代码层层面对 `team_forge.py` 和 `team_archive_backfill.py` 进行了多重防线的深度审计。以下是系统已具备的**安全隔离机制**：

### 2.1 递归级 Soft Update 合并策略 (`merge_frontmatter_defaults`)
在 `team_forge.py` 中，初始化或升级字段结构时使用 `merge_frontmatter_defaults`：
```python
def merge_frontmatter_defaults(existing: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(existing or {})
    for key, default_value in defaults.items():
        existing_value = merged.get(key)
        if isinstance(default_value, dict):
            child_existing = existing_value if isinstance(existing_value, dict) else {}
            merged[key] = merge_frontmatter_defaults(child_existing, default_value)
            continue
        if key not in merged:
            merged[key] = default_value
    return merged
```
*   **安全防线说明**：该函数遵循**“增量追加，已有保护”**的铁律。仅当现有 Frontmatter 中完全缺失某个 V4.2 字段（例如新引入的 `resilience_core`）时，才会用 Default 预设值填补。若用户已经手工精修了该字段（如 `manager_doctrine: "High-pressing and quick counter-attack"`），**现有的高价值内容会 100% 得到保留，绝对不会被重置为 `Unknown` 或默认值**。

### 2.2 Body 正文 100% 无损留存 (`read_existing_content`)
球队档案 Markdown 最珍贵的部分通常是写在 YAML Frontmatter 之外的 Body 正文（包含 `## Team Notes`、人工手写的战术演变、YouTube 视频纪要等）。
*   **安全防线说明**：`read_existing_content` 会严格分离 YAML 头部和 Markdown Body。在重写时，只会重组 Frontmatter（融入增量数据），然后将读取到的 **Body 内容原封不动、无损拼回**：
    ```python
    frontmatter, body = read_existing_content(target_path)
    # ... 进行增量 Frontmatter 合并 ...
    markdown_content = build_markdown(merged_frontmatter, body) # 拼接原有 body
    write_markdown_safely(target_path, markdown_content)
    ```

### 2.3 异常事实的动态硬过滤 (`team_archive_backfill.py`)
为防止低质量或幻觉情报污染已有档案，回填工具采用了基于事实可信度分级的回填策略：
*   **别名与状态过滤**：伤停列表中若检测到带有 `"transferred"`, `"retired"`, `"loaned out"`（转会、退役、出租）等字眼的球员，会自动移出 `injured_nodes` 关键盯防节点，防止已流失/历史信息污染底座。
*   **置信度分级隔离**：只有事实门禁 `fact_gate_status` 为 `PASS` 或 `SUPPORTED` 的数据才会正式参与物理面影响评估，而 `NEEDS_LATEST_CONFIRMATION`（过期或存疑事实）只作为提示信息保留。

---

## 3. 材料包 Skill 中的“只读拷贝（Read-Only Copy）”设计

为打通最后一公里并保证 `02_Team_Archives` 的绝对安全，我们正设计实现的 `football-prematch-material-pack` 技能在搬运档案到比赛日材料包目录时，必须遵循**只读拷贝**原则：

*   **拷贝不写回**：在材料包生成过程中，只根据主客队名进行模糊匹配，在目标材料包目录（如 `AresMatchday_YYYYMMDD`）下写为 `01_home.md` 和 `01_away.md`。**在拷贝过程中决不对 `02_Team_Archives` 目录进行写操作**。
*   **Gap 提醒而非暴力补全**：若材料包检测到底座档案依然存在 `archive_quality = placeholder` 等薄弱问题，将只在主索引表 `00_match_list.csv` 中标记为 `NEEDS_REVIEW` 或输出 Warning 日志，提醒人工或前置 Backfill 脚本处理，而不实施自动强制初始化。

### 3.1 与异常信息 Skill (Fact Gate 2.1) 的闭环防线联动

新优化的**异常信息 Skill** 具有强大的 **Fact Gate 事实校验** 机制（输出 `abnormal.json` 和 `abnormal.md`）。它能够把捕获到的 absences（伤停事实）严格分级为 `PASS` / `SUPPORTED`（高可信度）和 `NEEDS_LATEST_CONFIRMATION`（待确认/存疑）。该 Skill 在流程中的联动方式如下：

1.  **作为底座回填的“可信过滤器”**：
    在执行 `team_archive_backfill.py` 时，**优先读取 `abnormal.json` 里的事实门禁数据**。只有 `fact_gate_status` 评级为 `PASS` 和 `SUPPORTED` 的 absences 伤停事实，才被允许写入 `02_Team_Archives` 长期底座档案。**对 `FAIL` 级或高幻觉率的噪声事实进行强行隔离丢弃**，确保底座绝对纯净。
2.  **作为材料包 `xx_audit.md` 模版的“动态现场增量”**：
    在 `football-prematch-material-pack` 技能生成比赛日材料包时，它会动态扫描当期比赛的 `abnormal.json`，把经过校验的伤停信息、受影响单位（`affected_units`）和置信度标签，实时格式化地回填到 `01_audit.md`（推演卡）的 `team_abnormalities` 段落中。
3.  **双层防线闭环**：
    *   **底座档案**：记录球队的**“常态物理底稿与长期战术矩阵”**。
    *   **异常信息 Skill**：记录**“本场最新的临场变量（如突发流感、赛前新闻发布会的伤退等）”**。
    在材料包生成时，这两股力量在 `03_Match_Audits` 目标沙箱内完美汇流，供 ChatGPT 在推演中枢中一次性读取，完成高置信度推演。

---

## 4. 后续建议与监控策略

1.  **定期执行 `prematch_preflight.py` 诊断**
    在周末比赛日 OSINT 收集完成后，优先运行一次 preflight，它会输出一个 `Audit-{issue}-team-diagnostics.json`，一目了然地列出所有待补强（`needs_enrichment`）的球队，方便人工精准补料，而不必通盘扫描。
2.  **Git 备份机制**
    底座档案全部由 Git 跟踪。在每次执行 `team_archive_backfill.py` 之前，建议确保本地工作区干净，回填后可通过 `git diff 02_Team_Archives/` 实时审计到底补齐了哪些数据，若有异常可一键 `git checkout` 撤销，提供物理层面的容灾保障。

---

## 5. 未来前沿重构设想：OSINT 爬虫解耦与 xG 智能物理面 Skill 引入

为了将 Ares 的数据生产力提升至全新维度，针对当前“爬虫与分析耦合”以及“物理数据死板”的痛点，我们提出以下两项关键重构方向：

### 5.1 OSINT 爬虫的“纯粹解耦”与去重优化
*   **痛点**：目前的 OSINT 爬虫可能在抓取数据的同时混杂了简单的提取或拼凑逻辑，导致与赔率/异常 Skill 在业务分析、AI 提炼上存在重复计算与成本浪费。
*   **重构方案（Retriever-Reader 两段式解耦）**：
    *   **OSINT 爬虫（Retriever）**：彻底退化为**“高速、纯净的数据管道（Pure Data Pipe）”**。它只负责在赛前高可用地把 Raw Odds JSON、Raw Lineup/Injury HTML、Raw Match stats 抓取并安全落盘至冷数据湖（Cold Data Lake），**不塞入任何 prompt，不作任何赔率分析，完全用纯 Python 物理代码运行**，保证吞吐量与极低的出错率。
    *   **业务 Skill（Reader）**：作为内容提取引擎。赔率 Skill 和异常 Skill **绝对不发起网络爬取**，它们只以静态文件的形式消费爬虫落盘的 Raw 数据，通过 AI 提炼和公式计算将数据结构化。这样既节省了 LLM Token 成本，又避免了数据版本在抓取和分析阶段产生冲突。

### 5.2 引入 `football-physical-profile`（物理面画像智能提取 Skill）
*   **痛点**：完全通过物理代码去计算最近 5 场的平均 xG、防守泄漏和终结效率存在致命硬伤：
    *   **红牌噪点**：某场比赛早早少打一人被狂屠，均值会异常暴跌，但这不能代表真实物理实力。
    *   **轮换失真**：联赛夺冠后的垃圾时间练兵，或者杯赛全替补出战，均值无法反映主力水准。
    *   **缺失值污染**：数据源缺失某场比赛时，代码只能丢弃或填补默认值 `1.0`/`0.5`，从而阻断预检或污染底座。
*   **重构方案（大模型作为智能修剪与拟合器）**：
    将 xG、xGA 的计算逻辑升级为一个专门的 **Physical Profile Skill**：
    *   **智能修剪（Noise Pruning）**：通过 LLM 审视最近 5 场比赛的背景上下文（是否有红牌、是否是大轮换、是否有极端天气），自动将这些“物理噪点”在均值计算中进行平滑处理。
    *   **动态趋势提取（Trend Modeling）**：智能分析在主力中锋伤缺后，xG 下滑的斜率和防守抗压强度，给出具有前瞻性的物理趋势，而不仅仅是历史均值。
    *   **智能归一化补全**：若某场比赛 xG 数据缺失，LLM 能结合 RAG 中的新闻表现与球队实力锚点，给出一个高置信度的“智能拟合值”，彻底消灭默认值污染造成的预检阻断。
