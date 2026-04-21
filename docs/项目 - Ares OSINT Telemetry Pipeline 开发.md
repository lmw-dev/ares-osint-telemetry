---
tags:
  - project/ares-v4/osint-telemetry
  - data-pipeline
  - python
status: active
version: 2.1
creation_date: 2026-04-18
last_updated: 2026-04-21
related_area: "[[领域 - 量化系统与算法研发]]"
---

# 项目 - Ares OSINT Telemetry Pipeline 开发 v2.1

> [!tip] 系统定位：Ares v4.1 外部数据喂养节点
本项目的核心目标是构建自动化的赛后物理数据抓取管线，为 Ares 引擎提供复盘所需的“唯一事实来源 (SSOT)”。
**v2.1 升级核心**：在 v2.0 的基础上，落地“官方比分校验熔断 + 增强方差逻辑 + 全量冷数据落盘（赔率与赛后双端）”，并完成 Understat + FBref 双源回退框架。

## 1. 仓库命名建议 (GitHub Selection)
- **Primary**: `ares-osint-telemetry` (最推荐，体现遥测属性)
- **Secondary**: `ares-openclaw-pipeline` (延续 OpenClaw 爬虫代号)
- **Internal**: `ares-truth-extractor` (体现获取物理真相的使命)

## 2. 目录架构 (Lambda Architecture Structure)
采用冷热数据分离原则，冷数据屯集 JSON 用于未来回测，热数据提取核心 Alpha 因子进入知识库：
```text
/ares-osint-telemetry
├── src/
│   ├── data/
│   │   ├── osint_postmatch.py    # 核心执行脚本
│   │   └── team_alias_map.json   # 中英文队名映射字典
├── config/
│   └── settings.yaml             # API密钥及目标URL配置
├── raw_reports/                  # [冷数据] 保存原始全量 JSON 的矿坑
└── {ARES_VAULT_PATH}/
    └── 3_Resources/3.x_Match_Reports/   # [热数据] 供 Ares 引擎读取的 YAML 档案
````

## 3. 技术栈 (Tech Stack)

- **Language**: Python 3.10+
    
- **Crawler**: `requests` / `playwright` (处理动态 JS 渲染)
    
- **Parser**: `BeautifulSoup4` / `json`
    
- **Serialization**: `pyyaml` (生成标准 YAML Frontmatter)
    
- **Integration**: 直接对接 Obsidian 本地 Vault 路径
    

## 4. 数据 Schema 定义 (YAML)

所有抓取的赛后战报必须严格遵守以下格式，特别是 `validation_passed`、`data_source` 与 `passes_attacking_third` 字段：

YAML

```
---
version: 2.1
issue: "26063"
match_id: "MATCH_ID_STRING"
match_name: "Home vs Away"
data_source: "understat/fbref"
data_source_ref: "https://..."
result:
  score: "X-Y"
  winner: "home/away/draw"
  validation_passed: true  # 与 official_score 校验通过后才为 true
physical_metrics:
  home_xG: 0.00            # 预期进球：核心复盘指标
  away_xG: 0.00
  possession_home: 50
  possession_away: 50
  shots_on_target_home: 0
  shots_on_target_away: 0
  passes_attacking_third_home: 0 # 进攻三区成功传球 (识别物理屠杀)
  passes_attacking_third_away: 0
key_events:
  red_cards: []            # 格式: ["home_15", "away_88"]
  penalties: []
system_evaluation:
  variance_flag: boolean   # 自动计算：若高xG方未获胜（输/平）且差值>1.0，标记为 true
---
```

## 5. 执行流逻辑 (Execution Pipeline)

1. **获取参数**: 接收 `issue`、`match_id`，可选 `official_score`、`source`（`auto/understat/fbref`）。

2. **冷数据全量落盘 (Cold Dump)**:
   - 赛前赔率端：保存 `500` 原始 HTML 与全量 `data-*` 属性 JSON。
   - 赛后遥测端：保存 Understat/FBref 原始响应（HTML/JSON）与结构化冷数据。

3. **数据清洗与提纯 (Hot Extraction)**:
   - 使用 `team_alias_map.json` 标准化队名。
   - 提取 xG、控球率、射正、红牌、点球、以及进攻三区传球数。

4. **官方比分交叉校验 (Data Validation)**:
   - 若提供 `official_score`，抓取结果必须一致。
   - 若不一致，触发 `[ContaminationAlert]` 并中止热报告落盘。

5. **升级版方差计算 (Enhanced Variance Logic)**:
   - 若 `abs(home_xG - away_xG) > 1.0` 且高 xG 一方未赢（输/平），则 `variance_flag = true`。

6. **落盘存档**:
   - 热数据写入 `{ARES_VAULT_PATH}/3_Resources/3.x_Match_Reports/{issue}_{match_id}_postmatch.md`。
   - 每场独立文件，避免批量覆盖。
    

## 6. 开发指令 (Cursor Prompt)

**将以下指令粘贴至 Cursor 即可开始编写代码：**

> "你现在是 Ares v4.2 系统的高级开发工程师。请根据本项目 PRD 编写并维护 `osint_postmatch.py` 脚本。
> 
> 核心逻辑：
> 
> 1. 采用冷热分离：先将抓取到的全量原始响应（HTML/JSON）与结构化冷数据存入 `raw_reports/`。
>     
> 2. 支持 `Understat + FBref` 双源回退；优先 Understat，失败后自动切换 FBref。
>     
> 3. 实现增强版 `variance_flag`：若高 xG 一方未获胜（输球或平局）且差值 > 1.0，则标记为 True。
>     
> 4. 添加 `official_score` 校验模块：若抓取比分与官方比分不一致，抛出 `[ContaminationAlert]` 并终止热报告落盘。
>     
> 5. 使用 `pyyaml` 将热数据写入 YAML Frontmatter，并包含 `data_source`、`data_source_ref` 与 `validation_passed`。
>     
> 6. 请确保代码具备极高的健壮性和 type hinting。"
>     

## 7. 后续扩展计划 (V5.0 Roadmap)

- [ ] 接入 **Live Heatmap** (热力图分析) 判定进攻倾斜度。
    
- [ ] 增加 **Player Rating** (球员评分) 权重，评估核心节点的实际表现。
    
- [ ] 自动化同步 GitHub Action，实现 7x24 小时无人值守复盘。
    

---

## 知识连接 (Knowledge Connections)

- 上游架构：`[[体系 - Ares v4.1 量化推演引擎]]`
    
- 核心方法论：此项目落实了 `[[概念 - 从收藏家到炼金术士]]` 的理念，系统化提取具备复用价值的物理特征，而非简单囤积网页剪藏。
    
- 并行管线：`[[SOP - osint_crawler 赛前赔率抓取工作流]]`
