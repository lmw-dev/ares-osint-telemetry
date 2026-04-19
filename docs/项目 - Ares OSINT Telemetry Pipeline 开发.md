---
tags:
  - project/ares-v4/osint-telemetry
  - data-pipeline
  - python
status: active
creation_date: 2026-04-18
related_area: "[[领域 - 量化系统与算法研发]]"
---

# 项目 - Ares OSINT Telemetry Pipeline 开发

> [!tip] 系统定位：Ares v4.1 外部数据喂养节点
本项目的核心目标是构建自动化的赛后物理数据抓取管线，用于获取比赛真实的 xG (预期进球)、控球率、红黄牌等底层指标。通过将物理事实落盘为 YAML 档案，为 Ares 引擎提供复盘所需的“唯一事实来源 (SSOT)”，彻底消除模型复盘时的“幻觉”（即避免通过比分反推过程）。该项目体现了“从收藏家到炼金术士”的输出导向思维，确保收集的数据能够直接用于模型纠偏与价值提炼。

## 1. 仓库命名建议 (GitHub Selection)
- **Primary**: `ares-osint-telemetry` (最推荐，体现遥测属性)
- **Secondary**: `ares-openclaw-pipeline` (延续 OpenClaw 爬虫代号)
- **Internal**: `ares-truth-extractor` (体现获取物理真相的使命)

## 2. 目录架构 (Directory Structure)
为保持系统文件结构的统一，数据落盘路径已对齐知识库的标准 P.A.R.A. 框架：
```text
/ares-osint-telemetry
├── src/
│   ├── data/
│   │   ├── osint_postmatch.py    # 核心执行脚本
│   │   └── team_alias_map.json   # 中英文队名映射字典
├── config/
│   └── settings.yaml             # API密钥及目标URL配置
└── {ARES_VAULT_PATH}/
    └── 3_Resources/3.x_Match_Reports/   # 依据资源(Resources)层级落盘赛后物理报告
````

## 3. 技术栈 (Tech Stack)

- **Language**: Python 3.10+
    
- **Crawler**: `requests` / `playwright` (处理动态 JS 渲染)
    
- **Parser**: `BeautifulSoup4` / `json`
    
- **Serialization**: `pyyaml` (生成标准 YAML Frontmatter)
    
- **Integration**: 直接对接 Obsidian 本地 Vault 路径
    

## 4. 数据 Schema 定义 (YAML)

所有抓取的赛后战报必须严格遵守以下格式，以便 Ares 引擎进行结构化抓取和 Dataview 检索：

YAML

```
---
version: 1.0
issue: "26062"
match_id: "MATCH_ID_STRING"
match_name: "Home vs Away"
result:
  score: "X-Y"
  winner: "home/away/draw"
physical_metrics:
  home_xG: 0.00      # 预期进球：核心复盘指标
  away_xG: 0.00
  possession_home: 50
  possession_away: 50
  shots_on_target_home: 0
  shots_on_target_away: 0
key_events:
  red_cards: []      # 格式: ["home_15", "away_88"]
  penalties: []
system_evaluation:
  variance_flag: boolean # 自动计算：若比分与 xG 严重倒挂，标记为 true
---
```

## 5. 执行流逻辑 (Execution Pipeline)

1. **获取参数**: 接收期号 (Issue) 和 目标比赛 ID。
    
2. **遥测抓取**: 从 FBref/Understat/API-Football 等源头抓取赛后原始数据。
    
3. **清洗转换**:
    
    - 使用 `team_alias_map.json` 标准化队名。
        
    - 解析 xG 及关键物理事件。
        
4. **方差计算**:
    
    - 逻辑：`if (loser.xG - winner.xG) > 1.0 then variance_flag = true`。
        
5. **落盘存档**: 将数据写入 `{ARES_VAULT_PATH}/3_Resources/3.x_Match_Reports/{issue}_postmatch.md`。
    

## 6. 开发指令 (Cursor Prompt)

**将以下指令粘贴至 Cursor 即可开始编写代码：**

> "你现在是 Ares v4.1 系统的高级开发工程师。请根据本项目 PRD 编写 `osint_postmatch.py` 脚本。
> 
> 核心逻辑：
> 
> 1. 模拟抓取赛后数据函数 `fetch_stats(match_id)`，提取 xG、比分、控球率和红牌。
>     
> 2. 实现 `variance_flag` 计算逻辑：若输球方的 xG 领先赢球方超过 1.0，则标记为物理方差干扰。
>     
> 3. 使用 `pyyaml` 将清洗后的数据以 YAML Frontmatter 格式写入 Markdown 文件。
>     
> 4. 路径配置：读取环境变量 `ARES_VAULT_PATH` 作为落盘根目录。
>     
> 5. 请确保代码包含完整的异常处理机制和日志记录。"
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