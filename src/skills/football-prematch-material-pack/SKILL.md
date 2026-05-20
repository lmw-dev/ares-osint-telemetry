---
name: football-prematch-material-pack
version: "1.0"
source: ares-osint-telemetry native
description: >
  赛前资料包/材料包一键生成技能。
  基于“Retriever-Reader-Packager”三层协同大架构中的 Tier 3 设计，
  扫描比赛日目录下的 market.json 和 abnormal.json，
  利用队名别名强映射将 02_Team_Archives 底座只读拷贝为 xx_home.md / xx_away.md，
  智能解析并将临场存疑伤停和阵容残缺评级注入推演卡，
  最后交付 100% 对齐 SOP v1.0 规范的 00_match_list.csv 和 Obsidian 树形结构，
  支持一键打包与推演。
inputs:
  - date: 比赛日批次日期 (格式为 YYYYMMDD，例如 20260520)
outputs:
  - 空白推演材料包 → /vaults/AresVault/03_Match_Audits/AresMatchday_{date}/
  - 拉链总大表 → /vaults/AresVault/03_Match_Audits/AresMatchday_{date}/00_match_list.csv
  - 子对阵目录及文件 → 包含 xx_home.md, xx_away.md, xx_audit.md
---

# Football Prematch Material Pack V1.0

## 执行模式说明

本 Skill 是赛前材料包一键生成的自动化交付组件。
它的定位是 **Packager (打包打包机)**，贯彻**“只读只拷贝”**底座原则和**“异常信息 Fact Gate 数据闭环”**设计。
它能有效预防数据混乱、版本冲突，一键构建起支持 Obsidian 并发推演的高性能空白材料包。

---

## 核心原则

1. **底座只读拷贝原则 (Copy-on-Write)**：
   - 打包搬运时必须是只读的，将底座档案复制为 `xx_home.md` 和 `xx_away.md`。
   - 绝不允许对 `02_Team_Archives` 中的原始文件进行任何覆写，保证底层数据的持久与安全。
2. **防错强拉链 (Sanity Check Zipper)**：
   - 导出的 `00_match_list.csv` 格式、场次编号前缀（如 `01_`）、文件名对齐以及主客队关系必须 100% 对齐，杜绝错配和漏场。
3. **伤停数据隔离与透传 (Absences Sandboxing)**：
   - 将已过滤的临场悬疑伤停（`NEEDS_LATEST_CONFIRMATION`）和受灾评级作为动态数据注入到空白推演卡 `xx_audit.md` 中，让 ChatGPT 核心战术推演中枢可以直接阅读，无需反复检索。

---

## 3 阶段一键打包流程 (3-Phase Package Workflow)

### Phase 1: 检测数据源与别名校准 (Preflight Verification)
- 大模型或 Python 脚本自动读取目标比赛日目录 `AresMatchday_{date}` 下的 `market.json` 和 `abnormal.json`。
- 校验主客队名，通过 `team_alias_map.json` 做中英文和模糊拼写解析。

### Phase 2: 只读搬运与空白卡生成 (Generate Blanks)
- 大模型调用 `src/skills/football-prematch-material-pack/scripts/generate_material_pack.py` 一键生成 Obsidian 树形结构：
  - 自动为每场比赛新建单独的子目录（如 `01_Tottenham_Everton/`）。
  - 将最新修正后的主客队底座只读拷贝为 `01_home.md` 和 `01_away.md`。
  - 读取异常新闻和受灾评级，渲染并输出已填充高可信事实的空白推演卡 `01_audit.md`。

### Phase 3: 对齐交付与 ZIP 准备 (Index CSV Output)
- 写入 100% 对齐 SOP v1.0 规范的 `00_match_list.csv` 作为拉链总表。
- 提醒用户材料包已成功在 Obsidian Vault 中落盘，可直接进行下一步的战术推演。
