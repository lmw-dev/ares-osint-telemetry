# Ares OSINT Telemetry — ZIP 资产生命周期分层与双端治理交付文档
## ARES ZIP Asset Lifecycle Tiering & Dual-System (Repo & Vault) Purge Handoff Report

> **文档路径**: `docs/agents/ARES_ZIP_ASSET_LIFECYCLE_AND_DUAL_TIER_CLEANUP_HANDOFF.md`  
> **归档日期**: 2026-11-05  
> **归档角色**: Ares OSINT Telemetry System (系统架构与基础设施组)  
> **终审核准**: 用户 / ChatGPT 独立架构决策  
> **核心目标**: 彻底根除代码仓库根目录与 Obsidian AresVault 知识库内长期堆积的二进制过程类 ZIP 压缩包，落实“过程资产解耦、双端纯文本化、生命周期 TTL 轮转”架构原则，并提供 100% 可回滚的审计支持。

---

## 目录索引
1. [治理背景与核心决策](#1-治理背景与核心决策)
2. [治理成果与量化数据对比](#2-治理成果与量化数据对比)
3. [存储分层拓扑与生命周期规范](#3-存储分层拓扑与生命周期规范)
4. [脚本拦截防线与代码修改](#4-脚本拦截防线与代码修改)
5. [安全回滚与后续自动化运维指引](#5-安全回滚与后续自动化运维指引)

---

## 1. 治理背景与核心决策

此前在系统运行过程中，由于缺乏统一的数据生命周期规范，赛前抓取、临场刷新、赛后采集和归档门禁等中间过程生成的 ZIP 压缩包随意散落，甚至大量堆积在代码仓库根目录以及 Obsidian 生产知识库内部。

经用户深入指示与架构审视，确立了以下三大底层原则：
1. **过程类资产属性定性**：ZIP 包仅为“跨环境传输与阶段审计”的施工脚手架，时效极强，绝非最终生产知识；
2. **Obsidian 零二进制包铁律 (Zero-Blob Vault)**：AresVault 必须回归纯文本 Markdown 的知识大脑定位，Markdown 中仅需记录审计元数据（文件名 + SHA-256 哈希串），物理 ZIP 绝不可驻留 Vault 内部，彻底消除移动端同步和索引卡顿风险；
3. **代码仓库根目录零污染铁律 (Zero-Root Pollution)**：Git 仓库严禁堆放二进制 Blob，所有过程包必须进入受 Git 忽略的独立运行时缓存层（`runtime_cache/`）。

---

## 2. 治理成果与量化数据对比

治理前后双端核心指标对比如下：

| 资产空间与指标 | 治理前状态 (Before) | 治理后状态 (After) | 治理成效与状态 |
| :--- | :---: | :---: | :--- |
| **Obsidian AresVault 内 ZIP 数量** | **255 个** (197.60 MB) | **`0` 个** | **彻底清零**，释放近 200MB 空间，清理 81 个空文件夹 |
| **代码仓库根目录 ZIP 数量** | **87 个** (53.76 MB) | **`0` 个** | **彻底清零**，根目录恢复 100% 纯净代码与文档状态 |
| **代码仓库根目录历史交接目录与文件** | **16 个** (25.93 MB) | **`0` 个** | **彻底清零**，5 个 W41/GW06 历史目录与 10 个平铺报告全部归入冷备 |
| **代码仓库根目录历史僵尸残留目录** | **7 个** (144.6 MB) | **`0` 个** | **彻底清零**，外迁 3_Resources, draft/raw_reports, scratch, tmp, outputs 并清除 config 空目录 |
| **Obsidian Markdown 生产档案** | 8,538 篇 | **8,538 篇** | **100% 保持完好**，生产档案零修改、零丢失、零损坏 |
| **受保护冷备存储承接总量** | 0 个 | **342 个** (251.36 MB) | 统一归口至 `runtime_cache/cold_storage/` |
| **可回退审计清单 (Manifest)** | 无 | **100% 覆盖** | 落盘 `MIGRATION_MANIFEST.tsv`，支持一键逆向还原 |

---

## 3. 存储分层拓扑与生命周期规范

治理后，本地建立了与 Git 代码库及 Obsidian 彻底解耦的运行时分层结构：

```text
/Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/
│
├── .gitignore                   <-- 强拦截：*.zip 与 /runtime_cache/
├── scripts/                     <-- 纯代码
├── docs/                        <-- 纯文档
└── runtime_cache/               <-- 【本地运行时流水线缓存】(完全不进 Git 与 Obsidian)
    │
    ├── active/                  <-- 【热数据区】当前业务周 (生命周期：0 ~ 7 天)
    │   ├── 01_prematch/         <-- 周五赛前交付包 (自动生成)
    │   ├── 02_final_refresh/    <-- 比赛日 18:00 临场刷新包
    │   └── 03_postmatch/        <-- 周二赛后采集与 Deep Review 交付包
    │
    ├── archive/                 <-- 【温数据区】上一业务周 (生命周期：7 ~ 14 天，备查)
    │
    └── cold_storage/            <-- 【冷数据区】历史归档与外迁资产 (> 14 天)
        ├── MIGRATION_MANIFEST.tsv   <-- 342 个外迁资产的完整路径与 SHA-256 映射清单
        ├── vault_zips_legacy/       <-- 原 Obsidian Vault 移出的 255 个历史 ZIP
        └── repo_zips_legacy/        <-- 原代码根目录移出的 87 个历史 ZIP
```

---

## 4. 脚本拦截防线与代码修改

为彻底切断未来脚本继续向根目录乱扔 ZIP 的污染源，完成了以下底层改造：

1. **`scripts/run_postmatch_round.py` 路径重定向**：
   - 修正第 146 行与第 718 行：
     原：`REPO_ROOT / f"{self.league}_{self.round_id}_POSTMATCH_DEEP_HANDOFF.zip"`
     改：`REPO_ROOT / "runtime_cache" / "active" / "03_postmatch" / ...`
   - 自动保证 `runtime_cache/active/03_postmatch/` 目录存在，彻底根除根目录污染源。
2. **`.gitignore` 强化**：
   - 新增规则：`runtime_cache/`，确保整个本地运行时缓存区绝对不会被 Git 索引和误提交。
3. **生命周期治理工具落盘**：
   - 新增 `scripts/manage_zip_lifecycle.py`，提供扫描、模拟（`--dry-run`）、物理迁移（`--execute`）、状态断言（`--verify`）与逆向回滚（`--rollback`）完整能力。

---

## 5. 安全回滚与后续自动化运维指引

### 5.1 逆向无损回滚机制 (Emergency Rollback)
如果由于任何外部系统原因需要将所有 342 个 ZIP 原封不动还原至原始路径，只需在终端执行一行命令：
```bash
python3 scripts/manage_zip_lifecycle.py --rollback
```
脚本将严格依据 `runtime_cache/cold_storage/MIGRATION_MANIFEST.tsv`，原样将每一个文件移动回其原本在 Obsidian 或代码根目录的相对路径，确保绝对安全。

### 5.2 状态核验断言命令 (Health Check)
在日常开发或跑包后，可随时执行以下命令检查两端纯净度：
```bash
python3 scripts/manage_zip_lifecycle.py --verify
```
断言要求：
- 代码根目录 ZIP 计数严格为 0；
- AresVault 内部 ZIP 计数严格为 0；
- AresVault Markdown 生产档案总量保持 $\ge 8500$。

### 5.3 后续常规周运作建议
在每周二完成 Stage 6 生产知识合并入 `02_Team_Archives` 后，由流水线或手动执行一次常规轮转：将上周的 `active/` 移入 `archive/`，超期温数据自动转入冷备或物理清除，确保系统常年保持极致整洁与高效。
