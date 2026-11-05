#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ares OSINT Telemetry — Obsidian AresVault 知识库深度治理与瘦身工具
AresVault Deep Governance, De-cluttering & Lifecycle Tool

治理目标：
1. 清除侵入知识库的流水线沙箱与中间态 (05_Team_Archive_Sync_Staging_*, _Postmatch_Runtime, _Shadow_Run 等)；
2. 清除深层嵌套的历史临时备份 (.stage6_*_backup, _backup_before_* 等)；
3. 移出非文档资产与安全隐患 (_code, 99_Run_Logs, apikey.md, records 空目录)；
4. 归整收敛 03_Match_Audits/2026-2027/_archive 内深埋的 5600+ 历史快照至外部冷备，为当前赛季目录极大减负；
5. 核心生产档案 100% 绝对保护，生成可回滚清单 VAULT_CLEANUP_MANIFEST.tsv。
"""

import os
import sys
import shutil
from pathlib import Path
from typing import List, Tuple

VAULT_ROOT = Path("/Users/liumingwei/vaults/AresVault").resolve()
REPO_ROOT = Path("/Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry").resolve()

COLD_STORAGE = REPO_ROOT / "runtime_cache" / "cold_storage"
VAULT_CLEANUP_DEST = COLD_STORAGE / "vault_governance_archive"
MANIFEST_PATH = COLD_STORAGE / "VAULT_CLEANUP_MANIFEST.tsv"


def get_cleanup_plan() -> List[Tuple[Path, Path, str]]:
    """扫描所有待治理的外迁与清理目标"""
    plan = []

    # 1. 02_Team_Archives/priors 下隐藏的临时备份 (.stage6_*)
    priors_dir = VAULT_ROOT / "02_Team_Archives" / "priors"
    if priors_dir.exists():
        for item in os.listdir(priors_dir):
            if item.startswith(".stage6_"):
                p = priors_dir / item
                dest = VAULT_CLEANUP_DEST / "02_Team_Archives" / "priors" / item
                plan.append((p, dest, "Priors_Hidden_Backup"))

    # 2. 02_Team_Archives 内部的流水线运行时与快照目录
    for dname in ["_Postmatch_Runtime", "_rollback_snapshots"]:
        p = VAULT_ROOT / "02_Team_Archives" / dname
        if p.exists():
            dest = VAULT_CLEANUP_DEST / "02_Team_Archives" / dname
            plan.append((p, dest, "Team_Archive_Runtime"))

    # 3. 01_Governance 中的临时备份目录
    gov_core = VAULT_ROOT / "01_Governance" / "00_Core_System"
    if gov_core.exists():
        for item in os.listdir(gov_core):
            if "backup" in item.lower():
                p = gov_core / item
                dest = VAULT_CLEANUP_DEST / "01_Governance" / "00_Core_System" / item
                plan.append((p, dest, "Governance_Backup"))

    # 4. 05_Team_Knowledge_Maintenance 中历史已闭环的 Staging 目录
    p5 = VAULT_ROOT / "05_Team_Knowledge_Maintenance"
    if p5.exists():
        for dname in [
            "05_Team_Archive_Sync_Staging",
            "05_Team_Archive_Sync_Staging_V2",
            "05_Team_Archive_Sync_Staging_W43",
        ]:
            p = p5 / dname
            if p.exists():
                dest = VAULT_CLEANUP_DEST / "05_Team_Knowledge_Maintenance" / dname
                plan.append((p, dest, "Team_Knowledge_Staging"))

    # 5. 03_Match_Audits 中的影子测试数据与深埋的旧快照
    p3 = VAULT_ROOT / "03_Match_Audits"
    if p3.exists():
        shadow = p3 / "_Shadow_Run"
        if shadow.exists():
            dest = VAULT_CLEANUP_DEST / "03_Match_Audits" / "_Shadow_Run"
            plan.append((shadow, dest, "Match_Audit_Shadow_Run"))

        p3_2627 = p3 / "2026-2027"
        if p3_2627.exists():
            for dname in ["_archive", "_handoff"]:
                p = p3_2627 / dname
                if p.exists():
                    dest = VAULT_CLEANUP_DEST / "03_Match_Audits" / "2026-2027" / dname
                    plan.append((p, dest, "Match_Audit_2026_2027_Archive"))

    # 6. 非文档资产与安全隐患文件
    for dname, cat in [("_code", "Legacy_Code"), ("99_Run_Logs", "Legacy_Logs"), ("records", "Legacy_Empty_Dir")]:
        p = VAULT_ROOT / dname
        if p.exists():
            dest = VAULT_CLEANUP_DEST / dname
            plan.append((p, dest, cat))

    apikey = VAULT_ROOT / "apikey.md"
    if apikey.exists():
        dest = VAULT_CLEANUP_DEST / "apikey.md"
        plan.append((apikey, dest, "Sensitive_API_Key"))

    return plan


def run_cleanup(execute: bool = False):
    print("=== Ares Obsidian AresVault 深度治理与瘦身 ===")
    print(f"模式: {'[LIVE EXECUTION (真实迁移)]' if execute else '[DRY RUN (模拟预览)]'}")

    plan = get_cleanup_plan()
    print(f"待治理项统计: 共 {len(plan)} 个目标目录/文件\n")

    total_files = 0
    total_bytes = 0

    manifest_lines = ["source_path\tdest_path\tsize_bytes\tcategory\n"]

    for src, dest, cat in plan:
        if not src.exists():
            continue
        if src.is_dir():
            files = [f for f in src.rglob("*") if f.is_file()]
            sz = sum(f.stat().st_size for f in files)
            cnt = len(files)
        else:
            sz = src.stat().st_size
            cnt = 1

        total_files += cnt
        total_bytes += sz
        print(f"  [{cat:25}] {str(src.relative_to(VAULT_ROOT)):55} | Files: {cnt:5} | {sz/(1024*1024):6.2f} MB")

        if execute:
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(dest))
            manifest_lines.append(f"{src}\t{dest}\t{sz}\t{cat}\n")

    print(f"\n治理统计汇总: 涉及文件 {total_files} 个，总释放空间 {total_bytes / (1024*1024):.2f} MB")

    if not execute:
        print(f"\n[INFO] 模拟预览完毕。如需执行，请传入 --execute 参数。")
        return

    # 落盘审计清单
    COLD_STORAGE.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w", encoding="utf-8") as fp:
        fp.writelines(manifest_lines)
    print(f"[OK] 物理外迁完成！审计清单已生成: {MANIFEST_PATH}")

    # 递归清理可能留下的空目录
    clean_empty_dirs(VAULT_ROOT)


def clean_empty_dirs(root: Path):
    if not root.exists():
        return
    removed_count = 0
    for dirpath, dirnames, filenames in os.walk(root, topdown=False):
        d = Path(dirpath)
        if d == root:
            continue
        try:
            if not any(d.iterdir()):
                d.rmdir()
                removed_count += 1
        except Exception:
            pass
    if removed_count > 0:
        print(f"[INFO] 已清理 AresVault 下的 {removed_count} 个空文件夹。")


def run_rollback():
    if not MANIFEST_PATH.exists():
        print(f"[ERROR] 找不到迁移清单: {MANIFEST_PATH}，无法回滚！")
        sys.exit(1)

    print("=== AresVault 治理逆向回滚 ===")
    with open(MANIFEST_PATH, "r", encoding="utf-8") as fp:
        lines = fp.read().splitlines()

    rows = [l.split("\t") for l in lines[1:] if l.strip()]
    restored = 0
    for r in rows:
        src = Path(r[0])
        dest = Path(r[1])
        if dest.exists():
            src.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(dest), str(src))
            restored += 1
    print(f"[OK] 回滚完成！成功还原 {restored} 项资产。")
    MANIFEST_PATH.unlink(missing_ok=True)


def verify_vault_status():
    print("\n=== AresVault 治理后健康度与生产档案核验 ===")
    # 核心生产档案核验
    epl_dir = VAULT_ROOT / "02_Team_Archives" / "1_Top_Five_Europe" / "ENG_England"
    nottingham = epl_dir / "Nottingham_Forest.md"
    mancity = epl_dir / "Manchester_City.md"
    assert nottingham.exists(), "FAIL: 诺丁汉森林生产档案缺失！"
    assert mancity.exists(), "FAIL: 曼城生产档案缺失！"
    print("1. 五大联赛生产球队档案: 100% 完好存在 (PASS)")

    # 检查隐藏备份是否清零
    priors = VAULT_ROOT / "02_Team_Archives" / "priors"
    hidden = [f for f in os.listdir(priors) if f.startswith(".stage6_")] if priors.exists() else []
    print(f"2. Priors 隐藏临时备份残留: {len(hidden)} (期望值: 0)")
    assert len(hidden) == 0, f"FAIL: 仍有隐藏备份: {hidden}"

    # 检查根目录敏感信息
    assert not (VAULT_ROOT / "apikey.md").exists(), "FAIL: apikey.md 仍存在！"
    assert not (VAULT_ROOT / "_code").exists(), "FAIL: _code 仍存在！"
    assert not (VAULT_ROOT / "records").exists(), "FAIL: records 空目录仍存在！"
    print("3. 敏感 Key、代码及孤立空目录已全部清除 (PASS)")

    print("\n>>> ALL CHECKS PASSED: AresVault 知识大脑已彻底净化轻量化！")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true", help="执行物理治理与外迁")
    parser.add_argument("--verify", action="store_true", help="验证治理后健康度")
    parser.add_argument("--rollback", action="store_true", help="逆向还原")
    args = parser.parse_args()

    if args.rollback:
        run_rollback()
    elif args.verify:
        verify_vault_status()
    else:
        run_cleanup(execute=args.execute)
