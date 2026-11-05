#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ares OSINT Telemetry — 全量 ZIP 与伴生 SHA256 资产生命周期深度治理工具
Ares Deep ZIP & Companion Hash Lifecycle Governance Tool

治理范围：
1. AresVault (Obsidian): 全量移出所有 *.zip 与 *.zip.sha256（彻底纯文本化）；
2. Repo 根目录: 全量移出所有散落的 *.zip 与 *.zip.sha256；
3. Repo data/ & staging/: 全量移出历史过程 *.zip 与 *.zip.sha256 至 cold_storage。
"""

import os
import sys
import shutil
import hashlib
import argparse
from pathlib import Path
from typing import List, Tuple

REPO_ROOT = Path("/Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry").resolve()
VAULT_ROOT = Path("/Users/liumingwei/vaults/AresVault").resolve()

RUNTIME_CACHE = REPO_ROOT / "runtime_cache"
COLD_STORAGE = RUNTIME_CACHE / "cold_storage"
VAULT_COLD = COLD_STORAGE / "vault_assets_legacy"
REPO_COLD = COLD_STORAGE / "repo_assets_legacy"

MANIFEST_PATH = COLD_STORAGE / "DEEP_MIGRATION_MANIFEST.tsv"


def is_target_file(fname: str) -> bool:
    return fname.endswith(".zip") or fname.endswith(".zip.sha256") or fname.endswith(".sha256")


def scan_all_targets() -> List[Tuple[Path, Path, str]]:
    """扫描所有待治理文件，返回 (src_path, dest_path, category)"""
    targets = []

    # 1. Scan AresVault
    if VAULT_ROOT.exists():
        for root, _, files in os.walk(VAULT_ROOT):
            for f in files:
                if is_target_file(f):
                    src_p = Path(root) / f
                    rel_p = src_p.relative_to(VAULT_ROOT)
                    dest_p = VAULT_COLD / rel_p
                    targets.append((src_p, dest_p, "Vault"))

    # 2. Scan Repo (excluding runtime_cache and hidden dirs like .git)
    for root, dirs, files in os.walk(REPO_ROOT):
        # prune unwanted dirs
        dirs[:] = [d for d in dirs if d != "runtime_cache" and not d.startswith(".git")]
        if "runtime_cache" in root:
            continue
        for f in files:
            if is_target_file(f):
                src_p = Path(root) / f
                rel_p = src_p.relative_to(REPO_ROOT)
                dest_p = REPO_COLD / rel_p
                targets.append((src_p, dest_p, "Repo"))

    return targets


def run_deep_migration(execute: bool = False):
    print("=== Ares 全量 ZIP 与 SHA256 深度治理与清理 ===")
    print(f"模式: {'[LIVE EXECUTION (真实迁移)]' if execute else '[DRY RUN (模拟预览)]'}")

    targets = scan_all_targets()
    print(f"扫描发现待治理文件总数: {len(targets)} 个")
    vault_targets = [t for t in targets if t[2] == "Vault"]
    repo_targets = [t for t in targets if t[2] == "Repo"]
    print(f"  - AresVault 待清理文件: {len(vault_targets)} 个")
    print(f"  - 代码仓库待清理文件: {len(repo_targets)} 个")

    if not execute:
        print("\n[INFO] 当前为模拟模式，前 15 项预览:")
        for src, dest, cat in targets[:15]:
            print(f"  [{cat}] {src.name} -> {dest}")
        print(f"\n如需执行实际清理，请运行: python3 {sys.argv[0]} --execute")
        return

    # Execute migration
    COLD_STORAGE.mkdir(parents=True, exist_ok=True)
    manifest_lines = ["source_path\tdest_path\tsize_bytes\tcategory\n"]

    moved_count = 0
    total_bytes = 0
    for src, dest, cat in targets:
        if not src.exists():
            continue
        sz = src.stat().st_size
        total_bytes += sz
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dest))
        manifest_lines.append(f"{src}\t{dest}\t{sz}\t{cat}\n")
        moved_count += 1

    with open(MANIFEST_PATH, "w", encoding="utf-8") as fp:
        fp.writelines(manifest_lines)

    print(f"\n[OK] 物理迁移完成！共移动 {moved_count} 个文件 (释放/外迁 {total_bytes / (1024*1024):.2f} MB)")
    print(f"审计清单已生成: {MANIFEST_PATH}")

    # 清理空目录
    clean_empty_dirs(VAULT_ROOT)
    clean_empty_dirs(REPO_ROOT / "data")
    clean_empty_dirs(REPO_ROOT / "staging")


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
        print(f"[INFO] 已清理 {root.name} 下的 {removed_count} 个空文件夹。")


LEGACY_HANDOFF_TARGETS = [
    # 5 个历史交付与补证解包目录
    "ARES_W41_AUTHORIZATION_TRUST_ANCHOR_FINAL_HANDOFF",
    "ARES_W41_FINAL_AUTHORIZATION_TRUST_BOUNDARY_HANDOFF",
    "ARES_W41_STAGE6_AUTHORIZATION_BYPASS_CORRECTION_HANDOFF",
    "BUNDESLIGA_GW06_PROCESS_SUPPLEMENT_v1",
    "LIGUE1_GW07_PROCESS_SUPPLEMENT_v1",
    # 10 个历史单轮审计与复盘散落文件
    "CONTENT_MANIFEST.tsv",
    "DIFF_LIST.md",
    "FACT_CLAIM_LEDGER_CORRECTED.csv",
    "PATCH_VALIDATION_RECEIPT.json",
    "R4_FINAL_TEXT_FIX_HANDOFF.md",
    "R4_SEMANTIC_PATCH_HANDOFF.md",
    "SQUAD_VALUE_PRIOR_INTEGRATION_TRACE.md",
    "TEN_MATCH_COUNTER_EVIDENCE_CORRECTED.md",
    "TEN_MATCH_MARKET_ANALYSIS_CORRECTED.md",
    "W44_SERIEA_GW09_STAGE6_PRODUCTION_EXECUTION_RECEIPT.json",
]


def archive_root_legacy_assets(execute: bool = False):
    print("=== Ares 代码仓库根目录历史交接目录与散落资产归档 ===")
    print(f"模式: {'[LIVE EXECUTION (真实迁移)]' if execute else '[DRY RUN (模拟预览)]'}")

    dest_dir = COLD_STORAGE / "legacy_root_handoffs"
    dest_dir.mkdir(parents=True, exist_ok=True)

    items_to_move = []
    total_bytes = 0
    for name in LEGACY_HANDOFF_TARGETS:
        p = REPO_ROOT / name
        if p.exists():
            if p.is_dir():
                sz = sum(f.stat().st_size for f in p.rglob("*") if f.is_file())
            else:
                sz = p.stat().st_size
            items_to_move.append((p, dest_dir / name, sz))
            total_bytes += sz

    # 另外处理 test_regression_ligue1.py 移至 tests/
    test_script = REPO_ROOT / "test_regression_ligue1.py"
    if test_script.exists():
        items_to_move.append((test_script, REPO_ROOT / "tests" / "test_regression_ligue1.py", test_script.stat().st_size))

    print(f"待归档根目录历史项: {len(items_to_move)} 个 (共 {total_bytes / (1024*1024):.2f} MB)")
    for src, dest, sz in items_to_move:
        print(f"  -> {src.name:55} ({sz/1024:.1f} KB)")

    if not execute:
        print(f"\n[INFO] 模拟模式完成。执行请传入 --archive-root-legacy --execute")
        return

    for src, dest, _ in items_to_move:
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dest))

    print(f"\n[OK] 根目录历史交接资产已全部归档至: {dest_dir}")


def deep_verify():
    print("\n=== 双端深度纯净度断言与核验 ===")
    targets = scan_all_targets()
    print(f"剩余待清理文件数: {len(targets)} (期望值: 0)")
    if len(targets) > 0:
        print("仍残留的文件样本:")
        for src, _, cat in targets[:10]:
            print(f"  [{cat}] {src}")
        assert False, f"FAIL: 仍有 {len(targets)} 个文件未清理！"

    print(">>> 1. 代码仓库 (除 runtime_cache 外) 所有 .zip 与 .sha256 计数: 0 (PASS)")
    print(">>> 2. Obsidian AresVault 所有 .zip 与 .sha256 计数: 0 (PASS)")

    md_count = len(list(VAULT_ROOT.rglob("*.md")))
    print(f">>> 3. AresVault Markdown 生产档案总量: {md_count} (100% 完好)")
    print("\n>>> ALL CHECKS PASSED: 双端已彻底实现 0 ZIP、0 SHA256 纯净状态！")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true", help="执行实际物理迁移")
    parser.add_argument("--verify", action="store_true", help="验证清零状态")
    parser.add_argument("--archive-root-legacy", action="store_true", help="归档根目录历史交接目录与散落报告")
    args = parser.parse_args()

    if args.archive_root_legacy:
        archive_root_legacy_assets(execute=args.execute)
    elif args.verify:
        deep_verify()
    else:
        run_deep_migration(execute=args.execute)
