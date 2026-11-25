#!/usr/bin/env python3
"""
Ares 持续球队情报与战术研究系统 - 手动单点调度入口 (Manual MVP CLI - R2 修正版)
严格遵循审核规范：
- 完整记录逐请求审计字段并生成 request_manifest.json
- 去除硬编码足球断言
- 重复文章计数而不重入候选
- 严格禁止覆盖已有的历史 run 目录
- 动态生成日期并安全导出至 Obsidian 试运行区 (严格限制在白名单内)
"""

import os
import sys
import argparse
import json
from datetime import datetime, timezone

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.intelligence.fetcher import fetch_url, get_utc_now, sha256_bytes
from src.intelligence.dedup import FingerprintRegistry
from src.intelligence.extractor import discover_articles_from_index, extract_article_content
from src.intelligence.briefing import format_intelligence_briefing
from src.intelligence.vault_exporter import export_briefing_to_vault, DEFAULT_VAULT_PILOT_DIR

# 官方公开入口配置
TEAM_CONFIGS = {
    "MCI": {
        "name": "Manchester City",
        "index_url": "https://www.mancity.com/news/mens",
        "base_url": "https://www.mancity.com",
        "notes": "官网新闻主页，混合 SSR 架构"
    },
    "LIV": {
        "name": "Liverpool",
        "index_url": "https://www.liverpoolfc.com/news",
        "base_url": "https://www.liverpoolfc.com",
        "notes": "官网新闻主页，SSR 架构"
    },
    "NFO": {
        "name": "Nottingham Forest",
        "index_url": "https://www.nottinghamforest.co.uk/news",
        "base_url": "https://www.nottinghamforest.co.uk",
        "notes": "官网新闻主页，客户端渲染 (CSR)"
    },
    "CHE": {
        "name": "Chelsea",
        "index_url": "https://www.chelseafc.com/en/news",
        "base_url": "https://www.chelseafc.com",
        "notes": "官网新闻主页，纯单页应用 (SPA)"
    }
}

def parse_args():
    parser = argparse.ArgumentParser(description="Ares Continuous Team Intelligence Daily Harvester - R2")
    parser.add_argument("--scope", type=str, default="pilot_four", choices=["pilot_four", "all"],
                        help="采集范围: pilot_four (四队试点: MCI, LIV, NFO, CHE) 或 all")
    parser.add_argument("--team", type=str, default=None, choices=["MCI", "LIV", "NFO", "CHE"],
                        help="仅采集指定单个球队，优先级高于 --scope")
    parser.add_argument("--dry-run", action="store_true",
                        help="演练模式: 抓取与比对指纹，但不更新指纹库，不落盘运行原件，不导出 Vault")
    parser.add_argument("--run-id", type=str, default=None,
                        help="指定运行唯一标识符 Run ID (默认基于 UTC 时间戳自动生成)")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="指定落盘输出目录 (默认为 runs/<run_id>/)")
    parser.add_argument("--registry-file", type=str, default="data/intelligence/fingerprint_registry.json",
                        help="指纹去重注册表存储文件路径")
    parser.add_argument("--max-articles-per-team", type=int, default=5,
                        help="每支球队最多抓取解析的文章数上限 (默认 5)")
    parser.add_argument("--export-vault", action="store_true",
                        help="是否导出简报至 AresVault 试运行区 (包含写后独立物理回读校验)")
    parser.add_argument("--vault-dir", type=str, default=DEFAULT_VAULT_PILOT_DIR,
                        help="AresVault 试运行区目标目录 (必须在白名单内，严禁指向 02_Team_Archives)")
    parser.add_argument("--allowed-vault-root", type=str, default=DEFAULT_VAULT_PILOT_DIR,
                        help="允许导出的根目录白名单 (测试时可注入模拟目录)")
    return parser.parse_args()

def main():
    args = parse_args()
    start_utc = get_utc_now()

    # 1. 确定 run_id 与输出目录
    run_id = args.run_id or f"run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    base_runs_dir = os.path.abspath(args.output_dir or os.path.join("runs", run_id))

    # 防覆盖保护：历史 run 严禁被随意覆盖，确保存证不可篡改
    if os.path.exists(base_runs_dir) and not args.dry_run:
        print(f"[FATAL ERROR] RUN_ID_ALREADY_EXISTS: Output directory already exists: {base_runs_dir}", file=sys.stderr)
        print(f"Safety constraint violation: Overwrite of historical runs is strictly prohibited.", file=sys.stderr)
        sys.exit(1)

    raw_save_dir = os.path.join(base_runs_dir, "raw") if not args.dry_run else None

    # 2. 确定目标球队
    if args.team:
        target_teams = [args.team]
    else:
        target_teams = ["MCI", "LIV", "NFO", "CHE"]

    print(f"================================================================================")
    print(f"Ares Continuous Team Intelligence System - Manual MVP Run (R2)")
    print(f"Run ID:            {run_id}")
    print(f"Mode:              {'DRY-RUN (No state/disk mutation)' if args.dry_run else 'PRODUCTION HARVEST'}")
    print(f"Start UTC:         {start_utc}")
    print(f"Teams:             {', '.join(target_teams)}")
    print(f"Output Dir:        {base_runs_dir if not args.dry_run else '[DRY-RUN MEMORY ONLY]'}")
    print(f"Registry:          {args.registry_file}")
    print(f"Max Articles/Team: {args.max_articles_per_team}")
    print(f"Vault Export:      {args.export_vault}")
    print(f"================================================================================")

    # 3. 初始化指纹去重引擎 (损坏时报错保留，不静默清空)
    registry = FingerprintRegistry(args.registry_file)

    request_audit_log = []
    index_audit = []
    processed_articles = []

    # 4. 遍历球队执行采录
    for team_id in target_teams:
        cfg = TEAM_CONFIGS[team_id]
        print(f"\n>>> Processing [{team_id}] {cfg['name']} ...")

        # 4.1 抓取索引目录
        idx_filename = f"{team_id}_index.html" if not args.dry_run else None
        idx_fetch = fetch_url(cfg["index_url"], save_dir=raw_save_dir, save_filename=idx_filename)

        request_audit_log.append({
            "stage": "INDEX_DISCOVERY",
            "team_id": team_id,
            "requested_url": idx_fetch["requested_url"],
            "final_url": idx_fetch["final_url"],
            "fetch_utc": idx_fetch["fetch_utc"],
            "http_status": idx_fetch["http_status"],
            "bytes_len": idx_fetch["bytes_len"],
            "sha256": idx_fetch["sha256"],
            "saved_relpath": idx_fetch["saved_relpath"],
            "error": idx_fetch["error"]
        })

        if idx_fetch["error"]:
            print(f"  [ERROR] Index fetch failed: {idx_fetch['error']} (HTTP {idx_fetch['http_status']})")
            index_audit.append({
                "team_id": team_id,
                "url": cfg["index_url"],
                "http_status": idx_fetch["http_status"],
                "parser_status": "FETCH_FAILED",
                "discovered_count": 0,
                "checked_count": 0,
                "is_truncated": False,
                "note": f"网络异常: {idx_fetch['error']}"
            })
            continue

        # 4.2 动态发现文章
        discovered_urls, parse_status = discover_articles_from_index(
            team_id, idx_fetch["content_str"], cfg["base_url"]
        )
        total_discovered = len(discovered_urls)
        urls_to_check = discovered_urls[:args.max_articles_per_team]
        is_truncated = total_discovered > len(urls_to_check)

        print(f"  Index fetched ({idx_fetch['bytes_len']} bytes). Discovered {total_discovered} articles. Checked {len(urls_to_check)}. Parser: {parse_status}")

        index_audit.append({
            "team_id": team_id,
            "url": cfg["index_url"],
            "http_status": idx_fetch["http_status"],
            "parser_status": parse_status,
            "discovered_count": total_discovered,
            "checked_count": len(urls_to_check),
            "is_truncated": is_truncated,
            "note": cfg["notes"]
        })

        # 4.3 抓取并解析文章正文
        for idx, art_url in enumerate(urls_to_check):
            slug = art_url.strip("/").split("/")[-1]
            art_filename = f"{team_id}_article_{idx}_{slug[:30]}.html" if not args.dry_run else None
            art_fetch = fetch_url(art_url, save_dir=raw_save_dir, save_filename=art_filename)

            request_audit_log.append({
                "stage": "ARTICLE_HARVEST",
                "team_id": team_id,
                "requested_url": art_fetch["requested_url"],
                "final_url": art_fetch["final_url"],
                "fetch_utc": art_fetch["fetch_utc"],
                "http_status": art_fetch["http_status"],
                "bytes_len": art_fetch["bytes_len"],
                "sha256": art_fetch["sha256"],
                "saved_relpath": art_fetch["saved_relpath"],
                "error": art_fetch["error"]
            })

            if art_fetch["error"]:
                print(f"    [WARN] Failed to fetch article {art_url}: {art_fetch['error']}")
                processed_articles.append({
                    "team_id": team_id,
                    "url": art_url,
                    "title": "Failed to fetch",
                    "published_at": "UNKNOWN",
                    "match_kickoff": None,
                    "press_conference": None,
                    "category": "TECHNICAL_GAP",
                    "status": "FETCH_FAILED_OR_EMPTY",
                    "paragraphs": [],
                    "excerpt": art_fetch["error"],
                    "raw_sha256": None,
                    "semantic_fingerprint": None,
                    "dedup_status": "FETCH_FAILED",
                    "version": 0,
                    "supersedes": None
                })
                continue

            # 结构化正文提取
            extracted = extract_article_content(team_id, art_url, art_fetch["content_str"])

            # 语义去重判定
            dedup_res = registry.evaluate_and_register(
                url=art_url,
                raw_sha256=art_fetch["sha256"],
                title=extracted["title"],
                paragraphs=extracted["paragraphs"],
                fetch_utc=art_fetch["fetch_utc"],
                is_dry_run=args.dry_run
            )

            record = {
                "team_id": team_id,
                "url": art_url,
                "title": extracted["title"],
                "published_at": extracted["published_at"],
                "match_kickoff": extracted["match_kickoff"],
                "press_conference": extracted["press_conference"],
                "category": extracted["category"],
                "status": extracted["status"],
                "paragraphs": extracted["paragraphs"],
                "excerpt": extracted["excerpt"],
                "raw_sha256": art_fetch["sha256"],
                "semantic_fingerprint": dedup_res["semantic_fingerprint"],
                "dedup_status": dedup_res["status"],
                "version": dedup_res["version"],
                "supersedes": dedup_res.get("supersedes")
            }
            processed_articles.append(record)
            print(f"    - [{dedup_res['status']}] {extracted['title'][:45]} (Cat: {extracted['category']})")

    # 5. 组装 Markdown 候选简报
    briefing_md = format_intelligence_briefing(
        run_id=run_id,
        run_utc=start_utc,
        scope=args.scope,
        processed_articles=processed_articles,
        index_audit=index_audit,
        is_dry_run=args.dry_run
    )

    run_manifest = {
        "run_id": run_id,
        "run_utc": start_utc,
        "is_dry_run": args.dry_run,
        "scope": args.scope,
        "teams_requested": target_teams,
        "total_requests_count": len(request_audit_log),
        "articles_processed_count": len(processed_articles),
        "index_audit": index_audit,
        "articles": processed_articles
    }

    # 6. 落盘与可选导出
    if not args.dry_run:
        os.makedirs(base_runs_dir, exist_ok=True)

        # 写入候选简报
        briefing_path = os.path.join(base_runs_dir, "CANDIDATE_BRIEFING.md")
        with open(briefing_path, "w", encoding="utf-8") as f:
            f.write(briefing_md)
        briefing_sha = sha256_bytes(briefing_md.encode('utf-8'))
        print(f"\n[OK] Briefing written to {briefing_path} (SHA: {briefing_sha})")

        # 写入逐次请求追溯清单
        req_manifest_path = os.path.join(base_runs_dir, "request_manifest.json")
        with open(req_manifest_path, "w", encoding="utf-8") as f:
            json.dump(request_audit_log, f, indent=2, ensure_ascii=False)
        print(f"[OK] Request Manifest written to {req_manifest_path}")

        # 写入汇总 Manifest
        manifest_path = os.path.join(base_runs_dir, "run_manifest.json")
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(run_manifest, f, indent=2, ensure_ascii=False)
        print(f"[OK] Run Manifest written to {manifest_path}")

        # 导出至 Vault
        if args.export_vault:
            vault_res = export_briefing_to_vault(
                briefing_md=briefing_md,
                run_id=run_id,
                target_dir=args.vault_dir,
                allowed_root=args.allowed_vault_root
            )
            print(f"[OK] Vault Export Verified: {vault_res['target_path']}")
            print(f"     Readback SHA-256: {vault_res['sha256']} ({vault_res['byte_length']} bytes)")
    else:
        print(f"\n[DRY-RUN] Execution completed in memory. Briefing generated ({len(briefing_md)} chars).")

    print(f"\n================================================================================")
    print(f"Run completed successfully.")
    print(f"================================================================================")

if __name__ == "__main__":
    main()
