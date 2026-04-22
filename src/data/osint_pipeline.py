import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from audit_router import AuditRouter, load_dotenv_into_env
from osint_crawler import AresOsintCrawler
from osint_postmatch import MatchTelemetryPipeline


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("AresTelemetry.Pipeline")


def resolve_manifest_path(base_dir: Path, issue: str) -> Path:
    vault_path = os.getenv("ARES_VAULT_PATH")
    if vault_path:
        normalized = MatchTelemetryPipeline._normalize_vault_path(vault_path)
        primary = Path(normalized) / "04_RAG_Raw_Data" / "Cold_Data_Lake" / f"{issue}_dispatch_manifest.json"
        if primary.exists():
            return primary
    return base_dir / "raw_reports" / f"{issue}_dispatch_manifest.json"


def load_manifest(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def run_batch_postmatch(
    *,
    issue: str,
    manifest: Dict[str, Any],
    source: str,
    league: Optional[str],
) -> Dict[str, int]:
    success = 0
    skipped = 0
    failed = 0
    for match in manifest.get("matches", []):
        fbref_url = match.get("fbref_url")
        uid = match.get("understat_id") or fbref_url
        official_score = match.get("official_score") or match.get("result_score")
        if not uid:
            skipped += 1
            continue

        logger.info("==> 一键流程赛后复盘: %s (Ref: %s)", match.get("chinese"), uid)
        pipeline = MatchTelemetryPipeline(
            issue=issue,
            match_id=str(uid),
            source=source,
            fbref_url=fbref_url,
            official_score=official_score,
            league=(
                match.get("league")
                or match.get("competition")
                or manifest.get("league")
                or league
            ),
        )
        try:
            pipeline.run()
            success += 1
        except Exception as e:
            logger.error("一键流程赛后复盘失败 %s: %s", match.get("chinese"), e)
            failed += 1

    return {"success": success, "skipped": skipped, "failed": failed}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ares One-Command OSINT Pipeline")
    parser.add_argument("--issue", type=str, required=True, help="中国体彩期号，如 26064")
    parser.add_argument(
        "--source",
        type=str,
        default="auto",
        choices=["auto", "understat", "fbref"],
        help="赛后数据源策略: auto(先 Understat 后 FBref) | understat | fbref",
    )
    parser.add_argument("--league", type=str, required=False, help="联赛名，赛后定位 Team_Archives 时可用")
    parser.add_argument("--skip-crawler", action="store_true", help="跳过 crawler，仅消费已有 dispatch_manifest")
    parser.add_argument("--skip-postmatch", action="store_true", help="只跑 crawler 与目录路由，不跑赛后复盘")
    parser.add_argument("--no-prematch-stubs", action="store_true", help="不生成 Prematch 骨架文档")
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent.parent.parent
    load_dotenv_into_env(base_dir)

    router = AuditRouter(base_dir=base_dir)
    manifest_path: Optional[Path] = None

    if not args.skip_crawler:
        crawler = AresOsintCrawler(issue=args.issue)
        manifest_path = crawler.scan_and_map()
    else:
        manifest_path = resolve_manifest_path(base_dir, args.issue)

    if not manifest_path or not manifest_path.exists():
        logger.error("找不到 dispatch_manifest: %s", manifest_path)
        raise SystemExit(1)

    manifest = load_manifest(manifest_path)

    if router.enabled:
        try:
            router.ensure_issue_governance(
                issue=args.issue,
                manifest=manifest,
                create_prematch_stubs=not args.no_prematch_stubs,
            )
        except Exception as e:
            logger.warning("AuditRouter 预处理失败（不影响主流程）: %s", e)

    if args.skip_postmatch:
        logger.info("一键流程结束（已跳过赛后复盘） issue=%s", args.issue)
        raise SystemExit(0)

    summary = run_batch_postmatch(
        issue=args.issue,
        manifest=manifest,
        source=args.source,
        league=args.league,
    )

    if router.enabled:
        try:
            router.ensure_issue_governance(
                issue=args.issue,
                manifest=manifest,
                create_prematch_stubs=False,
            )
        except Exception as e:
            logger.warning("AuditRouter 收尾失败（不影响主流程）: %s", e)

    logger.info(
        "一键流程完成 issue=%s, success=%s, skipped=%s, failed=%s",
        args.issue,
        summary["success"],
        summary["skipped"],
        summary["failed"],
    )
