import argparse
import json
import logging
import os
import subprocess
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


def _resolve_engine_dir(explicit_engine_dir: Optional[str] = None) -> Path:
    """定位 20-engine 仓库根目录。"""
    current_repo = Path(__file__).resolve().parents[3]
    sibling = current_repo.parent / "20-ares-v4-engine"
    raw_path = explicit_engine_dir or os.getenv("ARES_ENGINE_DIR", str(sibling))
    return Path(raw_path).expanduser().resolve()


def preflight_checks(engine_dir: Path) -> list[str]:
    """对跨仓库串联所需的关键依赖做 fail-fast 检查。"""
    errors: list[str] = []

    if not engine_dir.exists():
        errors.append(f"找不到 20-engine 仓库目录: {engine_dir}")
        return errors

    engine_main = engine_dir / "main.py"
    if not engine_main.exists():
        errors.append(f"20-engine 缺少主入口: {engine_main}")

    engine_python = engine_dir / ".venv" / "bin" / "python"
    if not engine_python.exists():
        errors.append(f"20-engine 缺少虚拟环境解释器: {engine_python}")

    vault_path = os.getenv("ARES_VAULT_PATH")
    if not vault_path:
        errors.append("未配置 ARES_VAULT_PATH，无法写入统一 Vault。")
        return errors

    normalized_vault = Path(normalize_vault_path(vault_path))
    if not normalized_vault.exists():
        errors.append(f"ARES_VAULT_PATH 不存在: {normalized_vault}")
        return errors

    audit_root = normalized_vault / "03_Match_Audits"
    try:
        audit_root.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        errors.append(f"无法创建或访问审计目录 {audit_root}: {exc}")
        return errors

    if not os.access(audit_root, os.W_OK):
        errors.append(f"审计目录不可写: {audit_root}")

    return errors


def normalize_vault_path(path_text: str) -> str:
    """标准化 .env 中的 Vault 路径写法。"""
    return str(path_text).replace("\\ ", " ").replace("\\~", "~")


def run_prematch_engine(
    *,
    issue: str,
    manifest_path: Path,
    engine_dir: Path,
) -> Dict[str, int]:
    """调用 20-engine 的 audit-issue 入口执行 Prematch 批量推演。"""
    python_bin = engine_dir / ".venv" / "bin" / "python"
    if not python_bin.exists():
        raise FileNotFoundError(f"找不到 engine Python 解释器: {python_bin}")

    cmd = [
        str(python_bin),
        "main.py",
        "audit-issue",
        "--issue",
        issue,
        "--manifest",
        str(manifest_path),
    ]
    logger.info("==> 一键流程 Prematch 推演: %s", " ".join(cmd))
    result = subprocess.run(
        cmd,
        cwd=engine_dir,
        env=os.environ.copy(),
        text=True,
    )
    if result.returncode != 0:
        logger.error("Prematch 推演失败，exit_code=%s", result.returncode)
        return {"success": 0, "failed": 1}
    return {"success": 1, "failed": 0}


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
    parser.add_argument("--engine-dir", type=str, required=False, help="显式指定 20-engine 仓库路径")
    parser.add_argument("--skip-crawler", action="store_true", help="跳过 crawler，仅消费已有 dispatch_manifest")
    parser.add_argument("--skip-prematch", action="store_true", help="跳过 Prematch 推演，仅跑 crawler/路由/postmatch")
    parser.add_argument("--skip-postmatch", action="store_true", help="只跑 crawler 与目录路由，不跑赛后复盘")
    parser.add_argument("--no-prematch-stubs", action="store_true", help="不生成 Prematch 骨架文档")
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent.parent.parent
    load_dotenv_into_env(base_dir)
    engine_dir = _resolve_engine_dir(args.engine_dir)

    preflight_errors = preflight_checks(engine_dir)
    if preflight_errors:
        for err in preflight_errors:
            logger.error("Preflight 失败: %s", err)
        raise SystemExit(1)

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

    prematch_summary = {"success": 0, "failed": 0}
    if not args.skip_prematch:
        try:
            prematch_summary = run_prematch_engine(
                issue=args.issue,
                manifest_path=manifest_path,
                engine_dir=engine_dir,
            )
        except Exception as e:
            logger.error("Prematch orchestration 失败: %s", e)
            prematch_summary = {"success": 0, "failed": 1}

    if args.skip_postmatch:
        logger.info(
            "一键流程结束（已跳过赛后复盘） issue=%s, prematch_success=%s, prematch_failed=%s",
            args.issue,
            prematch_summary["success"],
            prematch_summary["failed"],
        )
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
        "一键流程完成 issue=%s, prematch_success=%s, prematch_failed=%s, postmatch_success=%s, postmatch_skipped=%s, postmatch_failed=%s",
        args.issue,
        prematch_summary["success"],
        prematch_summary["failed"],
        summary["success"],
        summary["skipped"],
        summary["failed"],
    )
