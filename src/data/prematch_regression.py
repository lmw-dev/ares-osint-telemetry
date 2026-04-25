import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from audit_router import load_dotenv_into_env, normalize_vault_path


def _run(cmd: List[str], *, cwd: Path) -> None:
    print(f"[run] {' '.join(cmd)}")
    subprocess.run(cmd, cwd=str(cwd), check=True)


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _count_smoke_matches(matches: List[Dict[str, Any]]) -> int:
    count = 0
    for match in matches:
        mode = str(match.get("manual_anchor_mode") or "").strip().lower()
        notes = str(match.get("manual_anchor_notes") or "").strip().lower()
        fbref_url = str(match.get("fbref_url") or "").strip().lower()
        if mode == "smoke" or "[smoke]" in notes or fbref_url.startswith("https://anchor.local/"):
            count += 1
    return count


def _build_seed_cmd(args: argparse.Namespace, python_exe: str, repo_root: Path) -> List[str]:
    cmd = [
        python_exe,
        str(repo_root / "src" / "data" / "unmapped_anchor_seed.py"),
        "--issue",
        args.issue,
    ]
    if args.clear_smoke:
        cmd.append("--clear-smoke")

    if args.mode == "smoke":
        cmd.extend(["--mode", "smoke", "--allow-smoke", "--smoke-count", str(args.smoke_count)])
        if args.indices:
            cmd.extend(["--indices", args.indices])
    else:
        cmd.extend(["--mode", "production", "--indices", args.indices])
        if args.understat_id:
            cmd.extend(["--understat-id", args.understat_id])
        if args.fbref_url:
            cmd.extend(["--fbref-url", args.fbref_url])
        if args.football_data_match_id:
            cmd.extend(["--football-data-match-id", args.football_data_match_id])
        if args.notes:
            cmd.extend(["--notes", args.notes])
        if args.force:
            cmd.append("--force")
    return cmd


def _validate_args(args: argparse.Namespace) -> None:
    if args.mode == "production" and not args.clear_smoke:
        if not args.indices:
            raise ValueError("production 模式必须提供 --indices。")
        if not (args.understat_id or args.fbref_url or args.football_data_match_id):
            raise ValueError("production 模式至少要提供一个真实锚点参数。")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="One-shot regression: anchor seed -> crawler -> preflight",
    )
    parser.add_argument("--issue", required=True, help="体彩期号，如 26066")
    parser.add_argument("--mode", choices=["smoke", "production"], default="smoke")
    parser.add_argument("--smoke-count", type=int, default=3)
    parser.add_argument("--indices", type=str, default="", help="指定场次索引，如 2,3,4")
    parser.add_argument("--clear-smoke", action="store_true", help="先清理 smoke 锚点再继续链路")
    parser.add_argument("--understat-id", type=str, default="")
    parser.add_argument("--fbref-url", type=str, default="")
    parser.add_argument("--football-data-match-id", type=str, default="")
    parser.add_argument("--notes", type=str, default="")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--engine-dir", type=str, default="", help="透传给 prematch_preflight.py")
    args = parser.parse_args()

    _validate_args(args)

    repo_root = Path(__file__).resolve().parent.parent.parent
    load_dotenv_into_env(repo_root)

    vault_env = os.getenv("ARES_VAULT_PATH")
    if not vault_env:
        raise EnvironmentError("未检测到 ARES_VAULT_PATH。")
    vault_root = Path(normalize_vault_path(vault_env)).expanduser()

    python_exe = sys.executable

    seed_cmd = _build_seed_cmd(args, python_exe, repo_root)
    _run(seed_cmd, cwd=repo_root)

    crawler_cmd = [
        python_exe,
        str(repo_root / "src" / "data" / "osint_crawler.py"),
        "--issue",
        args.issue,
    ]
    _run(crawler_cmd, cwd=repo_root)

    preflight_cmd: List[str] = [
        python_exe,
        str(repo_root / "src" / "data" / "prematch_preflight.py"),
        "--issue",
        args.issue,
    ]
    if args.engine_dir:
        preflight_cmd.extend(["--engine-dir", args.engine_dir])
    _run(preflight_cmd, cwd=repo_root)

    manifest_path = vault_root / "04_RAG_Raw_Data" / "Cold_Data_Lake" / f"{args.issue}_dispatch_manifest.json"
    diagnostics_path = vault_root / "03_Match_Audits" / str(args.issue) / f"Audit-{args.issue}-team-diagnostics.json"

    manifest = _load_json(manifest_path)
    diagnostics = _load_json(diagnostics_path)

    matches = manifest.get("matches") or []
    teams = diagnostics.get("teams") or []

    total_matches = len(matches)
    unmapped = sum(1 for row in matches if str(row.get("mapping_source") or "").lower() == "unmapped")
    smoke_matches = _count_smoke_matches(matches)

    usable = sum(1 for row in teams if str(row.get("archive_status") or "") == "usable")
    needs_enrichment = sum(1 for row in teams if bool(row.get("needs_enrichment")))
    thin_rag_docs = sum(1 for row in teams if int(row.get("rag_doc_count") or 0) <= 1)

    print("[summary]")
    print(f"issue={args.issue}")
    print(f"preflight_status={diagnostics.get('status')}")
    print(f"matches_total={total_matches}")
    print(f"unmapped={unmapped}")
    print(f"smoke_anchor_matches={smoke_matches}")
    print(f"usable_team_archives={usable}")
    print(f"needs_enrichment_teams={needs_enrichment}")
    print(f"thin_rag_docs={thin_rag_docs}")
    print(f"manifest={manifest_path}")
    print(f"team_diagnostics={diagnostics_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
