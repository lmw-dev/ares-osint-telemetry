import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import List

from audit_router import load_dotenv_into_env, normalize_vault_path


def _run(cmd: List[str], cwd: Path) -> None:
    print(f"[run] {' '.join(cmd)}")
    subprocess.run(cmd, cwd=str(cwd), check=True)


def _resolve_engine_python(repo_root: Path, explicit_engine_dir: str) -> str:
    engine_dir = Path(explicit_engine_dir).expanduser().resolve() if explicit_engine_dir else (repo_root.parent / "20-ares-v4-engine")
    py = engine_dir / ".venv" / "bin" / "python"
    if not py.exists():
        raise FileNotFoundError(f"找不到 20-engine python: {py}")
    return str(py)


def main() -> int:
    parser = argparse.ArgumentParser(description="Date mode one-shot prematch runner")
    parser.add_argument("--date", required=True, help="YYYYMMDD")
    parser.add_argument("--scope", default="top5", help="默认 top5")
    parser.add_argument("--date-source", default="understat", choices=["understat", "football-data"])
    parser.add_argument("--engine-dir", default="", help="20-engine 仓库路径（可选）")
    parser.add_argument("--titan-html-dir", default="", help="离线 Titan HTML 目录（可选）")
    parser.add_argument("--skip-crawler", action="store_true", help="跳过 crawler，复用已有 manifest")
    parser.add_argument("--ops-mode", action="store_true", help="prematch_synthesis 启用 ops 候选池")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent.parent
    load_dotenv_into_env(repo_root)

    vault_env = os.getenv("ARES_VAULT_PATH")
    if not vault_env:
        raise EnvironmentError("未检测到 ARES_VAULT_PATH。")
    vault_root = Path(normalize_vault_path(vault_env)).expanduser()

    issue = f"DATE-{args.date}-{str(args.scope).strip().lower()}"
    manifest_path = vault_root / "04_RAG_Raw_Data" / "Cold_Data_Lake" / f"{issue}_dispatch_manifest.json"

    py = sys.executable
    if not args.skip_crawler:
        _run([
            py,
            str(repo_root / "src" / "data" / "osint_crawler.py"),
            "--date",
            args.date,
            "--scope",
            args.scope,
            "--date-source",
            args.date_source,
        ], repo_root)

    intel_cmd = [
        py,
        str(repo_root / "src" / "data" / "injury_lineup_intel_collect.py"),
        "--issue",
        issue,
        "--merge",
    ]
    if args.titan_html_dir:
        intel_cmd.extend(["--titan-html-dir", args.titan_html_dir])
    _run(intel_cmd, repo_root)

    intel_file = vault_root / "03_Match_Audits" / issue / "03_Review_Reports" / f"TEAM-INTEL-{issue}.json"

    _run([
        py,
        str(repo_root / "src" / "data" / "team_archive_backfill.py"),
        "--issue",
        issue,
        "--intel-file",
        str(intel_file),
    ], repo_root)

    # 先生成 diagnostics，再刷新 Prematch Input Gate，最后再跑一次 preflight 读取新 gate。
    _run([
        py,
        str(repo_root / "src" / "data" / "prematch_preflight.py"),
        "--issue",
        issue,
    ], repo_root)

    # 强制刷新 Prematch Input Gate / Team Enrichment Queue，避免复用旧快照。
    _run([
        py,
        str(repo_root / "src" / "data" / "osint_pipeline.py"),
        "--date",
        args.date,
        "--scope",
        args.scope,
        "--skip-crawler",
        "--skip-prematch",
        "--skip-postmatch",
        "--skip-team-forge",
        "--skip-team-backfill",
    ], repo_root)

    _run([
        py,
        str(repo_root / "src" / "data" / "prematch_preflight.py"),
        "--issue",
        issue,
    ], repo_root)

    engine_py = _resolve_engine_python(repo_root, args.engine_dir)
    engine_dir = Path(args.engine_dir).expanduser().resolve() if args.engine_dir else (repo_root.parent / "20-ares-v4-engine")
    _run([
        engine_py,
        "main.py",
        "audit-issue",
        "--issue",
        issue,
        "--manifest",
        str(manifest_path),
    ], engine_dir)

    synthesis_cmd = [
        py,
        str(repo_root / "src" / "data" / "prematch_synthesis.py"),
        "--issue",
        issue,
        "--force-rule",
    ]
    if args.ops_mode:
        synthesis_cmd.append("--ops-mode")
    _run(synthesis_cmd, repo_root)

    print("[summary]")
    print(f"issue={issue}")
    print(f"manifest={manifest_path}")
    print(f"synthesis_md={vault_root / '03_Match_Audits' / issue / '02_Special_Analyses' / f'FINAL-{issue}-Prematch_Synthesis.md'}")
    print(f"synthesis_json={vault_root / '03_Match_Audits' / issue / '02_Special_Analyses' / f'FINAL-{issue}-Prematch_Synthesis.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
