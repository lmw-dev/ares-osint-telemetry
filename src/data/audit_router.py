import argparse
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("AresTelemetry.AuditRouter")


def load_dotenv_into_env(base_dir: Path) -> None:
    env_path = base_dir / ".env"
    if not env_path.exists():
        return
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if not key:
                    continue
                if value and value[0] == value[-1] and value[0] in ("'", '"'):
                    value = value[1:-1]
                os.environ.setdefault(key, value)
    except Exception:
        pass


def normalize_vault_path(path_text: str) -> str:
    normalized = str(path_text).replace("\\ ", " ").replace("\\~", "~")
    return str(Path(normalized).expanduser())


class AuditRouter:
    def __init__(self, base_dir: Path, vault_path: Optional[str] = None):
        self.base_dir = Path(base_dir)
        configured_vault_path = vault_path or os.getenv("ARES_VAULT_PATH")
        if configured_vault_path:
            configured_vault_path = normalize_vault_path(configured_vault_path)

        self.enabled = bool(configured_vault_path)
        self.vault_root = Path(configured_vault_path) if configured_vault_path else None
        self.audit_root = (self.vault_root / "03_Match_Audits") if self.vault_root else None
        self.postmatch_dir = (self.audit_root / "Postmatch_Telemetry") if self.audit_root else None
        self.governance_dir = (self.audit_root / "00_Governance") if self.audit_root else None
        self.adhoc_dir = (self.audit_root / "02_Adhoc_Team_Audits") if self.audit_root else None
        self.legacy_dir = (self.audit_root / "99_Legacy_Archive") if self.audit_root else None

    @staticmethod
    def _sanitize_segment(value: str, fallback: str = "segment") -> str:
        txt = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value).strip())
        txt = txt.strip("_")
        return txt or fallback

    @staticmethod
    def _split_match_english(english_text: str) -> Tuple[str, str]:
        if " vs " in english_text:
            home, away = english_text.split(" vs ", 1)
            return home.strip(), away.strip()
        if " VS " in english_text:
            home, away = english_text.split(" VS ", 1)
            return home.strip(), away.strip()
        return english_text.strip(), "Away"

    @staticmethod
    def _safe_str(value: Any) -> str:
        if value is None:
            return ""
        return str(value)

    def _ensure_core_dirs(self) -> None:
        if not self.enabled:
            return
        for d in [self.audit_root, self.postmatch_dir, self.governance_dir, self.adhoc_dir, self.legacy_dir]:
            d.mkdir(parents=True, exist_ok=True)

    def _issue_dirs(self, issue: str) -> Dict[str, Path]:
        issue_dir = self.audit_root / str(issue)
        prematch_dir = issue_dir / "01_Prematch_Audits"
        special_dir = issue_dir / "02_Special_Analyses"
        review_dir = issue_dir / "03_Review_Reports"
        postmatch_legacy_dir = issue_dir / "04_Postmatch_Legacy"
        return {
            "issue_dir": issue_dir,
            "prematch_dir": prematch_dir,
            "special_dir": special_dir,
            "review_dir": review_dir,
            "postmatch_legacy_dir": postmatch_legacy_dir,
        }

    def _ensure_issue_dirs(self, issue: str) -> Dict[str, Path]:
        dirs = self._issue_dirs(issue)
        for d in dirs.values():
            d.mkdir(parents=True, exist_ok=True)
        return dirs

    def _ensure_prematch_stubs(self, issue: str, matches: List[Dict[str, Any]], prematch_dir: Path) -> int:
        created = 0
        for match in matches:
            index = int(match.get("index", 0) or 0)
            if index <= 0:
                continue
            prefix = f"Audit-{issue}-{index:02d}-"
            if any(prematch_dir.glob(f"{prefix}*.md")):
                continue

            english = self._safe_str(match.get("english"))
            chinese = self._safe_str(match.get("chinese"))
            home, away = self._split_match_english(english or f"Match-{index:02d}")
            home_safe = self._sanitize_segment(home, f"Home{index:02d}")
            away_safe = self._sanitize_segment(away, f"Away{index:02d}")
            filename = f"{prefix}{home_safe}-vs-{away_safe}.md"
            target = prematch_dir / filename

            content = (
                "---\n"
                f'issue: "{issue}"\n'
                f"match_index: {index}\n"
                f'chinese: "{self._safe_str(chinese).replace(chr(34), chr(39))}"\n'
                f'english: "{self._safe_str(english).replace(chr(34), chr(39))}"\n'
                f'league: "{self._safe_str(match.get("league")).replace(chr(34), chr(39))}"\n'
                f'mapping_source: "{self._safe_str(match.get("mapping_source")).replace(chr(34), chr(39))}"\n'
                f'understat_id: "{self._safe_str(match.get("understat_id")).replace(chr(34), chr(39))}"\n'
                f'football_data_match_id: "{self._safe_str(match.get("football_data_match_id")).replace(chr(34), chr(39))}"\n'
                'status: "draft"\n'
                "---\n\n"
                "## Prematch Audit\n\n"
                "- 赔率结构与盘口偏移：\n"
                "- 情报面（伤停/轮换/舆论）：\n"
                "- 物理面（近期 xG / 转化效率）：\n"
                "- Ares 结论（方向 + 风险等级）：\n"
            )
            target.write_text(content, encoding="utf-8")
            created += 1
        return created

    def _sync_duplicate_postmatch(self, issue: str, issue_dir: Path) -> int:
        if not self.postmatch_dir.exists():
            return 0
        main_names = {p.name for p in self.postmatch_dir.glob(f"{issue}_*_postmatch.md")}
        if not main_names:
            return 0

        duplicate_dir = self.legacy_dir / "Duplicate_Postmatch" / str(issue)
        duplicate_dir.mkdir(parents=True, exist_ok=True)
        moved = 0
        for p in issue_dir.rglob(f"{issue}_*_postmatch.md"):
            if p.name not in main_names:
                continue
            if p.resolve().parent == self.postmatch_dir.resolve():
                continue

            target = duplicate_dir / p.name
            if target.exists():
                stem = target.stem
                suffix = target.suffix
                target = duplicate_dir / f"{stem}__{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}{suffix}"
            p.rename(target)
            moved += 1
        return moved

    def _write_issue_readme(self, issue: str, issue_dirs: Dict[str, Path], manifest: Optional[Dict[str, Any]]) -> None:
        issue_dir = issue_dirs["issue_dir"]
        prematch_count = sum(1 for _ in issue_dirs["prematch_dir"].glob("*.md"))
        special_count = sum(1 for _ in issue_dirs["special_dir"].glob("*.md"))
        review_count = sum(1 for _ in issue_dirs["review_dir"].glob("*.md"))
        postmatch_legacy_count = sum(1 for _ in issue_dirs["postmatch_legacy_dir"].glob("*.md"))
        postmatch_count = sum(1 for _ in self.postmatch_dir.glob(f"{issue}_*_postmatch.md"))

        mapped = 0
        total = 0
        if isinstance(manifest, dict):
            matches = manifest.get("matches", [])
            if isinstance(matches, list):
                total = len(matches)
                for m in matches:
                    if m.get("understat_id") or m.get("fbref_url") or m.get("football_data_match_id"):
                        mapped += 1

        content = (
            f"# Audit Issue {issue}\n\n"
            f"- Updated At: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%SZ')}\n"
            f"- Mapping Progress: {mapped}/{total}\n"
            f"- Postmatch Main Index (`Postmatch_Telemetry`): {postmatch_count}\n\n"
            "## Sections\n"
            f"- `01_Prematch_Audits/`: {prematch_count}\n"
            f"- `02_Special_Analyses/`: {special_count}\n"
            f"- `03_Review_Reports/`: {review_count}\n"
            f"- `04_Postmatch_Legacy/`: {postmatch_legacy_count}\n"
        )
        (issue_dir / "README.md").write_text(content, encoding="utf-8")

    def _write_global_index(self) -> None:
        issue_dirs = sorted([p for p in self.audit_root.iterdir() if p.is_dir() and p.name.isdigit()], key=lambda x: x.name)
        lines: List[str] = []
        lines.append("# 审计文档导航（自动更新）")
        lines.append("")
        lines.append("## 当前结构")
        for name in ["00_Governance", "Postmatch_Telemetry", "02_Adhoc_Team_Audits", "99_Legacy_Archive"]:
            p = self.audit_root / name
            if p.exists():
                lines.append(f"- `{name}/`：{sum(1 for _ in p.rglob('*.md'))} 篇 md")
        for d in issue_dirs:
            lines.append(f"- `{d.name}/`：{sum(1 for _ in d.rglob('*.md'))} 篇 md")
        lines.append("")
        lines.append("## 按期号")
        for d in issue_dirs:
            lines.append(f"- `{d.name}/`：{sum(1 for _ in d.rglob('*.md'))} 篇")

        target = self.governance_dir / "INDEX - 审计文档导航.md"
        target.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def ensure_issue_governance(
        self,
        issue: str,
        manifest: Optional[Dict[str, Any]] = None,
        create_prematch_stubs: bool = True,
    ) -> bool:
        if not self.enabled:
            return False

        self._ensure_core_dirs()
        issue_dirs = self._ensure_issue_dirs(issue)

        created_stubs = 0
        if create_prematch_stubs and isinstance(manifest, dict):
            matches = manifest.get("matches", [])
            if isinstance(matches, list):
                created_stubs = self._ensure_prematch_stubs(issue, matches, issue_dirs["prematch_dir"])

        moved_duplicates = self._sync_duplicate_postmatch(issue, issue_dirs["issue_dir"])
        self._write_issue_readme(issue, issue_dirs, manifest)
        self._write_global_index()
        logger.info(
            "AuditRouter 更新完成 issue=%s, created_stubs=%s, moved_duplicate_postmatch=%s",
            issue,
            created_stubs,
            moved_duplicates,
        )
        return True


def _load_manifest_for_issue(vault_root: Path, issue: str) -> Optional[Dict[str, Any]]:
    manifest_path = vault_root / "04_RAG_Raw_Data" / "Cold_Data_Lake" / f"{issue}_dispatch_manifest.json"
    if not manifest_path.exists():
        return None
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ares Audit Router - Normalize 03_Match_Audits structure")
    parser.add_argument("--issue", type=str, required=True, help="中国体彩期号，如 26064")
    parser.add_argument(
        "--no-prematch-stubs",
        action="store_true",
        help="不自动生成 Prematch 骨架文档",
    )
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent.parent.parent
    load_dotenv_into_env(base_dir)
    router = AuditRouter(base_dir=base_dir)
    if not router.enabled:
        logger.error("未检测到 ARES_VAULT_PATH，无法执行审计路由。")
        raise SystemExit(1)

    manifest = _load_manifest_for_issue(router.vault_root, args.issue)
    router.ensure_issue_governance(
        issue=args.issue,
        manifest=manifest,
        create_prematch_stubs=not args.no_prematch_stubs,
    )
