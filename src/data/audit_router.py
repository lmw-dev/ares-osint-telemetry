import argparse
import json
import logging
import os
import re
import unicodedata
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


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
        alias_path = self.base_dir / "src" / "data" / "team_alias_map.json"
        try:
            self.team_alias = json.loads(alias_path.read_text(encoding="utf-8"))
        except Exception:
            self.team_alias = {}
        self._team_patterns = self._build_team_patterns()

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

    @staticmethod
    def _contains_cjk(value: str) -> bool:
        return bool(re.search(r"[\u3400-\u9fff]", str(value)))

    @staticmethod
    def _looks_placeholder(value: str) -> bool:
        txt = str(value).strip()
        if not txt:
            return True
        return bool(re.fullmatch(r"(Home|Away|Match)\d+", txt, flags=re.IGNORECASE))

    @staticmethod
    def _normalize_name_for_score(value: str) -> str:
        ascii_name = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
        return re.sub(r"[^a-z0-9]+", "", ascii_name.strip().lower())

    def _canonical_team_name(self, value: str) -> str:
        txt = self._safe_str(value).strip().replace("_", " ").replace("-", " ")
        if self._contains_cjk(txt):
            txt = self._translate_zh_team(txt)
        normalized = self._normalize_name_for_score(txt)
        return normalized or self._normalize_name_for_score(value)

    def _build_team_patterns(self) -> List[Tuple[str, str, bool]]:
        patterns: Dict[Tuple[str, str], bool] = {}

        def register(pattern: str, canonical: str) -> None:
            txt = self._safe_str(pattern).strip()
            if not txt or not canonical:
                return
            patterns[(txt.lower(), canonical)] = self._contains_cjk(txt)

        for alias, english in self.team_alias.items():
            canonical = self._canonical_team_name(english)
            for candidate in {
                alias,
                english,
                english.replace("_", " "),
                english.replace("-", " "),
            }:
                register(candidate, canonical)

        ordered = sorted(patterns.items(), key=lambda item: len(item[0][0]), reverse=True)
        return [(pattern, canonical, has_cjk) for (pattern, canonical), has_cjk in ordered]

    def _display_team_name(self, canonical: str) -> str:
        for english in self.team_alias.values():
            if self._canonical_team_name(english) == canonical:
                return english
        return canonical

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

    def _split_pair_text(self, value: str) -> Tuple[str, str]:
        txt = self._safe_str(value)
        for token in [" vs ", " VS ", "vs", "VS"]:
            if token in txt:
                home, away = txt.split(token, 1)
                return home.strip(), away.strip()
        return txt.strip(), ""

    def _translate_zh_team(self, value: str) -> str:
        txt = self._safe_str(value).strip()
        return self._safe_str(self.team_alias.get(txt, txt))

    def _resolve_match_names(self, match: Dict[str, Any], index: int) -> Tuple[str, str]:
        english_home, english_away = self._split_pair_text(match.get("english", ""))
        chinese_home, chinese_away = self._split_pair_text(match.get("chinese", ""))

        if self._looks_placeholder(english_home) or self._contains_cjk(english_home):
            translated = self._translate_zh_team(chinese_home)
            english_home = translated if translated and not self._contains_cjk(translated) else english_home
        if self._looks_placeholder(english_away) or self._contains_cjk(english_away):
            translated = self._translate_zh_team(chinese_away)
            english_away = translated if translated and not self._contains_cjk(translated) else english_away

        if not english_home or self._looks_placeholder(english_home):
            english_home = f"Home{index:02d}"
        if not english_away or self._looks_placeholder(english_away):
            english_away = f"Away{index:02d}"

        return english_home, english_away

    def _build_stub_content(self, issue: str, match: Dict[str, Any], english_home: str, english_away: str) -> str:
        chinese = self._safe_str(match.get("chinese"))
        english = f"{english_home} vs {english_away}"
        return (
            "---\n"
            f'issue: "{issue}"\n'
            f'match_index: {int(match.get("index", 0) or 0)}\n'
            f'chinese: "{chinese.replace(chr(34), chr(39))}"\n'
            f'english: "{english.replace(chr(34), chr(39))}"\n'
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

    @staticmethod
    def _is_generated_prematch_stub(path: Path) -> bool:
        try:
            content = path.read_text(encoding="utf-8")
        except Exception:
            return False
        return content.startswith("---\n") and 'status: "draft"' in content and "## Prematch Audit" in content

    def _archive_prematch_duplicate(self, issue: str, path: Path) -> None:
        duplicate_dir = self.legacy_dir / "Duplicate_Prematch" / str(issue)
        duplicate_dir.mkdir(parents=True, exist_ok=True)
        target = duplicate_dir / path.name
        if target.exists():
            stem = target.stem
            target = duplicate_dir / f"{stem}__{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}{target.suffix}"
        path.rename(target)

    @staticmethod
    def _strip_host_suffix(stem: str) -> str:
        if stem.endswith("_Host"):
            return stem[:-5]
        return stem

    def _parse_expected_teams(self, path: Path, text: str) -> Set[str]:
        expected: Set[str] = set()

        filename_match = re.match(r"^Audit-\d+-\d+-(.+)-vs-(.+?)(?:_Host)?$", path.stem)
        if filename_match:
            expected.add(self._canonical_team_name(filename_match.group(1)))
            expected.add(self._canonical_team_name(filename_match.group(2)))

        english_match = re.search(r'english:\s*"([^"]+)"', text)
        if english_match:
            home, away = self._split_pair_text(english_match.group(1))
            expected.add(self._canonical_team_name(home))
            expected.add(self._canonical_team_name(away))

        title_match = re.search(r"^# Ares Prematch Audit - Issue \d+ - (.+)$", text, flags=re.MULTILINE)
        if title_match:
            home, away = self._split_match_english(title_match.group(1))
            expected.add(self._canonical_team_name(home))
            expected.add(self._canonical_team_name(away))

        return {team for team in expected if team}

    def _detect_cross_team_contamination(self, path: Path, text: str) -> List[str]:
        expected = self._parse_expected_teams(path, text)
        if not expected:
            return []

        normalized_text = text.lower()
        matched: Set[str] = set()
        for pattern, canonical, has_cjk in self._team_patterns:
            if canonical in expected:
                continue
            if has_cjk:
                if pattern in text:
                    matched.add(canonical)
            else:
                if pattern in normalized_text:
                    matched.add(canonical)

        man_city = self._canonical_team_name("Manchester City")
        if man_city not in expected and "rodri" in normalized_text:
            matched.add(man_city)

        return sorted(matched)

    @staticmethod
    def _has_insufficient_resilience_data(text: str) -> bool:
        markers = (
            "RAG 库逆境样本不足",
            "[Unknown: Insufficient Resilience Data]",
            "[HALT] RAG 库逆境样本不足",
            "停机",
        )
        return any(marker in text for marker in markers)

    @staticmethod
    def _extract_numeric_marker(text: str, label: str) -> Optional[float]:
        match = re.search(rf"{re.escape(label)}\**:\s*`?([0-9.]+)`?", text)
        if not match:
            return None
        try:
            return float(match.group(1))
        except ValueError:
            return None

    def _assess_report_quality(self, path: Path) -> Dict[str, Any]:
        try:
            text = path.read_text(encoding="utf-8")
        except Exception:
            return {"path": path, "status": "reject", "reasons": ["unreadable"], "text": ""}

        if self._is_generated_prematch_stub(path):
            return {"path": path, "status": "reject", "reasons": ["draft_stub"], "text": text}

        reasons: List[str] = []
        if self._has_insufficient_resilience_data(text):
            reasons.append("insufficient_resilience_data")

        overall_resilience = self._extract_numeric_marker(text, "整体韧性评分")
        if overall_resilience is None:
            overall_resilience = self._extract_numeric_marker(text, "整体韧性")
        if "insufficient_resilience_data" in reasons or overall_resilience == 0.0:
            reasons.append("low_confidence")

        contaminated_teams = self._detect_cross_team_contamination(path, text)
        if contaminated_teams:
            reasons.append("cross_team_contamination")

        return {
            "path": path,
            "status": "reject" if reasons else "accept",
            "reasons": sorted(set(reasons)),
            "text": text,
            "contaminated_teams": contaminated_teams,
        }

    @staticmethod
    def _reason_label(reason: str) -> str:
        mapping = {
            "draft_stub": "Draft Stub",
            "insufficient_resilience_data": "Insufficient Resilience Data",
            "low_confidence": "Low Confidence",
            "cross_team_contamination": "Cross-Team Contamination",
            "unreadable": "Unreadable",
        }
        return mapping.get(reason, reason)

    def _build_rejected_review_content(self, issue: str, assessment: Dict[str, Any]) -> str:
        path = assessment["path"]
        reasons = assessment.get("reasons", [])
        lines = [
            f"# Rejected Prematch Audit - {path.name}",
            "",
            f"- Issue: `{issue}`",
            f"- Rejected At: `{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%SZ')}`",
            f"- Source File: `01_Prematch_Audits/{path.name}`",
            f"- Reject Reasons: {', '.join(f'`{self._reason_label(reason)}`' for reason in reasons) if reasons else '`Unknown`'}",
        ]

        contaminated_teams = assessment.get("contaminated_teams") or []
        if contaminated_teams:
            lines.append(
                "- Foreign Team Signals: "
                + ", ".join(f"`{self._display_team_name(team)}`" for team in contaminated_teams)
            )

        lines.extend(
            [
                "",
                "## Original Content",
                "",
                assessment.get("text", "").rstrip(),
                "",
            ]
        )
        return "\n".join(lines)

    def _gate_prematch_reports(self, issue: str, prematch_dir: Path, review_dir: Path) -> Dict[str, List[str]]:
        rejected: Dict[str, List[str]] = defaultdict(list)
        for path in sorted(prematch_dir.glob("Audit-*.md")):
            assessment = self._assess_report_quality(path)
            if assessment["status"] != "reject":
                continue

            target = review_dir / f"REJECTED-{path.name}"
            target.write_text(self._build_rejected_review_content(issue, assessment), encoding="utf-8")
            path.unlink()
            for reason in assessment.get("reasons", []):
                rejected[reason].append(path.name)
        return {reason: sorted(names) for reason, names in rejected.items()}

    def _sync_prematch_stubs(
        self,
        issue: str,
        matches: List[Dict[str, Any]],
        prematch_dir: Path,
        review_dir: Path,
    ) -> Tuple[int, int]:
        created = 0
        archived = 0
        for match in matches:
            index = int(match.get("index", 0) or 0)
            if index <= 0:
                continue

            prefix = f"Audit-{issue}-{index:02d}-"
            english_home, english_away = self._resolve_match_names(match, index)
            home_safe = self._sanitize_segment(english_home, f"Home{index:02d}")
            away_safe = self._sanitize_segment(english_away, f"Away{index:02d}")
            canonical_name = f"{prefix}{home_safe}-vs-{away_safe}.md"
            canonical_path = prematch_dir / canonical_name
            rejected_review_path = review_dir / f"REJECTED-{canonical_name}"
            matching = sorted(prematch_dir.glob(f"{prefix}*.md"))
            stub_files = [p for p in matching if self._is_generated_prematch_stub(p)]
            real_reports = [p for p in matching if p not in stub_files]

            if real_reports:
                for p in stub_files:
                    self._archive_prematch_duplicate(issue, p)
                    archived += 1
                continue

            if rejected_review_path.exists():
                for p in stub_files:
                    self._archive_prematch_duplicate(issue, p)
                    archived += 1
                continue

            if not stub_files:
                canonical_path.write_text(
                    self._build_stub_content(issue, match, english_home, english_away),
                    encoding="utf-8",
                )
                created += 1
                continue

            chosen = None
            for p in stub_files:
                if p.name == canonical_name:
                    chosen = p
                    break
            if chosen is None:
                chosen = stub_files[0]

            for p in stub_files:
                if p == chosen:
                    continue
                self._archive_prematch_duplicate(issue, p)
                archived += 1

            chosen.write_text(
                self._build_stub_content(issue, match, english_home, english_away),
                encoding="utf-8",
            )
            if chosen.name != canonical_name:
                if canonical_path.exists():
                    self._archive_prematch_duplicate(issue, chosen)
                    archived += 1
                else:
                    chosen.rename(canonical_path)
        return created, archived

    def _sync_real_prematch_duplicates(self, issue: str, prematch_dir: Path) -> int:
        grouped: Dict[str, List[Path]] = {}
        for path in sorted(prematch_dir.glob("Audit-*.md")):
            if self._is_generated_prematch_stub(path):
                continue
            base_key = self._strip_host_suffix(path.stem)
            grouped.setdefault(base_key, []).append(path)

        archived = 0
        for paths in grouped.values():
            if len(paths) < 2:
                continue

            canonical = next((p for p in paths if not p.stem.endswith("_Host")), None)
            host_variant = next((p for p in paths if p.stem.endswith("_Host")), None)
            if canonical is None or host_variant is None:
                continue

            self._archive_prematch_duplicate(issue, host_variant)
            archived += 1
        return archived

    def _build_quality_findings(self, prematch_dir: Path, review_dir: Path) -> Dict[str, List[str]]:
        findings = {
            "accepted": [],
            "rejected": [],
            "drafts": [],
            "low_confidence": [],
            "insufficient_resilience_data": [],
            "cross_team_contamination": [],
        }
        for path in sorted(prematch_dir.glob("Audit-*.md")):
            findings["accepted"].append(path.name)
        for path in sorted(review_dir.glob("REJECTED-Audit-*.md")):
            name = path.name.replace("REJECTED-", "", 1)
            findings["rejected"].append(name)
            try:
                text = path.read_text(encoding="utf-8")
            except Exception:
                continue

            if "`Draft Stub`" in text:
                findings["drafts"].append(name)
            if "`Low Confidence`" in text:
                findings["low_confidence"].append(name)
            if "`Insufficient Resilience Data`" in text:
                findings["insufficient_resilience_data"].append(name)
            if "`Cross-Team Contamination`" in text:
                findings["cross_team_contamination"].append(name)
        return findings

    def _write_review_report(self, issue: str, review_dir: Path, prematch_dir: Path) -> None:
        findings = self._build_quality_findings(prematch_dir, review_dir)
        blocker_path = review_dir / f"REVIEW-{issue}-Prematch_Blocker.md"
        lines: List[str] = []
        lines.append(f"# Review {issue} - Prematch Data Quality")
        lines.append("")
        lines.append(f"- Updated At: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%SZ')}")
        lines.append(f"- Prematch Blocker Active: {'Yes' if blocker_path.exists() else 'No'}")
        lines.append(f"- Accepted Prematch Reports: {len(findings['accepted'])}")
        lines.append(f"- Rejected Prematch Reports: {len(findings['rejected'])}")
        lines.append(f"- Draft Stubs: {len(findings['drafts'])}")
        lines.append(f"- Low Confidence Reports: {len(findings['low_confidence'])}")
        lines.append(f"- Insufficient Resilience Data: {len(findings['insufficient_resilience_data'])}")
        lines.append(f"- Cross-Team Contamination: {len(findings['cross_team_contamination'])}")
        lines.append("")

        if blocker_path.exists():
            lines.append("## Active Blocker")
            lines.append(f"- `REVIEW-{issue}-Prematch_Blocker.md`")
            lines.append("")

        lines.append("## Accepted Prematch Reports")
        if findings["accepted"]:
            lines.extend(f"- `{name}`" for name in findings["accepted"])
        else:
            lines.append("- None")
        lines.append("")

        lines.append("## Rejected Prematch Reports")
        if findings["rejected"]:
            lines.extend(f"- `{name}`" for name in findings["rejected"])
        else:
            lines.append("- None")
        lines.append("")

        lines.append("## Draft Stubs")
        if findings["drafts"]:
            lines.extend(f"- `{name}`" for name in findings["drafts"])
        else:
            lines.append("- None")
        lines.append("")

        lines.append("## Low Confidence Reports")
        if findings["low_confidence"]:
            lines.extend(f"- `{name}`" for name in findings["low_confidence"])
        else:
            lines.append("- None")
        lines.append("")

        lines.append("## Insufficient Resilience Data")
        if findings["insufficient_resilience_data"]:
            lines.extend(f"- `{name}`" for name in findings["insufficient_resilience_data"])
        else:
            lines.append("- None")
        lines.append("")

        lines.append("## Cross-Team Contamination")
        if findings["cross_team_contamination"]:
            lines.extend(f"- `{name}`" for name in findings["cross_team_contamination"])
        else:
            lines.append("- None")
        lines.append("")

        target = review_dir / f"REVIEW-{issue}-Prematch_Data_Quality.md"
        target.write_text("\n".join(lines), encoding="utf-8")

    def write_prematch_blocker_report(
        self,
        issue: str,
        blocker_type: str,
        summary: str,
        details: List[str],
    ) -> Optional[Path]:
        if not self.enabled:
            return None

        issue_dirs = self._ensure_issue_dirs(issue)
        lines = [
            f"# Review {issue} - Prematch Blocker",
            "",
            f"- Updated At: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%SZ')}",
            f"- Blocker Type: `{blocker_type}`",
            f"- Summary: {summary}",
            "",
            "## Details",
        ]
        if details:
            lines.extend(f"- {detail}" for detail in details)
        else:
            lines.append("- None")
        lines.append("")

        target = issue_dirs["review_dir"] / f"REVIEW-{issue}-Prematch_Blocker.md"
        target.write_text("\n".join(lines), encoding="utf-8")
        return target

    def clear_prematch_blocker_report(self, issue: str) -> None:
        if not self.enabled:
            return
        blocker_path = self._ensure_issue_dirs(issue)["review_dir"] / f"REVIEW-{issue}-Prematch_Blocker.md"
        if blocker_path.exists():
            blocker_path.unlink()

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
        archived_prematch_duplicates = 0
        if create_prematch_stubs and isinstance(manifest, dict):
            matches = manifest.get("matches", [])
            if isinstance(matches, list):
                created_stubs, archived_prematch_duplicates = self._sync_prematch_stubs(
                    issue,
                    matches,
                    issue_dirs["prematch_dir"],
                    issue_dirs["review_dir"],
                )

        archived_prematch_duplicates += self._sync_real_prematch_duplicates(issue, issue_dirs["prematch_dir"])
        rejected_reports = self._gate_prematch_reports(issue, issue_dirs["prematch_dir"], issue_dirs["review_dir"])

        moved_duplicates = self._sync_duplicate_postmatch(issue, issue_dirs["issue_dir"])
        self._write_review_report(issue, issue_dirs["review_dir"], issue_dirs["prematch_dir"])
        self._write_issue_readme(issue, issue_dirs, manifest)
        self._write_global_index()
        logger.info(
            "AuditRouter 更新完成 issue=%s, created_stubs=%s, archived_prematch_duplicates=%s, rejected_prematch=%s, moved_duplicate_postmatch=%s",
            issue,
            created_stubs,
            archived_prematch_duplicates,
            len({name for names in rejected_reports.values() for name in names}),
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
