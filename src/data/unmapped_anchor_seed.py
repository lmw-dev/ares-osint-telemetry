import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from audit_router import load_dotenv_into_env, normalize_vault_path


SMOKE_FBREF_PREFIX = "https://anchor.local/"


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_indices(text: str) -> List[int]:
    values: List[int] = []
    for part in str(text or "").split(","):
        raw = part.strip()
        if not raw:
            continue
        try:
            values.append(int(raw))
        except ValueError:
            continue
    return sorted(set(values))


def _resolve_anchor_file(vault_root: Path, issue: str) -> Path:
    review_dir = vault_root / "03_Match_Audits" / str(issue) / "03_Review_Reports"
    review_dir.mkdir(parents=True, exist_ok=True)
    editable = review_dir / f"UNMAPPED-ANCHORS-{issue}.json"
    generated = review_dir / f"UNMAPPED-ANCHORS-{issue}.generated.json"
    if editable.exists():
        return editable
    if generated.exists():
        payload = _load_json(generated)
        _save_json(editable, payload)
        return editable
    payload = {
        "issue": issue,
        "source": "unmapped_anchor_seed.py",
        "description": "Editable unmapped anchors.",
        "matches": [],
    }
    _save_json(editable, payload)
    return editable


def _has_anchor(item: Dict[str, Any]) -> bool:
    return bool(item.get("understat_id") or item.get("fbref_url") or item.get("football_data_match_id"))


def _now_utc_text() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")


def _looks_like_smoke_anchor(item: Dict[str, Any]) -> bool:
    mode = str(item.get("anchor_mode") or "").strip().lower()
    notes = str(item.get("notes") or "").strip().lower()
    fbref_url = str(item.get("fbref_url") or "").strip().lower()
    return mode == "smoke" or "[smoke]" in notes or fbref_url.startswith(SMOKE_FBREF_PREFIX)


def _seed_smoke_anchor(issue: str, index: int, english: str) -> Dict[str, Any]:
    safe_name = "".join(ch.lower() if ch.isalnum() else "-" for ch in english).strip("-")
    safe_name = safe_name or f"match-{index}"
    return {
        "understat_id": None,
        "fbref_url": f"{SMOKE_FBREF_PREFIX}{issue}/{index:02d}/{safe_name}",
        "football_data_match_id": None,
        "mapping_source": "manual_anchor",
        "anchor_mode": "smoke",
        "notes": "[smoke] synthetic anchor for pipeline testing",
    }


def _build_production_anchor(
    *,
    understat_id: Optional[str],
    fbref_url: Optional[str],
    football_data_match_id: Optional[str],
    notes: Optional[str],
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "understat_id": understat_id.strip() if understat_id else None,
        "fbref_url": fbref_url.strip() if fbref_url else None,
        "football_data_match_id": football_data_match_id.strip() if football_data_match_id else None,
        "mapping_source": "manual_anchor",
        "anchor_mode": "production",
        "notes": (notes or "manual production anchor override").strip(),
    }
    return payload


def run(
    *,
    issue: str,
    mode: str,
    smoke_count: int,
    indices: Optional[List[int]],
    clear_smoke: bool,
    allow_smoke: bool,
    production_understat_id: Optional[str],
    production_fbref_url: Optional[str],
    production_football_data_match_id: Optional[str],
    production_notes: Optional[str],
    force: bool,
) -> int:
    base_dir = Path(__file__).resolve().parent.parent.parent
    load_dotenv_into_env(base_dir)
    vault_env = os.getenv("ARES_VAULT_PATH")
    if not vault_env:
        raise EnvironmentError("未检测到 ARES_VAULT_PATH。")
    vault_root = Path(normalize_vault_path(vault_env)).expanduser()

    anchor_file = _resolve_anchor_file(vault_root, issue)
    payload = _load_json(anchor_file)
    matches = payload.get("matches")
    if not isinstance(matches, list):
        raise ValueError(f"锚点文件格式无效: {anchor_file}")

    smoke_updated = 0
    production_updated = 0
    cleared = 0
    for item in matches:
        if not isinstance(item, dict):
            continue
        if clear_smoke and _looks_like_smoke_anchor(item):
            item["fbref_url"] = None
            item["understat_id"] = None
            item["football_data_match_id"] = None
            item["mapping_source"] = "manual_anchor"
            item["anchor_mode"] = "production"
            item["notes"] = "Fill at least one anchor field to override unmapped status."
            cleared += 1

    target_indices = set(indices or [])
    remaining = max(0, smoke_count)

    if not clear_smoke and mode == "smoke":
        if not allow_smoke:
            raise ValueError("smoke 模式需要显式添加 `--allow-smoke`，防止误注入测试锚点。")
        for item in matches:
            if not isinstance(item, dict):
                continue
            idx = int(item.get("index") or 0)
            if target_indices and idx not in target_indices:
                continue
            if not target_indices and remaining <= 0:
                break
            if _has_anchor(item):
                continue
            anchor = _seed_smoke_anchor(issue, idx, str(item.get("english") or ""))
            item.update(anchor)
            smoke_updated += 1
            if not target_indices:
                remaining -= 1

    if not clear_smoke and mode == "production":
        has_production_anchor = bool(
            (production_understat_id and production_understat_id.strip())
            or (production_fbref_url and production_fbref_url.strip())
            or (production_football_data_match_id and production_football_data_match_id.strip())
        )
        if not has_production_anchor:
            raise ValueError(
                "production 模式必须至少提供一个真实锚点字段：`--understat-id` / `--fbref-url` / `--football-data-match-id`。"
            )
        if not target_indices:
            raise ValueError("production 模式必须通过 `--indices` 指定目标场次。")
        production_anchor = _build_production_anchor(
            understat_id=production_understat_id,
            fbref_url=production_fbref_url,
            football_data_match_id=production_football_data_match_id,
            notes=production_notes,
        )
        for item in matches:
            if not isinstance(item, dict):
                continue
            idx = int(item.get("index") or 0)
            if idx not in target_indices:
                continue
            if _has_anchor(item) and not force:
                continue
            item.update(production_anchor)
            production_updated += 1

    payload["source"] = "unmapped_anchor_seed.py"
    payload["updated_at"] = _now_utc_text()
    payload["mode"] = mode
    _save_json(anchor_file, payload)
    print(f"anchor_file={anchor_file}")
    print(f"mode={mode}")
    print(f"smoke_updated={smoke_updated}")
    print(f"production_updated={production_updated}")
    print(f"cleared={cleared}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed unmapped anchors (smoke/production).")
    parser.add_argument("--issue", required=True, help="体彩期号，如 26066")
    parser.add_argument(
        "--mode",
        choices=["smoke", "production"],
        default="production",
        help="smoke=注入合成锚点，仅回归测试用；production=写入真实人工锚点。",
    )
    parser.add_argument(
        "--allow-smoke",
        action="store_true",
        help="仅 smoke 模式使用；必须显式确认才允许注入测试锚点。",
    )
    parser.add_argument("--smoke-count", type=int, default=3, help="自动注入 smoke 锚点数量（默认 3）")
    parser.add_argument("--indices", type=str, default="", help="指定 index 列表，如 2,3,4；优先于 smoke-count")
    parser.add_argument("--clear-smoke", action="store_true", help="清理 [smoke] 锚点并回退为空")
    parser.add_argument("--understat-id", type=str, default="", help="production 模式：写入 understat_id")
    parser.add_argument("--fbref-url", type=str, default="", help="production 模式：写入 fbref_url")
    parser.add_argument("--football-data-match-id", type=str, default="", help="production 模式：写入 football_data_match_id")
    parser.add_argument("--notes", type=str, default="", help="production 模式：写入 notes")
    parser.add_argument("--force", action="store_true", help="production 模式：覆盖已存在锚点")
    args = parser.parse_args()
    return run(
        issue=args.issue,
        mode=args.mode,
        smoke_count=args.smoke_count,
        indices=_parse_indices(args.indices),
        clear_smoke=args.clear_smoke,
        allow_smoke=args.allow_smoke,
        production_understat_id=args.understat_id,
        production_fbref_url=args.fbref_url,
        production_football_data_match_id=args.football_data_match_id,
        production_notes=args.notes,
        force=args.force,
    )


if __name__ == "__main__":
    raise SystemExit(main())
