import argparse
import logging
import os
import re
from pathlib import Path
from typing import Dict, Any

import yaml


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("AresTelemetry.TeamForge")


DEFAULT_FRONTMATTER: Dict[str, Any] = {
    "intel_base": {
        "manager_doctrine": "Unknown",
        "market_sentiment": "Neutral",
        "key_node_dependency": [],
        "recent_news_summary": "",
    },
    "physical_reality": {
        "avg_xG_last_5": 1.0,
        "conversion_efficiency": 0.05,
        "defensive_leakage": 0.5,
        "variance_history": [],
        "actual_tactical_entropy": 0.40,
    },
    "reality_gap": {
        "bias_type": "Aligned",
        "S_dynamic_modifier": 0.0,
    },
}


DEFAULT_BODY = (
    "## Team Notes\n\n"
    "- Baseline profile initialized by `team_forge.py`.\n"
    "- Add tactical observations, injury patterns, and review snapshots below.\n"
)

_INVALID_SEGMENT_PATTERN = re.compile(r'[<>:"/\\|?*\x00-\x1F]+')


def load_dotenv_into_env(base_dir: Path) -> None:
    env_path = base_dir / ".env"
    if not env_path.exists():
        return

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


def normalize_vault_path(path_text: str) -> Path:
    normalized = str(path_text).replace("\\ ", " ").replace("\\~", "~")
    return Path(normalized).expanduser()


def sanitize_segment(value: str, field_name: str) -> str:
    cleaned = value.strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = _INVALID_SEGMENT_PATTERN.sub("_", cleaned)
    cleaned = cleaned.strip(" .")

    if not cleaned:
        raise ValueError(f"{field_name} 不能为空或仅包含非法字符")
    if cleaned in {".", ".."}:
        raise ValueError(f"{field_name} 非法：不能为 . 或 ..")
    return cleaned


def read_existing_body(target_path: Path) -> str:
    if not target_path.exists():
        return DEFAULT_BODY

    content = target_path.read_text(encoding="utf-8")
    if not content.strip():
        return DEFAULT_BODY

    if content.startswith("---\n"):
        closing_marker_index = content.find("\n---\n", 4)
        if closing_marker_index != -1:
            existing_body = content[closing_marker_index + len("\n---\n") :]
            return existing_body.lstrip("\n") or DEFAULT_BODY

    return content


def build_markdown(frontmatter: Dict[str, Any], body: str) -> str:
    yaml_text = yaml.safe_dump(frontmatter, allow_unicode=True, sort_keys=False)
    body_text = body.rstrip() + "\n"
    return f"---\n{yaml_text}---\n\n{body_text}"


def write_markdown_safely(target_path: Path, content: str) -> None:
    temp_path = target_path.with_suffix(target_path.suffix + ".tmp")
    temp_path.write_text(content, encoding="utf-8")
    temp_path.replace(target_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Initialize or upgrade team archive Markdown with v4.2 schema."
    )
    parser.add_argument("--team", required=True, help="Team name, e.g. Arsenal")
    parser.add_argument("--league", required=True, help="League name, e.g. EPL")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_dir = Path(__file__).resolve().parent.parent.parent
    load_dotenv_into_env(base_dir)

    vault_env = os.getenv("ARES_VAULT_PATH")
    if not vault_env:
        raise EnvironmentError(
            "未检测到环境变量 ARES_VAULT_PATH，请先在 shell 或 .env 中配置后再执行。"
        )

    vault_root = normalize_vault_path(vault_env)
    team_name = sanitize_segment(args.team, "team")
    league_name = sanitize_segment(args.league, "league")

    archive_dir = vault_root / "02_Team_Archives" / league_name
    archive_dir.mkdir(parents=True, exist_ok=True)
    target_path = archive_dir / f"{team_name}.md"

    body = read_existing_body(target_path)
    markdown_content = build_markdown(DEFAULT_FRONTMATTER, body)
    write_markdown_safely(target_path, markdown_content)

    logger.info("球队档案写入完成 -> %s", target_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
