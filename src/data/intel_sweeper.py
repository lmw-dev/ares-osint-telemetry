import argparse
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests
import yaml
from bs4 import BeautifulSoup


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("AresTelemetry.IntelSweeper")


INTEL_BASE_DEFAULTS: Dict[str, Any] = {
    "manager_doctrine": "Unknown",
    "market_sentiment": "Neutral",
    "key_node_dependency": [],
    "recent_news_summary": "",
}

NEGATIVE_KEYWORDS = ("injury", "crisis")
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


def split_frontmatter(content: str) -> Tuple[Dict[str, Any], str]:
    if content.startswith("---\n"):
        closing_marker_index = content.find("\n---\n", 4)
        if closing_marker_index != -1:
            yaml_text = content[4:closing_marker_index]
            body = content[closing_marker_index + len("\n---\n") :]
            parsed = yaml.safe_load(yaml_text) or {}
            if not isinstance(parsed, dict):
                raise ValueError("frontmatter 结构非法：必须是 YAML 对象")
            return parsed, body
    return {}, content


def build_markdown(frontmatter: Dict[str, Any], body: str) -> str:
    yaml_text = yaml.safe_dump(frontmatter, allow_unicode=True, sort_keys=False)
    return f"---\n{yaml_text}---\n{body}"


def write_text_safely(target_path: Path, content: str) -> None:
    temp_path = target_path.with_suffix(target_path.suffix + ".tmp")
    temp_path.write_text(content, encoding="utf-8")
    temp_path.replace(target_path)


def fetch_article_from_url(url: str, timeout: int = 12) -> Dict[str, str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
    }
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    html = response.text

    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else "Untitled"
    paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
    text = "\n".join([p for p in paragraphs if p])
    if not text:
        text = soup.get_text(" ", strip=True)

    return {"title": title, "text": text, "source": url}


def analyze_sentiment(text: str) -> str:
    lowered = text.lower()
    if any(keyword in lowered for keyword in NEGATIVE_KEYWORDS):
        return "Pessimistic"
    return "Neutral"


def summarize_core_sentence(text: str) -> str:
    normalized = re.sub(r"\s+", " ", text).strip()
    if not normalized:
        return ""

    candidates = re.split(r"(?<=[.!?。！？])\s+", normalized)
    for sentence in candidates:
        s = sentence.strip()
        if len(s) >= 20:
            return s[:220]
    return normalized[:220]


def dump_cold_article(
    raw_dir: Path,
    team: str,
    league: str,
    idx: int,
    article: Dict[str, str],
) -> Path:
    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"{stamp}-{league}-{team}-Intel-{idx:02d}.md"
    target = raw_dir / filename

    payload = (
        f"# {article.get('title', 'Untitled')}\n\n"
        f"- team: {team}\n"
        f"- league: {league}\n"
        f"- source: {article.get('source', 'manual_input')}\n"
        f"- fetched_at_utc: {stamp}\n\n"
        f"## Full Text\n\n"
        f"{article.get('text', '').strip()}\n"
    )
    write_text_safely(target, payload)
    return target


def backfill_team_archive(
    archive_path: Path,
    market_sentiment: str,
    recent_news_summary: str,
) -> None:
    if not archive_path.exists():
        raise FileNotFoundError(f"球队档案不存在: {archive_path}")

    content = archive_path.read_text(encoding="utf-8")
    frontmatter, body = split_frontmatter(content)

    intel_base = frontmatter.get("intel_base")
    if not isinstance(intel_base, dict):
        intel_base = {}

    merged_intel_base = {**INTEL_BASE_DEFAULTS, **intel_base}
    merged_intel_base["market_sentiment"] = market_sentiment
    merged_intel_base["recent_news_summary"] = recent_news_summary
    frontmatter["intel_base"] = merged_intel_base

    rebuilt = build_markdown(frontmatter, body)
    write_text_safely(archive_path, rebuilt)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Nightly intel sweeper with team archive back-filling."
    )
    parser.add_argument("--team", required=True, help="Team name, e.g. Arsenal")
    parser.add_argument("--league", required=True, help="League name, e.g. EPL")
    parser.add_argument(
        "--url",
        action="append",
        default=[],
        help="News URL. Can be used multiple times.",
    )
    parser.add_argument(
        "--text",
        action="append",
        default=[],
        help="Manual news text input. Can be used multiple times.",
    )
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
    team = sanitize_segment(args.team, "team")
    league = sanitize_segment(args.league, "league")

    raw_dir = vault_root / "04_RAG_Raw_Data"
    raw_dir.mkdir(parents=True, exist_ok=True)

    archive_path = vault_root / "02_Team_Archives" / league / f"{team}.md"

    articles: List[Dict[str, str]] = []

    for url in args.url:
        try:
            article = fetch_article_from_url(url)
            articles.append(article)
            logger.info("抓取新闻成功: %s", url)
        except Exception as exc:
            logger.warning("抓取新闻失败，已跳过 %s -> %s", url, exc)

    for idx, text in enumerate(args.text, start=1):
        cleaned_text = text.strip()
        if not cleaned_text:
            continue
        articles.append(
            {
                "title": f"Manual Intel #{idx}",
                "text": cleaned_text,
                "source": "manual_input",
            }
        )

    if not articles:
        raise ValueError("没有可处理的新闻内容，请至少传入一个 --url 或 --text。")

    cold_refs: List[Path] = []
    for idx, article in enumerate(articles, start=1):
        cold_path = dump_cold_article(raw_dir, team, league, idx, article)
        cold_refs.append(cold_path)

    combined_text = "\n".join(article["text"] for article in articles if article.get("text"))
    sentiment = analyze_sentiment(combined_text)
    summary = summarize_core_sentence(combined_text)

    backfill_team_archive(
        archive_path=archive_path,
        market_sentiment=sentiment,
        recent_news_summary=summary,
    )

    logger.info("冷数据已写入 %d 份 -> %s", len(cold_refs), raw_dir)
    logger.info("球队档案已回填 -> %s", archive_path)
    logger.info("回填结果：market_sentiment=%s", sentiment)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
