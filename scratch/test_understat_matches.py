import sys
from pathlib import Path

# Add project src to sys.path
_proj_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_proj_root / "src"))
sys.path.insert(0, str(_proj_root / "src" / "data"))

from osint_crawler import AresOsintCrawler

crawler = AresOsintCrawler(date_source="understat", analysis_date="20260524", scope="top5")

# Let's inspect what understat db rows exist for year = 2025 (which represents 2025/2026 season)
rows = crawler.build_understat_db(year=2025)
print("Total rows fetched from Understat for year 2025:", len(rows) if rows else 0)

print("\nMatches scheduled around May 24th - 25th, 2026 in Understat:")
for row in rows:
    raw_date = row.get("date", "")
    if "2026-05-24" in raw_date or "2026-05-25" in raw_date or "2026-05-23" in raw_date:
        print(f"[{row.get('league')}] {row.get('home_en')} vs {row.get('away_en')} - ID: {row.get('id')} - Date: {raw_date}")
