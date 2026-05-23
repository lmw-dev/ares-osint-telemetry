import asyncio
import sys
from pathlib import Path

# Add project src to sys.path
_proj_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_proj_root / "src"))

from utils.scraper_v2 import fetch_html_via_playwright_v2

async def main():
    # Use a real page to test
    url = "https://zq.titan007.com/analysis/231217cn.htm"
    print(f"Testing ScraperV2 with URL: {url}")
    
    try:
        html = await fetch_html_via_playwright_v2(url, headless=True)
        print("Success! HTML Content Length:", len(html))
        print("HTML starts with:")
        print(html[:200])
        
        # Verify if it contains key terms
        if "table" in html.lower():
            print("Verified: HTML contains table tags.")
        else:
            print("Warning: HTML does not seem to contain table tags.")
            
    except Exception as e:
        print("ScraperV2 test failed with exception:", e)

if __name__ == "__main__":
    asyncio.run(main())
