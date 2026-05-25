import asyncio
import sys
from pathlib import Path

_proj_root = Path(__file__).resolve().parent.parent
if str(_proj_root) not in sys.path:
    sys.path.insert(0, str(_proj_root))
_src_dir = _proj_root / "src"
if str(_src_dir) not in sys.path:
    sys.path.insert(0, str(_src_dir))

from utils.scraper_v2 import fetch_html_via_playwright_v2
from bs4 import BeautifulSoup

async def test_url(url, name):
    print(f"\n--- Testing {name}: {url} ---")
    try:
        html = await fetch_html_via_playwright_v2(url)
        print("HTML length:", len(html))
        
        soup = BeautifulSoup(html, "html.parser")
        tables = soup.select("table")
        print("Total tables found:", len(tables))
        
        # 看看有没有 table_live 或者是 tr
        table_live = soup.select("table#table_live")
        print("table#table_live found:", len(table_live))
        
        trs = soup.select("tr")
        print("Total trs found:", len(trs))
        
        tr_matches = soup.select("tr[id^='tr1_']")
        print("tr[id^='tr1_'] count:", len(tr_matches))
        
        # 打印前 5 个比赛 tr 的 sid 属性和文本
        for tr in tr_matches[:5]:
            sid = tr.get("sid") or tr.get("sId")
            tds = tr.find_all("td")
            td_txts = [td.get_text(strip=True) for td in tds[:6]]
            print(f"  Match sid={sid}, tds={td_txts}")
            
    except Exception as e:
        print(f"Failed to fetch/parse {name}: {e}")

async def main():
    # 测试 index.htm 和 next.htm
    await test_url("https://bf.titan007.com/index.htm", "IndexPage")
    await test_url("https://bf.titan007.com/football/next.htm", "NextPage")

if __name__ == "__main__":
    asyncio.run(main())
