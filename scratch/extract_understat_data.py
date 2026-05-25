import requests
from bs4 import BeautifulSoup
import re

url = "https://understat.com/league/Serie_A"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

resp = requests.get(url, headers=headers)
html = resp.text

soup = BeautifulSoup(html, 'html.parser')
scripts = soup.find_all('script')

print(f"Found {len(scripts)} script tags.")
for idx, script in enumerate(scripts):
    content = script.text or ''
    if 'Lazio' in content or 'matchesData' in content or 'Pisa' in content:
        print(f"Script tag {idx} matches criteria. Length: {len(content)}")
        print("First 200 chars:", content[:200].strip())
        matches = re.findall(r"JSON\.parse\('([^']+)'\)", content)
        for jdx, m in enumerate(matches):
            try:
                decoded = m.encode("utf-8").decode("unicode_escape")
                print(f"  Match {jdx} decoded length: {len(decoded)}")
                if 'Lazio' in decoded:
                    print("  Found 'Lazio' in decoded JSON!")
                    # Let's search inside the decoded JSON string directly for Pisa or Lazio matches!
                    import json
                    matches_list = json.loads(decoded)
                    print(f"  Loaded {len(matches_list)} matches from decoded list.")
                    for match_item in matches_list:
                        h_team = match_item.get('h', {}).get('title')
                        a_team = match_item.get('a', {}).get('title')
                        is_result = match_item.get('isResult')
                        if h_team in ('Lazio', 'Pisa') or a_team in ('Lazio', 'Pisa'):
                            print(f"    * MATCH FOUND * ID: {match_item.get('id')} | {h_team} vs {a_team} | Date: {match_item.get('datetime')} | Result: {match_item.get('goals', {}).get('h')}-{match_item.get('goals', {}).get('a')} | Completed: {is_result}")
            except Exception as e:
                print("  Failed to decode match:", e)
