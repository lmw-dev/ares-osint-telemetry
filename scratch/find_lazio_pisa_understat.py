import requests
import re
import json

url = "https://understat.com/league/Serie_A/2025"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

resp = requests.get(url, headers=headers)
html = resp.text

# Understat stores match data in a JS variable: var matchesData = JSON.parse('...');
match = re.search(r"var matchesData\s*=\s*JSON\.parse\('([^']+)'\)", html)
if not match:
    print("Could not find matchesData on Understat Serie A page")
else:
    decoded = match.group(1).encode("utf-8").decode("unicode_escape")
    matches = json.loads(decoded)
    print(f"Found {len(matches)} matches in Understat Serie A database.")
    for m in matches:
        # Check if either team is Lazio
        h_team = m.get('h', {}).get('title')
        a_team = m.get('a', {}).get('title')
        is_completed = m.get('isResult', False)
        date = m.get('datetime', '')
        if "Lazio" in (h_team, a_team) or "Pisa" in (h_team, a_team):
            print(f"ID: {m.get('id')} | {h_team} vs {a_team} | Date: {date} | Result: {m.get('goals', {}).get('h')}-{m.get('goals', {}).get('a')} | Completed: {is_completed}")
