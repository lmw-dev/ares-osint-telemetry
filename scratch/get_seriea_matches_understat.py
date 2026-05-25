import requests
import json

url = "https://understat.com/getLeagueData/Serie_A/2025"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest"
}

resp = requests.get(url, headers=headers)
try:
    data = resp.json()
    # getLeagueData returns an object like: {"dates": [...], "teams": {...}}
    # Let's inspect dates which is the list of matches!
    dates = data.get("dates", [])
    print(f"Loaded {len(dates)} matches from getLeagueData.")
    for m in dates:
        h_team = m.get('h', {}).get('title')
        a_team = m.get('a', {}).get('title')
        is_result = m.get('isResult')
        date = m.get('datetime')
        if h_team in ('Lazio', 'Pisa') or a_team in ('Lazio', 'Pisa'):
            print(f"MATCH FOUND => ID: {m.get('id')} | {h_team} vs {a_team} | Date: {date} | Result: {m.get('goals', {}).get('h')}-{m.get('goals', {}).get('a')} | Completed: {is_result}")
except Exception as e:
    print("Failed to parse getLeagueData:", e)
    print("First 200 chars of response:", resp.text[:200])
