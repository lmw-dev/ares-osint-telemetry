import requests
import json
from datetime import datetime

def check_league(league, year):
    url = f"https://understat.com/getLeagueData/{league}/{year}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }
    print(f"Fetching {url}...")
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            print(f"Error fetching: HTTP {resp.status_code}")
            return
        data = resp.json()
        if "dates" not in data:
            print("No 'dates' key in response.")
            return
        
        matches = data["dates"]
        print(f"Total matches fetched: {len(matches)}")
        
        # 筛选 2026 年 5 月的比赛
        may_2026_matches = []
        for m in matches:
            dt_str = m.get("datetime", "")
            if not dt_str:
                continue
            # 判断是否是 2026-05
            if "2026-05" in dt_str:
                may_2026_matches.append(m)
                
        print(f"\n--- 2026年5月 {league} 比赛时间列表 (共 {len(may_2026_matches)} 场) ---")
        for m in sorted(may_2026_matches, key=lambda x: x.get("datetime", "")):
            print(f"ID: {m['id']} | Date: {m['datetime']} | {m['h']['title']} vs {m['a']['title']} | Is Result: {m.get('isResult')}")
            
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    check_league("Serie_A", "2025")
