import requests
from datetime import datetime

def check_all_leagues():
    leagues = ["EPL", "La_liga", "Bundesliga", "Serie_A", "Ligue_1"]
    year = "2025"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }
    
    all_matches = []
    for league in leagues:
        url = f"https://understat.com/getLeagueData/{league}/{year}"
        print(f"Fetching {league}...")
        try:
            resp = requests.get(url, headers=headers, timeout=12)
            if resp.status_code == 200:
                data = resp.json()
                if "dates" in data:
                    for m in data["dates"]:
                        dt_str = m.get("datetime", "")
                        if not dt_str:
                            continue
                        # 过滤 2026-05-22 至 2026-05-25 的比赛
                        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                        if 22 <= dt.day <= 25 and dt.month == 5 and dt.year == 2026:
                            all_matches.append({
                                "league": league,
                                "id": m["id"],
                                "datetime": dt_str,
                                "h": m["h"]["title"],
                                "a": m["a"]["title"],
                                "isResult": m.get("isResult")
                            })
        except Exception as e:
            print(f"Error fetching {league}: {e}")
            
    print("\n================== 2026-05-22 至 2026-05-25 全球五大联赛 Understat 比赛日历 ==================")
    all_matches_sorted = sorted(all_matches, key=lambda x: (x["datetime"], x["league"]))
    for m in all_matches_sorted:
        print(f"[{m['league']}] ID: {m['id']} | Date: {m['datetime']} | {m['h']} vs {m['a']} (Result: {m['isResult']})")
    print(f"==========================================================================================")
    print(f"总计找到 {len(all_matches_sorted)} 场比赛。")

if __name__ == "__main__":
    check_all_leagues()
