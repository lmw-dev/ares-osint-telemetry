import json
from pathlib import Path

dir_0524 = Path("/Users/liumingwei/vaults/AresVault/03_Match_Audits/AresMatchday_20260524")
market_path = dir_0524 / "market.json"
abnormal_path = dir_0524 / "abnormal.json"

if market_path.exists():
    with open(market_path, "r", encoding="utf-8") as f:
        market = json.load(f)
    print(f"market.json loaded. Keys: {list(market.keys())[:10]}")
    # print matches or length
    if isinstance(market, dict):
        print(f"market matches count: {len(market.get('matches', [])) or len(market)}")
    elif isinstance(market, list):
        print(f"market matches count (list): {len(market)}")
else:
    print("market.json does not exist")

if abnormal_path.exists():
    with open(abnormal_path, "r", encoding="utf-8") as f:
        abnormal = json.load(f)
    print(f"abnormal.json loaded. Keys: {list(abnormal.keys())[:10]}")
    if isinstance(abnormal, dict):
        print(f"abnormal matches count: {len(abnormal.get('matches', [])) or len(abnormal)}")
    elif isinstance(abnormal, list):
        print(f"abnormal matches count (list): {len(abnormal)}")
else:
    print("abnormal.json does not exist")
