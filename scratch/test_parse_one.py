import pandas as pd
import numpy as np

# Load Real Betis vs Levante files
chinese_match = "皇家贝蒂斯VS莱万特"
league_cn = "西甲"

asian_path = f"/Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/tmp/odds/{chinese_match}(亚盘).xls"
total_path = f"/Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/tmp/odds/{chinese_match}(大小).xls"
euro_path = f"/Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/tmp/odds/{chinese_match}({league_cn})欧洲数据.xls"

print("==================== PARSING EURO ====================")
df_euro = pd.read_excel(euro_path)

euro_map = {
    "威廉希尔": "威廉",
    "澳门": "澳门",
    "立博": "立博",
    "Bet365": "365",
    "易胜博": "易胜博",
    "伟德": "伟德",
    "Pinnacle平博": "Pinnacle/平博",
    "必发": "Betfair/交易所类"
}

parsed_euro = {}

for name_raw, canonical_name in euro_map.items():
    # Find matching row in column 0
    matches = df_euro[df_euro.iloc[:, 0] == name_raw]
    if not matches.empty:
        idx = matches.index[0]
        # Current is row idx
        curr_row = df_euro.iloc[idx]
        # Initial is row idx + 1
        init_row = df_euro.iloc[idx + 1]
        
        curr_odds = [str(curr_row.iloc[1]), str(curr_row.iloc[2]), str(curr_row.iloc[3])]
        init_odds = [str(init_row.iloc[1]), str(init_row.iloc[2]), str(init_row.iloc[3])]
        
        parsed_euro[canonical_name] = {
            "initial": init_odds,
            "current": curr_odds
        }
        print(f"Euro {canonical_name}: init={init_odds}, curr={curr_odds}")
    else:
        print(f"Euro {canonical_name} not found!")

print("\n==================== PARSING ASIAN ====================")
df_asian = pd.read_excel(asian_path)

asian_map = {
    "威**尔": "威廉",
    "*门": "澳门",
    "立*": "立博",
    "**t3*5": "365",
    "易*博": "易胜博",
    "伟*": "伟德",
    "Pi****le平*": "Pinnacle/平博"
}

parsed_asian = {}

def clean_handicap(h):
    if pd.isna(h):
        return None
    s = str(h).strip()
    s = s.replace(" 降", "").replace(" 升", "").strip()
    return s

for name_raw, canonical_name in asian_map.items():
    matches = df_asian[df_asian.iloc[:, 0] == name_raw]
    if not matches.empty:
        idx = matches.index[0]
        row = df_asian.iloc[idx]
        
        curr_water_home = f"{float(row.iloc[1]):.2f}" if not pd.isna(row.iloc[1]) else None
        curr_hc = clean_handicap(row.iloc[2])
        curr_water_away = f"{float(row.iloc[3]):.2f}" if not pd.isna(row.iloc[3]) else None
        
        init_water_home = f"{float(row.iloc[5]):.2f}" if not pd.isna(row.iloc[5]) else None
        init_hc = clean_handicap(row.iloc[6])
        init_water_away = f"{float(row.iloc[7]):.2f}" if not pd.isna(row.iloc[7]) else None
        
        parsed_asian[canonical_name] = {
            "initial": [init_water_home, init_hc, init_water_away],
            "current": [curr_water_home, curr_hc, curr_water_away]
        }
        print(f"Asian {canonical_name}: init={parsed_asian[canonical_name]['initial']}, curr={parsed_asian[canonical_name]['current']}")
    else:
        print(f"Asian {canonical_name} not found!")

# Betfair is missing, set to None
parsed_asian["Betfair/交易所类"] = {
    "initial": [None, None, None],
    "current": [None, None, None]
}
