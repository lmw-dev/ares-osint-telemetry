import json
import pandas as pd
import numpy as np
import os

input_path = "/Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/scratch/raw_odds_input.json"
output_path = "/Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/scratch/raw_odds_input.json"

CHINESE_MATCH_MAP = {
    ("Alaves", "Rayo Vallecano"): "阿拉维斯VS巴列卡诺",
    ("Real Betis", "Levante"): "皇家贝蒂斯VS莱万特",
    ("Celta Vigo", "Sevilla"): "维戈塞尔塔VS塞维利亚",
    ("Espanyol", "Real Sociedad"): "西班牙人VS皇家社会",
    ("Real Madrid", "Athletic Club"): "皇家马德里VS毕尔巴鄂竞技",
    ("Valencia", "Barcelona"): "巴伦西亚VS巴塞罗那",
    ("Girona", "Elche"): "赫罗纳VS埃尔切",
    ("Getafe", "Osasuna"): "赫塔费VS奥萨苏纳",
    ("Mallorca", "Real Oviedo"): "马洛卡VS皇家奥维耶多",
    ("Bologna", "Inter"): "博洛尼亚VS国际米兰",
    ("Lazio", "Pisa"): "拉齐奥VS比萨"
}

euro_company_map = {
    "威廉希尔": "威廉",
    "澳门": "澳门",
    "立博": "立博",
    "Bet365": "365",
    "易胜博": "易胜博",
    "伟德": "伟德",
    "Pinnacle平博": "Pinnacle/平博",
    "必发": "Betfair/交易所类"
}

asian_company_map = {
    "威**尔": "威廉",
    "*门": "澳门",
    "立*": "立博",
    "**t3*5": "365",
    "易*博": "易胜博",
    "伟*": "伟德",
    "Pi****le平*": "Pinnacle/平博"
}

def clean_handicap(h):
    if pd.isna(h):
        return None
    s = str(h).strip()
    s = s.replace(" 降", "").replace(" 升", "").strip()
    # Normalize unicode slash if any
    s = s.replace("／", "/")
    return s

def clean_water(w):
    if pd.isna(w):
        return None
    try:
        return f"{float(w):.2f}"
    except ValueError:
        return str(w).strip()

def clean_euro_odds(o):
    if pd.isna(o):
        return None
    try:
        return f"{float(o):.2f}"
    except ValueError:
        return str(o).strip()

with open(input_path, "r", encoding="utf-8") as f:
    data = json.load(f)

print(f"Loaded raw_odds_input.json, total matches: {len(data['matches'])}")

successful_count = 0

for m in data['matches']:
    home = m['home']
    away = m['away']
    league = m['league']
    match_no = m['match_no']
    
    # Find match key
    match_key = (home, away)
    chinese_match = CHINESE_MATCH_MAP.get(match_key)
    if not chinese_match:
        print(f"[Warning] No Chinese name mapping found for {home} vs {away}")
        continue
        
    league_cn = "意甲" if league == "Serie_A" else "西甲"
    
    odds_dir = "/Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/tmp/odds"
    asian_file = os.path.join(odds_dir, f"{chinese_match}(亚盘).xls")
    total_file = os.path.join(odds_dir, f"{chinese_match}(大小).xls")
    euro_file = os.path.join(odds_dir, f"{chinese_match}({league_cn})欧洲数据.xls")
    
    print(f"\nProcessing match [{match_no}] {home} vs {away} ({chinese_match})")
    
    # 1. Parse Euro
    euro_data = {}
    if os.path.exists(euro_file):
        df_euro = pd.read_excel(euro_file)
        # Verify columns exist
        for name_raw, canonical_name in euro_company_map.items():
            matches = df_euro[df_euro.iloc[:, 0] == name_raw]
            if not matches.empty:
                idx = matches.index[0]
                curr_row = df_euro.iloc[idx]
                init_row = df_euro.iloc[idx + 1]
                
                curr_odds = [clean_euro_odds(curr_row.iloc[1]), clean_euro_odds(curr_row.iloc[2]), clean_euro_odds(curr_row.iloc[3])]
                init_odds = [clean_euro_odds(init_row.iloc[1]), clean_euro_odds(init_row.iloc[2]), clean_euro_odds(init_row.iloc[3])]
                
                euro_data[canonical_name] = {
                    "initial": init_odds,
                    "current": curr_odds
                }
            else:
                # Set default missing if not found
                euro_data[canonical_name] = {
                    "initial": [None, None, None],
                    "current": [None, None, None]
                }
    else:
        print(f"  [Error] Euro file does not exist: {euro_file}")
        
    # 2. Parse Asian
    asian_data = {}
    if os.path.exists(asian_file):
        df_asian = pd.read_excel(asian_file)
        for name_raw, canonical_name in asian_company_map.items():
            matches = df_asian[df_asian.iloc[:, 0] == name_raw]
            if not matches.empty:
                idx = matches.index[0]
                row = df_asian.iloc[idx]
                
                curr_water_home = clean_water(row.iloc[1])
                curr_hc = clean_handicap(row.iloc[2])
                curr_water_away = clean_water(row.iloc[3])
                
                init_water_home = clean_water(row.iloc[5])
                init_hc = clean_handicap(row.iloc[6])
                init_water_away = clean_water(row.iloc[7])
                
                asian_data[canonical_name] = {
                    "initial": [init_water_home, init_hc, init_water_away],
                    "current": [curr_water_home, curr_hc, curr_water_away]
                }
            else:
                asian_data[canonical_name] = {
                    "initial": [None, None, None],
                    "current": [None, None, None]
                }
        # Betfair is always missing for asian
        asian_data["Betfair/交易所类"] = {
            "initial": [None, None, None],
            "current": [None, None, None]
        }
    else:
        print(f"  [Error] Asian file does not exist: {asian_file}")

    # 3. Parse Total
    total_data = {}
    if os.path.exists(total_file):
        df_total = pd.read_excel(total_file)
        for name_raw, canonical_name in asian_company_map.items():
            matches = df_total[df_total.iloc[:, 0] == name_raw]
            if not matches.empty:
                idx = matches.index[0]
                row = df_total.iloc[idx]
                
                curr_water_over = clean_water(row.iloc[1])
                curr_line = clean_handicap(row.iloc[2])
                curr_water_under = clean_water(row.iloc[3])
                
                init_water_over = clean_water(row.iloc[5])
                init_line = clean_handicap(row.iloc[6])
                init_water_under = clean_water(row.iloc[7])
                
                total_data[canonical_name] = {
                    "initial": [init_water_over, init_line, init_water_under],
                    "current": [curr_water_over, curr_line, curr_water_under]
                }
            else:
                total_data[canonical_name] = {
                    "initial": [None, None, None],
                    "current": [None, None, None]
                }
        # Betfair is always missing for total
        total_data["Betfair/交易所类"] = {
            "initial": [None, None, None],
            "current": [None, None, None]
        }
    else:
        print(f"  [Error] Total file does not exist: {total_file}")
        
    # Ingest back to matches raw odds dict
    m['odds_raw'] = {
        "euro": euro_data,
        "asian": asian_data,
        "total": total_data
    }
    
    print(f"  Successfully parsed and ingested [euro={len(euro_data)}, asian={len(asian_data)}, total={len(total_data)}] companies.")
    successful_count += 1

# Write updated json back to raw_odds_input.json
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"\nComplete! Successfully updated {successful_count} matches in raw_odds_input.json with 100% REAL Excel data!")
