import json
from pathlib import Path

def main():
    lake_dir = Path("/Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/Cold_Data_Lake")
    src_path = lake_dir / "26080_dispatch_manifest.json"
    dst_path = lake_dir / "DATE-20260524-top5_dispatch_manifest.json"
    
    if not src_path.exists():
        print(f"Error: Source manifest not found: {src_path}")
        return 1
    if not dst_path.exists():
        print(f"Error: Destination manifest not found: {dst_path}")
        return 1
        
    src_data = json.loads(src_path.read_text(encoding="utf-8"))
    dst_data = json.loads(dst_path.read_text(encoding="utf-8"))
    
    src_matches = src_data.get("matches") or []
    dst_matches = dst_data.get("matches") or []
    
    # 建立 src 匹配索引 (使用英文球队对或 understat_id)
    src_map = {}
    for m in src_matches:
        uid = str(m.get("understat_id") or "").strip()
        eng = str(m.get("english") or "").strip().lower()
        if uid:
            src_map[uid] = m
        if eng:
            src_map[eng] = m
            
    print(f"Loaded {len(src_matches)} matches from issue 26080.")
    print(f"Processing {len(dst_matches)} matches in DATE-20260524-top5.")
    
    merged_count = 0
    for dm in dst_matches:
        uid = str(dm.get("understat_id") or "").strip()
        eng = str(dm.get("english") or "").strip().lower()
        
        # 尝试匹配
        sm = None
        if uid and uid in src_map:
            sm = src_map[uid]
        elif eng and eng in src_map:
            sm = src_map[eng]
            
        if sm:
            # 开始精准合并赔率及 cn_match_id 字段
            dm["cn_match_id"] = sm.get("cn_match_id")
            dm["cn_match_id_source"] = sm.get("cn_match_id_source", "merged")
            dm["titan_prematch"] = sm.get("titan_prematch")
            dm["euro_odds"] = sm.get("euro_odds") or {}
            dm["asian_handicap"] = sm.get("asian_handicap") or {}
            dm["total_goals"] = sm.get("total_goals") or {}
            dm["market_behavior"] = sm.get("market_behavior") or dm.get("market_behavior", {})
            dm["market_odds_history"] = sm.get("market_odds_history") or []
            
            merged_count += 1
            print(f"  [Merge Success] {dm.get('english')} -> cn_match_id: {dm['cn_match_id']}")
            
    # 写回目的地
    dst_data["matches"] = dst_matches
    dst_path.write_text(json.dumps(dst_data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Success! Merged {merged_count} matches odds. Manifest saved back to {dst_path}")
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
