import json
from pathlib import Path

manifest_path = Path("/Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/Cold_Data_Lake/DATE-20260523-top5_dispatch_manifest.json")

with open(manifest_path, "r", encoding="utf-8") as f:
    manifest = json.load(f)

print(f"Issue: {manifest.get('issue')}")
print(f"Mode: {manifest.get('mode')}")
print(f"Total Matches in Manifest: {len(manifest.get('matches', []))}")
print("-" * 50)
for idx, match in enumerate(manifest.get("matches", []), 1):
    print(f"{idx}. {match.get('english')} | Understat ID: {match.get('understat_id')} | League: {match.get('league')}")
