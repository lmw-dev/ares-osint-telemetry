#!/usr/bin/env python3
"""
CLI runner for Ares FotMob Roster & Team-News Collector Skill
"""

import argparse
import json
import sys
import os
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from fotmob_roster_collector import collect_fixture_roster

def main():
    parser = argparse.ArgumentParser(description="Ares FotMob Roster Collector CLI")
    parser.add_argument("--fixture-json", required=True, help="Path to JSON file containing fixture input or raw JSON string")
    parser.add_argument("--mode", default="PREMATCH", choices=["PREMATCH", "FINAL_REFRESH"], help="Collection mode")
    parser.add_argument("--out-dir", default=None, help="Output directory for generated JSON files")
    args = parser.parse_args()

    if os.path.exists(args.fixture_json):
        with open(args.fixture_json, "r", encoding="utf-8") as f:
            fixture_data = json.load(f)
    else:
        fixture_data = json.loads(args.fixture_json)

    fixture_data["mode"] = args.mode
    if args.out_dir:
        fixture_data["output_dir"] = args.out_dir

    result = collect_fixture_roster(fixture_data)
    print(json.dumps(result, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
