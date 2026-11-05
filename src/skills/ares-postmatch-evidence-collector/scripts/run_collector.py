#!/usr/bin/env python3
"""
CLI runner for Ares Postmatch Evidence Collector
"""

import argparse
import json
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS_DIR))

from postmatch_collector import PostmatchCollector

def main():
    parser = argparse.ArgumentParser(description="Ares Postmatch Evidence Collector CLI")
    parser.add_argument("--mode", choices=["FULL_POSTMATCH", "PROCESS_FILL_ONLY"], default="PROCESS_FILL_ONLY")
    parser.add_argument("--round-id", help="Round or league identifier for summary")
    parser.add_argument("--fixture-json", help="Path to single fixture input JSON file")
    parser.add_argument("--batch-json", help="Path to batch fixtures list JSON file")
    parser.add_argument("--output-dir", required=True, help="Base staging output directory")
    args = parser.parse_args()

    collector = PostmatchCollector(output_base_dir=args.output_dir)

    if args.fixture_json:
        with open(args.fixture_json, "r", encoding="utf-8") as f:
            fix_data = json.load(f)
        res = collector.collect_fixture(fix_data, mode=args.mode)
        print(json.dumps(res, ensure_ascii=False, indent=2))
    elif args.batch_json:
        with open(args.batch_json, "r", encoding="utf-8") as f:
            batch_data = json.load(f)
        res = collector.collect_round_batch(batch_data, mode=args.mode, staging_dir=args.output_dir, round_id=args.round_id)
        print(f"Batch completed: {res['processed_count']} fixtures in {res['wall_clock_seconds']}s")
    else:
        print("Error: either --fixture-json or --batch-json is required.")
        sys.exit(1)

if __name__ == "__main__":
    main()
