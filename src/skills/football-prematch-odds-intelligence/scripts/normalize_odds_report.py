#!/usr/bin/env python3
"""Normalize football prematch odds records and summarize market direction.

Input: JSON file containing a list of match records. This helper is intentionally
schema-light: it provides reusable functions and a small CLI that can normalize
bookmaker names and summarize deltas when records follow the common structure:

{
  "matches": [
    {
      "match": "主队 vs 客队",
      "priority_euro": {"威廉": {"initial": [1.8,3.5,4.2], "current": [1.7,3.6,4.5]}},
      "asian_handicap": {"威廉": {"initial": [0.9,-0.5,0.95], "current": [0.82,-0.75,1.02]}},
      "total_goals": {"365": {"initial": [0.9,2.5,0.95], "current": [0.82,2.75,1.02]}}
    }
  ]
}
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

CANONICAL_BOOKMAKERS = [
    "威廉", "澳门", "立博", "365", "易胜博", "伟德", "Pinnacle/平博", "Betfair/交易所类"
]

ALIASES = {
    "威廉希尔": "威廉", "William": "威廉", "William Hill": "威廉",
    "澳彩": "澳门", "Macauslot": "澳门", "澳门彩票": "澳门",
    "Ladbrokes": "立博", "利记": "立博",
    "Bet365": "365", "bet365": "365",
    "Easbet": "易胜博", "易胜": "易胜博",
    "BetVictor": "伟德", "Victor Chandler": "伟德",
    "Pinnacle": "Pinnacle/平博", "平博": "Pinnacle/平博", "Pinnacle Sports": "Pinnacle/平博",
    "Betfair": "Betfair/交易所类", "交易所": "Betfair/交易所类", "Exchange": "Betfair/交易所类",
}


def canon_bookmaker(name: str) -> str:
    return ALIASES.get(str(name).strip(), str(name).strip())


def fnum(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        v = float(x)
        if math.isnan(v):
            return None
        return v
    except Exception:
        return None


def fmt_delta(delta: Optional[float], label: str) -> Optional[str]:
    if delta is None or abs(delta) < 0.005:
        return None
    return f"{label}{'升' if delta > 0 else '降'}{abs(delta):.2f}"


def euro_change(initial: Iterable[Any], current: Iterable[Any]) -> str:
    init = [fnum(x) for x in initial]
    cur = [fnum(x) for x in current]
    if len(init) < 3 or len(cur) < 3 or any(v is None for v in init[:3] + cur[:3]):
        return "数据不足"
    labels = ["主胜", "平赔", "客胜"]
    deltas = [cur[i] - init[i] for i in range(3)]
    parts = [fmt_delta(d, labels[i]) for i, d in enumerate(deltas)]
    parts = [p for p in parts if p]
    return "；".join(parts) if parts else "无明显变化"


def handicap_change(initial: Iterable[Any], current: Iterable[Any]) -> str:
    init = [fnum(x) for x in initial]
    cur = [fnum(x) for x in current]
    if len(init) < 3 or len(cur) < 3:
        return "数据不足"
    parts: List[str] = []
    if init[1] is not None and cur[1] is not None and abs(cur[1] - init[1]) >= 0.005:
        parts.append(f"盘口 {init[1]:g}→{cur[1]:g}")
    if init[0] is not None and cur[0] is not None:
        p = fmt_delta(cur[0] - init[0], "主水")
        if p:
            parts.append(p)
    if init[2] is not None and cur[2] is not None:
        p = fmt_delta(cur[2] - init[2], "客水")
        if p:
            parts.append(p)
    return "；".join(parts) if parts else "无明显变化"


def total_change(initial: Iterable[Any], current: Iterable[Any]) -> str:
    init = [fnum(x) for x in initial]
    cur = [fnum(x) for x in current]
    if len(init) < 3 or len(cur) < 3:
        return "数据不足"
    parts: List[str] = []
    if init[1] is not None and cur[1] is not None and abs(cur[1] - init[1]) >= 0.005:
        parts.append(f"大小球 {init[1]:g}→{cur[1]:g}")
    if init[0] is not None and cur[0] is not None:
        p = fmt_delta(cur[0] - init[0], "大球水")
        if p:
            parts.append(p)
    if init[2] is not None and cur[2] is not None:
        p = fmt_delta(cur[2] - init[2], "小球水")
        if p:
            parts.append(p)
    return "；".join(parts) if parts else "无明显变化"


def normalize_bookmaker_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {k: {"status": "source_missing"} for k in CANONICAL_BOOKMAKERS}
    for k, v in (d or {}).items():
        out[canon_bookmaker(k)] = v
    return out


def summarize_match(match: Dict[str, Any]) -> Dict[str, Any]:
    m = dict(match)
    if "priority_euro" in m:
        m["priority_euro"] = normalize_bookmaker_dict(m["priority_euro"])
        for row in m["priority_euro"].values():
            if isinstance(row, dict) and "initial" in row and "current" in row:
                row.setdefault("last_change", euro_change(row["initial"], row["current"]))
    if "asian_handicap" in m:
        m["asian_handicap"] = normalize_bookmaker_dict(m["asian_handicap"])
        for row in m["asian_handicap"].values():
            if isinstance(row, dict) and "initial" in row and "current" in row:
                row.setdefault("key_change", handicap_change(row["initial"], row["current"]))
    if "total_goals" in m:
        for row in m["total_goals"].values():
            if isinstance(row, dict) and "initial" in row and "current" in row:
                row.setdefault("key_change", total_change(row["initial"], row["current"]))
    return m


def main() -> int:
    if len(sys.argv) < 3:
        print("Usage: normalize_odds_report.py input.json output.json", file=sys.stderr)
        return 2
    src = Path(sys.argv[1])
    dst = Path(sys.argv[2])
    data = json.loads(src.read_text(encoding="utf-8"))
    matches = data.get("matches", data if isinstance(data, list) else [])
    out = {"matches": [summarize_match(m) for m in matches]}
    dst.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {dst}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
