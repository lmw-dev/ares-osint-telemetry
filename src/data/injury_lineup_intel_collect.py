import argparse
import html
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from audit_router import load_dotenv_into_env, normalize_vault_path
from team_forge import build_archive_path, iter_issue_teams, split_frontmatter

TIMEOUT_SEC = 10
USER_AGENT = "Mozilla/5.0 (AresTelemetry/1.0; +https://example.local)"
STATUS_VALUES = {"OUT", "DOUBTFUL", "SUSPENDED", "RETURNING", "UNKNOWN"}
ROLE_VALUES = {"GK", "CB", "FB", "DM", "CM", "AM", "W", "ST", "UNKNOWN"}
CONFIDENCE_VALUES = {"HIGH", "MEDIUM", "LOW", "UNKNOWN"}
LINEUP_ENUMS = {
    "expected_core_availability": {"FULL", "MOSTLY_AVAILABLE", "PARTIAL", "DAMAGED", "UNKNOWN"},
    "lineup_stability_precheck": {"GREEN", "YELLOW", "RED", "UNKNOWN"},
    "key_node_absence_risk": {"LOW", "MEDIUM", "HIGH", "CRITICAL", "UNKNOWN"},
}
LINEUP_SNAPSHOT_STATUS_ENUMS = {"LIVE_OK", "LIVE_BLOCKED", "SEEDED", "UNKNOWN"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _split_match(english: str) -> Tuple[str, str]:
    if " vs " in english:
        home, away = english.split(" vs ", 1)
        return home.strip(), away.strip()
    if " VS " in english:
        home, away = english.split(" VS ", 1)
        return home.strip(), away.strip()
    return english.strip(), ""


def _load_manifest(vault_root: Path, issue: str) -> Dict[str, Any]:
    path = vault_root / "04_RAG_Raw_Data" / "Cold_Data_Lake" / f"{issue}_dispatch_manifest.json"
    if not path.exists():
        raise FileNotFoundError(f"找不到 manifest: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _team_league_map(base_dir: Path, vault_root: Path, issue: str, manifest: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for team, league in iter_issue_teams(base_dir, vault_root, issue):
        if team and league:
            out[team] = league
    if out:
        return out
    for match in manifest.get("matches") or []:
        english = _safe_text(match.get("english"))
        league = _safe_text(match.get("league")) or "Unknown"
        home, away = _split_match(english)
        if home:
            out.setdefault(home, league)
        if away:
            out.setdefault(away, league)
    return out


def _build_team_match_urls(manifest: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for match in manifest.get("matches") or []:
        english = _safe_text(match.get("english"))
        home, away = _split_match(english)
        cn_match_id = _safe_text(match.get("cn_match_id"))
        if not cn_match_id or not cn_match_id.isdigit():
            continue
        nowscore = f"https://live.nowscore.com/analysis/{cn_match_id}cn.html"
        titan = f"https://zq.titan007.com/analysis/{cn_match_id}cn.htm"
        for team in (home, away):
            if not team:
                continue
            out[team] = {
                "nowscore_url": nowscore,
                "titan_url": titan,
            }
    return out


def _load_existing(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    teams = payload.get("teams") if isinstance(payload.get("teams"), list) else []
    out: Dict[str, Dict[str, Any]] = {}
    for item in teams:
        if not isinstance(item, dict):
            continue
        team = _safe_text(item.get("team"))
        if team:
            out[team] = item
    return out


def _normalize_lineup_value(key: str, value: Any) -> str:
    raw = _safe_text(value).upper() or "UNKNOWN"
    allowed = LINEUP_ENUMS[key]
    return raw if raw in allowed else "UNKNOWN"


def _merge_source_items(base: List[Dict[str, Any]], updates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for row in base:
        if not isinstance(row, dict):
            continue
        name = _safe_text(row.get("source_name"))
        if name:
            idx[name] = dict(row)
    for row in updates:
        name = _safe_text(row.get("source_name"))
        if not name:
            continue
        merged = idx.get(name, {})
        merged.update(row)
        idx[name] = merged
    ordered = sorted(idx.values(), key=lambda x: _safe_text(x.get("source_name")))
    return ordered


def _normalize_absence(item: Dict[str, Any], fallback_url: str = "") -> Optional[Dict[str, Any]]:
    player = _safe_text(item.get("player"))
    if not player:
        return None
    status = _safe_text(item.get("status")).upper() or "UNKNOWN"
    role = _safe_text(item.get("role")).upper() or "UNKNOWN"
    confidence = _safe_text(item.get("confidence")).upper() or "UNKNOWN"
    if status not in STATUS_VALUES:
        status = "UNKNOWN"
    if role not in ROLE_VALUES:
        role = "UNKNOWN"
    if confidence not in CONFIDENCE_VALUES:
        confidence = "UNKNOWN"
    key_node = bool(item.get("key_node"))
    impact_level = _safe_text(item.get("impact_level")).upper() or ("HIGH" if key_node else "MEDIUM")
    return {
        "player": player,
        "status": status,
        "role": role,
        "confidence": confidence,
        "key_node": key_node,
        "impact_level": impact_level,
        "source_url": _safe_text(item.get("source_url")) or fallback_url,
        "fetched_at": _safe_text(item.get("fetched_at")) or _utc_now(),
    }


def _merge_absences(old_rows: List[Dict[str, Any]], new_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for row in old_rows:
        norm = _normalize_absence(row)
        if not norm:
            continue
        idx[norm["player"].lower()] = norm
    for row in new_rows:
        norm = _normalize_absence(row)
        if not norm:
            continue
        idx[norm["player"].lower()] = norm
    return sorted(idx.values(), key=lambda x: x["player"].lower())


def _role_from_text(text: str) -> str:
    low = _safe_text(text).lower()
    if any(token in low for token in ["goalkeeper", "keeper", "门将"]):
        return "GK"
    if any(token in low for token in ["centre-back", "center-back", "defender", "后卫"]):
        return "CB"
    if any(token in low for token in ["full-back", "wing-back", "边后卫"]):
        return "FB"
    if any(token in low for token in ["defensive midfield", "dm", "后腰"]):
        return "DM"
    if any(token in low for token in ["midfield", "中场"]):
        return "CM"
    if any(token in low for token in ["attacking midfield", "am", "前腰"]):
        return "AM"
    if any(token in low for token in ["winger", "wing", "边锋"]):
        return "W"
    if any(token in low for token in ["striker", "forward", "前锋"]):
        return "ST"
    return "UNKNOWN"


def _extract_archive_intel(vault_root: Path, team: str, league: str) -> Tuple[List[Dict[str, Any]], Dict[str, str], str]:
    path = build_archive_path(vault_root, team, league)
    if not path.exists():
        return [], {
            "expected_core_availability": "UNKNOWN",
            "lineup_stability_precheck": "UNKNOWN",
            "key_node_absence_risk": "UNKNOWN",
        }, ""

    frontmatter, _ = split_frontmatter(path.read_text(encoding="utf-8"))
    archive_url = str(path)
    lineup_profile = frontmatter.get("lineup_risk_profile") if isinstance(frontmatter.get("lineup_risk_profile"), dict) else {}

    out_lineup = {
        "expected_core_availability": _normalize_lineup_value(
            "expected_core_availability", lineup_profile.get("expected_core_availability")
        ),
        "lineup_stability_precheck": _normalize_lineup_value(
            "lineup_stability_precheck", lineup_profile.get("lineup_stability_precheck")
        ),
        "key_node_absence_risk": _normalize_lineup_value(
            "key_node_absence_risk", lineup_profile.get("key_node_absence_risk")
        ),
    }

    absences: List[Dict[str, Any]] = []
    rows = frontmatter.get("absences") if isinstance(frontmatter.get("absences"), list) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        norm = _normalize_absence(row, fallback_url=archive_url)
        if norm:
            absences.append(norm)

    if not absences:
        for key, status in (("injured_nodes", "OUT"), ("suspended_nodes", "SUSPENDED")):
            legacy = frontmatter.get(key)
            if not isinstance(legacy, list):
                continue
            for item in legacy:
                name = _safe_text(item)
                if not name:
                    continue
                absences.append(
                    {
                        "player": name,
                        "status": status,
                        "role": "UNKNOWN",
                        "confidence": "MEDIUM",
                        "key_node": False,
                        "impact_level": "MEDIUM",
                        "source_url": archive_url,
                        "fetched_at": _utc_now(),
                    }
                )

    return absences, out_lineup, archive_url


def _fetch_transfermarkt_absences(team: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    now = _utc_now()
    encoded = requests.utils.quote(team)
    search_url = f"https://www.transfermarkt.com/schnellsuche/ergebnis/schnellsuche?query={encoded}"
    source_meta = {
        "source_name": "Transfermarkt",
        "url": search_url,
        "fetched_at": now,
        "reliability": "LOW",
        "raw_status": "SEARCH_FAILED",
    }

    try:
        resp = session.get(search_url, timeout=TIMEOUT_SEC)
        if resp.status_code >= 400:
            source_meta["raw_status"] = f"HTTP_{resp.status_code}"
            return [], source_meta
        html = resp.text
    except Exception as exc:
        source_meta["raw_status"] = f"ERROR:{type(exc).__name__}"
        return [], source_meta

    m = re.search(r'href="(?P<href>/[^"\s]*/startseite/verein/\d+)"', html)
    if not m:
        source_meta["raw_status"] = "NO_TEAM_MATCH"
        return [], source_meta

    team_href = m.group("href")
    injuries_url = f"https://www.transfermarkt.com{team_href.replace('/startseite/', '/verletztespieler/', 1)}"
    source_meta["url"] = injuries_url
    source_meta["raw_status"] = "DETAIL_FAILED"

    try:
        det = session.get(injuries_url, timeout=TIMEOUT_SEC)
        if det.status_code >= 400:
            source_meta["raw_status"] = f"HTTP_{det.status_code}"
            return [], source_meta
        page = det.text
    except Exception as exc:
        source_meta["raw_status"] = f"ERROR:{type(exc).__name__}"
        return [], source_meta

    rows = re.findall(
        r'<td class="hauptlink">\s*<a[^>]*>(?P<player>[^<]+)</a>.*?<td[^>]*>(?P<injury>[^<]*)</td>',
        page,
        flags=re.S,
    )
    absences: List[Dict[str, Any]] = []
    for player, injury in rows:
        name = re.sub(r"\s+", " ", player).strip()
        injury_txt = re.sub(r"\s+", " ", injury).strip()
        if not name:
            continue
        absences.append(
            {
                "player": name,
                "status": "OUT",
                "role": _role_from_text(injury_txt),
                "confidence": "LOW",
                "key_node": False,
                "impact_level": "LOW",
                "source_url": injuries_url,
                "fetched_at": now,
            }
        )

    source_meta["raw_status"] = "OK" if absences else "OK_EMPTY"
    source_meta["reliability"] = "LOW"
    return absences, source_meta


def _fetch_cn_absence_signals(
    source_name: str,
    url: str,
    *,
    raw_dump_dir: Optional[Path] = None,
    raw_dump_name: str = "",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    now = _utc_now()
    source_meta = {
        "source_name": source_name,
        "url": url,
        "fetched_at": now,
        "reliability": "MEDIUM",
        "raw_status": "FETCH_FAILED",
    }
    try:
        resp = requests.get(url, timeout=TIMEOUT_SEC, headers={"User-Agent": USER_AGENT})
        if resp.status_code >= 400:
            source_meta["raw_status"] = f"HTTP_{resp.status_code}"
            return [], source_meta
        text = resp.content.decode("utf-8-sig", errors="replace")
    except Exception as exc:
        source_meta["raw_status"] = f"ERROR:{type(exc).__name__}"
        return [], source_meta

    if raw_dump_dir is not None and raw_dump_name:
        try:
            raw_dump_dir.mkdir(parents=True, exist_ok=True)
            (raw_dump_dir / f"{raw_dump_name}.html").write_text(text, encoding="utf-8", errors="ignore")
        except Exception:
            pass

    low_text = text.lower()
    if len(text) < 1500 or any(
        marker in low_text
        for marker in [
            "access denied",
            "forbidden",
            "captcha",
            "cloudflare",
            "频繁",
            "验证",
            "拦截",
        ]
    ):
        source_meta["raw_status"] = "PAGE_BLOCKED_OR_EMPTY"
        source_meta["reliability"] = "LOW"
        return [], source_meta

    absences: List[Dict[str, Any]] = []
    parsed_modules: List[str] = []
    supplemental_texts: List[str] = []

    def parse_titan_lineup_rows(raw_text: str) -> List[List[str]]:
        marker = '<h2 class="fx_title2" id="013">阵容情况</h2>'
        start = raw_text.find(marker)
        if start < 0:
            return []
        end = raw_text.find('<h2 class="fx_title2"', start + len(marker))
        block = raw_text[start : end if end != -1 else None]
        rows: List[List[str]] = []
        for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", block, flags=re.S | re.I):
            cells: List[str] = []
            for td in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, flags=re.S | re.I):
                cell = re.sub(r"<[^>]+>", " ", td)
                cell = html.unescape(cell).replace("\xa0", " ")
                cell = re.sub(r"\s+", " ", cell).strip()
                if cell:
                    cells.append(cell)
            if cells:
                rows.append(cells)
        return rows

    def parse_titan_lineup_snapshot(raw_text: str, source_url: str) -> Dict[str, Any]:
        rows = parse_titan_lineup_rows(raw_text)
        if not rows:
            return {}
        start_idx = -1
        for i, row in enumerate(rows):
            if len(row) >= 2 and row[0] == "首发" and row[1] == "后备":
                start_idx = i + 1
                break
        if start_idx < 0:
            return {}
        starters: List[str] = []
        bench: List[str] = []
        for row in rows[start_idx:]:
            if len(row) < 2:
                continue
            left, right = row[0], row[1]
            if "上一场阵容" in left or "上一场阵容" in right:
                continue
            if "球员" in left or "缺阵原因" in right:
                continue
            if "评分" in left or "评分" in right:
                continue
            if left:
                starters.append(left)
            if right:
                bench.append(right)
            if len(starters) >= 11 and len(bench) >= 7:
                break
        if not starters and not bench:
            return {}
        return {
            "source": {"provider": "Titan007", "url": source_url, "fetched_at": _utc_now()},
            "starting_xi": starters[:22],
            "substitutes": bench[:22],
        }

    match_id = ""
    m1 = re.search(r"/analysis/(\d+)", url)
    m2 = re.search(r"sid=(\d+)", url)
    if m1:
        match_id = m1.group(1)
    elif m2:
        match_id = m2.group(1)
    if match_id.isdigit():
        extra_urls = [
            f"https://live.nowscore.com/MatchDetail/{match_id}cn.html",
            f"https://live.nowscore.com/odds/match/{match_id}.htm",
            f"https://live.nowscore.com/info/coach/{match_id}.htm",
            f"https://zq.titan007.com/analysis/{match_id}cn.htm",
        ]
        for eurl in extra_urls:
            try:
                er = requests.get(eurl, timeout=TIMEOUT_SEC, headers={"User-Agent": USER_AGENT})
                if er.status_code >= 400:
                    continue
                et = er.content.decode("utf-8-sig", errors="replace")
                if len(et) < 200:
                    continue
                supplemental_texts.append(et)
                if "titan007.com/analysis/" in eurl:
                    titan_rows = parse_titan_lineup_rows(et)
                    # Parse rows where table is "球员 | 缺阵原因" blocks.
                    for row in titan_rows:
                        if len(row) < 2:
                            continue
                        row_text = " ".join(row)
                        if "缺阵原因" in row_text or "球员" in row_text:
                            continue
                        # Heuristic: first item is jersey number, second is player name, third is reason.
                        player = ""
                        reason = ""
                        if len(row) >= 3 and re.fullmatch(r"\d+", row[0]):
                            player = row[1]
                            reason = row[2]
                        elif len(row) >= 2:
                            player = row[0]
                            reason = row[1]
                        player = re.sub(r"\s+", " ", str(player)).strip()
                        reason = re.sub(r"\s+", " ", str(reason)).strip()
                        if not player:
                            continue
                        absences.append(
                            {
                                "player": player,
                                "status": "OUT",
                                "role": _role_from_text(reason),
                                "confidence": "MEDIUM",
                                "key_node": False,
                                "impact_level": "MEDIUM",
                                "source_url": eurl,
                                "fetched_at": now,
                            }
                        )
                    if titan_rows and "titan_lineup_table" not in parsed_modules:
                        parsed_modules.append("titan_lineup_table")
                    titan_snapshot = parse_titan_lineup_snapshot(et, eurl)
                    if titan_snapshot:
                        source_meta["last_match_lineup_snapshot"] = titan_snapshot
                        if "titan_last_match_lineup" not in parsed_modules:
                            parsed_modules.append("titan_last_match_lineup")
            except Exception:
                continue
    # 常见中文信号（仅做MVP提取）
    patterns = [
        (r"([A-Za-z .'-]{3,})\s*(?:伤停|伤缺|缺阵|停赛)", "OUT"),
        (r"([A-Za-z .'-]{3,})\s*(?:复出|回归)", "RETURNING"),
    ]
    scan_text = text + "\n".join(supplemental_texts)
    for pat, status in patterns:
        for m in re.finditer(pat, scan_text, flags=re.I):
            player = re.sub(r"\s+", " ", m.group(1)).strip(" .,-")
            if not player or len(player) < 3:
                continue
            absences.append(
                {
                    "player": player,
                    "status": status,
                    "role": "UNKNOWN",
                    "confidence": "LOW",
                    "key_node": False,
                    "impact_level": "LOW",
                    "source_url": url,
                    "fetched_at": now,
                }
            )
            if "inline_text" not in parsed_modules:
                parsed_modules.append("inline_text")

    # 解析“预计首发/阵容”段落中的缺席信号（MVP）
    lineup_block = ""
    for marker in ("预计首发", "预计阵容", "首发阵容", "阵容"):
        pos = scan_text.find(marker)
        if pos >= 0:
            lineup_block = scan_text[pos : pos + 2000]
            break
    if lineup_block:
        for m in re.finditer(r"([A-Za-zÀ-ÿ\u4e00-\u9fa5 .'-]{2,40})\s*(?:缺阵|停赛|伤缺|伤停)", lineup_block, flags=re.I):
            player = re.sub(r"\s+", " ", m.group(1)).strip(" .,-")
            if not player or len(player) < 2:
                continue
            absences.append(
                {
                    "player": player,
                    "status": "OUT",
                    "role": "UNKNOWN",
                    "confidence": "MEDIUM",
                    "key_node": False,
                    "impact_level": "MEDIUM",
                    "source_url": url,
                    "fetched_at": now,
                }
            )
            if "lineup_block" not in parsed_modules:
                parsed_modules.append("lineup_block")

    # Try dynamic JS modules referenced by page (Nowscore/Titan both embed schedule-id scripts).
    js_paths = re.findall(r"""<script[^>]+src=["']([^"']+)["']""", text, flags=re.I)
    js_candidates = []
    for src in js_paths:
        s = _safe_text(src)
        if not s:
            continue
        low = s.lower()
        if "analysisjs/data" in low or "getscheduleinfo" in low:
            js_candidates.append(s)
    base = ""
    m_base = re.match(r"^(https?://[^/]+)", url, flags=re.I)
    if m_base:
        base = m_base.group(1)
    dynamic_fetch_ok = False
    for src in js_candidates[:4]:
        js_url = src
        if src.startswith("//"):
            js_url = "https:" + src
        elif src.startswith("/") and base:
            js_url = base + src
        elif not src.startswith("http") and base:
            js_url = base + "/" + src.lstrip("/")
        candidate_urls = [js_url]
        # Nowscore data.js often hops across www -> score -> live
        if "nowscore.com" in js_url:
            candidate_urls.extend(
                [
                    js_url.replace("https://www.nowscore.com", "http://score.nowscore.com"),
                    js_url.replace("https://www.nowscore.com", "http://live.nowscore.com"),
                    js_url.replace("https://score.nowscore.com", "http://live.nowscore.com"),
                    js_url.replace("https://live.nowscore.com", "http://score.nowscore.com"),
                ]
            )
        seen = set()
        js_text = ""
        final_js_url = ""
        for probe_url in candidate_urls:
            pu = _safe_text(probe_url)
            if not pu or pu in seen:
                continue
            seen.add(pu)
            try:
                js_resp = requests.get(pu, timeout=TIMEOUT_SEC, headers={"User-Agent": USER_AGENT})
            except Exception:
                continue
            if js_resp.status_code >= 400:
                continue
            body = js_resp.text
            # Detect move page and follow hinted target manually.
            m_move = re.search(r"""HREF=["']([^"']+)["']""", body, flags=re.I)
            if "<title>文档已移动" in body and m_move:
                continue
            js_text = body
            final_js_url = pu
            dynamic_fetch_ok = True
            break
        if not js_text:
            continue
        for m in re.finditer(r"([A-Za-zÀ-ÿ\u4e00-\u9fa5 .'-]{2,40})\s*(?:缺阵|停赛|伤缺|伤停|受伤)", js_text, flags=re.I):
            player = re.sub(r"\s+", " ", m.group(1)).strip(" .,-")
            if not player or len(player) < 2:
                continue
            absences.append(
                {
                    "player": player,
                    "status": "OUT",
                    "role": "UNKNOWN",
                    "confidence": "LOW",
                    "key_node": False,
                    "impact_level": "LOW",
                    "source_url": final_js_url or js_url,
                    "fetched_at": now,
                }
            )
            if "dynamic_js" not in parsed_modules:
                parsed_modules.append("dynamic_js")

    if absences:
        source_meta["raw_status"] = "OK"
    else:
        source_meta["raw_status"] = "OK_NO_SIGNAL"
        if js_candidates:
            source_meta["raw_status"] = "OK_NO_SIGNAL_DYNAMIC_JS_FETCHED" if dynamic_fetch_ok else "OK_NO_SIGNAL_DYNAMIC_PAGE"
    if parsed_modules:
        source_meta["parsed_modules"] = parsed_modules
    if isinstance(source_meta.get("last_match_lineup_snapshot"), dict):
        source_meta["lineup_snapshot_status"] = "LIVE_OK"
    elif source_meta.get("raw_status") in {"PAGE_BLOCKED_OR_EMPTY"} or str(source_meta.get("raw_status", "")).startswith("HTTP_"):
        source_meta["lineup_snapshot_status"] = "LIVE_BLOCKED"
    else:
        source_meta["lineup_snapshot_status"] = "UNKNOWN"
    return absences, source_meta


def _extract_titan_match_id_from_url(url: str) -> str:
    m = re.search(r"/analysis/(\d+)", _safe_text(url))
    return m.group(1) if m else ""


def _load_seeded_titan_snapshot(titan_html_dir: Optional[Path], match_id: str) -> Dict[str, Any]:
    if titan_html_dir is None or not match_id:
        return {}
    html_path = titan_html_dir / f"{match_id}.html"
    if not html_path.exists():
        return {}
    raw_text = html_path.read_text(encoding="utf-8", errors="replace")
    marker = '<h2 class="fx_title2" id="013">阵容情况</h2>'
    start = raw_text.find(marker)
    if start < 0:
        return {}
    end = raw_text.find('<h2 class="fx_title2"', start + len(marker))
    block = raw_text[start : end if end != -1 else None]
    rows: List[List[str]] = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", block, flags=re.S | re.I):
        cells: List[str] = []
        for td in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, flags=re.S | re.I):
            cell = re.sub(r"<[^>]+>", " ", td)
            cell = html.unescape(cell).replace("\xa0", " ")
            cell = re.sub(r"\s+", " ", cell).strip()
            if cell:
                cells.append(cell)
        if cells:
            rows.append(cells)
    if not rows:
        return {}
    start_idx = -1
    for i, row in enumerate(rows):
        if len(row) >= 2 and row[0] == "首发" and row[1] == "后备":
            start_idx = i + 1
            break
    if start_idx < 0:
        return {}
    starters: List[str] = []
    bench: List[str] = []
    for row in rows[start_idx:]:
        if len(row) < 2:
            continue
        left, right = row[0], row[1]
        if "上一场阵容" in left or "上一场阵容" in right:
            continue
        if "球员" in left or "缺阵原因" in right:
            continue
        if "评分" in left or "评分" in right:
            continue
        if left:
            starters.append(left)
        if right:
            bench.append(right)
        if len(starters) >= 11 and len(bench) >= 7:
            break
    if not starters and not bench:
        return {}
    return {
        "source": {"provider": "Titan007", "url": str(html_path), "fetched_at": _utc_now()},
        "starting_xi": starters[:22],
        "substitutes": bench[:22],
    }


def _derive_lineup_profile(absences: List[Dict[str, Any]]) -> Dict[str, str]:
    confirmed = 0
    key_nodes = 0
    for row in absences:
        if not isinstance(row, dict):
            continue
        status = _safe_text(row.get("status")).upper()
        if status in {"OUT", "SUSPENDED", "DOUBTFUL"}:
            confirmed += 1
            if bool(row.get("key_node")):
                key_nodes += 1
    if key_nodes >= 2 or confirmed >= 6:
        return {
            "expected_core_availability": "DAMAGED",
            "lineup_stability_precheck": "RED",
            "key_node_absence_risk": "CRITICAL" if key_nodes >= 2 else "HIGH",
        }
    if confirmed >= 3:
        return {
            "expected_core_availability": "PARTIAL",
            "lineup_stability_precheck": "YELLOW",
            "key_node_absence_risk": "MEDIUM",
        }
    if confirmed >= 1:
        return {
            "expected_core_availability": "MOSTLY_AVAILABLE",
            "lineup_stability_precheck": "YELLOW",
            "key_node_absence_risk": "LOW",
        }
    return {
        "expected_core_availability": "FULL",
        "lineup_stability_precheck": "GREEN",
        "key_node_absence_risk": "LOW",
    }


def _derive_rotation_intensity(last_match_lineup_snapshot: Dict[str, Any], absences: List[Dict[str, Any]]) -> Dict[str, Any]:
    starters = last_match_lineup_snapshot.get("starting_xi") if isinstance(last_match_lineup_snapshot, dict) else []
    starter_count = len(starters) if isinstance(starters, list) else 0
    absence_count = len(absences) if isinstance(absences, list) else 0
    triggers: List[str] = []
    level = "UNKNOWN"
    if starter_count >= 11 and absence_count >= 4:
        level = "HIGH"
        triggers.append("last_match_starters_with_multi_absence")
    elif starter_count >= 11 and absence_count >= 2:
        level = "MEDIUM"
        triggers.append("last_match_starters_with_some_absence")
    elif starter_count >= 11:
        level = "LOW"
        triggers.append("last_match_starting_xi_available")
    return {
        "rotation_intensity": level,
        "rotation_triggers": triggers,
        "confidence": "LOW" if level == "UNKNOWN" else "MEDIUM",
    }


def _default_team_payload(team: str, league: str) -> Dict[str, Any]:
    return {
        "team": team,
        "league": league,
        "source_items": [],
        "absences": [],
        "expected_core_availability": "UNKNOWN",
        "lineup_stability_precheck": "UNKNOWN",
        "key_node_absence_risk": "UNKNOWN",
        "last_match_lineup_snapshot": {},
        "lineup_snapshot_status": "UNKNOWN",
        "lineup_rotation_signals": {},
        "recent_news_summary": "",
        "key_node_dependency": [],
        "prematch_focus_items": [],
        "market_external_notes": [],
        "youtube_tactical_briefs": [],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect injury/lineup intel into TEAM-INTEL for one issue.")
    parser.add_argument("--issue", required=True, help="Issue id, e.g. DATE-20260504-top5")
    parser.add_argument("--merge", action="store_true", help="Merge with existing TEAM-INTEL-<issue>.json")
    parser.add_argument("--no-transfermarkt", action="store_true", help="Disable Transfermarkt web fetch")
    parser.add_argument("--titan-html-dir", default="", help="离线 Titan HTML 目录（文件名: <cn_match_id>.html）")
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent.parent.parent
    load_dotenv_into_env(base_dir)
    vault_env = os.getenv("ARES_VAULT_PATH")
    if not vault_env:
        raise EnvironmentError("未检测到 ARES_VAULT_PATH。")
    vault_root = Path(normalize_vault_path(vault_env)).expanduser()

    manifest = _load_manifest(vault_root, args.issue)
    team_map = _team_league_map(base_dir, vault_root, args.issue, manifest)
    team_match_urls = _build_team_match_urls(manifest)
    titan_html_dir = Path(args.titan_html_dir).expanduser() if str(args.titan_html_dir or "").strip() else None

    review_dir = vault_root / "03_Match_Audits" / str(args.issue) / "03_Review_Reports"
    review_dir.mkdir(parents=True, exist_ok=True)
    out_path = review_dir / f"TEAM-INTEL-{args.issue}.json"
    existing = _load_existing(out_path) if args.merge else {}

    out_teams: List[Dict[str, Any]] = []
    tm_ok = 0

    for team in sorted(team_map):
        league = team_map[team]
        seed = _default_team_payload(team, league)
        if team in existing:
            seed.update(existing[team])
            seed["source_items"] = existing[team].get("source_items", [])
            seed["absences"] = existing[team].get("absences", [])

        archive_absences, archive_lineup, archive_path = _extract_archive_intel(vault_root, team, league)
        merged_abs = _merge_absences(seed.get("absences", []), archive_absences)
        source_updates = [
            {
                "source_name": "TeamArchive",
                "url": archive_path,
                "fetched_at": _utc_now(),
                "reliability": "HIGH" if archive_path else "UNKNOWN",
                "raw_status": "OK" if archive_path else "MISSING_ARCHIVE",
            }
        ]

        match_urls = team_match_urls.get(team, {})
        nowscore_url = _safe_text(match_urls.get("nowscore_url"))
        titan_url = _safe_text(match_urls.get("titan_url"))
        if nowscore_url:
            ns_abs, ns_src = _fetch_cn_absence_signals(
                "Nowscore",
                nowscore_url,
                raw_dump_dir=vault_root / "04_RAG_Raw_Data" / "Cold_Data_Lake",
                raw_dump_name=f"{args.issue}_nowscore_{team.replace(' ', '_')}",
            )
            source_updates.append(ns_src)
            merged_abs = _merge_absences(merged_abs, ns_abs)
        else:
            source_updates.append(
                {
                    "source_name": "Nowscore",
                    "url": "",
                    "fetched_at": _utc_now(),
                    "reliability": "UNKNOWN",
                    "raw_status": "MISSING_MATCH_ID",
                }
            )

        titan_lineup_snapshot: Dict[str, Any] = {}
        if titan_url:
            tt_abs, tt_src = _fetch_cn_absence_signals(
                "Titan007",
                titan_url,
                raw_dump_dir=vault_root / "04_RAG_Raw_Data" / "Cold_Data_Lake",
                raw_dump_name=f"{args.issue}_titan_{team.replace(' ', '_')}",
            )
            source_updates.append(tt_src)
            merged_abs = _merge_absences(merged_abs, tt_abs)
            if isinstance(tt_src.get("last_match_lineup_snapshot"), dict):
                titan_lineup_snapshot = tt_src.get("last_match_lineup_snapshot", {})
            seed["lineup_snapshot_status"] = str(tt_src.get("lineup_snapshot_status") or "UNKNOWN").upper()
        else:
            source_updates.append(
                {
                    "source_name": "Titan007",
                    "url": "",
                    "fetched_at": _utc_now(),
                    "reliability": "UNKNOWN",
                    "raw_status": "MISSING_MATCH_ID",
                }
            )
            seed["lineup_snapshot_status"] = "UNKNOWN"

        if not titan_lineup_snapshot:
            cn_match_id = _extract_titan_match_id_from_url(titan_url)
            seeded_snapshot = _load_seeded_titan_snapshot(titan_html_dir, cn_match_id)
            if seeded_snapshot:
                titan_lineup_snapshot = seeded_snapshot
                seed["lineup_snapshot_status"] = "SEEDED"

        if not args.no_transfermarkt:
            tm_abs, tm_source = _fetch_transfermarkt_absences(team)
            source_updates.append(tm_source)
            if _safe_text(tm_source.get("raw_status")) == "OK":
                tm_ok += 1
            merged_abs = _merge_absences(merged_abs, tm_abs)

        seed["source_items"] = _merge_source_items(seed.get("source_items", []), source_updates)
        seed["absences"] = merged_abs
        if titan_lineup_snapshot:
            seed["last_match_lineup_snapshot"] = titan_lineup_snapshot
        raw_status = str(seed.get("lineup_snapshot_status") or "UNKNOWN").upper()
        seed["lineup_snapshot_status"] = raw_status if raw_status in LINEUP_SNAPSHOT_STATUS_ENUMS else "UNKNOWN"

        for key in LINEUP_ENUMS:
            if seed.get(key) in (None, "", "UNKNOWN"):
                seed[key] = archive_lineup[key]
            else:
                seed[key] = _normalize_lineup_value(key, seed.get(key))
        if all(seed.get(k) in (None, "", "UNKNOWN") for k in LINEUP_ENUMS):
            derived = _derive_lineup_profile(merged_abs)
            for k, v in derived.items():
                seed[k] = v
        seed["lineup_rotation_signals"] = _derive_rotation_intensity(
            seed.get("last_match_lineup_snapshot") if isinstance(seed.get("last_match_lineup_snapshot"), dict) else {},
            merged_abs,
        )

        out_teams.append(seed)

    payload = {
        "issue": args.issue,
        "updated_at": _utc_now(),
        "source": "injury_lineup_intel_collect.py",
        "description": "P0.3.2 auto collect: TeamArchive baseline + optional Transfermarkt probe.",
        "teams": out_teams,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print("[summary]")
    print(f"issue={args.issue}")
    print(f"teams={len(out_teams)}")
    print(f"transfermarkt_ok={tm_ok}")
    print(f"output={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
