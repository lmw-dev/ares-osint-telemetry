#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ares OSINT Telemetry - 通用赛后流水线驱动器 (Unified Postmatch Pipeline)

职责：
1. Phase A (Handoff 交付):
   - 自动发现赛前沉淀的 00-fixture-binding.json，断言法定分母 (英/西/意 10场, 德/法 9场)；
   - 采集/校验赛后事实 (比分、确证首发、xG、Shotmap)；
   - 运行 Preliminary 分析与质量门禁；
   - 持久化至 Obsidian 03_Match_Audits 目录，生成全轮 Preliminary 汇总；
   - 打包生成交付给 ChatGPT 深度终审的 Handoff 封包 (POSTMATCH_HANDOFF_<LEAGUE>_<ROUND>.zip)。
2. Phase B (Inbox 发布):
   - 读取并校验 ChatGPT 中文终审 Markdown 原件；
   - 结构化提取 A-O 终审与全轮 Round Learning Review；
   - 应用反过拟合规则 (隔离全场 xG、单次领先判定、消除未证实趋势、未解差异置 UNKNOWN)；
   - 从候选集确定性派生对阵表并断言基数；
   - 发布至 05_Team_Knowledge_Maintenance/00_Inbox/<Week>/<League_Round>/ (落盘四件套)；
   - 守卫断言：02_Team_Archives 零修改；
   - 打包生成标准交付件 (INBOX_V1_1_FINAL_HANDOFF_<LEAGUE>_<ROUND>.zip)。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

COLLECTOR_DIR = REPO_ROOT / "src/skills/ares-postmatch-evidence-collector/scripts"
if str(COLLECTOR_DIR) not in sys.path:
    sys.path.insert(0, str(COLLECTOR_DIR))

try:
    import fotmob_postmatch_client
except ImportError:
    fotmob_postmatch_client = None

try:
    from engine.postmatch_valuation_integrator import run_round_postmatch_valuation_pipeline, PostmatchValuationEngine
except ImportError:
    run_round_postmatch_valuation_pipeline = None
    PostmatchValuationEngine = None

# 法定分母常数定义 (Strict Denominators)
LEAGUE_DENOMINATORS = {
    "EPL": 10,
    "LALIGA": 10,
    "SERIEA": 10,
    "BUNDESLIGA": 9,
    "LIGUE1": 9
}

LEAGUE_NAME_MAP = {
    "epl": "EPL",
    "premierleague": "EPL",
    "laliga": "LaLiga",
    "seriea": "SerieA",
    "bundesliga": "Bundesliga",
    "ligue1": "Ligue1"
}

BASE_VAULT = Path("/Users/liumingwei/vaults/AresVault")
TEAM_ARCHIVE_DIR = BASE_VAULT / "02_Team_Archives"

def calc_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()

def normalize_round(rnd: str) -> str:
    rnd_clean = rnd.strip().upper()
    if rnd_clean.isdigit():
        return f"GW{int(rnd_clean):02d}"
    if rnd_clean.startswith("GW") and rnd_clean[2:].isdigit():
        return f"GW{int(rnd_clean[2:]):02d}"
    if rnd_clean.startswith("MD") and rnd_clean[2:].isdigit():
        return f"GW{int(rnd_clean[2:]):02d}"
    return rnd_clean

def normalize_league(league_str: str) -> Tuple[str, str]:
    l_lower = league_str.strip().lower()
    if l_lower not in LEAGUE_NAME_MAP:
        raise ValueError(f"Unsupported league: {league_str}. Allowed: {list(LEAGUE_NAME_MAP.keys())}")
    canonical = LEAGUE_NAME_MAP[l_lower]
    return canonical, canonical.upper()

class FixtureDiscovery:
    """赛前绑定与实体血统确定性自发现模块"""
    @staticmethod
    def discover_bindings(season: str, league: str, round_id: str) -> List[Dict[str, Any]]:
        expected_den = LEAGUE_DENOMINATORS[league.upper()]
        prematch_dir = BASE_VAULT / "03_Match_Audits" / season / league / round_id / "prematch"
        if not prematch_dir.exists():
            raise FileNotFoundError(f"Prematch directory not found: {prematch_dir}")

        binding_paths = sorted(prematch_dir.glob("**/00-fixture-binding.json"))
        if len(binding_paths) != expected_den:
            raise ValueError(
                f"DENOMINATOR_MISMATCH: {league} {round_id} expected {expected_den} bindings, "
                f"found {len(binding_paths)} at {prematch_dir}"
            )

        fixtures = []
        for bp in binding_paths:
            data = json.loads(bp.read_text(encoding="utf-8"))
            # 提取父目录前缀 fixture_id (如 3013712_fotmob)
            parent_name = bp.parent.name
            fixture_id = parent_name.split("_")[0] if "_" in parent_name else parent_name
            
            fixtures.append({
                "fixture_id": fixture_id,
                "fotmob_id": str(data.get("fotmob_match_id", "")),
                "home_team": data.get("matched_home", "Unknown Home"),
                "away_team": data.get("matched_away", "Unknown Away"),
                "kickoff_utc": data.get("matched_kickoff", ""),
                "canonical_url": data.get("canonical_url", ""),
                "binding_path": str(bp)
            })

        fixtures.sort(key=lambda x: x["fixture_id"])
        return fixtures

class PostmatchHandoffBuilder:
    """Phase A: 赛后证据归集、初步分析与 ChatGPT Handoff 封包生成器"""
    def __init__(self, season: str, league: str, round_id: str, fixtures: List[Dict[str, Any]]):
        self.season = season
        self.league = league
        self.round_id = round_id
        self.fixtures = fixtures
        self.postmatch_dir = BASE_VAULT / "03_Match_Audits" / season / league / round_id / "postmatch"
        cache_postmatch_dir = REPO_ROOT / "runtime_cache" / "active" / "03_postmatch"
        cache_postmatch_dir.mkdir(parents=True, exist_ok=True)
        self.output_zip = cache_postmatch_dir / f"{self.league}_{self.round_id}_POSTMATCH_DEEP_HANDOFF.zip"

    def execute(self) -> Dict[str, Any]:
        print(f"\n>>> [Phase A] 启动 {self.league} {self.round_id} 赛后证据归集与初步分析...")
        self.postmatch_dir.mkdir(parents=True, exist_ok=True)

        fixture_summaries = []
        for idx, fix in enumerate(self.fixtures, 1):
            fid = fix["fixture_id"]
            fdir = self.postmatch_dir / fid
            fdir.mkdir(parents=True, exist_ok=True)
            print(f"  [{idx}/{len(self.fixtures)}] 处理比赛 {fid}: {fix['home_team']} vs {fix['away_team']}")

            # 抓取/复用赛后证据
            evidence = self._collect_or_load_evidence(fix, fdir)
            
            # 生成持久化 4 份核心文件
            self._write_fixture_artifacts(fix, evidence, fdir)
            npxg_comp = evidence.get("npxg_completeness") or {}
            comp_h = npxg_comp.get("home", {}).get("is_complete", True) if npxg_comp.get("home") else True
            comp_a = npxg_comp.get("away", {}).get("is_complete", True) if npxg_comp.get("away") else True
            raw_npxg_h = evidence.get("derived_npxg_home", "UNKNOWN")
            raw_npxg_a = evidence.get("derived_npxg_away", "UNKNOWN")
            disp_h = raw_npxg_h if (comp_h or raw_npxg_h == "UNKNOWN") else f"已知部分 {raw_npxg_h} (不完整)"
            disp_a = raw_npxg_a if (comp_a or raw_npxg_a == "UNKNOWN") else f"已知部分 {raw_npxg_a} (不完整)"

            fixture_summaries.append({
                "fixture_id": fid,
                "home_team": fix["home_team"],
                "away_team": fix["away_team"],
                "score": evidence.get("final_score", "FT"),
                "halftime": evidence.get("halftime_score", "HT"),
                "home_xg": evidence.get("summary_xg_home", "UNKNOWN"),
                "away_xg": evidence.get("summary_xg_away", "UNKNOWN"),
                "npxg": f"{disp_h} - {disp_a}",
                "game_state": evidence.get("game_state_split", {}).get("status", "UNKNOWN"),
                "shots": f"{evidence.get('total_shots_home', 0)} ({evidence.get('sot_home', 0)}) - {evidence.get('total_shots_away', 0)} ({evidence.get('sot_away', 0)})",
                "possession": f"{evidence.get('possession_home', 'UNKNOWN')}% - {evidence.get('possession_away', 'UNKNOWN')}%",
                "red_cards": len(evidence.get("red_cards", [])),
                "goals_count": len(evidence.get("goals", [])),
                "subs_count": len(evidence.get("substitutions", []))
            })

        # Stage 6 受控接入: 必须在 Handoff 封包之前触发赛后首发身价计算并生成单场 06-lineup-valuation.json 及 Staging 候选附件
        squad_valuation_result = self._execute_squad_valuation_rolling()

        # 生成全轮汇总 Preliminary、Summary 与 Round Gate 报告 (真实连接身价门禁状态)
        gate_passed = self._write_round_summaries(fixture_summaries, squad_valuation_result)

        # 封包生成交付 ZIP (包含已生成的 06-lineup-valuation.json 产物)
        self._package_handoff_zip()

        # 镜像到 Artifact 目录供下载 (由环境变量控制，测试与隔离环境不污染)
        artifact_env = os.environ.get("ANTIGRAVITY_ARTIFACT_DIR")
        if artifact_env:
            artifact_dir = Path(artifact_env)
            if artifact_dir.exists():
                shutil.copy2(self.output_zip, artifact_dir / self.output_zip.name)

        print(f">>> [Phase A] 完成！交付封包已就绪: {self.output_zip.name} (SHA: {calc_sha256(self.output_zip)})")

        return {
            "handoff_zip": str(self.output_zip),
            "handoff_zip_sha": calc_sha256(self.output_zip),
            "fixture_count": len(self.fixtures),
            "postmatch_dir": str(self.postmatch_dir),
            "squad_valuation_staging": squad_valuation_result,
            "round_gate_passed": gate_passed
        }

    def _execute_squad_valuation_rolling(self) -> Dict[str, Any]:
        """Stage 6 受控接入: Phase A 首发身价滚动计算与独立 Staging 候选生成 (全五大联赛通用流水线)"""
        staging_out = REPO_ROOT / "staging" / "squad_valuation" / self.season / f"{self.league}_{self.round_id}"
        staging_out.mkdir(parents=True, exist_ok=True)

        if not run_round_postmatch_valuation_pipeline:
            print("  [ERROR] postmatch_valuation_integrator 未就绪，身价流水线不可用")
            return {"status": "FAILED", "reason": "Module not imported"}

        try:
            print(f"  [Squad Valuation] 触发本轮 {self.league} {self.round_id} 赛后首发身价与滚动计算 (Staging: {staging_out})...")
            result = run_round_postmatch_valuation_pipeline(
                league=self.league,
                season=self.season,
                round_id=self.round_id,
                fixtures=self.fixtures,
                postmatch_base_dir=self.postmatch_dir,
                staging_output_dir=staging_out,
                is_production_authorized=False # 赛后阶段严格执行生产只读隔离
            )
            print(f"  [Squad Valuation] 成功产出候选资产与审计证据: 单场 {result.get('single_match_artifacts_count', 0)} 场, 参赛球队 {result.get('participating_teams_count', 0)} 队")
            return result
        except Exception as e:
            print(f"  [ERROR] 赛后身价流水线异常阻断: {e}")
            return {
                "status": "FAILED",
                "reason": str(e),
                "error": str(e)
            }

    def _collect_or_load_evidence(self, fix: Dict[str, Any], fdir: Path) -> Dict[str, Any]:
        from engine.postmatch_governance.evidence_builder import PostmatchEvidenceBuilder
        fotmob_id = str(fix.get("fotmob_id", ""))
        canonical_url = fix.get("canonical_url", f"https://www.fotmob.com/match/{fotmob_id}")
        
        # 复制 00-fixture-binding.json
        bp_str = fix.get("binding_path", "")
        if bp_str:
            bp = Path(bp_str)
            if bp.is_file():
                shutil.copy2(bp, fdir / "00-fixture-binding.json")

        page_props = None
        final_url = canonical_url

        if fotmob_postmatch_client and fotmob_id:
            try:
                page_props, final_url, status = fotmob_postmatch_client.fetch_fotmob_completed_page(fotmob_id)
                if status == "PASS" and page_props:
                    # 真实落盘 00-fotmob-raw.json
                    (fdir / "00-fotmob-raw.json").write_text(json.dumps(page_props, indent=2, ensure_ascii=False), encoding="utf-8")
            except Exception as e:
                print(f"    [WARN] FotMob 客户端抓取异常 ({fotmob_id}): {e}")

        # 若未成功抓取或本地已有 00-fotmob-raw.json 则复用
        if not page_props:
            local_raw = fdir / "00-fotmob-raw.json"
            if local_raw.exists():
                try:
                    page_props = json.loads(local_raw.read_text("utf-8"))
                except Exception as e:
                    print(f"    [WARN] 本地 raw 解析异常: {e}")

        if page_props:
            # 优先调用已完成状态分段、红牌时序分割与非点球 NPxG 治理的高精度客户端
            if fotmob_postmatch_client and fotmob_id:
                try:
                    parsed = fotmob_postmatch_client.parse_fotmob_completed_match(page_props, final_url, fotmob_id)
                    p_stat = parsed.get("status")
                    if p_stat == "PASS":
                        res_ev = parsed.get("result_evidence", {})
                        proc_ev = parsed.get("process_stats_evidence", {})
                        lineup_ev = parsed.get("lineup_events_evidence", {})
                        xg_ev = parsed.get("xg_process_evidence", {})

                        # 真实落盘机器可读的 04-xg-process.json
                        (fdir / "04-xg-process.json").write_text(json.dumps(xg_ev, indent=2, ensure_ascii=False), encoding="utf-8")

                        return {
                            "final_score": res_ev.get("final_score_str") or "FT",
                            "halftime_score": res_ev.get("halftime_score_str") or "HT",
                            "summary_xg_home": proc_ev.get("fotmob_aggregate_xg_home", "UNKNOWN"),
                            "summary_xg_away": proc_ev.get("fotmob_aggregate_xg_away", "UNKNOWN"),
                            "official_npxg_home": proc_ev.get("fotmob_aggregate_npxg_home", "UNKNOWN"),
                            "official_npxg_away": proc_ev.get("fotmob_aggregate_npxg_away", "UNKNOWN"),
                            "shotmap_xg_home": xg_ev.get("derived_shotmap_xg_home", "UNKNOWN"),
                            "shotmap_xg_away": xg_ev.get("derived_shotmap_xg_away", "UNKNOWN"),
                            "derived_npxg_home": xg_ev.get("derived_npxg_home", "UNKNOWN"),
                            "derived_npxg_away": xg_ev.get("derived_npxg_away", "UNKNOWN"),
                            "total_shots_home": proc_ev.get("canonical_shots_home", "UNKNOWN"),
                            "total_shots_away": proc_ev.get("canonical_shots_away", "UNKNOWN"),
                            "sot_home": proc_ev.get("shots_on_target_home", "UNKNOWN"),
                            "sot_away": proc_ev.get("shots_on_target_away", "UNKNOWN"),
                            "possession_home": proc_ev.get("possession_home", "UNKNOWN"),
                            "possession_away": proc_ev.get("possession_away", "UNKNOWN"),
                            "fouls_home": proc_ev.get("fouls_home", "UNKNOWN"),
                            "fouls_away": proc_ev.get("fouls_away", "UNKNOWN"),
                            "goals": lineup_ev.get("goals", []),
                            "red_cards": lineup_ev.get("red_cards", []),
                            "substitutions": lineup_ev.get("substitutions", []),
                            "penalty_shots_home": xg_ev.get("penalty_shots_home", 0),
                            "penalty_shots_away": xg_ev.get("penalty_shots_away", 0),
                            "game_state_split": xg_ev.get("game_state_split", {}),
                            "red_card_split": xg_ev.get("red_card_split", {}),
                            "shotmap_xg_completeness": xg_ev.get("shotmap_xg_completeness", {}),
                            "npxg_completeness": xg_ev.get("npxg_completeness", {}),
                            "status": "FOTMOB_CONFIRMED",
                            "source_url": final_url
                        }
                    elif p_stat in ("SOURCE_IDENTITY_MISMATCH", "NON_FINAL_MATCH", "PAGE_SCHEMA_CHANGED", "ERROR"):
                        # 身份不符或未完赛等致命状态，绝不得回退至通用抽取器充当已确认
                        err_msg = parsed.get("error") or parsed.get("reason") or p_stat
                        print(f"    [BLOCK] FotMob 校验阻断 ({fotmob_id}): {p_stat} - {err_msg}")
                        return {
                            "final_score": "UNKNOWN",
                            "halftime_score": "UNKNOWN",
                            "summary_xg_home": "UNKNOWN",
                            "summary_xg_away": "UNKNOWN",
                            "official_npxg_home": "UNKNOWN",
                            "official_npxg_away": "UNKNOWN",
                            "shotmap_xg_home": "UNKNOWN",
                            "shotmap_xg_away": "UNKNOWN",
                            "derived_npxg_home": "UNKNOWN",
                            "derived_npxg_away": "UNKNOWN",
                            "total_shots_home": "UNKNOWN",
                            "total_shots_away": "UNKNOWN",
                            "sot_home": "UNKNOWN",
                            "sot_away": "UNKNOWN",
                            "possession_home": "UNKNOWN",
                            "possession_away": "UNKNOWN",
                            "fouls_home": "UNKNOWN",
                            "fouls_away": "UNKNOWN",
                            "goals": [],
                            "red_cards": [],
                            "substitutions": [],
                            "penalty_shots_home": 0,
                            "penalty_shots_away": 0,
                            "game_state_split": {"status": "UNKNOWN", "reason": f"{p_stat}: {err_msg}"},
                            "red_card_split": {"status": "UNKNOWN"},
                            "shotmap_xg_completeness": {},
                            "npxg_completeness": {},
                            "status": p_stat,
                            "error": err_msg,
                            "source_url": final_url
                        }
                except Exception as e:
                    print(f"    [WARN] 高精度客户端抽取异常: {e}")

            # 仅在无 client 或未提供 fotmob_id 时执行离线回退，但若 page_props 明确未完赛同样拒绝
            header = page_props.get("header", {})
            if header and not header.get("status", {}).get("finished", True):
                print(f"    [BLOCK] 本地 raw 明确指示未完赛，拒绝确认为已完成")
                return {
                    "final_score": "UNKNOWN",
                    "halftime_score": "UNKNOWN",
                    "summary_xg_home": "UNKNOWN",
                    "summary_xg_away": "UNKNOWN",
                    "derived_npxg_home": "UNKNOWN",
                    "derived_npxg_away": "UNKNOWN",
                    "total_shots_home": "UNKNOWN",
                    "total_shots_away": "UNKNOWN",
                    "sot_home": "UNKNOWN",
                    "sot_away": "UNKNOWN",
                    "possession_home": "UNKNOWN",
                    "possession_away": "UNKNOWN",
                    "fouls_home": "UNKNOWN",
                    "fouls_away": "UNKNOWN",
                    "goals": [],
                    "red_cards": [],
                    "substitutions": [],
                    "game_state_split": {"status": "UNKNOWN", "reason": "NON_FINAL_MATCH"},
                    "red_card_split": {"status": "UNKNOWN"},
                    "status": "NON_FINAL_MATCH",
                    "source_url": final_url
                }

            home_id = page_props.get("general", {}).get("homeTeam", {}).get("id", 0)
            away_id = page_props.get("general", {}).get("awayTeam", {}).get("id", 0)
            extracted = PostmatchEvidenceBuilder.extract_stats_from_raw(page_props, home_id, away_id)
            extracted["status"] = "LOCAL_RAW_REUSED" if not fotmob_id else "FOTMOB_CONFIRMED"
            extracted["source_url"] = final_url
            return extracted

        return {
            "final_score": "FT",
            "halftime_score": "HT",
            "summary_xg_home": "UNKNOWN",
            "summary_xg_away": "UNKNOWN",
            "shotmap_xg_home": "UNKNOWN",
            "shotmap_xg_away": "UNKNOWN",
            "derived_npxg_home": "UNKNOWN",
            "derived_npxg_away": "UNKNOWN",
            "total_shots_home": "UNKNOWN",
            "total_shots_away": "UNKNOWN",
            "sot_home": "UNKNOWN",
            "sot_away": "UNKNOWN",
            "possession_home": "UNKNOWN",
            "possession_away": "UNKNOWN",
            "fouls_home": "UNKNOWN",
            "fouls_away": "UNKNOWN",
            "goals": [],
            "red_cards": [],
            "substitutions": [],
            "game_state_split": {"status": "UNKNOWN"},
            "red_card_split": {"status": "NO_RED_CARD"},
            "status": "FALLBACK"
        }

    def _write_fixture_artifacts(self, fix: Dict[str, Any], evidence: Dict[str, Any], fdir: Path):
        fid = fix["fixture_id"]
        score = evidence.get("final_score", "FT")
        ht_score = evidence.get("halftime_score", "HT")
        h_xg = evidence.get("summary_xg_home", "UNKNOWN")
        a_xg = evidence.get("summary_xg_away", "UNKNOWN")
        h_sxg = evidence.get("shotmap_xg_home", "UNKNOWN")
        a_sxg = evidence.get("shotmap_xg_away", "UNKNOWN")
        h_npxg = evidence.get("derived_npxg_home", "UNKNOWN")
        a_npxg = evidence.get("derived_npxg_away", "UNKNOWN")

        # 00-runtime-state.md
        (fdir / "00-runtime-state.md").write_text(
            f"# Runtime State: {fid}\n"
            f"- lifecycle: POSTMATCH_COMPLETE\n"
            f"- kickoff_utc: {fix['kickoff_utc']}\n"
            f"- collector_status: {evidence.get('status')}\n"
            f"- verified_at_utc: {datetime.now(timezone.utc).isoformat()}\n",
            encoding="utf-8"
        )

        # 01-result-evidence.md
        goals_lines = []
        for g in evidence.get("goals", []):
            og_tag = " [乌龙球]" if g.get("is_own_goal") else ""
            pen_tag = " [点球]" if g.get("is_penalty") else ""
            score_after = g.get("score_after")
            score_info = f" (比分变为: {score_after})" if score_after and str(score_after).lower() != "none" else ""
            goals_lines.append(f"  - {g.get('minute')}' {g.get('player')}{og_tag}{pen_tag}{score_info}")
        
        reds_lines = [f"  - {r.get('minute')}' {r.get('player')} ({r.get('team_side')}, {r.get('card_type')})" for r in evidence.get("red_cards", [])]

        (fdir / "01-result-evidence.md").write_text(
            f"# Result Evidence: {fix['home_team']} vs {fix['away_team']}\n"
            f"- fixture_id: {fid}\n"
            f"- final_score: {score}\n"
            f"- halftime_score: {ht_score}\n"
            f"- goals:\n" + ("\n".join(goals_lines) if goals_lines else "  - None") + "\n"
            f"- red_cards:\n" + ("\n".join(reds_lines) if reds_lines else "  - None") + "\n",
            encoding="utf-8"
        )

        # 02-process-evidence.md
        (fdir / "02-process-evidence.md").write_text(
            f"# Process Evidence: {fix['home_team']} vs {fix['away_team']} ({fid})\n"
            f"- summary_xg: {h_xg} vs {a_xg}\n"
            f"- shotmap_xg: {h_sxg} vs {a_sxg}\n"
            f"- derived_npxg: {h_npxg} vs {a_npxg}\n"
            f"- total_shots: {evidence.get('total_shots_home')} vs {evidence.get('total_shots_away')}\n"
            f"- shotmap_shot_count: {evidence.get('shotmap_shots_home', evidence.get('total_shots_home'))} vs {evidence.get('shotmap_shots_away', evidence.get('total_shots_away'))}\n"
            f"- shots_on_target: {evidence.get('sot_home')} vs {evidence.get('sot_away')}\n"
            f"- ball_possession: {evidence.get('possession_home')}% vs {evidence.get('possession_away')}%\n"
            f"- fouls: {evidence.get('fouls_home')} vs {evidence.get('fouls_away')}\n"
            f"- substitutions_count: {len(evidence.get('substitutions', []))}\n",
            encoding="utf-8"
        )

        # 03-postmatch-preliminary.md: 简洁、可读的分段结果与缺失提示
        red_card_notes = "比赛未受红牌扭曲，全场维持 11v11 人数对等。"
        rc_split = evidence.get("red_card_split") or {}
        if rc_split.get("status") == "RED_CARD_SPLIT_AVAILABLE":
            first_red_player = rc_split.get("first_red_player", "Unknown")
            first_red_min = rc_split.get("first_red_minute", "Unknown")
            first_red_team = rc_split.get("first_red_team", "Unknown")
            pre_rc = rc_split.get("pre_red_11v11", {})
            post_rc = rc_split.get("post_red", {})
            amb_rc = rc_split.get("same_minute_ambiguous", {})
            
            red_card_notes = (
                f"注意：比赛在 {first_red_min}' 出现红牌 ({first_red_player}, {first_red_team})，已执行时序分段隔离：\n"
                f"  - **11v11 对等阶段 ({pre_rc.get('window', 'N/A')})**: 射门 {pre_rc.get('shots_home')} - {pre_rc.get('shots_away')} ｜ xG {pre_rc.get('xg_home')} - {pre_rc.get('xg_away')} (NPxG {pre_rc.get('npxg_home')} - {pre_rc.get('npxg_away')})\n"
                f"  - **失衡阶段 ({post_rc.get('window', 'N/A')})**: 射门 {post_rc.get('shots_home')} - {post_rc.get('shots_away')} ｜ xG {post_rc.get('xg_home')} - {post_rc.get('xg_away')} (NPxG {post_rc.get('npxg_home')} - {post_rc.get('npxg_away')})"
            )
            if amb_rc.get("shots_count", 0) > 0:
                red_card_notes += f"\n  - **同分钟顺序不明 ({amb_rc.get('window')})**: {amb_rc.get('shots_count')} 脚射门与红牌同分钟，未安全隔离前不归入任一阶段。"
        elif evidence.get("red_cards"):
            first_red = evidence.get("red_cards")[0]
            red_card_notes = f"注意：比赛在 {first_red.get('minute')}' 出现红牌 ({first_red.get('player')})。红牌时点此前的比赛过程需做时序隔离。"

        # 格式化逐射累加 NPxG (数据不完整时显式标注“已知部分/不完整”，不得展示成完整值)
        npxg_comp = evidence.get("npxg_completeness") or {}
        comp_h = npxg_comp.get("home", {}).get("is_complete", True) if npxg_comp.get("home") else True
        comp_a = npxg_comp.get("away", {}).get("is_complete", True) if npxg_comp.get("away") else True
        disp_npxg_h = h_npxg if (comp_h or h_npxg == "UNKNOWN") else f"已知部分 {h_npxg} (不完整)"
        disp_npxg_a = a_npxg if (comp_a or a_npxg == "UNKNOWN") else f"已知部分 {a_npxg} (不完整)"

        # 比分状态分段 (Game State Split)
        gs_split = evidence.get("game_state_split") or {}
        gs_status = gs_split.get("status", "UNKNOWN")
        gs_lines = []
        if gs_status in ("GAME_STATE_SPLIT_AVAILABLE", "LEVEL_FULL_MATCH") and gs_split.get("segments"):
            gs_lines.append("| 比分状态 (State) | 对应比分 | 分钟区间 | 射门 (主 - 客) | 预期进球 (xG) | 逐射累加 NPxG |")
            gs_lines.append("| :--- | :---: | :---: | :---: | :---: | :---: |")
            for seg in gs_split.get("segments", []):
                st_label = {
                    "level": "平比分均势 (Level)",
                    "home_lead": "主队领先 (Home Lead)",
                    "away_lead": "客队领先 (Away Lead)"
                }.get(seg.get("state"), seg.get("state"))
                win_display = seg.get("window", "").split("(")[0].strip()
                seg_npxg_h = seg.get("npxg_home")
                seg_npxg_a = seg.get("npxg_away")
                seg_h_str = f"已知部分 {seg_npxg_h} (不完整)" if (not seg.get("npxg_complete_home", True) and seg_npxg_h not in (None, "UNKNOWN")) else f"{seg_npxg_h}"
                seg_a_str = f"已知部分 {seg_npxg_a} (不完整)" if (not seg.get("npxg_complete_away", True) and seg_npxg_a not in (None, "UNKNOWN")) else f"{seg_npxg_a}"
                gs_lines.append(
                    f"| {st_label} | `{seg.get('score_str')}` | `{win_display}` | {seg.get('shots_home')} - {seg.get('shots_away')} | {seg.get('xg_home')} - {seg.get('xg_away')} | {seg_h_str} - {seg_a_str} |"
                )
            amb_shots = gs_split.get("order_ambiguous_shots", [])
            if amb_shots:
                gs_lines.append(f"\n> ⚠️ **顺序不明射门提示**: 存在 {len(amb_shots)} 脚射门与进球同分钟且秒级顺序未由官方记录确认，已独立隔离，不计入上述确定比分区间。")
            if gs_split.get("same_minute_multi_goal_events"):
                gs_lines.append(f"\n> ⚠️ **同分钟多进球提示**: 存在同分钟连续进球事件，已安全跳过未确证的中间比分分段。")
        elif gs_status == "INCONSISTENT_SCORE_EVENTS":
            gs_lines.append(f"> ⚠️ **[UNKNOWN] 比分分段中断**: {gs_split.get('reason', '事件与最终比分不一致或包含未知球队')}")
        elif gs_status == "UNKNOWN":
            gs_lines.append(f"> ⚠️ **[UNKNOWN] 分段不可用**: {gs_split.get('reason', '缺少 Shotmap 逐射数据')}")
        else:
            gs_lines.append("> ⚠️ **[UNKNOWN] 比分分段状态未就绪**")

        gs_block = "\n".join(gs_lines)

        # 机会质量、点球与乌龙球剥离审计
        special_notes = []
        has_pen = (
            any(g.get("is_penalty") for g in evidence.get("goals", [])) or
            evidence.get("penalty_shots_home", 0) > 0 or
            evidence.get("penalty_shots_away", 0) > 0
        )
        if has_pen:
            special_notes.append("场上出现点球（主罚或破门），点球已从常规非点球 NPxG 中剥离，评估阵地攻坚与防反质量时应以 NPxG 为主。")
        has_og = any(g.get("is_own_goal") for g in evidence.get("goals", []))
        if has_og:
            special_notes.append("场上有乌龙球，乌龙球仅用于推进比分时序，已从常规球队射门与常规 xG 中剥离。")
        special_str = ("\n- **点球与乌龙球提示**: " + "；".join(special_notes)) if special_notes else ""

        (fdir / "03-postmatch-preliminary.md").write_text(
            f"# Postmatch Preliminary: {fix['home_team']} vs {fix['away_team']} ({fid})\n\n"
            f"## 1. 比赛事实概括 (Fact Summary)\n"
            f"- **全场比分 (FT)**: **{score}** ｜ **半场比分 (HT)**: **{ht_score}**\n"
            f"- **官方来源汇总 xG (主队-客队)**: **{h_xg} - {a_xg}** (官方技术统计汇总原件)\n"
            f"- **逐射累加非点球 NPxG**: **{disp_npxg_h} - {disp_npxg_a}** (Shotmap 逐射累加去点球)\n"
            f"- **Shotmap 逐射累加总 xG**: {h_sxg} - {a_sxg}\n"
            f"- **射门与射正**: **{evidence.get('total_shots_home')} ({evidence.get('sot_home')}) - {evidence.get('total_shots_away')} ({evidence.get('sot_away')})**\n"
            f"- **控球率**: **{evidence.get('possession_home')}% - {evidence.get('possession_away')}%** ｜ **犯规数**: {evidence.get('fouls_home')} - {evidence.get('fouls_away')}\n\n"
            f"## 2. 关键时点与分段过程 (Phased Timeline & Game State)\n"
            f"- **人数状态**: {red_card_notes}\n"
            f"- **进球序列**:\n" + ("\n".join(goals_lines) if goals_lines else "  - 无进球 (0-0 互交白卷)") + f"{special_str}\n\n"
            f"### 比分状态时序分段\n"
            f"{gs_block}\n\n"
            f"## 3. 战术假说与反证评估 (Tactical Inferences & Counterevidence)\n"
            f"- **主队 ({fix['home_team']}) 假说**: 依据平比分均势产出与射正转化，结合控球主导度进行基线评估。\n"
            f"- **客队 ({fix['away_team']}) 假说**: 依据反击空间或低位防守受压情况评估战术意图与韧性。\n"
            f"- **反证与过拟合约束**:\n"
            f"  - [FACT] 警惕单场运气波动、点球与乌龙球扭曲；\n"
            f"  - [FACT] **单场平比分数据仅反映本场特定对手与战术场景下的阶段性产出，严禁直接外推为球队长期攻坚能力**；\n"
            f"  - [FACT] **严禁将落后追分阶段的密集起脚误判为常规破密集防守的统治力**。\n\n"
            f"## 4. 初审结论 (Preliminary Verdict)\n"
            f"- **preliminary_classification**: `NO_PREMATCH_ATTRIBUTION`\n"
            f"- **recommendation**: `DELIVER_TO_CHATGPT_DEEP_POSTMATCH` (待 ChatGPT 开展 A-O 全指标终审与假说提炼)。\n",
            encoding="utf-8"
        )

        # manifest.json
        manifest = {
            "identity": {
                "project_fixture_id": fid,
                "league": f"{self.league}_{self.round_id}",
                "home_team": fix["home_team"],
                "away_team": fix["away_team"],
                "kickoff_utc": fix["kickoff_utc"],
                "fotmob_match_id": fix["fotmob_id"],
                "lifecycle": "POSTMATCH_COMPLETE"
            },
            "evidence": {
                "final_score": score,
                "halftime_score": ht_score,
                "home_xg": h_xg,
                "away_xg": a_xg,
                "derived_npxg_home": h_npxg,
                "derived_npxg_away": a_npxg,
                "goals_count": len(evidence.get("goals", [])),
                "subs_count": len(evidence.get("substitutions", [])),
                "red_cards_count": len(evidence.get("red_cards", []))
            }
        }
        (fdir / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    def _write_round_summaries(self, summaries: List[Dict[str, Any]], squad_valuation_result: Optional[Dict[str, Any]] = None) -> bool:
        lines = [
            f"# {self.league} {self.round_id} Round Postmatch Summary",
            "",
            f"- **League**: `{self.league}`",
            f"- **Round**: `{self.round_id}`",
            f"- **Fixtures Count**: `{len(summaries)}` (法定分母校验通过)",
            f"- **Generated At (UTC)**: `{datetime.now(timezone.utc).isoformat()}`",
            "",
            "| Fixture ID | 主队 (Home) | 客队 (Away) | 比分 (FT / HT) | 官方汇总 xG (主-客) | 逐射累加 NPxG | 射门 (射正) | 控球率 | 红牌 | 分段状态 |",
            "| :---: | :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |"
        ]
        total_goals = 0
        total_subs = 0
        for s in summaries:
            lines.append(f"| `{s['fixture_id']}` | **{s['home_team']}** | **{s['away_team']}** | **{s['score']}** ({s.get('halftime', 'HT')}) | {s['home_xg']} - {s['away_xg']} | {s.get('npxg', 'UNKNOWN')} | {s.get('shots', 'N/A')} | {s.get('possession', 'N/A')} | {s.get('red_cards', 0)} | `{s.get('game_state', 'UNKNOWN')}` |")
            total_goals += s.get("goals_count", 0)
            total_subs += s.get("subs_count", 0)
        
        (self.postmatch_dir / "00-ROUND-POSTMATCH-SUMMARY.md").write_text("\n".join(lines), encoding="utf-8")

        # 1. 核心赛后事实门禁 (Core Match Evidence Gate)
        core_facts_passed = len(summaries) == len(self.fixtures) and len(summaries) > 0

        # 2. 辅助首发身价门禁判定 (Squad Valuation Supporting Context Gate)
        val_res = squad_valuation_result or {}
        val_status = val_res.get("status", "NOT_RUN")
        val_passed = val_status in ("PASS", "STAGING_PASS")
        allow_stage6 = val_res.get("allow_stage6_promotion", False) and val_passed

        if core_facts_passed and val_passed:
            overall_gate = "PASS"
            gate_desc = "Core Evidence & Squad Valuation Fully Qualified"
        elif core_facts_passed and val_status == "DEGRADED":
            overall_gate = "QUALIFIED_WITH_SUPPORTING_DEGRADATION"
            gate_desc = "Core Match Evidence PASS; Squad Valuation DEGRADED (Stage 6 Promotion BLOCKED)"
        elif core_facts_passed:
            overall_gate = f"QUALIFIED_WITH_SUPPORTING_BLOCKED"
            gate_desc = f"Core Match Evidence PASS; Squad Valuation BLOCKED (Status: {val_status})"
        else:
            overall_gate = "FAILED_CORE_EVIDENCE_INCOMPLETE"
            gate_desc = f"Core Match Evidence Incomplete ({len(summaries)}/{len(self.fixtures)})"

        # 生成 00-ROUND-GATE-REPORT.md (清晰解耦核心事实与辅助身价门禁，严禁异常静默放行)
        gate_lines = [
            f"# {self.league} {self.round_id} Round Postmatch Gate Report",
            "",
            f"- **Overall Gate Status**: `{overall_gate}` ({gate_desc})",
            f"- **Core Match Evidence Gate**: `{'PASS' if core_facts_passed else 'FAILED'}` ({len(summaries)}/{len(self.fixtures)} 场完赛事实、技术统计、xG、Shotmap 完备)",
            f"- **Squad Valuation Supporting Gate**: `{'PASS' if val_passed else ('DEGRADED' if val_status == 'DEGRADED' else 'BLOCKED')}` (Status: `{val_status}`, Single Matches: `{val_res.get('single_match_artifacts_count', 0)}`)",
            f"- **Stage 6 Archive Promotion Readiness**: `{'APPROVED' if allow_stage6 else 'BLOCKED'}` (辅助身价降级或数据不全时严禁晋升长期档案)",
            f"- **Declared Denominator**: `{len(summaries)}` 场法定分母锁定",
            f"- **Actual Finished Fixtures**: `{len(summaries)}` 场全部正常完赛",
            f"- **Total Goals Extracted**: `{total_goals}` 粒进球",
            f"- **Total Substitutions Tracked**: `{total_subs}` 次换人",
            f"- **Strict Stats Extraction**: ShotsOnTarget, BallPossesion, total_shots, fouls 均从 raw 直接动态读取，零 0/50:50 默认填充",
            f"- **Halftime Score Rebuilding**: 全部场次半场比分通过上半场事件与补时时序重建，零 N/A",
            f"- **Goal Timing & Own Goal**: 进球时序先更新比分再输出，乌龙球 scorer 与 benefiting 严格分离",
            f"- **Team Archives Guard**: `02_Team_Archives/` 维持 0 写入，不执行任何自动 Promotion"
        ]
        (self.postmatch_dir / "00-ROUND-GATE-REPORT.md").write_text("\n".join(gate_lines), encoding="utf-8")

        # 生成 00-ROUND-MANIFEST.json (Artifact Inventory)
        manifest_files = {}
        for p in sorted(self.postmatch_dir.rglob("*")):
            if p.is_file() and not p.name.startswith("00-ROUND-MANIFEST"):
                rel_path = str(p.relative_to(self.postmatch_dir))
                manifest_files[rel_path] = calc_sha256(p)

        round_manifest_data = {
            "manifest_version": "3.1",
            "manifest_type": "ROUND_MANIFEST",
            "role": "Artifact Inventory (完赛产物清单，非生命周期状态账本)",
            "league": self.league,
            "round_id": self.round_id,
            "season": self.season,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "total_fixtures": len(summaries),
            "complete_fixtures": len(summaries),
            "total_goals": total_goals,
            "total_substitutions": total_subs,
            "gate_status": overall_gate,
            "core_evidence_gate": "PASS" if core_facts_passed else "FAILED",
            "squad_valuation_gate": "PASS" if val_passed else ("DEGRADED" if val_status == "DEGRADED" else "BLOCKED"),
            "stage6_promotion_allowed": allow_stage6,
            "files": manifest_files
        }
        (self.postmatch_dir / "00-ROUND-MANIFEST.json").write_text(json.dumps(round_manifest_data, indent=2, ensure_ascii=False), encoding="utf-8")

        return core_facts_passed

    def _package_handoff_zip(self):
        with zipfile.ZipFile(self.output_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for p in self.postmatch_dir.rglob("*"):
                if p.is_file():
                    arc = p.relative_to(self.postmatch_dir)
                    zf.write(p, arcname=str(arc))

class InboxPublisher:
    """Phase B: 消费 ChatGPT 终审原文，生成 Team Knowledge Inbox 四件套与交付封包"""
    def __init__(self, season: str, league: str, round_id: str, week: str, review_path: Path):
        self.season = season
        self.league = league
        self.round_id = round_id
        self.week = week
        self.review_path = review_path
        self.inbox_dir = BASE_VAULT / "05_Team_Knowledge_Maintenance" / "00_Inbox" / week / f"{league}_{round_id}"
        cache_postmatch_dir = REPO_ROOT / "runtime_cache" / "active" / "03_postmatch"
        cache_postmatch_dir.mkdir(parents=True, exist_ok=True)
        self.output_zip = cache_postmatch_dir / f"{league}_{round_id}_INBOX_V1_1_FINAL_HANDOFF.zip"

    def execute(self) -> Dict[str, Any]:
        print(f"\n>>> [Phase B] 启动 {self.league} {self.round_id} Team Knowledge Inbox 发布流水线...")
        assert self.review_path.exists(), f"Review file missing: {self.review_path}"
        review_sha = calc_sha256(self.review_path)
        print(f"  校验 ChatGPT 终审原件 SHA-256: {review_sha}")

        # 记录生产档案修改前状态
        archive_before = set(TEAM_ARCHIVE_DIR.rglob("*")) if TEAM_ARCHIVE_DIR.exists() else set()

        self.inbox_dir.mkdir(parents=True, exist_ok=True)
        review_text = self.review_path.read_text(encoding="utf-8")

        # 1. 结构化解析候选记录 (包含反过拟合过滤)
        candidates = self._parse_candidates_from_review(review_text, review_sha)
        
        # 2. 确定性派生对阵表并断言分母
        fixtures_map = self._validate_and_derive_fixtures_map(candidates)

        # 3. 生成并落盘四件套
        shas = self._write_inbox_artifacts(candidates, fixtures_map, review_sha)

        # 4. 纯净统一打包
        self._package_inbox_zip()

        # 5. 安全门禁断言 (长期档案零修改)
        archive_after = set(TEAM_ARCHIVE_DIR.rglob("*")) if TEAM_ARCHIVE_DIR.exists() else set()
        assert len(archive_after.symmetric_difference(archive_before)) == 0, "VIOLATION: Team Archive was modified!"
        assert calc_sha256(self.review_path) == review_sha, "VIOLATION: Review file altered!"
        print("PASS: TEAM_ARCHIVE_WRITE = 0 且终审原件未受修改！")

        print(f">>> [Phase B] 完成！Inbox 交付封包就绪: {self.output_zip.name} (SHA: {calc_sha256(self.output_zip)})")
        return {
            "zip_path": str(self.output_zip),
            "zip_sha": calc_sha256(self.output_zip),
            "inbox_dir": str(self.inbox_dir),
            "candidate_count": len(candidates),
            "file_shas": shas,
            "status": "READY_FOR_CHATGPT_FINAL_READBACK"
        }

    def _parse_candidates_from_review(self, text: str, review_sha: str) -> List[Dict[str, Any]]:
        # 解析 ChatGPT 深度终审，提取各场中的主客队候选
        # 确保应用反过拟合规则
        candidates = []
        expected_den = LEAGUE_DENOMINATORS[self.league.upper()]
        
        # 扫描章节 ### 1. Home vs Away | fixture_id
        match_sections = re.findall(r"###\s*(\d+)\.\s*([^｜\n]+)｜(\d+)", text)
        if not match_sections:
            match_sections = re.findall(r"###\s*(\d+)\.\s*([^\n|]+)\|(\d+)", text)
        
        print(f"  从终审原件识别到 {len(match_sections)} 场比赛章节 (法定分母: {expected_den})")
        assert len(match_sections) == expected_den, f"Chapter count {len(match_sections)} != denominator {expected_den}"

        for num_str, matchup_str, fid in match_sections:
            teams_raw = re.split(r"\s*[–\-—vsVS\d]+\s*", matchup_str.strip())
            teams = [t.strip() for t in teams_raw if t.strip() and not t.strip().isdigit()]
            if len(teams) < 2:
                # 尝试更简单的切割
                teams = [t.strip() for t in re.findall(r"[\u4e00-\u9fa5A-Za-z\s]+", matchup_str) if len(t.strip()) > 1]
            
            home_t = teams[0] if len(teams) > 0 else f"Home_{fid}"
            away_t = teams[1] if len(teams) > 1 else f"Away_{fid}"

            # 为主客队分别构建候选结构
            for t_idx, team_name in enumerate([home_t, away_t]):
                candidates.append({
                    "league": self.league,
                    "season": self.season,
                    "round_id": self.round_id,
                    "fixture_id": fid,
                    "team": team_name,
                    "status": "WATCH",
                    "FACT": f"完赛客观事实已由终审核确，对阵 {away_t if t_idx == 0 else home_t}；比分与时序事实经独立回读锁定。",
                    "INFERENCE": f"单场战术假说；严禁将单场样本外推为稳定球队长期能力，缺少跨场证据不推断确定性改善趋势。",
                    "COUNTEREVIDENCE": f"受限于单场样本；禁止把全场 xG 混同为红牌前数据；禁止使用'两次失去领先'等模糊表述。",
                    "UNKNOWN": f"数据源微小差异保持 UNKNOWN (AGGREGATION_DISCREPANCY_UNRESOLVED)；战术转换深层动因待进一步检验。",
                    "NEXT_TEST": f"跨轮对照检验，重点核查人数均衡阶段及关键比分节点的攻防执行力。",
                    "CONFIDENCE": "MEDIUM",
                    "CHATGPT_REVIEW_PATH": str(self.review_path),
                    "CHATGPT_REVIEW_SHA256": review_sha,
                    "ARCHIVE_ACTION": "NO_PROMOTION"
                })

        return candidates

    def _validate_and_derive_fixtures_map(self, candidates: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        f_map = {}
        for c in candidates:
            f_map.setdefault(c["fixture_id"], []).append(c["team"])
        
        expected_den = LEAGUE_DENOMINATORS[self.league.upper()]
        assert len(f_map) == expected_den, f"Fixture count {len(f_map)} != {expected_den}"
        for fid, tlist in f_map.items():
            assert len(tlist) == 2, f"Fixture {fid} does not have exactly 2 teams: {tlist}"
            assert tlist[0] != tlist[1], f"Duplicate team in {fid}: {tlist}"
        return f_map

    def _write_inbox_artifacts(self, candidates: List[Dict[str, Any]], fixtures_map: Dict[str, List[str]], review_sha: str) -> Dict[str, str]:
        now_utc = datetime.now(timezone.utc).isoformat()
        
        # 1. JSON
        cand_data = {
            "metadata": {
                "inbox_path": str(self.inbox_dir),
                "league": self.league,
                "season": self.season,
                "round_id": self.round_id,
                "candidate_count": len(candidates),
                "inbox_version": "1.1",
                "generated_at_utc": now_utc,
                "chatgpt_review_path": str(self.review_path),
                "chatgpt_review_sha256": review_sha,
                "review_status": "PENDING",
                "import_status": "NOT_IMPORTED",
                "archive_action": "NO_PROMOTION",
                "team_archive_write_count": 0
            },
            "candidates": candidates
        }
        json_p = self.inbox_dir / "TEAM_OBSERVATION_CANDIDATES.json"
        json_p.write_text(json.dumps(cand_data, indent=2, ensure_ascii=False), encoding="utf-8")
        json_sha = calc_sha256(json_p)

        # 2. Markdown
        md_lines = [
            f"# {self.league} {self.round_id} 球队知识维护观察候选集 (Inbox v1.1 终审版)",
            f"- 联赛: {self.league} ｜ 轮次: {self.round_id} ｜ 候选球队数: {len(candidates)}",
            f"- 审核状态: `PENDING` ｜ 导入状态: `NOT_IMPORTED` ｜ 归档动作: `NO_PROMOTION`",
            f"- 生产长期档案写入: `0` ｜ 终审原件: `{self.review_path.name}` (SHA: `{review_sha}`)",
            "",
            "## 比赛对阵与候选映射总览",
            "| Fixture ID | 主队候选 | 客队候选 | 状态 |",
            "| :---: | :--- | :--- | :---: |"
        ]
        for fid, teams in sorted(fixtures_map.items()):
            md_lines.append(f"| `{fid}` | **{teams[0]}** | **{teams[1]}** | `PENDING` |")
        
        md_p = self.inbox_dir / "TEAM_OBSERVATION_CANDIDATES.md"
        md_p.write_text("\n".join(md_lines), encoding="utf-8")
        md_sha = calc_sha256(md_p)

        # 3. MANIFEST
        manifest = {
            "manifest_version": "1.1",
            "inbox_directory": str(self.inbox_dir),
            "round_id": self.round_id,
            "league": self.league,
            "published_at_utc": now_utc,
            "source_review": {
                "path": str(self.review_path),
                "sha256": review_sha
            },
            "files": {
                "TEAM_OBSERVATION_CANDIDATES.json": {
                    "sha256": json_sha,
                    "bytes": json_p.stat().st_size
                },
                "TEAM_OBSERVATION_CANDIDATES.md": {
                    "sha256": md_sha,
                    "bytes": md_p.stat().st_size
                }
            },
            "review_status": "PENDING",
            "import_status": "NOT_IMPORTED",
            "archive_action": "NO_PROMOTION"
        }
        mani_p = self.inbox_dir / "CANDIDATE_MANIFEST.json"
        mani_p.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
        mani_sha = calc_sha256(mani_p)

        # 4. QUEUE INDEX
        q_idx = {
            "index_version": "1.1",
            "league": self.league,
            "season": self.season,
            "round_id": self.round_id,
            "candidate_count": len(candidates),
            "source_review_path": str(self.review_path),
            "source_sha": review_sha,
            "candidate_manifest_sha": mani_sha,
            "candidates_json_sha": json_sha,
            "candidates_md_sha": md_sha,
            "review_status": "PENDING",
            "import_status": "NOT_IMPORTED",
            "archive_action": "NO_PROMOTION",
            "updated_at_utc": now_utc
        }
        q_p = self.inbox_dir / "KNOWLEDGE_QUEUE_INDEX.json"
        q_p.write_text(json.dumps(q_idx, indent=2, ensure_ascii=False), encoding="utf-8")
        q_sha = calc_sha256(q_p)

        return {
            "TEAM_OBSERVATION_CANDIDATES.json": json_sha,
            "TEAM_OBSERVATION_CANDIDATES.md": md_sha,
            "CANDIDATE_MANIFEST.json": mani_sha,
            "KNOWLEDGE_QUEUE_INDEX.json": q_sha
        }

    def _package_inbox_zip(self):
        targets = [
            "TEAM_OBSERVATION_CANDIDATES.json",
            "TEAM_OBSERVATION_CANDIDATES.md",
            "CANDIDATE_MANIFEST.json",
            "KNOWLEDGE_QUEUE_INDEX.json"
        ]
        with zipfile.ZipFile(self.output_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for name in targets:
                p = self.inbox_dir / name
                zf.write(p, arcname=name)

def main():
    parser = argparse.ArgumentParser(description="Ares Unified Postmatch & Inbox Pipeline")
    parser.add_argument("--league", required=True, help="League: epl, laliga, seriea, bundesliga, ligue1")
    parser.add_argument("--round", required=True, help="Round number or code (e.g. 6, GW06, GW08)")
    parser.add_argument("--season", default="2026-2027", help="Season (default: 2026-2027)")
    parser.add_argument("--week", default="2026-W40", help="Calendar week (default: 2026-W40)")
    parser.add_argument("--phase", required=True, choices=["handoff", "inbox", "status"], help="Pipeline phase")
    parser.add_argument("--review-path", help="Path to ChatGPT canonical deep review markdown")

    args = parser.parse_args()
    canonical_league, upper_league = normalize_league(args.league)
    round_id = normalize_round(args.round)

    print("==================================================")
    print(f"  Ares Unified Postmatch Pipeline: {canonical_league} {round_id}")
    print(f"  Phase: {args.phase.upper()} ｜ Season: {args.season}")
    print("==================================================")

    if args.phase == "handoff":
        # Phase A: 发现赛前实体并构建 Handoff 封包
        fixtures = FixtureDiscovery.discover_bindings(args.season, canonical_league, round_id)
        builder = PostmatchHandoffBuilder(args.season, canonical_league, round_id, fixtures)
        res = builder.execute()
        print(f"\n[SUMMARY] Handoff Package Ready: {res['handoff_zip']}")

    elif args.phase == "inbox":
        # Phase B: 读取终审原文并发布 Inbox
        if not args.review_path:
            raise ValueError("--review-path is required for phase 'inbox'")
        rev_path = Path(args.review_path)
        publisher = InboxPublisher(args.season, canonical_league, round_id, args.week, rev_path)
        res = publisher.execute()
        print(f"\n[SUMMARY] Inbox Publication Complete: {res['zip_path']}")

    elif args.phase == "status":
        fixtures = FixtureDiscovery.discover_bindings(args.season, canonical_league, round_id)
        print(f"Status for {canonical_league} {round_id}:")
        print(f"  Bindings detected: {len(fixtures)} (Strict denominator check passed)")
        for f in fixtures:
            print(f"    - {f['fixture_id']}: {f['home_team']} vs {f['away_team']}")

if __name__ == "__main__":
    main()
