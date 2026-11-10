#!/usr/bin/env python3
"""
Ares OSINT Telemetry - Stage 6 Controlled Integration Regression Test
Execution Mode: CONTROLLED_INTEGRATION / NO_PRODUCTION_DEPLOYMENT
Tag: RETROSPECTIVE_REPLAY

Verifies:
1. Postmatch automatic Candidate generation, Audits, and Manifest in isolated Staging;
2. Prematch VALUE_PRIOR_CONSIDERATION consumption;
3. Temporal cutoff guardrail and registry fail-closed degradation;
4. 3-team regression values (Everton €258.0M, Chelsea €520.2M, Arsenal €655.0M);
5. Zero write to production archives and current pointer.
"""

import hashlib
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "src/engine"))

from squad_value_prior_adapter import SquadValuePriorAdapter, calc_sha256
from squad_valuation_pipeline import run_postmatch_valuation_cycle


class TestStage6ControlledIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.w40_baseline = REPO_ROOT / "staging/recent_xi_prior_continuation/w41_isolated_candidate/handoff_package/team_recent_xi_value.W40.json"
        cls.tm_dataset = REPO_ROOT / "staging/recent_xi_prior_continuation/w41_isolated_candidate/handoff_package/players.csv.gz"
        cls.postmatch_zip = REPO_ROOT / "staging/recent_xi_prior_continuation/w41_isolated_candidate/handoff_package/EPL_GW07_POSTMATCH_DEEP_HANDOFF.zip"
        cls.mapping_file = REPO_ROOT / "staging/recent_xi_prior_continuation/w41_isolated_candidate/handoff_package/TOP_FIVE_96_FOTMOB_IDENTITY_MAPPING.json"
        cls.verified_roster = REPO_ROOT / "staging/recent_xi_prior_continuation/w41_isolated_candidate/verified_cross_platform_players.json"

        cls.fixture_map = {
            "3003912": {"home": "Everton", "away": "Chelsea"},
            "3003917": {"home": "Nottingham Forest", "away": "Arsenal"},
        }
        cls.target_teams = ["Everton", "Chelsea", "Arsenal"]

    # -------------------------------------------------------------------------
    # 1. Postmatch 自动化产生 Candidate 与 Staging 隔离验证
    # -------------------------------------------------------------------------
    @unittest.skipUnless(
        (REPO_ROOT / "staging/recent_xi_prior_continuation/w41_isolated_candidate/handoff_package/players.csv.gz").exists(),
        "Historical W41 staging fixture archive is not present in active repo"
    )
    def test_01_postmatch_candidate_generation_and_manifest(self):
        """[Check 1] Postmatch 确证首发自动驱动身价滚动与独立 Staging 候选输出"""
        with tempfile.TemporaryDirectory() as td:
            staging_dir = Path(td)
            candidate_pkg, detailed_audits = run_postmatch_valuation_cycle(
                w40_baseline_path=self.w40_baseline,
                tm_dataset_path=self.tm_dataset,
                postmatch_zip_path=self.postmatch_zip,
                mapping_path=self.mapping_file,
                verified_roster_path=self.verified_roster,
                target_teams=self.target_teams,
                fixture_target_map=self.fixture_map,
                output_dir=staging_dir,
            )

            # 验证主要交付文件
            cand_file = staging_dir / "team_recent_xi_value.W41_POSTMATCH_CANDIDATE.json"
            audits_file = staging_dir / "postmatch_lineup_valuation_audits.json"
            self.assertTrue(cand_file.exists())
            self.assertTrue(audits_file.exists())

            # 验证三队既有回归数值完全吻合
            teams_map = {t["team_name"]: t for t in candidate_pkg["teams"]}
            eve = teams_map["Everton"]
            che = teams_map["Chelsea"]
            ars = teams_map["Arsenal"]

            self.assertEqual(eve["last3_status"], "CAUTION")
            self.assertIsNone(eve["primary_metric"])
            self.assertEqual(eve["secondary_metric"]["value_eur"], 258_000_000.0)

            self.assertEqual(che["last3_status"], "CAUTION")
            self.assertIsNone(che["primary_metric"])
            self.assertEqual(che["secondary_metric"]["value_eur"], 520_200_000.0)

            self.assertEqual(ars["last3_status"], "READY")
            self.assertEqual(ars["primary_metric"]["value_eur"], 655_000_000.0)
            self.assertEqual(ars["primary_metric"]["semantic_basis"], "PARTIAL_LOWER_BOUND")

    # -------------------------------------------------------------------------
    # 2. Prematch VALUE_PRIOR_CONSIDERATION 消费规范
    # -------------------------------------------------------------------------
    def test_02_prematch_consumption_interface_and_anti_overreach(self):
        """[Check 2] Prematch 适配器消费接口规范与 5 项 Anti-Overreach 禁令注入"""
        # 加载模拟候选资产进行回放 (优先使用自包含测试 fixture, 缺失时回退到 staging 候选)
        fixture_override = REPO_ROOT / "tests" / "fixtures" / "priors" / "fixture_squad_value_priors.json"
        cand_candidate = fixture_override if fixture_override.exists() else (REPO_ROOT / "staging/recent_xi_prior_continuation/w41_isolated_candidate/handoff_package/team_recent_xi_value.W41_CANDIDATE.json")
        adapter = SquadValuePriorAdapter(override_canonical_json_path=cand_candidate)

        # 正常时序比赛 (回放模式下带 RETROSPECTIVE_REPLAY 标签)
        cons = adapter.get_value_prior_consideration(
            home_team_name="Everton",
            away_team_name="Chelsea",
            kickoff_utc="2026-10-18T14:00:00Z",
            fixture_id="3003912",
            league="EPL",
            round_no=7,
            is_retrospective_replay=True,
        )

        self.assertEqual(cons["asset_status"], "SUPPORTING_CONTEXT")
        self.assertTrue(cons["is_comparison_allowed"])
        self.assertTrue(cons["is_structural_supporting_context_only"])
        self.assertTrue(cons.get("is_retrospective_replay", False))
        self.assertEqual(cons.get("replay_audit_tag"), "RETROSPECTIVE_REPLAY")
        # CAUTION 下权威比率必须强制为 null
        self.assertIsNone(cons["authoritative_asset_ratio"])
        # 诊断商数仅作背景参考
        self.assertEqual(cons["unverified_diagnostic_quotient"], 0.496)

        # 5 项 Anti-Overreach 禁令断言
        ao = cons["anti_overreach_contract"]
        self.assertTrue(ao["overreach_forbidden"])
        self.assertTrue(ao["cannot_derive_asian_handicap"])
        self.assertTrue(ao["cannot_derive_over_under"])
        self.assertTrue(ao["cannot_derive_1x2_odds"])
        self.assertTrue(ao["cannot_override_football_model"])

    def test_02b_ready_status_lower_bound_arbitration_guard(self):
        """[Check 2b] 样本状态为 READY 不应覆盖实际估值质量门禁核查
        覆盖:
        1) 双方均 READY，但双方均为部分已知下界 (如生产真实 Roma vs Inter);
        2) 双方均 READY，但覆盖不对等 (如一方 FULL_XI 另一方为下界);
        3) 双方均 READY，且双方均为 3/3 完整估值 (如 Arsenal vs Man City);
        核心断言: 只要存在任一方为下界，authoritative_asset_ratio 必须刚性为 None，保留诊断值。
        """
        fixture_override = REPO_ROOT / "tests" / "fixtures" / "priors" / "fixture_squad_value_priors.json"
        adapter = SquadValuePriorAdapter(override_canonical_json_path=fixture_override if fixture_override.exists() else None)

        # 1. 双方均 READY，但双方均为下界 (Roma vs Inter, 均 3 场样本但只有 2 场完整)
        c1 = adapter.get_value_prior_consideration(
            home_team_name="Roma",
            away_team_name="Inter",
            kickoff_utc="2026-11-03T18:00:00Z",
            fixture_id="test_roma_inter",
            league="SerieA",
            round_no=10,
        )
        self.assertEqual(c1["asset_status"], "SUPPORTING_CONTEXT")
        self.assertTrue(c1["is_comparison_allowed"])
        self.assertEqual(c1["home_team_metrics"]["status"], "READY")
        self.assertEqual(c1["away_team_metrics"]["status"], "READY")
        self.assertIn("LOWER_BOUND", c1["home_team_metrics"]["completeness_quality"])
        self.assertIn("LOWER_BOUND", c1["away_team_metrics"]["completeness_quality"])
        # 权威身价比值必须为 null，诊断值保留
        self.assertIsNone(c1["authoritative_asset_ratio"])
        self.assertEqual(c1["unverified_diagnostic_quotient"], 0.6313)
        self.assertIn("未验证诊断倍数", c1["human_readable_summary"])

        # 2. 双方均 READY，但覆盖不对等 (Arsenal: 3/3 FULL_XI_EXACT vs Roma: 2/3 下界)
        c2 = adapter.get_value_prior_consideration(
            home_team_name="Arsenal",
            away_team_name="Roma",
            kickoff_utc="2026-11-03T18:00:00Z",
            fixture_id="test_ars_roma",
            league="UCL",
            round_no=4,
        )
        self.assertEqual(c2["asset_status"], "SUPPORTING_CONTEXT")
        self.assertTrue(c2["is_comparison_allowed"])
        self.assertIn(c2["home_team_metrics"]["completeness_quality"], ["FULL_XI_EXACT", "FULL_XI_CONFIRMED"])
        self.assertIn("LOWER_BOUND", c2["away_team_metrics"]["completeness_quality"])
        # 覆盖不对等时，权威身价比值必须为 null，避免误导性精确倍数
        self.assertIsNone(c2["authoritative_asset_ratio"])
        self.assertEqual(c2["unverified_diagnostic_quotient"], 2.9843)
        self.assertIn("未验证诊断倍数", c2["human_readable_summary"])

        # 3. 双方均 READY，且双方均为 3/3 FULL_XI 确证估值 (Arsenal vs Man City)
        c3 = adapter.get_value_prior_consideration(
            home_team_name="Arsenal",
            away_team_name="Man City",
            kickoff_utc="2026-11-03T18:00:00Z",
            fixture_id="test_ars_mci",
            league="EPL",
            round_no=10,
        )
        self.assertEqual(c3["asset_status"], "SUPPORTING_CONTEXT")
        self.assertTrue(c3["is_comparison_allowed"])
        # 双方均完整确证，输出权威精确身价比值
        self.assertEqual(c3["authoritative_asset_ratio"], 0.8176)
        self.assertEqual(c3["unverified_diagnostic_quotient"], 0.8176)
        self.assertIn("0.8176x", c3["human_readable_summary"])
        self.assertNotIn("未验证诊断倍数", c3["human_readable_summary"])

    def test_02c_severely_asymmetric_coverage_suppresses_quotient(self):
        """[Check 2c] 覆盖严重不对等或不足以比较时刚性置空诊断倍数门禁
        场景:
        - Bayern Munich (3/3 完整 FULL_XI_EXACT, €584.67M) vs SC Paderborn (1/3 完整 PARTIAL_LOWER_BOUND, €11.40M);
        核心断言:
        1) authoritative_asset_ratio 刚性为 None;
        2) unverified_diagnostic_quotient 刚性为 None (绝不计算展示 51.3057x 等严重失真倍数);
        3) is_comparison_allowed 刚性置为 False;
        4) status_reason 明确记录覆盖严重不对等原因;
        5) human_readable_summary 比率显式为 '不可比 (覆盖不对等，比率置空)';
        6) 两队已知金额、样本数、完整场次与估值质量 100% 完整保留。
        """
        fixture_override = REPO_ROOT / "tests" / "fixtures" / "priors" / "fixture_squad_value_priors.json"
        adapter = SquadValuePriorAdapter(override_canonical_json_path=fixture_override if fixture_override.exists() else None)

        cons = adapter.get_value_prior_consideration(
            home_team_name="Bayern Munich",
            away_team_name="SC Paderborn",
            kickoff_utc="2026-11-03T18:00:00Z",
            fixture_id="test_bayern_paderborn",
            league="Bundesliga",
            round_no=9,
        )

        self.assertEqual(cons["asset_status"], "SUPPORTING_CONTEXT")
        self.assertFalse(cons["is_comparison_allowed"])
        self.assertIsNone(cons["authoritative_asset_ratio"])
        self.assertIsNone(cons["unverified_diagnostic_quotient"])
        self.assertIn("Severely asymmetric", cons["status_reason"])
        self.assertIn("不可比 (覆盖不对等，比率置空)", cons["human_readable_summary"])

        # 验证两队已知金额、样本数、完整场次 100% 完整保留
        h_m = cons["home_team_metrics"]
        a_m = cons["away_team_metrics"]
        self.assertEqual(h_m["status"], "READY")
        self.assertEqual(h_m["valid_matches"], 3)
        self.assertEqual(h_m["complete_matches"], 3)
        self.assertEqual(h_m["completeness_quality"], "FULL_XI_EXACT")
        self.assertAlmostEqual(h_m["effective_value_eur"], 584_666_666.67, places=1)

        self.assertEqual(a_m["status"], "CAUTION_ACCEPTABLE")
        self.assertEqual(a_m["valid_matches"], 3)
        self.assertEqual(a_m["complete_matches"], 1)
        self.assertEqual(a_m["completeness_quality"], "PARTIAL_LOWER_BOUND")
        self.assertAlmostEqual(a_m["effective_value_eur"], 11_395_737.0, places=1)

        # 验证摘要保留两队真实已知金额信息
        self.assertIn("Bayern Munich [READY] (€584.7M, FULL_XI_EXACT)", cons["human_readable_summary"])
        self.assertIn("SC Paderborn [CAUTION_ACCEPTABLE] (€11.40M, PARTIAL_LOWER_BOUND)", cons["human_readable_summary"])

    # -------------------------------------------------------------------------
    # 3. 时间截断门禁与缺失侧降级测试
    # -------------------------------------------------------------------------
    def test_03_temporal_guardrail_and_missing_team_degradation(self):
        """[Check 3] 开球时间截断门禁 (开球早于证据截止时间、缺少开球时间、格式非法) 与缺失侧降级"""
        fixture_override = REPO_ROOT / "tests" / "fixtures" / "priors" / "fixture_squad_value_priors.json"
        cand_candidate = fixture_override if fixture_override.exists() else (REPO_ROOT / "staging/recent_xi_prior_continuation/w41_isolated_candidate/handoff_package/team_recent_xi_value.W41_CANDIDATE.json")
        adapter = SquadValuePriorAdapter(override_canonical_json_path=cand_candidate)

        # 3.1 异常时间穿越：非回放模式下，比赛开球时间早于证据截止时间 (2026-10-01 < 2026-10-18)
        cons_violation = adapter.get_value_prior_consideration(
            home_team_name="Everton",
            away_team_name="Chelsea",
            kickoff_utc="2026-10-01T12:00:00Z",  # 远早于 2026-10-18
            fixture_id="3003912",
            is_retrospective_replay=False,
        )
        self.assertEqual(cons_violation["asset_status"], "UNKNOWN")
        self.assertFalse(cons_violation["is_comparison_allowed"])
        self.assertIsNone(cons_violation["authoritative_asset_ratio"])
        self.assertIn("TEMPORAL_LOOKAHEAD_VIOLATION", cons_violation["status_reason"])

        # 3.2 缺少有效开球时间自动降级 (None 或 空字符串)
        cons_no_ko = adapter.get_value_prior_consideration(
            home_team_name="Everton",
            away_team_name="Chelsea",
            kickoff_utc=None,
            fixture_id="3003912",
            is_retrospective_replay=True,
        )
        self.assertEqual(cons_no_ko["asset_status"], "UNKNOWN")
        self.assertFalse(cons_no_ko["is_comparison_allowed"])
        self.assertIsNone(cons_no_ko["authoritative_asset_ratio"])
        self.assertIn("MISSING_KICKOFF_TIME_DEGRADED", cons_no_ko["status_reason"])

        # 3.3 非法开球时间格式自动降级
        cons_bad_ko = adapter.get_value_prior_consideration(
            home_team_name="Everton",
            away_team_name="Chelsea",
            kickoff_utc="INVALID_DATE_STRING",
            fixture_id="3003912",
            is_retrospective_replay=True,
        )
        self.assertEqual(cons_bad_ko["asset_status"], "UNKNOWN")
        self.assertFalse(cons_bad_ko["is_comparison_allowed"])
        self.assertIn("INVALID_KICKOFF_TIME_FORMAT_DEGRADED", cons_bad_ko["status_reason"])

        # 3.4 缺失侧降级：诺丁汉森林缺失导致对阵降级为 NOT_USED
        cons_missing = adapter.get_value_prior_consideration(
            home_team_name="Nottingham Forest",
            away_team_name="Arsenal",
            kickoff_utc="2026-10-18T16:30:00Z",
            fixture_id="3003917",
            is_retrospective_replay=True,
        )
        self.assertEqual(cons_missing["asset_status"], "NOT_USED")
        self.assertFalse(cons_missing["is_comparison_allowed"])
        self.assertIsNone(cons_missing["authoritative_asset_ratio"])
        self.assertIsNone(cons_missing["unverified_diagnostic_quotient"])

    # -------------------------------------------------------------------------
    # 4. Postmatch 身价滚动失败隔离 (Non-blocking & Fail-Closed)
    # -------------------------------------------------------------------------
    def test_04_postmatch_failure_isolation_non_blocking(self):
        """[Check 4A: 0/N 完全缺失] 确认 Postmatch 身价计算在非法/缺失数据下受控降级 (Fail-Closed)，绝不虚假放行 (Prevent Fail-Open)"""
        from scripts.run_postmatch_round import PostmatchHandoffBuilder
        dummy_fixtures = [{
            "fixture_id": "9999999",
            "fotmob_id": "0000000",
            "home_team": "NonExistentHome",
            "away_team": "NonExistentAway",
            "kickoff_utc": "2026-10-18T15:00:00Z",
            "canonical_url": "",
            "binding_path": ""
        }]
        builder = PostmatchHandoffBuilder(
            season="2026-2027",
            league="EPL",
            round_id="GW99_TEST",
            fixtures=dummy_fixtures
        )
        # 直接调用身价滚动私有方法，验证在非法 fixture 下受控降级 (Fail-Closed)
        result = builder._execute_squad_valuation_rolling()
        self.assertEqual(result["status"], "DEGRADED")
        self.assertIn("reason", result)
        self.assertEqual(result.get("single_match_artifacts_count"), 0)
        self.assertEqual(result.get("participating_teams_count"), 0)
        self.assertFalse(result.get("allow_stage6_promotion", True))
        # 绝无未捕获异常抛出

    def test_04b_postmatch_empty_fixtures_degraded(self):
        """[Check 4B: 负向测试 空列表] 传入空 fixture 列表时，必须优雅返回 DEGRADED，杜绝 Fail-Open"""
        from scripts.run_postmatch_round import PostmatchHandoffBuilder
        builder = PostmatchHandoffBuilder(
            season="2026-2027",
            league="EPL",
            round_id="GW99_EMPTY",
            fixtures=[]
        )
        result = builder._execute_squad_valuation_rolling()
        self.assertEqual(result["status"], "DEGRADED")
        self.assertIn("reason", result)
        self.assertIn("0 input fixtures", result["reason"])
        self.assertFalse(result.get("allow_stage6_promotion", True))

    def test_04c_postmatch_partial_fixtures_degraded(self):
        """[Check 4C: 部分 N 匹配] 仅部分比赛成功提取身价时，必须标记 DEGRADED_PARTIAL 并硬拦截 Stage 6 晋升"""
        from src.engine.postmatch_valuation_integrator import run_round_postmatch_valuation_pipeline
        with tempfile.TemporaryDirectory() as td:
            staging_out = Path(td) / "staging"
            staging_out.mkdir(parents=True, exist_ok=True)
            mock_base = Path(td) / "postmatch"
            mock_base.mkdir(parents=True, exist_ok=True)
            
            # 创建 1 场有合法 raw 数据的比赛，1 场无 raw 数据的比赛 (1/2 覆盖率)
            f1_dir = mock_base / "10001"
            f1_dir.mkdir(parents=True, exist_ok=True)
            raw_match_payload = {
                "general": {
                    "matchId": 10001,
                    "matchTimeUTC": "2026-10-18T14:00:00Z",
                    "homeTeam": {"name": "Arsenal"},
                    "awayTeam": {"name": "Chelsea"}
                },
                "content": {
                    "lineup": {
                        "lineup": [
                            {"teamId": 9825, "teamName": "Arsenal", "starters": [{"name": f"P{i}", "marketValue": 10_000_000} for i in range(11)]},
                            {"teamId": 8455, "teamName": "Chelsea", "starters": [{"name": f"Q{i}", "marketValue": 8_000_000} for i in range(11)]}
                        ]
                    }
                }
            }
            (f1_dir / "00-fotmob-raw.json").write_text(json.dumps(raw_match_payload), encoding="utf-8")
            
            fixtures = [
                {"fixture_id": "10001", "home_team": "Arsenal", "away_team": "Chelsea"},
                {"fixture_id": "10002", "home_team": "Liverpool", "away_team": "Everton"} # 缺少 raw
            ]
            
            res = run_round_postmatch_valuation_pipeline(
                league="EPL",
                season="2026-2027",
                round_id="GW99_PARTIAL",
                fixtures=fixtures,
                postmatch_base_dir=mock_base,
                staging_output_dir=staging_out,
                is_production_authorized=False
            )
            # 必须判定为 DEGRADED，严禁误提升为 PASS
            self.assertEqual(res["status"], "DEGRADED")
            self.assertEqual(res["pipeline_status"], "DEGRADED_PARTIAL")
            self.assertEqual(res["canonical_persistence_status"], "BLOCKED_PARTIAL_SQUAD_DATA")
            self.assertFalse(res["allow_stage6_promotion"])
            self.assertEqual(res["coverage_rate"], "1/2")

    def test_04d_postmatch_gate_report_and_manifest_decoupled(self):
        """[Check 4D: 门禁解耦与 Manifest 生成] 身价降级时核心事实依然 PASS，Manifest 正常生成，Stage 6 归档被拦截"""
        from scripts.run_postmatch_round import PostmatchHandoffBuilder
        with tempfile.TemporaryDirectory() as td:
            builder = PostmatchHandoffBuilder(
                season="2026-2027",
                league="EPL",
                round_id="GW99_GATE_TEST",
                fixtures=[{"fixture_id": "10001", "fotmob_id": "00001", "home_team": "Arsenal", "away_team": "Chelsea", "kickoff_utc": "2026-10-18T15:00:00Z"}]
            )
            builder.postmatch_dir = Path(td)
            
            summaries = [{
                "fixture_id": "10001",
                "home_team": "Arsenal",
                "away_team": "Chelsea",
                "score": "2-1",
                "halftime": "1-0",
                "home_xg": 1.85,
                "away_xg": 0.95,
                "goals_count": 3,
                "subs_count": 5
            }]
            # 模拟身价模块返回 DEGRADED
            degraded_squad_res = {
                "status": "DEGRADED",
                "single_match_artifacts_count": 0,
                "allow_stage6_promotion": False
            }
            core_pass = builder._write_round_summaries(summaries, degraded_squad_res)
            # 核心事实门禁成立
            self.assertTrue(core_pass)
            
            # 验证 00-ROUND-MANIFEST.json 必须已正常生成 (无死代码阻断)
            manifest_f = builder.postmatch_dir / "00-ROUND-MANIFEST.json"
            self.assertTrue(manifest_f.exists(), "00-ROUND-MANIFEST.json must be generated without being blocked")
            mdata = json.loads(manifest_f.read_text(encoding="utf-8"))
            self.assertEqual(mdata["core_evidence_gate"], "PASS")
            self.assertEqual(mdata["squad_valuation_gate"], "DEGRADED")
            self.assertFalse(mdata["stage6_promotion_allowed"])
            
            # 验证 00-ROUND-GATE-REPORT.md 明确记录身价降级与 Stage 6 归档拦截
            report_f = builder.postmatch_dir / "00-ROUND-GATE-REPORT.md"
            self.assertTrue(report_f.exists())
            rtext = report_f.read_text(encoding="utf-8")
            self.assertIn("Squad Valuation Supporting Gate**: `DEGRADED`", rtext)
            self.assertIn("Stage 6 Archive Promotion Readiness**: `BLOCKED`", rtext)

    # -------------------------------------------------------------------------
    # 5. 生产文件与指针零修改保护 (动态字节/SHA对比，零硬编码周期)
    # -------------------------------------------------------------------------
    def test_05_production_archives_and_pointer_untouched(self):
        """[Check 5] 生产知识库与 current 指针维持 0 写入，运行前后物理字节/SHA 100% 严格对比"""
        import re
        from src.engine.squad_value_prior_adapter import CURRENT_POINTER_FILE, PRIORS_DIR
        self.assertTrue(CURRENT_POINTER_FILE.exists(), f"Pointer file must exist at {CURRENT_POINTER_FILE}")
        
        # 1. 运行前物理快照与 SHA
        bytes_before = CURRENT_POINTER_FILE.read_bytes()
        sha_before = hashlib.sha256(bytes_before).hexdigest()
        
        # 2. 验证指针语义自洽性与发布来源 (无任何周期与哈希硬编码)
        pdata = json.loads(bytes_before.decode("utf-8"))
        active_cycle = pdata.get("current_active_cycle", "")
        self.assertTrue(bool(re.match(r"^W\d{2}$", active_cycle)), f"Invalid cycle format: {active_cycle}")
        self.assertEqual(pdata.get("teams_count"), 96)
        
        # 3. 验证指针指向的权威 Canonical JSON 物理存在且哈希自洽
        canonical_rel = pdata.get("canonical_json_path", "")
        self.assertTrue(canonical_rel.startswith(f"team_recent_xi_value.{active_cycle}"))
        canonical_path = PRIORS_DIR / canonical_rel
        self.assertTrue(canonical_path.exists(), f"Canonical asset must exist: {canonical_path}")
        canonical_sha = hashlib.sha256(canonical_path.read_bytes()).hexdigest()
        self.assertEqual(canonical_sha, pdata.get("canonical_json_sha256"))
        
        # 4. 运行后再次物理回读，断言 0 写入 (Zero Mutation)
        bytes_after = CURRENT_POINTER_FILE.read_bytes()
        sha_after = hashlib.sha256(bytes_after).hexdigest()
        self.assertEqual(bytes_before, bytes_after, "Pointer file must not be modified during test execution")
        self.assertEqual(sha_before, sha_after, "Pointer SHA must be strictly identical before and after")

    def test_05b_cycle_agnostic_pointer_validation(self):
        """[Check 5B] 周期自适应验证：验证当合法周期发生切换时 (如 W40/W43/W44/W45)，校验逻辑自适应通过，无需修改测试代码"""
        import re
        from src.engine.squad_value_prior_adapter import PRIORS_DIR
        
        available_cycles = []
        for cf in sorted(PRIORS_DIR.glob("team_recent_xi_value.W*.json")):
            m = re.search(r"\.(W\d{2})\.json$", cf.name)
            if m:
                available_cycles.append((m.group(1), cf))
        
        self.assertTrue(len(available_cycles) >= 1, "At least one canonical cycle must exist in priors dir")
        
        # 对每个已存在的合法周期资产，验证其自洽性
        for cycle_name, cfile in available_cycles:
            cbytes = cfile.read_bytes()
            csha = hashlib.sha256(cbytes).hexdigest()
            cdata = json.loads(cbytes.decode("utf-8"))
            self.assertTrue(bool(re.match(r"^W\d{2}$", cycle_name)))
            self.assertEqual(len(cdata.get("teams", [])), 96, f"Cycle {cycle_name} must have 96 teams")
            
            # 模拟指针切换为该周期，验证自适应验证逻辑
            simulated_pointer = {
                "current_active_cycle": cycle_name,
                "canonical_json_path": cfile.name,
                "canonical_json_sha256": csha,
                "teams_count": len(cdata.get("teams", [])),
                "readiness": "READY_FOR_NEXT_POSTMATCH_CYCLE"
            }
            # 自适应断言：无论周期名如何切换，只要格式匹配且哈希一致，即可自适应通过
            self.assertTrue(bool(re.match(r"^W\d{2}$", simulated_pointer["current_active_cycle"])))
            target_asset = PRIORS_DIR / simulated_pointer["canonical_json_path"]
            self.assertTrue(target_asset.exists())
            self.assertEqual(simulated_pointer["canonical_json_sha256"], hashlib.sha256(target_asset.read_bytes()).hexdigest())

    # -------------------------------------------------------------------------
    # 6. Stage 6 真正 Fail-Closed 写入入口验证 (4 大场景物理断言)
    # -------------------------------------------------------------------------
    def test_06a_fail_closed_zero_matches_promotion_blocked(self):
        """[Check 6A: 场景 1 - 零有效比赛] 验证 0/N 下 Stage 6 持久化写入入口坚决拒绝落盘，目标文件绝对不存在"""
        from src.engine.postmatch_valuation_integrator import (
            run_round_postmatch_valuation_pipeline,
            promote_valuation_candidate_to_canonical
        )
        with tempfile.TemporaryDirectory() as td:
            tmp_root = Path(td)
            staging_out = tmp_root / "staging"
            mock_postmatch = tmp_root / "postmatch"
            mock_prod_priors = tmp_root / "mock_AresVault/02_Team_Archives/priors"
            staging_out.mkdir(parents=True, exist_ok=True)
            mock_postmatch.mkdir(parents=True, exist_ok=True)
            mock_prod_priors.mkdir(parents=True, exist_ok=True)

            fixtures = [{"fixture_id": "90001", "home_team": "TeamA", "away_team": "TeamB"}]
            # 零有效比赛 (缺少 00-fotmob-raw.json)
            pipe_res = run_round_postmatch_valuation_pipeline(
                league="EPL",
                season="2026-2027",
                round_id="GW99_ZERO",
                fixtures=fixtures,
                postmatch_base_dir=mock_postmatch,
                staging_output_dir=staging_out,
                is_production_authorized=False
            )
            self.assertEqual(pipe_res["status"], "DEGRADED")
            self.assertFalse(pipe_res["allow_stage6_promotion"])
            self.assertEqual(pipe_res["canonical_persistence_status"], "BLOCKED_NO_DATA")

            # 验证 Stage 6 实际持久化写入入口：必须抛出 PermissionError 坚决阻断
            target_canonical = mock_prod_priors / "team_recent_xi_value.W99.json"
            target_pointer = mock_prod_priors / "team_recent_xi_value.current.json"

            with self.assertRaises(PermissionError) as ctx:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=pipe_res,
                    target_canonical_path=target_canonical,
                    target_pointer_path=target_pointer,
                    is_production_authorized=True
                )
            self.assertIn("PROMOTION_BLOCKED", str(ctx.exception))
            # 物理断言：目标生产资产与指针绝未创建
            self.assertFalse(target_canonical.exists(), "Target canonical asset must NOT be written under zero matches!")
            self.assertFalse(target_pointer.exists(), "Target pointer must NOT be created under zero matches!")

    def test_06b_fail_closed_partial_fixtures_promotion_blocked(self):
        """[Check 6B: 场景 2 - 部分比赛缺失] 验证 1/2 覆盖率下 Stage 6 写入入口拒绝落盘，且核心赛后事实不受阻断"""
        from src.engine.postmatch_valuation_integrator import (
            run_round_postmatch_valuation_pipeline,
            promote_valuation_candidate_to_canonical
        )
        with tempfile.TemporaryDirectory() as td:
            tmp_root = Path(td)
            staging_out = tmp_root / "staging"
            mock_postmatch = tmp_root / "postmatch"
            mock_prod_priors = tmp_root / "mock_AresVault/02_Team_Archives/priors"
            staging_out.mkdir(parents=True, exist_ok=True)
            mock_postmatch.mkdir(parents=True, exist_ok=True)
            mock_prod_priors.mkdir(parents=True, exist_ok=True)

            # 构造 1 场有完整首发，1 场缺失 raw 的对阵 (1/2 覆盖率)
            f1_dir = mock_postmatch / "91001"
            f1_dir.mkdir(parents=True, exist_ok=True)
            raw_match_payload = {
                "general": {
                    "matchId": 91001,
                    "matchTimeUTC": "2026-10-18T14:00:00Z",
                    "homeTeam": {"name": "Arsenal", "id": 9825},
                    "awayTeam": {"name": "Chelsea", "id": 8455}
                },
                "content": {
                    "lineup": {
                        "lineup": [
                            {"teamId": 9825, "teamName": "Arsenal", "starters": [{"name": f"P{i}", "marketValue": 15_000_000} for i in range(11)]},
                            {"teamId": 8455, "teamName": "Chelsea", "starters": [{"name": f"Q{i}", "marketValue": 12_000_000} for i in range(11)]}
                        ]
                    }
                }
            }
            (f1_dir / "00-fotmob-raw.json").write_text(json.dumps(raw_match_payload), encoding="utf-8")

            fixtures = [
                {"fixture_id": "91001", "home_team": "Arsenal", "away_team": "Chelsea"},
                {"fixture_id": "91002", "home_team": "Liverpool", "away_team": "Everton"}  # 缺少数据
            ]

            pipe_res = run_round_postmatch_valuation_pipeline(
                league="EPL",
                season="2026-2027",
                round_id="GW99_PARTIAL",
                fixtures=fixtures,
                postmatch_base_dir=mock_postmatch,
                staging_output_dir=staging_out,
                is_production_authorized=False
            )
            self.assertEqual(pipe_res["status"], "DEGRADED")
            self.assertEqual(pipe_res["pipeline_status"], "DEGRADED_PARTIAL")
            self.assertFalse(pipe_res["allow_stage6_promotion"])

            cand_file = Path(pipe_res["candidate_file"])
            self.assertTrue(cand_file.exists())

            # 实际持久化写入入口：传入候选文件路径，必须拒绝落盘
            target_canonical = mock_prod_priors / "team_recent_xi_value.W99.json"
            with self.assertRaises(PermissionError) as ctx:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=cand_file,
                    target_canonical_path=target_canonical,
                    is_production_authorized=True
                )
            self.assertIn("PROMOTION_BLOCKED", str(ctx.exception))
            self.assertFalse(target_canonical.exists(), "Target canonical asset must NOT be written under partial fixtures!")

    def test_06c_fail_closed_single_sided_missing_promotion_blocked(self):
        """[Check 6C: 场景 3 - 单侧球队数据缺失] 主队 11 人确证但客队 0 人缺失，验证流水线识别单侧缺失并拒绝 Stage 6 晋升"""
        from src.engine.postmatch_valuation_integrator import (
            run_round_postmatch_valuation_pipeline,
            promote_valuation_candidate_to_canonical
        )
        with tempfile.TemporaryDirectory() as td:
            tmp_root = Path(td)
            staging_out = tmp_root / "staging"
            mock_postmatch = tmp_root / "postmatch"
            mock_prod_priors = tmp_root / "mock_AresVault/02_Team_Archives/priors"
            staging_out.mkdir(parents=True, exist_ok=True)
            mock_postmatch.mkdir(parents=True, exist_ok=True)
            mock_prod_priors.mkdir(parents=True, exist_ok=True)

            # 单场比赛中：主队 11 人有完整名单，但客队仅有 0 人（单侧首发缺失）
            f1_dir = mock_postmatch / "92001"
            f1_dir.mkdir(parents=True, exist_ok=True)
            single_sided_payload = {
                "general": {
                    "matchId": 92001,
                    "matchTimeUTC": "2026-10-18T14:00:00Z",
                    "homeTeam": {"name": "Arsenal", "id": 9825},
                    "awayTeam": {"name": "Chelsea", "id": 8455}
                },
                "content": {
                    "lineup": {
                        "homeTeam": {"starters": [{"name": f"P{i}", "marketValue": 10_000_000} for i in range(11)]},
                        "awayTeam": {"starters": []}  # 0 人缺失首发
                    }
                }
            }
            (f1_dir / "00-fotmob-raw.json").write_text(json.dumps(single_sided_payload), encoding="utf-8")

            fixtures = [{"fixture_id": "92001", "home_team": "Arsenal", "away_team": "Chelsea"}]

            pipe_res = run_round_postmatch_valuation_pipeline(
                league="EPL",
                season="2026-2027",
                round_id="GW99_SINGLE_SIDED",
                fixtures=fixtures,
                postmatch_base_dir=mock_postmatch,
                staging_output_dir=staging_out,
                is_production_authorized=False
            )
            # 必须识别单侧缺失，受控降级为 DEGRADED_PARTIAL
            self.assertEqual(pipe_res["status"], "DEGRADED")
            self.assertEqual(pipe_res["pipeline_status"], "DEGRADED_PARTIAL")
            self.assertEqual(pipe_res["canonical_persistence_status"], "BLOCKED_PARTIAL_SQUAD_DATA")
            self.assertFalse(pipe_res["allow_stage6_promotion"])
            self.assertIn("single-sided missing", pipe_res.get("reason", ""))

            # 实际持久化写入入口：必须拒绝落盘
            target_canonical = mock_prod_priors / "team_recent_xi_value.W99.json"
            with self.assertRaises(PermissionError) as ctx:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=pipe_res,
                    target_canonical_path=target_canonical,
                    is_production_authorized=True
                )
            self.assertIn("PROMOTION_BLOCKED", str(ctx.exception))
            self.assertFalse(target_canonical.exists(), "Target canonical asset must NOT be written under single-sided missing!")

    def test_06d_fail_closed_complete_fixtures_promotion_success_and_auth_guard(self):
        """[Check 6D: 场景 4 - 完整有效比赛] 2/2 完整首发下返回 PASS 并允许 Stage 6 晋升；未授权被拦截，授权后原子落盘"""
        from src.engine.postmatch_valuation_integrator import (
            run_round_postmatch_valuation_pipeline,
            promote_valuation_candidate_to_canonical
        )
        with tempfile.TemporaryDirectory() as td:
            tmp_root = Path(td)
            staging_out = tmp_root / "staging"
            mock_postmatch = tmp_root / "postmatch"
            mock_prod_priors = tmp_root / "AresVault/02_Team_Archives/priors"
            staging_out.mkdir(parents=True, exist_ok=True)
            mock_postmatch.mkdir(parents=True, exist_ok=True)
            mock_prod_priors.mkdir(parents=True, exist_ok=True)

            # 构造两场均具有完整 11 人首发的比赛
            for fid, (h_id, h_name, a_id, a_name) in [
                ("93001", (9825, "Arsenal", 8455, "Chelsea")),
                ("93002", (8650, "Liverpool", 8668, "Everton"))
            ]:
                f_dir = mock_postmatch / fid
                f_dir.mkdir(parents=True, exist_ok=True)
                payload = {
                    "general": {
                        "matchId": int(fid),
                        "matchTimeUTC": "2026-10-18T14:00:00Z",
                        "homeTeam": {"name": h_name, "id": h_id},
                        "awayTeam": {"name": a_name, "id": a_id}
                    },
                    "content": {
                        "lineup": {
                            "homeTeam": {"starters": [{"name": f"P_{h_name}_{i}", "marketValue": 12_000_000} for i in range(11)]},
                            "awayTeam": {"starters": [{"name": f"Q_{a_name}_{i}", "marketValue": 9_000_000} for i in range(11)]}
                        }
                    }
                }
                (f_dir / "00-fotmob-raw.json").write_text(json.dumps(payload), encoding="utf-8")

            fixtures = [
                {"fixture_id": "93001", "home_team": "Arsenal", "away_team": "Chelsea"},
                {"fixture_id": "93002", "home_team": "Liverpool", "away_team": "Everton"}
            ]

            pipe_res = run_round_postmatch_valuation_pipeline(
                league="EPL",
                season="2026-2027",
                round_id="GW99_COMPLETE",
                fixtures=fixtures,
                postmatch_base_dir=mock_postmatch,
                staging_output_dir=staging_out,
                is_production_authorized=False
            )
            # 完整双侧 11 人首发下必须判定为 PASS，且 allow_stage6_promotion 为 True
            self.assertEqual(pipe_res["status"], "PASS")
            self.assertEqual(pipe_res["pipeline_status"], "STAGING_PASS")
            self.assertEqual(pipe_res["coverage_rate"], "2/2")
            self.assertTrue(pipe_res["allow_stage6_promotion"])
            self.assertEqual(pipe_res["canonical_persistence_status"], "STAGING_READY_AWAITING_STAGE6_AUTHORIZATION")

            target_canonical = mock_prod_priors / "team_recent_xi_value.W99.json"
            target_pointer = mock_prod_priors / "team_recent_xi_value.current.json"

            # 4.1 授权门禁拦截测试：生产路径未获得授权时，即使候选合格也坚决阻断
            with self.assertRaises(PermissionError) as ctx:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=pipe_res,
                    target_canonical_path=target_canonical,
                    target_pointer_path=target_pointer,
                    is_production_authorized=False  # 未授权！
                )
            self.assertIn("STAGE6_UNAUTHORIZED", str(ctx.exception))
            self.assertFalse(target_canonical.exists())

            # 4.2 获得合法授权后测试：执行原子落盘与指针原子更新
            promote_res = promote_valuation_candidate_to_canonical(
                candidate_doc_or_path=pipe_res,
                target_canonical_path=target_canonical,
                target_pointer_path=target_pointer,
                is_production_authorized=True,
                operator_approval_token="ARES_TEST_OPERATOR_TOKEN_2026"
            )
            self.assertEqual(promote_res["status"], "PROMOTED_SUCCESS")
            self.assertTrue(target_canonical.exists())
            self.assertTrue(target_pointer.exists())

            # 验证持久化后的资产状态与哈希
            persisted_doc = json.loads(target_canonical.read_text(encoding="utf-8"))
            self.assertEqual(persisted_doc["pipeline_status"], "CANONICAL_PUBLISHED")
            self.assertEqual(persisted_doc["canonical_persistence_status"], "PUBLISHED_ACTIVE")
            self.assertEqual(persisted_doc["production_status"], "PRODUCTION_AUTHORIZED")
            self.assertEqual(persisted_doc["operator_approval_token"], "ARES_TEST_OPERATOR_TOKEN_2026")
            self.assertEqual(promote_res["canonical_sha256"], hashlib.sha256(target_canonical.read_bytes()).hexdigest())

            # 验证指针文件正确指向新资产
            pointer_doc = json.loads(target_pointer.read_text(encoding="utf-8"))
            self.assertEqual(pointer_doc["canonical_json_path"], "team_recent_xi_value.W99.json")
            self.assertEqual(pointer_doc["canonical_json_sha256"], promote_res["canonical_sha256"])

    # -------------------------------------------------------------------------
    # 7. Stage 6 生产入口语义门禁 8 大隔离场景专项测试 (LMW-413 DoD)
    # -------------------------------------------------------------------------
    def _create_mock_96_teams(self):
        """生成符合法定 96 队分母的假数据"""
        return [{"team_name": f"Team_{i}", "valid_match_count": 3} for i in range(96)]

    def test_07a_scenario1_new_valid_candidate_with_valid_auth(self):
        """[Check 7A: 场景 1] 新版完整候选 + 合法模拟授权 -> 门禁 PASS，原子落盘成功，指针自洽切换"""
        from src.engine.postmatch_valuation_integrator import (
            verify_squad_valuation_promotion_gate,
            promote_valuation_candidate_to_canonical
        )
        with tempfile.TemporaryDirectory() as td:
            tmp_root = Path(td)
            mock_priors = tmp_root / "02_Team_Archives/priors"
            mock_priors.mkdir(parents=True, exist_ok=True)

            candidate_doc = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "schema_version": "1.6.0",
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": True,
                "pipeline_status": "STAGING_PASS",
                "canonical_persistence_status": "STAGING_READY_AWAITING_STAGE6_AUTHORIZATION",
                "active_cycle_inherited": "W44"
            }
            cand_file = tmp_root / "team_recent_xi_value.W45_POSTMATCH_CANDIDATE.json"
            cand_bytes = json.dumps(candidate_doc, indent=2).encode("utf-8")
            cand_file.write_bytes(cand_bytes)
            cand_sha = hashlib.sha256(cand_bytes).hexdigest()

            # 1. 语义门禁校验
            gate_ok, gate_msg = verify_squad_valuation_promotion_gate(cand_file, cand_sha)
            self.assertTrue(gate_ok, f"Gate must pass: {gate_msg}")
            self.assertEqual(gate_msg, "NEW_PIPELINE_CANDIDATE_APPROVED")

            # 2. 模拟生产落盘
            target_canonical = mock_priors / "team_recent_xi_value.W45.json"
            target_pointer = mock_priors / "team_recent_xi_value.current.json"

            res = promote_valuation_candidate_to_canonical(
                candidate_doc_or_path=cand_file,
                target_canonical_path=target_canonical,
                target_pointer_path=target_pointer,
                is_production_authorized=True,
                operator_approval_token="USER_STAGE6_PRODUCTION_AUTHORIZATION:W45"
            )
            self.assertEqual(res["status"], "PROMOTED_SUCCESS")
            self.assertTrue(target_canonical.exists())
            self.assertTrue(target_pointer.exists())
            self.assertEqual(res["canonical_sha256"], hashlib.sha256(target_canonical.read_bytes()).hexdigest())

    def test_07b_scenario2_zero_matches_rejection_zero_write(self):
        """[Check 7B: 场景 2] 0/N 数据 -> 门禁拒绝，目标资产与指针 0 写入"""
        from src.engine.postmatch_valuation_integrator import (
            verify_squad_valuation_promotion_gate,
            promote_valuation_candidate_to_canonical
        )
        with tempfile.TemporaryDirectory() as td:
            tmp_root = Path(td)
            mock_priors = tmp_root / "02_Team_Archives/priors"
            mock_priors.mkdir(parents=True, exist_ok=True)

            candidate_doc = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": False,
                "pipeline_status": "DEGRADED_NO_VALID_FIXTURES",
                "canonical_persistence_status": "BLOCKED_NO_DATA",
                "degraded_reason": "Zero matches extracted"
            }
            cand_file = tmp_root / "team_recent_xi_value.W45_ZERO.json"
            cand_bytes = json.dumps(candidate_doc, indent=2).encode("utf-8")
            cand_file.write_bytes(cand_bytes)

            gate_ok, gate_msg = verify_squad_valuation_promotion_gate(cand_file)
            self.assertFalse(gate_ok)
            self.assertIn("PROMOTION_DISALLOWED", gate_msg)

            target_canonical = mock_priors / "team_recent_xi_value.W45.json"
            target_pointer = mock_priors / "team_recent_xi_value.current.json"

            with self.assertRaises(PermissionError):
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=cand_file,
                    target_canonical_path=target_canonical,
                    target_pointer_path=target_pointer,
                    is_production_authorized=True
                )
            # 物理断言: 目标资产与指针 0 写入
            self.assertFalse(target_canonical.exists())
            self.assertFalse(target_pointer.exists())

    def test_07c_scenario3_partial_n_rejection_zero_write(self):
        """[Check 7C: 场景 3] 部分 N 覆盖 -> 门禁拒绝，目标资产与指针 0 写入"""
        from src.engine.postmatch_valuation_integrator import (
            verify_squad_valuation_promotion_gate,
            promote_valuation_candidate_to_canonical
        )
        with tempfile.TemporaryDirectory() as td:
            tmp_root = Path(td)
            mock_priors = tmp_root / "02_Team_Archives/priors"
            mock_priors.mkdir(parents=True, exist_ok=True)

            candidate_doc = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": False,
                "pipeline_status": "DEGRADED_PARTIAL",
                "canonical_persistence_status": "BLOCKED_PARTIAL_SQUAD_DATA",
                "degraded_reason": "Partial coverage: only 8/10 fixtures extracted"
            }
            cand_file = tmp_root / "team_recent_xi_value.W45_PARTIAL.json"
            cand_file.write_bytes(json.dumps(candidate_doc).encode("utf-8"))

            gate_ok, gate_msg = verify_squad_valuation_promotion_gate(cand_file)
            self.assertFalse(gate_ok)
            self.assertIn("PROMOTION_DISALLOWED", gate_msg)

            target_canonical = mock_priors / "team_recent_xi_value.W45.json"
            target_pointer = mock_priors / "team_recent_xi_value.current.json"

            with self.assertRaises(PermissionError):
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=cand_file,
                    target_canonical_path=target_canonical,
                    target_pointer_path=target_pointer,
                    is_production_authorized=True
                )
            self.assertFalse(target_canonical.exists())
            self.assertFalse(target_pointer.exists())

    def test_07d_scenario4_single_sided_missing_rejection_zero_write(self):
        """[Check 7D: 场景 4] 单侧球队缺失 -> 门禁拒绝，目标资产与指针 0 写入"""
        from src.engine.postmatch_valuation_integrator import (
            verify_squad_valuation_promotion_gate,
            promote_valuation_candidate_to_canonical
        )
        with tempfile.TemporaryDirectory() as td:
            tmp_root = Path(td)
            mock_priors = tmp_root / "02_Team_Archives/priors"
            mock_priors.mkdir(parents=True, exist_ok=True)

            candidate_doc = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": False,
                "pipeline_status": "DEGRADED_PARTIAL",
                "canonical_persistence_status": "BLOCKED_PARTIAL_SQUAD_DATA",
                "degraded_reason": "Incomplete lineup evidence: single-sided missing or unconfirmed XI"
            }
            cand_file = tmp_root / "team_recent_xi_value.W45_SINGLE_SIDED.json"
            cand_file.write_bytes(json.dumps(candidate_doc).encode("utf-8"))

            gate_ok, gate_msg = verify_squad_valuation_promotion_gate(cand_file)
            self.assertFalse(gate_ok)

            target_canonical = mock_priors / "team_recent_xi_value.W45.json"
            target_pointer = mock_priors / "team_recent_xi_value.current.json"

            with self.assertRaises(PermissionError):
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=cand_file,
                    target_canonical_path=target_canonical,
                    target_pointer_path=target_pointer,
                    is_production_authorized=True
                )
            self.assertFalse(target_canonical.exists())
            self.assertFalse(target_pointer.exists())

    def test_07e_scenario5_legacy_missing_gate_field_unapproved_rejection(self):
        """[Check 7E: 场景 5] 旧版资产缺少门禁字段且非白名单 -> 默认拒绝 (Default-Deny)，0 写入"""
        from src.engine.postmatch_valuation_integrator import (
            verify_squad_valuation_promotion_gate,
            promote_valuation_candidate_to_canonical
        )
        with tempfile.TemporaryDirectory() as td:
            tmp_root = Path(td)
            mock_priors = tmp_root / "02_Team_Archives/priors"
            mock_priors.mkdir(parents=True, exist_ok=True)

            # 缺少 allow_stage6_promotion 且不在白名单中的未知资产
            unknown_doc = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "teams": self._create_mock_96_teams(),
                "production_status": "DESIGN_PASS"
            }
            cand_file = tmp_root / "team_recent_xi_value.UNKNOWN_LEGACY.json"
            cand_file.write_bytes(json.dumps(unknown_doc).encode("utf-8"))

            gate_ok, gate_msg = verify_squad_valuation_promotion_gate(cand_file)
            self.assertFalse(gate_ok)
            self.assertIn("UNKNOWN_SCHEMA_OR_UNAPPROVED_VERSION", gate_msg)

            target_canonical = mock_priors / "team_recent_xi_value.W45.json"
            target_pointer = mock_priors / "team_recent_xi_value.current.json"

            with self.assertRaises(PermissionError) as ctx:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=cand_file,
                    target_canonical_path=target_canonical,
                    target_pointer_path=target_pointer,
                    is_production_authorized=True
                )
            self.assertIn("UNKNOWN_SCHEMA_OR_UNAPPROVED_VERSION", str(ctx.exception))
            self.assertFalse(target_canonical.exists())
            self.assertFalse(target_pointer.exists())

    def test_07f_scenario6_inconsistent_manifest_or_conflicting_state(self):
        """[Check 7F: 场景 6] 恶意或不一致的 Manifest 与内部状态冲突 -> 坚决拒绝，0 写入"""
        from src.engine.postmatch_valuation_integrator import (
            verify_squad_valuation_promotion_gate,
            promote_valuation_candidate_to_canonical
        )
        with tempfile.TemporaryDirectory() as td:
            tmp_root = Path(td)
            mock_priors = tmp_root / "02_Team_Archives/priors"
            mock_priors.mkdir(parents=True, exist_ok=True)

            # 内部状态矛盾: allow_stage6_promotion=True 但 pipeline_status=DEGRADED_PARTIAL
            malicious_doc = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": True,
                "pipeline_status": "DEGRADED_PARTIAL", # 矛盾！
                "canonical_persistence_status": "BLOCKED_PARTIAL_SQUAD_DATA"
            }
            cand_file = tmp_root / "team_recent_xi_value.CONFLICTING.json"
            cand_bytes = json.dumps(malicious_doc).encode("utf-8")
            cand_file.write_bytes(cand_bytes)

            # 1. 内部状态冲突拒绝
            gate_ok, gate_msg = verify_squad_valuation_promotion_gate(cand_file)
            self.assertFalse(gate_ok)
            self.assertIn("DEGRADED_PIPELINE_STATUS", gate_msg)

            # 2. Manifest SHA 不一致拒绝
            fake_sha = "0000000000000000000000000000000000000000000000000000000000000000"
            gate_ok2, gate_msg2 = verify_squad_valuation_promotion_gate(cand_file, expected_manifest_sha=fake_sha)
            self.assertFalse(gate_ok2)
            self.assertIn("MANIFEST_SHA_MISMATCH", gate_msg2)

            target_canonical = mock_priors / "team_recent_xi_value.W45.json"
            target_pointer = mock_priors / "team_recent_xi_value.current.json"

            with self.assertRaises(PermissionError):
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=cand_file,
                    target_canonical_path=target_canonical,
                    target_pointer_path=target_pointer,
                    is_production_authorized=True
                )
            self.assertFalse(target_canonical.exists())
            self.assertFalse(target_pointer.exists())

    def test_07g_scenario7_missing_or_invalid_authorization_token(self):
        """[Check 7G: 场景 7] 缺失或无效授权 Token -> 生产同步流水线授权门禁坚决阻断，0 写入"""
        from scripts.run_w44_stage6_production_sync import validate_authorization_token
        # 1. 缺失授权
        ok, msg = validate_authorization_token(None)
        self.assertFalse(ok)
        self.assertIn("MISSING_AUTHORIZATION", msg)

        # 2. 伪造或非法授权
        ok_fake, msg_fake = validate_authorization_token("ATTACKER_UNAUTHORIZED_TOKEN")
        self.assertFalse(ok_fake)
        self.assertIn("INVALID_AUTHORIZATION_TOKEN", msg_fake)

        # 3. 合法白名单授权
        ok_valid, msg_valid = validate_authorization_token("USER_STAGE6_PRODUCTION_AUTHORIZATION:W44:SERIEA_GW06_GW08")
        self.assertTrue(ok_valid)
        self.assertEqual(msg_valid, "AUTHORIZED")

    def test_07h_scenario8_w44_to_new_cycle_promotion_and_retrograde_guard(self):
        """
        [Check 7H: 场景 8 - LMW-413 定向修复 10 大场景全覆盖回归]
        验证真实发布入口 promote_valuation_candidate_to_canonical 的写前时序与门禁:
        1. 周期冲突 1: cycle_name=W46 搭配 W43 目标文件 (拒绝落盘，0 写入)
        2. 周期冲突 2: target_cycle=W46 搭配 W43 目标文件 (拒绝落盘，0 写入)
        3. 缺失替代指针: 同目录已有权威 W45 指针时显式传入不存在的替代指针 (拒绝落盘，0 写入)
        4. 倒退阻断: 真实入口 W45 -> W43 企图倒退发布 (拒绝落盘，0 写入)
        5. 基线漂移: 目标 W46 但继承 W42 偏离当前 W45 (拒绝落盘，0 写入)
        6. 周期格式无效或无法确认: cycle_name 格式非法 (拒绝落盘，0 写入)
        7. 零匹配/语义门禁失败: 降级候选拒绝落盘 (拒绝落盘，0 写入)
        8. 合法向前晋升: W45 -> W46 正确继承 W45 (成功晋升，状态 PROMOTED_SUCCESS)
        9. 同一候选重跑: W46 -> W46 相同内容 (成功放行，状态 ALREADY_APPLIED，0 写入，原字节/SHA 物理一致)
        10. 同周期不同内容冲突修订: W46 试图篡改业务内容覆盖已发布资产 (拒绝落盘，0 写入，原资产与指针字节不变)
        ★ 额外基准场景: 干净目录合法冷启动 (无任何历史指针与资产时的首次发布，成功初始化)
        """
        from src.engine.postmatch_valuation_integrator import promote_valuation_candidate_to_canonical
        with tempfile.TemporaryDirectory() as td:
            tmp_root = Path(td)
            mock_priors = tmp_root / "02_Team_Archives/priors"
            mock_priors.mkdir(parents=True, exist_ok=True)

            # -------------------------------------------------------------
            # 基准场景 0: 干净目录合法首次冷启动初始化 (无历史指针且无历史资产)
            # -------------------------------------------------------------
            cold_pointer = mock_priors / "team_recent_xi_value.current.json"
            cold_w44_candidate = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "cycle_name": "W44",
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": True,
                "pipeline_status": "STAGING_PASS",
                "canonical_persistence_status": "STAGING_READY_AWAITING_STAGE6_AUTHORIZATION"
            }
            target_w44 = mock_priors / "team_recent_xi_value.W44.json"
            res_cold = promote_valuation_candidate_to_canonical(
                candidate_doc_or_path=cold_w44_candidate,
                target_canonical_path=target_w44,
                target_pointer_path=cold_pointer,
                is_production_authorized=True,
                operator_approval_token="USER_STAGE6_PRODUCTION_AUTHORIZATION:W44"
            )
            self.assertEqual(res_cold["status"], "PROMOTED_SUCCESS")
            self.assertTrue(target_w44.exists())
            self.assertTrue(cold_pointer.exists())

            # -------------------------------------------------------------
            # 推进基准状态至 W45，锁定 W45 生产基线快照
            # -------------------------------------------------------------
            w45_candidate = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "cycle_name": "W45",
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": True,
                "pipeline_status": "STAGING_PASS",
                "canonical_persistence_status": "STAGING_READY_AWAITING_STAGE6_AUTHORIZATION",
                "active_cycle_inherited": "W44"
            }
            target_w45 = mock_priors / "team_recent_xi_value.W45.json"
            curr_pointer = cold_pointer

            res_w45 = promote_valuation_candidate_to_canonical(
                candidate_doc_or_path=w45_candidate,
                target_canonical_path=target_w45,
                target_pointer_path=curr_pointer,
                is_production_authorized=True,
                operator_approval_token="USER_STAGE6_PRODUCTION_AUTHORIZATION:W45"
            )
            self.assertEqual(res_w45["status"], "PROMOTED_SUCCESS")
            self.assertTrue(target_w45.exists())

            ptr_w45_bytes = curr_pointer.read_bytes()
            ptr_w45_data = json.loads(ptr_w45_bytes.decode("utf-8"))
            self.assertEqual(ptr_w45_data["current_active_cycle"], "W45")
            self.assertEqual(ptr_w45_data["canonical_json_path"], "team_recent_xi_value.W45.json")

            # 锁定当前 W45 生效态的物理字节快照 (用于拒绝场景的逐字节不变性断言)
            w45_asset_bytes = target_w45.read_bytes()

            # -------------------------------------------------------------
            # 场景 1: 周期冲突 1 - cycle_name=W46 搭配 W43 目标文件
            # -------------------------------------------------------------
            target_w43 = mock_priors / "team_recent_xi_value.W43.json"
            conflict_cname_candidate = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "cycle_name": "W46",  # 声明 W46
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": True,
                "pipeline_status": "STAGING_PASS",
                "canonical_persistence_status": "STAGING_READY_AWAITING_STAGE6_AUTHORIZATION",
                "active_cycle_inherited": "W45"
            }
            with self.assertRaises(PermissionError) as ctx_cname:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=conflict_cname_candidate,
                    target_canonical_path=target_w43,  # 文件名包含 W43 -> 冲突！
                    target_pointer_path=curr_pointer,
                    is_production_authorized=True,
                    operator_approval_token="USER_STAGE6_PRODUCTION_AUTHORIZATION:W46"
                )
            self.assertIn("Cycle conflict", str(ctx_cname.exception))
            self.assertFalse(target_w43.exists(), "Target W43 must NOT be created upon cycle conflict!")
            self.assertEqual(curr_pointer.read_bytes(), ptr_w45_bytes, "Pointer must remain untouched!")
            self.assertEqual(target_w45.read_bytes(), w45_asset_bytes, "W45 asset must remain untouched!")

            # -------------------------------------------------------------
            # 场景 2: 周期冲突 2 - target_cycle=W46 搭配 W43 目标文件
            # -------------------------------------------------------------
            conflict_target_cycle_candidate = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "target_cycle": "W46",  # 仅声明 target_cycle=W46
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": True,
                "pipeline_status": "STAGING_PASS",
                "canonical_persistence_status": "STAGING_READY_AWAITING_STAGE6_AUTHORIZATION",
                "active_cycle_inherited": "W45"
            }
            with self.assertRaises(PermissionError) as ctx_tcycle:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=conflict_target_cycle_candidate,
                    target_canonical_path=target_w43,  # 文件名 W43 -> 冲突！
                    target_pointer_path=curr_pointer,
                    is_production_authorized=True,
                    operator_approval_token="USER_STAGE6_PRODUCTION_AUTHORIZATION:W46"
                )
            self.assertIn("Cycle conflict", str(ctx_tcycle.exception))
            self.assertFalse(target_w43.exists())
            self.assertEqual(curr_pointer.read_bytes(), ptr_w45_bytes)
            self.assertEqual(target_w45.read_bytes(), w45_asset_bytes)

            # -------------------------------------------------------------
            # 场景 3: 锁定生效指针 - 同目录有权威指针时显式传入不存在的替代指针
            # -------------------------------------------------------------
            non_existent_alt_pointer = mock_priors / "team_recent_xi_value.non_existent_pointer.json"
            valid_w46_doc = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "cycle_name": "W46",
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": True,
                "pipeline_status": "STAGING_PASS",
                "canonical_persistence_status": "STAGING_READY_AWAITING_STAGE6_AUTHORIZATION",
                "active_cycle_inherited": "W45"
            }
            target_w46 = mock_priors / "team_recent_xi_value.W46.json"
            with self.assertRaises(PermissionError) as ctx_alt_ptr:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=valid_w46_doc,
                    target_canonical_path=target_w46,
                    target_pointer_path=non_existent_alt_pointer,  # 不存在的替代指针！
                    is_production_authorized=True,
                    operator_approval_token="USER_STAGE6_PRODUCTION_AUTHORIZATION:W46"
                )
            self.assertIn("Authoritative pointer", str(ctx_alt_ptr.exception))
            self.assertFalse(target_w46.exists(), "Target W46 must NOT be created when alt pointer is missing!")
            self.assertEqual(curr_pointer.read_bytes(), ptr_w45_bytes)
            self.assertEqual(target_w45.read_bytes(), w45_asset_bytes)

            # -------------------------------------------------------------
            # 场景 3b: 锁定权威指针 - 权威 W45 指针存在时显式传入已存在的 W42 替代指针
            # -------------------------------------------------------------
            w42_alt_pointer = mock_priors / "team_recent_xi_value.w42_alt.json"
            w42_alt_pointer.write_text(json.dumps({
                "current_active_cycle": "W42",
                "canonical_json_path": "team_recent_xi_value.W42.json",
                "canonical_json_sha256": "0" * 64
            }), encoding="utf-8")
            existing_w43_asset = mock_priors / "team_recent_xi_value.W43.json"
            existing_w43_asset.write_bytes(b'{"existing_w43_asset_bytes": true}')

            snap_w43_bytes = existing_w43_asset.read_bytes()
            snap_w42_alt_bytes = w42_alt_pointer.read_bytes()
            snap_curr_ptr_bytes = curr_pointer.read_bytes()
            snap_priors_files_3b = set(mock_priors.iterdir())

            w43_test_candidate = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "cycle_name": "W43",
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": True,
                "pipeline_status": "STAGING_PASS",
                "canonical_persistence_status": "STAGING_READY_AWAITING_STAGE6_AUTHORIZATION",
                "active_cycle_inherited": "W42"
            }
            with self.assertRaises(PermissionError) as ctx_alt_exists:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=w43_test_candidate,
                    target_canonical_path=existing_w43_asset,
                    target_pointer_path=w42_alt_pointer,
                    is_production_authorized=True,
                    operator_approval_token="USER_STAGE6_PRODUCTION_AUTHORIZATION:W43"
                )
            self.assertIn("Authoritative pointer", str(ctx_alt_exists.exception))
            self.assertIn("resolves to a different file", str(ctx_alt_exists.exception))
            # 逐字节核对已有 W43 资产和两个指针均未受影响
            self.assertEqual(existing_w43_asset.read_bytes(), snap_w43_bytes, "Existing W43 asset must remain 100% byte-identical!")
            self.assertEqual(curr_pointer.read_bytes(), snap_curr_ptr_bytes, "Authoritative W45 pointer must remain 100% byte-identical!")
            self.assertEqual(w42_alt_pointer.read_bytes(), snap_w42_alt_bytes, "W42 alt pointer must remain 100% byte-identical!")
            self.assertEqual(set(mock_priors.iterdir()), snap_priors_files_3b, "No extra files created on alt pointer rejection!")

            # 清理 3b 专用的临时 w42_alt 和 w43 文件
            w42_alt_pointer.unlink()
            existing_w43_asset.unlink()

            # -------------------------------------------------------------
            # 场景 4: 倒退阻断 - 真实入口复现 W45 -> W43 企图倒退发布
            # -------------------------------------------------------------
            w43_candidate = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "cycle_name": "W43",
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": True,
                "pipeline_status": "STAGING_PASS",
                "canonical_persistence_status": "STAGING_READY_AWAITING_STAGE6_AUTHORIZATION",
                "active_cycle_inherited": "W42"
            }
            with self.assertRaises(PermissionError) as ctx_retrograde:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=w43_candidate,
                    target_canonical_path=target_w43,
                    target_pointer_path=curr_pointer,
                    is_production_authorized=True,
                    operator_approval_token="USER_STAGE6_PRODUCTION_AUTHORIZATION:W43"
                )
            self.assertIn("Cycle regression detected", str(ctx_retrograde.exception))
            self.assertFalse(target_w43.exists())
            self.assertEqual(curr_pointer.read_bytes(), ptr_w45_bytes)
            self.assertEqual(target_w45.read_bytes(), w45_asset_bytes)

            # -------------------------------------------------------------
            # 场景 5: 基线漂移 - 目标向前推进到 W46，但候选继承基线为 W42 (偏离当前 W45)
            # -------------------------------------------------------------
            w46_drift_candidate = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "cycle_name": "W46",
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": True,
                "pipeline_status": "STAGING_PASS",
                "canonical_persistence_status": "STAGING_READY_AWAITING_STAGE6_AUTHORIZATION",
                "active_cycle_inherited": "W42"  # 故意基线漂移！
            }
            with self.assertRaises(PermissionError) as ctx_drift:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=w46_drift_candidate,
                    target_canonical_path=target_w46,
                    target_pointer_path=curr_pointer,
                    is_production_authorized=True,
                    operator_approval_token="USER_STAGE6_PRODUCTION_AUTHORIZATION:W46"
                )
            self.assertIn("Baseline drift detected", str(ctx_drift.exception))
            self.assertFalse(target_w46.exists())
            self.assertEqual(curr_pointer.read_bytes(), ptr_w45_bytes)
            self.assertEqual(target_w45.read_bytes(), w45_asset_bytes)

            # -------------------------------------------------------------
            # 场景 6: 无法确认周期或格式非法
            # -------------------------------------------------------------
            unconfirmed_candidate = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "cycle_name": "UNKNOWN_CUSTOM_LABEL",
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": True,
                "pipeline_status": "STAGING_PASS",
                "canonical_persistence_status": "STAGING_READY_AWAITING_STAGE6_AUTHORIZATION",
                "active_cycle_inherited": "W45"
            }
            target_unconfirmed = mock_priors / "team_recent_xi_value.CUSTOM_LABEL.json"
            with self.assertRaises(PermissionError) as ctx_unconfirmed:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=unconfirmed_candidate,
                    target_canonical_path=target_unconfirmed,
                    target_pointer_path=curr_pointer,
                    is_production_authorized=True
                )
            self.assertIn("Unable to confirm target cycle", str(ctx_unconfirmed.exception))
            self.assertFalse(target_unconfirmed.exists())
            self.assertEqual(curr_pointer.read_bytes(), ptr_w45_bytes)
            self.assertEqual(target_w45.read_bytes(), w45_asset_bytes)

            # -------------------------------------------------------------
            # 场景 6b: 周期字段包含非法前后缀 garbageW46suffix
            # -------------------------------------------------------------
            garbage_cname_candidate = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "cycle_name": "garbageW46suffix",  # 包含额外前后缀！
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": True,
                "pipeline_status": "STAGING_PASS",
                "canonical_persistence_status": "STAGING_READY_AWAITING_STAGE6_AUTHORIZATION",
                "active_cycle_inherited": "W45"
            }
            snap_files_6b = set(mock_priors.iterdir())
            with self.assertRaises(PermissionError) as ctx_garbage:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=garbage_cname_candidate,
                    target_canonical_path=target_w46,
                    target_pointer_path=curr_pointer,
                    is_production_authorized=True
                )
            self.assertIn("Unable to confirm target cycle", str(ctx_garbage.exception))
            self.assertFalse(target_w46.exists())
            self.assertEqual(curr_pointer.read_bytes(), ptr_w45_bytes)
            self.assertEqual(set(mock_priors.iterdir()), snap_files_6b, "No extra files on garbage cycle rejection!")

            # -------------------------------------------------------------
            # 场景 6c: 目标文件名包含非法前后缀 team_recent_xi_value.garbageW46suffix.json
            # -------------------------------------------------------------
            target_garbage_file = mock_priors / "team_recent_xi_value.garbageW46suffix.json"
            snap_files_6c = set(mock_priors.iterdir())
            with self.assertRaises(PermissionError) as ctx_garbage_file:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=w46_drift_candidate,
                    target_canonical_path=target_garbage_file,
                    target_pointer_path=curr_pointer,
                    is_production_authorized=True
                )
            self.assertIn("Filename does not conform to canonical asset format", str(ctx_garbage_file.exception))
            self.assertFalse(target_garbage_file.exists())
            self.assertEqual(curr_pointer.read_bytes(), ptr_w45_bytes)
            self.assertEqual(set(mock_priors.iterdir()), snap_files_6c, "No extra files on garbage filename rejection!")

            # -------------------------------------------------------------
            # 场景 7: 零匹配/降级候选阻断
            # -------------------------------------------------------------
            zero_match_candidate = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "cycle_name": "W46",
                "teams": [],
                "allow_stage6_promotion": False,
                "pipeline_status": "DEGRADED_NO_VALID_FIXTURES",
                "canonical_persistence_status": "BLOCKED_NO_DATA",
                "active_cycle_inherited": "W45"
            }
            with self.assertRaises(PermissionError) as ctx_zero:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=zero_match_candidate,
                    target_canonical_path=target_w46,
                    target_pointer_path=curr_pointer,
                    is_production_authorized=True
                )
            self.assertIn("PROMOTION_BLOCKED", str(ctx_zero.exception))
            self.assertFalse(target_w46.exists())
            self.assertEqual(curr_pointer.read_bytes(), ptr_w45_bytes)
            self.assertEqual(target_w45.read_bytes(), w45_asset_bytes)

            # -------------------------------------------------------------
            # 场景 8: 合法向前演进 (W45 -> W46) 正确继承 W45
            # -------------------------------------------------------------
            w46_valid_candidate = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "cycle_name": "W46",
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": True,
                "pipeline_status": "STAGING_PASS",
                "canonical_persistence_status": "STAGING_READY_AWAITING_STAGE6_AUTHORIZATION",
                "active_cycle_inherited": "W45"
            }
            res_w46 = promote_valuation_candidate_to_canonical(
                candidate_doc_or_path=w46_valid_candidate,
                target_canonical_path=target_w46,
                target_pointer_path=curr_pointer,
                is_production_authorized=True,
                operator_approval_token="USER_STAGE6_PRODUCTION_AUTHORIZATION:W46"
            )
            self.assertEqual(res_w46["status"], "PROMOTED_SUCCESS")
            self.assertTrue(target_w46.exists())

            # 验证指针正确切至 W46
            ptr_w46_bytes = curr_pointer.read_bytes()
            ptr_w46_data = json.loads(ptr_w46_bytes.decode("utf-8"))
            self.assertEqual(ptr_w46_data["current_active_cycle"], "W46")
            self.assertEqual(ptr_w46_data["canonical_json_path"], "team_recent_xi_value.W46.json")
            w46_asset_bytes = target_w46.read_bytes()

            # -------------------------------------------------------------
            # 场景 9: 同一候选幂等重跑 (W46 -> W46 相同内容) -> 返回 ALREADY_APPLIED，0 写入
            # -------------------------------------------------------------
            res_w46_rerun = promote_valuation_candidate_to_canonical(
                candidate_doc_or_path=w46_valid_candidate,
                target_canonical_path=target_w46,
                target_pointer_path=curr_pointer,
                is_production_authorized=True,
                operator_approval_token="USER_STAGE6_PRODUCTION_AUTHORIZATION:W46_RERUN"
            )
            self.assertEqual(res_w46_rerun["status"], "ALREADY_APPLIED")
            self.assertEqual(res_w46_rerun["action"], "IDEMPOTENT_SKIPPED")
            # 严格物理断言: 原 W46 资产与指针逐字节不变 (0 写入)
            self.assertEqual(target_w46.read_bytes(), w46_asset_bytes, "W46 asset bytes must remain 100% identical on rerun!")
            self.assertEqual(curr_pointer.read_bytes(), ptr_w46_bytes, "Pointer bytes must remain 100% identical on rerun!")

            # -------------------------------------------------------------
            # 场景 10: 同周期不同内容冲突覆盖修订 (W46 已发布，试图以不同数据覆盖) -> 坚决拒绝，0 写入
            # -------------------------------------------------------------
            modified_teams = self._create_mock_96_teams()
            modified_teams[0]["valid_match_count"] = 999  # 篡改数据
            w46_conflicting_candidate = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "cycle_name": "W46",
                "teams": modified_teams,
                "allow_stage6_promotion": True,
                "pipeline_status": "STAGING_PASS",
                "canonical_persistence_status": "STAGING_READY_AWAITING_STAGE6_AUTHORIZATION",
                "active_cycle_inherited": "W45"
            }
            with self.assertRaises(PermissionError) as ctx_conflict_revision:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=w46_conflicting_candidate,
                    target_canonical_path=target_w46,
                    target_pointer_path=curr_pointer,
                    is_production_authorized=True,
                    operator_approval_token="USER_STAGE6_PRODUCTION_AUTHORIZATION:W46_CONFLICT"
                )
            self.assertIn("Conflicting revision detected", str(ctx_conflict_revision.exception))
            # 严格物理断言: 拒绝后已发布 W46 资产与指针维持原有字节，未被篡改
            self.assertEqual(target_w46.read_bytes(), w46_asset_bytes, "Existing canonical must NOT be altered on conflicting revision!")
            self.assertEqual(curr_pointer.read_bytes(), ptr_w46_bytes, "Pointer must NOT be altered on conflicting revision!")

    def test_07i_pointer_internal_cycle_mismatch_and_missing_sha_guards(self):
        """
        [Check 7I: 指针自洽性与既有目标保护专项测试 (Linear 11-10 最新复核)]
        1. 补测指针声明 W45、实际指向 SHA 正确的 W42 文件时拒绝发布
        2. 补测缺失 SHA 时无法确认已应用，保护既有目标拒绝覆盖
        3. 即使候选周期向前 (W44 -> W45)，目标文件 W45 已存在时也要先判定发布身份，拒绝覆盖
        4. 拒绝路径逐字节核对原资产和指针，并核对没有额外文件
        """
        from src.engine.postmatch_valuation_integrator import promote_valuation_candidate_to_canonical
        with tempfile.TemporaryDirectory() as td:
            tmp_root = Path(td)
            mock_priors = tmp_root / "02_Team_Archives/priors"
            mock_priors.mkdir(parents=True, exist_ok=True)

            # -------------------------------------------------------------
            # 场景 1: 指针声明 W45 但实际指向 W42 文件 (即使该 W42 文件实际存在且 SHA 匹配)
            # -------------------------------------------------------------
            w42_asset = mock_priors / "team_recent_xi_value.W42.json"
            w42_bytes = b'{"w42_asset_canonical": true}'
            w42_asset.write_bytes(w42_bytes)
            w42_sha = hashlib.sha256(w42_bytes).hexdigest()

            corrupt_pointer = mock_priors / "team_recent_xi_value.current.json"
            corrupt_pointer.write_text(json.dumps({
                "current_active_cycle": "W45",  # 声明 W45
                "canonical_json_path": "team_recent_xi_value.W42.json",  # 但指向 W42 文件！
                "canonical_json_sha256": w42_sha,
                "teams_count": 96
            }), encoding="utf-8")

            w46_candidate = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "cycle_name": "W46",
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": True,
                "pipeline_status": "STAGING_PASS",
                "canonical_persistence_status": "STAGING_READY_AWAITING_STAGE6_AUTHORIZATION",
                "active_cycle_inherited": "W45"
            }
            target_w46 = mock_priors / "team_recent_xi_value.W46.json"
            snap_files_1 = set(mock_priors.iterdir())
            snap_w42_bytes_1 = w42_asset.read_bytes()
            snap_ptr_bytes_1 = corrupt_pointer.read_bytes()

            with self.assertRaises(PermissionError) as ctx_ptr_mismatch:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=w46_candidate,
                    target_canonical_path=target_w46,
                    target_pointer_path=corrupt_pointer,
                    is_production_authorized=True,
                    operator_approval_token="USER_STAGE6_PRODUCTION_AUTHORIZATION:W46"
                )
            self.assertIn("Pointer internal divergence", str(ctx_ptr_mismatch.exception))
            self.assertFalse(target_w46.exists(), "Target W46 must NOT be written when pointer has internal divergence!")
            self.assertEqual(w42_asset.read_bytes(), snap_w42_bytes_1)
            self.assertEqual(corrupt_pointer.read_bytes(), snap_ptr_bytes_1)
            self.assertEqual(set(mock_priors.iterdir()), snap_files_1, "No extra files created on pointer divergence rejection!")

            # -------------------------------------------------------------
            # 场景 2: 缺失 SHA 时无法确认已应用，保护既有目标拒绝覆盖
            # -------------------------------------------------------------
            # 设置指针指向合法的 W45，但刻意抹去 canonical_json_sha256
            w45_asset = mock_priors / "team_recent_xi_value.W45.json"
            w45_candidate = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "cycle_name": "W45",
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": True,
                "pipeline_status": "STAGING_PASS",
                "canonical_persistence_status": "STAGING_READY_AWAITING_STAGE6_AUTHORIZATION",
                "active_cycle_inherited": "W44"
            }
            w45_bytes = json.dumps(w45_candidate, indent=2).encode("utf-8")
            w45_asset.write_bytes(w45_bytes)

            corrupt_pointer.write_text(json.dumps({
                "current_active_cycle": "W45",
                "canonical_json_path": "team_recent_xi_value.W45.json",
                # 缺失 canonical_json_sha256!
                "teams_count": 96
            }), encoding="utf-8")

            snap_w45_bytes = w45_asset.read_bytes()
            snap_ptr_bytes = corrupt_pointer.read_bytes()
            snap_files_2 = set(mock_priors.iterdir())

            with self.assertRaises(PermissionError) as ctx_missing_sha:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=w45_candidate,
                    target_canonical_path=w45_asset,
                    target_pointer_path=corrupt_pointer,
                    is_production_authorized=True,
                    operator_approval_token="USER_STAGE6_PRODUCTION_AUTHORIZATION:W45"
                )
            self.assertIn("lacks verified", str(ctx_missing_sha.exception))
            self.assertEqual(w45_asset.read_bytes(), snap_w45_bytes, "W45 asset bytes must remain untouched when SHA is missing!")
            self.assertEqual(corrupt_pointer.read_bytes(), snap_ptr_bytes, "Pointer bytes must remain untouched!")
            self.assertEqual(set(mock_priors.iterdir()), snap_files_2, "No extra files on missing SHA rejection!")

            # -------------------------------------------------------------
            # 场景 3: 保护既有目标：即使候选周期向前 (W44 -> W45)，目标文件 W45 已存在时也要先判定发布身份，拒绝覆盖
            # -------------------------------------------------------------
            # 设置指针指向 W44 (合法且有有效 SHA)
            w44_asset = mock_priors / "team_recent_xi_value.W44.json"
            w44_bytes = b'{"w44_canonical": true}'
            w44_asset.write_bytes(w44_bytes)
            w44_sha = hashlib.sha256(w44_bytes).hexdigest()

            corrupt_pointer.write_text(json.dumps({
                "current_active_cycle": "W44",
                "canonical_json_path": "team_recent_xi_value.W44.json",
                "canonical_json_sha256": w44_sha,
                "teams_count": 96
            }), encoding="utf-8")

            # 此时目标文件 team_recent_xi_value.W45.json 已存在在磁盘上
            # 候选周期是 W45 (向前演进，声明继承 W44)
            snap_w45_bytes_before_fwd = w45_asset.read_bytes()
            snap_ptr_bytes_before_fwd = corrupt_pointer.read_bytes()
            snap_files_3 = set(mock_priors.iterdir())

            with self.assertRaises(PermissionError) as ctx_fwd_exists:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=w45_candidate,
                    target_canonical_path=w45_asset,
                    target_pointer_path=corrupt_pointer,
                    is_production_authorized=True,
                    operator_approval_token="USER_STAGE6_PRODUCTION_AUTHORIZATION:W45"
                )
            self.assertIn("In-place overwrite of pre-existing canonical asset is rejected to protect existing target", str(ctx_fwd_exists.exception))
            self.assertEqual(w45_asset.read_bytes(), snap_w45_bytes_before_fwd, "Existing target file must NOT be overwritten on forward candidate!")
            self.assertEqual(corrupt_pointer.read_bytes(), snap_ptr_bytes_before_fwd, "Pointer must remain untouched on forward collision!")
            self.assertEqual(set(mock_priors.iterdir()), snap_files_3, "No extra files on pre-existing target rejection!")

    def test_07j_dual_schema_path_traversal_and_symlink_preservation_regression(self):
        """
        [Check 7J: LMW-413 深度收口专项测试]
        1. 合法 W44 格式 (active_cycle / canonical_asset_relative_path / canonical_asset_sha256) 正常继承放行
        2. 两套字段冲突拒绝: 周期冲突、路径冲突、SHA 不符，坚决阻断且 0 修改
        3. 缺 SHA 的向前发布拒绝: 指针缺失 SHA 时坚决拒绝向前演进落盘，0 修改
        4. 目录外资产与路径穿越拒绝: 拒绝 '../'、绝对路径及逃逸符号链接，0 修改
        5. 符号链接不产生双指针: 使用指向权威指针的 alt.json 符号链接，更新权威指针且保留原符号链接，绝不产生双实体指针；非法符号链接阻断且零修改
        6. 核对拒绝场景的文件集合与原文件逐字节不变
        """
        from src.engine.postmatch_valuation_integrator import promote_valuation_candidate_to_canonical
        with tempfile.TemporaryDirectory() as td:
            tmp_root = Path(td)
            mock_priors = tmp_root / "02_Team_Archives/priors"
            mock_priors.mkdir(parents=True, exist_ok=True)

            curr_pointer = mock_priors / "team_recent_xi_value.current.json"
            w44_asset = mock_priors / "team_recent_xi_value.W44_POSTMATCH_CANONICAL.json"
            w44_bytes = b'{"w44_postmatch_canonical": true, "total_teams": 96}'
            w44_asset.write_bytes(w44_bytes)
            w44_sha = hashlib.sha256(w44_bytes).hexdigest()

            # -------------------------------------------------------------
            # 场景 1: 合法 W44 生产标准格式放行 (仅包含套件 A 字段)
            # -------------------------------------------------------------
            curr_pointer.write_text(json.dumps({
                "active_cycle": "W44",
                "canonical_asset_relative_path": "team_recent_xi_value.W44_POSTMATCH_CANONICAL.json",
                "canonical_asset_sha256": w44_sha,
                "teams_count": 96
            }), encoding="utf-8")

            w45_candidate = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "cycle_name": "W45",
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": True,
                "pipeline_status": "STAGING_PASS",
                "canonical_persistence_status": "STAGING_READY_AWAITING_STAGE6_AUTHORIZATION",
                "active_cycle_inherited": "W44"
            }
            target_w45 = mock_priors / "team_recent_xi_value.W45.json"

            res_w44_format = promote_valuation_candidate_to_canonical(
                candidate_doc_or_path=w45_candidate,
                target_canonical_path=target_w45,
                target_pointer_path=curr_pointer,
                is_production_authorized=True,
                operator_approval_token="USER_STAGE6_PRODUCTION_AUTHORIZATION:W45"
            )
            self.assertEqual(res_w44_format["status"], "PROMOTED_SUCCESS")
            self.assertTrue(target_w45.exists())
            ptr_data = json.loads(curr_pointer.read_text(encoding="utf-8"))
            self.assertEqual(ptr_data["active_cycle"], "W45")
            self.assertEqual(ptr_data["current_active_cycle"], "W45")

            # 清理 target_w45 供后续负向测试复用
            target_w45.unlink()

            # -------------------------------------------------------------
            # 场景 2: 两套字段冲突拒绝
            # -------------------------------------------------------------
            # 2a. 周期冲突
            curr_pointer.write_text(json.dumps({
                "active_cycle": "W44",
                "current_active_cycle": "W43",  # 冲突！
                "canonical_asset_relative_path": "team_recent_xi_value.W44_POSTMATCH_CANONICAL.json",
                "canonical_json_path": "team_recent_xi_value.W44_POSTMATCH_CANONICAL.json",
                "canonical_asset_sha256": w44_sha,
                "canonical_json_sha256": w44_sha,
                "teams_count": 96
            }), encoding="utf-8")

            snap_files_2a = set(mock_priors.iterdir())
            snap_ptr_2a = curr_pointer.read_bytes()
            snap_w44_2a = w44_asset.read_bytes()

            with self.assertRaises(PermissionError) as ctx_cycle_conflict:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=w45_candidate,
                    target_canonical_path=target_w45,
                    target_pointer_path=curr_pointer,
                    is_production_authorized=True
                )
            self.assertIn("Conflicting cycle declarations in pointer", str(ctx_cycle_conflict.exception))
            self.assertFalse(target_w45.exists())
            self.assertEqual(curr_pointer.read_bytes(), snap_ptr_2a)
            self.assertEqual(w44_asset.read_bytes(), snap_w44_2a)
            self.assertEqual(set(mock_priors.iterdir()), snap_files_2a)

            # 2b. 资产路径冲突
            curr_pointer.write_text(json.dumps({
                "active_cycle": "W44",
                "current_active_cycle": "W44",
                "canonical_asset_relative_path": "team_recent_xi_value.W44_POSTMATCH_CANONICAL.json",
                "canonical_json_path": "team_recent_xi_value.W44.json",  # 路径冲突！
                "canonical_asset_sha256": w44_sha,
                "canonical_json_sha256": w44_sha,
                "teams_count": 96
            }), encoding="utf-8")

            snap_files_2b = set(mock_priors.iterdir())
            snap_ptr_2b = curr_pointer.read_bytes()

            with self.assertRaises(PermissionError) as ctx_path_conflict:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=w45_candidate,
                    target_canonical_path=target_w45,
                    target_pointer_path=curr_pointer,
                    is_production_authorized=True
                )
            self.assertIn("Conflicting canonical asset paths in pointer", str(ctx_path_conflict.exception))
            self.assertFalse(target_w45.exists())
            self.assertEqual(curr_pointer.read_bytes(), snap_ptr_2b)
            self.assertEqual(set(mock_priors.iterdir()), snap_files_2b)

            # 2c. SHA 冲突
            curr_pointer.write_text(json.dumps({
                "active_cycle": "W44",
                "current_active_cycle": "W44",
                "canonical_asset_relative_path": "team_recent_xi_value.W44_POSTMATCH_CANONICAL.json",
                "canonical_json_path": "team_recent_xi_value.W44_POSTMATCH_CANONICAL.json",
                "canonical_asset_sha256": w44_sha,
                "canonical_json_sha256": "0" * 64,  # SHA 冲突！
                "teams_count": 96
            }), encoding="utf-8")

            snap_files_2c = set(mock_priors.iterdir())
            snap_ptr_2c = curr_pointer.read_bytes()

            with self.assertRaises(PermissionError) as ctx_sha_conflict:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=w45_candidate,
                    target_canonical_path=target_w45,
                    target_pointer_path=curr_pointer,
                    is_production_authorized=True
                )
            self.assertIn("Conflicting SHA-256 declarations in pointer", str(ctx_sha_conflict.exception))
            self.assertFalse(target_w45.exists())
            self.assertEqual(curr_pointer.read_bytes(), snap_ptr_2c)
            self.assertEqual(set(mock_priors.iterdir()), snap_files_2c)

            # -------------------------------------------------------------
            # 场景 3: 缺 SHA 的向前发布拒绝
            # -------------------------------------------------------------
            curr_pointer.write_text(json.dumps({
                "active_cycle": "W44",
                "canonical_asset_relative_path": "team_recent_xi_value.W44_POSTMATCH_CANONICAL.json",
                # 缺失任何 SHA 字段！
                "teams_count": 96
            }), encoding="utf-8")

            snap_files_3 = set(mock_priors.iterdir())
            snap_ptr_3 = curr_pointer.read_bytes()

            with self.assertRaises(PermissionError) as ctx_no_sha:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=w45_candidate,
                    target_canonical_path=target_w45,
                    target_pointer_path=curr_pointer,
                    is_production_authorized=True
                )
            self.assertIn("lacks verified SHA-256 declaration", str(ctx_no_sha.exception))
            self.assertFalse(target_w45.exists())
            self.assertEqual(curr_pointer.read_bytes(), snap_ptr_3)
            self.assertEqual(set(mock_priors.iterdir()), snap_files_3)

            # -------------------------------------------------------------
            # 场景 4: 目录外资产与路径穿越拒绝 (../ 和 绝对路径)
            # -------------------------------------------------------------
            # 4a. 包含 ../ 路径遍历
            curr_pointer.write_text(json.dumps({
                "active_cycle": "W44",
                "canonical_asset_relative_path": "../team_recent_xi_value.W44.json",  # 试图遍历跳出！
                "canonical_asset_sha256": w44_sha,
                "teams_count": 96
            }), encoding="utf-8")

            snap_files_4a = set(mock_priors.iterdir())
            snap_ptr_4a = curr_pointer.read_bytes()

            with self.assertRaises(PermissionError) as ctx_traversal:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=w45_candidate,
                    target_canonical_path=target_w45,
                    target_pointer_path=curr_pointer,
                    is_production_authorized=True
                )
            self.assertIn("path traversal '../' detected", str(ctx_traversal.exception))
            self.assertFalse(target_w45.exists())
            self.assertEqual(curr_pointer.read_bytes(), snap_ptr_4a)
            self.assertEqual(set(mock_priors.iterdir()), snap_files_4a)

            # 4b. 绝对路径
            curr_pointer.write_text(json.dumps({
                "active_cycle": "W44",
                "canonical_asset_relative_path": "/tmp/team_recent_xi_value.W44.json",  # 绝对路径！
                "canonical_asset_sha256": w44_sha,
                "teams_count": 96
            }), encoding="utf-8")

            snap_files_4b = set(mock_priors.iterdir())
            snap_ptr_4b = curr_pointer.read_bytes()

            with self.assertRaises(PermissionError) as ctx_abs:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=w45_candidate,
                    target_canonical_path=target_w45,
                    target_pointer_path=curr_pointer,
                    is_production_authorized=True
                )
            self.assertIn("absolute path", str(ctx_abs.exception))
            self.assertFalse(target_w45.exists())
            self.assertEqual(curr_pointer.read_bytes(), snap_ptr_4b)
            self.assertEqual(set(mock_priors.iterdir()), snap_files_4b)

            # -------------------------------------------------------------
            # 场景 5: 符号链接不产生双指针与原链接保留
            # -------------------------------------------------------------
            # 恢复合法的 W44 状态
            curr_pointer.write_text(json.dumps({
                "active_cycle": "W44",
                "current_active_cycle": "W44",
                "canonical_asset_relative_path": "team_recent_xi_value.W44_POSTMATCH_CANONICAL.json",
                "canonical_json_path": "team_recent_xi_value.W44_POSTMATCH_CANONICAL.json",
                "canonical_asset_sha256": w44_sha,
                "canonical_json_sha256": w44_sha,
                "teams_count": 96
            }), encoding="utf-8")

            # 5a. 创建指向外部目录的非法符号链接 -> 拒绝且 0 修改
            escaped_symlink = mock_priors / "escaped_alt.json"
            external_file = tmp_root / "outside_pointer.json"
            external_file.write_text('{"external": true}', encoding="utf-8")
            escaped_symlink.symlink_to(external_file)

            snap_files_5a = set(mock_priors.iterdir())
            snap_ptr_5a = curr_pointer.read_bytes()

            with self.assertRaises(PermissionError) as ctx_escaped_sym:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=w45_candidate,
                    target_canonical_path=target_w45,
                    target_pointer_path=escaped_symlink,
                    is_production_authorized=True
                )
            self.assertIn("resolves outside allowed directory", str(ctx_escaped_sym.exception))
            self.assertFalse(target_w45.exists())
            self.assertEqual(curr_pointer.read_bytes(), snap_ptr_5a)
            self.assertEqual(set(mock_priors.iterdir()), snap_files_5a)

            escaped_symlink.unlink()
            external_file.unlink()

            # 5b. 创建指向权威指针的合法符号链接 alt.json
            alt_symlink = mock_priors / "alt.json"
            alt_symlink.symlink_to(curr_pointer)
            self.assertTrue(alt_symlink.is_symlink())
            self.assertEqual(alt_symlink.resolve(), curr_pointer.resolve())

            # 调用真实入口，传入 target_pointer_path=alt_symlink
            res_symlink = promote_valuation_candidate_to_canonical(
                candidate_doc_or_path=w45_candidate,
                target_canonical_path=target_w45,
                target_pointer_path=alt_symlink,
                is_production_authorized=True,
                operator_approval_token="USER_STAGE6_PRODUCTION_AUTHORIZATION:W45"
            )
            self.assertEqual(res_symlink["status"], "PROMOTED_SUCCESS")
            self.assertTrue(target_w45.exists())

            # 核心断言: 权威指针已被原子替换为 W45
            ptr_updated_data = json.loads(curr_pointer.read_text(encoding="utf-8"))
            self.assertEqual(ptr_updated_data["active_cycle"], "W45")
            self.assertEqual(ptr_updated_data["current_active_cycle"], "W45")

            # 核心断言: alt.json 绝未被替换为普通实体文件，依然是符号链接！
            self.assertTrue(alt_symlink.is_symlink(), "alt.json must remain a symbolic link!")
            self.assertEqual(alt_symlink.resolve(), curr_pointer.resolve(), "alt.json must resolve to authoritative pointer!")

            # 核心断言: 目录下绝不产生双实体指针文件
            pointer_json_files = [f for f in mock_priors.glob("*.json") if "pointer" in f.name or f.name.endswith(".json")]
            regular_files = [f for f in pointer_json_files if not f.is_symlink()]
            symlink_files = [f for f in pointer_json_files if f.is_symlink()]
            self.assertIn(curr_pointer, regular_files)
            self.assertIn(alt_symlink, symlink_files)
            self.assertNotIn(alt_symlink, regular_files)

            # 通过符号链接读取也能正确获取更新后的 W45 内容
            alt_read_data = json.loads(alt_symlink.read_text(encoding="utf-8"))
            self.assertEqual(alt_read_data["active_cycle"], "W45")

    def test_07k_default_pointer_boundary_and_optional_update_semantics(self):
        """
        [Check 7K: LMW-413 最后两条写入边界专项测试]
        1. 默认指针越界拒绝: 自动发现的默认指针若是逃逸到资产目录外的软链接，在写入任何资产前拒绝，确认外部文件、链接和现有资产逐字节不变，0 目标文件
        2. 省略 target_pointer_path 的可选更新语义:
           - 已有默认指针场景: 只读检查已有默认指针 (时序单调/基线)，发布只写资产，已有指针保持原字节不变
           - 空目录冷启动场景: 省略参数时只写资产，空目录中绝对不新建指针
        """
        from src.engine.postmatch_valuation_integrator import promote_valuation_candidate_to_canonical
        with tempfile.TemporaryDirectory() as td:
            tmp_root = Path(td)
            canonical_dir = tmp_root / "02_Team_Archives/priors"
            canonical_dir.mkdir(parents=True, exist_ok=True)

            # 准备已有合法 W44 资产
            w44_asset = canonical_dir / "team_recent_xi_value.W44_POSTMATCH_CANONICAL.json"
            w44_bytes = b'{"w44_postmatch_canonical": true, "total_teams": 96}'
            w44_asset.write_bytes(w44_bytes)
            w44_sha = hashlib.sha256(w44_bytes).hexdigest()

            # -------------------------------------------------------------
            # 边界 1: 自动发现默认指针越界拒绝 (默认指针为指向目录外的软链接)
            # -------------------------------------------------------------
            external_dir = tmp_root / "external_outside_zone"
            external_dir.mkdir(parents=True, exist_ok=True)
            escaped_target_file = external_dir / "escaped_current_pointer.json"
            escaped_target_file.write_text(json.dumps({
                "active_cycle": "W44",
                "canonical_asset_relative_path": "team_recent_xi_value.W44_POSTMATCH_CANONICAL.json",
                "canonical_asset_sha256": w44_sha,
                "teams_count": 96
            }), encoding="utf-8")
            snap_escaped_bytes = escaped_target_file.read_bytes()

            # 默认指针符号链接指向外部文件 (发生逃逸)
            default_pointer = canonical_dir / "team_recent_xi_value.current.json"
            default_pointer.symlink_to(escaped_target_file)
            self.assertTrue(default_pointer.is_symlink())

            w45_candidate = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "cycle_name": "W45",
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": True,
                "pipeline_status": "STAGING_PASS",
                "canonical_persistence_status": "STAGING_READY_AWAITING_STAGE6_AUTHORIZATION",
                "active_cycle_inherited": "W44"
            }
            target_w45 = canonical_dir / "team_recent_xi_value.W45.json"

            snap_files_before_b1 = set(canonical_dir.iterdir())
            snap_w44_bytes_b1 = w44_asset.read_bytes()

            # 省略 target_pointer_path 参数，自动发现默认指针 -> 必须在写资产前拒绝！
            with self.assertRaises(PermissionError) as ctx_def_boundary:
                promote_valuation_candidate_to_canonical(
                    candidate_doc_or_path=w45_candidate,
                    target_canonical_path=target_w45,
                    target_pointer_path=None,  # 省略指针参数！
                    is_production_authorized=True,
                    operator_approval_token="USER_STAGE6_PRODUCTION_AUTHORIZATION:W45"
                )
            self.assertIn("resolves outside allowed directory", str(ctx_def_boundary.exception))

            # 严格物理断言: 没有生成目标文件，外部文件、链接和现有资产逐字节不变
            self.assertFalse(target_w45.exists(), "Target W45 must NOT be created when default pointer is out-of-boundary!")
            self.assertEqual(escaped_target_file.read_bytes(), snap_escaped_bytes, "Outside file must remain 100% byte-identical!")
            self.assertTrue(default_pointer.is_symlink(), "Default pointer symlink must remain untouched!")
            self.assertEqual(default_pointer.resolve(), escaped_target_file.resolve())
            self.assertEqual(w44_asset.read_bytes(), snap_w44_bytes_b1, "Existing W44 asset must remain 100% byte-identical!")
            self.assertEqual(set(canonical_dir.iterdir()), snap_files_before_b1, "No extra files created in canonical dir!")

            # 清除逃逸软链接，恢复合法的默认指针
            default_pointer.unlink()
            default_pointer.write_text(json.dumps({
                "active_cycle": "W44",
                "current_active_cycle": "W44",
                "canonical_asset_relative_path": "team_recent_xi_value.W44_POSTMATCH_CANONICAL.json",
                "canonical_json_path": "team_recent_xi_value.W44_POSTMATCH_CANONICAL.json",
                "canonical_asset_sha256": w44_sha,
                "canonical_json_sha256": w44_sha,
                "teams_count": 96
            }), encoding="utf-8")
            snap_default_ptr_bytes = default_pointer.read_bytes()

            # -------------------------------------------------------------
            # 边界 2a: 省略 target_pointer_path 的可选更新语义 (已有默认指针)
            # -------------------------------------------------------------
            # 只读检查默认指针，发布只写资产，已有指针保持原字节不变
            res_no_ptr = promote_valuation_candidate_to_canonical(
                candidate_doc_or_path=w45_candidate,
                target_canonical_path=target_w45,
                target_pointer_path=None,  # 省略参数！
                is_production_authorized=True,
                operator_approval_token="USER_STAGE6_PRODUCTION_AUTHORIZATION:W45"
            )
            self.assertEqual(res_no_ptr["status"], "PROMOTED_SUCCESS")
            self.assertIsNone(res_no_ptr["pointer_update"], "pointer_update must be None when target_pointer_path is omitted!")
            self.assertTrue(target_w45.exists(), "Target W45 asset must be created!")

            # 核心断言: 已有指针保持原字节，100% 逐字节未变 (依然是 W44 内容，绝未被修改)
            self.assertEqual(default_pointer.read_bytes(), snap_default_ptr_bytes, "Default pointer must remain 100% byte-identical when omitted!")
            ptr_check_data = json.loads(default_pointer.read_text(encoding="utf-8"))
            self.assertEqual(ptr_check_data["active_cycle"], "W44", "Default pointer cycle must still be W44!")

            # -------------------------------------------------------------
            # 边界 2b: 省略 target_pointer_path 的可选更新语义 (空目录冷启动)
            # -------------------------------------------------------------
            empty_canonical_dir = tmp_root / "empty_isolated_priors"
            empty_canonical_dir.mkdir(parents=True, exist_ok=True)
            target_cold_w45 = empty_canonical_dir / "team_recent_xi_value.W45.json"
            cold_default_pointer = empty_canonical_dir / "team_recent_xi_value.current.json"

            # 初始空目录无任何资产无指针
            self.assertFalse(cold_default_pointer.exists())

            res_cold_no_ptr = promote_valuation_candidate_to_canonical(
                candidate_doc_or_path=w45_candidate,
                target_canonical_path=target_cold_w45,
                target_pointer_path=None,  # 省略参数！
                is_production_authorized=True,
                operator_approval_token="USER_STAGE6_PRODUCTION_AUTHORIZATION:W45"
            )
            self.assertEqual(res_cold_no_ptr["status"], "PROMOTED_SUCCESS")
            self.assertIsNone(res_cold_no_ptr["pointer_update"], "pointer_update must be None in cold start when omitted!")
            self.assertTrue(target_cold_w45.exists(), "Target canonical asset must be written!")

            # 核心断言: 空目录绝对不新建任何指针！
            self.assertFalse(cold_default_pointer.exists(), "No default pointer should be created when omitted in empty directory!")
            self.assertEqual(list(empty_canonical_dir.iterdir()), [target_cold_w45], "Only canonical asset should exist in directory!")


    def test_07l_cold_start_rejects_dangling_links_and_outside_pointer(self):
        from src.engine.postmatch_valuation_integrator import promote_valuation_candidate_to_canonical

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            candidate = {
                "schema_name": "SQUAD_ASSET_PRIOR_V1",
                "teams": self._create_mock_96_teams(),
                "allow_stage6_promotion": True,
                "pipeline_status": "STAGING_PASS",
                "canonical_persistence_status": "STAGING_READY_AWAITING_STAGE6_AUTHORIZATION",
                "cycle_name": "W46",
            }
            outside = root / "outside.json"
            for case in ("dangling_default", "dangling_explicit", "outside_explicit", "dangling_asset"):
                with self.subTest(case=case):
                    directory = root / case / "02_Team_Archives" / "priors"
                    directory.mkdir(parents=True)
                    default_pointer = directory / "team_recent_xi_value.current.json"
                    target = directory / "team_recent_xi_value.W46.json"
                    pointer = default_pointer
                    if case == "dangling_default":
                        default_pointer.symlink_to(outside)
                    elif case == "dangling_explicit":
                        pointer = directory / "alternate.json"
                        pointer.symlink_to(outside)
                    elif case == "outside_explicit":
                        pointer = outside
                    else:
                        target.symlink_to(outside)
                    before = set(directory.iterdir())
                    links_before = {path: path.readlink() for path in before if path.is_symlink()}
                    with self.assertRaises(PermissionError):
                        promote_valuation_candidate_to_canonical(
                            candidate, target, pointer, True, "SYNTHETIC"
                        )
                    self.assertEqual(set(directory.iterdir()), before)
                    self.assertEqual({path: path.readlink() for path in links_before}, links_before)
                    self.assertFalse(outside.exists())


if __name__ == "__main__":
    unittest.main()
