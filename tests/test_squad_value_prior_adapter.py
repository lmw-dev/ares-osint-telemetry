"""
tests/test_squad_value_prior_adapter.py
======================================
LMW-413 身价先验消费适配器 (SquadValuePriorAdapter) 自包含核心契约回归测试套件。
针对 4 项核心行为进行独立闭环断言:
1. test_01_prematch_consumption_interface_and_anti_overreach:
   CAUTION 状态下 authoritative_asset_ratio 必须为 null，保留未验证诊断倍数与 5 项 Anti-Overreach 禁令;
2. test_02_ready_status_lower_bound_arbitration_guard:
   样本状态为 READY 不得覆盖实际估值质量 (Roma vs Inter 下界置空权威比值保留诊断商数，双方确证输出权威比值);
3. test_03_severely_asymmetric_coverage_suppresses_quotient:
   覆盖严重不对等或不足以比较时 (拜仁 vs 帕德博恩) 刚性置空诊断商数 (消除 51.3057x)，保留单场/部分身价背景参考;
4. test_04_temporal_guardrail_and_missing_team_degradation:
   开球时间截断门禁 (时间穿越、缺时间、格式非法) 刚性降级为 UNKNOWN，单侧缺失降级为 NOT_USED。

本测试文件纯自包含，仅依赖 SquadValuePriorAdapter 与 fixture_squad_value_priors.json，
完全解耦外部物理生产 Vault 与其他大管线模块。
"""

import sys
import unittest
from pathlib import Path

# 动态定位项目根目录与模块路径
CURRENT_FILE = Path(__file__).resolve()
REPO_ROOT = CURRENT_FILE.parent.parent
ENGINE_DIR = REPO_ROOT / "src" / "engine"

if str(ENGINE_DIR) not in sys.path:
    sys.path.insert(0, str(ENGINE_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from squad_value_prior_adapter import SquadValuePriorAdapter

# 固化自包含测试数据路径
FIXTURE_PATH = CURRENT_FILE.parent / "fixtures" / "priors" / "fixture_squad_value_priors.json"


class TestSquadValuePriorAdapterSelfContained(unittest.TestCase):
    """身价消费适配器自包含核心契约测试"""

    def setUp(self):
        self.assertTrue(FIXTURE_PATH.exists(), f"Self-contained fixture not found at {FIXTURE_PATH}")
        self.adapter = SquadValuePriorAdapter(override_canonical_json_path=FIXTURE_PATH)

    # -------------------------------------------------------------------------
    # 1. CAUTION 权威比值置空与 5 项 Anti-Overreach 禁令 (原 Check 2)
    # -------------------------------------------------------------------------
    def test_01_prematch_consumption_interface_and_anti_overreach(self):
        """[Behavior 1] CAUTION 状态下权威比值为空，保留诊断商数与 5 项 Anti-Overreach 禁令"""
        cons = self.adapter.get_value_prior_consideration(
            home_team_name="Everton",
            away_team_name="Chelsea",
            kickoff_utc="2026-10-18T14:00:00Z",
            fixture_id="3003912",
            league="EPL",
            round_no=7,
            is_retrospective_replay=True,
        )

        # 状态机仲裁断言
        self.assertEqual(cons["asset_status"], "SUPPORTING_CONTEXT")
        self.assertTrue(cons["is_comparison_allowed"])
        self.assertTrue(cons["is_structural_supporting_context_only"])
        self.assertTrue(cons.get("is_retrospective_replay", False))
        self.assertEqual(cons.get("replay_audit_tag"), "RETROSPECTIVE_REPLAY")

        # 核心契约: CAUTION 下权威比率必须刚性为 null，非权威诊断商数安全保留
        self.assertIsNone(cons["authoritative_asset_ratio"])
        self.assertEqual(cons["unverified_diagnostic_quotient"], 0.496)

        # 保留两场 (2 场) 均值金额作为背景参考
        self.assertEqual(cons["home_team_metrics"]["valid_matches"], 2)
        self.assertEqual(cons["home_team_metrics"]["complete_matches"], 2)
        self.assertEqual(cons["home_team_metrics"]["effective_value_eur"], 258_000_000.0)

        self.assertEqual(cons["away_team_metrics"]["valid_matches"], 2)
        self.assertEqual(cons["away_team_metrics"]["complete_matches"], 1)
        self.assertEqual(cons["away_team_metrics"]["effective_value_eur"], 520_200_000.0)

        # 5 项 Anti-Overreach 禁令刚性注入断言
        ao = cons["anti_overreach_contract"]
        self.assertTrue(ao["overreach_forbidden"])
        self.assertTrue(ao["cannot_derive_asian_handicap"])
        self.assertTrue(ao["cannot_derive_over_under"])
        self.assertTrue(ao["cannot_derive_1x2_odds"])
        self.assertTrue(ao["cannot_override_football_model"])
        self.assertEqual(ao["allowed_usage_scope"], "SUPPORTING_CONTEXT_MACRO_SQUAD_QUALITY_ONLY")

    # -------------------------------------------------------------------------
    # 2. READY 状态不得覆盖估值质量门禁 (原 Check 2b)
    # -------------------------------------------------------------------------
    def test_02_ready_status_lower_bound_arbitration_guard(self):
        """[Behavior 2] 样本状态为 READY 不得覆盖实际估值质量 (下界置空权威比值，确证输出权威比值)"""
        # 2.1 双方均 READY，但双方均为部分已知下界 (Roma vs Inter, 均 3 场样本但只有 2 场完整)
        c1 = self.adapter.get_value_prior_consideration(
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
        # 两队已知金额完整保留作为宏观背景参考
        self.assertAlmostEqual(c1["home_team_metrics"]["effective_value_eur"], 222_833_333.33, places=1)
        self.assertAlmostEqual(c1["away_team_metrics"]["effective_value_eur"], 353_000_000.0, places=1)

        # 2.2 双方均 READY，但覆盖不对等 (Arsenal 3/3 确证 vs Roma 2/3 下界)
        c2 = self.adapter.get_value_prior_consideration(
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
        self.assertIsNone(c2["authoritative_asset_ratio"])
        self.assertEqual(c2["unverified_diagnostic_quotient"], 2.9843)
        self.assertIn("未验证诊断倍数", c2["human_readable_summary"])

        # 2.3 双方均 READY，且双方均为 3/3 FULL_XI 确证估值 (Arsenal vs Man City)
        c3 = self.adapter.get_value_prior_consideration(
            home_team_name="Arsenal",
            away_team_name="Man City",
            kickoff_utc="2026-11-03T18:00:00Z",
            fixture_id="test_ars_mci",
            league="EPL",
            round_no=10,
        )
        self.assertEqual(c3["asset_status"], "SUPPORTING_CONTEXT")
        self.assertTrue(c3["is_comparison_allowed"])
        self.assertEqual(c3["authoritative_asset_ratio"], 0.8176)
        self.assertEqual(c3["unverified_diagnostic_quotient"], 0.8176)
        self.assertIn("0.8176x", c3["human_readable_summary"])
        self.assertNotIn("未验证诊断倍数", c3["human_readable_summary"])

    # -------------------------------------------------------------------------
    # 3. 覆盖严重不对等刚性置空诊断倍数 (原 Check 2c)
    # -------------------------------------------------------------------------
    def test_03_severely_asymmetric_coverage_suppresses_quotient(self):
        """[Behavior 3] 覆盖严重不对等 (拜仁 vs 帕德博恩) 刚性置空诊断倍数，保留单场/部分身价背景参考"""
        cons = self.adapter.get_value_prior_consideration(
            home_team_name="Bayern Munich",
            away_team_name="SC Paderborn",
            kickoff_utc="2026-11-03T18:00:00Z",
            fixture_id="test_bayern_paderborn",
            league="Bundesliga",
            round_no=9,
        )

        self.assertEqual(cons["asset_status"], "SUPPORTING_CONTEXT")
        self.assertFalse(cons["is_comparison_allowed"])
        # 权威比值与未验证诊断商数均刚性置空 (坚决不计算、不展示 51.3057x)
        self.assertIsNone(cons["authoritative_asset_ratio"])
        self.assertIsNone(cons["unverified_diagnostic_quotient"])
        self.assertIn("Severely asymmetric", cons["status_reason"])
        self.assertIn("不可比 (覆盖不对等，比率置空)", cons["human_readable_summary"])

        # 验证保留单场 (1 场) 与部分已知身价作为背景参考 (绝不置零)
        h_m = cons["home_team_metrics"]
        a_m = cons["away_team_metrics"]
        self.assertEqual(h_m["status"], "READY")
        self.assertEqual(h_m["valid_matches"], 3)
        self.assertEqual(h_m["complete_matches"], 3)
        self.assertEqual(h_m["completeness_quality"], "FULL_XI_EXACT")
        self.assertAlmostEqual(h_m["effective_value_eur"], 584_666_666.67, places=1)

        self.assertEqual(a_m["status"], "CAUTION_ACCEPTABLE")
        self.assertEqual(a_m["valid_matches"], 3)
        self.assertEqual(a_m["complete_matches"], 1)  # 仅 1 场完整
        self.assertEqual(a_m["completeness_quality"], "PARTIAL_LOWER_BOUND")
        self.assertAlmostEqual(a_m["effective_value_eur"], 11_395_737.0, places=1)  # 真实保留已知下界 €11.40M

        # 验证 Markdown 摘要真实保留两队已知金额信息
        self.assertIn("Bayern Munich [READY] (€584.7M, FULL_XI_EXACT)", cons["human_readable_summary"])
        self.assertIn("SC Paderborn [CAUTION_ACCEPTABLE] (€11.40M, PARTIAL_LOWER_BOUND)", cons["human_readable_summary"])

    # -------------------------------------------------------------------------
    # 4. 时间截断门禁与缺失侧降级 (原 Check 3)
    # -------------------------------------------------------------------------
    def test_04_temporal_guardrail_and_missing_team_degradation(self):
        """[Behavior 4] 开球时间截断门禁 (时间穿越、缺少时间、格式非法) 与缺失侧降级"""
        # 4.1 异常时间穿越：非回放模式下，比赛开球时间早于证据截止时间 (2026-10-01 < 2026-10-18)
        c_viol = self.adapter.get_value_prior_consideration(
            home_team_name="Everton",
            away_team_name="Chelsea",
            kickoff_utc="2026-10-01T12:00:00Z",
            fixture_id="3003912",
            is_retrospective_replay=False,
        )
        self.assertEqual(c_viol["asset_status"], "UNKNOWN")
        self.assertFalse(c_viol["is_comparison_allowed"])
        self.assertIsNone(c_viol["authoritative_asset_ratio"])
        self.assertIn("TEMPORAL_LOOKAHEAD_VIOLATION", c_viol["status_reason"])

        # 4.2 缺失开球时间自动降级为 UNKNOWN
        c_missing = self.adapter.get_value_prior_consideration(
            home_team_name="Everton",
            away_team_name="Chelsea",
            kickoff_utc=None,
            fixture_id="3003912",
        )
        self.assertEqual(c_missing["asset_status"], "UNKNOWN")
        self.assertFalse(c_missing["is_comparison_allowed"])
        self.assertIn("MISSING_KICKOFF_TIME_DEGRADED", c_missing["status_reason"])

        # 4.3 开球时间格式非法自动降级为 UNKNOWN
        c_invalid = self.adapter.get_value_prior_consideration(
            home_team_name="Everton",
            away_team_name="Chelsea",
            kickoff_utc="INVALID_DATE_TIME",
            fixture_id="3003912",
        )
        self.assertEqual(c_invalid["asset_status"], "UNKNOWN")
        self.assertFalse(c_invalid["is_comparison_allowed"])
        self.assertIn("INVALID_KICKOFF_TIME_FORMAT_DEGRADED", c_invalid["status_reason"])

        # 4.4 单侧队伍未收录降级为 NOT_USED
        c_unrec = self.adapter.get_value_prior_consideration(
            home_team_name="Arsenal",
            away_team_name="CompletelyUnknownTeam999",
            kickoff_utc="2026-11-03T18:00:00Z",
            fixture_id="test_unrec",
        )
        self.assertEqual(c_unrec["asset_status"], "NOT_USED")
        self.assertFalse(c_unrec["is_comparison_allowed"])
        self.assertIn("Incomplete pair coverage", c_unrec["status_reason"])


if __name__ == "__main__":
    unittest.main()
