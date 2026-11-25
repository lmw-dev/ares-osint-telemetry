import os
import sys
import tempfile
import shutil
import unittest
import subprocess
import json
from unittest.mock import patch

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.intelligence.dedup import FingerprintRegistry, compute_semantic_fingerprint
from src.intelligence.extractor import extract_article_content, discover_articles_from_index
from src.intelligence.vault_exporter import export_briefing_to_vault
from src.intelligence.briefing import format_intelligence_briefing
from src.intelligence.fetcher import fetch_url

class TestTeamIntelligenceBehaviorR2(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="ares_test_intelligence_r2_")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_01_international_news_not_misclassified_by_kit_mentions(self):
        """1. 修正内容分类：国家队出场/绝杀报道不因赞助词误排为商业"""
        # 模拟包含赞助商与 kit 词汇的真实曼城国家队战报
        sample_haaland_html = '''
        <html>
        <head><title>International round-up: Haaland hits injury time winner!</title></head>
        <body>
            <nav><a href="/store">Official Kit Store</a></nav>
            <main>
                <article>
                    <time>Tue 17 Nov 2026, 22:00</time>
                    <p>Erling Haaland scored a dramatic 93rd-minute winner for Norway in their international fixture tonight.</p>
                    <p>The Manchester City striker played the full 90 minutes and suffered no injury concerns ahead of the weekend clash.</p>
                    <p>City will return to Premier League action this Saturday afternoon at the Etihad Stadium.</p>
                </article>
            </main>
            <aside class="recommendation">
                <p>Buy the new adidas home kit collection at the official store with special retail discount.</p>
            </aside>
        </body>
        </html>
        '''
        ext = extract_article_content("MCI", "https://www.mancity.com/news/mens/haaland-winner", sample_haaland_html)
        self.assertEqual(ext["status"], "SUCCESS_EXTRACTED")
        # 必须判定为 FOOTBALL_INTELLIGENCE，绝不能被误判为 COMMERCIAL_EXCLUDED
        self.assertEqual(ext["category"], "FOOTBALL_INTELLIGENCE")
        self.assertEqual(len(ext["paragraphs"]), 3)
        self.assertIn("Erling Haaland scored", ext["paragraphs"][0])

        # 对照组：纯商业服饰发售文章必须被归入 COMMERCIAL_EXCLUDED
        sample_pure_commercial = '''
        <html>
        <head><title>New Tommy Hilfiger Lifestyle Fashion Collection Launch</title></head>
        <body>
            <main>
                <article>
                    <p>Check out our brand new Tommy Hilfiger lifestyle apparel collection now available on the store.</p>
                    <p>Browse the exclusive fashion clothing retail range online with great discount offers.</p>
                </article>
            </main>
        </body>
        </html>
        '''
        ext_comm = extract_article_content("LIV", "https://www.liverpoolfc.com/news/tommy-collection", sample_pure_commercial)
        self.assertEqual(ext_comm["category"], "COMMERCIAL_EXCLUDED")

    def test_02_recommendation_change_does_not_mutate_main_body_fingerprint(self):
        """2. 仅修改推荐栏/aside内容，主正文语义指纹保持绝对不变"""
        html_run1 = '''
        <html>
        <head><title>Kevin De Bruyne Training Return</title></head>
        <body>
            <main>
                <article>
                    <p>Kevin De Bruyne completed a full training session with the first team squad this morning.</p>
                    <p>The midfielder is fit to start against Fulham on Saturday after recovering from his hamstring issue.</p>
                </article>
            </main>
            <aside class="sidebar recommendation">
                <p>Recommended Article Number One: Under-18 Match Report and Highlights.</p>
            </aside>
        </body>
        </html>
        '''

        # 侧边栏推荐文章由 Number One 变为 Number Two，主正文文字完全相同
        html_run2 = '''
        <html>
        <head><title>Kevin De Bruyne Training Return</title></head>
        <body>
            <main>
                <article>
                    <p>Kevin De Bruyne completed a full training session with the first team squad this morning.</p>
                    <p>The midfielder is fit to start against Fulham on Saturday after recovering from his hamstring issue.</p>
                </article>
            </main>
            <aside class="sidebar recommendation">
                <p>Recommended Article Number Two: Women Team Secure Vital Continental Victory.</p>
            </aside>
        </body>
        </html>
        '''

        ext1 = extract_article_content("MCI", "https://www.mancity.com/news/mens/kdb", html_run1)
        ext2 = extract_article_content("MCI", "https://www.mancity.com/news/mens/kdb", html_run2)

        fp1 = compute_semantic_fingerprint(ext1["title"], ext1["paragraphs"])
        fp2 = compute_semantic_fingerprint(ext2["title"], ext2["paragraphs"])

        # 断言：语义指纹必须完全一致，侧边栏噪音必须被提取器完全过滤
        self.assertEqual(fp1, fp2)

    def test_03_dedup_two_runs_baseline_duplicate_and_mutation(self):
        """3. 端到端两轮运行：第一轮统一基线，第二轮重复仅计数不重发，正文修改保留版本关联"""
        reg_file = os.path.join(self.test_dir, "test_lifecycle_reg.json")
        reg = FingerprintRegistry(reg_file)

        url_a = "https://www.mancity.com/news/mens/art_a"
        url_b = "https://www.mancity.com/news/mens/art_b"

        # --- 第一次运行 (注册表初始为空) ---
        res_a1 = reg.evaluate_and_register(url_a, "raw_sha_a", "Title A", ["Para A1", "Para A2"], "2026-11-18T10:00:00Z")
        res_b1 = reg.evaluate_and_register(url_b, "raw_sha_b", "Title B", ["Para B1", "Para B2"], "2026-11-18T10:00:00Z")

        # 批次首次运行，必须统一定义为 STATUS_BASELINE_NEW
        self.assertEqual(res_a1["status"], "STATUS_BASELINE_NEW")
        self.assertEqual(res_b1["status"], "STATUS_BASELINE_NEW")

        # --- 第二次运行 (注册表已存在记录) ---
        reg_run2 = FingerprintRegistry(reg_file)
        self.assertFalse(reg_run2.is_initial_empty)

        # A 保持不变 -> STATUS_DUPLICATE
        res_a2 = reg_run2.evaluate_and_register(url_a, "raw_sha_a_new_ads", "Title A", ["Para A1", "Para A2"], "2026-11-18T11:00:00Z")
        self.assertEqual(res_a2["status"], "STATUS_DUPLICATE")

        # B 修改正文内容 (Para B2 变动) -> STATUS_MUTATED
        res_b2 = reg_run2.evaluate_and_register(url_b, "raw_sha_b2", "Title B", ["Para B1", "Para B2 Modified by club"], "2026-11-18T11:00:00Z")
        self.assertEqual(res_b2["status"], "STATUS_MUTATED")
        self.assertEqual(res_b2["version"], 2)
        self.assertEqual(res_b2["supersedes"]["version"], 1)

        # 验证简报组装器行为：DUPLICATE 仅计数，不重新进入正文候选卡片
        processed = [
            {"team_id": "MCI", "url": url_a, "title": "Title A", "dedup_status": "STATUS_DUPLICATE", "category": "FOOTBALL_INTELLIGENCE", "status": "SUCCESS_EXTRACTED"},
            {"team_id": "MCI", "url": url_b, "title": "Title B", "dedup_status": "STATUS_MUTATED", "version": 2, "supersedes": res_b2["supersedes"], "category": "FOOTBALL_INTELLIGENCE", "status": "SUCCESS_EXTRACTED", "paragraphs": ["Para B1", "Para B2 Modified by club"], "excerpt": "Para B1", "raw_sha256": "raw_sha_b2", "semantic_fingerprint": res_b2["semantic_fingerprint"]}
        ]
        briefing = format_intelligence_briefing("run_test_02", "2026-11-18T11:00:00Z", "pilot_four", processed, [])
        self.assertIn("| **相同重复忽略 (Duplicate)** | `1` |", briefing)
        self.assertIn("status: PENDING_REVIEW", briefing)
        # 确认 Title A 没有出现在正文候选详细卡片中，而 Title B 包含关联旧版
        self.assertNotIn("### 1. [MCI] Title A", briefing)
        self.assertIn("### 1. [MCI] Title B", briefing)
        self.assertIn("关联旧版 (Supersedes)", briefing)

    def test_04_chelsea_dynamic_discovery_not_hardcoded_empty(self):
        """4. 删除硬编码判断：切尔西发现逻辑根据实际传入页面结果运行，有链接时实际发现"""
        chelsea_html_with_links = '''
        <html>
        <body>
            <div id="root">
                <a href="/en/news/article/enzo-maresca-fulham-press-conference-preview">Maresca Press Conference</a>
                <a href="/en/news/article/cole-palmer-fitness-update-latest">Cole Palmer Fitness Update</a>
            </div>
        </body>
        </html>
        '''
        discovered, status = discover_articles_from_index("CHE", chelsea_html_with_links, "https://www.chelseafc.com")
        self.assertEqual(status, "INDEX_PARSED_SUCCESS")
        self.assertEqual(len(discovered), 2)
        self.assertIn("https://www.chelseafc.com/en/news/article/enzo-maresca-fulham-press-conference-preview", discovered)

    def test_05_network_and_csr_failures_never_fake_zero_news(self):
        """5. 网络失败与解析失败不冒充无新增"""
        # 5.1 CSR 页面空正文检测
        csr_html = '<html><head><title>Forest News</title></head><body><div id="app"></div></body></html>' + ('x' * 50000)
        ext_csr = extract_article_content("NFO", "https://www.nottinghamforest.co.uk/news/art1", csr_html)
        self.assertEqual(ext_csr["status"], "CLIENT_RENDERED_BODY_EMPTY")
        self.assertEqual(ext_csr["category"], "TECHNICAL_GAP")

        # 5.2 网络失败检测 (通过模拟 HTTP 500)
        with patch('urllib.request.urlopen') as mock_open:
            import urllib.error
            mock_open.side_effect = urllib.error.HTTPError("https://example.com", 500, "Internal Server Error", {}, None)
            res = fetch_url("https://example.com")
            self.assertEqual(res["http_status"], 500)
            self.assertIsNotNone(res["error"])
            self.assertEqual(res["bytes_len"], 0)

    def test_06_dry_run_immutability(self):
        """6. dry-run 演练模式绝不污染注册表与 Vault"""
        mock_vault = os.path.join(self.test_dir, "vault_pilot")
        reg_file = os.path.join(self.test_dir, "dry_reg.json")

        # 运行 CLI dry-run
        cmd = [
            sys.executable,
            "scripts/run_team_intelligence_daily.py",
            "--dry-run",
            "--registry-file", reg_file,
            "--vault-dir", mock_vault,
            "--team", "MCI"
        ]
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.assertEqual(proc.returncode, 0)
        # 断言：注册表与 Vault 目录在磁盘上绝对未生成
        self.assertFalse(os.path.exists(reg_file))
        self.assertFalse(os.path.exists(mock_vault))

    def test_07_security_boundaries_and_corruption_handling(self):
        """7. 目标越界、已有目标覆盖、损坏注册表明确失败拦截"""
        allowed_vault = os.path.join(self.test_dir, "allowed_vault_root")
        os.makedirs(allowed_vault, exist_ok=True)

        # 7.1 路径穿越/越界拦截
        outside_vault = os.path.join(self.test_dir, "outside_forbidden_dir")
        with self.assertRaises(PermissionError):
            export_briefing_to_vault("# Briefing", "run_test", target_dir=outside_vault, allowed_root=allowed_vault)

        # 7.2 严禁写入 02_Team_Archives
        with self.assertRaises(PermissionError):
            export_briefing_to_vault("# Briefing", "run_test", target_dir="/Users/liumingwei/vaults/AresVault/02_Team_Archives")

        # 7.3 已有目标同名覆盖拦截
        test_file = os.path.join(allowed_vault, "existing_file.md")
        with open(test_file, "w") as f:
            f.write("existing content")
        with self.assertRaises(FileExistsError):
            export_briefing_to_vault("# Briefing", "run_test", target_dir=allowed_vault, allowed_root=allowed_vault, custom_filename="existing_file.md")

        # 7.4 注册表损坏时报错并自动备份，绝不静默抹除
        bad_reg_file = os.path.join(self.test_dir, "bad_registry.json")
        with open(bad_reg_file, "w") as f:
            f.write("INVALID JSON CONTENT DATA")

        with self.assertRaises(ValueError):
            FingerprintRegistry(bad_reg_file)

        # 检查是否自动生成了 .corrupted 备份文件
        corrupted_backups = [f for f in os.listdir(self.test_dir) if "bad_registry.json.corrupted" in f]
        self.assertTrue(len(corrupted_backups) > 0)

        # 7.5 CLI 拒绝无提示覆盖历史 run
        existing_run_dir = os.path.join(self.test_dir, "history_run_01")
        os.makedirs(existing_run_dir, exist_ok=True)
        cmd = [
            sys.executable,
            "scripts/run_team_intelligence_daily.py",
            "--run-id", "history_run_01",
            "--output-dir", existing_run_dir,
            "--team", "MCI"
        ]
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("RUN_ID_ALREADY_EXISTS", proc.stderr)

if __name__ == "__main__":
    unittest.main()
