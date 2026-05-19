"""
skill_runner.py
================
将 src/skills 中的 skill 定义与 ares-osint-telemetry 流水线集成。

用法示例：
    from src.skills.skill_runner import SkillRunner

    runner = SkillRunner("football-team-news-flags")
    context = runner.build_context(
        league="英超",
        date="2026-05-19",
        round="第38轮",
        matches=["阿森纳 vs 切尔西", "曼城 vs 利物浦"],
    )
    print(runner.get_system_prompt())
    print(runner.get_user_prompt(context))
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from datetime import datetime

from src.skills import (
    get_skill_path,
    list_skills,
    load_skill_definition,
    load_skill_prompts,
    load_skill_reference,
    load_skill_template,
)

# ────────────────────────────────────────────────────────────────
# 常量
# ────────────────────────────────────────────────────────────────

SKILL_TEAM_NEWS = "football-team-news-flags"
SKILL_ODDS = "football-prematch-odds-intelligence"


# ────────────────────────────────────────────────────────────────
# SkillRunner
# ────────────────────────────────────────────────────────────────

class SkillRunner:
    """
    Skill 执行器。
    负责加载指定 skill 的定义、提示词和模板，
    并提供与 ares-osint-telemetry 输入/输出路径约定集成的辅助方法。
    """

    def __init__(self, skill_name: str) -> None:
        if skill_name not in list_skills():
            raise ValueError(
                f"未知 skill: '{skill_name}'。已注册: {list_skills()}"
            )
        self.skill_name = skill_name
        self.skill_path = get_skill_path(skill_name)
        self._definition: str | None = None
        self._prompts: dict[str, str] | None = None

    # ─── 加载接口 ────────────────────────────────────────────────

    @property
    def definition(self) -> str:
        """SKILL.md 全文（懒加载）。"""
        if self._definition is None:
            self._definition = load_skill_definition(self.skill_name)
        return self._definition

    @property
    def prompts(self) -> dict[str, str]:
        """prompts/ 目录所有 md 文件（懒加载）。"""
        if self._prompts is None:
            self._prompts = load_skill_prompts(self.skill_name)
        return self._prompts

    def get_system_prompt(self) -> str:
        """提取 agent_prompts.md 中的 System Prompt 代码块内容。"""
        raw = self.prompts.get("agent_prompts", "")
        match = re.search(
            r"## System Prompt\s+```text\s+(.*?)```",
            raw,
            re.DOTALL,
        )
        return match.group(1).strip() if match else self.definition

    def get_user_prompt_template(self) -> str:
        """提取 agent_prompts.md 中 User Prompt for a Full League Round 代码块。"""
        raw = self.prompts.get("agent_prompts", "")
        match = re.search(
            r"## User Prompt for a Full League Round\s+```text\s+(.*?)```",
            raw,
            re.DOTALL,
        )
        return match.group(1).strip() if match else ""

    def get_single_team_worker_prompt(self) -> str:
        """提取单队 worker prompt 代码块。"""
        raw = self.prompts.get("agent_prompts", "")
        match = re.search(
            r"## Single-Team Worker Prompt\s+```text\s+(.*?)```",
            raw,
            re.DOTALL,
        )
        return match.group(1).strip() if match else ""

    def get_reviewer_prompt(self) -> str:
        """提取审校 reviewer prompt 代码块。"""
        raw = self.prompts.get("agent_prompts", "")
        match = re.search(
            r"## Reviewer Prompt\s+```text\s+(.*?)```",
            raw,
            re.DOTALL,
        )
        return match.group(1).strip() if match else ""

    def get_reference(self, name: str) -> str:
        """加载指定参考文档。name 含扩展名。"""
        return load_skill_reference(self.skill_name, name)

    def get_template(self, name: str) -> str:
        """加载指定模板文件。name 含扩展名。"""
        return load_skill_template(self.skill_name, name)

    # ─── 输出路径约定 ─────────────────────────────────────────────

    @staticmethod
    def _project_root() -> Path:
        """返回项目根目录（src/skills 向上两级）。"""
        return Path(__file__).parent.parent.parent

    def raw_output_path(self, league: str, date: str, ext: str = "jsonl") -> Path:
        """
        返回 raw_reports/ 中间产物路径。
        格式：raw_reports/{skill_name}_{league}_{date}.{ext}
        """
        root = self._project_root()
        safe_league = league.replace(" ", "_")
        filename = f"{self.skill_name}_{safe_league}_{date}.{ext}"
        return root / "raw_reports" / filename

    def draft_output_path(self, league: str, date: str, ext: str = "md") -> Path:
        """
        返回 draft_reports/ 最终产物路径。
        格式：draft_reports/{skill_name}_{league}_{date}.{ext}
        """
        root = self._project_root()
        safe_league = league.replace(" ", "_")
        filename = f"{self.skill_name}_{safe_league}_{date}.{ext}"
        return root / "draft_reports" / filename

    # ─── 上下文构建 ───────────────────────────────────────────────

    def build_context(self, **kwargs) -> dict:
        """
        构建传给 user prompt 模板的上下文字典。
        支持任意 key=value 参数，额外注入 generated_at。
        """
        ctx = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "skill_name": self.skill_name,
        }
        ctx.update(kwargs)
        # 若 matches 是列表，转换为带序号的字符串
        if isinstance(ctx.get("matches"), list):
            ctx["match_list"] = "\n".join(
                f"{i + 1}. {m}" for i, m in enumerate(ctx["matches"])
            )
        return ctx

    def render_user_prompt(self, context: dict) -> str:
        """
        将上下文填充到 user prompt 模板中。
        支持 {variable} 占位符替换，未匹配占位符保留原样。
        """
        template = self.get_user_prompt_template()
        if not template:
            return json.dumps(context, ensure_ascii=False, indent=2)

        def replace_placeholder(match):
            key = match.group(1)
            return str(context.get(key, match.group(0)))

        return re.sub(r"\{(\w+)\}", replace_placeholder, template)

    def __repr__(self) -> str:
        return f"<SkillRunner skill='{self.skill_name}'>"


# ────────────────────────────────────────────────────────────────
# 便捷工厂函数
# ────────────────────────────────────────────────────────────────

def team_news_runner() -> SkillRunner:
    """返回 football-team-news-flags SkillRunner 实例。"""
    return SkillRunner(SKILL_TEAM_NEWS)


def odds_runner() -> SkillRunner:
    """返回 football-prematch-odds-intelligence SkillRunner 实例。"""
    return SkillRunner(SKILL_ODDS)


# ────────────────────────────────────────────────────────────────
# 调试入口
# ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== 测试 football-team-news-flags ===")
    runner = team_news_runner()
    ctx = runner.build_context(
        league="英超",
        season="2025/26",
        round="第38轮",
        date="2026-05-19",
        matches=["阿森纳 vs 切尔西", "曼城 vs 利物浦"],
    )
    print(runner.get_system_prompt()[:300])
    print("...")
    print(f"原始输出路径: {runner.raw_output_path('英超', '2026-05-19')}")
    print(f"报告输出路径: {runner.draft_output_path('英超', '2026-05-19')}")

    print("\n=== 测试 football-prematch-odds-intelligence ===")
    runner2 = odds_runner()
    print(f"skill 路径: {runner2.skill_path}")
    print(f"已加载 references: {list((runner2.skill_path / 'references').iterdir())}")
