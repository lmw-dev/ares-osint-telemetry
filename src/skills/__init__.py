"""
ares-osint-telemetry :: src/skills
===================================
Skill 注册目录。

每个子目录对应一个独立 skill，包含：
  SKILL.md        — skill 主定义（frontmatter + 说明 + 执行规范）
  references/     — 参考文档（触发规则、平台适配、数据源策略等）
  templates/      — 输出模板（Markdown 报告模板、JSON Schema）
  prompts/        — 可复用提示词（仅部分 skill 有）
  scripts/        — 辅助脚本（规范化、校验等）

已注册的 Skill
--------------
football-team-news-flags
    赛前球队关键异常信息收集工作流。
    触发词：球队异常、新闻标志、team news flags、球队新闻扫描

football-prematch-odds-intelligence
    赛前公司级赔率市场情报工作流（欧赔/亚盘/大小球/盘口时间逻辑）。
    触发词：赔率报告、欧赔分析、亚盘移动、prematch odds、盘口时间逻辑
"""

import os
from pathlib import Path

SKILLS_DIR = Path(__file__).parent


def list_skills() -> list[str]:
    """返回所有已注册 skill 的名称列表。"""
    return [
        d.name
        for d in SKILLS_DIR.iterdir()
        if d.is_dir() and (d / "SKILL.md").exists()
    ]


def get_skill_path(skill_name: str) -> Path:
    """返回指定 skill 的根目录路径。"""
    path = SKILLS_DIR / skill_name
    if not path.exists():
        raise FileNotFoundError(f"Skill '{skill_name}' not found in {SKILLS_DIR}")
    return path


def load_skill_definition(skill_name: str) -> str:
    """读取并返回指定 skill 的 SKILL.md 全文。"""
    skill_path = get_skill_path(skill_name)
    return (skill_path / "SKILL.md").read_text(encoding="utf-8")


def load_skill_prompts(skill_name: str) -> dict[str, str]:
    """
    加载 skill 的 prompts/ 目录中所有 .md 文件。
    返回 {filename_stem: content} 字典。
    """
    prompts_dir = get_skill_path(skill_name) / "prompts"
    if not prompts_dir.exists():
        return {}
    return {
        f.stem: f.read_text(encoding="utf-8")
        for f in prompts_dir.glob("*.md")
    }


def load_skill_template(skill_name: str, template_name: str) -> str:
    """
    加载 skill 的 templates/ 目录中指定模板文件内容。
    template_name 含扩展名，如 'team_news_report_template.md'
    """
    template_path = get_skill_path(skill_name) / "templates" / template_name
    if not template_path.exists():
        raise FileNotFoundError(
            f"Template '{template_name}' not found in skill '{skill_name}'"
        )
    return template_path.read_text(encoding="utf-8")


def load_skill_reference(skill_name: str, ref_name: str) -> str:
    """
    加载 skill 的 references/ 目录中指定参考文档内容。
    ref_name 含扩展名，如 'flag_taxonomy_and_review.md'
    """
    ref_path = get_skill_path(skill_name) / "references" / ref_name
    if not ref_path.exists():
        raise FileNotFoundError(
            f"Reference '{ref_name}' not found in skill '{skill_name}'"
        )
    return ref_path.read_text(encoding="utf-8")


if __name__ == "__main__":
    print("已注册的 Skills：")
    for name in list_skills():
        print(f"  - {name}")
