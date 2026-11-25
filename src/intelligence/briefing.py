import os
from typing import Dict, Any, List

def format_intelligence_briefing(
    run_id: str,
    run_utc: str,
    scope: str,
    processed_articles: List[Dict[str, Any]],
    index_audit: List[Dict[str, Any]],
    is_dry_run: bool = False
) -> str:
    """
    Generates a structured Candidate Intelligence Briefing markdown.
    - Includes standard Obsidian frontmatter with status: PENDING_REVIEW
    - Distinguishes candidate contents from confirmed facts
    - Eliminates static hardcoded assertions (no hardcoded Guardiola/dates)
    - Counts and filters duplicates (duplicates are counted, NOT re-emitted)
    - Separates football candidates, club culture/history, commercial excluded, technical gaps
    """
    baseline_candidates = []
    new_candidates = []
    mutated_candidates = []
    duplicate_count = 0
    culture_history = []
    commercial_excluded = []
    technical_gaps = []

    for art in processed_articles:
        dedup_status = art.get("dedup_status")
        cat = art.get("category")
        status = art.get("status")

        if dedup_status == "STATUS_DUPLICATE":
            duplicate_count += 1
            continue

        if status in ["CLIENT_RENDERED_BODY_EMPTY", "CLIENT_RENDERED_SPA_EMPTY_BODY", "FETCH_FAILED_OR_EMPTY", "NO_PARAGRAPHS_EXTRACTED"]:
            technical_gaps.append(art)
        elif cat == "COMMERCIAL_EXCLUDED":
            commercial_excluded.append(art)
        elif cat == "CLUB_CULTURE_HISTORY":
            culture_history.append(art)
        elif dedup_status == "STATUS_BASELINE_NEW":
            baseline_candidates.append(art)
        elif dedup_status == "STATUS_MUTATED":
            mutated_candidates.append(art)
        elif dedup_status == "STATUS_NEW":
            new_candidates.append(art)
        else:
            if cat == "FOOTBALL_INTELLIGENCE":
                new_candidates.append(art)
            else:
                technical_gaps.append(art)

    total_active_candidates = len(baseline_candidates) + len(new_candidates) + len(mutated_candidates)

    lines = []
    # Frontmatter
    lines.append("---")
    lines.append("status: PENDING_REVIEW")
    lines.append(f"run_id: \"{run_id}\"")
    lines.append(f"run_utc: \"{run_utc}\"")
    lines.append(f"scope: \"{scope}\"")
    lines.append("doc_type: candidate_intelligence_briefing")
    lines.append("classification: UNCONFIRMED_RAW_CANDIDATE")
    lines.append("generated_by: Ares Continuous Intelligence System (Manual MVP R2)")
    lines.append("---")
    lines.append("")

    # Header
    lines.append(f"# Ares 持续球队情报候选简报 (Candidate Intelligence Briefing)")
    lines.append("")
    lines.append(f"> [!IMPORTANT]")
    lines.append(f"> **审定状态**: `PENDING_REVIEW (待独立审核席审定)`")
    lines.append(f"> 本简报所有条目仅表示正文内容成功提取可读取，**不等于已确证的足球事实**。严禁未经独立审核席复核直接作为出单或生产知识库输入。")
    lines.append("")
    lines.append(f"- **运行标识 (Run ID)**: `{run_id}`")
    lines.append(f"- **运行模式**: `{'DRY_RUN (演练未落盘)' if is_dry_run else 'PRODUCTION_HARVEST (生产采集中枢)'}`")
    lines.append(f"- **抓取执行 UTC**: `{run_utc}` | **覆盖范围**: `{scope}`")
    lines.append(f"")
    lines.append(f"### 采集与去重指标面板")
    lines.append(f"| 指标项 | 统计数量 | 说明 |")
    lines.append(f"| :--- | :---: | :--- |")
    lines.append(f"| 扫描入口数 | `{len(index_audit)}` | 球队官方新闻目录覆盖点 |")
    lines.append(f"| 检查文章总数 | `{len(processed_articles)}` | 抓取并解析的总文章数 |")
    lines.append(f"| **首次建立基线 (Baseline)** | `{len(baseline_candidates)}` | 首次运行初始登记的足球情报候选 |")
    lines.append(f"| **增量新发现 (New)** | `{len(new_candidates)}` | 后续运行新发现的足球情报候选 |")
    lines.append(f"| **正文内容变更 (Mutated)** | `{len(mutated_candidates)}` | 正文发生修改，已关联旧版指纹 |")
    lines.append(f"| **相同重复忽略 (Duplicate)** | `{duplicate_count}` | 语义指纹完全一致，已计数并略过 |")
    lines.append(f"| **文化与名宿特稿 (Culture)** | `{len(culture_history)}` | 历史回顾/纪念专栏，不作为战术输入 |")
    lines.append(f"| **商业排除 (Commercial Excluded)** | `{len(commercial_excluded)}` | 涉及商品联名、票务、零售发售 |")
    lines.append(f"| **技术屏障/正文空 (Technical Gap)** | `{len(technical_gaps)}` | CSR 渲染/网络受阻/解析空 |")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Section 1: 当前足球候选条目
    lines.append("## 一、待审足球情报候选条目 (Candidate Football Intelligence)")
    lines.append("")
    if total_active_candidates == 0:
        lines.append("*本次运行未产生新的未见足球情报候选条目（重复条目已自动略过）。*")
        lines.append("")
    else:
        candidate_items = baseline_candidates + new_candidates + mutated_candidates
        for idx, art in enumerate(candidate_items, 1):
            team_id = art.get("team_id", "UNKNOWN")
            title = art.get("title", "No Title")
            url = art.get("url", "")
            pub_date = art.get("published_at", "UNKNOWN")
            dedup_status = art.get("dedup_status", "UNKNOWN")
            ko = art.get("match_kickoff")
            pc = art.get("press_conference")
            excerpt = art.get("excerpt", "")
            raw_sha = art.get("raw_sha256", "UNKNOWN")
            sem_fp = art.get("semantic_fingerprint", "UNKNOWN")
            version = art.get("version", 1)
            supersedes = art.get("supersedes")

            lines.append(f"### {idx}. [{team_id}] {title}")
            lines.append(f"- **原始 URL**: [{url}]({url})")
            lines.append(f"- **发布时间**: `{pub_date}` | **去重状态**: `{dedup_status}` (v{version})")
            lines.append(f"- **语义指纹 (Layer 1)**: `{sem_fp[:16]}...` | **原始字节 SHA**: `{raw_sha[:16]}...`")
            if supersedes:
                lines.append(f"- **关联旧版 (Supersedes)**: 版本 `v{supersedes.get('version')}` (旧指纹: `{str(supersedes.get('semantic_fingerprint'))[:16]}...`)")
            if ko:
                lines.append(f"- **正文开球提及**: `{ko}`")
            if pc:
                lines.append(f"- **官方发布会安排提及**: **`{pc}`**")
            lines.append(f"- **主正文提取摘要**: {excerpt}")
            lines.append("")

    # Section 2: 俱乐部文化与历史
    lines.append("---")
    lines.append("")
    lines.append("## 二、俱乐部文化与历史特稿 (Club Culture & Heritage - Non-Tactical)")
    if not culture_history:
        lines.append("*无文化/历史特稿。*")
        lines.append("")
    else:
        for idx, art in enumerate(culture_history, 1):
            team_id = art.get("team_id")
            title = art.get("title")
            url = art.get("url")
            pub_date = art.get("published_at")
            lines.append(f"- [{team_id}] **{title}** (发布: `{pub_date}`) -> *文化/名宿特稿，隔离出战术底座* [来源链接]({url})")
        lines.append("")

    # Section 3: 商业与零售排除清单
    lines.append("---")
    lines.append("")
    lines.append("## 三、商业发售与零售排除清单 (Commercial Excluded)")
    if not commercial_excluded:
        lines.append("*无商业排除条目。*")
        lines.append("")
    else:
        for idx, art in enumerate(commercial_excluded, 1):
            team_id = art.get("team_id")
            title = art.get("title")
            url = art.get("url")
            pub_date = art.get("published_at")
            lines.append(f"- [{team_id}] **{title}** (发布: `{pub_date}`) -> *零售/赞助商排除* [来源链接]({url})")
        lines.append("")

    # Section 4: 技术审计与边界
    lines.append("---")
    lines.append("")
    lines.append("## 四、采集技术审计与边界记录 (Technical Audit)")
    lines.append("")
    lines.append("| 球队代码 | 采集入口 | HTTP 状态 | 解析状态 | 发现文章数 | 检查数 | 边界说明 |")
    lines.append("| :--- | :--- | :---: | :--- | :---: | :---: | :--- |")
    for ia in index_audit:
        team_id = ia.get("team_id")
        entry_url = ia.get("url")
        status_code = ia.get("http_status")
        p_status = ia.get("parser_status")
        discovered_count = ia.get("discovered_count", 0)
        checked_count = ia.get("checked_count", 0)
        note = ia.get("note", "")
        lines.append(f"| **{team_id}** | `{entry_url}` | `{status_code}` | `{p_status}` | `{discovered_count}` | `{checked_count}` | {note} |")
    lines.append("")

    if technical_gaps:
        lines.append("### 技术屏障与空正文详情")
        for tg in technical_gaps:
            team_id = tg.get("team_id")
            url = tg.get("url")
            status = tg.get("status")
            excerpt = tg.get("excerpt")
            lines.append(f"- **[{team_id}]** `{url}`: 状态 `{status}` - {excerpt}")
        lines.append("")

    return "\n".join(lines)
