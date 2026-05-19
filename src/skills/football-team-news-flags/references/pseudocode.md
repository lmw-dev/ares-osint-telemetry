# Pseudocode and Flowchart

本文件描述平台无关的执行算法。它可以被改写为 Python、TypeScript、shell pipeline、Notebook 或任意 Agent 工作流。

## Data model

```python
CanonicalFlag = Literal[
    "主帅下课/更衣室问题",
    "核心球员伤停",
    "多名主力轮换",
    "杯赛前后",
    "欧战资格已锁定",
    "已降级/已夺冠/已无欲无求",
    "球队近期负面舆论明显",
    "临场阵容确认后有重大变化",
]

TeamInput = {
    "team": str,
    "opponent": str,
    "home_away": "home" | "away" | "neutral",
    "fixture": str,
    "kickoff": str | None,
    "league": str,
    "round": str | None,
    "known_context": str | None,
}

TeamFlagRecord = {
    "team": str,
    "fixture": str,
    "kickoff": str | None,
    "news_status": "暂无明显异常" | "有异常",
    "news_flags": list[CanonicalFlag],
    "key_abnormalities": str,
    "sources": list[dict],
    "confidence": "high" | "medium" | "low",
    "review_notes": str,
}
```

## Main algorithm

```python
def collect_team_news_flags(scope):
    matches = normalize_match_list(scope)
    team_inputs = expand_matches_to_teams(matches)
    league_context = collect_league_context(scope)

    raw_records = []
    for team in team_inputs:  # 可替换为并行 map
        queries = build_queries(team, scope, league_context)
        documents = fetch_and_extract_sources(queries)
        candidate_facts = extract_candidate_facts(team, documents, league_context)
        raw_records.append(make_preliminary_record(team, candidate_facts))

    normalized = []
    for record in raw_records:
        record.news_flags = map_to_canonical_flags(record.candidate_flags)
        record = remove_noncanonical_and_weak_flags(record)
        record = enforce_status_rules(record)
        normalized.append(record)

    reviewed = cross_team_review(normalized, matches, league_context)
    report_md = render_markdown_report(reviewed, scope, league_context)
    report_json = render_json(reviewed, scope)
    return report_md, report_json
```

## Team worker algorithm

```python
def make_preliminary_record(team, candidate_facts):
    evidence = []
    candidate_flags = []

    for fact in candidate_facts:
        if is_current_match_relevant(fact) and has_credible_source(fact):
            flag = classify_fact_into_flag(fact)
            if flag:
                candidate_flags.append(flag)
                evidence.append(fact.source)

    if not candidate_flags:
        return {
            "team": team.name,
            "news_status": "暂无明显异常",
            "news_flags": [],
            "key_abnormalities": "未核实到足以进入指定 news_flags 的重大异常。",
            "sources": best_sources_used(candidate_facts),
            "confidence": confidence_from_source_coverage(candidate_facts),
            "review_notes": "No qualifying flag after exclusion tests.",
        }

    return {
        "team": team.name,
        "news_status": "有异常",
        "news_flags": unique_preserve_order(candidate_flags),
        "key_abnormalities": summarize_material_evidence(candidate_flags, evidence),
        "sources": evidence,
        "confidence": confidence_from_evidence(evidence),
        "review_notes": "Preliminary positive flags; requires cross-team review.",
    }
```

## Review algorithm

```python
def cross_team_review(records, matches, league_context):
    records = ensure_one_record_per_team(records)
    records = remove_flags_not_in_canonical_taxonomy(records)
    records = remove_opponent_leakage(records, matches)
    records = verify_table_status_flags(records, league_context)
    records = downgrade_unverified_rotation(records)
    records = enforce_no_obvious_abnormality_format(records)
    records = attach_reference_ids(records)
    return records
```

## Flowchart

```mermaid
flowchart TD
    A[确认联赛/轮次/日期/球队列表] --> B[构建每队输入]
    B --> C[收集联赛积分榜和赛程背景]
    C --> D[逐队检索官方/媒体/伤停/发布会来源]
    D --> E[抽取候选事实和来源]
    E --> F[映射到规范 news_flags]
    F --> G{是否满足触发条件?}
    G -- 否 --> H[news_status=暂无明显异常; flags=[]]
    G -- 是 --> I[news_status=有异常; 填写规范 flags]
    H --> J[横向审校]
    I --> J[横向审校]
    J --> K[删除弱证据和非规范标签]
    K --> L[生成 Markdown 报告]
    K --> M[生成 JSON/CSV]
```

## Query builder

```python
def build_queries(team, scope, league_context):
    names = [team.team]
    if team.local_name:
        names.append(team.local_name)
    queries = []
    for name in names:
        queries.extend([
            f"{name} team news {team.opponent} {scope.date}",
            f"{name} injury suspension press conference {scope.round}",
            f"{name} predicted lineup rotation {team.opponent}",
            f"{name} manager dressing room crisis",
        ])
    queries.append(f"{scope.league} standings relegated qualified champion {scope.date}")
    return dedupe(queries)
```
