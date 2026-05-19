# {league} {season_or_round}（{date}）球队关键异常信息

**作者：{author}**  
**口径说明：**本报告按指定 `news_flags` 口径整理，只保留对赛前判断有显著影响的异常：主帅下课/更衣室问题、核心球员伤停、多名主力轮换、杯赛前后、欧战资格已锁定、已降级/已夺冠/已无欲无求、球队近期负面舆论明显、临场阵容确认后有重大变化。若未核实到足够显著的异常，则写作 `news_status: 暂无明显异常`。本报告基于公开赛前信息，不等同于临场官方首发确认。

> **总体背景。** {one_paragraph_league_context_with_reference_ids}

## 一、赛程核对

| 比赛 | 开球时间 | 说明 |
|---|---:|---|
| {home} vs {away} | {kickoff} | {brief_context} |

## 二、球队关键异常清单

| 球队 | 对阵 | news_status | news_flags | 关键异常说明 | 置信度 |
|---|---|---|---|---|---|
| **{team}** | {fixture} | {暂无明显异常/有异常} | {flag1；flag2 or 暂无明显异常} | {one_sentence_evidence_based_summary} | {high/medium/low} |

## 三、审校后的高优先级关注点

{Write two or three professional paragraphs. The first paragraph should group the strongest material anomalies. The second paragraph should explain which weak signals were rejected, such as ordinary motivation pressure, unverified rotation, or low-quality social media claims. If there are no strong anomalies, explicitly state that the round is clean under the selected taxonomy.}

## 四、结构化数据说明

结构化文件与本报告一一对应。每支球队包含 `team`、`fixture`、`kickoff`、`news_status`、`news_flags`、`key_abnormalities`、`sources`、`confidence` 和 `review_notes`。其中 `news_flags` 只使用规范标签；未达到阈值的球队固定为 `news_status: 暂无明显异常`。

## References

[1]: {url} "{source title}"
