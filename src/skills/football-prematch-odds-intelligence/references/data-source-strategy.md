# Data Source Strategy for Prematch Odds Intelligence

This reference guides source discovery when a user needs company-level football odds instead of screenshots.

## Source priority

Use sources that expose bookmaker-level rows with opening and current values. Prefer endpoints or rendered data that provide European odds, Asian handicap, totals, and bookmaker IDs in the same match object. Averages are useful for summary, but they are not enough for a complete report.

| Priority | Source pattern | Use case | Caution |
|---|---|---|---|
| 1 | Odds-center Ajax/JSON endpoints | Company-level opening/current values for multiple markets | Often lacks historical timestamps |
| 2 | Rendered odds tables with selectable companies | Current odds and sometimes opening values | Requires DOM inspection and company normalization |
| 3 | Odds history/change pages | Real update times and movement sequences | May require company ID and match ID mapping |
| 4 | Public editorial or league pages | Team news, injuries, standings, motivation | Do not use for odds unless odds are explicitly present |
| 5 | Screenshots | Visual confirmation only | Do not substitute for tables unless explicitly requested |

## Discovery workflow

First, load the main odds page in the browser and save or inspect the full HTML. Search the HTML and scripts for strings like `odds`, `oyzs_ajax`, `change`, `listOdds`, `company`, `pankou`, `dxpankou`, `FIRST_HOST`, `HOST`, `FIRST_GUEST`, `GUEST`, `SOURCE_COMPANY_ID`, and `COMPANY_NAME`.

If an endpoint is found, call it with the same parameters that the page uses. Common parameters are market type, issue/date, selected companies, league filters, and match IDs. Preserve the raw JSON file before transformation.

If company selection is controlled by checkboxes, enumerate company IDs from the page and request the priority companies. If the site limits selected companies, make multiple requests and merge by match ID and company ID.

## Timestamp handling

Use this precedence order:

| Priority | Field | Report as |
|---|---|---|
| 1 | Historical last-change time | `update_time: YYYY-MM-DD HH:mm` |
| 2 | Source-provided current update time | `update_time: YYYY-MM-DD HH:mm` |
| 3 | Scrape time only | `update_time: source_no_timestamp; scrape_time=...` |
| 4 | No usable time | `update_time: null; status: source_no_timestamp` |

Do not infer a timestamp from kickoff time, page date, or line order.

## Bookmaker normalization

Normalize common names into these keys whenever possible:

| Canonical key | Common aliases |
|---|---|
| 威廉 | William, William Hill, 威廉希尔 |
| 澳门 | Macauslot, 澳彩, 澳门彩票 |
| 立博 | Ladbrokes, 利记, 立博国际 |
| 365 | Bet365, bet365 |
| 易胜博 | Easbet, 易胜 |
| 伟德 | BetVictor, Victor Chandler |
| Pinnacle/平博 | Pinnacle, 平博, Pinnacle Sports |
| Betfair/交易所类 | Betfair, 交易所, Exchange |

## Validation traps

Always check home/away orientation after merging data from multiple sources. Some sources list odds from the market's home/away perspective; others invert when using neutral betting IDs. Confirm against team names in the same record.

Asian handicap signs should be from the home-team perspective. A negative line means the home side gives goals; a positive line means the home side receives goals. Keep the source convention explicit if uncertain.

Totals are usually `over_water / line / under_water`. Do not confuse this with Asian handicap `home_water / line / away_water`.
