import re
from bs4 import BeautifulSoup
from typing import Dict, Any, List, Tuple

# 关键词库
FOOTBALL_SIGNALS = [
    "goal", "winner", "minute", "injury", "international", "duty", "squad",
    "training", "clash", "fixture", "premier league", "appearance", "scored",
    "lineup", "striker", "midfielder", "defender", "goalkeeper", "hamstring",
    "fitness", "substitute", "assist", "match", "kick-off", "press conference",
    "ruled out", "fit to start", "qualifier", "caps", "scored for"
]

CULTURE_HISTORY_SIGNALS = [
    "former player", "former players", "anniversary", "tribute", "legend",
    "hall of fame", "historian", "museum", "decades ago", "ex-player",
    "heritage", "veteran", "once a blue"
]

COMMERCIAL_PRIMARY_SIGNALS = [
    "store", "retail", "collection", "fashion", "clothing", "apparel",
    "sale on", "buy now", "shop online", "merchandise", "discount",
    "tommy hilfiger", "lifestyle range", "away kit release", "home kit launch"
]

NOISE_TAGS = ["script", "style", "nav", "aside", "footer", "header", "noscript", "svg", "form"]
NOISE_CLASSES_OR_IDS = [
    "sidebar", "recommendation", "recommended", "related", "ad-container",
    "advertisement", "social-share", "share-bar", "menu", "breadcrumb",
    "newsletter-signup", "footer", "cookie-banner"
]

def clean_soup_for_main_content(soup: BeautifulSoup) -> BeautifulSoup:
    """
    Strips noise elements (nav, aside, footer, recommendations, ads)
    to isolate the true main article body.
    """
    for tag in soup.find_all(NOISE_TAGS):
        tag.decompose()

    for bad in soup.find_all(attrs={"class": lambda c: c and any(n in str(c).lower() for n in NOISE_CLASSES_OR_IDS)}):
        bad.decompose()

    for bad in soup.find_all(attrs={"id": lambda i: i and any(n in str(i).lower() for n in NOISE_CLASSES_OR_IDS)}):
        bad.decompose()

    return soup

def discover_articles_from_index(team_id: str, index_html: str, base_url: str) -> Tuple[List[str], str]:
    """
    Discovers article URLs dynamically from index HTML.
    Eliminates hardcoded per-team zero-results.
    """
    if not index_html:
        return [], "EMPTY_HTML"

    soup = BeautifulSoup(index_html, 'html.parser')
    discovered = []

    # General regex search for news articles across standard club patterns
    # Matches href like: /news/... or /news/mens/...
    patterns = [
        r'href=["\'](/news/[a-zA-Z0-9_\-/]+)["\']',
        r'href=["\'](https?://[a-zA-Z0-9_\-\.]+/news/[a-zA-Z0-9_\-/]+)["\']',
        r'href=["\'](/[a-zA-Z0-9_\-/]*news/[0-9]{4}/[a-zA-Z0-9_\-/]+)["\']',
        r'href=["\'](/en/news/[a-zA-Z0-9_\-/]+)["\']'
    ]

    for p in patterns:
        for match in re.findall(p, index_html):
            clean_match = match.split("?")[0].split("#")[0]
            if any(ign in clean_match.lower() for ign in ["/category/", "/listing", "/page/", "/tags/"]):
                continue
            if len(clean_match.strip("/")) < 15:
                continue

            if clean_match.startswith("http"):
                full_url = clean_match
            else:
                full_url = base_url.rstrip("/") + "/" + clean_match.lstrip("/")

            if full_url not in discovered:
                discovered.append(full_url)

    # Fallback to <a> tag inspection
    if not discovered:
        for a in soup.find_all('a', href=True):
            href = a['href'].split("?")[0].split("#")[0]
            if '/news/' in href and len(href.strip("/")) > 15:
                if href.startswith("http"):
                    full_url = href
                else:
                    full_url = base_url.rstrip("/") + "/" + href.lstrip("/")
                if full_url not in discovered:
                    discovered.append(full_url)

    if discovered:
        return discovered, "INDEX_PARSED_SUCCESS"

    # If no links discovered, check if page is a Client-Side Rendered SPA skeleton
    has_spa_container = bool(soup.find(id=re.compile(r'(root|__next|app|main-content)')))
    if has_spa_container and len(index_html) < 200000:
        return [], "CLIENT_RENDERED_SPA_EMPTY_BODY"

    return [], "NO_LINKS_FOUND"

def extract_main_article_body(soup: BeautifulSoup) -> List[str]:
    """
    Extracts clean paragraphs exclusively from the primary content container,
    ignoring aside, recommendations, and noise.
    """
    # Clone soup to avoid mutating original
    clean_soup = clean_soup_for_main_content(BeautifulSoup(str(soup), 'html.parser'))

    # Try identifying article container
    article_container = (
        clean_soup.find('article') or
        clean_soup.find('main') or
        clean_soup.find(attrs={"role": "main"}) or
        clean_soup.find(class_=re.compile(r'(article-body|article__body|article-content|entry-content)', re.I)) or
        clean_soup.find('body')
    )

    if not article_container:
        return []

    paras = []
    for p in article_container.find_all('p'):
        text = p.get_text(separator=' ', strip=True)
        if len(text) >= 25 and not any(ign in text.lower() for ign in [
            "cookie", "privacy policy", "all rights reserved", "terms and conditions", "sign up for"
        ]):
            paras.append(text)
    return paras

def extract_article_content(team_id: str, url: str, article_html: str) -> Dict[str, Any]:
    """
    Extracts article title, published_date, match_kickoff, press_conference_info, paragraphs.
    Categorizes into:
    - FOOTBALL_INTELLIGENCE: Match results, minutes played, injuries, team fitness, press conferences.
    - CLUB_CULTURE_HISTORY: Ex-player tributes, anniversaries, museum/heritage features.
    - COMMERCIAL_EXCLUDED: Apparel sales, retail promotions, commercial sponsors without team facts.
    - TECHNICAL_GAP: Empty body due to CSR or parsing failure.
    """
    if not article_html:
        return {
            "url": url,
            "title": "Empty Response",
            "published_at": "UNKNOWN",
            "match_kickoff": None,
            "press_conference": None,
            "category": "TECHNICAL_GAP",
            "status": "FETCH_FAILED_OR_EMPTY",
            "paragraphs": [],
            "excerpt": "无法获取文章响应内容或响应为空。"
        }

    soup = BeautifulSoup(article_html, 'html.parser')

    # Check for empty body / CSR barrier
    body = soup.find('body')
    body_text = body.get_text(separator=' ', strip=True) if body else ""
    if len(body_text) < 120 and len(article_html) > 40000:
        return {
            "url": url,
            "title": soup.title.string.strip() if soup.title else "No Title",
            "published_at": "UNKNOWN",
            "match_kickoff": None,
            "press_conference": None,
            "category": "TECHNICAL_GAP",
            "status": "CLIENT_RENDERED_BODY_EMPTY",
            "paragraphs": [],
            "excerpt": "正文依赖前端客户端渲染异步注入，静态 HTML 源码缺少段落文字。"
        }

    title = soup.title.string.strip() if soup.title else "No Title"

    # Extract clean paragraphs from main body only
    clean_paras = extract_main_article_body(soup)

    if not clean_paras:
        return {
            "url": url,
            "title": title,
            "published_at": "UNKNOWN",
            "match_kickoff": None,
            "press_conference": None,
            "category": "TECHNICAL_GAP",
            "status": "NO_PARAGRAPHS_EXTRACTED",
            "paragraphs": [],
            "excerpt": "未从正文主区域解析到有效文本段落。"
        }

    # 1. Date extraction
    published_at = "UNKNOWN"
    pub_match = re.search(r'([A-Za-z]{3}\s+\d{1,2}\s+[A-Za-z]{3}\s+\d{4},\s+\d{1,2}:\d{2})', article_html)
    if pub_match:
        published_at = pub_match.group(1).strip()
    else:
        time_tag = soup.find('time')
        if time_tag:
            published_at = time_tag.get_text(strip=True)

    # 2. Match kickoff extraction
    match_kickoff = None
    ko_match = re.search(r'(Saturday\s+\d{1,2}\s+[A-Za-z]+\s+with the game scheduled for kick-off at [^\.]*for\s+\d{1,2}:\d{2}\s+[A-Za-z]+)', article_html, re.IGNORECASE)
    if ko_match:
        match_kickoff = ko_match.group(1).strip()
    else:
        gen_ko = re.search(r'(\d{1,2}:\d{2}\s*(?:UK|GMT|BST|CST)[^\.\n]*\d{1,2}\s+[A-Za-z]+\s+\d{4})', article_html)
        if gen_ko:
            match_kickoff = gen_ko.group(1).strip()

    # 3. Press conference extraction (Only from valid explicit text)
    press_conference = None
    pc_match = re.search(r'(The manager will speak to the press at [^\.]*\.)', article_html, re.IGNORECASE)
    if pc_match:
        press_conference = pc_match.group(1).strip()

    # 4. Multi-tier classification logic
    # Join title and clean paragraphs
    combined_text = (title + " " + " ".join(clean_paras)).lower()

    football_score = sum(1 for sig in FOOTBALL_SIGNALS if sig in combined_text)
    culture_score = sum(1 for sig in CULTURE_HISTORY_SIGNALS if sig in combined_text)
    commercial_score = sum(1 for sig in COMMERCIAL_PRIMARY_SIGNALS if sig in combined_text)

    # Decisions:
    # 1) If it explicitly reports international appearances, goals, injuries, minutes played -> FOOTBALL_INTELLIGENCE
    # (Even if the page has partner mentions or kit branding, it's not commercial excluded)
    if football_score >= 2 or any(term in combined_text for term in ["haaland hits", "international round-up", "international duty", "players feature in international", "ruled out", "injury update", "fitness update"]):
        category = "FOOTBALL_INTELLIGENCE"
    elif commercial_score >= 2 and football_score < 2:
        category = "COMMERCIAL_EXCLUDED"
    elif culture_score >= 1 and football_score < 2:
        category = "CLUB_CULTURE_HISTORY"
    elif commercial_score > 0:
        category = "COMMERCIAL_EXCLUDED"
    else:
        category = "FOOTBALL_INTELLIGENCE"

    return {
        "url": url,
        "title": title,
        "published_at": published_at,
        "match_kickoff": match_kickoff,
        "press_conference": press_conference,
        "category": category,
        "status": "SUCCESS_EXTRACTED",
        "paragraphs": clean_paras,
        "excerpt": clean_paras[0] if clean_paras else "无正文段落"
    }
