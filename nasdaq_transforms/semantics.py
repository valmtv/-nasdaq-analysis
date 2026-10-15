"""
News Semantic Intelligence & Financial Taxonomy Engine
------------------------------------------------------
Enriches raw financial news headlines and summaries with:
1. Standard 7-Category Financial Taxonomy (Earnings, M&A/Capex, Regulatory, Product/AI, Leadership, Macro, Commentary)
2. News Genre Classification ('Breaking Catalyst', 'Post-Hoc Recap', 'Routine Churn')
3. Financial Sentiment Direction ('Bullish', 'Bearish', 'Neutral')
4. Materiality Priority Tier (Tier 1: High -> Tier 4: Noise)

Designed for dual-mode execution:
- Local / Cluster Mode: Pure deterministic financial ontology & rule-based scoring ($0 token cost, instant).
- Databricks Serverless Mode: Pluggable toggle for `ai_classify` / `ai_analyze_sentiment`.
"""

import re
from typing import Dict, Any, Tuple


# ---------------------------------------------------------------------------
# 1. Standard 7-Category Financial Catalyst Taxonomy
# ---------------------------------------------------------------------------

CATALYST_CATEGORIES = [
    "Earnings & Forward Guidance",
    "Mergers, Acquisitions & Capex",
    "Regulatory, Antitrust & Legal",
    "Product, AI & Hardware Launches",
    "Executive & Board Governance",
    "Macroeconomic & Rate Policy",
    "Market Commentary & Routine Churn"
]

# Keywords and regex patterns for financial domain ontology
CATEGORY_PATTERNS = {
    "Earnings & Forward Guidance": [
        r"\bearnings\b", r"\bq[1-4]\b", r"\brevenue\b", r"\bprofit\b", r"\beps\b",
        r"\bguidance\b", r"\boutlook\b", r"\bforecast\b", r"\bquarterly\b", r"\bfy\d{2,4}\b",
        r"\bbeat\b", r"\bmiss\b", r"\bmargin\b", r"\btopline\b", r"\bbottomline\b",
        r"\bfinancial results\b", r"\bdividend\b", r"\bbuyback\b", r"\bshare repurchase\b"
    ],
    "Mergers, Acquisitions & Capex": [
        r"\bacqui\w+", r"\bmerger\b", r"\bbuyout\b", r"\btakeover\b", r"\bdeal\b",
        r"\bdebt deal\b", r"\bwhale\b", r"\biren\b", r"\bbillion.*deal\b", r"\bcapex\b",
        r"\bcapital expenditure\b", r"\binfrastructure\b", r"\bpartnership\b", r"\bjoint venture\b",
        r"\bdata ?center build\b", r"\bfunding round\b", r"\binvestment\b"
    ],
    "Regulatory, Antitrust & Legal": [
        r"\bregulat\w+", r"\bantitrust\b", r"\bdoj\b", r"\bftc\b", r"\bsec\b", r"\beu\b",
        r"\blawsuit\b", r"\bsue[ds]?\b", r"\bpatent\b", r"\binvestigation\b", r"\bprobe\b",
        r"\btariff\b", r"\bsanction\b", r"\bexport control\b", r"\bmonopoly\b", r"\bcompliance\b",
        r"\bsubpoena\b", r"\bfine[ds]?\b", r"\bpenalty\b", r"\bcourt\b"
    ],
    "Product, AI & Hardware Launches": [
        r"\bclaude\b", r"\banthropic\b", r"\bopenai\b", r"\bgpt\w*\b", r"\bcopilot\b",
        r"\bblackwell\b", r"\bchip\b", r"\bgpu\b", r"\bsemiconductor\b", r"\bhardware\b",
        r"\bmodel\b", r"\bfrontier model\b", r"\bsoftware\b", r"\blaunch\w*\b", r"\brelease\w*\b",
        r"\bproduct\b", r"\biphone\b", r"\bvision pro\b", r"\bautopilot\b", r"\bfsd\b",
        r"\brobotaxi\b", r"\bquantum\b", r"\bdatacenter\b", r"\benergy\b", r"\bgrid\b",
        r"\bpower\b", r"\bbloom energy\b"
    ],
    "Executive & Board Governance": [
        r"\bceo\b", r"\bcfo\b", r"\bcto\b", r"\bcoo\b", r"\bchief executive\b",
        r"\btim cook\b", r"\bternus\b", r"\bsuccession\b", r"\bboard\b", r"\bchairman\b",
        r"\bresign\w*\b", r"\bstepp?ing down\b", r"\bactivist investor\b", r"\belliott\b",
        r"\bshareholder vote\b", r"\bproxy\b", r"\bexecutive\b"
    ],
    "Macroeconomic & Rate Policy": [
        r"\bfed\b", r"\bfederal reserve\b", r"\bpowell\b", r"\brate cut\b", r"\brate hike\b",
        r"\binterest rates?\b", r"\binflation\b", r"\bcpi\b", r"\bpce\b", r"\btreasury\b",
        r"\byield\b", r"\bjobs report\b", r"\bpayrolls?\b", r"\bgdp\b", r"\brecession\b",
        r"\bcentral bank\b", r"\bjackson hole\b", r"\bfomc\b"
    ]
}

# ---------------------------------------------------------------------------
# 2. News Genre Patterns ('Breaking Catalyst', 'Post-Hoc Recap', 'Routine Churn')
# ---------------------------------------------------------------------------

POST_HOC_PATTERNS = [
    r"\bwhy .* (falling|dropping|rising|plunging|soaring|down|up) today\b",
    r"\bmidday movers\b",
    r"\bmarket wrap\b",
    r"\bstock (tumbles|slides|jumps|pops|slumps) as\b",
    r"\bwhat to watch in\b",
    r"\bhere'?s why\b",
    r"\bthe real reason\b",
    r"\bstocks making biggest moves\b",
    r"\bmarket recap\b"
]

ROUTINE_CHURN_PATTERNS = [
    r"\bzacks rank\b",
    r"\bmotley fool\b",
    r"\b3 stocks to buy\b",
    r"\bshould you buy\b",
    r"\btechnical analysis\b",
    r"\boptions activity\b",
    r"\bis .* a good buy\b",
    r"\betf holdings update\b",
    r"\branked #\d\b"
]

# ---------------------------------------------------------------------------
# 3. Financial Sentiment Lexicon
# ---------------------------------------------------------------------------

BULLISH_KEYWORDS = [
    "surge", "surges", "surged", "jump", "jumps", "jumped", "beat", "beats", "beating",
    "exceeds", "record", "upgrade", "upgrades", "upgraded", "rally", "rallies", "growth",
    "boost", "boosts", "gain", "gains", "soar", "soars", "soaring", "strong", "outperform",
    "profit jumps", "revenue beats", "partnership", "breakthrough", "expands", "approved"
]

BEARISH_KEYWORDS = [
    "plunge", "plunges", "drop", "drops", "slump", "slumps", "miss", "misses", "missing",
    "downgrade", "downgrades", "fall", "falls", "falling", "lawsuit", "sued", "probe",
    "investigation", "fine", "fined", "warning", "warns", "cut", "cuts", "cutting",
    "tariff", "antitrust", "subpoena", "recession", "loss", "losses", "weak", "disappoint"
]


# ---------------------------------------------------------------------------
# Core Semantic Classification Functions
# ---------------------------------------------------------------------------

def classify_catalyst_category(headline: str, summary: str = "") -> str:
    """
    Categorizes financial news into the 7 standard categories.
    Evaluates matches in headline (weighted 2x) and summary.
    """
    text = f"{headline or ''} {summary or ''}".lower()
    headline_lower = (headline or "").lower()

    # Score each category based on keyword hits
    scores: Dict[str, int] = {cat: 0 for cat in CATALYST_CATEGORIES}

    for cat, patterns in CATEGORY_PATTERNS.items():
        for pat in patterns:
            # Matches in headline get higher weight
            if re.search(pat, headline_lower):
                scores[cat] += 3
            elif re.search(pat, text):
                scores[cat] += 1

    # Select category with max score
    best_cat = max(scores, key=scores.get)
    if scores[best_cat] > 0:
        return best_cat
    return "Market Commentary & Routine Churn"


def classify_news_genre(headline: str) -> str:
    """
    Classifies the reporting genre:
    - 'Post-Hoc Recap': Article written after price already moved ("Why X is down today")
    - 'Routine Churn': Automated algorithmic / clickbait articles ("3 stocks to buy", Zacks)
    - 'Breaking Catalyst': Forward-looking news, primary announcements, material disclosures
    """
    hl = (headline or "").lower()

    if any(re.search(pat, hl) for pat in POST_HOC_PATTERNS):
        return "Post-Hoc Recap"
    if any(re.search(pat, hl) for pat in ROUTINE_CHURN_PATTERNS):
        return "Routine Churn"
    return "Breaking Catalyst"


def analyze_financial_sentiment(headline: str, summary: str = "") -> Tuple[str, float]:
    """
    Determines sentiment direction and confidence score:
    Returns (Sentiment: 'Bullish' | 'Bearish' | 'Neutral', Confidence: float 0.0-1.0)
    """
    text = f"{headline or ''} {summary or ''}".lower()

    bull_hits = sum(1 for kw in BULLISH_KEYWORDS if kw in text)
    bear_hits = sum(1 for kw in BEARISH_KEYWORDS if kw in text)

    net = bull_hits - bear_hits
    total = bull_hits + bear_hits

    if total == 0:
        return "Neutral", 0.50

    confidence = min(0.95, round(0.50 + (abs(net) / (total + 1.0)) * 0.45, 2))

    if net > 0:
        return "Bullish", confidence
    elif net < 0:
        return "Bearish", confidence
    else:
        return "Neutral", 0.50


def get_materiality_tier(category: str, genre: str) -> Tuple[int, float]:
    """
    Returns (Tier: 1 to 4, Weight: 0.1 to 1.0)
    Tier 1: Material Corporate Action (Earnings, M&A, Regulatory)
    Tier 2: Product & Executive (Launches, Leadership)
    Tier 3: Macro Context
    Tier 4: Noise & Churn
    """
    if genre == "Routine Churn":
        return 4, 0.10
    if genre == "Post-Hoc Recap":
        return 3, 0.25

    if category in ["Earnings & Forward Guidance", "Mergers, Acquisitions & Capex", "Regulatory, Antitrust & Legal"]:
        return 1, 1.00
    elif category in ["Product, AI & Hardware Launches", "Executive & Board Governance"]:
        return 2, 0.75
    elif category == "Macroeconomic & Rate Policy":
        return 3, 0.50
    else:
        return 4, 0.20


def enrich_news_article(headline: str, summary: str = "") -> Dict[str, Any]:
    """
    Full semantic enrichment record for a single news article.
    """
    category = classify_catalyst_category(headline, summary)
    genre = classify_news_genre(headline)
    sentiment, confidence = analyze_financial_sentiment(headline, summary)
    tier, weight = get_materiality_tier(category, genre)

    return {
        "CatalystCategory": category,
        "NewsGenre": genre,
        "NewsSentiment": sentiment,
        "SentimentConfidence": confidence,
        "MaterialityTier": tier,
        "MaterialityWeight": weight
    }
