"""
NASDAQ Medallion Pipeline Transformations Package
-------------------------------------------------
Transformation logic for prices, news, semantics, event-window impact, and star schema dimensions.
"""

from .semantics import (
    classify_catalyst_category,
    classify_news_genre,
    analyze_financial_sentiment,
    get_materiality_tier,
    enrich_news_article,
    CATALYST_CATEGORIES
)

try:
    from .prices import clean_price_ticks
    from .news import clean_news_articles
    from .impact import evaluate_news_impact
    from .dimensions import build_dim_company, build_dim_date, build_dim_news_source
except ImportError:
    clean_price_ticks = None
    clean_news_articles = None
    evaluate_news_impact = None
    build_dim_company = None
    build_dim_date = None
    build_dim_news_source = None

__all__ = [
    "classify_catalyst_category",
    "classify_news_genre",
    "analyze_financial_sentiment",
    "get_materiality_tier",
    "enrich_news_article",
    "CATALYST_CATEGORIES",
    "clean_price_ticks",
    "clean_news_articles",
    "evaluate_news_impact",
    "build_dim_company",
    "build_dim_date",
    "build_dim_news_source",
]
