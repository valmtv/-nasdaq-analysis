"""
Gold Star Schema Layer Module (Lakeflow Declarative Pipeline)
-------------------------------------------------------------
Constructs the Star Schema dimensional model:
- Dimensions: dim_company (with CAPM Beta), dim_date, dim_news_source
- Facts: fact_stock_prices_5m, fact_news_impact (with Beta-adjusted True Alpha,
  Time-of-Day volume normalization in Eastern Time, strict temporal bounds,
  pre-market bundle ranking, and 5-state causality verdicts).
"""

try:
    import dlt
except ImportError:
    import pyspark.pipelines as dlt

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# -------------------------------------------------------------------------
# Dimensions
# -------------------------------------------------------------------------

COMPANY_METADATA = [
    ("AAPL", "Apple Inc.", "Technology", "Consumer Electronics", "NASDAQ", 1.22),
    ("NVDA", "NVIDIA Corporation", "Technology", "Semiconductors", "NASDAQ", 2.15),
    ("MSFT", "Microsoft Corporation", "Technology", "Software - Infrastructure", "NASDAQ", 1.18),
    ("AMZN", "Amazon.com Inc.", "Consumer Cyclical", "Internet Retail", "NASDAQ", 1.35),
    ("TSLA", "Tesla Inc.", "Consumer Cyclical", "Auto Manufacturers", "NASDAQ", 2.05),
    ("QQQ", "Invesco QQQ Trust", "Financial Services", "Exchange Traded Fund", "NASDAQ", 1.00),
]


@dlt.table(
    name="dim_company",
    comment="Company dimension with industry classifications, ticker surrogate keys, and CAPM Betas",
    table_properties={"quality": "gold"}
)
def dim_company():
    df_prices = dlt.read("silver_nasdaq_price_5m")
    meta_df = spark.createDataFrame(
        COMPANY_METADATA,
        ["Symbol_ref", "CompanyName", "Sector", "Industry", "Exchange", "Beta"]
    )

    return (
        df_prices
        .select("Symbol")
        .distinct()
        .join(meta_df, F.col("Symbol") == meta_df.Symbol_ref, "left")
        .withColumn("company_sk", F.abs(F.hash(F.col("Symbol"))))
        .withColumn("CompanyName", F.coalesce(F.col("CompanyName"), F.col("Symbol")))
        .withColumn("Sector", F.coalesce(F.col("Sector"), F.lit("Unknown")))
        .withColumn("Industry", F.coalesce(F.col("Industry"), F.lit("Unknown")))
        .withColumn("Exchange", F.coalesce(F.col("Exchange"), F.lit("NASDAQ")))
        .withColumn("Beta", F.coalesce(F.col("Beta"), F.lit(1.00)))
        .select("company_sk", "Symbol", "CompanyName", "Sector", "Industry", "Exchange", "Beta")
    )


@dlt.table(
    name="dim_date",
    comment="Conformed calendar date dimension table",
    table_properties={"quality": "gold"}
)
def dim_date():
    df_prices = dlt.read("silver_nasdaq_price_5m")
    df_news = dlt.read("silver_finnhub_news")

    price_dates = df_prices.select(F.col("TradeDate").cast("date").alias("CalendarDate"))
    news_dates = df_news.select(F.col("NewsDate").cast("date").alias("CalendarDate"))

    return (
        price_dates
        .union(news_dates)
        .filter(F.col("CalendarDate").isNotNull())
        .distinct()
        .withColumn("date_sk", F.date_format("CalendarDate", "yyyyMMdd").cast("integer"))
        .withColumn("Year", F.year("CalendarDate"))
        .withColumn("Quarter", F.quarter("CalendarDate"))
        .withColumn("Month", F.month("CalendarDate"))
        .withColumn("DayOfMonth", F.dayofmonth("CalendarDate"))
        .withColumn("DayOfWeek", F.dayofweek("CalendarDate"))
        .withColumn("DayName", F.date_format("CalendarDate", "EEEE"))
        .withColumn(
            "IsWeekend",
            F.when(F.dayofweek("CalendarDate").isin(1, 7), F.lit(True)).otherwise(F.lit(False))
        )
        .select(
            "date_sk",
            "CalendarDate",
            "Year",
            "Quarter",
            "Month",
            "DayOfMonth",
            "DayOfWeek",
            "DayName",
            "IsWeekend"
        )
        .orderBy("CalendarDate")
    )


@dlt.table(
    name="dim_news_source",
    comment="News source publisher and editorial category dimension",
    table_properties={"quality": "gold"}
)
def dim_news_source():
    df_news = dlt.read("silver_finnhub_news")

    return (
        df_news
        .select("Source", "Category")
        .distinct()
        .withColumn("source_sk", F.abs(F.hash(F.concat(F.col("Source"), F.lit("_"), F.col("Category")))))
        .select("source_sk", F.col("Source").alias("SourceName"), "Category")
    )


# -------------------------------------------------------------------------
# Facts
# -------------------------------------------------------------------------

@dlt.table(
    name="fact_stock_prices_5m",
    comment="Fact table containing 5-minute stock prices for charting and overlays",
    table_properties={"quality": "gold"}
)
def fact_stock_prices_5m():
    df_prices = dlt.read("silver_nasdaq_price_5m")

    return (
        df_prices
        .withColumn("company_sk", F.abs(F.hash(F.col("Symbol"))))
        .withColumn("date_sk", F.date_format("TradeDate", "yyyyMMdd").cast("integer"))
        .select(
            "company_sk",
            "date_sk",
            "Symbol",
            "PriceTimestamp",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume"
        )
    )


@dlt.table(
    name="fact_news_impact",
    comment="Gold fact table evaluating multi-dimensional stock market reactions to news events with Beta-adjusted Alpha, strict temporal validity, and causality",
    table_properties={"quality": "gold"}
)
def fact_news_impact():
    df_news = dlt.read("silver_finnhub_news")
    df_prices = dlt.read("silver_nasdaq_price_5m")

    # 1. Filter company news
    company_news = df_news.filter(F.col("Symbol") != "GENERAL")

    # 2. Compute 20-period baseline volume with realistic ticker cold-start fallbacks
    ticker_baseline_fallback = (
        F.when(F.col("Symbol") == "NVDA", F.lit(1500000.0))
        .when(F.col("Symbol") == "TSLA", F.lit(800000.0))
        .when(F.col("Symbol") == "AAPL", F.lit(750000.0))
        .when(F.col("Symbol") == "AMZN", F.lit(500000.0))
        .when(F.col("Symbol") == "MSFT", F.lit(450000.0))
        .when(F.col("Symbol") == "QQQ", F.lit(1800000.0))
        .otherwise(F.lit(500000.0))
    )

    window_baseline = Window.partitionBy("Symbol").orderBy("PriceTimestamp").rowsBetween(-20, -1)
    prices_with_baseline = (
        df_prices
        .withColumn("BaselineAvgVolume", F.coalesce(F.avg("Volume").over(window_baseline), ticker_baseline_fallback))
    )

    # 3. Benchmark QQQ prices
    qqq_prices = (
        prices_with_baseline
        .filter(F.col("Symbol") == "QQQ")
        .select(
            F.col("PriceTimestamp").alias("QQQ_Timestamp"),
            F.col("Close").alias("QQQ_Close")
        )
    )

    # 4. Snap pre-event price (closest price bar on or after news)
    window_pre = Window.partitionBy("ArticleId").orderBy("PriceTimestamp")
    join_pre = (
        company_news
        .join(
            prices_with_baseline,
            (company_news.Symbol == prices_with_baseline.Symbol) &
            (prices_with_baseline.PriceTimestamp >= company_news.NewsTimestamp),
            "left"
        )
        .withColumn("pre_rank", F.row_number().over(window_pre))
        .filter((F.col("pre_rank") == 1) | F.col("pre_rank").isNull())
        .select(
            company_news["*"],
            F.col("PriceTimestamp").alias("Pre_Timestamp"),
            F.col("Open").alias("Pre_Open"),
            F.col("Close").alias("Pre_Close"),
            F.col("BaselineAvgVolume")
        )
    )

    # 5. Snap post-event window price (30 minutes after Pre_Timestamp)
    window_post = Window.partitionBy("ArticleId").orderBy("PriceTimestamp")
    join_post = (
        join_pre
        .join(
            prices_with_baseline,
            (join_pre.Symbol == prices_with_baseline.Symbol) &
            (prices_with_baseline.PriceTimestamp >= F.from_unixtime(
                F.unix_timestamp(join_pre.Pre_Timestamp) + 1800
            )),
            "left"
        )
        .withColumn("post_rank", F.row_number().over(window_post))
        .filter((F.col("post_rank") == 1) | F.col("post_rank").isNull())
        .select(
            join_pre["*"],
            F.col("PriceTimestamp").alias("Post_Timestamp"),
            F.col("Close").alias("Post_Close"),
            F.col("High").alias("Post_High"),
            F.col("Low").alias("Post_Low"),
            F.col("Volume").alias("Post_Volume")
        )
    )

    # 6. Join QQQ Pre & Post Close
    join_qqq = (
        join_post
        .join(
            qqq_prices.withColumnRenamed("QQQ_Close", "QQQ_Pre_Close"),
            join_post.Pre_Timestamp == F.col("QQQ_Timestamp"),
            "left"
        )
        .drop("QQQ_Timestamp")
        .join(
            qqq_prices.withColumnRenamed("QQQ_Close", "QQQ_Post_Close"),
            join_post.Post_Timestamp == F.col("QQQ_Timestamp"),
            "left"
        )
        .drop("QQQ_Timestamp")
    )

    # 7. Evaluate Impact Metrics with strict temporal validity guards
    pre_news_diff_hours = (F.unix_timestamp("Post_Timestamp") - F.unix_timestamp("Pre_Timestamp")) / 3600.0 # time diff post-pre
    news_to_trade_hours = (F.unix_timestamp("Pre_Timestamp") - F.unix_timestamp("NewsTimestamp")) / 3600.0

    # Strict Completion Guard: Price bar must exist within 72 hours of news release (max weekend/holiday gap)
    is_completed = (
        F.col("Pre_Close").isNotNull() &
        F.col("Post_Close").isNotNull() &
        (news_to_trade_hours >= 0.0) &
        (news_to_trade_hours <= 72.0) &
        (pre_news_diff_hours <= 24.0)
    )

    stock_return = F.round(((F.col("Post_Close") - F.col("Pre_Close")) / F.col("Pre_Close")) * 100.0, 4)
    qqq_return = F.when(
        F.col("QQQ_Pre_Close").isNotNull() & F.col("QQQ_Post_Close").isNotNull(),
        F.round(((F.col("QQQ_Post_Close") - F.col("QQQ_Pre_Close")) / F.col("QQQ_Pre_Close")) * 100.0, 4)
    ).otherwise(F.lit(0.0))

    # Ticker CAPM Beta assignment
    ticker_beta = (
        F.when(F.col("Symbol") == "NVDA", F.lit(2.15))
        .when(F.col("Symbol") == "TSLA", F.lit(2.05))
        .when(F.col("Symbol") == "AMZN", F.lit(1.35))
        .when(F.col("Symbol") == "AAPL", F.lit(1.22))
        .when(F.col("Symbol") == "MSFT", F.lit(1.18))
        .otherwise(F.lit(1.00))
    )

    expected_beta_return = F.round(ticker_beta * qqq_return, 4)
    true_alpha = F.round(stock_return - expected_beta_return, 4)
    raw_alpha = F.round(stock_return - qqq_return, 4)
    intraday_volatility = F.round(((F.col("Post_High") - F.col("Post_Low")) / F.col("Pre_Close")) * 100.0, 4)

    # Time-of-Day (TOD) Volume Normalization in America/New_York Eastern Time
    tod_eastern = F.from_utc_timestamp(F.col("Pre_Timestamp"), "America/New_York")
    tod_minute = F.hour(tod_eastern) * 60 + F.minute(tod_eastern)
    tod_multiplier = (
        F.when((tod_minute >= 570) & (tod_minute < 600), F.lit(3.5))   # 9:30 - 10:00 AM Open
        .when((tod_minute >= 600) & (tod_minute < 690), F.lit(1.4))   # 10:00 - 11:30 AM
        .when((tod_minute >= 690) & (tod_minute < 840), F.lit(0.7))   # 11:30 AM - 2:00 PM Lunch
        .when((tod_minute >= 840) & (tod_minute < 930), F.lit(1.1))   # 2:00 - 3:30 PM
        .when((tod_minute >= 930) & (tod_minute <= 960), F.lit(2.5))  # 3:30 - 4:00 PM Close
        .otherwise(F.lit(1.0))
    )
    expected_volume = F.greatest(F.col("BaselineAvgVolume") * tod_multiplier, F.lit(1000.0))
    volume_surge = F.round(F.col("Post_Volume") / expected_volume, 2)

    # Off-hours & Overnight Session Tagging in Eastern Time
    news_eastern = F.from_utc_timestamp(F.col("NewsTimestamp"), "America/New_York")
    news_hour = F.hour(news_eastern)
    news_minute = F.minute(news_eastern)
    is_off_hours = (
        (news_hour < 9) |
        ((news_hour == 9) & (news_minute < 30)) |
        (news_hour >= 16) |
        (F.dayofweek(news_eastern).isin(1, 7))
    )
    session_type = F.when(is_off_hours, F.lit("OVERNIGHT_GAP")).otherwise(F.lit("REGULAR_HOURS"))

    # Composite impact score: -100 to +100
    raw_score = (
        (true_alpha * 20.0) +
        F.when(true_alpha >= 0, (F.least(volume_surge, F.lit(5.0)) - 1.0) * 8.0)
        .otherwise(-((F.least(volume_surge, F.lit(5.0)) - 1.0) * 8.0)) +
        F.when(true_alpha >= 0, intraday_volatility * 10.0)
        .otherwise(-(intraday_volatility * 10.0))
    )
    composite_score = F.round(F.greatest(F.lit(-100.0), F.least(F.lit(100.0), raw_score)), 2)

    reaction_pattern = (
        F.when(F.abs(true_alpha) >= 0.4, F.lit("Sustained Move"))
        .when((intraday_volatility >= 0.5) & (F.abs(true_alpha) < 0.2), F.lit("Reversal/Fade"))
        .otherwise(F.lit("Flat Noise"))
    )

    # Fallback semantic expressions for historical rows where silver was ingested earlier
    category_expr = (
        F.when(F.lower(F.col("Headline")).rlike(r"earnings|q[1-4]|revenue|profit|eps|guidance|outlook|financial results|beat|miss|margin|dividend|buyback"), F.lit("Earnings & Forward Guidance"))
        .when(F.lower(F.col("Headline")).rlike(r"acqui|merger|buyout|takeover|debt deal|whale|iren|capex|partnership|funding|investment"), F.lit("Mergers, Acquisitions & Capex"))
        .when(F.lower(F.col("Headline")).rlike(r"regulat|antitrust|doj|ftc|sec|lawsuit|sued|patent|probe|tariff|monopoly|subpoena|penalty"), F.lit("Regulatory, Antitrust & Legal"))
        .when(F.lower(F.col("Headline")).rlike(r"claude|anthropic|openai|copilot|blackwell|chip|gpu|semiconductor|launch|vision pro|robotaxi|grid|power|bloom energy|hardware"), F.lit("Product, AI & Hardware Launches"))
        .when(F.lower(F.col("Headline")).rlike(r"ceo|cfo|chief executive|tim cook|ternus|succession|board|resign|activist|leadership"), F.lit("Executive & Board Governance"))
        .when(F.lower(F.col("Headline")).rlike(r"fed|federal reserve|powell|rate cut|rate hike|inflation|cpi|treasury|yield|jobs report|fomc"), F.lit("Macroeconomic & Rate Policy"))
        .otherwise(F.lit("Market Commentary & Routine Churn"))
    )

    genre_expr = (
        F.when(F.lower(F.col("Headline")).rlike(r"why .* (falling|dropping|rising|plunging|down|up) today|midday movers|market wrap|stocks making biggest moves|market recap"), F.lit("Post-Hoc Recap"))
        .when(F.lower(F.col("Headline")).rlike(r"zacks rank|motley fool|3 stocks to buy|should you buy|etf holdings update"), F.lit("Routine Churn"))
        .otherwise(F.lit("Breaking Catalyst"))
    )

    sentiment_expr = (
        F.when(F.lower(F.col("Headline")).rlike(r"surge|jump|beat|exceed|record|upgrade|rally|growth|boost|gain|soar|strong|outperform|approved"), F.lit("Bullish"))
        .when(F.lower(F.col("Headline")).rlike(r"plunge|drop|slump|miss|downgrade|fall|lawsuit|sued|probe|warning|cut|tariff|antitrust|weak|disappoint"), F.lit("Bearish"))
        .otherwise(F.lit("Neutral"))
    )

    materiality_expr = (
        F.when(genre_expr == "Routine Churn", F.lit(4))
        .when(genre_expr == "Post-Hoc Recap", F.lit(3))
        .when(category_expr.isin("Earnings & Forward Guidance", "Mergers, Acquisitions & Capex", "Regulatory, Antitrust & Legal"), F.lit(1))
        .when(category_expr.isin("Product, AI & Hardware Launches", "Executive & Board Governance"), F.lit(2))
        .when(category_expr == "Macroeconomic & Rate Policy", F.lit(3))
        .otherwise(F.lit(4))
    )

    final_category = F.coalesce(F.col("CatalystCategory"), category_expr)
    final_genre = F.coalesce(F.col("NewsGenre"), genre_expr)
    final_sentiment = F.coalesce(F.col("NewsSentiment"), sentiment_expr)
    final_materiality = F.coalesce(F.col("MaterialityTier"), materiality_expr)

    # Pre-market bundle ranking
    window_bundle = Window.partitionBy("Symbol", "Pre_Timestamp").orderBy(
        final_materiality.asc(),
        F.col("NewsTimestamp").desc()
    )

    # 5-State Causality Verdict
    raw_causality = (
        F.when(final_genre == "Post-Hoc Recap", F.lit("Post-Hoc Explainer"))
        .when((final_genre == "Routine Churn") | (final_materiality == 4), F.lit("Flat Noise"))
        .when((F.abs(stock_return) >= 0.4) & (F.abs(true_alpha) < 0.2), F.lit("Beta Drift / Macro Mover"))
        .when((intraday_volatility >= 0.5) & (F.abs(stock_return) < 0.2), F.lit("Market Fade / False Alarm"))
        .when((F.abs(true_alpha) >= 0.4) & (volume_surge >= 1.2), F.lit("Verified Catalyst"))
        .otherwise(F.lit("Flat Noise"))
    )

    # Divergence type
    divergence_type = (
        F.when((final_sentiment == "Bullish") & (true_alpha < -0.4), F.lit("Guidance Shock / Contrarian Drop"))
        .when((final_sentiment == "Bearish") & (true_alpha > 0.4), F.lit("Relief Rally / Contrarian Gain"))
        .when((final_sentiment == "Bullish") & (true_alpha >= 0.4), F.lit("Confirmed Bull Catalyst"))
        .when((final_sentiment == "Bearish") & (true_alpha <= -0.4), F.lit("Confirmed Bear Catalyst"))
        .otherwise(F.lit("Neutral / In Line"))
    )

    return (
        join_qqq
        .withColumn("company_sk", F.abs(F.hash(F.col("Symbol"))))
        .withColumn("date_sk", F.date_format("NewsDate", "yyyyMMdd").cast("integer"))
        .withColumn("source_sk", F.abs(F.hash(F.concat(F.col("Source"), F.lit("_"), F.col("Category")))))
        .withColumn("bundle_rank", F.row_number().over(window_bundle))
        .withColumn("ImpactStatus", F.when(is_completed, F.lit("COMPLETED")).otherwise(F.lit("PENDING_EVALUATION")))
        .withColumn("SessionType", session_type)
        .withColumn("PrePrice", F.when(is_completed, F.col("Pre_Close")).otherwise(F.lit(None).cast("double")))
        .withColumn("PostPrice", F.when(is_completed, F.col("Post_Close")).otherwise(F.lit(None).cast("double")))
        .withColumn("StockReturnPct", F.when(is_completed, stock_return).otherwise(F.lit(None).cast("double")))
        .withColumn("QQQReturnPct", F.when(is_completed, qqq_return).otherwise(F.lit(None).cast("double")))
        .withColumn("TickerBeta", ticker_beta)
        .withColumn("ExpectedBetaReturnPct", F.when(is_completed, expected_beta_return).otherwise(F.lit(None).cast("double")))
        .withColumn("AlphaReturnPct", F.when(is_completed, raw_alpha).otherwise(F.lit(None).cast("double")))
        .withColumn("TrueAlphaPct", F.when(is_completed, true_alpha).otherwise(F.lit(None).cast("double")))
        .withColumn("IntradayVolatilityPct", F.when(is_completed, intraday_volatility).otherwise(F.lit(None).cast("double")))
        .withColumn("VolumeSurgeRatio", F.when(is_completed, volume_surge).otherwise(F.lit(None).cast("double")))
        .withColumn("CompositeImpactScore", F.when(is_completed, composite_score).otherwise(F.lit(None).cast("double")))
        .withColumn("ReactionPattern", F.when(is_completed, reaction_pattern).otherwise(F.lit("Pending Window")))
        .withColumn("CatalystCategory", final_category)
        .withColumn("NewsGenre", final_genre)
        .withColumn("NewsSentiment", final_sentiment)
        .withColumn("MaterialityTier", final_materiality)
        .withColumn(
            "CausalityVerdict",
            F.when(~is_completed, F.lit("Pending Evaluation"))
            .when((session_type == "OVERNIGHT_GAP") & (F.col("bundle_rank") > 1) & (raw_causality == "Verified Catalyst"), F.lit("Overnight Echo / Secondary"))
            .otherwise(raw_causality)
        )
        .withColumn("DivergenceType", F.when(is_completed, divergence_type).otherwise(F.lit("Pending Evaluation")))
        .select(
            "ArticleId",
            "company_sk",
            "date_sk",
            "source_sk",
            "Symbol",
            "NewsTimestamp",
            "Headline",
            "Summary",
            "ImpactStatus",
            "SessionType",
            "CatalystCategory",
            "NewsGenre",
            "NewsSentiment",
            "MaterialityTier",
            "PrePrice",
            "PostPrice",
            "StockReturnPct",
            "QQQReturnPct",
            "TickerBeta",
            "ExpectedBetaReturnPct",
            "AlphaReturnPct",
            "TrueAlphaPct",
            "IntradayVolatilityPct",
            "VolumeSurgeRatio",
            "CompositeImpactScore",
            "ReactionPattern",
            "CausalityVerdict",
            "DivergenceType",
            "Url"
        )
    )
