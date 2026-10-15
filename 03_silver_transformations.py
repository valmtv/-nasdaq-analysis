"""
Silver Layer Cleansing & Validation Module (Lakeflow Declarative Pipeline)
--------------------------------------------------------------------------
Enforces data quality rules using DLT expectations, deduplicates streaming ticks,
and routes rejected records to quarantine tables for auditing.
"""

try:
    import dlt
except ImportError:
    import pyspark.pipelines as dlt

from pyspark.sql import functions as F


# -------------------------------------------------------------------------
# Price Stream & Quarantine
# -------------------------------------------------------------------------

@dlt.table(
    name="silver_price_quarantine",
    comment="Quarantined raw price records failing quality constraints (negative prices/volumes or null keys)",
    table_properties={"quality": "quarantine"}
)
def silver_price_quarantine():
    df_raw = dlt.read_stream("bronze_nasdaq_price_raw")
    cols = df_raw.columns
    ts_col = "PriceTimestamp" if "PriceTimestamp" in cols else ("Datetime" if "Datetime" in cols else "Date")

    return (
        df_raw
        .withColumn("ParsedTimestamp", F.to_timestamp(F.col(ts_col)))
        .withColumn("ParsedClose", F.col("Close").cast("double"))
        .withColumn("ParsedVolume", F.col("Volume").cast("long"))
        .filter(
            F.col("Symbol").isNull() |
            (F.trim(F.col("Symbol")) == "") |
            F.col("ParsedTimestamp").isNull() |
            F.col("ParsedClose").isNull() |
            (F.col("ParsedClose") <= 0.0) |
            F.col("ParsedVolume").isNull() |
            (F.col("ParsedVolume") < 0)
        )
        .withColumn("quarantine_reason", F.when(
            F.col("Symbol").isNull() | (F.trim(F.col("Symbol")) == ""), F.lit("Missing or Empty Symbol")
        ).when(
            F.col("ParsedTimestamp").isNull(), F.lit("Invalid PriceTimestamp format")
        ).when(
            F.col("ParsedClose").isNull() | (F.col("ParsedClose") <= 0.0), F.lit("Invalid Close Price (<= 0)")
        ).otherwise(F.lit("Negative or Null Volume")))
        .withColumn("quarantined_at", F.current_timestamp())
    )


@dlt.table(
    name="silver_nasdaq_price_5m",
    comment="Cleansed, validated, and deduplicated 5-minute stock price bars",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect_or_fail("valid_symbol_key", "Symbol IS NOT NULL AND Symbol != ''")
@dlt.expect_or_drop("valid_price_bounds", "Close > 0.0 AND Volume >= 0")
@dlt.expect("valid_price_timestamp", "PriceTimestamp IS NOT NULL AND PriceTimestamp <= current_timestamp()")
def silver_nasdaq_price_5m():
    df_raw = dlt.read_stream("bronze_nasdaq_price_raw")
    cols = df_raw.columns
    ts_col = "PriceTimestamp" if "PriceTimestamp" in cols else ("Datetime" if "Datetime" in cols else "Date")

    return (
        df_raw
        .withColumn("PriceTimestamp", F.to_timestamp(F.col(ts_col)))
        .withColumn("TradeDate", F.to_date(F.col("PriceTimestamp")))
        .withColumn("Symbol", F.upper(F.trim(F.col("Symbol"))))
        .withColumn("Open", F.col("Open").cast("double"))
        .withColumn("High", F.col("High").cast("double"))
        .withColumn("Low", F.col("Low").cast("double"))
        .withColumn("Close", F.col("Close").cast("double"))
        .withColumn("Volume", F.col("Volume").cast("long"))
        .select(
            "Symbol",
            "PriceTimestamp",
            "TradeDate",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "_source_file",
            "_ingestion_timestamp"
        )
        .filter(
            F.col("Symbol").isNotNull() &
            (F.col("Symbol") != "") &
            F.col("PriceTimestamp").isNotNull() &
            F.col("Close").isNotNull() &
            (F.col("Close") > 0.0) &
            F.col("Volume").isNotNull() &
            (F.col("Volume") >= 0)
        )
    )


# -------------------------------------------------------------------------
# News Stream & Quarantine
# -------------------------------------------------------------------------

@dlt.table(
    name="silver_news_quarantine",
    comment="Quarantined news records failing required key or text validations",
    table_properties={"quality": "quarantine"}
)
def silver_news_quarantine():
    df_raw = dlt.read_stream("bronze_finnhub_news_raw")

    return (
        df_raw
        .withColumn("ArticleId", F.col("id").cast("string"))
        .withColumn("Headline", F.trim(F.col("headline")))
        .withColumn("ParsedTimestamp", F.from_unixtime(F.col("datetime").cast("long")).cast("timestamp"))
        .filter(
            F.col("ArticleId").isNull() |
            (F.trim(F.col("ArticleId")) == "") |
            F.col("Headline").isNull() |
            (F.trim(F.col("Headline")) == "") |
            F.col("ParsedTimestamp").isNull()
        )
        .withColumn("quarantine_reason", F.when(
            F.col("ArticleId").isNull() | (F.trim(F.col("ArticleId")) == ""), F.lit("Missing or Empty ArticleId")
        ).when(
            F.col("Headline").isNull() | (F.trim(F.col("Headline")) == ""), F.lit("Missing or Empty Headline")
        ).otherwise(F.lit("Unparseable Unix datetime timestamp")))
        .withColumn("quarantined_at", F.current_timestamp())
    )


@dlt.table(
    name="silver_finnhub_news",
    comment="Cleansed, normalized, and deduplicated Finnhub market and company news",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect_or_fail("valid_article_id", "ArticleId IS NOT NULL AND ArticleId != ''")
@dlt.expect_or_drop("valid_headline", "Headline IS NOT NULL AND Headline != ''")
@dlt.expect("valid_news_timestamp", "NewsTimestamp IS NOT NULL")
def silver_finnhub_news():
    df_raw = dlt.read_stream("bronze_finnhub_news_raw")

    return (
        df_raw
        .withColumn("ArticleId", F.col("id").cast("string"))
        .withColumn("NewsTimestamp", F.from_unixtime(F.col("datetime").cast("long")).cast("timestamp"))
        .withColumn("NewsDate", F.to_date(F.col("NewsTimestamp")))
        .withColumn("Headline", F.trim(F.col("headline")))
        .withColumn("Summary", F.trim(F.col("summary")))
        .withColumn(
            "Symbol",
            F.when(
                (F.col("related").isNull()) | (F.trim(F.col("related")) == ""),
                F.lit("GENERAL")
            ).otherwise(F.upper(F.trim(F.col("related"))))
        )
        .withColumn("Source", F.coalesce(F.trim(F.col("source")), F.lit("Unknown")))
        .withColumn("Category", F.coalesce(F.trim(F.col("category")), F.lit("general")))
        .withColumn("Url", F.coalesce(F.trim(F.col("url")), F.lit("")))
        .withColumn(
            "CatalystCategory",
            F.when(F.lower(F.col("Headline")).rlike(r"earnings|q[1-4]|revenue|profit|eps|guidance|outlook|financial results|beat|miss|margin|dividend|buyback"), F.lit("Earnings & Forward Guidance"))
            .when(F.lower(F.col("Headline")).rlike(r"acqui|merger|buyout|takeover|debt deal|whale|iren|capex|partnership|funding|investment"), F.lit("Mergers, Acquisitions & Capex"))
            .when(F.lower(F.col("Headline")).rlike(r"regulat|antitrust|doj|ftc|sec|lawsuit|sued|patent|probe|tariff|monopoly|subpoena|penalty"), F.lit("Regulatory, Antitrust & Legal"))
            .when(F.lower(F.col("Headline")).rlike(r"claude|anthropic|openai|copilot|blackwell|chip|gpu|semiconductor|launch|vision pro|robotaxi|grid|power|bloom energy|hardware"), F.lit("Product, AI & Hardware Launches"))
            .when(F.lower(F.col("Headline")).rlike(r"ceo|cfo|chief executive|tim cook|ternus|succession|board|resign|activist|leadership"), F.lit("Executive & Board Governance"))
            .when(F.lower(F.col("Headline")).rlike(r"fed|federal reserve|powell|rate cut|rate hike|inflation|cpi|treasury|yield|jobs report|fomc"), F.lit("Macroeconomic & Rate Policy"))
            .otherwise(F.lit("Market Commentary & Routine Churn"))
        )
        .withColumn(
            "NewsGenre",
            F.when(F.lower(F.col("Headline")).rlike(r"why .* (falling|dropping|rising|plunging|down|up) today|midday movers|market wrap|stocks making biggest moves|market recap"), F.lit("Post-Hoc Recap"))
            .when(F.lower(F.col("Headline")).rlike(r"zacks rank|motley fool|3 stocks to buy|should you buy|etf holdings update"), F.lit("Routine Churn"))
            .otherwise(F.lit("Breaking Catalyst"))
        )
        .withColumn(
            "NewsSentiment",
            F.when(F.lower(F.col("Headline")).rlike(r"surge|jump|beat|exceed|record|upgrade|rally|growth|boost|gain|soar|strong|outperform|approved"), F.lit("Bullish"))
            .when(F.lower(F.col("Headline")).rlike(r"plunge|drop|slump|miss|downgrade|fall|lawsuit|sued|probe|warning|cut|tariff|antitrust|weak|disappoint"), F.lit("Bearish"))
            .otherwise(F.lit("Neutral"))
        )
        .withColumn(
            "MaterialityTier",
            F.when(F.col("NewsGenre") == "Routine Churn", F.lit(4))
            .when(F.col("NewsGenre") == "Post-Hoc Recap", F.lit(3))
            .when(F.col("CatalystCategory").isin("Earnings & Forward Guidance", "Mergers, Acquisitions & Capex", "Regulatory, Antitrust & Legal"), F.lit(1))
            .when(F.col("CatalystCategory").isin("Product, AI & Hardware Launches", "Executive & Board Governance"), F.lit(2))
            .when(F.col("CatalystCategory") == "Macroeconomic & Rate Policy", F.lit(3))
            .otherwise(F.lit(4))
        )
        .select(
            "ArticleId",
            "Symbol",
            "NewsDate",
            "NewsTimestamp",
            "Headline",
            "Summary",
            "Source",
            "Category",
            "CatalystCategory",
            "NewsGenre",
            "NewsSentiment",
            "MaterialityTier",
            "Url",
            "_source_file",
            "_ingestion_timestamp"
        )
        .filter(
            F.col("ArticleId").isNotNull() &
            (F.col("ArticleId") != "") &
            F.col("Headline").isNotNull() &
            (F.col("Headline") != "") &
            F.col("NewsTimestamp").isNotNull()
        )
    )
