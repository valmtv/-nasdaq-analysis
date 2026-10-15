# AI/BI Catalyst Intelligence & Microstructure Dashboard (Lab 06)

A state-of-the-art Databricks **AI/BI Dashboard** and automated **SQL Alert** specification built over the Gold Star Schema (`fact_news_impact`, `fact_stock_prices_5m`, `dim_company`, `dim_date`, `dim_news_source`) in `workspace.default`.

---

## 1. Executive Summary & Dashboard Architecture

The dashboard transforms raw financial news into actionable quantitative signals:
1. **Executive KPI Scorecard**: Real-time summary of top bull/bear catalysts, extreme volume surges, and catalyst conviction.
2. **Media Blitz & Story Cluster Radar**: Natural language narrative clustering grouping wire syndication into core market catalysts (e.g. *Apple CEO Succession*, *NVIDIA Whale Debt Deals*, *AI Power Grid Bottlenecks*).
3. **Macro Market Weather & Fed Sentiment Gauge**: Isolates macro benchmark (`QQQ`) news to give macro market context (Jackson Hole, Fed policy, Treasury yields) without diluting company-level alpha.
4. **Intraday Microstructure Price Action**: 5-minute stock prices overlaid with exact catalyst event pins.
5. **Alpha vs. Volume Surge Matrix**: Scatter plot classifying events into *Sustained Move*, *Reversal/Fade*, and *Flat Noise*.

---

## 2. Step-by-Step UI Creation Guide (Where to Click in Databricks)

1. In the Databricks left navigation bar, click **Dashboards** (or **Workspace** → **Create** → **Dashboard**).
2. Click **Create AI/BI Dashboard** in the top-right corner.
3. Title the dashboard: **NASDAQ Market Radar: Catalyst Intelligence & Alpha Observatory**.
4. In the top toolbar, switch from **Canvas** to the **Data** tab.
5. Add the 5 SQL datasets described in Section 3 below.
6. Switch back to the **Canvas** tab and arrange the visual widgets as described in Section 4.
7. Click **Publish** in the top right to share with your organization.

---

## 3. Base SQL Queries (Data Tab)

> [!TIP]
> Modern Databricks AI/BI Dashboards use **clean SQL without embedded parameter syntax**. You add **Filter Widgets** directly to the Canvas (e.g., Symbol dropdown, Date picker) and Databricks filters the charts dynamically and natively.

### Dataset 1: `kpi_market_summary` (For Executive KPI Cards)
```sql
SELECT 
    COUNT(*) AS TotalEvaluatedCatalysts,
    ROUND(MAX(f.AlphaReturnPct), 2) AS MaxBullAlphaPct,
    ROUND(MIN(f.AlphaReturnPct), 2) AS MaxBearAlphaPct,
    ROUND(MAX(f.VolumeSurgeRatio), 1) AS MaxVolumeSurgeRatio,
    ROUND(100.0 * SUM(CASE WHEN f.ReactionPattern = 'Sustained Move' THEN 1 ELSE 0 END) / COUNT(*), 1) AS SustainedMovePct
FROM fact_news_impact f
WHERE f.ImpactStatus = 'COMPLETED'
  AND f.Symbol != 'QQQ';
```

---

### Dataset 2: `top_catalyst_spotlight` (Detailed Bull & Bear Winner Cards with Real Beta-Adjusted Math)
```sql
WITH ranked AS (
    SELECT 
        c.Symbol,
        c.CompanyName,
        f.Headline,
        f.TrueAlphaPct,
        f.StockReturnPct,
        f.QQQReturnPct,
        f.TickerBeta,
        f.VolumeSurgeRatio,
        ROW_NUMBER() OVER (ORDER BY f.TrueAlphaPct DESC) AS bull_rank,
        ROW_NUMBER() OVER (ORDER BY f.TrueAlphaPct ASC) AS bear_rank
    FROM fact_news_impact f
    JOIN dim_company c ON f.company_sk = c.company_sk
    WHERE f.ImpactStatus = 'COMPLETED' AND f.Symbol != 'QQQ'
)
SELECT 
    'Top Bull Catalyst (+True Alpha)' AS CatalystType,
    Symbol,
    CompanyName,
    Headline,
    TrueAlphaPct AS AlphaReturnPct,
    CONCAT('Stock ', StockReturnPct, '% vs Beta ', TickerBeta, 'x QQQ (True Alpha: +', TrueAlphaPct, '%)') AS ReturnFormulaBreakdown,
    CONCAT(VolumeSurgeRatio, 'x TOD Volume') AS VolumeSurgeDescription
FROM ranked WHERE bull_rank = 1
UNION ALL
SELECT 
    'Top Bear Catalyst (-True Alpha)' AS CatalystType,
    Symbol,
    CompanyName,
    Headline,
    TrueAlphaPct AS AlphaReturnPct,
    CONCAT('Stock ', StockReturnPct, '% vs Beta ', TickerBeta, 'x QQQ (True Alpha: ', TrueAlphaPct, '%)') AS ReturnFormulaBreakdown,
    CONCAT(VolumeSurgeRatio, 'x TOD Volume') AS VolumeSurgeDescription
FROM ranked WHERE bear_rank = 1;
```

---

### Dataset 3: `news_signal_vs_noise_funnel` (Quantitative Causality Verification)
```sql
SELECT 
    COUNT(*) AS TotalEvaluatedNews,
    SUM(CASE WHEN f.CausalityVerdict = 'Verified Catalyst' THEN 1 ELSE 0 END) AS VerifiedMovers,
    SUM(CASE WHEN f.CausalityVerdict = 'Flat Noise' THEN 1 ELSE 0 END) AS IgnoredNoise,
    SUM(CASE WHEN f.CausalityVerdict = 'Market Fade / False Alarm' THEN 1 ELSE 0 END) AS FakeoutSpikes,
    SUM(CASE WHEN f.CausalityVerdict = 'Post-Hoc Explainer' THEN 1 ELSE 0 END) AS PostHocRecaps,
    SUM(CASE WHEN f.CausalityVerdict = 'Beta Drift / Macro Mover' THEN 1 ELSE 0 END) AS BetaDriftMovers,
    CONCAT(ROUND(100.0 * SUM(CASE WHEN f.CausalityVerdict = 'Verified Catalyst' THEN 1 ELSE 0 END) / COUNT(*), 0), '%') AS VerifiedMoverPct,
    CONCAT(ROUND(100.0 * SUM(CASE WHEN f.CausalityVerdict IN ('Flat Noise', 'Post-Hoc Explainer') THEN 1 ELSE 0 END) / COUNT(*), 0), '%') AS FilteredNoisePct
FROM fact_news_impact f
WHERE f.ImpactStatus = 'COMPLETED'
  AND f.Symbol != 'QQQ';
```

---

### Dataset 4: `media_story_clusters` (Standard 7-Category Financial Taxonomy)
```sql
SELECT 
    f.CatalystCategory AS StoryCluster,
    COUNT(f.ArticleId) AS MediaBuzzCount,
    CONCAT_WS(', ', COLLECT_SET(c.Symbol)) AS ImpactedTickers,
    ROUND(AVG(f.TrueAlphaPct), 3) AS AvgAlphaPct,
    ROUND(MAX(ABS(f.TrueAlphaPct)), 3) AS PeakAlphaPct,
    ROUND(AVG(f.VolumeSurgeRatio), 2) AS AvgVolumeSurgeRatio,
    ROUND(AVG(f.CompositeImpactScore), 1) AS AvgCompositeScore
FROM fact_news_impact f
JOIN dim_company c ON f.company_sk = c.company_sk
WHERE f.ImpactStatus = 'COMPLETED'
  AND f.Symbol != 'QQQ'
GROUP BY f.CatalystCategory
ORDER BY MediaBuzzCount DESC, PeakAlphaPct DESC;
```

---

### Dataset 5: `macro_weather_gauge` (Dedicated QQQ Benchmark & Macro Mood)
```sql
SELECT 
    f.NewsTimestamp,
    f.Headline,
    f.Summary,
    f.StockReturnPct AS QQQReturnPct,
    f.VolumeSurgeRatio,
    f.ImpactStatus,
    f.Url
FROM fact_news_impact f
WHERE f.Symbol = 'QQQ'
  AND f.ImpactStatus = 'COMPLETED'
ORDER BY f.NewsTimestamp DESC;
```

---

### Dataset 6: `company_catalyst_leaderboard` (Friendly Names & Scatter Matrix)
```sql
SELECT 
    c.Symbol,
    c.CompanyName,
    c.Sector,
    c.Beta AS TickerBeta,
    f.NewsTimestamp,
    f.Headline,
    f.CatalystCategory,
    f.NewsGenre,
    f.NewsSentiment,
    f.TrueAlphaPct,
    f.AlphaReturnPct AS RawAlphaPct,
    f.StockReturnPct,
    f.ExpectedBetaReturnPct,
    f.VolumeSurgeRatio AS VolumeSurgeMultiplier,
    f.CausalityVerdict,
    f.DivergenceType,
    f.CompositeImpactScore,
    f.ImpactStatus,
    f.Url
FROM fact_news_impact f
JOIN dim_company c ON f.company_sk = c.company_sk
WHERE f.Symbol != 'QQQ'
ORDER BY ABS(f.TrueAlphaPct) DESC;
```

---

### Dataset 7: `stock_price_history_5m` (Intraday Chart with Catalyst Overlay)
```sql
SELECT 
    p.Symbol,
    p.PriceTimestamp,
    p.Open,
    p.High,
    p.Low,
    p.Close,
    p.Volume,
    n.Headline,
    n.AlphaReturnPct,
    n.CompositeImpactScore,
    n.ReactionPattern
FROM fact_stock_prices_5m p
LEFT JOIN fact_news_impact n 
  ON p.Symbol = n.Symbol 
 AND p.PriceTimestamp >= n.NewsTimestamp 
 AND p.PriceTimestamp <= n.NewsTimestamp + INTERVAL 5 MINUTES
ORDER BY p.PriceTimestamp ASC;
```

---

## 4. Canvas Layout & Visualizations

Switch to the **Canvas** tab and assemble the visual dashboard layout:

```
+---------------------------------------------------------------------------------------------------+
|  [FILTER: Symbol (Dropdown)]   [FILTER: ReactionPattern (Dropdown)]   [FILTER: ImpactStatus]      |
+---------------------------------------------------------------------------------------------------+
|  [CARD: Total Catalysts]  [CARD: Max Bull Alpha]  [CARD: Max Bear Alpha]  [CARD: Peak Surge Ratio] |
+---------------------------------------------------------------------------------------------------+
|  [WIDGET 1: Media Blitz & Story Clusters]         |  [WIDGET 2: Macro Market Weather (QQQ)]       |
|  (Bar/Table: Buzz Count vs Peak Alpha Impact)     |  (Macro Sentiment & Fed News Feed)            |
+---------------------------------------------------------------------------------------------------+
|  [WIDGET 3: Intraday Stock Price Action Overlay]  |  [WIDGET 4: Microstructure Impact Matrix]     |
|  (5m Price Line Chart with Catalyst Event Points) |  (Scatter: Alpha % vs Volume Surge Multiplier)|
+---------------------------------------------------------------------------------------------------+
|  [WIDGET 5: Top Alpha Movers Leaderboard Table]                                                   |
|  (Symbol, Company, Headline, Alpha %, Stock Return %, Surge Ratio, Pattern, Composite Score)      |
+---------------------------------------------------------------------------------------------------+
```

### 1. Global Filter Controls (Top Canvas Bar)
* **Symbol Filter**: Dropdown from `company_catalyst_leaderboard.Symbol` (Multi-select enabled).
* **Reaction Pattern Filter**: Dropdown from `company_catalyst_leaderboard.ReactionPattern` (`Sustained Move`, `Reversal/Fade`, `Flat Noise`).
* **Impact Status Filter**: Single-select dropdown (`COMPLETED`, `PENDING_EVALUATION`).

### 2. Row 1: Executive KPI Scorecard Cards
* **Card 1 (Total Catalysts)**: Metric `TotalEvaluatedCatalysts` from `kpi_market_summary`.
* **Card 2 (Top Bull Catalyst)**: Metric `MaxBullAlphaPct` (Format: `+0.00%`, Green).
* **Card 3 (Top Bear Catalyst)**: Metric `MaxBearAlphaPct` (Format: `-0.00%`, Red).
* **Card 4 (Max Volume Surge)**: Metric `MaxVolumeSurgeRatio` (Format: `0.0x Baseline`).
* **Card 5 (Catalyst Conviction)**: Metric `SustainedMovePct` (Format: `0.0% Sustained Moves`).

### 3. Row 2: Narrative Intelligence & Macro Weather
* **Widget 1: Media Blitz & Story Clusters (Bar Chart or Table)**:
  * **Dataset**: `media_story_clusters`
  * **X-axis / Categories**: `StoryCluster`
  * **Y-axis / Metric**: `MediaBuzzCount`
  * **Color / Secondary**: `PeakAlphaPct`
  * *Insight*: Immediately shows if the market is fixated on CEO succession, energy constraints, or hyperscaler deals.
* **Widget 2: Macro Market Weather Feed (Table / List)**:
  * **Dataset**: `macro_weather_gauge`
  * **Columns**: `NewsTimestamp`, `Headline`, `QQQReturnPct`, `VolumeSurgeRatio`
  * *Insight*: Keeps broad market Fed/Treasury news accessible in one click without diluting company-level alpha.

### 4. Row 3: Intraday Microstructure & Alpha Matrix
* **Widget 3: 5-Minute Price Action with Catalyst Events (Line Chart)**:
  * **Dataset**: `stock_price_history_5m`
  * **X-axis**: `PriceTimestamp`
  * **Y-axis**: `Close`
  * **Series / Group By**: `Symbol`
  * **Tooltips**: `Headline`, `AlphaReturnPct`, `ReactionPattern`
* **Widget 4: Alpha Return vs Volume Surge Matrix (Scatter Chart)**:
  * **Dataset**: `company_catalyst_leaderboard`
  * **X-axis**: `VolumeSurgeRatio`
  * **Y-axis**: `AlphaReturnPct`
  * **Color / Group By**: `ReactionPattern`
  * **Point Size**: `ABS(CompositeImpactScore)`
  * **Tooltips**: `Symbol`, `Headline`, `VolumeSurgeRatio`, `AlphaReturnPct`

### 5. Row 4: Top Alpha Movers Leaderboard
* **Widget 5: Company Catalyst Leaderboard (Table)**:
  * **Dataset**: `company_catalyst_leaderboard`
  * **Columns**: `Symbol`, `CompanyName`, `Headline`, `AlphaReturnPct`, `StockReturnPct`, `VolumeSurgeRatio`, `ReactionPattern`, `CompositeImpactScore`
  * **Conditional Formatting**: `AlphaReturnPct` > 0 Green, < 0 Red.

---

## 5. Automated Data Volume & Microstructure Alerts

### Alert 1: Extreme Catalyst Alpha Breakout Alert
* **Trigger Condition**: Any evaluated news catalyst generates $> \pm 1.5\%$ alpha within 30 minutes.
* **Query**:
  ```sql
  SELECT 
      Symbol, 
      Headline, 
      AlphaReturnPct, 
      VolumeSurgeRatio, 
      NewsTimestamp
  FROM fact_news_impact
  WHERE ImpactStatus = 'COMPLETED'
    AND ABS(AlphaReturnPct) >= 1.5
    AND NewsTimestamp >= current_timestamp() - INTERVAL 1 DAY
  ORDER BY ABS(AlphaReturnPct) DESC
  LIMIT 1;
  ```
* **Alert Setup**: In Databricks SQL → **Alerts** → **Create Alert** → Trigger when row count $> 0$.

---

### Alert 2: Pipeline Ingestion Drop Alert (Missing Data Detection)
* **Trigger Condition**: Ingested ticks drop by $> 50\%$ compared to previous rolling period.
* **Query**:
  ```sql
  WITH hourly_counts AS (
      SELECT 
          date_trunc('hour', PriceTimestamp) AS price_hour,
          COUNT(*) AS tick_count
      FROM fact_stock_prices_5m
      WHERE PriceTimestamp >= current_timestamp() - INTERVAL 4 HOURS
      GROUP BY 1
  ),
  ranked_hours AS (
      SELECT 
          price_hour,
          tick_count,
          LAG(tick_count, 1) OVER (ORDER BY price_hour ASC) AS prev_hour_count
      FROM hourly_counts
  )
  SELECT 
      price_hour,
      tick_count,
      prev_hour_count,
      CASE 
          WHEN prev_hour_count IS NOT NULL AND tick_count < (0.5 * prev_hour_count) 
          THEN 1 
          ELSE 0 
      END AS is_volume_drop
  FROM ranked_hours
  ORDER BY price_hour DESC
  LIMIT 1;
  ```
* **Alert Setup**: Trigger when `is_volume_drop == 1`.
