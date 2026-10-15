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

### Dataset 2: `top_catalyst_spotlight` (Detailed Bull & Bear Winner Cards with Real Math)
```sql
WITH ranked AS (
    SELECT 
        c.Symbol,
        c.CompanyName,
        f.Headline,
        f.AlphaReturnPct,
        f.StockReturnPct,
        f.QQQReturnPct,
        f.VolumeSurgeRatio,
        ROW_NUMBER() OVER (ORDER BY f.AlphaReturnPct DESC) AS bull_rank,
        ROW_NUMBER() OVER (ORDER BY f.AlphaReturnPct ASC) AS bear_rank
    FROM fact_news_impact f
    JOIN dim_company c ON f.company_sk = c.company_sk
    WHERE f.ImpactStatus = 'COMPLETED' AND f.Symbol != 'QQQ'
)
SELECT 
    'Top Bull Catalyst (+Alpha)' AS CatalystType,
    Symbol,
    CompanyName,
    Headline,
    AlphaReturnPct,
    CONCAT('Stock +', StockReturnPct, '% vs QQQ +', QQQReturnPct, '% (Outperformed by +', AlphaReturnPct, '%)') AS ReturnFormulaBreakdown,
    CONCAT(VolumeSurgeRatio, 'x Normal Volume') AS VolumeSurgeDescription
FROM ranked WHERE bull_rank = 1
UNION ALL
SELECT 
    'Top Bear Catalyst (-Alpha)' AS CatalystType,
    Symbol,
    CompanyName,
    Headline,
    AlphaReturnPct,
    CONCAT('Stock ', StockReturnPct, '% vs QQQ ', QQQReturnPct, '% (Underperformed by ', AlphaReturnPct, '%)') AS ReturnFormulaBreakdown,
    CONCAT(VolumeSurgeRatio, 'x Normal Volume') AS VolumeSurgeDescription
FROM ranked WHERE bear_rank = 1;
```

---

### Dataset 3: `news_signal_vs_noise_funnel` (How Much Noise Was Filtered Out)
```sql
SELECT 
    COUNT(*) AS TotalEvaluatedNews,
    SUM(CASE WHEN f.ReactionPattern = 'Sustained Move' THEN 1 ELSE 0 END) AS RealMarketMovers,
    SUM(CASE WHEN f.ReactionPattern = 'Flat Noise' THEN 1 ELSE 0 END) AS IgnoredNoise,
    SUM(CASE WHEN f.ReactionPattern = 'Reversal/Fade' THEN 1 ELSE 0 END) AS FakeoutSpikes,
    CONCAT(ROUND(100.0 * SUM(CASE WHEN f.ReactionPattern = 'Sustained Move' THEN 1 ELSE 0 END) / COUNT(*), 0), '%') AS RealMoverPct,
    CONCAT(ROUND(100.0 * SUM(CASE WHEN f.ReactionPattern = 'Flat Noise' THEN 1 ELSE 0 END) / COUNT(*), 0), '%') AS IgnoredNoisePct
FROM fact_news_impact f
WHERE f.ImpactStatus = 'COMPLETED'
  AND f.Symbol != 'QQQ';
```

---

### Dataset 4: `media_story_clusters` (Narrative Intelligence & Echo Chamber)
```sql
WITH categorized_news AS (
    SELECT 
        f.ArticleId,
        c.Symbol,
        c.CompanyName,
        f.Headline,
        f.NewsTimestamp,
        f.AlphaReturnPct,
        f.VolumeSurgeRatio,
        f.CompositeImpactScore,
        f.ReactionPattern,
        CASE 
            WHEN LOWER(f.Headline) LIKE '%ceo%' OR LOWER(f.Headline) LIKE '%ternus%' OR LOWER(f.Headline) LIKE '%tim cook%'
                THEN 'Apple Leadership & CEO Succession'
            WHEN LOWER(f.Headline) LIKE '%power%' OR LOWER(f.Headline) LIKE '%grid%' OR LOWER(f.Headline) LIKE '%emergency%' OR LOWER(f.Headline) LIKE '%bloom energy%'
                THEN 'AI Power Grid & Energy Bottlenecks'
            WHEN LOWER(f.Headline) LIKE '%whale%' OR LOWER(f.Headline) LIKE '%iren%' OR LOWER(f.Headline) LIKE '%debt deal%' OR LOWER(f.Headline) LIKE '%blackwell%'
                THEN 'NVIDIA Hyperscaler & Whale Flows'
            WHEN LOWER(f.Headline) LIKE '%claude%' OR LOWER(f.Headline) LIKE '%anthropic%' OR LOWER(f.Headline) LIKE '%openai%' OR LOWER(f.Headline) LIKE '%copilot%'
                THEN 'LLM & Foundation Model Wars'
            WHEN LOWER(f.Headline) LIKE '%earnings%' OR LOWER(f.Headline) LIKE '%q2%' OR LOWER(f.Headline) LIKE '%q3%'
                THEN 'Quarterly Financial Results & Guidance'
            ELSE 'Corporate & Industry Developments'
        END AS StoryCluster
    FROM fact_news_impact f
    JOIN dim_company c ON f.company_sk = c.company_sk
    WHERE f.ImpactStatus = 'COMPLETED'
      AND f.Symbol != 'QQQ'
)
SELECT 
    StoryCluster,
    COUNT(ArticleId) AS MediaBuzzCount,
    CONCAT_WS(', ', COLLECT_SET(Symbol)) AS ImpactedTickers,
    ROUND(AVG(AlphaReturnPct), 3) AS AvgAlphaPct,
    ROUND(MAX(ABS(AlphaReturnPct)), 3) AS PeakAlphaPct,
    ROUND(AVG(VolumeSurgeRatio), 2) AS AvgVolumeSurgeRatio,
    ROUND(AVG(CompositeImpactScore), 1) AS AvgCompositeScore
FROM categorized_news
GROUP BY StoryCluster
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
    f.NewsTimestamp,
    f.Headline,
    f.AlphaReturnPct,
    f.StockReturnPct,
    f.QQQReturnPct,
    f.VolumeSurgeRatio AS VolumeSurgeMultiplier,
    CASE 
        WHEN f.ReactionPattern = 'Sustained Move' THEN 'Real Trend (High Impact)'
        WHEN f.ReactionPattern = 'Reversal/Fade' THEN 'Fakeout (Quick Spike)'
        ELSE 'Market Ignored (Noise)'
    END AS MarketReaction,
    f.CompositeImpactScore,
    f.ImpactStatus,
    f.Url
FROM fact_news_impact f
JOIN dim_company c ON f.company_sk = c.company_sk
WHERE f.Symbol != 'QQQ'
ORDER BY ABS(f.AlphaReturnPct) DESC;
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
