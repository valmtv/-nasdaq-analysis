# NASDAQ Market Radar: Medallion Lakehouse

Production-grade Medallion Lakehouse pipeline analyzing 5-minute stock prices and news impact across NASDAQ tickers (`AAPL`, `NVDA`, `MSFT`, `AMZN`, `TSLA`, `QQQ`). Governed by Unity Catalog.

```
+-----------------------------------------------------------------------------------------+
|                                       01_landing                                        |
|     yfinance 5m Prices (CSV)                Finnhub News Articles (JSON)                |
+------------------------------------+----------------------------------------------------+
                                     | (Auto Loader Streaming Append)
                                     v
+-----------------------------------------------------------------------------------------+
|                                 02_bronze_ingestion.py                                  |
|     bronze_nasdaq_price_raw                 bronze_finnhub_news_raw                     |
+------------------------------------+----------------------------------------------------+
                                     | (DLT Expectations & Quarantine Routing)
                                     v
+-----------------------------------------------------------------------------------------+
|                              03_silver_transformations.py                               |
|     silver_nasdaq_price_5m                  silver_finnhub_news                         |
|     [PK: Symbol + Timestamp]               [PK: ArticleId]                             |
|     --> silver_price_quarantine             --> silver_news_quarantine                  |
+------------------------------------+----------------------------------------------------+
                                     | (Star Schema Modeling & Microstructure Event Window)
                                     v
+-----------------------------------------------------------------------------------------+
|                                 04_gold_star_schema.py                                  |
|   DIMENSIONS:  dim_company | dim_date | dim_news_source                                 |
|   FACTS:       fact_stock_prices_5m (5-minute price ticks for chart overlays)           |
|                fact_news_impact (Alpha, Volatility, Surge, Trajectory, Pending states)  |
+-----------------------------------------------------------------------------------------+
                                     |
                                     v
+-----------------------------------------------------------------------------------------+
|                       05_governance_rls_cls.sql (Unity Catalog)                         |
|   Row-Level Security (RLS) hides pending news; Column-Level Security (CLS) masks scores |
+-----------------------------------------------------------------------------------------+
```

---

## File Structure

```
nasdaq-analysis/
├── databricks.yml                     # DAB configuration & orchestrated workflow job
├── 00_setup_lakehouse.ipynb           # Unity Catalog volume & schema setup (run once)
├── 01_landing/                        # External API fetchers (day-by-day incremental)
│   ├── 01d_landing_nasdaq_price.ipynb # 5m price bars from yfinance
│   └── 01e_landing_finnhub_news.ipynb # News articles by unique ID from Finnhub (with seed fallback)
├── 02_bronze_ingestion.py             # Lakeflow: Auto Loader streaming bronze tables (explicit schema)
├── 03_silver_transformations.py       # Lakeflow: DLT expectations & quarantine routing
├── 04_gold_star_schema.py             # Lakeflow: Dual-Fact Star Schema (facts + dimensions)
├── 05_governance_rls_cls.sql          # Unity Catalog RLS (pending filter) & CLS (score mask)
├── 06_dashboard_and_alerts.md         # AI/BI dashboard layout & volume drop alert spec
├── 07_maintenance_vacuum.ipynb        # Z-Order optimization and stale file vacuuming
├── ISSUES_AND_SOLUTIONS.md            # Compact log of fixed architectural issues
├── nasdaq_transforms/                 # Pure transformation library for CI/CD unit tests
│   ├── prices.py, news.py, impact.py, dimensions.py
└── tests/                             # 14 automated unit tests & reconciliation suite
    ├── test_price_transforms.py, test_news_impact.py, test_dimensions.py,
    ├── test_data_quality_gates.py, reconciliation_checks.py
```

---

## Deployment & Execution

### 1. Deploy via Databricks Asset Bundle (Recommended)
Deploys the pipeline, workflow job, and files to your Databricks Free workspace:
```bash
# 1. Validate bundle configuration
databricks bundle validate -t dev_free

# 2. Deploy bundle to cloud workspace
databricks bundle deploy -t dev_free

# 3. Trigger the orchestrated workflow (Landing -> Lakeflow -> Maintenance)
databricks bundle run -t dev_free nasdaq_daily_workflow
```

### 2. Run via Databricks Workflows UI
1. In the workspace UI, navigate to **Workflows**.
2. Open **`[dev_free] NASDAQ Daily End-to-End Workflow`**.
3. Click **Run Now**.
4. Once completed, open **Delta Live Tables** to inspect the pipeline DAG, table counts, and expectation metrics.

---

## Local Unit Testing
```bash
python3 -m unittest discover tests -v
# Ran 14 tests in 0.001s -> OK
```
