"""
Unit Tests: News Semantic Intelligence & Financial Taxonomy Engine
-------------------------------------------------------------------
Validates:
1. 7-category taxonomy classification against real financial headlines.
2. News genre detection: Breaking vs Post-Hoc Recap vs Routine Churn.
3. Financial sentiment and confidence scoring.
4. Materiality tiers and priority weighting.
"""

import unittest
from nasdaq_transforms.semantics import (
    classify_catalyst_category,
    classify_news_genre,
    analyze_financial_sentiment,
    get_materiality_tier,
    enrich_news_article
)


class TestNewsSemantics(unittest.TestCase):

    def test_catalyst_taxonomy_classification(self):
        """Verifies accurate assignment across the 7 financial categories."""
        # 1. Earnings & Forward Guidance
        self.assertEqual(
            classify_catalyst_category("Amazon beats Q2 revenue expectations with cloud surge"),
            "Earnings & Forward Guidance"
        )
        self.assertEqual(
            classify_catalyst_category("Microsoft issues weak Q3 margin guidance on datacenter spending"),
            "Earnings & Forward Guidance"
        )

        # 2. Mergers, Acquisitions & Capex
        self.assertEqual(
            classify_catalyst_category("NVIDIA backs billion dollar debt deal and datacenter funding with IREN"),
            "Mergers, Acquisitions & Capex"
        )

        # 3. Regulatory, Antitrust & Legal
        self.assertEqual(
            classify_catalyst_category("DOJ initiates antitrust investigation into Google search monopoly"),
            "Regulatory, Antitrust & Legal"
        )
        self.assertEqual(
            classify_catalyst_category("EU hits Apple with 500M fine in music streaming probe"),
            "Regulatory, Antitrust & Legal"
        )

        # 4. Product, AI & Hardware Launches
        self.assertEqual(
            classify_catalyst_category("Anthropic and OpenAI unveil next-generation frontier reasoning models"),
            "Product, AI & Hardware Launches"
        )
        self.assertEqual(
            classify_catalyst_category("Tesla reveals Robotaxi autonomous prototype event date"),
            "Product, AI & Hardware Launches"
        )
        self.assertEqual(
            classify_catalyst_category("AI power grid bottleneck triggers regional energy emergency"),
            "Product, AI & Hardware Launches"
        )

        # 5. Executive & Board Governance
        self.assertEqual(
            classify_catalyst_category("Tim Cook and John Ternus discuss Apple leadership and CEO succession"),
            "Executive & Board Governance"
        )

        # 6. Macroeconomic & Rate Policy
        self.assertEqual(
            classify_catalyst_category("Federal Reserve Chair Jerome Powell signals rate cut at Jackson Hole"),
            "Macroeconomic & Rate Policy"
        )

        # 7. Routine Churn
        self.assertEqual(
            classify_catalyst_category("3 Tech Stocks To Watch This Afternoon"),
            "Market Commentary & Routine Churn"
        )

    def test_news_genre_detection(self):
        """Differentiates Breaking News from Post-Hoc Recaps and Routine Churn."""
        # Post-Hoc Recaps
        self.assertEqual(
            classify_news_genre("Why Tesla Is Falling Today After Missing Deliveries"),
            "Post-Hoc Recap"
        )
        self.assertEqual(
            classify_news_genre("Midday Market Wrap: Tech Stocks Plunge as Yields Rise"),
            "Post-Hoc Recap"
        )

        # Routine Churn
        self.assertEqual(
            classify_news_genre("Zacks Rank #1 Strong Buys: Apple and Microsoft"),
            "Routine Churn"
        )
        self.assertEqual(
            classify_news_genre("Should You Buy NVDA Stock Right Now? The Motley Fool"),
            "Routine Churn"
        )

        # Breaking Catalyst
        self.assertEqual(
            classify_news_genre("NVIDIA launches Blackwell Ultra AI accelerators"),
            "Breaking Catalyst"
        )
        self.assertEqual(
            classify_news_genre("Apple acquires AI vision startup for $1.2B"),
            "Breaking Catalyst"
        )

    def test_financial_sentiment_analysis(self):
        """Verifies Bullish, Bearish, and Neutral classification with confidence."""
        bull_sent, bull_conf = analyze_financial_sentiment("NVIDIA surges as earnings beat record expectations")
        self.assertEqual(bull_sent, "Bullish")
        self.assertGreater(bull_conf, 0.6)

        bear_sent, bear_conf = analyze_financial_sentiment("Tesla stock slumps as DOJ probes Autopilot safety warnings")
        self.assertEqual(bear_sent, "Bearish")
        self.assertGreater(bear_conf, 0.6)

        neutral_sent, _ = analyze_financial_sentiment("Apple schedules annual shareholder meeting for March 15")
        self.assertEqual(neutral_sent, "Neutral")

    def test_materiality_tiers(self):
        """Ensures corporate filings and regulatory events outrank routine commentary."""
        t1, w1 = get_materiality_tier("Earnings & Forward Guidance", "Breaking Catalyst")
        self.assertEqual(t1, 1)
        self.assertEqual(w1, 1.00)

        t2, w2 = get_materiality_tier("Product, AI & Hardware Launches", "Breaking Catalyst")
        self.assertEqual(t2, 2)
        self.assertEqual(w2, 0.75)

        t_churn, w_churn = get_materiality_tier("Earnings & Forward Guidance", "Routine Churn")
        self.assertEqual(t_churn, 4)
        self.assertEqual(w_churn, 0.10)

    def test_full_enrichment_record(self):
        """Tests the unified dictionary output for downstream pipelines."""
        record = enrich_news_article(
            headline="NVIDIA beats Q2 profit estimates with 120% datacenter revenue surge",
            summary="Strong demand for AI chips drove record quarterly results and bullish Q3 guidance."
        )
        self.assertEqual(record["CatalystCategory"], "Earnings & Forward Guidance")
        self.assertEqual(record["NewsGenre"], "Breaking Catalyst")
        self.assertEqual(record["NewsSentiment"], "Bullish")
        self.assertEqual(record["MaterialityTier"], 1)


if __name__ == "__main__":
    unittest.main()
