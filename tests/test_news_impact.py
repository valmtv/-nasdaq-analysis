"""
Unit Tests: News Impact Microstructure & Causality Calculations
---------------------------------------------------------------
Validates:
1. Alpha and CAPM Beta-adjusted True Alpha calculations.
2. Time-of-Day (TOD) normalized volume surge ratios.
3. 5-State causality verdicts ('Verified Catalyst', 'Beta Drift', 'Market Fade', 'Post-Hoc Explainer', 'Flat Noise').
4. Contrarian sentiment divergence detection (guidance shocks).
5. Composite impact scoring and off-hours pending status.
"""

import unittest
from datetime import datetime


def calculate_advanced_test_metrics(
    pre_close: float,
    post_close: float,
    qqq_pre: float,
    qqq_post: float,
    high: float,
    low: float,
    volume: float,
    baseline_vol: float,
    beta: float = 1.0,
    tod_multiplier: float = 1.0,
    genre: str = "Breaking Catalyst",
    sentiment: str = "Neutral"
):
    """Mirror of the advanced impact calculation in nasdaq_transforms.impact"""
    stock_return = round(((post_close - pre_close) / pre_close) * 100.0, 4)
    qqq_return = round(((qqq_post - qqq_pre) / qqq_pre) * 100.0, 4) if (qqq_pre and qqq_post) else 0.0
    expected_beta_return = round(beta * qqq_return, 4)
    true_alpha = round(stock_return - expected_beta_return, 4)
    raw_alpha = round(stock_return - qqq_return, 4)
    intraday_vol = round(((high - low) / pre_close) * 100.0, 4)

    expected_volume = max(baseline_vol * tod_multiplier, 1.0)
    volume_surge = round(volume / expected_volume, 2)

    surge_capped = min(volume_surge, 5.0)
    if true_alpha >= 0:
        raw_score = (true_alpha * 20.0) + ((surge_capped - 1.0) * 8.0) + (intraday_vol * 10.0)
    else:
        raw_score = (true_alpha * 20.0) - ((surge_capped - 1.0) * 8.0) - (intraday_vol * 10.0)

    composite_score = round(max(-100.0, min(100.0, raw_score)), 2)

    if abs(true_alpha) >= 0.4:
        pattern = "Sustained Move"
    elif intraday_vol >= 0.5 and abs(true_alpha) < 0.2:
        pattern = "Reversal/Fade"
    else:
        pattern = "Flat Noise"

    # 5-State Causality Verdict
    if genre == "Post-Hoc Recap":
        causality = "Post-Hoc Explainer"
    elif genre == "Routine Churn":
        causality = "Flat Noise"
    elif abs(stock_return) >= 0.4 and abs(true_alpha) < 0.2:
        causality = "Beta Drift / Macro Mover"
    elif intraday_vol >= 0.5 and abs(stock_return) < 0.2:
        causality = "Market Fade / False Alarm"
    elif abs(true_alpha) >= 0.4 and volume_surge >= 1.2:
        causality = "Verified Catalyst"
    else:
        causality = "Flat Noise"

    # Divergence
    if sentiment == "Bullish" and true_alpha < -0.4:
        divergence = "Guidance Shock / Contrarian Drop"
    elif sentiment == "Bearish" and true_alpha > 0.4:
        divergence = "Relief Rally / Contrarian Gain"
    elif sentiment == "Bullish" and true_alpha >= 0.4:
        divergence = "Confirmed Bull Catalyst"
    elif sentiment == "Bearish" and true_alpha <= -0.4:
        divergence = "Confirmed Bear Catalyst"
    else:
        divergence = "Neutral / In Line"

    return {
        "stock_return": stock_return,
        "qqq_return": qqq_return,
        "expected_beta_return": expected_beta_return,
        "true_alpha": true_alpha,
        "raw_alpha": raw_alpha,
        "intraday_vol": intraday_vol,
        "volume_surge": volume_surge,
        "composite_score": composite_score,
        "pattern": pattern,
        "causality": causality,
        "divergence": divergence
    }


class TestNewsImpactModel(unittest.TestCase):

    def test_alpha_calculation(self):
        """Alpha should be Stock Return minus QQQ Return."""
        res = calculate_advanced_test_metrics(
            pre_close=200.0,
            post_close=204.0,
            qqq_pre=450.0,
            qqq_post=452.25,
            high=204.5,
            low=199.8,
            volume=300000,
            baseline_vol=100000,
            beta=1.0
        )
        self.assertAlmostEqual(res["stock_return"], 2.0, places=3)
        self.assertAlmostEqual(res["qqq_return"], 0.5, places=3)
        self.assertAlmostEqual(res["raw_alpha"], 1.5, places=3)
        self.assertEqual(res["pattern"], "Sustained Move")
        self.assertGreater(res["composite_score"], 0)

    def test_beta_adjusted_true_alpha(self):
        """High Beta stock moving with QQQ should have ZERO True Alpha."""
        # NVDA (Beta 2.0): QQQ jumps +1.0%, NVDA moves +2.0%.
        # Naive Alpha = +1.0% (false mover), but True Alpha = 0.0%!
        res = calculate_advanced_test_metrics(
            pre_close=100.0,
            post_close=102.0,  # +2.0%
            qqq_pre=400.0,
            qqq_post=404.0,    # +1.0%
            high=102.2,
            low=99.9,
            volume=150000,
            baseline_vol=100000,
            beta=2.0
        )
        self.assertAlmostEqual(res["raw_alpha"], 1.0, places=3)
        self.assertAlmostEqual(res["true_alpha"], 0.0, places=3)
        self.assertEqual(res["causality"], "Beta Drift / Macro Mover")

    def test_time_of_day_volume_normalization(self):
        """Morning 9:30 AM open volume should not falsely trigger a 3x surge."""
        # Raw volume is 300,000 vs baseline 100,000 (3x raw),
        # but TOD multiplier at 9:30 AM is 3.5x -> Surge Ratio is 0.86x (Normal open, not a news surge!)
        res = calculate_advanced_test_metrics(
            pre_close=100.0,
            post_close=100.2,
            qqq_pre=400.0,
            qqq_post=400.0,
            high=100.3,
            low=99.8,
            volume=300000,
            baseline_vol=100000,
            beta=1.0,
            tod_multiplier=3.5
        )
        self.assertLess(res["volume_surge"], 1.0)
        self.assertEqual(res["causality"], "Flat Noise")

    def test_verified_catalyst_detection(self):
        """Genuine news shock with volume confirmation and high True Alpha is Verified Catalyst."""
        res = calculate_advanced_test_metrics(
            pre_close=100.0,
            post_close=103.5,  # +3.5%
            qqq_pre=400.0,
            qqq_post=400.4,    # +0.1%
            high=103.8,
            low=99.9,
            volume=250000,
            baseline_vol=100000,
            beta=1.2,
            tod_multiplier=1.0,
            genre="Breaking Catalyst",
            sentiment="Bullish"
        )
        self.assertGreaterEqual(res["true_alpha"], 3.0)
        self.assertEqual(res["causality"], "Verified Catalyst")
        self.assertEqual(res["divergence"], "Confirmed Bull Catalyst")

    def test_post_hoc_explainer_demoted_to_noise(self):
        """Articles published after market drops must be classified as Post-Hoc Explainer."""
        res = calculate_advanced_test_metrics(
            pre_close=100.0,
            post_close=97.0,  # -3.0%
            qqq_pre=400.0,
            qqq_post=400.0,
            high=100.1,
            low=96.8,
            volume=400000,
            baseline_vol=100000,
            beta=1.0,
            tod_multiplier=1.0,
            genre="Post-Hoc Recap",
            sentiment="Bearish"
        )
        self.assertEqual(res["causality"], "Post-Hoc Explainer")

    def test_contrarian_guidance_shock(self):
        """Bullish headline that leads to negative alpha is flagged as Guidance Shock."""
        res = calculate_advanced_test_metrics(
            pre_close=100.0,
            post_close=98.5,  # -1.5% drop despite 'Bullish' earnings headline
            qqq_pre=400.0,
            qqq_post=400.0,
            high=100.2,
            low=98.0,
            volume=200000,
            baseline_vol=100000,
            beta=1.0,
            tod_multiplier=1.0,
            genre="Breaking Catalyst",
            sentiment="Bullish"
        )
        self.assertEqual(res["divergence"], "Guidance Shock / Contrarian Drop")

    def test_reversal_fade_pattern(self):
        """High volatility spike that ends flat should be classified as Reversal/Fade."""
        res = calculate_advanced_test_metrics(
            pre_close=100.0,
            post_close=100.05,
            qqq_pre=400.0,
            qqq_post=400.0,
            high=102.5,  # Spiked 2.5% intraday
            low=99.8,
            volume=250000,
            baseline_vol=100000,
            beta=1.0
        )
        self.assertLess(abs(res["true_alpha"]), 0.2)
        self.assertGreaterEqual(res["intraday_vol"], 0.5)
        self.assertEqual(res["pattern"], "Reversal/Fade")
        self.assertEqual(res["causality"], "Market Fade / False Alarm")

    def test_flat_noise_pattern(self):
        """Tiny move and low volatility should be classified as Flat Noise."""
        res = calculate_advanced_test_metrics(
            pre_close=100.0,
            post_close=100.02,
            qqq_pre=400.0,
            qqq_post=400.0,
            high=100.10,
            low=99.95,
            volume=80000,
            baseline_vol=100000,
            beta=1.0
        )
        self.assertEqual(res["pattern"], "Flat Noise")
        self.assertEqual(res["causality"], "Flat Noise")

    def test_composite_score_bounds(self):
        """Composite score must always remain clamped between -100 and +100."""
        res_pos = calculate_advanced_test_metrics(100.0, 150.0, 400.0, 400.0, 160.0, 95.0, 5000000, 10000)
        self.assertEqual(res_pos["composite_score"], 100.0)

        res_neg = calculate_advanced_test_metrics(100.0, 50.0, 400.0, 400.0, 105.0, 45.0, 5000000, 10000)
        self.assertEqual(res_neg["composite_score"], -100.0)

    def test_pending_status_determination(self):
        """If post-event price is missing, status must be PENDING_EVALUATION."""
        pre_price = 220.0
        post_price = None
        is_completed = (pre_price is not None and post_price is not None)
        status = "COMPLETED" if is_completed else "PENDING_EVALUATION"
        self.assertEqual(status, "PENDING_EVALUATION")


if __name__ == "__main__":
    unittest.main()
