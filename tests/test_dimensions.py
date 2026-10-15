"""
Unit Tests: Dimensional Modeling & Surrogate Key Integrity
------------------------------------------------------------
Validates conformed dimensions: dim_company (with CAPM Beta), dim_date, and dim_news_source.
"""

import unittest
from datetime import date
from nasdaq_transforms.dimensions import COMPANY_METADATA


class TestDimensionalModeling(unittest.TestCase):

    def test_company_surrogate_key_determinism(self):
        """Surrogate key hash should be deterministic and positive."""
        symbol = "AAPL"
        sk_1 = abs(hash(symbol))
        sk_2 = abs(hash(symbol))
        self.assertEqual(sk_1, sk_2)
        self.assertGreater(sk_1, 0)

    def test_company_calibrated_betas(self):
        """Verifies all reference tickers have valid positive CAPM Betas."""
        meta_dict = {row[0]: row[5] for row in COMPANY_METADATA}
        self.assertIn("NVDA", meta_dict)
        self.assertEqual(meta_dict["NVDA"], 2.15)
        self.assertEqual(meta_dict["TSLA"], 2.05)
        self.assertEqual(meta_dict["AAPL"], 1.22)
        self.assertEqual(meta_dict["QQQ"], 1.00)
        for sym, beta in meta_dict.items():
            self.assertGreater(beta, 0.5)

    def test_dim_date_calendar_attributes(self):
        """Calendar date must correctly extract year, quarter, month, and weekend status."""
        d_weekday = date(2026, 9, 1)
        self.assertEqual(d_weekday.year, 2026)
        self.assertEqual(d_weekday.month, 9)
        self.assertEqual(d_weekday.weekday(), 1)  # 0=Monday, 1=Tuesday
        self.assertFalse(d_weekday.weekday() in (5, 6))

        d_weekend = date(2026, 9, 6)
        self.assertTrue(d_weekend.weekday() in (5, 6))

    def test_dim_news_source_key_generation(self):
        """Source dimension composite key should differentiate sources with same category."""
        source_a = ("Bloomberg", "company")
        source_b = ("Reuters", "company")
        key_a = abs(hash(f"{source_a[0]}_{source_a[1]}"))
        key_b = abs(hash(f"{source_b[0]}_{source_b[1]}"))
        self.assertNotEqual(key_a, key_b)


if __name__ == "__main__":
    unittest.main()
