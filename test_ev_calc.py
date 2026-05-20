import unittest
from scraper.oddsportal_scraper import OddsportalScraper

class TestEVCalculation(unittest.TestCase):
    def setUp(self):
        self.scraper = OddsportalScraper()

    def test_calculate_ev_with_pinnacle(self):
        # Scenario: Pinnacle exists as baseline
        soft_odds = 2.15
        all_odds = [
            {'bookmaker': 'Pinnacle', 'odds_1': '2.00'},
            {'bookmaker': 'Bet365', 'odds_1': '2.15'}
        ]
        result = self.scraper.calculate_ev(soft_odds, all_odds, 'odds_1')

        # Fair odds = 2.00
        # EV = (2.15 / 2.00) - 1 = 0.075 (7.5%)
        self.assertAlmostEqual(result['ev'], 0.075)
        self.assertAlmostEqual(result['ev_pct'], 7.5)

    def test_calculate_ev_without_pinnacle(self):
        # Scenario: No Pinnacle, use market average adjusted for overround
        soft_odds = 2.20
        all_odds = [
            {'bookmaker': 'Bet365', 'odds_1': '2.00'},
            {'bookmaker': 'Unibet', 'odds_1': '2.00'}
        ]
        # Odds 2.00, 2.00.
        # Implied probs: 0.5, 0.5.
        # Overround: 1.0.
        # Avg odds: 2.0.
        # Baseline: 2.0 * 1.0 = 2.0
        # EV = (2.20 / 2.0) - 1 = 0.1 (10%)
        result = self.scraper.calculate_ev(soft_odds, all_odds, 'odds_1')
        self.assertAlmostEqual(result['ev'], 0.1)
        self.assertAlmostEqual(result['ev_pct'], 10.0)

    def test_calculate_ev_with_overround(self):
        # Scenario: Overround present
        soft_odds = 2.0
        all_odds = [
            {'bookmaker': 'Bet365', 'odds_1': '1.80'}, # 0.555
            {'bookmaker': 'Unibet', 'odds_1': '1.80'}  # 0.555
        ]
        # Implied probs: 1/1.8 + 1/1.8 = 0.555 + 0.555 = 1.111
        # Overround = 1.111
        # Avg odds = 1.8
        # Baseline = 1.8 * 1.111 = 1.9998
        # EV = (2.0 / 1.9998) - 1 ~ 0
        result = self.scraper.calculate_ev(soft_odds, all_odds, 'odds_1')
        self.assertAlmostEqual(result['ev'], 0, places=3)

    def test_invalid_odds(self):
        # Test behavior with missing or invalid odds
        soft_odds = 2.0
        all_odds = [{'bookmaker': 'Bet365', 'odds_1': 'invalid'}]
        result = self.scraper.calculate_ev(soft_odds, all_odds, 'odds_1')
        self.assertEqual(result['ev'], 0)

if __name__ == '__main__':
    unittest.main()
