"""
CrazyNinja Odds Parser - Specifically designed for CrazyNinjaOdds format
"""
import re

class CrazyNinjaOddsParser:
    @staticmethod
    def parse_crazyninja_odds(odds_text):
        """
        Specifically designed for CrazyNinjaOdds format where odds might be
        in market descriptions like "Player Touchdowns", "Player Rushing Yards"
        """
        if not odds_text:
            return None

        cleaned = str(odds_text).strip()

        # First try to extract any obvious odds patterns
        # Look for American odds: +150, -125
        american_match = re.search(r'([+-]\d+)', cleaned)
        if american_match:
            american_odds = int(american_match.group(1))
            if american_odds > 0:
                return round(american_odds / 100 + 1, 2)
            else:
                return round(100 / abs(american_odds) + 1, 2)

        # Look for decimal odds: 2.50, 1.80
        decimal_match = re.search(r'(\d+\.\d{2})', cleaned)
        if decimal_match:
            return float(decimal_match.group(1))

        # Look for fractional odds: 5/2, 3/1
        fractional_match = re.search(r'(\d+)/(\d+)', cleaned)
        if fractional_match:
            numerator = float(fractional_match.group(1))
            denominator = float(fractional_match.group(2))
            return round((numerator / denominator) + 1, 2)

        # For market types without explicit odds, use reasonable defaults
        market_types = {
            'touchdown': 2.50, 'td': 2.50, 'points': 1.90, 'yards': 1.90,
            'rushing': 1.90, 'receiving': 1.90, 'passing': 1.90,
            'over': 1.90, 'under': 1.90, 'moneyline': 2.00
        }

        cleaned_lower = cleaned.lower()
        for market_key, default_odds in market_types.items():
            if market_key in cleaned_lower:
                return default_odds

        return 1.90  # Default fallback