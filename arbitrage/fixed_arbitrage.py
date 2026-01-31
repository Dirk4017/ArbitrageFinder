"""
Fixed Arbitrage System - Processes betting opportunities
"""
import re
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from .kelly_bankroll import KellyBankrollManager
from duplicate_detector import EnhancedDuplicateDetector

logger = logging.getLogger(__name__)


class FixedArbitrageSystem:
    def __init__(self, config, bankroll_manager: KellyBankrollManager):
        self.config = config
        self.bankroll_manager = bankroll_manager
        self.duplicate_detector = EnhancedDuplicateDetector()

    def parse_odds(self, odds_text: str) -> Optional[float]:
        """
        Parse odds from text to decimal odds
        Specifically designed for CrazyNinjaOdds format
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

    def process_opportunities(self, opportunities: List[Dict], current_bankroll: float,
                              pending_bets: List[Dict]) -> List[Dict]:
        """Process opportunities and filter to bettable ones"""
        logger.info(f"Processing {len(opportunities)} opportunities")

        filtered_opportunities = []

        for opp in opportunities:
            odds_str = opp.get('odds', '')
            sportsbook = opp.get('sportsbook', 'Unknown')

            # Parse odds
            decimal_odds = self.parse_odds(odds_str)
            if decimal_odds is None:
                logger.warning(f"Could not convert odds: '{odds_str}'")
                continue

            ev_value = float(opp.get('ev', 0))
            event = opp.get('event', '')
            market = opp.get('market', '')
            player = opp.get('player', 'Unknown')

            # Duplicate detection
            if self.duplicate_detector.is_duplicate_bet(event, market, decimal_odds):
                logger.info(f"Skipping duplicate: {event} - {player}")
                continue

            # Kelly-based stake calculation
            stake = self.bankroll_manager.calculate_stake(ev_value, current_bankroll,
                                                          pending_bets, decimal_odds)
            if stake <= 0:
                kelly_info = self.bankroll_manager.get_kelly_calculation_details(ev_value,
                                                                                 decimal_odds,
                                                                                 current_bankroll)
                if kelly_info:
                    logger.info(
                        f"No bet: {event} - {player} (Edge: {kelly_info['edge']}%, Kelly: {kelly_info['recommended_kelly_percent']}%)")
                else:
                    logger.info(f"No bet: {event} - {player} (EV: {ev_value:.1%}, Odds: {decimal_odds:.2f})")
                continue

            filtered_opp = opp.copy()
            filtered_opp['decimal_odds'] = decimal_odds
            filtered_opp['calculated_stake'] = stake
            filtered_opp['sportsbook'] = sportsbook
            filtered_opportunities.append(filtered_opp)

            self.duplicate_detector.add_bet(event, market, decimal_odds)

            # Show Kelly calculation details
            kelly_info = self.bankroll_manager.get_kelly_calculation_details(ev_value,
                                                                             decimal_odds,
                                                                             current_bankroll)
            stake_percent = (stake / current_bankroll * 100) if current_bankroll > 0 else 0

            logger.info(f"APPROVED: {event}")
            logger.info(f"   Player: {player} | Market: {market} | Odds: {decimal_odds:.2f} | EV: {ev_value:.1%}")
            logger.info(
                f"   Kelly: {kelly_info['recommended_kelly_percent']}% | Stake: {stake_percent:.2f}% (€{stake:.2f})")
            logger.info(f"   Edge: +{kelly_info['edge']}% | Win Prob: {kelly_info['actual_prob']}%")

        logger.info(f"Filtered to {len(filtered_opportunities)} bettable opportunities")
        return filtered_opportunities

    def calculate_stake(self, ev: float, bankroll: float, pending_bets: List[Dict],
                        odds: float, market_classification: Optional[Dict] = None) -> float:
        """Calculate stake for a bet"""
        return self.bankroll_manager.calculate_stake(ev, bankroll, pending_bets, odds,
                                                     market_classification)