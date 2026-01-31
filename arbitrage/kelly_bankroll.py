"""
Kelly Criterion Bankroll Management - Enhanced with Safe Config Access
"""
import logging
from typing import Dict, List, Optional
import math

logger = logging.getLogger(__name__)


class KellyBankrollManager:
    def __init__(self, config):
        self.config = config

        # Safely get min_ev_threshold from betting config
        self.min_ev_threshold = self._get_config_value(['betting', 'min_ev_threshold'], 0.02)

    def _get_config_value(self, path: List[str], default: float) -> float:
        """Safely get a value from nested config structure"""
        try:
            current = self.config

            for key in path:
                if hasattr(current, key):
                    current = getattr(current, key)
                elif isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    logger.warning(f"Config path {path} not found, using default {default}")
                    return default

            return float(current)
        except Exception as e:
            logger.error(f"Error getting config value {path}: {e}")
            return default

    def calculate_stake(self, ev: float, bankroll: float, pending_bets: List[Dict],
                        odds: float, market_classification: Optional[Dict] = None) -> float:
        """
        Calculate optimal stake using Kelly Criterion with enhanced risk management

        Args:
            ev: Expected value (e.g., 0.05 for 5% EV)
            bankroll: Current bankroll amount
            pending_bets: List of pending bets with 'stake' field
            odds: Decimal odds
            market_classification: Optional market classification data

        Returns:
            Calculated stake amount, or 0 if bet should be skipped
        """
        # Use min_ev_threshold from betting config
        if ev < self.min_ev_threshold or odds <= 1.0:
            logger.debug(f"Skipping: EV {ev:.2%} < threshold {self.min_ev_threshold:.2%} or odds {odds} <= 1.0")
            return 0.0

        try:
            # Get bankroll config values safely
            max_kelly_fraction = self._get_config_value(['bankroll', 'max_kelly_fraction'], 0.25)
            min_stake_percent = self._get_config_value(['bankroll', 'min_stake_percent'], 0.005)
            max_stake_percent = self._get_config_value(['bankroll', 'max_stake_percent'], 0.10)
            max_total_stake_percent = self._get_config_value(['bankroll', 'max_total_stake_percent'], 0.40)

            # Calculate implied probability from odds
            implied_prob = 1.0 / odds

            # Calculate actual probability (implied probability + EV edge)
            actual_prob = implied_prob * (1.0 + ev)

            # Ensure probabilities are valid
            if actual_prob <= 0.01 or actual_prob >= 0.99:
                logger.debug(f"Invalid probability: {actual_prob:.2%}")
                return 0.0

            # Kelly Criterion formula
            b = odds - 1.0  # net odds
            p = actual_prob
            q = 1.0 - actual_prob

            # Full Kelly fraction
            full_kelly = (b * p - q) / b

            # Safety checks - must be positive edge
            if full_kelly <= 0:
                logger.debug(f"Negative Kelly fraction: {full_kelly:.4f}")
                return 0.0

            # Use fractional Kelly for safety
            kelly_fraction = full_kelly * max_kelly_fraction

            # Calculate stake as percentage of bankroll
            stake_percent = kelly_fraction

            # Apply minimum and maximum limits
            stake_percent = max(stake_percent, min_stake_percent)
            stake_percent = min(stake_percent, max_stake_percent)

            # Check total commitment limits
            current_pending = sum(float(bet.get('stake', 0)) for bet in pending_bets)
            current_committed_percent = current_pending / bankroll if bankroll > 0 else 0

            # Apply market classification adjustments if available
            if market_classification:
                # Handle both dictionary and MarketClassification dataclass
                if hasattr(market_classification, 'get') and callable(getattr(market_classification, 'get')):
                    # It's a dictionary
                    confidence = market_classification.get('confidence', 0.5)
                else:
                    # It's a MarketClassification dataclass
                    confidence = getattr(market_classification, 'confidence', 0.5)

                # Reduce stake for lower confidence classifications
                stake_percent *= confidence
            if current_committed_percent >= max_total_stake_percent:
                logger.debug(f"Over-committed: {current_committed_percent:.2%} >= {max_total_stake_percent:.2%}")
                # Reduce stake if over-committed
                available_percent = max(0, max_total_stake_percent - current_committed_percent)
                stake_percent = min(stake_percent, available_percent * 0.5)
                if stake_percent < min_stake_percent:
                    return 0.0

            # Calculate actual stake amount
            stake = bankroll * stake_percent

            # Absolute minimum and maximum stakes
            absolute_min_stake = 10.0  # €10 minimum
            absolute_max_stake = 500.0  # €500 maximum

            stake = max(stake, absolute_min_stake)
            stake = min(stake, absolute_max_stake)

            # Final validation - ensure stake is meaningful
            if stake < (bankroll * min_stake_percent * 0.5):
                logger.debug(f"Stake too small: {stake:.2f}")
                return 0.0

            logger.debug(f"Calculated stake: €{stake:.2f} ({stake_percent:.2%} of bankroll) "
                         f"for EV={ev:.2%}, odds={odds:.2f}")

            return round(stake, 2)

        except Exception as e:
            logger.error(f"Kelly calculation error: {e}", exc_info=True)
            return 0.0

    def get_kelly_calculation_details(self, ev: float, odds: float, bankroll: float) -> Dict:
        """Get detailed Kelly calculation info"""
        if ev < self.min_ev_threshold or odds <= 1.0:
            return {}

        try:
            max_kelly_fraction = self._get_config_value(['bankroll', 'max_kelly_fraction'], 0.25)

            implied_prob = 1.0 / odds
            actual_prob = implied_prob * (1.0 + ev)
            b = odds - 1.0
            p = actual_prob
            q = 1.0 - p

            full_kelly = (b * p - q) / b
            recommended_kelly = full_kelly * max_kelly_fraction
            recommended_stake = bankroll * recommended_kelly

            # Apply practical limits
            min_stake_percent = self._get_config_value(['bankroll', 'min_stake_percent'], 0.005)
            max_stake_percent = self._get_config_value(['bankroll', 'max_stake_percent'], 0.10)

            recommended_kelly = max(recommended_kelly, min_stake_percent)
            recommended_kelly = min(recommended_kelly, max_stake_percent)
            recommended_stake = bankroll * recommended_kelly

            return {
                'implied_prob': round(implied_prob * 100, 1),
                'actual_prob': round(actual_prob * 100, 1),
                'edge': round((actual_prob - implied_prob) * 100, 1),
                'full_kelly_percent': round(full_kelly * 100, 2),
                'recommended_kelly_percent': round(recommended_kelly * 100, 2),
                'recommended_stake': round(recommended_stake, 2),
                'edge_to_stake_ratio': round(ev / recommended_kelly if recommended_kelly > 0 else 0, 2)
            }
        except Exception as e:
            logger.error(f"Error getting Kelly details: {e}")
            return {}

    def calculate_expected_profit(self, stake: float, odds: float, win_probability: float) -> Dict:
        """
        Calculate expected profit metrics for a bet

        Returns:
            Dict with expected value, variance, and Sharpe ratio
        """
        try:
            potential_win = stake * (odds - 1)
            potential_loss = stake

            expected_value = (win_probability * potential_win) - ((1 - win_probability) * potential_loss)
            variance = (win_probability * (potential_win - expected_value) ** 2 +
                        (1 - win_probability) * (-potential_loss - expected_value) ** 2)
            std_dev = math.sqrt(variance)

            sharpe_ratio = expected_value / std_dev if std_dev > 0 else 0

            return {
                'expected_value': round(expected_value, 2),
                'variance': round(variance, 2),
                'std_dev': round(std_dev, 2),
                'sharpe_ratio': round(sharpe_ratio, 2),
                'win_amount': round(potential_win, 2),
                'loss_amount': round(potential_loss, 2)
            }
        except Exception as e:
            logger.error(f"Error calculating expected profit: {e}")
            return {}

    def adjust_for_correlation(self, stake: float, existing_bets: List[Dict],
                               new_bet: Dict) -> float:
        """
        Adjust stake based on correlation with existing bets

        Args:
            stake: Initial calculated stake
            existing_bets: List of existing bets
            new_bet: New bet being considered

        Returns:
            Adjusted stake amount
        """
        try:
            if not existing_bets:
                return stake

            # Simple correlation adjustment based on sport/market similarity
            new_sport = new_bet.get('sport', '').lower()
            new_market = new_bet.get('market', '').lower()

            correlated_exposure = 0
            for bet in existing_bets:
                bet_sport = bet.get('sport', '').lower()
                bet_market = bet.get('market', '').lower()

                # Check if bets are correlated (same sport, similar markets)
                if new_sport == bet_sport:
                    correlated_exposure += bet.get('stake', 0)

                    # Further reduce if same market type
                    if 'over' in new_market and 'over' in bet_market:
                        correlated_exposure += bet.get('stake', 0) * 0.5
                    elif 'under' in new_market and 'under' in bet_market:
                        correlated_exposure += bet.get('stake', 0) * 0.5

            # Reduce stake proportionally to correlated exposure
            if correlated_exposure > 0:
                correlation_penalty = 0.5  # Reduce stake by 50% for correlated bets
                adjusted_stake = stake * (1 - correlation_penalty)
                logger.debug(f"Adjusted stake for correlation: {stake:.2f} -> {adjusted_stake:.2f}")
                return max(adjusted_stake, 10.0)  # Minimum €10

            return stake
        except Exception as e:
            logger.error(f"Error adjusting for correlation: {e}")
            return stake