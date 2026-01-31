"""
Enhanced Duplicate Detector
"""
import re
from datetime import datetime, timedelta
from typing import Set
from fuzzywuzzy import fuzz

class EnhancedDuplicateDetector:
    def __init__(self):
        self.placed_bets_tracker: Set[str] = set()
        self.bet_time_threshold = timedelta(hours=2)

    def is_duplicate_bet(self, event: str, market: str, odds: float, timestamp=None) -> bool:
        """Enhanced duplicate detection with time-based filtering"""
        current_time = datetime.now()

        # Create a temporary set to check recent bets
        recent_bets = set()

        for bet_key in self.placed_bets_tracker:
            parts = bet_key.split('_')
            if len(parts) >= 4:  # event_market_odds_timestamp
                bet_time = datetime.strptime(parts[3], '%Y%m%d%H%M%S')
                # Only consider bets within the time threshold
                if (current_time - bet_time) < self.bet_time_threshold:
                    recent_bets.add(bet_key)

        # Check against recent bets only
        for existing_key in recent_bets:
            existing_parts = existing_key.split('_')
            if len(existing_parts) >= 3:
                existing_event, existing_market, existing_odds = existing_parts[0], existing_parts[1], float(existing_parts[2])

                # Check event similarity
                event_similarity = fuzz.ratio(event.lower(), existing_event.lower())

                # Check market similarity
                market_similarity = fuzz.ratio(market.lower(), existing_market.lower())

                # Check odds similarity (allow small differences)
                odds_similarity = abs(odds - existing_odds) < 0.2

                if event_similarity > 80 and market_similarity > 80 and odds_similarity:
                    return True
        return False

    def add_bet(self, event: str, market: str, odds: float):
        """Add a bet to the tracker with timestamp"""
        bet_key = f"{event}_{market}_{odds:.2f}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        self.placed_bets_tracker.add(bet_key)

        # Clean up old bets
        self.clean_old_bets()

    def clean_old_bets(self):
        """Remove bets older than the time threshold"""
        current_time = datetime.now()
        to_remove = set()

        for bet_key in self.placed_bets_tracker:
            parts = bet_key.split('_')
            if len(parts) >= 4:
                bet_time = datetime.strptime(parts[3], '%Y%m%d%H%M%S')
                if (current_time - bet_time) >= self.bet_time_threshold:
                    to_remove.add(bet_key)

        self.placed_bets_tracker -= to_remove