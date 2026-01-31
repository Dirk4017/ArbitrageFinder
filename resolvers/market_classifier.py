import re
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class MarketClassification:
    """Represents a classified market with all extracted information"""
    category: str  # 'player_stat', 'scorer', 'game_total', 'team_win', 'period'
    subcategory: str  # 'player_points', 'first_scorer', 'total_points', etc.
    clean_player: str
    clean_team: str
    direction: Optional[str]  # 'over', 'under', None
    line_value: Optional[float]
    period: Optional[str]  # '1st quarter', '1st half', etc.
    stat_type: Optional[str]  # 'points', 'yards', 'tds', etc.
    original_player: str
    original_market: str
    confidence: float = 0.5  # Confidence score for prediction reliability


class MarketClassifier:
    """Classifies 157+ market types from CrazyNinjaOdds"""

    def __init__(self):
        # Confidence scores for different market types
        self.CONFIDENCE_SCORES = {
            'player_stat_ou': 0.7,  # Player stat O/U - usually reliable
            'player_stat': 0.6,  # Regular player stat
            'scorer': 0.4,  # Scorer props - less reliable
            'game': 0.8,  # Game outcomes - most reliable
            'player_prop': 0.5,  # Generic player prop - moderate
            'player_combo': 0.7,  # Combo props - usually reliable
            'player_special': 0.6,  # Special props (double double, etc.)
        }

        # Player stat markets
        self.PLAYER_STAT_MARKETS = {
            # NBA
            'player points': ('points', 'points'),
            'player rebounds': ('rebounds', 'rebounds'),
            'player assists': ('assists', 'assists'),
            'player steals': ('steals', 'steals'),
            'player blocks': ('blocks', 'blocks'),
            'player turnovers': ('turnovers', 'turnovers'),
            'player threes': ('three_pointers_made', 'three_pointers_made'),
            'player free throws': ('free_throws_made', 'free_throws_made'),
            'player field goals': ('field_goals_made', 'field_goals_made'),
            'player double double': ('double_double', 'boolean_positive'),
            'player triple double': ('triple_double', 'boolean_positive'),
            'player to have most points': ('most_points', 'tournament'),
            'player to have most rebounds': ('most_rebounds', 'tournament'),
            'player to have most assists': ('most_assists', 'tournament'),

            # COMBO MARKETS - CRITICAL ADDITION!
            'player points + rebounds + assists': ('points_rebounds_assists', 'pra'),
            'player points + rebounds': ('points_rebounds', 'pr'),
            'player points + assists': ('points_assists', 'pa'),
            'player rebounds + assists': ('rebounds_assists', 'ra'),
            'player blocks + steals': ('blocks_steals', 'blocks_steals'),

            # NFL
            'player passing yards': ('passing_yards', 'passing_yards'),
            'player passing touchdowns': ('passing_tds', 'passing_tds'),
            'player passing interceptions': ('passing_ints', 'passing_ints'),
            'player passing completions': ('completions', 'completions'),
            'player passing attempts': ('attempts', 'attempts'),
            'player rushing yards': ('rushing_yards', 'rushing_yards'),
            'player rushing touchdowns': ('rushing_tds', 'rushing_tds'),
            'player rushing attempts': ('rushing_attempts', 'rushing_attempts'),
            'player receiving yards': ('receiving_yards', 'receiving_yards'),
            'player receiving touchdowns': ('receiving_tds', 'receiving_tds'),
            'player receptions': ('receptions', 'receptions'),
            'player targets': ('targets', 'targets'),

            # NHL - ADDED NHL MARKETS!
            'player goals': ('goals', 'goals'),
            'player assists': ('assists', 'assists'),
            'player points': ('points', 'points'),
            'player shots on goal': ('shots', 'shots'),
            'player saves': ('saves', 'saves'),
            'player shots': ('shots', 'shots'),
            'player blocked shots': ('blocked_shots', 'blocked_shots'),
            'player hits': ('hits', 'hits'),
            'player penalty minutes': ('penalty_minutes', 'penalty_minutes'),
            'player plus/minus': ('plus_minus', 'plus_minus'),
            'player time on ice': ('time_on_ice', 'time_on_ice'),
            'player faceoffs won': ('faceoffs_won', 'faceoffs_won'),
            'player goals against': ('goals_against', 'goals_against'),

            # Football (generic)
            'player touchdowns': ('total_tds', 'touchdowns'),
            'player yards': ('total_yards', 'yards'),
        }

        # Over/under markets
        self.OVER_UNDER_MARKETS = [
            'over', 'under', ' o ', ' u '
        ]

        # Scorer markets (your special ones) - UPDATED WITH NHL PATTERNS
        self.SCORER_MARKETS = [
            'first scorer', 'last scorer', 'anytime scorer',
            'first touchdown', 'last touchdown', 'first td', 'last td',
            'first basket', 'first field goal', 'player to score',
            'team player first field goal', 'first goal', 'last goal',
            # NHL-SPECIFIC PATTERNS - ADDED THESE!
            'team player first goal',  # For "Team Player First Goal Scorer"
            'player first goal scorer',  # For "Player First Goal Scorer"
            'player first goal',  # For "Player First Goal"
            'first goal scorer',  # Generic first goal scorer
            'team player first goal scorer',  # Full pattern
            'goal scorer',  # Catch-all for goal scorer markets
            'player to score first',  # Another common pattern
            'player to score last',  # For last goal scorer
        ]

        # Game markets
        self.GAME_MARKETS = [
            'moneyline', 'total points', 'point spread', 'spread'
        ]

        # Period markets
        self.PERIOD_KEYWORDS = {
            '1st quarter': ['1st quarter', 'first quarter', '1q', 'q1'],
            '2nd quarter': ['2nd quarter', 'second quarter', '2q', 'q2'],
            '3rd quarter': ['3rd quarter', 'third quarter', '3q', 'q3'],
            '4th quarter': ['4th quarter', 'fourth quarter', '4q', 'q4'],
            '1st half': ['1st half', 'first half', '1h', 'h1'],
            '2nd half': ['2nd half', 'second half', '2h', 'h2'],
            # NHL Periods
            '1st period': ['1st period', 'first period'],
            '2nd period': ['2nd period', 'second period'],
            '3rd period': ['3rd period', 'third period'],
        }

    def classify(self, market: str, player_or_team: str) -> MarketClassification:
        """
        Main classification method for ANY market from CrazyNinjaOdds
        Returns: MarketClassification object with all extracted info
        """
        market_lower = market.lower()
        player_lower = (player_or_team or "").lower()

        logger.debug(f"Classifying market: '{market}' | player/team: '{player_or_team}'")

        # Step 1: Extract period information
        period = self._extract_period(market_lower)

        # Step 2: Extract direction and line value
        direction, line_value = self._extract_direction_and_line(player_or_team, market_lower)

        # Step 3: Extract clean player/team names
        clean_player, clean_team = self._extract_clean_names(player_or_team, direction, line_value)

        # Step 4: Determine category - FIXED to check combo markets first
        category, subcategory = self._determine_category_fixed(market_lower, player_lower)

        # Step 5: Extract stat type
        stat_type = self._extract_stat_type(market_lower, subcategory)

        # Step 6: Get confidence score
        confidence = self.CONFIDENCE_SCORES.get(category, 0.5)

        logger.info(f"Market classified: {category}.{subcategory}")
        logger.info(f"  Player: '{clean_player}', Team: '{clean_team}'")
        logger.info(f"  Direction: {direction}, Line: {line_value}")
        logger.info(f"  Period: {period}, Stat: {stat_type}")
        logger.info(f"  Confidence: {confidence:.2f}")

        return MarketClassification(
            category=category,
            subcategory=subcategory,
            clean_player=clean_player,
            clean_team=clean_team,
            direction=direction,
            line_value=line_value,
            period=period,
            stat_type=stat_type,
            original_player=player_or_team,
            original_market=market,
            confidence=confidence
        )

    def _extract_period(self, market: str) -> Optional[str]:
        """Extract period/quarter/half information from market"""
        for period_name, keywords in self.PERIOD_KEYWORDS.items():
            for keyword in keywords:
                if keyword in market:
                    return period_name
        return None

    def _extract_direction_and_line(self, player_string: str, market: str) -> Tuple[Optional[str], Optional[float]]:
        """Extract over/under direction and line value"""
        combined_string = f"{player_string} {market}"

        # Patterns for extraction
        patterns = [
            # "Over 120.5" or "Under 45.5"
            (r'([Oo]ver|[Uu]nder)\s+(\d+\.?\d*)', 'full'),
            # "O 120.5" or "U 45.5"
            (r'\b([Oo]|[Uu])\s+(\d+\.?\d*)', 'short'),
            # "+2.5" or "-7.5" (spreads)
            (r'([+-]\d+\.?\d*)', 'spread'),
            # Just a number at the end
            (r'(\d+\.?\d*)$', 'number_only'),
        ]

        direction = None
        line_value = None

        for pattern, pattern_type in patterns:
            match = re.search(pattern, combined_string)
            if match:
                if pattern_type == 'full':
                    direction = 'over' if match.group(1).lower() == 'over' else 'under'
                    line_value = float(match.group(2))
                elif pattern_type == 'short':
                    direction = 'over' if match.group(1).lower() == 'o' else 'under'
                    line_value = float(match.group(2))
                elif pattern_type == 'spread':
                    line_value = float(match.group(1))
                    direction = 'spread'
                elif pattern_type == 'number_only':
                    line_value = float(match.group(1))

                if direction or line_value:
                    break

        # If no direction found but we have a line, check market for direction
        if line_value and not direction:
            if 'over' in market.lower():
                direction = 'over'
            elif 'under' in market.lower():
                direction = 'under'

        return direction, line_value

    def _extract_clean_names(self, player_string: str, direction: Optional[str],
                             line_value: Optional[float]) -> Tuple[str, str]:
        """Extract clean player and team names"""
        if not player_string:
            return "", ""

        clean = player_string

        # Remove patterns
        patterns_to_remove = [
            r'\s+[Oo]ver\s+\d+\.?\d*\s*',
            r'\s+[Uu]nder\s+\d+\.?\d*\s*',
            r'\s+[Oo]\s+\d+\.?\d*\s*',
            r'\s+[Uu]\s+\d+\.?\d*\s*',
            r'\s+\+\d+\.?\d*\s*',
            r'\s+-\d+\.?\d*\s*',
            r'\s+\d+\.?\d*\s*$',
            r'\s*\(.*?\)\s*',  # Remove parentheses content
        ]

        for pattern in patterns_to_remove:
            clean = re.sub(pattern, ' ', clean)

        clean = re.sub(r'\s+', ' ', clean).strip()

        # SPECIAL HANDLING: Check for "Team Player Name" pattern
        # e.g., "Chicago Blackhawks Colton Dach" or "Boston Celtics Jayson Tatum"
        team_names = self._get_team_names()
        clean_lower = clean.lower()

        # First check if the entire string is a known team (for team bets)
        for team_name in team_names:
            if team_name == clean_lower:
                return "", clean

        # Try to find a team name at the beginning
        # IMPORTANT: Only match team names that are at least 4 characters to avoid
        # false matches like "col" matching "Colton"
        best_match = ("", "")
        best_match_length = 0
        MIN_TEAM_NAME_LENGTH = 4  # Minimum length to avoid false matches

        for team_name in team_names:
            # Skip short team names that could cause false matches
            if len(team_name) < MIN_TEAM_NAME_LENGTH:
                continue

            if clean_lower.startswith(team_name):
                # Check if this is a longer/better match
                if len(team_name) > best_match_length:
                    # Make sure the team name is followed by a space (not part of a word)
                    if len(clean_lower) == len(team_name) or clean_lower[len(team_name)] == ' ':
                        team_part = clean[:len(team_name)]
                        player_part = clean[len(team_name):].strip()

                        # Clean up any extra spaces, hyphens, or " - "
                        player_part = re.sub(r'^\s*[-\s]+\s*|\s*[-\s]+\s*$', '', player_part)
                        player_part = player_part.strip()

                        if player_part:
                            best_match = (player_part, team_part)
                            best_match_length = len(team_name)

        if best_match[0] or best_match[1]:
            return best_match

        # Also check for team name in the middle (e.g., "Player Name - Team Name")
        for team_name in team_names:
            # Skip short team names
            if len(team_name) < MIN_TEAM_NAME_LENGTH:
                continue

            if team_name in clean_lower:
                # Split on team name
                parts = re.split(team_name, clean_lower, flags=re.IGNORECASE)
                if len(parts) == 2:
                    player_part = parts[0].strip()
                    team_part = clean[clean_lower.index(team_name):clean_lower.index(team_name) + len(team_name)]

                    # Clean up player part
                    player_part = re.sub(r'[-\s]+$', '', player_part).strip()

                    if player_part:
                        return player_part, team_part

        # Assume it's a player
        return clean, ""

    def _determine_category_fixed(self, market: str, player: str) -> Tuple[str, str]:
        """
        Determine the main category and subcategory
        FIXED VERSION: Checks for combo markets first
        """
        market_lower = market.lower()

        # 0. CHECK FOR COMBO MARKETS FIRST (CRITICAL FIX!)
        # Points + Rebounds + Assists (PRA)
        if 'points' in market_lower and 'rebounds' in market_lower and 'assists' in market_lower:
            return 'player_combo', 'player points + rebounds + assists'

        # Points + Rebounds
        if ('points' in market_lower and 'rebounds' in market_lower) or ('p+r' in market_lower):
            return 'player_combo', 'player points + rebounds'

        # Points + Assists
        if ('points' in market_lower and 'assists' in market_lower) or ('p+a' in market_lower):
            return 'player_combo', 'player points + assists'

        # Rebounds + Assists
        if ('rebounds' in market_lower and 'assists' in market_lower) or ('r+a' in market_lower):
            return 'player_combo', 'player rebounds + assists'

        # Blocks + Steals
        if ('blocks' in market_lower and 'steals' in market_lower):
            return 'player_combo', 'player blocks + steals'

        # 1. Check for special player markets (double double, triple double, most points, etc.)
        if 'double double' in market_lower:
            return 'player_special', 'player double double'

        if 'triple double' in market_lower:
            return 'player_special', 'player triple double'

        if 'to have most points' in market_lower:
            return 'player_special', 'player to have most points'

        if 'to have most rebounds' in market_lower:
            return 'player_special', 'player to have most rebounds'

        if 'to have most assists' in market_lower:
            return 'player_special', 'player to have most assists'

        # 2. Check for scorer markets (your special markets)
        for scorer_keyword in self.SCORER_MARKETS:
            if scorer_keyword in market_lower:
                if 'first' in scorer_keyword:
                    return 'scorer', 'first_scorer'
                elif 'last' in scorer_keyword:
                    return 'scorer', 'last_scorer'
                else:
                    return 'scorer', 'anytime_scorer'

        # 2a. ADDITIONAL FIX: Check for NHL goal scorer patterns
        if 'goal' in market_lower and ('first' in market_lower or 'scorer' in market_lower):
            if 'first' in market_lower:
                return 'scorer', 'first_scorer'
            else:
                return 'scorer', 'anytime_scorer'

        # 3. Check for player stat markets
        for market_pattern, (stat_key, _) in self.PLAYER_STAT_MARKETS.items():
            if market_pattern in market_lower:
                # Check if it's over/under
                for ou_keyword in self.OVER_UNDER_MARKETS:
                    if ou_keyword in market_lower or ou_keyword in player:
                        return 'player_stat_ou', market_pattern
                return 'player_stat', market_pattern

        # 4. Check for game markets
        for game_market in self.GAME_MARKETS:
            if game_market in market_lower:
                if 'moneyline' in game_market:
                    return 'game', 'moneyline'
                elif 'total' in game_market:
                    return 'game', 'total'
                elif 'spread' in game_market:
                    return 'game', 'spread'

        # 5. Check for over/under markets
        for ou_keyword in self.OVER_UNDER_MARKETS:
            if ou_keyword in market_lower:
                # NHL stats
                if 'goals' in market_lower:
                    return 'player_stat_ou', 'player goals'
                elif 'shots' in market_lower and 'goal' not in market_lower:  # Shots on goal
                    return 'player_stat_ou', 'player shots'
                elif 'saves' in market_lower:
                    return 'player_stat_ou', 'player saves'
                elif 'assists' in market_lower:
                    return 'player_stat_ou', 'player assists'
                elif 'points' in market_lower:
                    return 'player_stat_ou', 'player points'
                elif 'hits' in market_lower:
                    return 'player_stat_ou', 'player hits'
                elif 'blocked shots' in market_lower or 'blocks' in market_lower:
                    return 'player_stat_ou', 'player blocked shots'

                # Other sports
                if 'receptions' in market_lower:
                    return 'player_stat_ou', 'player receptions'
                elif 'yards' in market_lower:
                    if 'receiving' in market_lower:
                        return 'player_stat_ou', 'player receiving yards'
                    elif 'rushing' in market_lower:
                        return 'player_stat_ou', 'player rushing yards'
                    elif 'passing' in market_lower:
                        return 'player_stat_ou', 'player passing yards'
                    else:
                        return 'player_stat_ou', 'player yards'
                elif 'touchdowns' in market_lower or 'td' in market_lower:
                    return 'player_stat_ou', 'player touchdowns'
                elif 'points' in market_lower:
                    return 'player_stat_ou', 'player points'
                elif 'rebounds' in market_lower:
                    return 'player_stat_ou', 'player rebounds'
                elif 'assists' in market_lower:
                    return 'player_stat_ou', 'player assists'
                elif 'threes' in market_lower or '3-point' in market_lower or '3pt' in market_lower:
                    return 'player_stat_ou', 'player threes'

        # 6. Default to player prop
        return 'player_prop', 'generic'

    def _extract_stat_type(self, market: str, subcategory: str) -> Optional[str]:
        """Extract the stat type from market"""
        # Map subcategories to stat types
        stat_mapping = {
            'player points': 'points',
            'player rebounds': 'rebounds',
            'player assists': 'assists',
            'player steals': 'steals',
            'player blocks': 'blocks',
            'player turnovers': 'turnovers',
            'player threes': 'three_pointers_made',
            'player passing yards': 'passing_yards',
            'player rushing yards': 'rushing_yards',
            'player receiving yards': 'receiving_yards',
            'player yards': 'total_yards',
            'player touchdowns': 'touchdowns',
            'player receptions': 'receptions',
            'player targets': 'targets',
            'player field goals': 'field_goals_made',
            'player free throws': 'free_throws_made',
            'player double double': 'double_double',
            'player triple double': 'triple_double',
            'player to have most points': 'most_points',
            'player to have most rebounds': 'most_rebounds',
            'player to have most assists': 'most_assists',

            # NHL stats - ADDED THESE!
            'player goals': 'goals',
            'player assists': 'assists',
            'player points': 'points',
            'player shots': 'shots',
            'player saves': 'saves',
            'player blocked shots': 'blocked_shots',
            'player hits': 'hits',
            'player penalty minutes': 'penalty_minutes',
            'player plus/minus': 'plus_minus',
            'player time on ice': 'time_on_ice',
            'player faceoffs won': 'faceoffs_won',
            'player goals against': 'goals_against',

            # Combo markets
            'player points + rebounds + assists': 'points_rebounds_assists',
            'player points + rebounds': 'points_rebounds',
            'player points + assists': 'points_assists',
            'player rebounds + assists': 'rebounds_assists',
            'player blocks + steals': 'blocks_steals',
        }

        return stat_mapping.get(subcategory)

    def _get_team_names(self) -> List[str]:
        """Get list of common team names

        IMPORTANT: Only include full team names, NOT short abbreviations like 'col', 'chi', etc.
        Short abbreviations cause false matches with player names (e.g., 'Colton' starts with 'col')
        """
        return [
            # NBA Teams - Full city + team names first (longer = better match)
            'los angeles lakers', 'golden state warriors', 'boston celtics', 'miami heat',
            'chicago bulls', 'new york knicks', 'dallas mavericks', 'denver nuggets',
            'phoenix suns', 'milwaukee bucks', 'philadelphia 76ers', 'los angeles clippers',
            'brooklyn nets', 'toronto raptors', 'atlanta hawks', 'washington wizards',
            'charlotte hornets', 'detroit pistons', 'indiana pacers', 'cleveland cavaliers',
            'orlando magic', 'minnesota timberwolves', 'oklahoma city thunder',
            'portland trail blazers', 'sacramento kings', 'san antonio spurs',
            'houston rockets', 'utah jazz', 'new orleans pelicans', 'memphis grizzlies',
            # NBA Teams - Short names
            'lakers', 'warriors', 'celtics', 'heat', 'bulls', 'knicks',
            'mavericks', 'nuggets', 'suns', 'bucks', '76ers', 'clippers',
            'nets', 'raptors', 'hawks', 'wizards', 'hornets', 'pistons',
            'pacers', 'cavaliers', 'magic', 'timberwolves', 'thunder',
            'trail blazers', 'kings', 'spurs', 'rockets', 'jazz', 'pelicans', 'grizzlies',

            # NFL Teams - Full city + team names
            'kansas city chiefs', 'san francisco 49ers', 'philadelphia eagles',
            'dallas cowboys', 'baltimore ravens', 'green bay packers', 'buffalo bills',
            'miami dolphins', 'new york jets', 'new england patriots', 'pittsburgh steelers',
            'seattle seahawks', 'los angeles rams', 'cincinnati bengals', 'minnesota vikings',
            'new orleans saints', 'new york giants', 'washington commanders',
            'denver broncos', 'las vegas raiders', 'los angeles chargers', 'cleveland browns',
            'tennessee titans', 'jacksonville jaguars', 'atlanta falcons', 'carolina panthers',
            'chicago bears', 'arizona cardinals', 'indianapolis colts', 'tampa bay buccaneers',
            # NFL Teams - Short names
            'chiefs', '49ers', 'eagles', 'cowboys', 'ravens', 'packers',
            'bills', 'dolphins', 'jets', 'patriots', 'steelers', 'seahawks',
            'rams', 'bengals', 'vikings', 'saints', 'giants', 'commanders',
            'broncos', 'raiders', 'chargers', 'browns', 'titans', 'jaguars',
            'falcons', 'panthers', 'bears', 'cardinals', 'colts', 'buccaneers',

            # NHL Teams - Full city + team names (most important for disambiguation)
            'chicago blackhawks', 'boston bruins', 'buffalo sabres', 'calgary flames',
            'carolina hurricanes', 'colorado avalanche', 'columbus blue jackets',
            'dallas stars', 'detroit red wings', 'edmonton oilers', 'florida panthers',
            'los angeles kings', 'minnesota wild', 'montreal canadiens', 'nashville predators',
            'new jersey devils', 'new york islanders', 'new york rangers', 'ottawa senators',
            'philadelphia flyers', 'arizona coyotes', 'pittsburgh penguins', 'san jose sharks',
            'st. louis blues', 'st louis blues', 'tampa bay lightning', 'toronto maple leafs',
            'vancouver canucks', 'vegas golden knights', 'washington capitals', 'winnipeg jets',
            # NHL Teams - Short names only (NO 2-3 letter abbreviations!)
            'blackhawks', 'bruins', 'sabres', 'flames', 'hurricanes', 'avalanche',
            'blue jackets', 'stars', 'red wings', 'oilers', 'panthers', 'kings',
            'wild', 'canadiens', 'predators', 'devils', 'islanders', 'rangers',
            'senators', 'flyers', 'coyotes', 'penguins', 'sharks', 'blues',
            'lightning', 'maple leafs', 'canucks', 'golden knights', 'capitals'
            # REMOVED short abbreviations: chi, bos, col, etc. - they cause false matches!
        ]

    def get_resolution_type(self, classification: MarketClassification) -> str:
        """Get the resolution type for routing to appropriate R script"""
        category_map = {
            'player_stat': 'player_stat',
            'player_stat_ou': 'player_stat',
            'scorer': 'scorer',
            'game': 'game',
            'player_prop': 'player_stat',  # Default
            'player_combo': 'player_stat',  # Combo props use player_stat resolver
            'player_special': 'player_stat'  # Special props use player_stat
        }

        return category_map.get(classification.category, 'player_stat')

    def get_market_for_r_script(self, classification: MarketClassification) -> str:
        """
        Get the market string to send to R script
        CRITICAL: Returns the ORIGINAL market name for R to parse, not simplified version
        """
        # For scorer markets, make sure we send the full market name
        if classification.category == 'scorer':
            # Map subcategory to appropriate market name for R
            if classification.subcategory == 'first_scorer':
                # Check for NHL vs NBA specific patterns
                if 'goal' in classification.original_market.lower():
                    return 'Player First Goal'
                elif 'basket' in classification.original_market.lower():
                    return 'Player First Basket'
                elif 'touchdown' in classification.original_market.lower():
                    return 'Player First Touchdown Scorer'
                else:
                    return 'Player First Goal'  # Default for NHL
            elif classification.subcategory == 'last_scorer':
                if 'goal' in classification.original_market.lower():
                    return 'Player Last Goal'
                elif 'touchdown' in classification.original_market.lower():
                    return 'Player Last Touchdown Scorer'
                else:
                    return 'Player Last Goal'
            else:
                return classification.original_market

        # For combo markets, make sure we send the full market name
        if classification.category == 'player_combo':
            # Map subcategory back to full market name
            combo_mapping = {
                'player points + rebounds + assists': 'Player Points + Rebounds + Assists',
                'player points + rebounds': 'Player Points + Rebounds',
                'player points + assists': 'Player Points + Assists',
                'player rebounds + assists': 'Player Rebounds + Assists',
                'player blocks + steals': 'Player Blocks + Steals'
            }

            full_market = combo_mapping.get(
                classification.subcategory,
                classification.original_market
            )

            logger.info(f"Sending to R: Full combo market: '{full_market}'")
            return full_market

        # For other markets, send the original market name
        logger.info(f"Sending to R: Original market: '{classification.original_market}'")
        return classification.original_market

    def to_dict(self, classification: MarketClassification) -> Dict:
        """Convert MarketClassification to dictionary for compatibility"""
        return {
            'category': classification.category,
            'subcategory': classification.subcategory,
            'clean_player': classification.clean_player,
            'clean_team': classification.clean_team,
            'direction': classification.direction,
            'line_value': classification.line_value,
            'period': classification.period,
            'stat_type': classification.stat_type,
            'original_player': classification.original_player,
            'original_market': classification.original_market,
            'confidence': classification.confidence
        }


# Test function
def test_market_classifier():
    """Test the market classifier with various markets"""
    classifier = MarketClassifier()

    test_cases = [
        # NHL SCORER MARKETS - YOUR PROBLEM CASES
        ("Team Player First Goal Scorer", "Chicago Blackhawks Colton Dach"),
        ("Player First Goal", "Colton Dach"),
        ("First Goal Scorer", "Mark Scheifele"),

        # NHL Player Stats
        ("Player Goals", "Alex Ovechkin Over 0.5"),
        ("Player Assists", "Connor McDavid Over 1.5"),
        ("Player Shots On Goal", "Auston Matthews Over 4.5"),
        ("Player Saves", "Andrei Vasilevskiy Over 28.5"),

        # Working case for comparison
        ("Player Assists", "Dougie Hamilton Over 0.5"),

        # Joel Embiid case - COMBO MARKET
        ("Player Points + Rebounds + Assists", "Joel Embiid Over 40.5"),

        # Other combo markets
        ("Player Points + Rebounds", "Stephen Curry Over 15.5"),
        ("Player Points + Assists", "Luka Doncic Over 25.5"),
        ("Player Rebounds + Assists", "Nikola Jokic Over 20.5"),

        # Special markets
        ("Player Double Double", "Anthony Davis Yes"),
        ("Player Triple Double", "LeBron James Yes"),
        ("Player To Have Most Points", "Kevin Durant"),
        ("Player Threes", "Klay Thompson Over 2.5"),

        # Regular markets
        ("Player Points", "Giannis Antetokounmpo Over 30.5"),
        ("Player Rebounds", "Rudy Gobert Over 12.5"),
        ("Player Assists", "Chris Paul Over 8.5"),

        # Scorer markets
        ("Player First Basket", "Jayson Tatum"),
        ("Player First Touchdown Scorer", "Christian McCaffrey"),

        # Game markets
        ("Moneyline", "Boston Celtics"),
        ("Point Spread", "Golden State Warriors -7.5"),
        ("Total Points Over", "Over 225.5"),
    ]

    print("=" * 80)
    print("MARKET CLASSIFIER TEST - INCLUDING NHL MARKETS")
    print("=" * 80)

    for market, player in test_cases:
        print(f"\nTest: '{market}' | Player/Team: '{player}'")
        print("-" * 40)

        try:
            result = classifier.classify(market, player)
            print(f"  Category: {result.category}")
            print(f"  Subcategory: {result.subcategory}")
            print(f"  Clean Player: '{result.clean_player}'")
            print(f"  Clean Team: '{result.clean_team}'")
            print(f"  Direction: {result.direction}")
            print(f"  Line: {result.line_value}")
            print(f"  Market for R: '{classifier.get_market_for_r_script(result)}'")

            if result.category == 'scorer':
                print(f"  ✓ SCORER MARKET DETECTED")
            elif result.category == 'player_combo':
                print(f"  ✓ COMBO MARKET DETECTED")
            elif result.category == 'player_prop' and result.subcategory == 'generic':
                print(f"  ⚠️ GENERIC (PROBLEM!)")

        except Exception as e:
            print(f"  ERROR: {e}")


def test_nhl_specific():
    """Test NHL-specific markets"""
    classifier = MarketClassifier()

    print("\n" + "=" * 80)
    print("NHL-SPECIFIC MARKET TEST")
    print("=" * 80)

    # Test your exact problematic markets
    problematic_markets = [
        ("Team Player First Goal Scorer", "Chicago Blackhawks Colton Dach"),
        ("Player First Goal", "Colton Dach"),
        ("Player Assists", "Dougie Hamilton Over 0.5"),
        ("Player Goals", "Kyle Connor Over 0.5"),
        ("Player Points", "Mark Scheifele Over 1.5"),
        ("Player Shots On Goal", "Nikolaj Ehlers Over 2.5"),
    ]

    for market, player in problematic_markets:
        print(f"\nTesting: '{market}' | '{player}'")
        result = classifier.classify(market, player)

        print(f"  Category: {result.category}")
        print(f"  Subcategory: {result.subcategory}")
        print(f"  Player: '{result.clean_player}'")
        print(f"  Team: '{result.clean_team}'")
        print(f"  Market for R: '{classifier.get_market_for_r_script(result)}'")

        if result.category == 'player_prop' and result.subcategory == 'generic':
            print(f"  ❌ FAILED: Still generic!")
        else:
            print(f"  ✓ PASSED: Correctly classified")


if __name__ == "__main__":
    test_market_classifier()
    test_nhl_specific()