"""
Comprehensive Market Resolver - Handles ALL bet types
This is your existing ComprehensiveMarketResolver class moved to its own file
"""
import logging
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

class ComprehensiveMarketResolver:
    """Comprehensive market resolver that handles ALL bet types"""

    def __init__(self, r_script_path=None):
        # Market categorization - EXPANDED FOR YOUR MARKETS
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

            # Football (generic)
            'player touchdowns': ('total_tds', 'touchdowns'),
            'player yards': ('total_yards', 'yards'),

            # CRAZYNINJA SPECIFIC
            'player first touchdown scorer': ('first_scorer', 'equals_player'),
            'player last touchdown scorer': ('last_scorer', 'equals_player'),
            'player anytime touchdown scorer': ('touchdowns', 'boolean_positive'),
            'player to score a touchdown': ('touchdowns', 'boolean_positive'),
            'player to score first': ('first_scorer', 'equals_player'),
            'player to score last': ('last_scorer', 'equals_player'),
            'team player first field goal': ('first_scorer', 'equals_player'),
            'player first basket': ('first_scorer', 'equals_player'),
            'player first field goal': ('first_scorer', 'equals_player'),
        }

        self.OVER_UNDER_MARKETS = {
            'player over': ('points', 'over_under'),
            'player under': ('points', 'over_under'),
            'player receptions over': ('receptions', 'over_under'),
            'player receptions under': ('receptions', 'over_under'),
            'player receiving yards over': ('receiving_yards', 'over_under'),
            'player receiving yards under': ('receiving_yards', 'over_under'),
            'player rushing yards over': ('rushing_yards', 'over_under'),
            'player rushing yards under': ('rushing_yards', 'over_under'),
            'player passing yards over': ('passing_yards', 'over_under'),
            'player passing yards under': ('passing_yards', 'over_under'),
            'player touchdowns over': ('touchdowns', 'over_under'),
            'player touchdowns under': ('touchdowns', 'over_under'),
            # NEW: ADD THESE FOR NBA
            'player threes over': ('three_pointers_made', 'over_under'),
            'player threes under': ('three_pointers_made', 'over_under'),
            'player blocks over': ('blocks', 'over_under'),
            'player blocks under': ('blocks', 'over_under'),
            'player steals over': ('steals', 'over_under'),
            'player steals under': ('steals', 'over_under'),
            'player rebounds over': ('rebounds', 'over_under'),
            'player rebounds under': ('rebounds', 'over_under'),
            'player assists over': ('assists', 'over_under'),
            'player assists under': ('assists', 'over_under'),
            'player points over': ('points', 'over_under'),
            'player points under': ('points', 'over_under'),
            'player points + assists over': ('points_assists', 'over_under'),
            'player points + assists under': ('points_assists', 'over_under'),
            'player rebounds + assists over': ('rebounds_assists', 'over_under'),
            'player rebounds + assists under': ('rebounds_assists', 'over_under'),
        }

        self.GAME_MARKETS = {
            # Moneyline
            'moneyline': ('moneyline', 'team_win'),

            # Totals
            'total points': ('total_points', 'total_over_under'),
            'over': ('total_points', 'total_over_under'),
            'under': ('total_points', 'total_over_under'),
            '1st half total points': ('total_points', 'total_over_under'),

            # Spread
            'point spread': ('spread', 'spread'),
            'spread': ('spread', 'spread'),
        }

    def resolve_market(self, r_result: Dict, bet: Dict, line_value: Optional[float] = None) -> Tuple[bool, float]:
        """Main resolution function that handles ALL market types"""
        try:
            market = bet.get('market', '').lower()
            sport = bet.get('sport', '').lower()
            player = bet.get('player', '')

            # Extract over/under direction from market or player name
            direction = self._extract_direction(market, player)

            # Clean market name (remove direction for matching)
            clean_market = self._clean_market_name(market, direction)

            # Get resolution method
            resolution_method = self._get_resolution_method(clean_market, sport)

            if resolution_method:
                won, actual_value = resolution_method(r_result, bet, clean_market, line_value, direction)
                return won, actual_value
            else:
                # Fallback to standard player prop
                return self._resolve_fallback(r_result, bet, line_value, direction)

        except Exception as e:
            raise Exception(f"Market resolution error: {str(e)}")

    def _get_resolution_method(self, market: str, sport: str):
        """Get appropriate resolution method"""
        # Check for first/last scorer markets
        if any(keyword in market for keyword in ['first', 'last', 'scorer', 'field goal', 'basket', 'td']):
            return self._resolve_scorer_market

        # Check for over/under markets
        if 'over' in market or 'under' in market:
            return self._resolve_over_under_market

        # Check for player stat markets
        for pattern, (stat_key, _) in self.PLAYER_STAT_MARKETS.items():
            if pattern in market:
                return self._resolve_player_stat_market

        # Check for game markets
        for pattern, (stat_key, _) in self.GAME_MARKETS.items():
            if pattern in market:
                return self._resolve_game_market

        return None

    def _resolve_scorer_market(self, r_result: Dict, bet: Dict, market: str,
                               line_value: Optional[float], direction: str) -> Tuple[bool, float]:
        """Resolve first/last scorer markets"""
        player = bet.get('player', '')
        data = r_result.get('../data', {})

        # Extract just the player name (remove team name if present)
        player_clean = self._extract_player_name_from_bet(player)

        # Get scorer from R result
        if 'first' in market:
            scorer = data.get('first_scorer', '')
        elif 'last' in market:
            scorer = data.get('last_scorer', '')
        else:
            scorer = ''

        # Check if player matches scorer
        won = self._player_name_matches(player_clean, scorer)
        actual_value = 1 if won else 0

        return won, actual_value


    def _resolve_over_under_market(self, r_result: Dict, bet: Dict, market: str,
                                   line_value: Optional[float], direction: str) -> Tuple[bool, float]:
        """Resolve over/under markets"""
        player = bet.get('player', '')
        data = r_result.get('../data', {})
        stats = data.get('stats', {})

        # Determine which stat to use
        stat_key = None
        if 'receptions' in market:
            stat_key = 'receptions'
        elif 'receiving yards' in market:
            stat_key = 'receiving_yards'
        elif 'rushing yards' in market:
            stat_key = 'rushing_yards'
        elif 'passing yards' in market:
            stat_key = 'passing_yards'
        elif 'touchdowns' in market:
            stat_key = 'touchdowns'
        elif 'threes' in market or 'three' in market:
            stat_key = 'three_pointers_made'
        elif 'blocks' in market:
            stat_key = 'blocks'
        elif 'steals' in market:
            stat_key = 'steals'
        elif 'rebounds' in market:
            stat_key = 'rebounds'
        elif 'assists' in market:
            stat_key = 'assists'
        elif 'points' in market:
            stat_key = 'points'
        elif 'points + assists' in market:
            # Calculate combined stat
            points = stats.get('points', 0)
            assists = stats.get('assists', 0)
            actual_value = points + assists
        elif 'rebounds + assists' in market:
            # Calculate combined stat
            rebounds = stats.get('rebounds', 0)
            assists = stats.get('assists', 0)
            actual_value = rebounds + assists
        else:
            # Default to points
            stat_key = 'points'

        if stat_key:
            actual_value = stats.get(stat_key, 0)

        if direction == 'over':
            if line_value is not None:
                won = actual_value > line_value
            else:
                won = actual_value > 0
        elif direction == 'under':
            if line_value is not None:
                won = actual_value < line_value
            else:
                won = actual_value < 0
        else:
            # Default to over
            if line_value is not None:
                won = actual_value > line_value
            else:
                won = actual_value > 0

        return won, actual_value

    def _resolve_player_stat_market(self, r_result: Dict, bet: Dict, market: str,
                                    line_value: Optional[float], direction: str) -> Tuple[bool, float]:
        """Resolve standard player stat markets"""
        player = bet.get('player', '')
        data = r_result.get('../data', {})
        stats = data.get('stats', {})

        # Find the stat key
        stat_key = None
        for pattern, (key, _) in self.PLAYER_STAT_MARKETS.items():
            if pattern in market:
                stat_key = key
                break

        if not stat_key:
            # Default to points
            stat_key = 'points'

        actual_value = stats.get(stat_key, 0)

        # For Yes/No markets (like "player to score a touchdown")
        if 'score' in market or 'scorer' in market or 'touchdown' in market or 'double double' in market:
            won = actual_value > 0 if stat_key != 'double_double' else actual_value
        elif line_value is not None:
            # Over/under comparison
            if direction == 'over':
                won = actual_value > line_value
            elif direction == 'under':
                won = actual_value < line_value
            else:
                won = actual_value > line_value
        else:
            # Boolean check (did they have any?)
            won = actual_value > 0

        return won, actual_value

    def _resolve_game_market(self, r_result: Dict, bet: Dict, market: str,
                             line_value: Optional[float], direction: str) -> Tuple[bool, float]:
        """Resolve game-level markets (moneyline, totals, spread)"""
        data = r_result.get('../data', {})
        event = bet.get('event', '')
        player_or_team = bet.get('player', '')

        if 'moneyline' in market:
            # Team win market
            home_team = data.get('home_team', '')
            away_team = data.get('away_team', '')
            home_score = data.get('home_score', 0)
            away_score = data.get('away_score', 0)

            # Determine which team was bet on
            if self._team_name_matches(player_or_team, home_team) or 'home' in player_or_team.lower():
                won = home_score > away_score
                actual_value = home_score - away_score
            elif self._team_name_matches(player_or_team, away_team) or 'away' in player_or_team.lower():
                won = away_score > home_score
                actual_value = away_score - home_score
            else:
                raise Exception(f"Could not determine team for moneyline: {player_or_team}")

        elif 'total' in market or 'over' in market or 'under' in market:
            # Total points market
            home_score = data.get('home_score', 0)
            away_score = data.get('away_score', 0)
            total = home_score + away_score

            if direction == 'over':
                if line_value is not None:
                    won = total > line_value
                else:
                    won = total > 0
            elif direction == 'under':
                if line_value is not None:
                    won = total < line_value
                else:
                    won = total < 0
            else:
                if line_value is not None:
                    won = total > line_value
                else:
                    won = total > 0

            actual_value = total

        elif 'spread' in market:
            # Point spread market
            home_team = data.get('home_team', '')
            away_team = data.get('away_team', '')
            home_score = data.get('home_score', 0)
            away_score = data.get('away_score', 0)

            if self._team_name_matches(player_or_team, home_team):
                adjusted_home_score = home_score + (line_value or 0)
                won = adjusted_home_score > away_score
                actual_value = adjusted_home_score - away_score
            elif self._team_name_matches(player_or_team, away_team):
                adjusted_away_score = away_score + (line_value or 0)
                won = adjusted_away_score > home_score
                actual_value = adjusted_away_score - home_score
            else:
                raise Exception(f"Could not determine team for spread: {player_or_team}")

        else:
            raise Exception(f"Unknown game market: {market}")

        return won, actual_value

    def _resolve_fallback(self, r_result: Dict, bet: Dict, line_value: Optional[float],
                          direction: str) -> Tuple[bool, float]:
        """Fallback resolution for unknown markets"""
        # Try to extract stat from market name
        market = bet.get('market', '').lower()
        data = r_result.get('../data', {})
        stats = data.get('stats', {})

        # Try to guess which stat
        if 'yards' in market:
            if 'receiving' in market:
                stat_key = 'receiving_yards'
            elif 'rushing' in market:
                stat_key = 'rushing_yards'
            elif 'passing' in market:
                stat_key = 'passing_yards'
            else:
                stat_key = 'total_yards'
        elif 'touchdown' in market or 'td' in market:
            stat_key = 'touchdowns'
        elif 'three' in market or '3pt' in market or 'threes' in market:
            stat_key = 'three_pointers_made'
        elif 'reception' in market:
            stat_key = 'receptions'
        elif 'points' in market:
            stat_key = 'points'
        elif 'rebounds' in market:
            stat_key = 'rebounds'
        elif 'assists' in market:
            stat_key = 'assists'
        elif 'steals' in market:
            stat_key = 'steals'
        elif 'blocks' in market:
            stat_key = 'blocks'
        elif 'turnovers' in market:
            stat_key = 'turnovers'
        else:
            stat_key = 'points'  # Default

        actual_value = stats.get(stat_key, 0)

        # Determine win/loss
        if direction == 'over':
            if line_value is not None:
                won = actual_value > line_value
            else:
                won = actual_value > 0
        elif direction == 'under':
            if line_value is not None:
                won = actual_value < line_value
            else:
                won = actual_value < 0
        else:
            # Boolean check
            won = actual_value > (line_value or 0.5)

        return won, actual_value

    def _extract_player_name_from_bet(self, player_string: str) -> str:
        """Extract just the player name from bet string"""
        import re

        # Remove team names and over/under indicators
        player_clean = player_string

        # Remove common team prefixes
        team_prefixes = [
            'Portland Trail Blazers', 'Oklahoma City Thunder', 'Brooklyn Nets',
            'Chicago Bulls', 'Cleveland Cavaliers', 'Philadelphia 76ers',
            'Los Angeles Clippers', 'Toronto Raptors', 'Detroit Pistons',
            'Boston Celtics', 'Minnesota Timberwolves', 'Phoenix Suns',
            'Utah Jazz', 'Houston Rockets', 'Denver Broncos', 'Buffalo Bills',
            'New England Patriots', 'Houston Texans', 'Miami (FL)', 'Indiana',
            'Miami Dolphins', 'Kansas City Chiefs', 'Baltimore Ravens',
            'San Francisco 49ers', 'Green Bay Packers', 'Dallas Cowboys'
        ]

        for prefix in team_prefixes:
            if player_clean.startswith(prefix):
                player_clean = player_clean.replace(prefix, '').strip()
                break

        # Remove Yes/No suffixes
        if player_clean.endswith('Yes'):
            player_clean = player_clean[:-3].strip()
        elif player_clean.endswith('No'):
            player_clean = player_clean[:-2].strip()

        # Remove Over/Under with numbers
        player_clean = re.sub(r'\s+[Oo]ver\s+\d+\.?\d*\s*$', '', player_clean)
        player_clean = re.sub(r'\s+[Uu]nder\s+\d+\.?\d*\s*$', '', player_clean)
        player_clean = re.sub(r'\s+-\s*\d+\.?\d*\s*$', '', player_clean)
        player_clean = re.sub(r'\s+\d+\.?\d*\s*$', '', player_clean)

        return player_clean.strip()

    def _extract_direction(self, market: str, player: str) -> str:
        """Extract over/under direction from market or player name"""
        market_lower = market.lower()
        player_lower = player.lower()

        if 'over' in market_lower or 'over' in player_lower or ' o ' in player_lower:
            return 'over'
        elif 'under' in market_lower or 'under' in player_lower or ' u ' in player_lower:
            return 'under'
        return ''

    def _clean_market_name(self, market: str, direction: str) -> str:
        """Remove direction and clean up market name"""
        clean = market.lower()
        if direction:
            clean = clean.replace(direction, '').strip()
        clean = clean.replace('over', '').replace('under', '').strip()
        return clean

    def _player_name_matches(self, name1: str, name2: str) -> bool:
        """Check if two player names match"""
        if not name1 or not name2:
            return False

        # Simple matching
        clean1 = name1.lower().replace('.', '').replace('jr', '').replace('sr', '').strip()
        clean2 = name2.lower().replace('.', '').replace('jr', '').replace('sr', '').strip()

        return (clean1 == clean2 or
                clean1 in clean2 or
                clean2 in clean1 or
                clean1.split()[-1] == clean2.split()[-1])

    def _team_name_matches(self, name1: str, name2: str) -> bool:
        """Check if two team names match"""
        if not name1 or not name2:
            return False

        clean1 = name1.lower()
        clean2 = name2.lower()

        # Common team mappings
        team_aliases = {
            'jets': ['ny jets', 'new york jets'],
            'dolphins': ['miami dolphins'],
            'chiefs': ['kansas city chiefs'],
            'ravens': ['baltimore ravens'],
            'lakers': ['la lakers', 'los angeles lakers'],
            'warriors': ['golden state warriors'],
            'celtics': ['boston celtics'],
        }

        if clean1 == clean2:
            return True

        # Check aliases
        for base, aliases in team_aliases.items():
            if clean1 == base and clean2 in aliases:
                return True
            if clean2 == base and clean1 in aliases:
                return True

        # Partial match
        return (clean1 in clean2 or clean2 in clean1)
