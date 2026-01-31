"""
Universal Bet Parser - Intelligently parses ANY bet type from CrazyNinjaOdds
"""
import re
from typing import Dict, List

class UniversalBetParser:
    def __init__(self):
        self.stat_mappings = {
            'points': {'sources': ['espn', 'basketball_reference'], 'key': 'points'},
            'rebounds': {'sources': ['espn', 'basketball_reference'], 'key': 'rebounds'},
            'assists': {'sources': ['espn', 'basketball_reference'], 'key': 'assists'},
            'touchdowns': {'sources': ['espn', 'pro_football_reference'], 'key': 'touchdowns'},
            'yards': {'sources': ['espn', 'pro_football_reference'], 'key': 'yards'},
            'receptions': {'sources': ['espn', 'pro_football_reference'], 'key': 'receptions'},
        }

    def parse_any_bet(self, sport: str, event: str, market: str, selection: str) -> Dict:
        """
        Universal parser that identifies:
        1. Bet type
        2. Required data to scrape
        3. Resolution logic needed
        """
        result = {
            'bet_type': 'unknown',
            'sport': sport.lower(),
            'event': event,
            'market_raw': market,
            'selection_raw': selection,
            'required_data': [],
            'resolution_logic': 'unknown',
            'parsed_components': {}
        }

        # Identify bet type
        bet_type = self._identify_bet_type(market, selection)
        result['bet_type'] = bet_type

        # Parse components
        components = self._parse_components(bet_type, market, selection, event)
        result['parsed_components'] = components

        # Determine required data
        required_data = self._determine_required_data(bet_type, components, sport)
        result['required_data'] = required_data

        # Set resolution logic
        resolution_logic = self._determine_resolution_logic(bet_type, components)
        result['resolution_logic'] = resolution_logic

        return result

    def _identify_bet_type(self, market: str, selection: str) -> str:
        """Identify what type of bet this is"""
        market_lower = market.lower()
        selection_lower = selection.lower()

        # Player props
        if 'player' in market_lower:
            if 'first' in market_lower and 'scorer' in market_lower:
                return 'player_first_scorer'
            elif 'last' in market_lower and 'scorer' in market_lower:
                return 'player_last_scorer'
            elif 'touchdown' in market_lower or 'td' in market_lower:
                return 'player_touchdown'
            elif any(stat in market_lower for stat in ['points', 'rebounds', 'assists', 'yards', 'receptions']):
                if 'over' in selection_lower or 'under' in selection_lower:
                    return 'player_stat_over_under'
                else:
                    return 'player_stat_exact'
            else:
                return 'player_prop_generic'

        # Team/Game props
        elif 'point spread' in market_lower or 'spread' in market_lower:
            return 'point_spread'
        elif 'moneyline' in market_lower:
            return 'moneyline'
        elif 'total points' in market_lower or 'over/under' in market_lower:
            return 'game_total'
        elif 'quarter' in market_lower and 'total' in market_lower:
            return 'quarter_total'

        # Team props
        elif 'team' in market_lower:
            return 'team_prop'

        return 'unknown'

    def _parse_components(self, bet_type: str, market: str, selection: str, event: str) -> Dict:
        """Extract all relevant components from the bet"""
        components = {}

        # Extract teams from event
        teams = self._extract_teams(event)
        components['teams'] = teams

        if bet_type.startswith('player_'):
            # Player bets
            player_info = self._parse_player_selection(selection)
            components.update(player_info)

            # Extract stat from market
            stat_match = re.search(r'(points|rebounds|assists|yards|touchdowns|receptions|threes)', market.lower())
            if stat_match:
                components['stat'] = stat_match.group(1)

            # Extract line if present
            line_match = re.search(r'(\d+[\.,]?\d*)', selection)
            if line_match:
                components['line'] = float(line_match.group(1).replace(',', '.'))

            # Extract direction
            if 'over' in selection.lower():
                components['direction'] = 'over'
            elif 'under' in selection.lower():
                components['direction'] = 'under'

        elif bet_type in ['point_spread', 'moneyline', 'team_prop']:
            # Team bets
            team_info = self._parse_team_selection(selection, teams)
            components.update(team_info)

        elif bet_type in ['game_total', 'quarter_total']:
            # Total bets
            total_info = self._parse_total_selection(selection)
            components.update(total_info)

        return components

    def _extract_teams(self, event: str) -> List[str]:
        """Extract team names from event string"""
        separators = [' @ ', ' vs ', ' at ', ' - ']
        for sep in separators:
            if sep in event:
                parts = event.split(sep)
                if len(parts) >= 2:
                    return [part.strip() for part in parts[:2]]
        return [event, 'Unknown']

    def _parse_player_selection(self, selection: str) -> Dict:
        """Extract player name from selection"""
        # Remove $ amounts
        clean = re.sub(r'\(\$[^)]+\)', '', selection).strip()

        # Try to find player name (everything before Over/Under)
        patterns = [
            r'^(.*?)\s+over\s+\d+',  # "Player Over X.X"
            r'^(.*?)\s+under\s+\d+',  # "Player Under X.X"
            r'^(.*?)\s+to\s+',  # "Player To Score"
            r'^(.*?)\s+first\s+',  # "Player First"
            r'^(.*?)\s+last\s+',  # "Player Last"
        ]

        for pattern in patterns:
            match = re.match(pattern, clean, re.IGNORECASE)
            if match:
                return {'player': match.group(1).strip().title()}

        # If no pattern matches, assume entire string is player name
        return {'player': clean.title()}

    def _parse_team_selection(self, selection: str, teams: List[str]) -> Dict:
        """Extract team info from selection"""
        # Check if selection matches a team
        for team in teams:
            if team.lower() in selection.lower():
                # Check for spread
                spread_match = re.search(r'([+-]\d+[\.,]?\d*)', selection)
                if spread_match:
                    return {
                        'team': team,
                        'spread': spread_match.group(1),
                        'line': float(spread_match.group(1)[1:].replace(',', '.'))
                    }
                return {'team': team}

        return {'team': selection}

    def _parse_total_selection(self, selection: str) -> Dict:
        """Extract over/under info from totals bet"""
        if 'over' in selection.lower():
            direction = 'over'
        elif 'under' in selection.lower():
            direction = 'under'
        else:
            direction = None

        line_match = re.search(r'(\d+[\.,]?\d*)', selection)
        line = float(line_match.group(1).replace(',', '.')) if line_match else None

        return {'direction': direction, 'line': line}

    def _determine_required_data(self, bet_type: str, components: Dict, sport: str) -> List[str]:
        """Determine what data needs to be scraped for this bet"""
        required = []

        if bet_type.startswith('player_'):
            required.append('player_stats')
            if 'first_scorer' in bet_type or 'last_scorer' in bet_type:
                required.append('play_by_play')

        elif bet_type in ['point_spread', 'moneyline', 'game_total', 'team_prop', 'quarter_total']:
            required.append('game_result')

        return required

    def _determine_resolution_logic(self, bet_type: str, components: Dict) -> str:
        """Determine what logic to apply for resolution"""
        if bet_type == 'player_stat_over_under':
            return 'compare_stat_to_line'
        elif bet_type == 'player_first_scorer':
            return 'check_first_scorer'
        elif bet_type == 'player_last_scorer':
            return 'check_last_scorer'
        elif bet_type == 'player_touchdown':
            return 'check_touchdown'
        elif bet_type == 'point_spread':
            return 'calculate_spread'
        elif bet_type == 'moneyline':
            return 'check_winner'
        elif bet_type == 'game_total':
            return 'sum_game_points'
        elif bet_type == 'quarter_total':
            return 'sum_quarter_points'

        return 'generic_comparison'