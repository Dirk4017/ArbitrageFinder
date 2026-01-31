"""
Enhanced Game Matcher - Improved team matching logic
"""
import re
from fuzzywuzzy import fuzz

class EnhancedGameMatcher:
    def __init__(self):
        self.setup_team_mappings()

    def setup_team_mappings(self):
        """Comprehensive team name mappings"""
        self.team_variations = {
            # NBA teams
            'lakers': ['los angeles lakers', 'la lakers'],
            'warriors': ['golden state warriors', 'gsw'],
            'celtics': ['boston celtics'],
            'bulls': ['chicago bulls'],
            'knicks': ['new york knicks'],
            'heat': ['miami heat'],
            # ... add all your team mappings here
        }

    def extract_teams_enhanced(self, event: str):
        """Enhanced team extraction with common name handling"""
        # Clean the event name first
        clean_event = re.sub(r'[^\w\s@vs\.\-&]', ' ', event)
        clean_event = re.sub(r'\s+', ' ', clean_event).strip()

        # Try different separators
        separators = [' @ ', ' vs ', ' at ', ' - ', ' vs. ']

        for sep in separators:
            if sep in clean_event:
                parts = clean_event.split(sep)
                if len(parts) >= 2:
                    team1 = parts[0].strip().lower()
                    team2 = parts[1].strip().lower()

                    # Expand common team names to full names
                    for short_name, full_names in self.team_variations.items():
                        if team1 == short_name:
                            team1 = full_names[0]  # Use the first full name
                        if team2 == short_name:
                            team2 = full_names[0]

                    return [team1, team2]

        return []

    def teams_match_enhanced(self, bet_teams, game_teams):
        """Enhanced team matching with multiple strategies"""
        if not bet_teams or len(bet_teams) != 2:
            return False

        bet_team1, bet_team2 = bet_teams
        game_home = game_teams['home'].lower()
        game_away = game_teams['away'].lower()

        # Strategy 1: Exact matching
        exact_matches = (
                (bet_team1 == game_home and bet_team2 == game_away) or
                (bet_team1 == game_away and bet_team2 == game_home)
        )

        if exact_matches:
            return True

        # Strategy 2: Partial matching
        partial_matches = (
                (bet_team1 in game_home and bet_team2 in game_away) or
                (bet_team1 in game_away and bet_team2 in game_home) or
                (game_home in bet_team1 and game_away in bet_team2) or
                (game_home in bet_team2 and game_away in bet_team1)
        )

        if partial_matches:
            return True

        # Strategy 3: Common name mapping
        for short_name, full_names in self.team_variations.items():
            expanded_bet1 = full_names[0] if bet_team1 == short_name else bet_team1
            expanded_bet2 = full_names[0] if bet_team2 == short_name else bet_team2
            expanded_home = full_names[0] if game_home == short_name else game_home
            expanded_away = full_names[0] if game_away == short_name else game_away

            expanded_matches = (
                    (expanded_bet1 == expanded_home and expanded_bet2 == expanded_away) or
                    (expanded_bet1 == expanded_away and expanded_bet2 == expanded_home)
            )

            if expanded_matches:
                return True

        # Strategy 4: Fuzzy matching as final fallback
        try:
            home_match1 = fuzz.partial_ratio(bet_team1, game_home) > 70
            away_match2 = fuzz.partial_ratio(bet_team2, game_away) > 70
            home_match2 = fuzz.partial_ratio(bet_team2, game_home) > 70
            away_match1 = fuzz.partial_ratio(bet_team1, game_away) > 70

            fuzzy_match = (home_match1 and away_match2) or (home_match2 and away_match1)

            if fuzzy_match:
                return True

        except Exception as e:
            print(f"Fuzzy matching error: {e}")

        return False