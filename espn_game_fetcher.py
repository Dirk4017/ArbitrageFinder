# espn_game_fetcher.py
import requests
import json
import re
from datetime import datetime


class ESPNGameFetcher:
    """Fetches real game data from ESPN API - COMPLETE VERSION"""

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }

    def get_game_summary(self, game_id):
        """Get complete game data"""
        url = f"https://site.web.api.espn.com/apis/site/v2/sports/football/nfl/summary"
        params = {'event': game_id}

        try:
            response = requests.get(url, params=params, headers=self.headers, timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"   ⚠️ API Error: {response.status_code}")
                return None
        except Exception as e:
            print(f"   ⚠️ Request error: {e}")
            return None

    def find_game_id_by_date_teams(self, date_str, team1, team2):
        """Find game ID by date and teams"""
        # Format: YYYYMMDD
        espn_date = date_str.replace('-', '')
        url = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
        params = {'dates': espn_date}

        try:
            response = requests.get(url, params=params, headers=self.headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                events = data.get('events', [])

                if not events:
                    print(f"   ⚠️ No games found on {date_str}")
                    return None

                # Search for matching game
                for event in events:
                    game_name = event.get('name', '').lower()
                    game_id = event.get('id')

                    # Clean team names for matching
                    team1_clean = re.sub(r'[^\w\s]', '', team1).lower().strip()
                    team2_clean = re.sub(r'[^\w\s]', '', team2).lower().strip()
                    game_name_clean = re.sub(r'[^\w\s]', '', game_name).lower()

                    # Check if both teams are in game name
                    if (team1_clean in game_name_clean and team2_clean in game_name_clean):
                        print(f"   ✅ Found game: {game_name} (ID: {game_id})")
                        return game_id

                print(f"   ❌ No game found matching {team1} vs {team2} on {date_str}")
                print(f"   Available games:")
                for event in events:
                    print(f"     - {event.get('name')}")
                return None

        except Exception as e:
            print(f"   ⚠️ Search error: {e}")

        return None

    def get_player_stats_from_game(self, game_id, player_name):
        """Get specific player stats from a game"""
        game_data = self.get_game_summary(game_id)

        if not game_data:
            return None

        # Clean player name for matching
        player_name_clean = re.sub(r'[^\w\s]', '', player_name).lower().strip()

        # Search for player in boxscore
        boxscore = game_data.get('boxscore', {})

        # Check both teams
        for team in ['away', 'home']:
            players = boxscore.get('players', {}).get(team, [])

            for player in players:
                athlete = player.get('athlete', {})
                first_name = athlete.get('firstName', '').lower()
                last_name = athlete.get('lastName', '').lower()
                full_name = f"{first_name} {last_name}"
                full_name_clean = re.sub(r'[^\w\s]', '', full_name).lower()

                # Check for match
                if (player_name_clean in full_name_clean or
                        full_name_clean in player_name_clean or
                        self._names_match(player_name_clean, first_name, last_name)):
                    print(f"   ✅ Found player in game: {full_name.title()}")
                    return self._extract_player_stats(player)

        print(f"   ❌ Player '{player_name}' not found in game {game_id}")
        return None

    def _names_match(self, search_name, first_name, last_name):
        """Check if names match approximately"""
        search_parts = set(search_name.split())
        first_last = f"{first_name} {last_name}"
        last_first = f"{last_name} {first_name}"

        return (search_name in first_last or
                search_name in last_first or
                any(part in first_last for part in search_parts))

    def _extract_player_stats(self, player_data):
        """Extract stats from player data"""
        stats = {}

        try:
            player_stats = player_data.get('stats', [])

            for stat in player_stats:
                name = stat.get('name', '').lower()
                value = stat.get('value', 0)

                # Convert to float
                try:
                    num_value = float(value)
                except:
                    num_value = 0

                # Map stats
                if 'passing' in name:
                    if 'yards' in name:
                        stats['passing_yards'] = num_value
                    elif 'touchdowns' in name or 'td' in name:
                        stats['passing_touchdowns'] = num_value
                    elif 'completions' in name:
                        stats['completions'] = num_value
                    elif 'attempts' in name:
                        stats['passing_attempts'] = num_value
                elif 'rushing' in name:
                    if 'yards' in name:
                        stats['rushing_yards'] = num_value
                    elif 'touchdowns' in name or 'td' in name:
                        stats['rushing_touchdowns'] = num_value
                    elif 'attempts' in name:
                        stats['rushing_attempts'] = num_value
                elif 'receiving' in name:
                    if 'yards' in name:
                        stats['receiving_yards'] = num_value
                    elif 'receptions' in name:
                        stats['receptions'] = num_value
                    elif 'touchdowns' in name or 'td' in name:
                        stats['receiving_touchdowns'] = num_value
                    elif 'targets' in name:
                        stats['targets'] = num_value
                elif 'fumbles' in name:
                    stats['fumbles'] = num_value
                elif 'interceptions' in name:
                    stats['interceptions'] = num_value

            # Calculate totals
            total_td = (stats.get('passing_touchdowns', 0) +
                        stats.get('rushing_touchdowns', 0) +
                        stats.get('receiving_touchdowns', 0))
            if total_td > 0:
                stats['touchdowns'] = total_td

            total_yards = (stats.get('passing_yards', 0) +
                           stats.get('rushing_yards', 0) +
                           stats.get('receiving_yards', 0))
            if total_yards > 0:
                stats['yards'] = total_yards

            # Points for basketball/hockey (if needed later)
            if 'points' in str(player_stats).lower():
                for stat in player_stats:
                    if stat.get('name', '').lower() == 'points':
                        stats['points'] = float(stat.get('value', 0))

        except Exception as e:
            print(f"   ⚠️ Stats extraction error: {e}")

        return stats if stats else {}

    def get_scoring_plays(self, game_id):
        """Get all scoring plays - CRITICAL for first/last TD scorer!"""
        game_data = self.get_game_summary(game_id)

        if not game_data:
            return []

        scoring_plays = []
        plays = game_data.get('plays', [])

        for play in plays:
            text = play.get('text', '').lower()
            play_type = play.get('type', {}).get('text', '').lower()

            # Look for scoring plays (touchdowns, field goals, safeties)
            if ('touchdown' in text or
                    ('field goal' in text and 'good' in text) or
                    'safety' in text or
                    play_type in ['field-goal', 'touchdown', 'extra-point', 'two-point-conversion']):

                scoring_play = {
                    'text': play.get('text', ''),
                    'period': play.get('period', {}).get('number', 0),
                    'clock': play.get('clock', {}).get('displayValue', ''),
                    'play_type': play_type,
                    'scoring_play': True
                }

                # Try to extract player who scored
                athletes = play.get('athletesInvolved', [])
                if athletes:
                    athlete = athletes[0]
                    scoring_play['scorer'] = athlete.get('displayName', '')
                    scoring_play['scorer_id'] = athlete.get('id', '')

                scoring_plays.append(scoring_play)

        return scoring_plays

    def get_first_scorer(self, game_id, score_type='touchdown'):
        """Get first scorer of specified type"""
        scoring_plays = self.get_scoring_plays(game_id)

        if not scoring_plays:
            return None

        for play in scoring_plays:
            if score_type == 'touchdown' and 'touchdown' in play['text'].lower():
                return play.get('scorer', '')
            elif score_type == 'any':
                return play.get('scorer', '')

        return None

    def get_last_scorer(self, game_id, score_type='touchdown'):
        """Get last scorer of specified type"""
        scoring_plays = self.get_scoring_plays(game_id)

        if not scoring_plays:
            return None

        for play in reversed(scoring_plays):
            if score_type == 'touchdown' and 'touchdown' in play['text'].lower():
                return play.get('scorer', '')
            elif score_type == 'any':
                return play.get('scorer', '')

        return None

    def get_player_touchdowns(self, game_id, player_name):
        """Get how many touchdowns a player scored in a game"""
        stats = self.get_player_stats_from_game(game_id, player_name)
        if stats:
            return stats.get('touchdowns', 0)
        return 0


# Quick test function
def test():
    """Test the fetcher"""
    print("🧪 Testing ESPN Game Fetcher")
    print("=" * 60)

    fetcher = ESPNGameFetcher()

    # Test with a known game
    game_id = "401772836"  # Saints @ Rams, 2024

    print(f"\n🔍 Testing game ID: {game_id}")

    # Get scoring plays
    scoring_plays = fetcher.get_scoring_plays(game_id)
    print(f"Found {len(scoring_plays)} scoring plays")

    if scoring_plays:
        print(f"First TD scorer: {fetcher.get_first_scorer(game_id)}")
        print(f"Last TD scorer: {fetcher.get_last_scorer(game_id)}")

    # Test player stats
    print(f"\n🔍 Testing player stats:")
    players = ["Derek Carr", "Chris Olave", "Alvin Kamara", "Matthew Stafford"]

    for player in players:
        stats = fetcher.get_player_stats_from_game(game_id, player)
        if stats:
            print(f"✅ {player}: {stats}")
        else:
            print(f"❌ {player}: Not found")


if __name__ == "__main__":
    test()