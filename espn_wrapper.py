# espn_wrapper.py
import requests
import json
from datetime import datetime
import re


class ESPNPlayerStats:
    """Working ESPN API wrapper - updated with correct endpoints"""

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }

    def search_player(self, player_name, sport='football', league='nfl'):
        """Search for player using ESPN's search API - FIXED"""
        # Clean the player name
        clean_name = re.sub(r'[^\w\s]', '', player_name).strip()

        print(f"   🔍 Searching ESPN for: {clean_name} (sport: {sport})")

        # Different search endpoints for different sports
        if sport.lower() in ['football', 'nfl']:
            url = "https://site.web.api.espn.com/apis/site/v2/sports/football/nfl/athletes"
        elif sport.lower() in ['basketball', 'nba']:
            url = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/athletes"
        else:
            # Default to NFL
            url = "https://site.web.api.espn.com/apis/site/v2/sports/football/nfl/athletes"

        params = {'search': clean_name}

        try:
            response = requests.get(url, params=params, headers=self.headers, timeout=10)

            if response.status_code == 200:
                data = response.json()
                athletes = data.get('athletes', [])

                if athletes:
                    # Find best match
                    for athlete in athletes:
                        full_name = f"{athlete.get('firstName', '')} {athlete.get('lastName', '')}"

                        # Check if names match reasonably well
                        if (clean_name.lower() in full_name.lower() or
                                full_name.lower() in clean_name.lower() or
                                self._name_similarity(clean_name, full_name) > 0.6):
                            player_info = {
                                'id': athlete.get('id'),
                                'name': full_name,
                                'team': athlete.get('team', {}).get('displayName', 'Unknown'),
                                'position': athlete.get('position', {}).get('displayName', 'Unknown'),
                                'headshot': athlete.get('headshot', {}).get('href', '')
                            }
                            print(f"   ✅ Found: {full_name} (ID: {athlete.get('id')})")
                            return player_info

                    # If no close match, return first result
                    first = athletes[0]
                    return {
                        'id': first.get('id'),
                        'name': f"{first.get('firstName', '')} {first.get('lastName', '')}",
                        'team': first.get('team', {}).get('displayName', 'Unknown'),
                        'position': first.get('position', {}).get('displayName', 'Unknown')
                    }
                else:
                    print(f"   ❌ No athletes found for {clean_name}")
                    return None

        except Exception as e:
            print(f"   ⚠️ ESPN search error: {e}")

        return None
    def _name_similarity(self, name1, name2):
        """Simple name similarity check"""
        name1_lower = name1.lower()
        name2_lower = name2.lower()

        # Check if one contains the other
        if name1_lower in name2_lower or name2_lower in name1_lower:
            return 0.9

        # Split into parts and check overlap
        parts1 = set(name1_lower.split())
        parts2 = set(name2_lower.split())

        if parts1 & parts2:  # Intersection
            return len(parts1 & parts2) / max(len(parts1), len(parts2))

        return 0

    def get_player_stats(self, player_id, sport='football', season_type=2):
        """Get player stats - season_type: 2=regular, 3=playoffs"""
        if sport.lower() in ['football', 'nfl']:
            url = f"https://site.web.api.espn.com/apis/site/v2/sports/football/nfl/athletes/{player_id}/statistics"
            params = {'seasonType': season_type}
        elif sport.lower() in ['basketball', 'nba']:
            url = f"https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/athletes/{player_id}/statistics"
            params = {'seasonType': season_type}
        else:
            return None

        try:
            response = requests.get(url, params=params, headers=self.headers, timeout=10)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            print(f"   ⚠️ Stats fetch error: {e}")

        return None

    def get_game_stats_by_date(self, player_id, game_date, sport='football'):
        """Get stats for a specific game date"""
        # Get player's season stats
        stats_data = self.get_player_stats(player_id, sport)

        if not stats_data:
            return None

        # Parse date to match ESPN format
        try:
            target_date = datetime.strptime(game_date, '%Y-%m-%d').strftime('%Y%m%d')
        except:
            target_date = game_date.replace('-', '')

        # Look for splits (game-by-game stats)
        splits = stats_data.get('splits', {}).get('categories', [])

        for category in splits:
            if category.get('name') == 'game':
                for split in category.get('splits', []):
                    stat_date = split.get('date', '')
                    if target_date in stat_date:
                        return self._extract_game_stats(split)

        return None

    def _extract_game_stats(self, game_data):
        """Extract stats from game data"""
        stats = {}

        # Extract stats from each category
        for stat in game_data.get('stats', []):
            stat_name = stat.get('name', '').lower()
            stat_value = stat.get('value', 0)

            # Map common stat names
            if 'passingyards' in stat_name or 'passing yards' in stat_name:
                stats['passing_yards'] = float(stat_value)
            elif 'rushingyards' in stat_name or 'rushing yards' in stat_name:
                stats['rushing_yards'] = float(stat_value)
            elif 'receivingyards' in stat_name or 'receiving yards' in stat_name:
                stats['receiving_yards'] = float(stat_value)
            elif 'receptions' in stat_name:
                stats['receptions'] = float(stat_value)
            elif 'passingtouchdowns' in stat_name or 'passing tds' in stat_name:
                stats['passing_touchdowns'] = float(stat_value)
            elif 'rushingtouchdowns' in stat_name or 'rushing tds' in stat_name:
                stats['rushing_touchdowns'] = float(stat_value)
            elif 'receivingtouchdowns' in stat_name or 'receiving tds' in stat_name:
                stats['receiving_touchdowns'] = float(stat_value)
            elif 'points' in stat_name:
                stats['points'] = float(stat_value)
            elif 'rebounds' in stat_name:
                stats['rebounds'] = float(stat_value)
            elif 'assists' in stat_name:
                stats['assists'] = float(stat_value)

        # Calculate totals
        if 'passing_touchdowns' in stats or 'rushing_touchdowns' in stats or 'receiving_touchdowns' in stats:
            total_td = (stats.get('passing_touchdowns', 0) +
                        stats.get('rushing_touchdowns', 0) +
                        stats.get('receiving_touchdowns', 0))
            stats['touchdowns'] = total_td

        if 'passing_yards' in stats or 'rushing_yards' in stats or 'receiving_yards' in stats:
            total_yards = (stats.get('passing_yards', 0) +
                           stats.get('rushing_yards', 0) +
                           stats.get('receiving_yards', 0))
            stats['yards'] = total_yards

        return stats if stats else None

    def get_recent_games(self, team_abbr, sport='football', limit=5):
        """Get recent games for a team"""
        if sport.lower() in ['football', 'nfl']:
            url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_abbr}/schedule"
        else:
            return None

        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                events = data.get('events', [])
                return events[:limit]
        except Exception as e:
            print(f"   ⚠️ Schedule fetch error: {e}")

        return None


# Alternative: Use a simpler direct approach
class SimpleESPNFetcher:
    """Simple ESPN data fetcher for common cases"""

    @staticmethod
    def get_nfl_boxscore(game_id):
        """Get NFL boxscore data"""
        url = f"http://site.api.espn.com/apis/site/v2/sports/football/nfl/summary"
        params = {'event': game_id}

        headers = {'User-Agent': 'Mozilla/5.0'}

        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            if response.status_code == 200:
                return response.json()
        except:
            pass

        return None

    @staticmethod
    def get_nfl_scoreboard(date_str):
        """Get NFL games for a specific date"""
        url = "http://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
        params = {'dates': date_str.replace('-', '')}

        headers = {'User-Agent': 'Mozilla/5.0'}

        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            if response.status_code == 200:
                return response.json()
        except:
            pass

        return None


# Test function
def test_espn_direct():
    """Test ESPN API directly"""
    print("🧪 Testing ESPN API Directly")
    print("=" * 50)

    # Test 1: Get today's NFL games
    today = datetime.now().strftime('%Y%m%d')
    print(f"\n📅 Testing NFL games for {today}")

    url = "http://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
    params = {'dates': today}
    headers = {'User-Agent': 'Mozilla/5.0'}

    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        print(f"Status: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            events = data.get('events', [])
            print(f"Found {len(events)} games")

            for event in events[:3]:  # Show first 3
                name = event.get('name', 'Unknown')
                status = event.get('status', {}).get('type', {}).get('description', 'Unknown')
                print(f"  📍 {name} - {status}")
        else:
            print(f"Error: {response.text[:200]}")

    except Exception as e:
        print(f"Exception: {e}")

    # Test 2: Search for a player using a different endpoint
    print(f"\n🔍 Testing player search...")
    test_players = ["Tom Brady", "Aaron Rodgers", "Patrick Mahomes"]

    for player in test_players:
        print(f"\nSearching: {player}")

        # Try a different search endpoint
        search_url = "https://site.web.api.espn.com/apis/common/v3/search"
        params = {
            'query': player,
            'limit': 5,
            'type': 'player',
            'lang': 'en',
            'region': 'us'
        }

        try:
            response = requests.get(search_url, params=params, headers=headers, timeout=10)
            print(f"Status: {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                print(f"Found {len(results)} results")

                for i, result in enumerate(results[:2]):
                    print(f"  {i + 1}. {result.get('displayName')} - {result.get('type')}")
            else:
                print(f"Error: {response.text[:100]}")

        except Exception as e:
            print(f"Exception: {e}")


if __name__ == "__main__":
    # Test the direct approach first
    test_espn_direct()

    print("\n" + "=" * 50)
    print("Testing ESPN Wrapper Class")
    print("=" * 50)

    # Then test the wrapper
    espn = ESPNPlayerStats()

    # Test with players that definitely exist
    test_cases = [
        ("Tom Brady", "football"),
        ("LeBron James", "basketball"),
        ("Aaron Rodgers", "football"),
    ]

    for player_name, sport in test_cases:
        print(f"\n🔍 Testing: {player_name} ({sport})")

        player_info = espn.search_player(player_name, sport)

        if player_info:
            print(f"✅ Found: {player_info['name']}")
            print(f"   ID: {player_info['id']}")
            print(f"   Team: {player_info['team']}")

            # Try to get stats (use a recent date)
            if player_info['id']:
                recent_date = "2024-11-24"
                stats = espn.get_game_stats_by_date(player_info['id'], recent_date, sport)

                if stats:
                    print(f"📊 Stats for {recent_date}:")
                    for key, value in stats.items():
                        print(f"   {key}: {value}")
                else:
                    print(f"⚠️ No stats found for {recent_date}")
        else:
            print(f"❌ Player not found")